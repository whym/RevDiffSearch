package org.wikimedia.diffdb;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.HashSet;
import java.util.Set;
import java.util.Collections;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.LogDocMergePolicy;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.util.Version;

public class Indexer {
	private static final Logger logger = Logger.getLogger(Indexer.class.getName());

	private final long reportInterval;
	private final IndexWriter writer;
	private final ExecutorService producerExecutor;
	private final ExecutorService consumerExecutor;
	private final BlockingQueue<Document> prodq;
	private final BlockingQueue<Document> poolq;
	private final long start;
	private final DiffDocumentProducer.Filter filter;
	private final double consumerRatio;
	private boolean finished;
	
	public Indexer(final IndexWriter writer, int nthreads, int poolsize, long duration, DiffDocumentProducer.Filter filter) {
		if ( nthreads <= 1 ) {
			throw new IllegalArgumentException("number of threads must be at least 2");
		}
		this.consumerRatio = 0.5;
		this.start = System.currentTimeMillis();
		this.writer = writer;
		this.reportInterval = duration;
		this.finished = false;
		int nconsumers = Math.max((int)(nthreads * consumerRatio), 1);
		this.consumerExecutor = Executors.newFixedThreadPool(nconsumers);
		this.producerExecutor = Executors.newFixedThreadPool(nthreads - nconsumers);
		this.prodq = new ArrayBlockingQueue<Document>(poolsize);
		this.poolq = new ArrayBlockingQueue<Document>(poolsize);
		this.filter = filter;
		// run a thread that reports the progress periodically
		final Object this_ = this;
		new Thread(new Runnable() {
				public void run() {
					try {
						while ( true ) {
							synchronized (this_) {
								if ( !isClosed() ) {
									System.err.println("" + writer.numDocs()
																		 + " documents have been indexed in "
																		 + (System.currentTimeMillis() - start)
																		 + " msecs (products " + prodq.size() + ")");
									Thread.sleep(reportInterval);
								} else {
									break;
								}
							}
						}
					} catch (IOException e) {
						e.printStackTrace();
					} catch (InterruptedException e) {
						System.err.println("Interrupted");
					}
				}
			}).start();

		// initialize queues
		while ( this.poolq.remainingCapacity() > 0 ) {
			this.poolq.add(DiffDocumentProducer.createEmptyDocument());
		}
		for ( int i = 0; i < nconsumers; ++i ) {
			this.consumerExecutor.execute(new DiffDocumentConsumer(this.writer, this.prodq, this.poolq));
		}
	}
	public Indexer(final IndexWriter writer, int nthreads, int poolsize, long duration) {
		this(writer, nthreads, poolsize, duration, DiffDocumentProducer.Filter.PASS_ALL);
	}


	public boolean isClosed() {
		return this.finished;
	}
	public synchronized void finish() throws InterruptedException, IOException {
		try {
			// This will make the executor accept no new threads
			// and finish all existing threads in the queue
			this.producerExecutor.shutdown();
			
			// Wait until all threads are finish
			while (!this.producerExecutor.isTerminated()) {
				Thread.sleep(100L);
			}
			// Make sure all products are consumed
			while (this.prodq.size() > 0) {
				Thread.sleep(100L);
			}
			// Destroy consumers
			this.consumerExecutor.shutdownNow();
			System.out.println("Finished all threads");
			// this.writer.optimize();
			System.out.println("Writing " + this.writer.numDocs() + " documents.");
		} finally {
			this.writer.close();
			this.finished = true;
		}
	}
	
	public void indexDocuments(File file) throws IOException {
		if (file.canRead()) {
			if (file.isDirectory()) {
				for (File f : file.listFiles()) {
					indexDocuments(f);
				}
			} else {
				producerExecutor.execute(new DiffDocumentProducer(new FileReader(file), this.prodq, this.poolq, this.filter));
			}
		}
	}

	public static void main(String[] args) throws IOException, InterruptedException {
		// load configurations
		int nThreads = 15;
		long reportInterval = 10000L;
		if (args.length != 2) {
			System.err.println("Usage: java -Dngram=N " + Indexer.class.getName()
												 + " <index dir> <data dir>");
			System.exit(1);
		}
		final long start = System.currentTimeMillis();
		String indexDir = args[0];
		String dataDir = args[1];
		double ramBufferSizeMB = 1024;
		int poolsize = nThreads * 10000;
		DiffDocumentProducer.Filter filter = DiffDocumentProducer.Filter.PASS_ALL;
		{
			String s;
			if ( (s = System.getProperty("poolSize")) != null ) {
				poolsize = Integer.parseInt(s);
			}
			if ( (s = System.getProperty("reportInterval")) != null ) {
				reportInterval = Integer.parseInt(s);
			}
			if ( (s = System.getProperty("nThreads")) != null ) {
				nThreads = Integer.parseInt(s);
			}
			if ( (s = System.getProperty("ramBufferSize")) != null ) {
				ramBufferSizeMB = Integer.parseInt(s);
			}
			if ( (s = System.getProperty("filter")) != null ) {
				filter = DiffDocumentProducer.Filter.valueOf(s);
			}
		}

		// setup the writer configuration
		Directory dir = new NIOFSDirectory(new File(indexDir), null);
		LogDocMergePolicy lmp = new LogDocMergePolicy();
		lmp.setUseCompoundFile(true); // This might fix the too many open files,
		// see
		// http://wiki.apache.org/lucene-java/LuceneFAQ#Why_am_I_getting_an_IOException_that_says_.22Too_many_open_files.22.3F

		IndexWriterConfig cfg = new IndexWriterConfig(Version.LUCENE_35,
																									DiffDbUtils.getAnalyzer());
		cfg.setOpenMode(OpenMode.CREATE_OR_APPEND); // http://lucene.apache.org/java/3_2_0/api/core/org/apache/lucene/index/IndexWriterConfig.OpenMode.html#CREATE_OR_APPEND
		cfg.setRAMBufferSizeMB(ramBufferSizeMB);
		cfg.setMergePolicy(lmp);

		Indexer indexer = null;
		try {
			indexer = new Indexer(new IndexWriter(dir, cfg), nThreads, poolsize, reportInterval, filter);
			indexer.indexDocuments(new File(dataDir));
		} finally {
			if ( indexer != null ) {
				indexer.finish();
			}
			System.err.println("Finished in "
					+ (System.currentTimeMillis() - start) + " msecs");
		}
	}

}
/*
 * Local variables:
 * tab-width: 2
 * c-basic-offset: 2
 * indent-tabs-mode: t
 * End:
 */
