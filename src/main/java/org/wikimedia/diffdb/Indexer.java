package org.wikimedia.diffdb;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.ArrayList;
import java.util.List;
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
	private final ExecutorService executor;
	private final BlockingQueue<Document> prodq;
	private final BlockingQueue<Document> poolq;
	private final List<Runnable> producers;
	private final long start;
	
	public Indexer(final IndexWriter writer, int nthreads, int poolsize, long duration) {
		this.start = System.currentTimeMillis();
		this.writer = writer;
		this.reportInterval = duration;
		this.executor = Executors.newFixedThreadPool(nthreads);
		this.prodq = new ArrayBlockingQueue<Document>(poolsize);
		this.poolq = new ArrayBlockingQueue<Document>(poolsize);
		this.producers = Collections.synchronizedList(new ArrayList<Runnable>());
		// run a thread that reports the progress periodically
		new Thread(new Runnable() {
				public void run() {
					try {
						while (!executor.isTerminated()) {
							System.err.println("" + writer.numDocs()
																 + " documents have been indexed in "
																 + (System.currentTimeMillis() - start)
																 + " msecs (products " + prodq.size() + ", producers " + producers.size() +  ")");
							Thread.sleep(reportInterval);
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
	}


	public void finish() throws InterruptedException, IOException {
		try {
			// This will make the executor accept no new threads
			// and finish all existing threads in the queue
			this.executor.shutdown();
			
			// Wait until all threads are finish
			while (!this.executor.isTerminated()) {
				Thread.sleep(1000L);
			}
			System.out.println("Finished all threads");
			// this.writer.optimize();
			System.out.println("Writing " + this.writer.numDocs() + " documents.");
		} finally {
			this.writer.close();
		}
	}
	
	public void indexDocuments(File file) throws IOException {
		if (file.canRead()) {
			if (file.isDirectory()) {
				for (File f : file.listFiles()) {
					indexDocuments(f);
				}
			} else {
				executor.execute(new DiffDocumentProducer(new FileReader(file), this.prodq, this.poolq, this.producers));
				executor.execute(new DiffDocumentConsumer(this.writer, this.prodq, this.poolq, this.producers));
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
		int ngram = 3;
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
			if ( (s = System.getProperty("ngram")) != null ) {
				ngram = Integer.parseInt(s);
			}
		}

		// setup the writer configuration
		Directory dir = new NIOFSDirectory(new File(indexDir), null);
		LogDocMergePolicy lmp = new LogDocMergePolicy();
		lmp.setUseCompoundFile(true); // This might fix the too many open files,
		// see
		// http://wiki.apache.org/lucene-java/LuceneFAQ#Why_am_I_getting_an_IOException_that_says_.22Too_many_open_files.22.3F

		IndexWriterConfig cfg = new IndexWriterConfig(Version.LUCENE_34,
																									new SimpleNGramAnalyzer(ngram));
		cfg.setOpenMode(OpenMode.CREATE_OR_APPEND); // http://lucene.apache.org/java/3_2_0/api/core/org/apache/lucene/index/IndexWriterConfig.OpenMode.html#CREATE_OR_APPEND
		cfg.setRAMBufferSizeMB(ramBufferSizeMB);
		cfg.setMergePolicy(lmp);

		Indexer indexer = null;
		try {
			indexer = new Indexer(new IndexWriter(dir, cfg), nThreads, poolsize, reportInterval);
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
