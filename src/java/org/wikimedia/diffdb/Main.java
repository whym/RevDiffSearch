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

import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.LogDocMergePolicy;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.util.Version;

public class Main {
	private static final int NTHREDS = 9;
	private static final long REPORT_DURATION_MSECS = 10000L;
	public static String indexDir = null;
	public static String dataDir = null;

	private static void indexDocuments(ExecutorService executor,
																		 BlockingQueue<Document> prodq,
																		 BlockingQueue<Document> poolq,
																		 List<Runnable> producers,
																		 IndexWriter writer, File file) {
		if (file.canRead()) {
			if (file.isDirectory()) {
				for (File f : file.listFiles()) {
					indexDocuments(executor, prodq, poolq, producers, writer, f);
				}
			} else {
				//Runnable worker = new Indexer(writer, file);
				try {
					executor.execute(new DiffDocumentProducer(new FileReader(file), prodq, poolq, producers));
					executor.execute(new DiffDocumentConsumer(writer, prodq, poolq, producers));
				} catch ( IOException e ) {
					System.err.println(e);
				}
			}
		}
	}

	public static void main(String[] args) throws IOException,
	InterruptedException {
		if (args.length != 2) {
			System.err.println("Usage: java " + Main.class.getName()
					+ " <index dir> <data dir>");
			System.exit(1);
		}
		indexDir = args[0];
		dataDir = args[1];
		double ramBufferSizeMB = 128;

		final long start = System.currentTimeMillis();

		final ExecutorService executor = Executors.newFixedThreadPool(NTHREDS);
		Directory dir = new NIOFSDirectory(new File(indexDir), null);
		LogDocMergePolicy lmp = new LogDocMergePolicy();
		lmp.setUseCompoundFile(true); // This might fix the too many open files,
		// see
		// http://wiki.apache.org/lucene-java/LuceneFAQ#Why_am_I_getting_an_IOException_that_says_.22Too_many_open_files.22.3F

		IndexWriterConfig cfg = new IndexWriterConfig(Version.LUCENE_34,
				new SimpleNGramAnalyzer(3));

		cfg.setOpenMode(OpenMode.CREATE_OR_APPEND); // http://lucene.apache.org/java/3_2_0/api/core/org/apache/lucene/index/IndexWriterConfig.OpenMode.html#CREATE_OR_APPEND
		cfg.setRAMBufferSizeMB(ramBufferSizeMB);
		cfg.setMergePolicy(lmp);

		final IndexWriter writer = new IndexWriter(dir, cfg);
		final BlockingQueue<Document> prodq = new ArrayBlockingQueue<Document>(NTHREDS * 100);
		final BlockingQueue<Document> poolq = new ArrayBlockingQueue<Document>(NTHREDS * 100);
		try {
			// run a thread that reports the progress periodically
			new Thread(new Runnable() {
				public void run() {
					try {
						while (!executor.isTerminated()) {
							System.err.println("" + writer.numDocs()
									+ " documents have been indexed in "
									+ (System.currentTimeMillis() - start)
																 + " msecs (prod " + prodq.size() + ")");
							Thread.sleep(REPORT_DURATION_MSECS);
						}
					} catch (IOException e) {
						e.printStackTrace();
					} catch (InterruptedException e) {
						System.err.println("Interrupted");
					}
				}
			}).start();

			for ( int i = 0; i < NTHREDS; ++i ) {
				poolq.add(DiffDocumentProducer.createEmptyDocument());
			}
			indexDocuments(executor,
										 prodq,
										 poolq,
										 Collections.synchronizedList(new ArrayList<Runnable>()),
										 writer,
										 new File(dataDir));
			// This will make the executor accept no new threads
			// and finish all existing threads in the queue
			executor.shutdown();

			// Wait until all threads are finish
			while (!executor.isTerminated()) {
				Thread.sleep(1000L);
			}
			System.out.println("Finished all threads");
			// writer.optimize();
			System.out.println("Writing " + writer.numDocs() + " documents.");

		} finally {
			writer.close();
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
