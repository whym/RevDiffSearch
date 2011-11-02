package org.wikimedia.diffdb;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.LogDocMergePolicy;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.util.Version;

public class Main {
	private static final int NTHREDS = 10;
	private static final long REPORT_DURATION_MSECS = 10000L;
	public static String indexDir = null;
	public static String dataDir = null;

	private static int indexDocumentsRec(ExecutorService executor,
			IndexWriter writer, File file, int numFiles) {
		if (file.canRead()) {
			if (file.isDirectory()) {
				for (File f : file.listFiles()) {
					numFiles = indexDocumentsRec(executor, writer, f, numFiles);
				}
			} else {
				Runnable worker = new Indexer(writer, file);
				executor.execute(worker);
				++numFiles;
			}
		}
		return numFiles;
	}

	private static int indexDocuments(ExecutorService executor,
			IndexWriter writer, File file) {
		return indexDocumentsRec(executor, writer, file, 0);
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
		try {
			// run a thread that reports the progress periodically
			new Thread(new Runnable() {
				public void run() {
					try {
						while (!executor.isTerminated()) {
							System.err.println("" + writer.numDocs()
									+ " documents have been indexed in "
									+ (System.currentTimeMillis() - start)
									+ " msecs");
							Thread.sleep(REPORT_DURATION_MSECS);
						}
					} catch (IOException e) {
						e.printStackTrace();
					} catch (InterruptedException e) {
						System.err.println("Interrupted");
					}
				}
			}).start();

			int numFiles = indexDocuments(executor, writer, new File(dataDir));
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
			System.err.println("Finished in "
					+ (System.currentTimeMillis() - start) + " msecs");
			writer.close();
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
