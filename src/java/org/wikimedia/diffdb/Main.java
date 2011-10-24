package org.wikimedia.diffdb;

import java.io.File;
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
	public static String indexDir = null;
	public static String dataDir = null;

	private static void indexDocuments(ExecutorService executor,
			IndexWriter writer, File file) {
		if (file.canRead()) {
			if (file.isDirectory()) {
				for (File f : file.listFiles()) {
					indexDocuments(executor, writer, f);
				}
			} else {
				Runnable worker = new Indexer(writer, file);
				executor.execute(worker);
			}
		}
	}

	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			throw new Exception("Usage: java " + Main.class.getName()
					+ " <index dir> <data dir>");
		}
		indexDir = args[0];
		dataDir = args[1];
		double ramBufferSizeMB = 48;

		ExecutorService executor = Executors.newFixedThreadPool(NTHREDS);
		Directory dir = new NIOFSDirectory(new File(indexDir), null);
		LogDocMergePolicy lmp = new LogDocMergePolicy();
		lmp.setUseCompoundFile(true); // This might fix the too many open files,
		// see http://wiki.apache.org/lucene-java/LuceneFAQ#Why_am_I_getting_an_IOException_that_says_.22Too_many_open_files.22.3F
		
		IndexWriterConfig cfg = new IndexWriterConfig(Version.LUCENE_34,
				new SimpleNGramAnalyzer(3));
		
		
		cfg.setOpenMode(OpenMode.CREATE_OR_APPEND); //http://lucene.apache.org/java/3_2_0/api/core/org/apache/lucene/index/IndexWriterConfig.OpenMode.html#CREATE_OR_APPEND
		cfg.setRAMBufferSizeMB(ramBufferSizeMB);
		cfg.setMergePolicy(lmp);

		IndexWriter writer = new IndexWriter(dir, cfg);
		
		indexDocuments(executor, writer, new File(dataDir));

		// This will make the executor accept no new threads
		// and finish all existing threads in the queue
		executor.shutdown();

		// Wait until all threads are finish
		while (!executor.isTerminated()) {

		}
		System.out.println("Finished all threads");
		writer.commit();
		System.out.println("Writing " + writer.numDocs() + " documents.");
		writer.close();
	}

}