package org.wikimedia.diffdb;

import java.io.File;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.util.Version;

public class Main {
	private static final int NTHREDS = 10;
	public static String indexDir = null;
	public static String dataDir = null;
	
	public static void main(String[] args) throws Exception {
		if (args.length!= 2) {
			throw new Exception ("Usage: java " + Main.class.getName() + " <index dir> <data dir>");
		}
		indexDir = args[0];
		dataDir = args[1];

		ExecutorService executor = Executors.newFixedThreadPool(NTHREDS);
		Directory dir =  new NIOFSDirectory(new File(indexDir), null);
		IndexWriterConfig cfg = new IndexWriterConfig(Version.LUCENE_34, new StandardAnalyzer(Version.LUCENE_34));
		//writer = new  IndexWriter(dir, new StandardAnalyzer(Version.LUCENE_34), true, IndexWriter.MaxFieldLength.UNLIMITED);
		IndexWriter writer = new IndexWriter(dir, cfg);		

		for (File f: new File(dataDir).listFiles()) {
			Runnable worker = new Indexer(writer, f);
			executor.execute(worker);
			}
			// This will make the executor accept no new threads
			// and finish all existing threads in the queue
			executor.shutdown();
		
		// Wait until all threads are finish
		while (!executor.isTerminated()) {

		}
		System.out.println("Finished all threads");
	}
	
	
}