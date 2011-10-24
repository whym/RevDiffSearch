package org.wikimedia.diffdb;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.util.Version;


public class Indexer implements Runnable {
	
	public static String indexDir = null;
	public static String dataDir = null;
	public static int numIndexed = 0;
	
	public static IndexWriter writer;
	public File sourceFile = null;
	
	public static void main(String[] args) throws Exception {
		indexDir = "//Users//diederik//Downloads";
		Directory dir =  new NIOFSDirectory(new File(indexDir), null);
		IndexWriterConfig cfg = new IndexWriterConfig(Version.LUCENE_34, new StandardAnalyzer(Version.LUCENE_34));
		//writer = new  IndexWriter(dir, new StandardAnalyzer(Version.LUCENE_34), true, IndexWriter.MaxFieldLength.UNLIMITED);
		writer = new IndexWriter(dir, cfg);
		File f = new File("//Users//diederik//Downloads//verzekeraar.csv.txt");
		Indexer indexer = new Indexer(writer, f);
		int numIndexed = indexer.index();
//		if (args.length!= 2) {
//			throw new Exception ("Usage: java " + Indexer.class.getName() + " <index dir> <data dir>");
//		}
//		indexDir = args[0];
//		dataDir = args[1];
		
//		long start = System.currentTimeMillis();
//		Indexer indexer = new Indexer(indexDir);
//		int numIndexed = indexer.index(dataDir);
//		indexer.close();
//		long end = System.currentTimeMillis();
//		
//		System.out.println("Indexing" + numIndexed + "files took " + (end-start) + " milliseconds");
	}
	
	
	public Indexer (String indexDir) throws IOException {
		Directory dir =  new NIOFSDirectory(new File(indexDir), null);
		IndexWriterConfig cfg = new IndexWriterConfig(Version.LUCENE_34, new StandardAnalyzer(Version.LUCENE_34));
		//writer = new  IndexWriter(dir, new StandardAnalyzer(Version.LUCENE_34), true, IndexWriter.MaxFieldLength.UNLIMITED);
		writer = new IndexWriter(dir, cfg);
	}
	
	public Indexer (IndexWriter writer, File f) {
		this.sourceFile =f;
		Indexer.writer = writer;
	}
	
	synchronized public void close() throws IOException {
		// TODO Auto-generated method stub
    if ( writer != null ) {
      writer.commit();
      writer.close();
      writer = null;
    }
	}

	
	public boolean fileReadable(File f) {
		if (!f.isDirectory() &&
				!f.isHidden() &&
				f.exists() &&
				f.canRead() &&
				acceptFile(f)) {
			return true;
		} else {
			return false;
		}
	}
	
	public int index(String dataDir) throws Exception {
		File[] files = new File(dataDir).listFiles();
		for (int i=0; i<files.length; i++) {
			File f = files[i];
			if (fileReadable(f)) {
				indexFile(f);
			}
//			if (!f.isDirectory() &&
//				!f.isHidden() &&
//				f.exists() &&
//				f.canRead() &&
//				acceptFile(f)) {
		}
			
		return writer.numDocs();
	}
	
	public int index() throws Exception {
		if (fileReadable(this.sourceFile)) {
			indexFile(this.sourceFile);
		}
		return writer.numDocs();
	}

	protected Document getDocument(File f) throws Exception{
		// TODO Auto-generated method stub
		Document doc = new Document();
		doc.add(new Field("contents", new FileReader(f)));
		doc.add(new Field("filename", f.getCanonicalPath(),
				Field.Store.YES, Field.Index.NOT_ANALYZED));
		return doc;
	}
	
	private void indexFile(File f) throws Exception {
		System.out.println("Indexing "+ f.getCanonicalPath());
		Document doc = getDocument(f);
		if (doc!=null){
			writer.addDocument(doc);
		}
	}

	protected boolean acceptFile(File f) {
		// TODO Auto-generated method stub
		return f.getName().endsWith(".txt");
	}


	@Override
	public void run() {
		// TODO Auto-generated method stub
		long start = System.currentTimeMillis();
		//Indexer indexer;
		
		try {
			//Indexer indexer = new Indexer(Indexer.writer, this.sourceFile);
			numIndexed += this.index();
			this.close();
		} catch(IOException e) {
			System.out.println(e);
		} catch(Exception e) {
			System.out.println(e); 
		}
		
		long end = System.currentTimeMillis();
		
		System.out.println("Indexing " + numIndexed + " files took " + (end-start) + " milliseconds");
		
	}
	
}
	
