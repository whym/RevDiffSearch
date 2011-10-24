package org.wikimedia.diffdb;

import java.io.File;
import java.io.FileReader;
import java.io.Reader;
import java.io.IOException;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexWriter;

public class Indexer implements Runnable {
	public static int numIndexed = 0;

	public static IndexWriter writer;
	public File sourceFile = null;

	public Indexer(IndexWriter writer, File f) {
		this.sourceFile = f;
		Indexer.writer = writer;
	}

	synchronized public void close() throws IOException {
		// TODO Auto-generated method stub
	}

	public boolean fileReadable(File f) {
		if (!f.isDirectory() && !f.isHidden() && f.exists() && f.canRead()
				&& acceptFile(f)) {
			return true;
		} else {
			return false;
		}
	}

	public int index() throws Exception {
		if (fileReadable(this.sourceFile)) {
			indexFile(this.sourceFile);
		} else {
			System.err.println("File " + this.sourceFile + " is not readable.");
		}
		return writer.numDocs();
	}

	protected Document getDocument(File f) throws Exception {
		// TODO Auto-generated method stub
		Document doc = new Document();
		Reader reader = new FileReader(f);
		doc.add(new Field("contents", reader));
		doc.add(new Field("path", f.getCanonicalPath(), Field.Store.YES,
				Field.Index.NOT_ANALYZED));
		return doc;
	}

	private void indexFile(File f) throws Exception {
		System.out.println("Indexing " + f.getCanonicalPath());
		Document doc = getDocument(f);
		if (doc != null) {
			writer.addDocument(doc);
		}
	}

	protected boolean acceptFile(File f) {
		// TODO Auto-generated method stub
		if (f.getName().endsWith(".txt")) {
			return true;
		}
		try {
			int x = Integer.parseInt(f.getName());
		} catch (NumberFormatException e) {
			return false;
		}
		return true;
	}

	@Override
	public void run() {
		// TODO Auto-generated method stub
		long start = System.currentTimeMillis();
		// Indexer indexer;

		try {
			// Indexer indexer = new Indexer(Indexer.writer, this.sourceFile);
			numIndexed += this.index();
			this.close();
		} catch (IOException e) {
			System.out.println(e);
		} catch (Exception e) {
			System.out.println(e);
		}

		long end = System.currentTimeMillis();

		System.out.println("Indexing " + numIndexed + " files took "
				+ (end - start) + " milliseconds");

	}

}
