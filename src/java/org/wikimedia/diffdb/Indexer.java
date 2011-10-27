package org.wikimedia.diffdb;

import java.io.File;
import java.io.Reader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.index.IndexWriter;

class Rev_id {
	public static final Store store = Field.Store.YES;
	public static final Index index = Field.Index.NOT_ANALYZED;
}

class Page_id {
	public static final Store store = Field.Store.YES;
	public static final Index index = Field.Index.NOT_ANALYZED; 
}

class Namespace {
	public static final Store store = Field.Store.YES;
	public static final Index index = Field.Index.ANALYZED;
}

class Title {
	public static final Store store = Field.Store.YES;
	public static final Index index = Field.Index.NOT_ANALYZED;
}

class Timestamp {
	public static final Store store = Field.Store.YES;
	public static final Index index = Field.Index.NOT_ANALYZED;
}

class Comment {
	public static final Store store = Field.Store.NO;
	public static final Index index = Field.Index.NOT_ANALYZED;
}

class Minor {
	public static final Store store = Field.Store.NO;
	public static final Index index = Field.Index.NOT_ANALYZED;
}

class User_id {
	public static final Store store = Field.Store.YES;
	public static final Index index = Field.Index.NOT_ANALYZED;
}

class User_text {
	public static final Store store = Field.Store.YES;
	public static final Index index = Field.Index.NOT_ANALYZED;
}

class Diff_position {
	public static final Store store = Field.Store.NO;
	public static final Index index = Field.Index.NOT_ANALYZED;
}
class Diff_action {
	public static final Store store = Field.Store.YES;
	public static final Index index = Field.Index.ANALYZED;
}

class Diff_content {
	public static final Store store = Field.Store.NO;
	public static final Index index = Field.Index.ANALYZED;
}
class KeyMap {
	public static HashMap<Integer, Object> map = new HashMap<Integer, Object>();
	public static final int length = 11;

	public KeyMap() {
		map.put(0, "Rev_id");
		map.put(1, "Page_id");
		map.put(2, "Namespace");
		map.put(3, "Title");
		map.put(4, "Timestamp");
		map.put(5, "Comment");
		map.put(6, "Minor");
		map.put(7, "User_id");
		map.put(8, "User_text");
		map.put(9, "Diff_position");
		map.put(10, "Diff_action");
		map.put(11, "Diff_content");
	}

}

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
		Document doc = new Document();
		BufferedReaderIterable in = new BufferedReaderIterable(f);
		for (Hashtable<Integer, String> read : in) {
		    // Do something with the line
			Iterator<Entry<Integer, String>> it = read.entrySet().iterator();
			int i = 0;
			
			while (it.hasNext()){
				Map.Entry<Integer, String> map = it.next();
				Integer key = map.getKey();
				String value = map.getValue();
				String classname  = (String) KeyMap.map.get(i);
				Class<?> Prop = Class.forName(classname);
				Object props = Prop.newInstance();
				//doc.add(new Field(key, value, props.store, props.index));
				i++;
			}
			//Reader reader = new FileReader(f);
			//doc.add(new Field("contents", reader));
			//doc.add(new Field("path", f.getCanonicalPath(), Field.Store.YES,
			//		Field.Index.NOT_ANALYZED));
			//return doc;
		}
		return doc;
	}

	protected void closeReader(Document doc) throws IOException {
		Fieldable field = doc.getFieldable("contents");
		final Reader reader = field.readerValue();
		reader.close();
	}

	private void indexFile(File f) throws Exception {
		//System.out.println("" + Thread.currentThread()+ ": Indexing " + f.getCanonicalPath());
		Document doc = getDocument(f);
		if (doc != null) {
			writer.addDocument(doc);
		}
		closeReader(doc);

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
		//long start = System.currentTimeMillis();
		// Indexer indexer;

		try {
			// Indexer indexer = new Indexer(Indexer.writer, this.sourceFile);
			numIndexed = this.index();
			this.close();
		} catch (IOException e) {
			System.out.println(e);
		} catch (Exception e) {
			System.out.println(e);
		}

		//long end = System.currentTimeMillis();

//		System.out.println("Indexing " + numIndexed + " files took "
//				+ (end - start) + " milliseconds");

	}

}
