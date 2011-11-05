package org.wikimedia.diffdb;

import java.io.File;
import java.io.Reader;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.IOException;
import java.util.Iterator;
import java.util.Date;
import java.text.SimpleDateFormat;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.IndexWriter;
import org.apache.commons.lang.StringEscapeUtils;

class Prop {
	// Note that fields which are not stored are not available in documents
	// retrieved from the index
	private final String name;
	private final Store store;
	private final Index index;
	protected Prop() {
		this("", Field.Store.NO, Field.Index.NOT_ANALYZED);
	}
	public Prop(String name, Store store, Index index) {
		this.name = name;
		this.store = store;
		this.index = index;
	}
	public String name() { return this.name; }
	public Store store() { return this.store; }
	public Index index() { return this.index; }
	
}

public class Indexer implements Runnable {
	private static final Prop[] propTypes = new Prop[] {
		new Prop("rev_id",    Field.Store.YES, Field.Index.NOT_ANALYZED),
		new Prop("page_id",   Field.Store.YES, Field.Index.NOT_ANALYZED),
		new Prop("namespace", Field.Store.YES, Field.Index.NOT_ANALYZED),
		new Prop("title",     Field.Store.YES, Field.Index.ANALYZED),
		new Prop("timestamp", Field.Store.YES, Field.Index.NOT_ANALYZED),
		new Prop("comment",     Field.Store.YES, Field.Index.ANALYZED),
		new Prop("minor",     Field.Store.YES, Field.Index.NOT_ANALYZED),
		new Prop("user_id",   Field.Store.YES, Field.Index.NOT_ANALYZED),
		new Prop("user_text", Field.Store.YES, Field.Index.ANALYZED),
	};
	private static final SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");

	private final File sourceFile;
	private final IndexWriter writer;

	public Indexer(IndexWriter writer, File f) {
		this.sourceFile = f;
		this.writer = writer;
	}

	synchronized public void close() throws IOException {
		// TODO Auto-generated method stub
	}

	public static boolean fileReadable(File f) {
		if (!f.isDirectory() && !f.isHidden() && f.exists() && f.canRead()
				&& acceptFile(f)) {
			return true;
		} else {
			return false;
		}
	}

	protected static boolean acceptFile(File f) {
		// TODO Auto-generated method stub
		if (f.getName().endsWith(".txt") || f.getName().endsWith(".tsv")) {
			return true;
		}
		try {
			int x = Integer.parseInt(f.getName());
		} catch (NumberFormatException e) {
			return false;
		}
		return true;
	}

	private static String[] parseDiff(String str) {
		int i = str.indexOf(":");
		int j = str.indexOf(":", i+1);
		if ( j >= str.length() - 3 ) {
			return new String[]{
				str.substring(0,i),
				str.substring(i+1,j),
				""
			};
		} else {
			return new String[]{
				str.substring(0,i),
				str.substring(i+1,j),
				str.substring(j+3, str.length() - 1)
			};
		}
	}

	private void createField(Document doc, Prop prop, String value) {
		doc.add(new Field(prop.name(), value, prop.store(), prop.index()));
	}
	private void setField(Document doc, Prop prop, String value) {
	}

	protected Iterable<Document> getDocument(Reader reader_) throws IOException {
		final Document doc = new Document();
		final BufferedReader reader = new BufferedReader(reader_);
		final String[] line = new String[] { null };
		final int[] linenumber = new int[] { 0 };

		// Initialise document
		for (int i = 0; i < propTypes.length; ++i) {
			createField(doc, propTypes[i], "");
		}
		createField(doc, new Prop("added_size",   Field.Store.YES, Field.Index.NOT_ANALYZED), "");
		createField(doc, new Prop("removed_size", Field.Store.YES, Field.Index.NOT_ANALYZED), "");
		createField(doc, new Prop("added",   Field.Store.YES, Field.Index.ANALYZED), "");
		createField(doc, new Prop("removed", Field.Store.YES, Field.Index.ANALYZED), "");

		final StringBuffer abuff = new StringBuffer("");
		final StringBuffer rbuff = new StringBuffer("");

		return new Iterable<Document>() {
			public Iterator<Document> iterator() {
				return new Iterator<Document>() {
					@Override
					public Document next() {
						String[] props = line[0].split("\t");
						for (int i = 0; i < propTypes.length; ++i) {
							Prop proptype = propTypes[i];
							Field field = (Field) doc.getFieldable(proptype
									.name());
							field.setValue(props[i]);
							// System.err.println(propTypes[i] + ": " +
							// props[i]);//!
						}
						// extract additions and removals and store them in the buffers
						abuff.delete(0, abuff.length());
						rbuff.delete(0, rbuff.length());
						for (int i = propTypes.length; i < props.length; ++i) {
							String[] p = parseDiff(props[i]);
							("-1".equals(p[1]) ? rbuff: abuff).append(p[0] + p[1] + StringEscapeUtils.unescapeJava(p[2]) + "\t");
						}
						((Field)doc.getFieldable("timestamp")).setValue(formatter.format(new Date(Long.parseLong(props[4])*1000)));
						((Field)doc.getFieldable("added")).setValue(abuff.toString());
						((Field)doc.getFieldable("removed")).setValue(rbuff.toString());
						((Field)doc.getFieldable("added_size")).setValue("" + abuff.length());
						((Field)doc.getFieldable("removed_size")).setValue("" + rbuff.length());
						line[0] = null;
						if (props.length < propTypes.length) {
							System.err.println("line " + linenumber[0]
									+ ": illegal line format");
						}
						return doc;
					}

					@Override
					public void remove() {
						throw new UnsupportedOperationException();
					}

					@Override
					public boolean hasNext() {
						if (line[0] != null) {
							return true;
						}
						try {
							if ((line[0] = reader.readLine()) != null) {
								++linenumber[0];
								return true;
							}
						} catch (IOException e) {
						}
						return false;
					}
				};
			}
		};
	}

	private void indexFile(File f) throws IOException {
		Reader reader = new FileReader(f);
		// System.out.println("" + Thread.currentThread()+ ": Indexing " +
		// f.getCanonicalPath());
		for (Document doc : getDocument(reader)) {
			writer.addDocument(doc);
		}
		reader.close();
	}

	@Override
	public void run() {
		// TODO Auto-generated method stub
		// long start = System.currentTimeMillis();
		// Indexer indexer;

		try {
			if (fileReadable(this.sourceFile)) {
				indexFile(this.sourceFile);
			} else {
				System.err.println("File " + this.sourceFile
						+ " is not readable.");
			}
			this.close();
		} catch (IOException e) {
			System.out.println(e);
		}

		// long end = System.currentTimeMillis();

		// System.out.println("Indexing " + numIndexed + " files took "
		// + (end - start) + " milliseconds");

	}

}
/*
 * Local variables:
 * tab-width: 2
 * c-basic-offset: 2
 * indent-tabs-mode: t
 * End:
 */
