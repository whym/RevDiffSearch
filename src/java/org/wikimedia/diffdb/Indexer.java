package org.wikimedia.diffdb;

import java.io.File;
import java.io.Reader;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.IOException;
import java.util.Iterator;
import java.util.Arrays;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.index.IndexWriter;

interface Prop {
	//Note that fields which are not stored are not available in documents retrieved from the index
  String name();
	Store store();
	Index index();
}

class Rev_id implements Prop {
  public String name() { return "rev_id"; }
	public Store store() {return Field.Store.YES;}
	public Index index() {return Field.Index.NOT_ANALYZED;}
}

class Page_id implements Prop {
  public String name() { return "page_id"; }
	public Store store() {return Field.Store.YES;}
	public Index index() {return Field.Index.NOT_ANALYZED;}
}

class Namespace implements Prop {
  public String name() { return "namespace"; }
	public Store store() {return Field.Store.YES;}
	public Index index() {return Field.Index.ANALYZED;}
}

class Title implements Prop {
  public String name() { return "title"; }
	public Store store() {return Field.Store.YES;}
	public Index index() {return Field.Index.ANALYZED;}
}

class Timestamp implements Prop {
  public String name() { return "timestamp"; }
	public Store store() {return Field.Store.YES;}
	public Index index() {return Field.Index.NOT_ANALYZED;}
}

class Comment implements Prop {
  public String name() { return "comment"; }
	public Store store() {return Field.Store.NO;}
	public Index index() {return Field.Index.ANALYZED;}
}

class Minor implements Prop {
  public String name() { return "minor"; }
	public Store store() {return Field.Store.NO;}
	public Index index() {return Field.Index.NOT_ANALYZED;}
}

class User_id implements Prop {
  public String name() { return "user_id"; }
	public Store store() {return Field.Store.YES;}
	public Index index() {return Field.Index.NOT_ANALYZED;}
}

class User_text implements Prop {
  public String name() { return "user_text"; }
	public Store store() {return Field.Store.YES;}
	public Index index() {return Field.Index.ANALYZED;}
}

class Diff implements Prop {
  public String name() { return "diff"; }
	public Store store() {return Field.Store.NO;}
	public Index index() {return Field.Index.ANALYZED;}
}

class Added implements Prop {
	public String name() {return "added";}
	public Store store() {return Field.Store.YES;}
	public Index index() {return Field.Index.NOT_ANALYZED;}
}

class Removed implements Prop {
	public String name() {return "removed";}
	public Store store() {return Field.Store.YES;}
	public Index index() {return Field.Index.NOT_ANALYZED;}
}


public class Indexer implements Runnable {
	public static IndexWriter writer;
  private static Prop[] propTypes = new Prop[]{
    new Rev_id(),
    new Page_id(),
    new Namespace(),
    new Title(),
    new Timestamp(),
    new Comment(),
    new Minor(),
    new User_id(),
    new User_text(),
    new Added(),
    new Removed(),
  };

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

	protected Iterable<Document> getDocument(Reader reader_) throws IOException {
		final Document doc = new Document();
    final BufferedReader reader = new BufferedReader(reader_);
    final String[] line = new String[]{null};
    final int[] linenumber = new int[]{0};

    // initialize document
    for ( int i = 0; i < propTypes.length; ++i ) {
      Prop proptype = propTypes[i];
      doc.add(new Field(proptype.name(), "", proptype.store(), proptype.index()));
    }
    final Prop diff = new Diff();
    doc.add(new Field(diff.name(), "", diff.store(), diff.index()));

    final StringBuffer buff = new StringBuffer("");
    
    return new Iterable<Document>() {
      public Iterator<Document> iterator() {
        return new Iterator<Document>() {
          @Override
            public Document next() {
            String[] props = line[0].split("\t");
            for ( int i = 0; i < propTypes.length; ++i ) {
              Prop proptype = propTypes[i];
              Field field = (Field)doc.getFieldable(proptype.name());
              field.setValue(props[i]);
              //System.err.println(propTypes[i] + ": " + props[i]);//!
            }
            buff.delete(0, buff.length());
            for ( int i = propTypes.length; i < props.length; ++i ) {
              buff.append(props[i] + "\n");
            }
            Field field = (Field)doc.getFieldable(diff.name());
            field.setValue(buff.toString());
            line[0] = null;
            if ( props.length < propTypes.length ) {
              System.err.println("line " + linenumber[0] + ": illegal line format");
            }
            return doc;
          }
          
          @Override
            public void remove() {
            throw new UnsupportedOperationException();
          }
          
          @Override
            public boolean hasNext() {
            if ( line[0] != null ) {
              return true;
            }
            try {
              if ( (line[0] = reader.readLine()) != null ) {
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
		for ( Document doc: getDocument(reader) ) {
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
        System.err.println("File " + this.sourceFile + " is not readable.");
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
