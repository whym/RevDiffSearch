package org.wikimedia.revdiffsearch;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Set;
import java.util.TimeZone;
import java.util.logging.Logger;
import java.util.concurrent.BlockingQueue;

import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.document.Field.Store;

public class DiffDocumentProducer implements Runnable {
	private static final Logger logger = Logger.getLogger(DiffDocumentProducer.class.getName());
	private static final String[] propTypes = new String[] {
		("rev_id"),
		("page_id"),
		("namespace"),
		("title"),
		("timestamp"),
		("comment"),
		("minor"),
		("user_id"),
		("user_text"),
	};

	public static enum Filter {
		PASS_ALL {
			@Override
				public boolean pass(Document doc) {
				return true;
			}
		}, PASS_TALK_NAMESPACE_ONLY {
			@Override
				public boolean pass(Document doc) {
				return Integer.parseInt(doc.getField("namespace").stringValue()) % 2 == 1;
			}
		}, PASS_USER_TALK_NAMESPACE_ONLY {
			@Override
				public boolean pass(Document doc) {
				return Integer.parseInt(doc.getField("namespace").stringValue()) == 3;
			}
		};
		public abstract boolean pass(Document doc);
	};

  private final BlockingQueue<Document> prodq;
  private final BlockingQueue<Document> poolq;
  private final BufferedReader reader;
	private final SimpleDateFormat formatter;
	private final Filter filter;

  public DiffDocumentProducer(Reader reader, BlockingQueue<Document> prodq, BlockingQueue<Document> poolq, Filter filter) {
    this.prodq = prodq;
    this.poolq = poolq;
    this.reader = new BufferedReader(reader);
		this.filter = filter;
		this.formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
		this.formatter.setTimeZone(TimeZone.getTimeZone("UTC"));
  }
  public DiffDocumentProducer(Reader reader, BlockingQueue<Document> prodq, BlockingQueue<Document> poolq) {
		this(reader, prodq, poolq, Filter.PASS_ALL);
	}

	public int hashCode() {
		return this.reader.hashCode();
	}

	public boolean equals(Object o) {
		if ( o instanceof DiffDocumentProducer ) {
			DiffDocumentProducer p = (DiffDocumentProducer)o;
			return this.reader.equals(p.reader);
		}
		return false;
	}

  public static Document createEmptyDocument() {
		SearchPropertySet spset = SearchPropertySet.getInstance();
    final Document doc = new Document();
    // Initialise document
    for (int i = 0; i < propTypes.length; ++i) {
      createField(doc, spset.getProperty(propTypes[i]), "");
    }
    createField(doc, spset.getProperty("added_size"), "");
    createField(doc, spset.getProperty("removed_size"), "");
    createField(doc, spset.getProperty("added"), "");
    createField(doc, spset.getProperty("removed"), "");
    createField(doc, spset.getProperty("action"), "");
    return doc;
  }

	private static void createField(Document doc, SearchPropertySet.Property prop, String value) {
		doc.add(new Field(prop.name(), value, prop.type()));
	}

  public void run() {
		SearchPropertySet spset = SearchPropertySet.getInstance();
    String line;
    int linenumber = 1;
		final StringBuffer abuff = new StringBuffer("");
		final StringBuffer rbuff = new StringBuffer("");
    try {
      while ( (line = this.reader.readLine()) != null ) {
        Document doc = this.poolq.take();
        String[] props = line.split("\t");
        for (int i = 0; i < propTypes.length; ++i) {
          SearchPropertySet.Property proptype = spset.getProperty(propTypes[i]);
          Field field = (Field)doc.getField(proptype.name());
          field.setStringValue(parseString(props[i]));
        }
        // extract additions and removals and store them in the buffers
        abuff.delete(0, abuff.length());
        rbuff.delete(0, rbuff.length());
        for (int i = propTypes.length; i < props.length; ++i) {
          String[] p;
          try {
            p = parseDiff(props[i]);
          } catch (Exception e) {
            logger.warning(e + ": " + props[0] + ", " + props[i]);
						e.printStackTrace();
            continue;
          }
          ("-1".equals(p[1]) ? rbuff: abuff).append(p[0] + p[1] +p[2] + "\t");
        }
        ((Field)doc.getField("timestamp")).setStringValue(formatter.format(new Date(Long.parseLong(props[4])*1000L)));
        ((Field)doc.getField("added")).setStringValue(abuff.toString());
        ((Field)doc.getField("removed")).setStringValue(rbuff.toString());
        ((Field)doc.getField("added_size")).setStringValue("" + abuff.length());
        ((Field)doc.getField("removed_size")).setStringValue("" + rbuff.length());

				String action = "none";
				if ( abuff.length() > 0  &&  rbuff.length() > 0 ) {
					action = "both";
				} else if ( abuff.length() > 0 ) {
					action = "addition";
				} else {
					action = "removal";
				}
					
        ((Field)doc.getField("action")).setStringValue(action);

        if (props.length < propTypes.length) {
          logger.warning("line " + linenumber
												 + ": illegal line format");
        }

        ++linenumber;
				if ( this.filter.pass(doc) ) {
					this.prodq.put(doc);
				} else {
					this.poolq.put(doc);
				}
      }
		} catch ( InterruptedException e ) {
			System.out.println(e);
		} catch ( IOException e ) {
			System.out.println(e);
		} finally {
    }
  }

	public static String parseString(String str) {
		if ( str.charAt(0) == 'u' && str.charAt(1) == '\'' ) {
			assert str.length() >= 4;
			return str.substring(2, str.length() - 1);
		} else if ( str.charAt(0) == '\'' ) {
			assert str.length() >= 3;
			return str.substring(1, str.length() - 1);
		} else {
			return str;
		}
	}

	private static String[] parseDiff(String str) {
		int i = str.indexOf(":");
		assert i > 0  : "the first colon not found in '" + str + "'";
		int j = str.indexOf(":", i+1);
		assert j > 0  : "the second colon not found in '" + str + "'";
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
				StringEscapeUtils.unescapeJava(parseString(str.substring(j+1)))
			};
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
