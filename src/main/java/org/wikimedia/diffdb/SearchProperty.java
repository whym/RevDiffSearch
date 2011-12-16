package org.wikimedia.diffdb;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;

public class SearchProperty {

	public static final Pattern STRING_PATTERN = Pattern.compile(":[a-z0-9]*");
	public static final Pattern TIMESTAMP_PATTERN = Pattern
			.compile("timestamp:\\[[0-9\\-\\sTO\\s]*\\]");
	public static final Pattern DIGIT_PATTERN = Pattern.compile(":[0-9]*\\s");
  private static final SearchProperty singleton;
  private static Property defaultProp = new Property("__default__", Field.Store.NO,
                                                     Field.Index.NOT_ANALYZED, STRING_PATTERN);
  static {
    singleton = new SearchProperty();
  }

  public static SearchProperty getInstance() {
    return singleton;
  }

	public Map<String, Property> propTypes = null;

	// Note that fields which are not stored are not available in documents
	// retrieved from the index
	public static class Property {

		private final String name;
		private final Store store;
		private final Index index;
		private final Pattern pattern;

		public Property(String name, Store store, Index index, Pattern pattern) {
			this.name = name;
			this.store = store;
			this.index = index;
			this.pattern = pattern;
		}

		public String name() {
			return this.name;
		}

		public Store store() {
			return this.store;
		}

		public Index index() {
			return this.index;
		}

		public Pattern pattern() {
			return this.pattern;
		}

		public boolean isStored() {
			boolean result;
			if (this.store.equals(Field.Store.YES)) {
				result = true;
			} else {
				result = false;
			}
			return result;
		}

		public boolean isAnalyzed() {
			boolean result;
			if (this.index.equals(Field.Index.ANALYZED)) {
				result = true;
			} else {
				result = false;
			}
			return result;
		}
	}

	public Map<String, Property> getPropertyTypes() {
    return this.propTypes;
  }

  private SearchProperty(){
		propTypes = new HashMap<String, Property>();
		// private static final HashMap<String, SearchProperty> propTypes = new
		// HashMap<String, SearchProperty>();
		
		propTypes.put("rev_id", new Property("rev_id", Field.Store.YES,
				Field.Index.NOT_ANALYZED, DIGIT_PATTERN));
		propTypes.put("page_id", new Property("page_id", Field.Store.YES,
				Field.Index.NOT_ANALYZED, DIGIT_PATTERN));
		propTypes.put("namespace", new Property("namespace", Field.Store.YES,
				Field.Index.NOT_ANALYZED, DIGIT_PATTERN));
		propTypes.put("title", new Property("title", Field.Store.YES,
				Field.Index.ANALYZED, STRING_PATTERN));
		propTypes.put("timestmap", new Property("timestamp", Field.Store.YES,
				Field.Index.NOT_ANALYZED, TIMESTAMP_PATTERN));
		propTypes.put("comment", new Property("comment", Field.Store.YES,
				Field.Index.ANALYZED, STRING_PATTERN));
		propTypes.put("minor", new Property("minor", Field.Store.YES,
				Field.Index.NOT_ANALYZED, STRING_PATTERN));
		propTypes.put("user_id", new Property("user_id", Field.Store.YES,
				Field.Index.NOT_ANALYZED, DIGIT_PATTERN));
		propTypes.put("user_text", new Property("user_text", Field.Store.YES,
				Field.Index.ANALYZED, STRING_PATTERN));
		propTypes.put("added_size", new Property("added_size", Field.Store.YES,
				Field.Index.NOT_ANALYZED, DIGIT_PATTERN));
		propTypes.put("removed_size", new Property("removed_size",
				Field.Store.YES, Field.Index.NOT_ANALYZED, DIGIT_PATTERN));
		propTypes.put("added", new Property("added", Field.Store.YES,
				Field.Index.ANALYZED, STRING_PATTERN));
		propTypes.put("removed", new Property("removed", Field.Store.YES,
				Field.Index.ANALYZED, STRING_PATTERN));
		propTypes.put("action", new Property("action", Field.Store.YES,
				Field.Index.NOT_ANALYZED, DIGIT_PATTERN));
		//propTypes.put("__default__", );
	}

	public Property getProperty(String name) {
		Property p = this.propTypes.get(name);
    if ( p == null ) {
      p = defaultProp;
    }
    return p;
	}

	public Map<String, Property> getProptypes() {
		return this.propTypes;
	}

}