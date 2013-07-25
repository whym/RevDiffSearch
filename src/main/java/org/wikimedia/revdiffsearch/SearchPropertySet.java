package org.wikimedia.revdiffsearch;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.document.Field.Store;

public class SearchPropertySet {

	public static final Pattern STRING_PATTERN = Pattern.compile(":[a-z0-9]*");
	public static final Pattern TIMESTAMP_PATTERN = Pattern
			.compile("timestamp:\\[[0-9\\-\\sTO\\s]*\\]");
	public static final Pattern DIGIT_PATTERN = Pattern.compile(":[0-9]*\\s");
  private static final SearchPropertySet singleton;
  private static final FieldType defaultType = StringField.TYPE_NOT_STORED;
  static {
    singleton = new SearchPropertySet();
  }

  public static SearchPropertySet getInstance() {
    return singleton;
  }

	public Map<String, Property> propTypes = null;

	// Note that fields which are not stored are not available in documents
	// retrieved from the index
	public static class Property {

		private final String name;
		private final FieldType type;
		private final Pattern pattern;

		public Property(String name, FieldType type, Pattern pattern) {
			this.name = name;
			this.type = type;
			this.pattern = pattern;
		}

		public String name() {
			return this.name;
		}

		public FieldType type() {
			return this.type;
		}

		public Pattern pattern() {
			return this.pattern;
		}

		public boolean isStored() {
			return this.type.stored();
		}

		public boolean isAnalyzed() {
      return this.type.tokenized();
		}

    @Override public String toString() {
      return String.format("(name=%s, type=%s, pattern=%s)", this.name, this.type, this.pattern);
    }
	}

	public Map<String, Property> getPropertyTypes() {
    return this.propTypes;
  }

  private SearchPropertySet(){
		propTypes = new HashMap<String, Property>();
		// private static final HashMap<String, Property> propTypes = new
		// HashMap<String, Property>();
		
		propTypes.put("rev_id",
                  new Property("rev_id",    StringField.TYPE_STORED, DIGIT_PATTERN));
		propTypes.put("page_id",
                  new Property("page_id",   StringField.TYPE_STORED, DIGIT_PATTERN));
		propTypes.put("namespace",
                  new Property("namespace", StringField.TYPE_STORED, DIGIT_PATTERN));
		propTypes.put("action",
                  new Property("action",    StringField.TYPE_STORED, DIGIT_PATTERN));
		propTypes.put("timestamp",
                  new Property("timestamp", StringField.TYPE_STORED, TIMESTAMP_PATTERN));
		propTypes.put("minor",
                  new Property("minor",     StringField.TYPE_STORED, STRING_PATTERN));
		propTypes.put("user_id",
                  new Property("user_id",   StringField.TYPE_STORED, DIGIT_PATTERN));
		propTypes.put("added_size",
                  new Property("added_size", StringField.TYPE_STORED, DIGIT_PATTERN));
		propTypes.put("removed_size",
                  new Property("removed_size", StringField.TYPE_STORED, DIGIT_PATTERN));
		propTypes.put("comment",
                  new Property("comment",   TextField.TYPE_STORED, STRING_PATTERN));
		propTypes.put("user_text",
                  new Property("user_text", TextField.TYPE_STORED, STRING_PATTERN));
		propTypes.put("title",
                  new Property("title",     TextField.TYPE_STORED, STRING_PATTERN));
		propTypes.put("added",
                  new Property("added",     TextField.TYPE_NOT_STORED, STRING_PATTERN));
		propTypes.put("removed",
                  new Property("removed",   TextField.TYPE_NOT_STORED, STRING_PATTERN));
		//propTypes.put("__default__", );
	}

	public Property getProperty(String name) {
		return this.propTypes.get(name);
	}

	public Property getPropertyOrDefault(String name) {
		Property p = this.propTypes.get(name);
    if ( p == null ) {
      p = new Property(name, defaultType, STRING_PATTERN);
    }
    return p;
	}

	public Map<String, Property> getProperties() {
		return new HashMap<String, Property>(this.propTypes);
	}

}