package org.wikimedia.revdiffsearch;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.BitSet;

import org.apache.lucene.search.Collector;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.util.Version;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.search.Query;

public class DiffCollector extends Collector implements SearchResults {
	private static final Logger logger = Logger.getLogger(DiffCollector.class.getName());

  protected IndexSearcher searcher;
  protected final BitSet hits;
  private Map<String,Set<String>> queryFields;
  private int docBase;
  private int maxRevs;
  private int skipped;
  protected Query parsed;

  public DiffCollector(IndexSearcher searcher, int max) {
    this.searcher = searcher;
    this.hits = new BitSet(searcher.getIndexReader().maxDoc());
    this.maxRevs = max;
    this.skipped = 0;
  }
  public void setSearcher(IndexSearcher seacher) {
    this.searcher = searcher;
  }
  protected void prepare(String query, QueryParser parser) throws ParseException {
    this.queryFields = getQueryFields(query);
    this.parsed = parser.parse(query);
  }

  public void issue(String query, QueryParser parser) throws ParseException, IOException {
    this.prepare(query, parser);
    this.searcher.search(parsed, this);
  }
  protected static Map<String,Set<String>> getQueryFields(final String query) {
    final Map<String,Set<String>> ret = new HashMap<String,Set<String>>();
    final Map<String,Set<String>> rem = new HashMap<String,Set<String>>();
    try {
      QueryParser ps = new QueryParser(Version.LUCENE_44, "added", new CallbackAnalyzer(new CallbackAnalyzer.Callback() {
          public void execute(String field, String value) {
            assert value.length() > 0;

            Set<String> ls = ret.get(field);
            if ( ls == null ) {
              ls = new HashSet<String>();
              ret.put(field, ls);
            }
            ls.add(value);

            // remove substrings (TODO: this is inefficient)
            ls = rem.get(field);
            if ( ls == null ) {
              ls = new HashSet<String>();
              rem.put(field, ls);
            }
            for (int i = 0; i < value.length() - 1; ++i ) {
              for (int j = i + 1; j < value.length(); ++j ) {
                ls.add(value.substring(i, j));
              }
            }
          }
        }));
      ps.parse(query);
    } catch ( ParseException e ) {
      logger.severe(e.toString());
    }
    for ( Map.Entry<String,Set<String>> ent: ret.entrySet() ) {
      Set<String> r = rem.get(ent.getKey());
      if ( r != null ) {
        ent.getValue().removeAll(r);
      }
    }
    return ret;
  }
  
  public boolean acceptsDocsOutOfOrder() {
    return true;
  }
  
  public void setNextReader(AtomicReaderContext context) {
    this.docBase = context.docBase;
  }
  
  public void setScorer(Scorer scorer) {
  }
  
  public int getNumberOfSkippedDocuments() {
    return this.skipped;
  }

  public void collect(int doc) {
    doc += this.docBase;

    if ( this.hits.cardinality() >= this.maxRevs ) {
      ++this.skipped;
      this.hits.clear(doc);
      return;
    }
    try {
      this.hits.set(doc);
    } catch ( IllegalArgumentException e ) {
      String str = "";
      try {
        str = this.searcher.doc(doc).getFields().toString();
        logger.warning("unexpected set of fields: " + str);
      } catch (IOException ex) {
      }
      throw new RuntimeException(str, e);
    }
  }
  public BitSet getHits() {
			return this.hits;
  }
}

/*
 * Local variables:
 * tab-width: 2
 * c-basic-offset: 2
 * indent-tabs-mode: nil
 * End:
 */

