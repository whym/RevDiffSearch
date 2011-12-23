package org.wikimedia.diffdb;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.TreeMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.BitSet;
import java.util.Date;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

import org.apache.lucene.search.Collector;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.search.TopDocsCollector;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.util.Version;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.KeywordTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

public class DiffCollector extends Collector {
	private static final Logger logger = Logger.getLogger(DiffCollector.class.getName());

  private final IndexSearcher searcher;
  private final BitSet hits;
  private final Map<String,String> queryFields;
  private int docBase;
  private int maxRevs;
  private int positives;
  private int skipped;
  
  public DiffCollector(IndexSearcher searcher, String query, int max) {
    this.searcher = searcher;
    this.hits = new BitSet(searcher.getIndexReader().maxDoc());
    this.maxRevs = max;
    this.positives = 0;
    this.skipped = 0;
    this.queryFields = getQueryFields(query);
  }
  public static Map<String,String> getQueryFields(final String query) {
    final Map<String,String> map = new TreeMap<String,String>();
    try {
      QueryParser ps = new QueryParser(Version.LUCENE_35, "added", new Analyzer() {
          public final TokenStream tokenStream(final String field, final Reader reader) {
            final TokenStream ts = new KeywordTokenizer(reader);
            return new TokenStream() {
              private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
              public boolean incrementToken() throws IOException {
                if ( ts.incrementToken() ) {
                  String s = termAtt.toString();
                  map.put(field, s);
                  System.err.println("got: " + field + " " + s + " in " + query);//!
                  return true;
                } else {
                  return false;
                }
              }
            };
          }
          public final TokenStream reusableTokenStream(String field, final Reader reader) {
            return tokenStream(field, reader);
          }
        });
      ps.parse(query);
    } catch ( ParseException e ) {
      logger.severe(e.toString());
    }
    return map;
  }
  
  public boolean acceptsDocsOutOfOrder() {
    return true;
  }
  
  public void setNextReader(IndexReader reader, int docBase) {
    this.docBase = docBase;
  }
  
  public void setScorer(Scorer scorer) {
  }
  
  public int positives() {
    return this.positives;
  }
  
  public int skipped() {
    return this.skipped;
  }
  
  public void collect(int doc) {
    doc += this.docBase;
    ++this.positives;
    // TODO: it must work for other fileds than 'added' and 'removed'
    if ( this.hits.cardinality() >= this.maxRevs ) {
      ++this.skipped;
      this.hits.clear(doc);
      return;
    }					
    try {
      for ( Map.Entry<String,String> ent: this.queryFields.entrySet() ) {
        if ( searcher.doc(doc).getField(ent.getKey()).stringValue().indexOf(ent.getValue()) < 0 ) {
          this.hits.clear(doc);
          return;
        }
      }
      this.hits.set(doc);
    } catch (IOException e) {
      logger.severe("failed to read " + doc);
    } catch ( IllegalArgumentException e ) {
      String str = "";
      try {
        str = searcher.doc(doc).getFields().toString();
        System.err.println(str);//!
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

