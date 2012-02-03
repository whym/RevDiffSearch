package org.wikimedia.revdiffsearch;
import org.junit.*;
import java.io.*;
import java.util.*;
import ie.ucd.murmur.MurmurHash;
import org.apache.lucene.analysis.tokenattributes.*;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.*;
import org.apache.lucene.store.*;
import org.apache.lucene.search.*;
import org.apache.lucene.document.*;
import org.apache.lucene.util.Version;
import org.apache.commons.codec.binary.Base64;

import static org.junit.Assert.*;

public class TestHashedNGramAnalyzer {
  private static int hash(String s, int seed) {
    return MurmurHash.hash32(s.getBytes(), s.getBytes().length, seed);
  }

  private static int decodeInteger(String enc) {
    int x = 0;
    int scale = 1;
    for ( byte b: Base64.decodeBase64(enc + "==") ) {
      x += scale * (0xFF & b);
      scale = scale << 8;
    }
    return x;
  }

  private static Document newDocument(String field, String value) {
    Document doc = new Document();
    doc.add(new Field(field, value, Field.Store.YES, Field.Index.ANALYZED));
    return doc;
  }

  @Test public void stringShorterThanMaxN() throws IOException {
    TokenStream ts = new HashedNGramAnalyzer(1,5,11).tokenStream("title", new StringReader("test"));
    OffsetAttribute offset = (OffsetAttribute) ts.addAttribute(OffsetAttribute.class);
    CharTermAttribute terma = (CharTermAttribute) ts.addAttribute(CharTermAttribute.class);
    NGramHashAttribute hasha = (NGramHashAttribute)ts.addAttribute(NGramHashAttribute.class);
    List<Integer> hashes = new ArrayList<Integer>();
    List<Integer> hashes2 = new ArrayList<Integer>();
    while (ts.incrementToken()) {
      hashes.add(hasha.getValue());
      hashes2.add(decodeInteger(terma.toString()));
    }
    assertEquals(Arrays.asList(new Integer[]{hash("t", 11), hash("te", 11), hash("tes", 11), hash("test", 11)}), hashes2);
  }

  @Test public void shortString() throws IOException {
    TokenStream ts = new HashedNGramAnalyzer(3,4,11).tokenStream("title", new StringReader("cadabra"));
    OffsetAttribute offset = (OffsetAttribute) ts.addAttribute(OffsetAttribute.class);
    CharTermAttribute terma = (CharTermAttribute) ts.addAttribute(CharTermAttribute.class);
    NGramHashAttribute hasha = (NGramHashAttribute)ts.addAttribute(NGramHashAttribute.class);
    List<Integer> hashes = new ArrayList<Integer>();
    List<Integer> hashes2 = new ArrayList<Integer>();
    while (ts.incrementToken()) {
      hashes.add(hasha.getValue());
      hashes2.add(decodeInteger(terma.toString()));
    }
    // assertEquals(Arrays.asList(new Integer[]{hash("cad", 11), hash("ada", 11), hash("dab", 11), hash("abr", 11), hash("bra", 11), hash("cada", 11), hash("adab", 11), hash("dabr", 11), hash("abra", 11)}), hashes); // TODO: NGramHashAttribute didn't work as index token in search; disabled until we find a better way to embed the hash value
    assertEquals(Arrays.asList(new Integer[]{hash("cad", 11), hash("cada", 11), hash("ada", 11), hash("adab", 11), hash("dab", 11), hash("dabr", 11), hash("abr", 11), hash("abra", 11), hash("bra", 11)}), hashes2);
  }

  @Test public void createDcumentAndExtractTerms() throws IOException, ParseException {
    Directory dir = new RAMDirectory();
    IndexWriter writer = new IndexWriter(dir,
                                         new IndexWriterConfig(Version.LUCENE_35,
                                                               new HashedNGramAnalyzer(3, 4, 1234)));
    writer.addDocument(newDocument("title", "help"));
    writer.commit();
    writer.optimize();
    writer.close();
    IndexReader reader = IndexReader.open(dir);
    assertEquals(1, reader.maxDoc());
    TermEnum terms = reader.terms();
    Set<Integer> observed = new HashSet<Integer>();
    while ( terms.next()) {
      observed.add(decodeInteger(terms.term().text()));
    }
    assertEquals(new HashSet<Integer>(Arrays.asList(new Integer[]{hash("hel", 1234), 
                                                                  hash("elp", 1234),
                                                                  hash("help", 1234),
          })), observed);
  }

  @Test public void createDcumentAndSearch() throws IOException, ParseException {
    Directory dir = new RAMDirectory();
    IndexWriter writer = new IndexWriter(dir,
                                         new IndexWriterConfig(Version.LUCENE_35,
                                                               new HashedNGramAnalyzer(3, 4, 1234)));
    writer.addDocument(newDocument("title", "help"));
    writer.commit();
    writer.optimize();
    writer.close();
    IndexReader reader = IndexReader.open(dir);
    IndexSearcher searcher = new IndexSearcher(reader);
    HashedNGramTokenizer tk = new HashedNGramTokenizer(null, 3, 4, 1234);
    Query query = new TermQuery(new Term("title", tk.encodeIntegerAsString(tk.hashString("hel"))));
    final int[] hit = new int []{-1};
    searcher.search(query, new Collector() {
        private int docBase;
        
        // ignore scorer
        public void setScorer(Scorer scorer) {
        }
        
        public boolean acceptsDocsOutOfOrder() {
          return true;
        }
        
        public void collect(int doc) {
          hit[0] = doc;
        }
        
        public void setNextReader(IndexReader reader, int docBase) {
          this.docBase = docBase;
        }
      });
    assertEquals(0, hit[0]);
  }

  @Test public void createDcumentAndSearchPhrase() throws IOException, ParseException {
    Directory dir = new RAMDirectory();
    IndexWriter writer = new IndexWriter(dir,
                                         new IndexWriterConfig(Version.LUCENE_35,
                                                               new HashedNGramAnalyzer(3, 5, 1234)));
    writer.addDocument(newDocument("title", "help page 1"));
    writer.addDocument(newDocument("title", "help page 2"));
    writer.addDocument(newDocument("title", "help page"));
    writer.addDocument(newDocument("title", "page 1"));
    writer.addDocument(newDocument("title", "help"));
    writer.commit();
    writer.optimize();
    writer.close();
    IndexReader reader = IndexReader.open(dir);
    IndexSearcher searcher = new IndexSearcher(reader);
    System.err.println("query");
    QueryParser parser = new QueryParser(Version.LUCENE_35, "title", new HashedNGramAnalyzer(3, 5, 1234));
    parser.setDefaultOperator(QueryParser.AND_OPERATOR);
    Query query = parser.parse("\"help page\"~2");
    final Set<Integer> hits = new HashSet<Integer>();
    searcher.search(query, new Collector() {
        private int docBase;
        
        // ignore scorer
        public void setScorer(Scorer scorer) {
        }
        
        public boolean acceptsDocsOutOfOrder() {
          return true;
        }
        
        public void collect(int doc) {
          hits.add(doc);
        }
        
        public void setNextReader(IndexReader reader, int docBase) {
          this.docBase = docBase;
        }
      });
    assertEquals(3, hits.size());
  }
}

/*
 * Local variables:
 * tab-width: 2
 * c-basic-offset: 2
 * indent-tabs-mode: nil
 * End:
 */

