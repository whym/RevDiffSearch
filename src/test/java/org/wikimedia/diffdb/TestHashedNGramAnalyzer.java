package org.wikimedia.diffdb;
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

import static org.junit.Assert.*;

public class TestHashedNGramAnalyzer {
  private static int hash(String s, int seed) {
    return MurmurHash.hash32(s.getBytes(), s.getBytes().length, seed);
  }
  @Test public void shortString() throws IOException {
    TokenStream ts = new HashedNGramAnalyzer(3,4,11).tokenStream("default", new StringReader("cadabra"));
    OffsetAttribute offset = (OffsetAttribute) ts.addAttribute(OffsetAttribute.class);
    CharTermAttribute terma = (CharTermAttribute) ts.addAttribute(CharTermAttribute.class);
    NGramHashAttribute hasha = (NGramHashAttribute)ts.addAttribute(NGramHashAttribute.class);
    List<Integer> hashes = new ArrayList<Integer>();
    while (ts.incrementToken()) {
      hashes.add(hasha.getValue());
    }
    // assertEquals(Arrays.asList(new Integer[]{hash("cad", 11), hash("ada", 11), hash("dab", 11), hash("abr", 11), hash("bra", 11), hash("cada", 11), hash("adab", 11), hash("dabr", 11), hash("abra", 11)}), hashes); // TODO: NGramHashAttribute didn't work as index token in search; disabled until we find a better way to embed the hash value
  }

  @Test public void createDcument() throws IOException, ParseException {
    Directory dir = new RAMDirectory();
    IndexWriter writer = new IndexWriter(dir,
                                         new IndexWriterConfig(Version.LUCENE_35,
                                                               new HashedNGramAnalyzer(3, 4, 1234)));
    Document doc = new Document();
    doc.add(new Field("f1", "help", Field.Store.YES, Field.Index.ANALYZED));
    writer.addDocument(doc);
    writer.commit();
    writer.optimize();
    writer.close();
    IndexReader reader = IndexReader.open(dir);
    IndexSearcher searcher = new IndexSearcher(reader);
    HashedNGramTokenizer tk = new HashedNGramTokenizer(null, 3, 4, 1234);
    Query query = new TermQuery(new Term("f1", tk.encodeIntegerAsString(tk.hashString("hel"))));
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
  // @Test public void search() throws IOException, ParseException {
  //   Query parser = new QueryParser(Version.LUCENE_35, "added", new HashedNGramAnalyzer(3, 3, 11));
  //   assertEquals("test", parser.parse("help"));
  // }
}

/*
 * Local variables:
 * tab-width: 2
 * c-basic-offset: 2
 * indent-tabs-mode: nil
 * End:
 */

