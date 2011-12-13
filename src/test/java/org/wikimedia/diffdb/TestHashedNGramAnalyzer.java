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
import org.apache.lucene.util.Version;

import static org.junit.Assert.*;

public class TestHashedNGramAnalyzer {
  private static int hash(String s, int seed) {
    return MurmurHash.hash32(s.getBytes(), s.getBytes().length, seed);
  }
  @Test public void shortString() throws IOException {
    TokenStream ts = new HashedNGramAnalyzer(3,4,11).tokenStream("default", new StringReader("cadabra"));
    OffsetAttribute offset = (OffsetAttribute) ts.addAttribute(OffsetAttribute.class);
    NGramHashAttribute hasha = (NGramHashAttribute)ts.addAttribute(NGramHashAttribute.class);
    List<Integer> hashes = new ArrayList<Integer>();
    while (ts.incrementToken()) {
      hashes.add(hasha.getValue());
    }
    assertEquals(Arrays.asList(new Integer[]{hash("cad", 11), hash("ada", 11), hash("dab", 11), hash("abr", 11), hash("bra", 11), hash("cada", 11), hash("adab", 11), hash("dabr", 11), hash("abra", 11)}), hashes);
  }

  @Test public void parseQuery() throws IOException, ParseException {
    QueryParser parser = new QueryParser(Version.LUCENE_34, "added", new HashedNGramAnalyzer(3, 3, 11));
    assertEquals("test", parser.parse("help"));
  }
}

/*
 * Local variables:
 * tab-width: 2
 * c-basic-offset: 2
 * indent-tabs-mode: nil
 * End:
 */

