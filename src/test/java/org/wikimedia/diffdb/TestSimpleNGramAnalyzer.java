package org.wikimedia.diffdb;
import org.junit.*;
import java.io.*;
import java.util.*;
import org.apache.lucene.analysis.tokenattributes.*;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import static org.junit.Assert.*;

public class TestSimpleNGramAnalyzer {
  @Test public void shortString() throws IOException {
    TokenStream ts = new SimpleNGramAnalyzer(3).tokenStream("default", new StringReader("cadabra"));
    PositionIncrementAttribute posIncr = (PositionIncrementAttribute)
      ts.addAttribute(PositionIncrementAttribute.class);
    CharTermAttribute term = (CharTermAttribute)ts.addAttribute(CharTermAttribute.class);
    List<Integer> increments = new ArrayList<Integer>();
    List<String> ngrams = new ArrayList<String>();
    while (ts.incrementToken()) {
      ngrams.add(term.toString());
      increments.add(posIncr.getPositionIncrement());
    }
    assertEquals(Arrays.asList(new String[]{"cad", "ada", "dab", "abr", "bra"}), ngrams);
    assertEquals(Arrays.asList(new Integer[]{1, 1, 1, 1, 1}), increments);
  }
}

/*
 * Local variables:
 * tab-width: 2
 * c-basic-offset: 2
 * indent-tabs-mode: nil
 * End:
 */

