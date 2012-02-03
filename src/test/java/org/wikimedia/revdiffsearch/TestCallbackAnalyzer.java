package org.wikimedia.revdiffsearch;
import org.junit.*;
import java.io.*;
import java.util.*;
import java.net.*;
import org.apache.lucene.analysis.*;
import org.apache.lucene.analysis.tokenattributes.*;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.util.Version;
import static org.junit.Assert.*;

public class TestCallbackAnalyzer {
  @Test public void testWithQueryParser() throws Exception {
    final String[] result = new String[]{null, null};
    QueryParser parser = new QueryParser
      (Version.LUCENE_35, "added",
       new CallbackAnalyzer(new CallbackAnalyzer.Callback() {
           public void execute(String f, String v) {
             result[0] = f;
             result[1] = v;
           }
         }));
    System.err.println("parsed: " + parser.parse("value1"));
    assertEquals("added", result[0]);
    assertEquals("value1", result[1]);
  }

  @Test public void testKeywordTokenizer() throws Exception {
    List<String> result = new ArrayList<String>();
    TokenStream ts = new KeywordTokenizer(new StringReader("cadabra"));
    CharTermAttribute term = (CharTermAttribute)ts.addAttribute(CharTermAttribute.class);
    List<String> ngrams = new ArrayList<String>();
    while (ts.incrementToken()) {
      result.add(term.toString());
    }
    assertEquals(Collections.singletonList("cadabra"), result);
  }
}

/*
 * Local variables:
 * tab-width: 2
 * c-basic-offset: 2
 * indent-tabs-mode: nil
 * End:
 */
