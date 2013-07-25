package org.wikimedia.revdiffsearch;
import org.junit.*;
import java.io.*;
import java.util.*;
import org.apache.lucene.analysis.tokenattributes.*;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import static org.junit.Assert.*;

public class TestNGramAnalyzer {
  @Test public void shortString() throws IOException {
    TokenStream ts = new NGramAnalyzer(3,4).tokenStream("title", new StringReader("cadabra"));
    OffsetAttribute offset = (OffsetAttribute) ts.addAttribute(OffsetAttribute.class);
    CharTermAttribute term = (CharTermAttribute)ts.addAttribute(CharTermAttribute.class);
    List<Integer> offsets = new ArrayList<Integer>();
    List<String> ngrams = new ArrayList<String>();
    ts.reset();
    while (ts.incrementToken()) {
      ngrams.add(term.toString());
      offsets.add(offset.startOffset());
      offsets.add(offset.endOffset());
    }
    assertEquals(Arrays.asList(new String[]{"cad", "cada", "ada", "adab", "dab", "dabr", "abr", "abra", "bra"}), ngrams);
    assertEquals(Arrays.asList(new Integer[]{0, 3, 0, 4, 1, 4, 1, 5, 2, 5, 2, 6, 3, 6, 3, 7, 4, 7}), offsets);
  }
}

/*
 * Local variables:
 * tab-width: 2
 * c-basic-offset: 2
 * indent-tabs-mode: nil
 * End:
 */

