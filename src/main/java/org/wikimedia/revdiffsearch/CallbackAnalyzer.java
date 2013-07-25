package org.wikimedia.revdiffsearch;

import java.io.Reader;
import java.io.IOException;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.KeywordTokenizer;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

public class CallbackAnalyzer extends Analyzer {
  public static interface Callback {
    void execute(String field, String value);
  }

  private final Callback cb;
  public CallbackAnalyzer(Callback cb) {
    this.cb = cb;
  }

  private static String readAll(Reader reader) {
    try {
      StringBuffer buff = new StringBuffer();
      char[] buffb = new char[4096];
      int len;
      while ( (len = reader.read(buffb)) > 0 ) {
        buff.append(buffb, 0, len);
      }
      return buff.toString();
    } catch (IOException e) {
      e.printStackTrace();
      System.exit(1);
    }
    return null;
  }

  @Override protected TokenStreamComponents createComponents(final String field, final Reader reader) {
    return new TokenStreamComponents(new Tokenizer(reader) {
        final Tokenizer ts = new KeywordTokenizer(reader);
        final CharTermAttribute tsTermAtt = ts.addAttribute(CharTermAttribute.class);
        final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
        @Override public boolean incrementToken() throws IOException {
          boolean ret = ts.incrementToken();
          if ( ret ) {
            String s = tsTermAtt.toString();
            char[] str = s.toCharArray();
            this.termAtt.copyBuffer(str, 0, str.length);
            cb.execute(field, s);
            return true;
          } else {
            return false;
          }
        }

        @Override public void reset() throws IOException {
          ts.reset();
        }

        @Override public void end() throws IOException {
          ts.end();
        }
      });
  }

}