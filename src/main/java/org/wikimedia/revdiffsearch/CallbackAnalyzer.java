package org.wikimedia.revdiffsearch;

import java.io.Reader;
import java.io.IOException;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.KeywordTokenizer;
import org.apache.lucene.analysis.TokenStream;
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

  // @Override public final TokenStream tokenStream(final String field, Reader reader) {
  //   System.err.println("CB tokenStream");//!
  //   final String[] str = new String[]{null};
  //   final String strAll = readAll(reader);
  //   return new TokenStream() {
  //     public boolean incrementToken() throws IOException {
  //       System.err.println("inc");//!
  //       if ( str[0] != null ) {
  //         cb.execute(field, str[0]);
  //         str[0] = null;
  //         return true;
  //       } else {
  //         return false;
  //       }
  //     }
  //     public void reset() {
  //       str[0] = strAll;
  //       System.err.println(strAll);//!
  //     }
  //   };
  // }

  @Override public final TokenStream tokenStream(final String field, final Reader reader) {
    return new Tokenizer(reader) {
      final TokenStream ts = new KeywordTokenizer(reader);
      final CharTermAttribute tsTermAtt = ts.addAttribute(CharTermAttribute.class);
      final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
      public boolean incrementToken() throws IOException {
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
    };
  }
  
  @Override public final TokenStream reusableTokenStream(String field, final Reader reader) {
    return tokenStream(field, reader);
  }
}