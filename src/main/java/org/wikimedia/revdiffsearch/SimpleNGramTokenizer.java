package org.wikimedia.revdiffsearch;

import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.tokenattributes.*;
import java.io.Reader;
import java.io.IOException;

public class SimpleNGramTokenizer extends Tokenizer {
  private final CharTermAttribute termAttr = addAttribute(CharTermAttribute.class);
  private final int n;
  private CharSequence buff;
  private int endpos;
  
  public SimpleNGramTokenizer(Reader reader, int n) {
		super(reader);
    this.n = n;
  }
  
  @Override public final boolean incrementToken() {
		if ( this.buff == null ) {
			try {
				this.reset();
			} catch ( IOException e ) {
				throw new IllegalArgumentException("fail to read", e);
			}
		}
    if (this.endpos > buff.length()) {
      return false;
    }
    termAttr.setEmpty();
    termAttr.append(this.buff, this.endpos - this.n, this.endpos);
    ++this.endpos;
    return true;
  }

	@Override public void reset() throws IOException {
		super.reset();
		this.buff = readAll(input);
		this.endpos = this.n;
	}
  
  private static CharSequence readAll(Reader reader) throws IOException {
		StringBuffer buff = new StringBuffer();
		char[] buffb = new char[4096];
		int len;
		while ((len = reader.read(buffb)) > 0) {
			buff.append(buffb, 0, len);
		}
		return buff.toString();
  }
}

/*
 * Local variables:
 * tab-width: 2
 * c-basic-offset: 2
 * indent-tabs-mode: t
 * End:
 */
