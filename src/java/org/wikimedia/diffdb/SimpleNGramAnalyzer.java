package org.wikimedia.diffdb;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.*;
import org.apache.lucene.analysis.Analyzer;
import java.io.*;

public class SimpleNGramAnalyzer extends Analyzer {

	private static class NGramTokenStream extends TokenStream {
		private final int n;
		private final Reader reader;
		private CharSequence buff;
		private int endpos;
		private CharTermAttribute termAttr;

		public NGramTokenStream(int n, Reader reader) {
			this.n = n;
			this.reader = reader;
			this.buff = readAll(reader);
			this.endpos = n;
			this.termAttr = (CharTermAttribute) addAttribute(CharTermAttribute.class);
		}

		public boolean incrementToken() {
			if (this.endpos > buff.length()) {
				return false;
			}
			termAttr.setEmpty();
			termAttr.append(this.buff, this.endpos - this.n, this.endpos);
			++this.endpos;
			return true;
		}

		private static CharSequence readAll(Reader reader) {
			try {
				StringBuffer buff = new StringBuffer();
				char[] buffb = new char[4096];
				int len;
				while ((len = reader.read(buffb)) > 0) {
					buff.append(buffb, 0, len);
				}
				return buff.toString();
			} catch (IOException e) {
				throw new RuntimeException(e.toString());
			}
		}
	}

	private final int n;

	public SimpleNGramAnalyzer(int n) {
		this.n = n;
	}

	public TokenStream tokenStream(String field, final Reader reader) {
		return new NGramTokenStream(this.n, reader);
	}

	/*
	 * public static void main(String[] args) throws IOException { TokenStream
	 * ts = new SimpleNGramAnalyzer(3).tokenStream("default", new
	 * InputStreamReader(System.in)); PositionIncrementAttribute posIncr =
	 * (PositionIncrementAttribute)
	 * ts.addAttribute(PositionIncrementAttribute.class); CharTermAttribute term
	 * = (CharTermAttribute)ts.addAttribute(CharTermAttribute.class); while
	 * (ts.incrementToken()) { System.out.println(term);
	 * System.out.println(posIncr); } }
	 */
}

/*
 * Local variables:
 * tab-width: 2
 * c-basic-offset: 2
 * indent-tabs-mode: t
 * End:
 */
