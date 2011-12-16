package org.wikimedia.diffdb;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.KeywordTokenizer;
import java.io.*;

public class SimpleNGramAnalyzer extends Analyzer {
	public static class N1 extends SimpleNGramAnalyzer {
		public N1() { super(1); }
	}
	public static class N2 extends SimpleNGramAnalyzer {
		public N2() { super(2); }
	}
	public static class N3 extends SimpleNGramAnalyzer {
		public N3() { super(3); }
	}
	public static class N4 extends SimpleNGramAnalyzer {
		public N4() { super(4); }
	}
	public static class N5 extends SimpleNGramAnalyzer {
		public N5() { super(5); }
	}
	public static class N6 extends SimpleNGramAnalyzer {
		public N6() { super(6); }
	}

	private final int n;

	public SimpleNGramAnalyzer(int n) {
		this.n = n;
	}

	public final TokenStream tokenStream(String field, final Reader reader) {
		if ( SearchProperty.getInstance().getProperty(field).isAnalyzed() ) {
			return new SimpleNGramTokenizer(reader, this.n);
		} else {
			return new KeywordTokenizer(reader);
		}
	}
	public final TokenStream reusableTokenStream(String field, final Reader reader) {
		return tokenStream(field, reader);
	}
}

/*
 * Local variables:
 * tab-width: 2
 * c-basic-offset: 2
 * indent-tabs-mode: t
 * End:
 */
