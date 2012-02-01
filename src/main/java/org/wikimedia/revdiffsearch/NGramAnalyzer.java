package org.wikimedia.revdiffsearch;

import org.apache.lucene.analysis.ngram.NGramTokenizer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.KeywordTokenizer;
import java.io.Reader;

public class NGramAnalyzer extends Analyzer {
  public static class N1_2 extends NGramAnalyzer {
    public N1_2() { super(1, 2); }
  }
  public static class N1_3 extends NGramAnalyzer {
    public N1_3() { super(1, 3); }
  }
  public static class N1_4 extends NGramAnalyzer {
    public N1_4() { super(1, 4); }
  }
  public static class N1_5 extends NGramAnalyzer {
    public N1_5() { super(1, 5); }
  }
  public static class N1_6 extends NGramAnalyzer {
    public N1_6() { super(1, 6); }
  }

	private final int minN;
	private final int maxN;

	public NGramAnalyzer(int minN, int maxN) {
		this.minN = minN;
		this.maxN = maxN;
	}

	public final TokenStream tokenStream(String field, final Reader reader) {
		if ( SearchProperty.getInstance().getProperty(field).isAnalyzed() ) {
			return new NGramTokenizer(reader, this.minN, this.maxN);
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
