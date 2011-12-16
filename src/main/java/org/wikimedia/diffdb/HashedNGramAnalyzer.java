package org.wikimedia.diffdb;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.KeywordTokenizer;
import java.io.Reader;

public class HashedNGramAnalyzer extends Analyzer {
	private final int minN;
	private final int maxN;
  private final int seed;

	public HashedNGramAnalyzer(int minN, int maxN, int seed) {
		this.minN = minN;
		this.maxN = maxN;
    this.seed = seed;
	}

	public final TokenStream tokenStream(String field, final Reader reader) {
		if ( SearchProperty.getInstance().getProperty(field).isAnalyzed() ) {
			return new HashedNGramTokenizer(reader, this.minN, this.maxN, this.seed);
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
