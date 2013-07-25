package org.wikimedia.revdiffsearch;

import org.apache.lucene.analysis.Analyzer;
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

  @Override protected TokenStreamComponents createComponents(String field, Reader reader) {
		return new TokenStreamComponents(new HashedNGramTokenizer(reader, this.minN, this.maxN, this.seed));
	}

}

/*
 * Local variables:
 * tab-width: 2
 * c-basic-offset: 2
 * indent-tabs-mode: t
 * End:
 */
