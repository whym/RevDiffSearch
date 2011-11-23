package org.wikimedia.diffdb;

import org.apache.lucene.analysis.ngram.NGramTokenizer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Analyzer;
import java.io.Reader;

public class NGramAnalyzer extends Analyzer {
	private final int minN;
	private final int maxN;

	public NGramAnalyzer(int minN, int maxN) {
		this.minN = minN;
		this.maxN = maxN;
	}

	public TokenStream tokenStream(String field, final Reader reader) {
		return new NGramTokenizer(reader, this.minN, this.maxN);
	}
}
