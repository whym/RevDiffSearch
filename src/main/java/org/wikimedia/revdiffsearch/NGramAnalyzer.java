package org.wikimedia.revdiffsearch;

import org.apache.lucene.analysis.ngram.NGramTokenizer;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.util.Version;
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

  @Override protected TokenStreamComponents createComponents(String field, Reader reader) {
		return new TokenStreamComponents(new NGramTokenizer(Version.LUCENE_44, reader, this.minN, this.maxN));
	}

}

/*
 * Local variables:
 * tab-width: 2
 * c-basic-offset: 2
 * indent-tabs-mode: t
 * End:
 */
