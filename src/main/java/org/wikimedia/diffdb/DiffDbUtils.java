package org.wikimedia.diffdb;

import java.util.Properties;
import org.apache.lucene.analysis.Analyzer;
import java.util.logging.Logger;

public class DiffDbUtils {
	private static final Logger logger = Logger.getLogger(DiffDbUtils.class.getName());
  private static Properties prop;
  static {
    try {
      prop = System.getProperties();
    } catch (SecurityException e) {
      prop = new Properties();
    }
  }

  public static String getProperty(String k, String def) {
    String s = prop.getProperty(k);
    if ( s == null ) {
      return def;
    } else {
      return s;
    }
  }

  public static int getProperty(String k, int def) throws NumberFormatException {
    String s = prop.getProperty(k);
    if ( s == null ) {
      return def;
    } else {
      return Integer.parseInt(s);
    }
  }

  public static long getProperty(String k, long def) throws NumberFormatException {
    String s = prop.getProperty(k);
    if ( s == null ) {
      return def;
    } else {
      return Long.parseLong(s);
    }
  }

  public static double getProperty(String k, double def) throws NumberFormatException {
    String s = prop.getProperty(k);
    if ( s == null ) {
      return def;
    } else {
      return Double.parseDouble(s);
    }
  }


  public static Analyzer getAnalyzer() {
    int seed = getProperty("ngram_seed", -1);
    int m = getProperty("ngram_max", 0);
    int n = getProperty("ngram", 3);
    if ( m != 0 && n != 0 && seed >= 0) {
			logger.info(String.format("using HashedNGramAnalyzer(%d, %d, %d)", n, m, seed));
      return new HashedNGramAnalyzer(n, m, seed);
    } else if ( m != 0 && n != 0 ) {
			logger.info(String.format("using NGramAnalyzer(%d, %d)", n, m));
			return new NGramAnalyzer(n, m);
		} else {
			logger.info(String.format("using SimpleNGramAnalyzer(%d)", n));
			return new SimpleNGramAnalyzer(n);
		}
  }
}
/*
 * Local variables:
 * tab-width: 2
 * c-basic-offset: 2
 * indent-tabs-mode: t
 * End:
 */
