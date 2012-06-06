package org.wikimedia.revdiffsearch;

import java.util.Properties;
import java.util.Map;
import java.io.IOException;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.File;
import org.apache.lucene.analysis.Analyzer;
import java.util.logging.Logger;

public class RevDiffSearchUtils {
	private static final Logger logger = Logger.getLogger(RevDiffSearchUtils.class.getName());
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

  public static boolean getProperty(String k, boolean def) {
    String s = prop.getProperty(k);
    if ( s == null ) {
      return def;
    } else {
      return !("false".equals(s.toLowerCase()));
    }
  }

  /**
   * Loads the default property file for this class. Properties can be retrieved with {@link #getProperty(String)} or {@link #getProperties()}.
   */
  public static void loadProperties() {
    loadProperties(RevDiffSearchUtils.class);
  }
  public static void loadProperties(File file) throws IOException {
    loadProperties(new FileInputStream(file));
  }
  public static <T> void loadProperties(Class<T> cls) {
    try {
      String name = cls.getName().replace('.','/') + ".properties";
      InputStream is = cls.getClassLoader().getResourceAsStream(name);
      if ( is == null ) {
        logger.warning(name + " not found");
        return;
      } else {
        logger.info("loading Properies: " + name);
        loadProperties(is);
      }
    } catch (IOException e) {
      if ( getProperty("verbose", false) ) {
        e.printStackTrace();
      }
    }
  }
  private static void loadProperties(InputStream is) throws IOException {
    Properties loaded = new Properties();
    loaded.load(new InputStreamReader(is));
    for (Map.Entry<Object,Object> ent: loaded.entrySet()) {
      if ( prop.getProperty(ent.getKey().toString()) == null )
        prop.setProperty(ent.getKey().toString(), ent.getValue().toString());
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
