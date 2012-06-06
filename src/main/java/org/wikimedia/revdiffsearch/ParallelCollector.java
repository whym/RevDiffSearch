package org.wikimedia.revdiffsearch;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;
import java.util.List;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.BitSet;

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.index.IndexReader;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Executor;
import java.util.concurrent.Callable;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.io.IOException;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.FilteredQuery;
import org.apache.lucene.util.ThreadInterruptedException;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.util.Version;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.analysis.Analyzer;

public class ParallelCollector implements SearchResults {
	private static final Logger logger = Logger.getLogger(ParallelCollector.class.getName());

  private IndexSearcher searcher;
  private final BitSet hits;
  private Map<String,Set<String>> queryFields;
  private int docBase;
  private int maxRevs;
  private int skipped;
  private final ExecutorService executor;
  
  public ParallelCollector(IndexSearcher searcher, int max, ExecutorService executor) {
    this.searcher = searcher;
    this.hits = new BitSet(searcher.getIndexReader().maxDoc());
    this.maxRevs = max;
    this.skipped = 0;
    this.executor = executor;
  }
  public void setSearcher(IndexSearcher seacher) {
    this.searcher = searcher;
  }
  public void issue(String query, QueryParser parser) throws ParseException, IOException {
    Query q = parser.parse(query);
    List<InnerCollector> collectors = new ArrayList<InnerCollector>();
    for (IndexReader reader: this.searcher.getSubReaders()) {
      IndexSearcher srch = new IndexSearcher(reader);
      InnerCollector c = new InnerCollector(srch, this.maxRevs);
      collectors.add(c);
      c.prepare(query, parser);
      executor.execute(c);
    }

    // wait for the threads to finish
    try {
      this.executor.shutdown();
      while (!this.executor.isTerminated()) {
				Thread.sleep(100L);
      }
    } catch ( InterruptedException e ) {
    }

    // collect the results
    this.skipped = 0;
    for ( InnerCollector c: collectors ) {
      this.hits.or(c.hits);
      this.skipped += c.getNumberOfSkippedDocuments();
    }
  }
  public int getNumberOfSkippedDocuments() {
    return this.skipped;
  }

  public BitSet getHits() {
    return this.hits;
  }

  public static class InnerCollector extends DiffCollector implements Runnable {
    public InnerCollector(IndexSearcher searcher, int max) {
      super(searcher, max);
    }
    public void run() {
      try {
        this.searcher.search(parsed, this);
      } catch ( IOException e ) {
        logger.severe("in InnerCollector " + e);
      }
    }
  }
}

/*
 * Local variables:
 * tab-width: 2
 * c-basic-offset: 2
 * indent-tabs-mode: nil
 * End:
 */

