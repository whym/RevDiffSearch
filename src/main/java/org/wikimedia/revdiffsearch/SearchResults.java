package org.wikimedia.revdiffsearch;

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.queryParser.ParseException;
import java.util.BitSet;
import java.io.IOException;

public interface SearchResults {
  void setSearcher(IndexSearcher searcher);
  void issue(String query, QueryParser parser) throws ParseException, IOException;
  BitSet getHits();
  int getNumberOfSkippedDocuments();
}
