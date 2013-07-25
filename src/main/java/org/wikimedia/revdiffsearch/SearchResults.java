package org.wikimedia.revdiffsearch;

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.queryparser.classic.ParseException;
import java.util.BitSet;
import java.io.IOException;

public interface SearchResults {
  void setSearcher(IndexSearcher searcher);
  void issue(String query, QueryParser parser) throws ParseException, IOException;
  BitSet getHits();
  int getNumberOfSkippedDocuments();
}
