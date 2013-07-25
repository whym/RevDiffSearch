package org.wikimedia.revdiffsearch;

import static org.junit.Assert.*;

import org.apache.lucene.search.Query;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.util.Version;
import org.junit.Before;
import org.junit.Test;

public class TestQueryParser {
	
	public QueryParser qp; 
	
	public TestQueryParser() throws ParseException {
		this.qp = new QueryParser(Version.LUCENE_44, "added", RevDiffSearchUtils.getAnalyzerCombined(new SimpleNGramAnalyzer(3)));
	}

	@Test
	public void testQueryNoExplicitField() throws ParseException {
		String querystr = "foo";
		Query query = qp.parse(querystr);
		assertEquals("added:foo", query.toString());
	}
	
	@Test
	public void testQueryExplicitField() throws ParseException {
		String querystr = "title:foo";
		Query query = qp.parse(querystr);
		assertEquals(TermQuery.class, query.getClass());
		assertEquals("title:foo", query.toString());
	}
	
	@Test
	public void testQueryNoExplicitFieldCompound() throws ParseException {
		String querystr = "foo1 bar2";
		Query query = qp.parse(querystr);
		assertEquals(BooleanQuery.class, query.getClass());
		assertEquals("(added:foo added:oo1) (added:bar added:ar2)", query.toString());
	}
	
	@Test
	public void testQueryNoExplicitFieldCompoundExact() throws ParseException {
		String querystr = "\"foo bar\"";
		Query query = qp.parse(querystr);
		assertEquals("added:\"foo oo  o b  ba bar\"", query.toString());
	}
	
	@Test
	public void testQueryImplicitAddedAndNamespace() throws ParseException {
		String querystr = "foo namespace:1";
		Query query = qp.parse(querystr);
		assertEquals("added:foo namespace:1", query.toString());
	}
	
	@Test
	public void testQueryExplicitAddedAndNamespaceSimple() throws ParseException {
		String querystr = "added:foo namespace:1";
		Query query = qp.parse(querystr);
		assertEquals("added:foo namespace:1", query.toString());
	}
	
	@Test
	public void testQueryExplicitAddedAndNamespaceComplex() throws ParseException {
		String querystr = "added:\"foo bar\" namespace:1";
		Query query = qp.parse(querystr);
		assertEquals("added:\"foo oo  o b  ba bar\" namespace:1", query.toString());
	}
	
	@Test
	public void testQueryImplicitAndTimestamp() throws ParseException {
		String querystr = "added:foo timestamp:[2006-01 TO 2007-01]";
		Query query = qp.parse(querystr);
		assertEquals("added:foo timestamp:[2006-01 TO 2007-01]", query.toString());
	}
	
	@Test
	public void testQueryExplicitAddedAndTimestampSimple() throws ParseException {
		String querystr = "added:foo timestamp:[2006-01 TO 2007-01]";
		Query query = qp.parse(querystr);
		assertEquals("added:foo timestamp:[2006-01 TO 2007-01]", query.toString());
	}

	@Test
	public void testQueryExplicitAddedAndTimestampComplex() throws ParseException {
		String querystr = "added:\"foo bar\" timestamp:[2006-01 TO 2007-01]";
		Query query = qp.parse(querystr);
		assertEquals("added:\"foo oo  o b  ba bar\" timestamp:[2006-01 TO 2007-01]", query.toString());
	}

	@Test
	public void testQueryExplicitAddedAndNotSimple() throws ParseException {
		String querystr = "added:foo -bar";
		Query query = qp.parse(querystr);
		assertEquals("added:foo -added:bar", query.toString());
	}


	@Test
	public void testQueryExplicitAddedAndNotComplex() throws ParseException {
		String querystr = "added:foo -bar bear beer";
		Query query = qp.parse(querystr);
		assertEquals("added:foo -added:bar (added:bea added:ear) (added:bee added:eer)", query.toString());
	}
	
}
