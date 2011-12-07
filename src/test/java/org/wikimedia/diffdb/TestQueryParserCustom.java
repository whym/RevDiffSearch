package org.wikimedia.diffdb;

import static org.junit.Assert.*;

import org.apache.lucene.search.Query;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.TermQuery;
import org.junit.Before;
import org.junit.Test;

public class TestQueryParserCustom {
	
	public QueryParserCustom qp; 
	
	public TestQueryParserCustom() {
		this.qp = new QueryParserCustom(new SimpleNGramAnalyzer(3));
	}
	
	@Test
	public void testQueryNoExplicitField() {
		String querystr = "foo";
		Query query = qp.construct(querystr);
		assertEquals("added:foo", query.toString());
	}
	
	@Test
	public void testQueryExplicitField() {
		String querystr = "title:foo";
		Query query = qp.construct(querystr);
		assertEquals(TermQuery.class, query.getClass());
		assertEquals("title:foo", query.toString());
	}
	
	@Test
	public void testQueryNoExplicitFieldCompound() {
		String querystr = "foo1 bar2";
		Query query = qp.construct(querystr);
		assertEquals(BooleanQuery.class, query.getClass());
		assertEquals("added:foo added:oo1 added:bar added:ar2", query.toString());
	}
	
	@Test
	public void testQueryNoExplicitFieldCompoundExact() {
		String querystr = "\"foo bar\"";
		Query query = qp.construct(querystr);
		assertEquals("\"foo oo o b  ba bar\"", query.toString());
	}
	
	@Test
	public void testQueryImplicitAddedAndNamespace() {
		String querystr = "foo namespace:1";
		Query query = qp.construct(querystr);
		assertEquals("foo namespace:1", query.toString());
	}
	
	@Test
	public void testQueryExplicitAddedAndNamespaceSimple() {
		String querystr = "added:foo namespace:1";
		Query query = qp.construct(querystr);
		assertEquals("added:foo namespace:1", query.toString());
	}
	
	@Test
	public void testQueryExplicitAddedAndNamespaceComplex() {
		String querystr = "added:foo namespace:1";
		Query query = qp.construct(querystr);
		assertEquals("added:foo oo o b  ba bar namespace:1", query.toString());
	}
	
	@Test
	public void testQueryImplicitAndTimestamp() {
		String querystr = "foo timestamp:[2006-01 TO 2007-01]";
		Query query = qp.construct(querystr);
		assertEquals("foo timestamp:[2006-01 TO 2007-01]", query.toString());
	}
	
	@Test
	public void testQueryExplicitAddedAndTimestampSimple() {
		String querystr = "added:foo timestamp:[2006-01 TO 2007-01]";
		Query query = qp.construct(querystr);
		assertEquals("added:foo timestamp:[2006-01 TO 2007-01]", query.toString());
	}

	@Test
	public void testQueryExplicitAddedAndTimestampComplex() {
		String querystr = "added:foo timestamp:[2006-01 TO 2007-01]";
		Query query = qp.construct(querystr);
		assertEquals("added:foo oo o b  ba bar timestamp:[2006-01 TO 2007-01]", query.toString());
	}

	@Test
	public void testQueryExplicitAddedAndNotSimple() {
		String querystr = "added:foo -bar";
		Query query = qp.construct(querystr);
		assertEquals("added:foo -bar", query.toString());
	}


	@Test
	public void testQueryExplicitAddedAndNotComplex() {
		String querystr = "added:foo -bar bear beer";
		Query query = qp.construct(querystr);
		assertEquals("added:foo -bar -ar -r b -bea -ear -ar  -r b - be -bee -eer", query.toString());
	}

}
