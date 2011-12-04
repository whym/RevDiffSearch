package org.wikimedia.diffdb;

import org.wikimedia.diffdb.QueryParserCustom;
import org.wikimedia.diffdb.SimpleNGramAnalyzer;

import static org.junit.Assert.*;

import org.apache.lucene.search.Query;
import org.apache.lucene.util.Version;
import org.junit.Before;
import org.junit.Test;

public class TestQueryParserCustom {
	
	private QueryParserCustom qp; 
	
	@Before
	public void setUp() throws Exception {
		SimpleNGramAnalyzer analyzer = new SimpleNGramAnalyzer(0);
		qp = new QueryParserCustom(Version.LUCENE_34, "", analyzer);
	}
	
	@Test
	public void testQueryNoExplicitField() {
		String querystr = "foo";
		Query query = qp.parse(querystr);
		assertEquals(query.toString(), "foo");
	}
	
	@Test
	public void testQueryExplicitField() {
		String querystr = "title:foo";
		Query query = qp.parse(querystr);
		assertEquals(query.toString(), "title:foo");
	}
	
	@Test
	public void testQueryNoExplicitFieldCompound() {
		String querystr = "foo bar";
		Query query = qp.parse(querystr);
		assertEquals(query.toString(), "foo oo o b  ba bar");
	}
	
	@Test
	public void testQueryNoExplicitFieldCompoundExect() {
		String querystr = "\"foo bar\"";
		Query query = qp.parse(querystr);
		assertEquals(query.toString(), "\"foo oo o b  ba bar\"");
	}
	
	@Test
	public void testQueryImplicitAddedAndNamespace() {
		String querystr = "foo namespace:1";
		Query query = qp.parse(querystr);
		assertEquals(query.toString(), "foo namespace:1");
	}
	
	@Test
	public void testQueryExplicitAddedAndNamespaceSimple() {
		String querystr = "added:foo namespace:1";
		Query query = qp.parse(querystr);
		assertEquals(query.toString(), "added:foo namespace:1");
	}
	
	@Test
	public void testQueryExplicitAddedAndNamespaceComplex() {
		String querystr = "added:foo namespace:1";
		Query query = qp.parse(querystr);
		assertEquals(query.toString(), "added:foo oo o b  ba bar namespace:1");
	}
	
	@Test
	public void testQueryImplicitAndTimestamp() {
		String querystr = "foo timestamp:[2006-01 TO 2007-01]";
		Query query = qp.parse(querystr);
		assertEquals(query.toString(), "foo timestamp:[2006-01 TO 2007-01]");
	}
	
	@Test
	public void testQueryExplicitAddedAndTimestampSimple() {
		String querystr = "added:foo timestamp:[2006-01 TO 2007-01]";
		Query query = qp.parse(querystr);
		assertEquals(query.toString(), "added:foo timestamp:[2006-01 TO 2007-01]");
	}

	@Test
	public void testQueryExplicitAddedAndTimestampComplex() {
		String querystr = "added:foo timestamp:[2006-01 TO 2007-01]";
		Query query = qp.parse(querystr);
		assertEquals(query.toString(), "added:foo oo o b  ba bar timestamp:[2006-01 TO 2007-01]");
	}

	@Test
	public void testQueryExplicitAddedAndNotSimple() {
		String querystr = "added:foo -bar";
		Query query = qp.parse(querystr);
		assertEquals(query.toString(), "added:foo -bar");
	}


	@Test
	public void testQueryExplicitAddedAndNotComplex() {
		String querystr = "added:foo -bar bear beer";
		Query query = qp.parse(querystr);
		assertEquals(query.toString(), "added:foo -bar -ar -r b -bea -ear -ar  -r b - be -bee -eer");
	}

}
