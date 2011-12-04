package org.wikimedia.diffdb;

import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.util.AttributeImpl;
import org.apache.lucene.util.Version;
import org.apache.lucene.search.Query;
import org.apache.lucene.analysis.ngram.NGramTokenizer;
import org.wikimedia.diffdb.SearchProperty.Property;


public class QueryParserCustom extends QueryParser {

	
	public QueryParserCustom(Version matchVersion, String f, Analyzer a) {
		super(matchVersion, f, a);
		// TODO Auto-generated constructor stub
	}
	
	private HashMap<String, String> deconstruct(String query) {
		HashMap<String, String> terms = new HashMap<String, String>();
		String term = null;
		SearchProperty sp = new SearchProperty();
		Map<String, Property> properties = sp.init();
		Iterator<Entry<String, Property>> it = properties.entrySet().iterator();
		while (it.hasNext()){
			Property prop = it.next().getValue();
			Matcher match = prop.pattern().matcher(query);
			if (match.find()) {
				int start = match.start();
				int end =  match.end();
				term = query.substring(start, end);
				query = query.replace(term, "");
			}
			if (prop.isAnalyzed() && prop.isStored() && term !=null) {
				terms.put(prop.name(), term);
			}
		}
		// what remains is the main query
		terms.put("added", query);
		return terms;
	}
	

	private String reconstruct(HashMap<String, String> terms) {
		String query ="";
	    Iterator<Entry<String, String>> it = terms.entrySet().iterator();
	    SearchProperty sp = new  SearchProperty();
	    Map<String, Property> properties = sp.init();
	    while (it.hasNext()) {
	        Map.Entry<String, String> pairs = (Map.Entry<String, String>)it.next();
	        Property prop = properties.get(pairs.getKey());
	        if (prop.pattern().equals(SearchProperty.STRING_PATTERN)) {
	        	String term = pairs.getValue();
	        	term = term.replace(pairs.getKey()+":","");	//Strip out the name of the property to ngram
	        	String ngram = createNgram(term);
	        	query = String.format("%s %s:%s", query,pairs.getKey(), ngram.toString());
	        } else {
	        	
	        }
	        //System.out.println(pairs.getKey() + " = " + pairs.getValue());
	        it.remove(); // avoids a ConcurrentModificationException
	    }
 			    
		return query;
	}
	
	private String createNgram(String term) {
		StringReader reader = new StringReader(term);
		NGramTokenizer tokenizer = new NGramTokenizer(reader,3,3);
		String ngram = "";
		Iterator<AttributeImpl> it = tokenizer.getAttributeImplsIterator();
		while (it.hasNext()) { 
				ngram= ngram + it.next().toString();
		}
		return ngram;
	}
	
	@Override
	public Query parse(String querystr) {
		HashMap<String, String> terms = deconstruct(querystr);
		String query = reconstruct(terms);
		Query q = null;
		try {
			q = Query(query);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return q;
	}
}