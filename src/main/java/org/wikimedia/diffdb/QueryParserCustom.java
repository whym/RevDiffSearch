package org.wikimedia.diffdb;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.util.Version;
import org.apache.lucene.search.Query;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.wikimedia.diffdb.SearchProperty.Property;


public class QueryParserCustom {

	public final Version matchVersion = Version.LUCENE_34;
	public final Analyzer analyzer; 
	
  public QueryParserCustom(Analyzer analyzer) {
    this.analyzer = analyzer;
  }
	
	private HashMap<String, String> deconstruct(String query) {
		HashMap<String, String> terms = new HashMap<String, String>();
		String term = null;
		StringBuilder sb = new StringBuilder();
		sb.append(query);
		SearchProperty sp = new SearchProperty();
		Map<String, Property> properties = sp.init();
		Iterator<Entry<String, Property>> it = properties.entrySet().iterator();
		while (it.hasNext()){
			Property prop = it.next().getValue();
			if (sb.indexOf(prop.name()) >-1) {
				Matcher match = prop.pattern().matcher(sb);
				if (match.find()) {
					int start = match.start() +1;
					int end =  match.end();
					term = sb.substring(start, end);
					start = start - prop.name().length() -1;
					System.out.println(sb.toString());
					//This is ridiculous
					sb= sb.replace(start, end-1, "");
					sb= sb.delete(start, end-1);
					System.out.println(sb.toString());
					//replace(String.format("%s%s",prop.name(), term), "");
				}	
			}
			
			if (prop.isAnalyzed() && prop.isStored() && term !=null) {
				terms.put(prop.name(), term);
				term = null;
			}
		}
		// what remains is the main query
		if (!sb.toString().equals("")){
			terms.put("added", sb.toString());
		}
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
	        	//term = term.replace(pairs.getKey()+":","");	//Strip out the name of the property to ngram
	        	List<String> ngrams = createNgram(term);
	        	for (String ngram : ngrams) {
	        		query = String.format("%s %s:%s", query,pairs.getKey(), ngram.toString());
	        	}
	        	//System.out.println(query);
	        } else {
	        	
	        }
	        //System.out.println(pairs.getKey() + " = " + pairs.getValue());
	        it.remove(); // avoids a ConcurrentModificationException
	    }
 		//System.out.println("FINAL RESULT:" + query);
		return query;
	}
	
	private List<String> createNgram(String phrase) {
	    TokenStream ts = new SimpleNGramAnalyzer(3).tokenStream("default", new StringReader(phrase));
	    PositionIncrementAttribute posIncr = (PositionIncrementAttribute)
	      ts.addAttribute(PositionIncrementAttribute.class);
	    CharTermAttribute term = (CharTermAttribute)ts.addAttribute(CharTermAttribute.class);
	    List<Integer> increments = new ArrayList<Integer>();
	    List<String> ngrams = new ArrayList<String>();
	    try {
			while (ts.incrementToken()) {
			  ngrams.add(term.toString());
			  increments.add(posIncr.getPositionIncrement());
			
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	    
		return ngrams;
	}
	
	
	public Query construct(String querystr)  {
		HashMap<String, String> terms = deconstruct(querystr);
		String queryterms = reconstruct(terms);
		Query query = null;
    System.err.println("----" + this.matchVersion + queryterms + this.analyzer);//!
		QueryParser qp =  new QueryParser(this.matchVersion, queryterms, this.analyzer);
		try {
			query = qp.parse(queryterms);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println(query.toString());
		return query;
	}
}