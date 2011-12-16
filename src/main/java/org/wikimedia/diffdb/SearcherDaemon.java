package org.wikimedia.diffdb;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.TreeMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.BitSet;
import java.util.Date;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.time.DurationFormatUtils;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.search.TopDocsCollector;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Version;
import org.apache.lucene.document.Document;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.KeywordTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.codec.string.StringDecoder;
import org.jboss.netty.handler.codec.string.StringEncoder;

import org.json.JSONObject;
import org.json.JSONArray;
import org.json.JSONException;

public class SearcherDaemon implements Runnable {
	private static final Logger logger = Logger.getLogger(SearcherDaemon.class.getName());
	private static final Map<String, Pattern> collapsePatterns;
	static {
		collapsePatterns = new TreeMap<String, Pattern>();
		collapsePatterns.put("day",    Pattern.compile("\\d\\d\\d\\d-\\d\\d-\\d\\d"));
		collapsePatterns.put("month",  Pattern.compile("\\d\\d\\d\\d-\\d\\d"));
	}
	private final InetSocketAddress address;
	private final IndexSearcher searcher;
	private final QueryParser parser;
	private final long startTimeMillis;

	public SearcherDaemon(InetSocketAddress address, String dir, final QueryParser parser) throws IOException {
		this(address, new IndexSearcher(FSDirectory.open(new File(dir)), true), parser);
	}

	public SearcherDaemon(InetSocketAddress address, IndexSearcher searcher, final QueryParser parser) throws IOException {
		this.address = address;
		this.searcher = searcher;
		this.parser = parser;
		this.parser.setDefaultOperator(QueryParser.AND_OPERATOR);
		this.startTimeMillis = System.currentTimeMillis();
	}
	
	public void run() {
    ServerBootstrap bootstrap = new ServerBootstrap
      (new NioServerSocketChannelFactory
       (Executors.newCachedThreadPool(),
        Executors.newCachedThreadPool()));
    
    bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
        @Override
          public ChannelPipeline getPipeline() throws Exception {
          ChannelPipeline pipeline = Channels.pipeline(new SearcherHandler(searcher, parser));
          Charset charset = Charset.forName("UTF-8");
					
					pipeline.addLast("decoder", new StringEncoder());
					pipeline.addLast("encoder", new StringDecoder());

          return pipeline;
        }
      });
    
    bootstrap.bind(this.address);
		logger.info("SearcherDaemon is launched in " + DurationFormatUtils.formatDurationHMS(System.currentTimeMillis() - this.startTimeMillis) + " at " + this.address);
	}

	private static class MyCollector extends Collector {
		private final IndexSearcher searcher;
		private final BitSet hits;
		private final Map<String,String> queryFields;
		private int docBase;
		private int maxRevs;
		private int positives;
		private int skipped;

		public MyCollector(IndexSearcher searcher, String query, int max) {
			this.searcher = searcher;
			this.hits = new BitSet(searcher.getIndexReader().maxDoc());
			this.maxRevs = max;
			this.positives = 0;
			this.skipped = 0;
			this.queryFields = getQueryFields(query);
		}
		private static Map<String,String> getQueryFields(String query) {
			final Map<String,String> map = new TreeMap<String,String>();
			try {
				QueryParser ps = new QueryParser(Version.LUCENE_35, "added", new Analyzer() {
						public final TokenStream tokenStream(final String field, final Reader reader) {
							final TokenStream ts = new KeywordTokenizer(reader);
							return new TokenStream() {
								CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
								public boolean incrementToken() throws IOException {
									if ( ts.incrementToken() ) {
										map.put(field, termAtt.toString());
										return true;
									} else {
										return false;
									}
								}
							};
						}
						public final TokenStream reusableTokenStream(String field, final Reader reader) {
							return tokenStream(field, reader);
						}
					});
				ps.parse(query);
			} catch ( ParseException e ) {
				logger.severe(e.toString());
			}
			return map;
		}

		public boolean acceptsDocsOutOfOrder() {
			return true;
		}

		public void setNextReader(IndexReader reader, int docBase) {
			this.docBase = docBase;
		}

		public void setScorer(Scorer scorer) {
		}

		public int positives() {
			return this.positives;
		}

		public int skipped() {
			return this.skipped;
		}

		public void collect(int doc) {
			doc += this.docBase;
			++this.positives;
			// TODO: it must work for other fileds than 'added' and 'removed'
			if ( this.hits.cardinality() >= this.maxRevs ) {
				++this.skipped;
				this.hits.clear(doc);
				return;
			}					
			try {
				for ( Map.Entry<String,String> ent: this.queryFields.entrySet() ) {
					if ( searcher.doc(doc).getField(ent.getKey()).stringValue().indexOf(ent.getValue()) < 0 ) {
						this.hits.clear(doc);
						return;
					}
				}
				this.hits.set(doc);
			} catch (IOException e) {
				logger.severe("failed to read " + doc);
			} catch ( IllegalArgumentException e ) {
				String str = "";
				try {
					str = searcher.doc(doc).getFields().toString();
					System.err.println(str);//!
				} catch (IOException ex) {
				}
				throw new RuntimeException(str, e);
			}
		}
		public BitSet getHits() {
			return this.hits;
		}
	}

  public static class SearcherHandler extends SimpleChannelUpstreamHandler {
    private static final Logger logger = Logger.getLogger(SearcherHandler.class.getName());
    private final IndexSearcher searcher;
    private final QueryParser parser;

    public SearcherHandler(IndexSearcher searcher, QueryParser parser) {
      this.searcher = searcher;
      this.parser = parser;
    }

		private JSONArray writeCollapsedHitsByTimestamp(BitSet hits, List<String> fields, Pattern pattern) throws IOException, JSONException {
			Map<String, JSONArray> map = new TreeMap<String, JSONArray>();
			for(int i = hits.nextSetBit(0); i >= 0; i = hits.nextSetBit(i+1) ) {
				Document doc = this.searcher.doc(i);
				JSONArray array = new JSONArray();
				for ( String f: fields ) {
					array.put(StringEscapeUtils.unescapeJava(doc.getField(f).stringValue()));
				}
				Matcher matcher = pattern.matcher(doc.getField("timestamp").stringValue());
				String key;
				if (!matcher.find()) {
					key = "none";
				} else {
					key = matcher.group(0);
				}
				JSONArray ls;
				if ( (ls = map.get(key)) == null ) {
					ls = new JSONArray();
					map.put(key,ls);
				}
				ls.put(array);
			}
			JSONArray ret = new JSONArray();
			for ( Map.Entry<String, JSONArray> ent: map.entrySet() ) {
				ret.put(new JSONArray(new Object[]{ent.getKey(), ent.getValue()}));
			}
			return ret;
		}

		private JSONArray writeHits(BitSet hits, List<String> fields) throws IOException {
			// collect field values
			JSONArray ret = new JSONArray();
			for(int i = hits.nextSetBit(0); i >= 0; i = hits.nextSetBit(i+1) ) {
				Document doc = this.searcher.doc(i);
				JSONArray array = new JSONArray();
				for ( String f: fields ) {
					array.put(StringEscapeUtils.unescapeJava(doc.getField(f).stringValue()));
				}
				ret.put(array);
			}
			return ret;
		}

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {
			long processingStartMillis = System.currentTimeMillis();
			try {
				JSONObject qobj = new JSONObject(((ChannelBuffer)e.getMessage()).toString(Charset.defaultCharset()));
				logger.info("received query: " + qobj.toString(2) + " at " + ctx);
				String query   = qobj.getString("q");						 // to be fed to QueryParser
				String hitsper = qobj.optString("collapse_hits", "no"); // no or day or month
				int maxrevs    = qobj.optInt("max_revs", 1000);
				double version = qobj.optDouble("version", 0.1);		// 
				JSONArray fields_ = qobj.optJSONArray("fields"); // the fields to be given in the output. if empty, only number of hits will be emitted

				MyCollector collector = new MyCollector(this.searcher, query, maxrevs);
				Query parsedQuery = this.parser.parse(query);
				this.searcher.search(parsedQuery, collector);
				BitSet hits = collector.getHits();
				JSONObject ret = new JSONObject();

				ret.put("hits_all", hits.cardinality());
				ret.put("parsed_query", parsedQuery);

				List<String> fields = new ArrayList<String>();
				if ( fields_ == null ) {
					String f = qobj.optString("fields");
					if ( f == null  ||  "".equals(f) ) {
						fields = Collections.emptyList();
					} else {
						fields = Collections.singletonList(f);
					}
				} else {
					for ( int i = 0; i < fields_.length(); ++i ) {
						fields.add(fields_.getString(i));
					}
				}
				ret.put("debug_positives", collector.positives());
				ret.put("debug_unchecked", collector.skipped());
				logger.info("response (without hits): " + ret);
				
				Pattern cpattern;
				if ( "no".equals(hitsper) || (cpattern = collapsePatterns.get(hitsper)) == null ) {
					if ( fields.size() > 0 ) {
						ret.put("hits", writeHits(hits, fields));
					} else {
						ret.put("hits", hits.cardinality());
					}
				} else {
					JSONArray hitEntries = writeCollapsedHitsByTimestamp(hits, fields, cpattern);
					if ( fields.size() > 0 ) {
						ret.put("hits", hitEntries);
					} else {
						JSONArray hits_ = new JSONArray();
						for ( int i = 0; i < hitEntries.length(); ++i ) {
							hits_.put(new JSONArray(new Object[]{
										hitEntries.getJSONArray(i).getString(0),
										hitEntries.getJSONArray(i).getJSONArray(1).length(),
									}));
						}
						ret.put("hits", hits_);
					}
				}
				// include query to response
				ret.put("query",
								qobj.put("collapse_hits", hitsper).
								put("max_revs", maxrevs).
								put("version", version).
								put("fields", new JSONArray(fields.toArray())));

				ret.put("elapsed", System.currentTimeMillis() - processingStartMillis);
				String str = ret.toString();
				e.getChannel().write(str);
				logger.info("responded in " + DurationFormatUtils.formatDurationHMS(System.currentTimeMillis() - processingStartMillis) + " (" + str.length() + " characters)");
      } catch (IOException ex) {
        e.getChannel().write("{\"exception\": \"" + StringEscapeUtils.escapeJava(ex.toString()) + "\"}\n");
				ex.printStackTrace();
      } catch (JSONException ex) {
        e.getChannel().write("{\"exception\": \"" + StringEscapeUtils.escapeJava(ex.toString()) + "\"}\n");
				ex.printStackTrace();
      } catch (ParseException ex) {
        e.getChannel().write("{\"exception\": \"" + StringEscapeUtils.escapeJava(ex.toString()) + "\"}\n");
				ex.printStackTrace();
      }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
      logger.log(Level.WARNING,
                 "Unexpected exception from downstream.",
                 e.getCause());
      e.getChannel().close();
    }
  }

  public static void main(String[] args) throws IOException {
		int ngram = 3;
		if ( args.length < 1 ) {
			System.err.println("usage: java -Dngram=N " + SearcherDaemon.class + " <INDEX_DIR> <PORT_NUMBER>");
		}
		{
			String s;
			if ( (s = System.getProperty("ngram")) != null ) {
				ngram = Integer.parseInt(s);
			}
		}
		int port = 8080;
		String dir = args[0];
		if ( args.length >= 2 ) {
			try {
				port = Integer.parseInt(args[1]);
			} catch ( NumberFormatException e ) {
				// do nothing
			}
		}
		new SearcherDaemon(new InetSocketAddress(port),
											 dir,
											 new QueryParser(Version.LUCENE_35, "added", new SimpleNGramAnalyzer(ngram))).run();
  }
}

/*
 * Local variables:
 * tab-width: 2
 * c-basic-offset: 2
 * indent-tabs-mode: t
 * End:
 */
