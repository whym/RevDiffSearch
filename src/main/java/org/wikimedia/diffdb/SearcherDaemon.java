package org.wikimedia.diffdb;

import java.io.File;
import java.io.IOException;
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
import java.util.regex.Pattern;
import java.util.regex.Matcher;

import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.search.TopDocsCollector;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;
import org.apache.lucene.document.Document;

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
	private static final Map<String, Pattern> collapsePatterns;
	static {
		collapsePatterns = new TreeMap<String, Pattern>();
		collapsePatterns.put("day",    Pattern.compile("\\d\\d\\d\\d-\\d\\d-\\d\\d"));
		collapsePatterns.put("month",  Pattern.compile("\\d\\d\\d\\d-\\d\\d"));
	}
	private final InetSocketAddress address;
	private final IndexSearcher searcher;
	private final QueryParser parser;

	public SearcherDaemon(InetSocketAddress address, final IndexSearcher searcher, final QueryParser parser) {
		this.address = address;
		this.searcher = searcher;
		this.parser = parser;
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

					// pipeline.addLast("decoder", new HttpRequestDecoder());
					// pipeline.addLast("encoder", new HttpResponseEncoder());
					// pipeline.addLast("deflater", new HttpContentCompressor());
					// pipeline.addLast("handler", new HttpRequestHandler());

          return pipeline;
        }
      });
    
    bootstrap.bind(address);
	}

	private static class MyCollector extends Collector {
		private final IndexSearcher searcher;
		private final String query;
		private final BitSet hits;
		private int docBase;
		public MyCollector(IndexSearcher searcher, String query) {
			this.query = "";
			this.searcher = searcher;
			this.hits = new BitSet(searcher.getIndexReader().maxDoc());
		}
		public boolean acceptsDocsOutOfOrder() {
			return true;
		}
		public void setNextReader(IndexReader reader, int docBase) {
			this.docBase = docBase;
		}
		public void setScorer(Scorer scorer) {
		}
		public void collect(int doc) {
			// TODO: it must work for other fileds than 'added' and 'removed'
			try {
				if ( ( StringEscapeUtils.unescapeJava(searcher.doc(doc).getField("added").stringValue()).indexOf(this.query) < 0
							 && StringEscapeUtils.unescapeJava(searcher.doc(doc).getField("removed").stringValue()).indexOf(this.query) < 0  ) ) {
					this.hits.clear(doc + this.docBase);
				} else {
					this.hits.set(doc + this.docBase);
				}
				System.err.println("check " + doc);
			} catch (IOException e) {
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
			System.err.println("collapse " + hits + fields + pattern);//!
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
				System.err.println(key + array);//!
				ls.put(array);
			}
			System.err.println(map);//!
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
      // very tentative format of query: max-results<tab>query
      String q = ((ChannelBuffer)e.getMessage()).toString(Charset.defaultCharset());
			try {
				JSONObject json = new JSONObject(q);
				System.err.println("received query: " + json.toString(2));//!
				String query   = json.getString("q");						 // to be fed to QueryParser
				String hitsper = json.optString("collapse_hits", "no"); // no or day or month
				int maxrevs    = json.optInt("max_revs", 100);
				double version = json.optDouble("version", 0.1);		// 
				JSONArray fields_ = json.optJSONArray("fields"); // the fields to be given in the output. if empty, only number of hits will be emitted
				MyCollector collector = new MyCollector(this.searcher, query);
				this.searcher.search(this.parser.parse(query), collector);
				BitSet hits = collector.getHits();
				JSONObject ret = new JSONObject();

				ret.put("hits_all", hits.cardinality());

				List<String> fields = new ArrayList<String>();
				if ( fields_ == null ) {
					String f = json.optString("fields");
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
				System.err.println("f " + fields + fields_);//!
				Pattern cpattern;
				if ( "no".equals(hitsper) || (cpattern = collapsePatterns.get(hitsper)) == null ) {
					if ( fields.size() > 0 ) {
						ret.put("hits", writeHits(hits, fields));
					}
				} else {
					JSONArray hitEntries = writeCollapsedHitsByTimestamp(hits, fields, cpattern);
					if ( fields.size() > 0 ) {
						ret.put("hits", hitEntries);
					} else {
						JSONArray hits_ = new JSONArray();
						System.err.println("hits " + hitEntries);
						for ( int i = 0; i < hitEntries.length(); ++i ) {
							hits_.put(new JSONArray(new Object[]{
										hitEntries.getJSONArray(i).getString(0),
										hitEntries.getJSONArray(i).getJSONArray(1).length(),
									}));
						}
						ret.put("hits", hits_);
					}
				}
				e.getChannel().write(ret.toString());
      } catch (Exception ex) {
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

  public static void main(String[] args) throws Exception {
		new SearcherDaemon(new InetSocketAddress(8080),
											 new IndexSearcher(FSDirectory.open(new File("index"))),
											 new QueryParser(Version.LUCENE_34, "added", new SimpleNGramAnalyzer(3))).run();
  }
}

/*
 * Local variables:
 * tab-width: 2
 * c-basic-offset: 2
 * indent-tabs-mode: t
 * End:
 */
