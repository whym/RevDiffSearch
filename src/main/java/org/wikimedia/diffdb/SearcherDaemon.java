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
import java.util.Date;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.time.DurationFormatUtils;
import org.apache.lucene.queryParser.QueryParser;
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
    
    bootstrap.bind(address);
		logger.info("SearcherDaemon is launched in " + DurationFormatUtils.formatDurationHMS(System.currentTimeMillis() - this.startTimeMillis));
	}

	private static class MyCollector extends Collector {
		private final IndexSearcher searcher;
		private final String query;
		private final BitSet hits;
		private int docBase;
		private int maxRevs;
		public MyCollector(IndexSearcher searcher, String query, int max) {
			this.query = query;
			this.searcher = searcher;
			this.hits = new BitSet(searcher.getIndexReader().maxDoc());
			this.maxRevs = max;
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
			doc += this.docBase;
			// TODO: it must work for other fileds than 'added' and 'removed'
			if ( this.hits.cardinality() >= this.maxRevs ) {
				this.hits.clear(doc);
				return;
			}					
			try {
				if ( ( searcher.doc(doc).getField("added").stringValue().indexOf(this.query) >= 0
							 || searcher.doc(doc).getField("removed").stringValue().indexOf(this.query) >= 0  ) ) {
					this.hits.set(doc);
				} else {
					this.hits.clear(doc);
				}
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
				this.searcher.search(this.parser.parse(query), collector);
				BitSet hits = collector.getHits();
				logger.info("finished searching: " + hits);
				JSONObject ret = new JSONObject();

				ret.put("hits_all", hits.cardinality());

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
				logger.info("hits: " + ret.optJSONArray("hits"));//!
				ret.put("q", query);
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
		if ( args.length < 1 ) {
			System.err.println("usage: java " + SearcherDaemon.class + " <INDEX_DIR> <PORT_NUMBER>");
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
