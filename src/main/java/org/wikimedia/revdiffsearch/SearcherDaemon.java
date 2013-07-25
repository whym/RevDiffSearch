package org.wikimedia.revdiffsearch;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.net.InetSocketAddress;
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
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.util.Version;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.DirectoryReader;

import org.jboss.netty.util.CharsetUtil;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.codec.string.StringDecoder;
import org.jboss.netty.handler.codec.string.StringEncoder;
import org.jboss.netty.handler.codec.frame.DelimiterBasedFrameDecoder;
import org.jboss.netty.handler.codec.frame.Delimiters;

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
	private final int threads;

	public SearcherDaemon(InetSocketAddress address, String dir, final QueryParser parser, int threads) throws IOException {
		this(address, new IndexSearcher(DirectoryReader.open(FSDirectory.open(new File(dir)))), parser, threads);
	}

	public SearcherDaemon(InetSocketAddress address, IndexSearcher searcher, final QueryParser parser, int threads) throws IOException {
		this.address = address;
		this.searcher = searcher;
		this.parser = parser;
		this.parser.setDefaultOperator(QueryParser.AND_OPERATOR);
		this.startTimeMillis = System.currentTimeMillis();
		this.threads = threads;
		logger.info("given the index containing " + searcher.getIndexReader().maxDoc() + " entries");
	}
	
	@Override public void run() {
    ServerBootstrap bootstrap = new ServerBootstrap
      (new NioServerSocketChannelFactory
       (Executors.newCachedThreadPool(),
        Executors.newCachedThreadPool()));
    
    bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
        @Override
          public ChannelPipeline getPipeline() throws Exception {
          ChannelPipeline pipeline = Channels.pipeline();
					pipeline.addLast("framer", new DelimiterBasedFrameDecoder(RevDiffSearchUtils.getProperty("maxQueryLength", 100000), Delimiters.lineDelimiter()));
					pipeline.addLast("decoder", new StringEncoder(CharsetUtil.UTF_8));
					pipeline.addLast("encoder", new StringDecoder(CharsetUtil.UTF_8));
					pipeline.addLast("handler", new SearcherHandler(searcher, parser, threads));
          return pipeline;
        }
      });
    
    bootstrap.bind(this.address);
		logger.info("SearcherDaemon is launched in " + DurationFormatUtils.formatDurationHMS(System.currentTimeMillis() - this.startTimeMillis) + " at " + this.address);
	}

  public static class SearcherHandler extends SimpleChannelUpstreamHandler {
    private final IndexSearcher searcher;
    private final QueryParser parser;
		private final int threads;

    public SearcherHandler(IndexSearcher searcher, QueryParser parser, int threads) {
      this.searcher = searcher;
      this.parser = parser;
			this.threads = threads;
    }


		@Override	public void handleUpstream(ChannelHandlerContext ctx, ChannelEvent e) throws Exception {
			if (e instanceof ChannelStateEvent) {
				logger.info(e.toString());
			}
			super.handleUpstream(ctx, e);
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
					IndexableField field = doc.getField(f);
					if ( field == null ) {
						array.put("null field: " + f);
					} else {
						array.put(StringEscapeUtils.unescapeJava(field.stringValue()));
					}
				}
				ret.put(array);
			}
			return ret;
		}

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {
			long processingStartMillis = System.currentTimeMillis();
			try {
				String qstr = (String)e.getMessage();
				logger.info("received query: " + qstr + " at " + ctx);
				JSONObject qobj = new JSONObject(qstr);
				logger.info("received query (JSON): " + qobj.toString(2) + " at " + ctx);
				String query   = qobj.getString("q");						 // to be fed to QueryParser
				String hitsper = qobj.optString("collapse_hits", "no"); // no or day or month
				int maxrevs    = qobj.optInt("max_revs", 1000);
				double version = qobj.optDouble("version", 0.1);		// 
				JSONArray fields_ = qobj.optJSONArray("fields"); // the fields to be given in the output. if empty, only number of hits will be emitted

				//SearchResults collector = new DiffCollector(this.searcher, maxrevs);
				SearchResults collector = new ParallelCollector(this.searcher, maxrevs, Executors.newFixedThreadPool(threads));
				collector.issue(query, this.parser);
				BitSet hits = collector.getHits();
				JSONObject ret = new JSONObject();

				ret.put("hits_all", hits.cardinality());
				ret.put("parsed_query", this.parser.parse(query).toString());

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
				ret.put("debug_unchecked", collector.getNumberOfSkippedDocuments());
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
				ChannelFuture f = e.getChannel().write(str);
				logger.info("responded in " + DurationFormatUtils.formatDurationHMS(System.currentTimeMillis() - processingStartMillis) + " (" + str.length() + " characters)");
				f.addListener(new ChannelFutureListener() {
            public void operationComplete(ChannelFuture future) {
							Channel ch = future.getChannel();
							ch.close();
							logger.info("connection closed");
            }
        });
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
			System.err.println("usage: java -Dngram=N -DnThreads=N " + SearcherDaemon.class + " <INDEX_DIR> <PORT_NUMBER>");
		}
		int port = 8080;
		int threads = RevDiffSearchUtils.getProperty("nThreads", Runtime.getRuntime().availableProcessors());
		String dir = args[0];
		if ( args.length >= 2 ) {
			try {
				port = Integer.parseInt(args[1]);
			} catch ( NumberFormatException e ) {
				// do nothing
			}
		}
		if ( args.length >= 3 ) {
			try {
				threads = Integer.parseInt(args[2]);
			} catch ( NumberFormatException e ) {
				// do nothing
			}
		}
		logger.info("using " + threads + " threads");
		new SearcherDaemon(new InetSocketAddress(port),
											 dir,
											 new QueryParser(Version.LUCENE_44, "added", RevDiffSearchUtils.getAnalyzer()),
											 threads).run();
  }
}

/*
 * Local variables:
 * tab-width: 2
 * c-basic-offset: 2
 * indent-tabs-mode: t
 * End:
 */
