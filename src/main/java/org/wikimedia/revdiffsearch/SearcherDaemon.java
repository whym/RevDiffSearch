package org.wikimedia.revdiffsearch;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.TreeMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Arrays;
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

import io.netty.util.CharsetUtil;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponseEncoder;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.QueryStringDecoder;
import io.netty.handler.codec.DelimiterBasedFrameDecoder;
import io.netty.handler.codec.Delimiters;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;

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
	private final boolean http;

	public SearcherDaemon(InetSocketAddress address, String dir, final QueryParser parser, int threads, boolean http) throws IOException {
		this(address, new IndexSearcher(DirectoryReader.open(FSDirectory.open(new File(dir)))), parser, threads, http);
	}

	public SearcherDaemon(InetSocketAddress address, IndexSearcher searcher, final QueryParser parser, int threads, boolean http) throws IOException {
		this.address = address;
		this.searcher = searcher;
		this.parser = parser;
		this.parser.setDefaultOperator(QueryParser.AND_OPERATOR);
		this.startTimeMillis = System.currentTimeMillis();
		this.threads = threads;
		this.http = http;
		logger.info("given the index containing " + searcher.getIndexReader().maxDoc() + " entries");
	}
	
	@Override public void run() {
		EventLoopGroup bossGroup = new NioEventLoopGroup();
		EventLoopGroup workerGroup = new NioEventLoopGroup();
		try {
			ServerBootstrap bootstrap = new ServerBootstrap();
			
			bootstrap.group(bossGroup, workerGroup)
				.channel(NioServerSocketChannel.class)
				.handler(new LoggingHandler(LogLevel.INFO))
				.childHandler(new ChannelInitializer<SocketChannel>() {
						@Override
							public void initChannel(SocketChannel ch) throws Exception {
							ChannelPipeline pipeline = ch.pipeline();
							if ( http ) {
								pipeline.addLast("decoder", new HttpRequestDecoder());
								pipeline.addLast("encoder", new HttpResponseEncoder());
								pipeline.addLast("handler", new SearcherHandler(searcher, parser, threads));
							} else {
								pipeline.addLast("framer", new DelimiterBasedFrameDecoder(RevDiffSearchUtils.getProperty("maxQueryLength", 100000), Delimiters.lineDelimiter()));
								pipeline.addLast("encoder", new StringEncoder(CharsetUtil.UTF_8));
								pipeline.addLast("decoder", new StringDecoder(CharsetUtil.UTF_8));
								pipeline.addLast("handler", new SearcherHandler(searcher, parser, threads));
							}
        }
      });
    
			ChannelFuture f = bootstrap.bind(this.address).sync();
			logger.info("SearcherDaemon is launched in " + DurationFormatUtils.formatDurationHMS(System.currentTimeMillis() - this.startTimeMillis) + " at " + this.address);
			f.channel().closeFuture().sync();
		} catch (InterruptedException e) {
			logger.warning("interrupted");
		} finally {
			bossGroup.shutdownGracefully();
			workerGroup.shutdownGracefully();
		}
	}

  public static class SearcherHandler extends SimpleChannelInboundHandler<Object> {
    private final IndexSearcher searcher;
    private final QueryParser parser;
		private final int threads;

    public SearcherHandler(IndexSearcher searcher, QueryParser parser, int threads) {
      this.searcher = searcher;
      this.parser = parser;
			this.threads = threads;
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

		private String stror(String x, String y) {
			return x == null ? y : x;
		}

		@Override
			public void channelRead0(ChannelHandlerContext ctx, Object msg) {
			final long processingStartMillis = System.currentTimeMillis();
			try {
				String					query = "";	 // to be fed to QueryParser
				String					hitsper = "no"; // no or day or month
				int							maxrevs = 1000; // 
				String					version = "0.1"; // 
				String          format = "json";
				List<String> fields = new ArrayList<String>();
				JSONObject qobj = new JSONObject();
				String callback = "";
				if ( msg instanceof String ) {
					JSONArray				fields_; // the fields to be given in the output. if empty, only number of hits will be emitted
					String qstr = (String)msg;
					qobj = new JSONObject(qstr);
					logger.info("received query: " + qstr + " at " + ctx);
					logger.info("received query: " + qstr + " at " + ctx);
					logger.info("received query (JSON): " + qobj.toString(2) + " at " + ctx);
					query   = qobj.getString("q");
					hitsper = qobj.optString("collapse_hits", hitsper);
					maxrevs = qobj.optInt("max_revs", maxrevs);
					version = qobj.optString("version", version);
					fields_ = qobj.optJSONArray("fields");
					format  = qobj.optString("format", format);
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
				} else if ( msg instanceof HttpRequest ) {
					HttpRequest request = (HttpRequest)msg;
					logger.info("HTTP" + request.getUri());
					QueryStringDecoder queryStringDecoder = new QueryStringDecoder(request.getUri());
					Map<String, List<String>> params = queryStringDecoder.parameters();
					if (!params.isEmpty()) {
						Map<String,String> map = new HashMap<String,String>();
						for (Map.Entry<String, List<String>> p: params.entrySet()) {
							String key = p.getKey();
							List<String> vals = p.getValue();
							for (String val : vals) {
								map.put(key, val);
							}
						}
						logger.info("HTTP parameters " + map);
						query   = map.get("q");
						hitsper = stror(map.get("collapse_hits"), hitsper);
						maxrevs = (int)ClassUtils.parseValue(stror(map.get("max_revs"), "" + maxrevs), int.class);
						version = stror(map.get("version"), "" + version);
						fields  = map.get("fields") != null ? Arrays.asList(map.get("fields").split(",")) : Collections.<String>emptyList();
						format  = stror(map.get("format"), format);
						callback = map.get("callback");
						qobj.put("q", query);
						qobj.put("collapse_hits", hitsper);
						qobj.put("max_revs", maxrevs);
						qobj.put("version", version);
						qobj.put("fields", fields);
						qobj.put("format", format);
					}
				// } else if (msg instanceof HttpContent) {
				// 	HttpContent httpContent = (HttpContent) msg;
				// 	ByteBuf content = httpContent.content();
				// 	//TODO: parse POST request
				// 	if (content.isReadable()) {
				// 	}
				// 	//TODO: parse LastHttpCoontent
				} else {
					logger.warning("unexpected msg: " + msg);
					return;
				}

				//SearchResults collector = new DiffCollector(this.searcher, maxrevs);
				SearchResults collector = new ParallelCollector(this.searcher, maxrevs, Executors.newFixedThreadPool(threads));
				collector.issue(query, this.parser);
				BitSet hits = collector.getHits();
				JSONObject ret = new JSONObject();

				ret.put("hits_all", hits.cardinality());
				ret.put("parsed_query", this.parser.parse(query).toString());

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
				final StringBuffer res = new StringBuffer();
				if ( msg instanceof HttpRequest && format.equals("jsonp") && !"".equals(callback) ) {
					res.append(callback + "(");
					res.append(ret.toString());
					res.append(");");
				} else {
					res.append(ret.toString());
				}
				ChannelFuture f;
				if ( msg instanceof HttpRequest ) {
					FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
					response.content().writeBytes(res.toString().getBytes());
					f = ctx.write(response);
				} else {
					f = ctx.write(res);
				}
				f.addListener(new ChannelFutureListener() {
            public void operationComplete(ChannelFuture future) {
							logger.info("responded in " + DurationFormatUtils.formatDurationHMS(System.currentTimeMillis() - processingStartMillis) + " (" + res.length() + " characters)");
							Channel ch = future.channel();
							ch.close();
							logger.info("connection closed");
            }
        });
      } catch (IOException ex) {
        ctx.write("{\"exception\": \"" + StringEscapeUtils.escapeJava(ex.toString()) + "\"}\n");
				ex.printStackTrace();
      } catch (JSONException ex) {
        ctx.write("{\"exception\": \"" + StringEscapeUtils.escapeJava(ex.toString()) + "\"}\n");
				ex.printStackTrace();
      } catch (ParseException ex) {
        ctx.write("{\"exception\": \"" + StringEscapeUtils.escapeJava(ex.toString()) + "\"}\n");
				ex.printStackTrace();
      } finally {
				ctx.flush();
			}
    }
	}

  public static void main(String[] args) throws IOException {
		if ( args.length < 1 ) {
			System.err.println("usage: java -Dngram=N -DnThreads=N " + SearcherDaemon.class + " <INDEX_DIR> <PORT_NUMBER>");
		}
		int port = 8080;
		int threads = RevDiffSearchUtils.getProperty("nThreads", Runtime.getRuntime().availableProcessors());
		boolean http = RevDiffSearchUtils.getProperty("http", false);
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
											 threads,
											 http).run();
  }
}

/*
 * Local variables:
 * tab-width: 2
 * c-basic-offset: 2
 * indent-tabs-mode: t
 * End:
 */
