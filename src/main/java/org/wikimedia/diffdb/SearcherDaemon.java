package org.wikimedia.diffdb;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.BitSet;

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

public class SearcherDaemon implements Runnable {
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
		private final BitSet bits;
		private int docBase;
		public MyCollector(IndexSearcher searcher, String query) {
			this.query = "";
			this.searcher = searcher;
			this.bits = new BitSet(searcher.getIndexReader().maxDoc());
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
					this.bits.clear(doc + this.docBase);
				} else {
					this.bits.set(doc + this.docBase);
				}
			} catch (IOException e) {
			}
		}
		public BitSet getHits() {
			return this.bits;
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

		// private Map<String,JSONArray> collapse_hits(Iterable<ScoreDoc> docs) {
		// }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {
      // very tentative format of query: max-results<tab>query
      String q = ((ChannelBuffer)e.getMessage()).toString(Charset.defaultCharset());
			try {
				JSONObject json = new JSONObject(q);
				System.err.println("received: " + json.toString(2));//!
				String query   = json.getString("q");						 // to be fed to QueryParser
				String hitsper = json.optString("collapse_hits", "no"); // TODO: no or day or week or month
				int maxrevs    = json.optInt("max_revs", 100);
				double version = json.optDouble("version", 0.1);		// 
				JSONArray fields_ = json.optJSONArray("fields"); // if null, rev_id only
				MyCollector collector = new MyCollector(this.searcher, query);
				this.searcher.search(this.parser.parse(query), collector);
				BitSet bits = collector.getHits();
				JSONObject ret = new JSONObject();
				ret.put("hits_all", bits.cardinality());

				List<String> fields = new ArrayList<String>();
				if ( fields_ == null ) {
					fields = Collections.singletonList("rev_id");
				} else {
					for ( int i = 0; i < fields_.length(); ++i ) {
						fields.add(fields_.getString(i));
					}
				}
				// collect field values
				JSONArray hits = new JSONArray();
				for(int i = bits.nextSetBit(0); i >= 0; i = bits.nextSetBit(i+1) ) {
					Document doc = this.searcher.doc(i);
					JSONArray array = new JSONArray();
					for ( String f: fields ) {
						array.put(StringEscapeUtils.unescapeJava(doc.getField(f).stringValue()));
					}
					hits.put(array);
				}
				ret.put("hits", hits);
				e.getChannel().write(ret.toString());
      } catch (Exception ex) {
        e.getChannel().write("{\"exception\": \"" + StringEscapeUtils.escapeJava(ex.toString()) + "\"}\n");
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
