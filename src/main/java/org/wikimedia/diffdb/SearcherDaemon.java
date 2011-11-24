package org.wikimedia.diffdb;

import java.io.File;
import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

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

public class SearcherDaemon {
  public static class SearcherHandler extends SimpleChannelUpstreamHandler {
    private static final Logger logger = Logger.getLogger(SearcherHandler.class.getName());
    private final IndexSearcher searcher;
    private final QueryParser parser;

    public SearcherHandler(IndexSearcher searcher, QueryParser parser) {
      this.searcher = searcher;
      this.parser = parser;
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {
      // very tentative format of query: max-results<tab>query
      String[] q = ((ChannelBuffer)e.getMessage()).toString(Charset.defaultCharset()).split("\t");
      int n = Integer.parseInt(StringEscapeUtils.unescapeJava(q[0]));
      try {
        String query = StringEscapeUtils.unescapeJava(q[1]);
        TopDocs results = this.searcher.search(this.parser.parse(query), n);
        e.getChannel().write("" + results.totalHits + "\n");
        for ( ScoreDoc doc: results.scoreDocs ) {
          e.getChannel().write("" + this.searcher.doc(doc.doc).getFields() + "\n");
        }
      } catch (Exception ex) {
        e.getChannel().write("exception: " + ex.toString());
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
    ServerBootstrap bootstrap = new ServerBootstrap
      (new NioServerSocketChannelFactory
       (Executors.newCachedThreadPool(),
        Executors.newCachedThreadPool()));
    
    bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
        @Override
          public ChannelPipeline getPipeline() throws Exception {
          ChannelPipeline pipeline = Channels.pipeline(new SearcherHandler(new IndexSearcher(FSDirectory.open(new File("index"))),
                                                                           new QueryParser(Version.LUCENE_34, "added", new SimpleNGramAnalyzer(3))));
          Charset charset = Charset.forName("UTF-8");
          pipeline.addLast("stringDecoder", new StringDecoder(charset));
          pipeline.addLast("stringEncoder", new StringEncoder(charset));
          return pipeline;
        }
      });
    
    bootstrap.bind(new InetSocketAddress(8080));
  }
}

/*
 * Local variables:
 * tab-width: 2
 * c-basic-offset: 2
 * indent-tabs-mode: t
 * End:
 */
