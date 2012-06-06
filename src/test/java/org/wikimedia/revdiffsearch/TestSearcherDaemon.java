package org.wikimedia.revdiffsearch;
import org.junit.*;
import java.io.*;
import java.util.*;
import java.util.logging.*;
import java.net.*;
import org.apache.lucene.index.*;
import org.apache.lucene.analysis.*;
import org.apache.lucene.store.*;
import org.apache.lucene.search.*;
import org.apache.lucene.document.*;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.util.Version;
import org.json.JSONObject;
import org.json.JSONArray;
import org.json.JSONException;

import static org.junit.Assert.*;

public class TestSearcherDaemon {
  private static File newTempFile(String content) throws IOException {
    File file = File.createTempFile("indexer", ".txt");
    file.deleteOnExit();
    BufferedWriter writer = new BufferedWriter(new FileWriter(file));
    writer.write(content);
    writer.close();
    return file;
  }

  private static String readAll(Reader reader) throws IOException {
    StringBuffer buff = new StringBuffer();
    char[] buffb = new char[4096];
    int len;
    while ( (len = reader.read(buffb)) > 0 ) {
      buff.append(buffb, 0, len);
    }
    return buff.toString();
  }


  private static JSONObject retrieve(InetSocketAddress address, JSONObject query) throws IOException, JSONException {
    Socket socket = new Socket(address.getAddress(), address.getPort());
    BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
    writer.write(query.toString());
    writer.write("\n");
    writer.flush();
    socket.shutdownOutput();
    BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
    JSONObject ret = new JSONObject(readAll(reader));
    writer.close();
    reader.close();
    socket.close();
    return ret;
  }

  private static InetSocketAddress findFreeAddress() throws IOException {
    ServerSocket socket = new ServerSocket(0);
    InetSocketAddress address = new InetSocketAddress(socket.getLocalPort());
    socket.close();
    return address;
  }

  private static boolean waitUntilPrepared(InetSocketAddress address, long limit) throws IOException, InterruptedException {
    long start = System.currentTimeMillis();
    while ( true) {
      try {
        Socket sock = new Socket(address.getAddress(), address.getPort());
        if ( sock.isConnected() ) {
          sock.close();
          return true;
        }
      } catch ( ConnectException e ) {
        Thread.sleep(limit / 20);
        if ( System.currentTimeMillis() - start > limit ) {
          return false;
        }
      }
    }
  }

  @Before public void setup() {
    Logger.getLogger(SearcherDaemon.class.getName()).setLevel(Level.WARNING);
    Logger.getLogger(Indexer.class.getName()).setLevel(Level.WARNING);
  }

  @Test public void smallDocuments() throws IOException, JSONException, InterruptedException {
    Directory dir = new RAMDirectory();
    IndexWriter writer = new IndexWriter(dir,
                                         new IndexWriterConfig(Version.LUCENE_36,
                                                               new SimpleNGramAnalyzer(3)));
    Indexer indexer = new Indexer(writer, 2, 2, 100);
    indexer.indexDocuments(newTempFile("233192	10	0	'Accessiblecomputing'	980043141	u'*'	False	99	u'RoseParks'	0:1:u'This subject covers\\n\\n* AssistiveTechnology\\n\\n* AccessibleSoftware\\n\\n* AccessibleWeb\\n\\n* LegalIssuesInAccessibleComputing\\n\\n'\n" +
                                       "18201	12	0	'Anarchism'	1014649222	u'Automated conversion'	True	None	u'Conversion script'	9230:1:u'[[talk:Anarchism|'	9252:1:u']]'	9260:1:u'[[Anarchism'	9276:1:u'|/Todo]]'	9292:1:u'talk:'	9304:-1:u'/Talk'	9464:1:u'\\n'\n"));
    indexer.finish();

    IndexSearcher searcher = new IndexSearcher(IndexReader.open(dir));
    InetSocketAddress address = findFreeAddress();
    new Thread(new SearcherDaemon(address, searcher, new QueryParser(Version.LUCENE_36, "added", new SimpleNGramAnalyzer(3)))).start();

    assertTrue(waitUntilPrepared(address, 1000L));

    // query "/Todo" and receive rev_ids
    {
      JSONObject q = new JSONObject();
      q.put("q", "/Todo");
      q.put("fields", "rev_id");
      JSONObject json = retrieve(address, q);
      System.err.println(json);//!
      assertEquals(1, json.getInt("hits_all"));
      assertEquals(18201, json.getJSONArray("hits").getJSONArray(0).getInt(0));
    }
    // query "* Legal" and receive rev_ids and timestamps
    {
      JSONObject q = new JSONObject();
      q.put("q", "\"* Legal\"");
      q.put("fields", new JSONArray(new String[]{"rev_id", "timestamp"}));
      JSONObject json = retrieve(address, q);
      assertEquals(1, json.getInt("hits_all"));
      assertEquals(233192,
                   json.getJSONArray("hits").getJSONArray(0).getInt(0));
      assertEquals("2001-01-21T02:12:21Z",
                   json.getJSONArray("hits").getJSONArray(0).getString(1));
    }
  }

  @Test public void smallDocumentsComplexQueries() throws IOException, JSONException, InterruptedException {
    Directory dir = new RAMDirectory();
    IndexWriter writer = new IndexWriter(dir,
                                         new IndexWriterConfig(Version.LUCENE_36,
                                                               new SimpleNGramAnalyzer(3)));
    Indexer indexer = new Indexer(writer, 2, 2, 100);
    indexer.indexDocuments(newTempFile("233192	10	0	'Accessiblecomputing'	980043141	u'*'	False	99	u'RoseParks'	0:1:u'This subject covers\\n\\n* AssistiveTechnology\\n\\n* AccessibleSoftware\\n\\n* AccessibleWeb\\n\\n* LegalIssuesInAccessibleComputing\\n\\n'\n" +
                                       "18201	12	0	'Anarchism'	1014649222	u'Automated conversion'	True	None	u'Conversion script'	9230:1:u'[[talk:Anarchism|'	9252:1:u']]'	9260:1:u'[[Anarchism'	9276:1:u'|/Todo]]'	9292:1:u'talk:'	9304:-1:u'/Talk'	9464:1:u'\\n'\n"));
    indexer.finish();

    IndexSearcher searcher = new IndexSearcher(IndexReader.open(dir));
    InetSocketAddress address = findFreeAddress();
    new Thread(new SearcherDaemon(address, searcher, new QueryParser(Version.LUCENE_36, "added", new SimpleNGramAnalyzer(3)))).start();

    assertTrue(waitUntilPrepared(address, 1000L));

    // query namespace:0 and page_id:12 and receive rev_ids
    {
      JSONObject q = new JSONObject();
      q.put("q", "namespace:0 page_id:12");
      q.put("fields", "rev_id");
      JSONObject json = retrieve(address, q);
      System.err.println(json);//!
      assertEquals(1, json.getInt("hits_all"));
      assertEquals(18201, json.getJSONArray("hits").getJSONArray(0).getInt(0));
    }
  }

  @Test public void smallDocumentsWithCollapsedHits() throws IOException, JSONException, InterruptedException {
    Directory dir = new RAMDirectory();
    IndexWriter writer = new IndexWriter(dir,
                                         new IndexWriterConfig(Version.LUCENE_36,
                                                               new SimpleNGramAnalyzer(1)));
    Indexer indexer = new Indexer(writer, 2, 10, 100);
    indexer.indexDocuments(newTempFile("233192	10	0	'Accessiblecomputing'	980043141	u'*'	False	99	u'RoseParks'	0:1:u'This subject covers\\n\\n* AssistiveTechnology\\n\\n* AccessibleSoftware\\n\\n* AccessibleWeb\\n\\n* LegalIssuesInAccessibleComputing\\n\\n'\n" +
                                       "18201	12	0	'Anarchism'	1014649222	u'Automated conversion'	True	None	u'Conversion script'	9230:1:u'[[talk:Anarchism|'	9252:1:u']]'	9260:1:u'[[Anarchism'	9276:1:u'|/Todo]]'	9292:1:u'talk:'	9304:-1:u'/Talk'	9464:1:u'\\n'\n" +
                                       "18210	12	0	'Anarchism'	1014749333	u'Automated conversion'	True	None	u'Conversion script'	9230:1:u'[[talk:Anarchism|'	9252:1:u']]'	9260:1:u'[[Anarchism'	9276:1:u'|/Todo]]'	9292:1:u'talk:'	9304:-1:u'/Talk'	9464:1:u'\\n'\n"));
    indexer.finish();

    IndexSearcher searcher = new IndexSearcher(IndexReader.open(dir));
    InetSocketAddress address = findFreeAddress();
    new Thread(new SearcherDaemon(address, searcher, new QueryParser(Version.LUCENE_36, "added", new SimpleNGramAnalyzer(1)))).start();

    assertTrue(waitUntilPrepared(address, 1000L));

    // query "/Todo" and receive rev_ids monthly
    {
      JSONObject q = new JSONObject();
      q.put("q", "/Todo");
      q.put("fields", "rev_id");
      q.put("collapse_hits", "month");
      JSONObject json = retrieve(address, q);
      System.err.println(json);//!
      assertEquals(2, json.getInt("hits_all"));
      assertEquals("2002-02", json.getJSONArray("hits").getJSONArray(0).getString(0));
      assertEquals(18201,     json.getJSONArray("hits").getJSONArray(0).getJSONArray(1).getJSONArray(0).getInt(0));
      assertEquals(18210,     json.getJSONArray("hits").getJSONArray(0).getJSONArray(1).getJSONArray(1).getInt(0));
    }
    // query "/Todo" and receive rev_ids daily
    {
      JSONObject q = new JSONObject();
      q.put("q", "/Todo");
      q.put("fields", "rev_id");
      q.put("collapse_hits", "day");
      JSONObject json = retrieve(address, q);
      assertEquals(2, json.getInt("hits_all"));
      System.err.println(json);//!
      assertEquals("2002-02-25", json.getJSONArray("hits").getJSONArray(0).getString(0));
      assertEquals("2002-02-26", json.getJSONArray("hits").getJSONArray(1).getString(0));
      assertEquals(18201,        json.getJSONArray("hits").getJSONArray(0).getJSONArray(1).getJSONArray(0).getInt(0));
      assertEquals(18210,        json.getJSONArray("hits").getJSONArray(1).getJSONArray(1).getJSONArray(0).getInt(0));
    }
    // query "/Todo" and receive daily counts
    {
      JSONObject q = new JSONObject();
      q.put("q", "/Todo");
      q.put("collapse_hits", "day");
      JSONObject json = retrieve(address, q);
      assertEquals(2, json.getInt("hits_all"));
      System.err.println(json);//!
      assertEquals("2002-02-25", json.getJSONArray("hits").getJSONArray(0).getString(0));
      assertEquals("2002-02-26", json.getJSONArray("hits").getJSONArray(1).getString(0));
      assertEquals(1,            json.getJSONArray("hits").getJSONArray(0).getInt(1));
      assertEquals(1,            json.getJSONArray("hits").getJSONArray(1).getInt(1));
    }
  }

  private static void phraseQuery(Analyzer analyzer) throws IOException, JSONException, InterruptedException {
    Directory dir = new RAMDirectory();
    IndexWriter writer = new IndexWriter(dir,
                                         new IndexWriterConfig(Version.LUCENE_36,
                                                               analyzer));
    Indexer indexer = new Indexer(writer, 2, 2, 100);
    indexer.indexDocuments(newTempFile("233192	10	0	'Title1'	980043141	u'comment1'	False	99	u'uname1'	0:1:u' subject cover \n'q" +
                                       "18201	12	0	'Title2'	1014649222	u'comment2'	True	None	u'uname2'	9230:1:u'This covers subject to'\n"));
    indexer.finish();

    IndexSearcher searcher = new IndexSearcher(IndexReader.open(dir));
    InetSocketAddress address = findFreeAddress();
    new Thread(new SearcherDaemon(address, searcher, new QueryParser(Version.LUCENE_36, "added", analyzer))).start();

    assertTrue(waitUntilPrepared(address, 1000L));

    // query "subject cover" without quotes and receive rev_ids
    {
      JSONObject q = new JSONObject();
      q.put("q", "subject cover");
      q.put("fields", "rev_id");
      JSONObject json = retrieve(address, q);
      System.err.println(json);//!
      assertEquals(2, json.getInt("hits_all"));
    }
    // query "subject cover" with quotes and receive rev_ids
    {
      // TODO: this must pass if we want to user longer N-grams
      JSONObject q = new JSONObject();
      q.put("q", "\"subject cover\"");
      q.put("fields", "rev_id");
      JSONObject json = retrieve(address, q);
      System.err.println(json);//!
      assertEquals(1, json.getInt("hits_all"));
      assertEquals(233192, json.getJSONArray("hits").getJSONArray(0).getInt(0));
    }
  }

  @Test public void phraseQueryWithSimpleNGramAnalysis() throws IOException, JSONException, InterruptedException {
    phraseQuery(new SimpleNGramAnalyzer(3));
  }

  // TODO: NGramTokenizer needs to be changed similarly to HashedNGramTokenzer to be consistent with the index word positions
  // @Test public void phraseQueryWithNGramAnalysis() throws IOException, JSONException, InterruptedException {
  //   phraseQuery(new NGramAnalyzer(1, 2));
  // }

  @Test public void phraseQueryWithHashedNGramAnalysis() throws IOException, JSONException, InterruptedException {
    phraseQuery(new HashedNGramAnalyzer(1, 2, 9876));
  }

  @Test public void operatorQueryOR() throws IOException, JSONException, InterruptedException {
    Directory dir = new RAMDirectory();
    IndexWriter writer = new IndexWriter(dir,
                                         new IndexWriterConfig(Version.LUCENE_36,
                                                               new SimpleNGramAnalyzer(3)));
    Indexer indexer = new Indexer(writer, 2, 2, 100);
    indexer.indexDocuments(newTempFile("233192	10	0	'Accessiblecomputing'	980043141	u'*'	False	99	u'RoseParks'	0:1:u'This subject covers\\n\\n* AssistiveTechnology\\n\\n* AccessibleSoftware\\n\\n* AccessibleWeb\\n\\n* LegalIssuesInAccessibleComputing\\n\\n'\n" +
                                       "18201	12	0	'Anarchism'	1014649222	u'Automated conversion'	True	None	u'Conversion script'	9230:1:u'[[talk:Anarchism|'	9252:1:u']]'	9260:1:u'[[Anarchism'	9276:1:u'|/Todo]]'	9292:1:u'talk:'	9304:-1:u'/Talk'	9464:1:u'\\n'\n" +
                                       "12345	10	0	'Accessiblecomputing'	1980043141	u'*'	False	99	u'Automated conversion'	0:1:u'[[fr:ABC]]\\n'"));
    indexer.finish();

    IndexSearcher searcher = new IndexSearcher(IndexReader.open(dir));
    InetSocketAddress address = findFreeAddress();
    new Thread(new SearcherDaemon(address, searcher, new QueryParser(Version.LUCENE_36, "added", new SimpleNGramAnalyzer(3)))).start();

    assertTrue(waitUntilPrepared(address, 1000L));

    {
      JSONObject q = new JSONObject();
      q.put("q", "rev_id:12345 OR page_id:12");
      q.put("fields", "rev_id");
      JSONObject json = retrieve(address, q);
      System.err.println(json);//!
      assertEquals(2, json.getInt("hits_all"));
      Set<Integer> system = new HashSet<Integer>();
      system.add(json.getJSONArray("hits").getJSONArray(0).getInt(0));
      system.add(json.getJSONArray("hits").getJSONArray(1).getInt(0));
      assertEquals(new HashSet<Integer>(Arrays.asList(new Integer[]{12345, 18201})),
                   system);
    }
  }

  @Test public void operatorQueryWildcard() throws IOException, JSONException, InterruptedException {
    Directory dir = new RAMDirectory();
    IndexWriter writer = new IndexWriter(dir,
                                         new IndexWriterConfig(Version.LUCENE_36,
                                                               new SimpleNGramAnalyzer(3)));
    Indexer indexer = new Indexer(writer, 2, 2, 100);
    indexer.indexDocuments(newTempFile("233192	10	0	'Accessiblecomputing'	980043141	u'*'	False	99	u'RoseParks'	0:1:u'This subject covers\\n\\n* AssistiveTechnology\\n\\n* AccessibleSoftware\\n\\n* AccessibleWeb\\n\\n* LegalIssuesInAccessibleComputing\\n\\n'\n" +
                                       "18201	12	0	'Anarchism'	1014649222	u'Automated conversion'	True	None	u'Conversion script'	9230:1:u'[[talk:Anarchism|'	9252:1:u']]'	9260:1:u'[[Anarchism'	9276:1:u'|/Todo]]'	9292:1:u'talk:'	9304:-1:u'/Talk'	9464:1:u'\\n'\n" +
                                       "12345	10	0	'Accessiblecomputing'	1980043141	u'*'	False	99	u'Automated conversion'	0:1:u'[[fr:ABC]]\\n'"));
    indexer.finish();

    IndexSearcher searcher = new IndexSearcher(IndexReader.open(dir));
    InetSocketAddress address = findFreeAddress();
    new Thread(new SearcherDaemon(address, searcher, new QueryParser(Version.LUCENE_36, "added", new SimpleNGramAnalyzer(3)))).start();

    assertTrue(waitUntilPrepared(address, 1000L));

    {
      JSONObject q = new JSONObject();
      q.put("q", "namespace:0 AND NOT \\[\\[?");
      q.put("fields", "rev_id");
      JSONObject json = retrieve(address, q);
      System.err.println(json);//!
      assertEquals(1, json.getInt("hits_all"));
      Set<Integer> system = new HashSet<Integer>();
      assertEquals(json.getJSONArray("hits").getJSONArray(0).getInt(0), 233192);
    }
    {
      JSONObject q = new JSONObject();
      q.put("q", "\\[\\[?");
      q.put("fields", "rev_id");
      JSONObject json = retrieve(address, q);
      System.err.println(json);//!
      assertEquals(2, json.getInt("hits_all"));
    }
  }
}

/*
 * Local variables:
 * tab-width: 2
 * c-basic-offset: 2
 * indent-tabs-mode: nil
 * End:
 */

