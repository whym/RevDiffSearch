package org.wikimedia.diffdb;
import org.junit.*;
import java.io.*;
import java.util.*;
import java.net.*;
import org.apache.lucene.index.*;
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

  @Test public void smallDocuments() throws IOException, JSONException, InterruptedException {
    Directory dir = new RAMDirectory();
    IndexWriter writer = new IndexWriter(dir,
                                         new IndexWriterConfig(Version.LUCENE_35,
                                                               new SimpleNGramAnalyzer(3)));
    Indexer indexer = new Indexer(writer, 2, 2, 100);
    indexer.indexDocuments(newTempFile("233192	10	0	'Accessiblecomputing'	980043141	u'*'	False	99	u'RoseParks'	0:1:u'This subject covers\\n\\n* AssistiveTechnology\\n\\n* AccessibleSoftware\\n\\n* AccessibleWeb\\n\\n* LegalIssuesInAccessibleComputing\\n\\n'\n" +
                                       "18201	12	0	'Anarchism'	1014649222	u'Automated conversion'	True	None	u'Conversion script'	9230:1:u'[[talk:Anarchism|'	9252:1:u']]'	9260:1:u'[[Anarchism'	9276:1:u'|/Todo]]'	9292:1:u'talk:'	9304:-1:u'/Talk'	9464:1:u'\\n'\n"));
    indexer.finish();

    IndexSearcher searcher = new IndexSearcher(IndexReader.open(dir));
    InetSocketAddress address = findFreeAddress();
    new Thread(new SearcherDaemon(address, searcher, new QueryParser(Version.LUCENE_35, "added", new SimpleNGramAnalyzer(3)))).start();

    Thread.sleep(1000L);

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
    // query "Accessible" and receive rev_ids and timestamps
    {
      JSONObject q = new JSONObject();
      q.put("q", "Accessible");
      q.put("fields", new JSONArray(new String[]{"rev_id", "timestamp"}));
      JSONObject json = retrieve(address, q);
      assertEquals(1, json.getInt("hits_all"));
      assertEquals(233192,
                   json.getJSONArray("hits").getJSONArray(0).getInt(0));
      assertEquals("2001-01-21T11:12:21Z",
                   json.getJSONArray("hits").getJSONArray(0).getString(1));
    }
  }

  @Test public void smallDocumentsComplexQueries() throws IOException, JSONException, InterruptedException {
    Directory dir = new RAMDirectory();
    IndexWriter writer = new IndexWriter(dir,
                                         new IndexWriterConfig(Version.LUCENE_35,
                                                               new SimpleNGramAnalyzer(3)));
    Indexer indexer = new Indexer(writer, 2, 2, 100);
    indexer.indexDocuments(newTempFile("233192	10	0	'Accessiblecomputing'	980043141	u'*'	False	99	u'RoseParks'	0:1:u'This subject covers\\n\\n* AssistiveTechnology\\n\\n* AccessibleSoftware\\n\\n* AccessibleWeb\\n\\n* LegalIssuesInAccessibleComputing\\n\\n'\n" +
                                       "18201	12	0	'Anarchism'	1014649222	u'Automated conversion'	True	None	u'Conversion script'	9230:1:u'[[talk:Anarchism|'	9252:1:u']]'	9260:1:u'[[Anarchism'	9276:1:u'|/Todo]]'	9292:1:u'talk:'	9304:-1:u'/Talk'	9464:1:u'\\n'\n"));
    indexer.finish();

    IndexSearcher searcher = new IndexSearcher(IndexReader.open(dir));
    InetSocketAddress address = findFreeAddress();
    new Thread(new SearcherDaemon(address, searcher, new QueryParser(Version.LUCENE_35, "added", new SimpleNGramAnalyzer(3)))).start();

    Thread.sleep(1000L);

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
                                         new IndexWriterConfig(Version.LUCENE_35,
                                                               new SimpleNGramAnalyzer(1)));
    Indexer indexer = new Indexer(writer, 2, 10, 100);
    indexer.indexDocuments(newTempFile("233192	10	0	'Accessiblecomputing'	980043141	u'*'	False	99	u'RoseParks'	0:1:u'This subject covers\\n\\n* AssistiveTechnology\\n\\n* AccessibleSoftware\\n\\n* AccessibleWeb\\n\\n* LegalIssuesInAccessibleComputing\\n\\n'\n" +
                                       "18201	12	0	'Anarchism'	1014649222	u'Automated conversion'	True	None	u'Conversion script'	9230:1:u'[[talk:Anarchism|'	9252:1:u']]'	9260:1:u'[[Anarchism'	9276:1:u'|/Todo]]'	9292:1:u'talk:'	9304:-1:u'/Talk'	9464:1:u'\\n'\n" +
                                       "18210	12	0	'Anarchism'	1014749333	u'Automated conversion'	True	None	u'Conversion script'	9230:1:u'[[talk:Anarchism|'	9252:1:u']]'	9260:1:u'[[Anarchism'	9276:1:u'|/Todo]]'	9292:1:u'talk:'	9304:-1:u'/Talk'	9464:1:u'\\n'\n"));
    indexer.finish();

    IndexSearcher searcher = new IndexSearcher(IndexReader.open(dir));
    InetSocketAddress address = findFreeAddress();
    new Thread(new SearcherDaemon(address, searcher, new QueryParser(Version.LUCENE_35, "added", new SimpleNGramAnalyzer(1)))).start();

    Thread.sleep(1000L);

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
      assertEquals("2002-02-26", json.getJSONArray("hits").getJSONArray(0).getString(0));
      assertEquals("2002-02-27", json.getJSONArray("hits").getJSONArray(1).getString(0));
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
      assertEquals("2002-02-26", json.getJSONArray("hits").getJSONArray(0).getString(0));
      assertEquals("2002-02-27", json.getJSONArray("hits").getJSONArray(1).getString(0));
      assertEquals(1,            json.getJSONArray("hits").getJSONArray(0).getInt(1));
      assertEquals(1,            json.getJSONArray("hits").getJSONArray(1).getInt(1));
    }
  }

  @Test public void smallDocumentsHash() throws IOException, JSONException, InterruptedException {
    Directory dir = new RAMDirectory();
    IndexWriter writer = new IndexWriter(dir,
                                         new IndexWriterConfig(Version.LUCENE_35,
                                                               new HashedNGramAnalyzer(3, 3, 11)));
    Indexer indexer = new Indexer(writer, 2, 2, 100);
    indexer.indexDocuments(newTempFile("233192	10	0	'Accessiblecomputing'	980043141	u'*'	False	99	u'RoseParks'	0:1:u'This subject covers\\n\\n* AssistiveTechnology\\n\\n* AccessibleSoftware\\n\\n* AccessibleWeb\\n\\n* LegalIssuesInAccessibleComputing\\n\\n'\n" +
                                       "18201	12	0	'Anarchism'	1014649222	u'Automated conversion'	True	None	u'Conversion script'	9230:1:u'[[talk:Anarchism|'	9252:1:u']]'	9260:1:u'[[Anarchism'	9276:1:u'|/Todo]]'	9292:1:u'talk:'	9304:-1:u'/Talk'	9464:1:u'\\n'\n"));
    indexer.finish();

    IndexSearcher searcher = new IndexSearcher(IndexReader.open(dir));
    InetSocketAddress address = findFreeAddress();
    new Thread(new SearcherDaemon(address, searcher, new QueryParser(Version.LUCENE_35, "added", new HashedNGramAnalyzer(3, 3, 11)))).start();

    Thread.sleep(1000L);

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
    // query "Accessible" and receive rev_ids and timestamps
    {
      JSONObject q = new JSONObject();
      q.put("q", "Accessible");
      q.put("fields", new JSONArray(new String[]{"rev_id", "timestamp"}));
      JSONObject json = retrieve(address, q);
      assertEquals(1, json.getInt("hits_all"));
      assertEquals(233192,
                   json.getJSONArray("hits").getJSONArray(0).getInt(0));
      assertEquals("2001-01-21T11:12:21Z",
                   json.getJSONArray("hits").getJSONArray(0).getString(1));
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

