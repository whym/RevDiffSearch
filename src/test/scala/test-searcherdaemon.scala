import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith
import org.scalatest.FunSpec
import scala.collection.JavaConversions._

import org.wikimedia.revdiffsearch._
import java.io._
import java.util.logging._
import java.net._
import org.apache.lucene.index._
import org.apache.lucene.analysis._
import org.apache.lucene.store._
import org.apache.lucene.search._
import org.apache.lucene.document._
import org.apache.lucene.queryparser.classic.QueryParser
import org.apache.lucene.util.Version
import scala.io
import org.json4s.{JInt, JString, JField, JArray, JValue, JObject}
import org.json4s.native.{JsonMethods => JM}
import org.json4s.JsonDSL._
import scala.language.reflectiveCalls

@RunWith(classOf[JUnitRunner])
class SearcherDaemonSpec extends FunSpec {

  def newTempFile(content: String): File = {
    val file = File.createTempFile("indexer", ".txt")
    file.deleteOnExit
    val writer = new BufferedWriter(new FileWriter(file))
    writer.write(content)
    writer.close()
    file
  }

  def retrieve(address: InetSocketAddress, query: JValue): JValue = {
    val socket = new Socket(address.getAddress, address.getPort)
    val writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()))
    writer.write(JM.compact(JM.render(query)))
    writer.write("\n")
    writer.flush()
    socket.shutdownOutput()
    val ret = JM.parse(io.Source.fromInputStream(socket.getInputStream).mkString)
    writer.close
    socket.close
    info(JM.compact(JM.render(ret)))
    ret
  }

  def findFreeAddress(): InetSocketAddress = {
    val socket = new ServerSocket(0)
    val address = new InetSocketAddress(socket.getLocalPort())
    socket.close
    address
  }

  def waitUntilPrepared(address: InetSocketAddress, limit: Long): Boolean = {
    val start = System.currentTimeMillis
    while ( true) {
      try {
        val sock = new Socket(address.getAddress(), address.getPort())
        if ( sock.isConnected ) {
          sock.close
          return true
        }
      } catch {
        case e: ConnectException => {
          Thread.sleep(limit / 20)
          if ( System.currentTimeMillis - start > limit ) {
            return false
          }
        }
      }
    }
    false
  }

  Logger.getLogger(classOf[SearcherDaemon].getName()).setLevel(Level.INFO);
  Logger.getLogger(classOf[Indexer].getName()).setLevel(Level.WARNING);


  def daemon(analyzer: Analyzer, doc: String) = {
    new {
      val dir = new RAMDirectory()
      val writer = new IndexWriter(dir,
                                   new IndexWriterConfig(Version.LUCENE_44,
                                                         analyzer))
      val indexer = new Indexer(writer, 2, 2, 100)
      indexer.indexDocuments(newTempFile(doc))
      indexer.finish
      
      val searcher = new IndexSearcher(DirectoryReader.open(dir))
      val address = findFreeAddress()
      new Thread(new SearcherDaemon(address, searcher, new QueryParser(Version.LUCENE_44, "added", RevDiffSearchUtils.getAnalyzerCombined(analyzer)), 1)).start()
      waitUntilPrepared(address, 1000L)
    }
  }

  def n3daemon(doc: String) = daemon(new SimpleNGramAnalyzer(3), doc)


  describe("SearcherDaemon with two small documents") {

    val d = n3daemon("""233192	10	0	'Accessiblecomputing'	980043141	u'*'	False	99	u'RoseParks'	0:1:u'This subject covers\\n\\n* AssistiveTechnology\\n\\n* AccessibleSoftware\\n\\n* AccessibleWeb\\n\\n* LegalIssuesInAccessibleComputing\\n\\n'
18201	12	0	'Anarchism'	1014649222	u'Automated conversion'	True	None	u'Conversion script'	9230:1:u'[[talk:Anarchism|'	9252:1:u']]'	9260:1:u'[[Anarchism'	9276:1:u'|/Todo]]'	9292:1:u'talk:'	9304:-1:u'/Talk'	9464:1:u'\\n'
""")
      
    it("should process simple string queries to get rev_ids") {
      val q = ("q" -> "\"/Todo\"")~("fields" -> "rev_id")
      val json = retrieve(d.address, q)
      assert(JInt(1) == (json \ "hits_all"))
      assert(JString("18201") == (json \ "hits")(0)(0))
    }

    it("shout get rev_ids and timestamps") {
      val q = ("q" -> "\"* Legal\"") ~ ("fields" -> List("rev_id", "timestamp"))
      val json = retrieve(d.address, q)
      assert(JInt(1) == (json \ "hits_all"))
      assert(JString("233192") ==
        (json \ "hits")(0)(0))
      assert(JString("2001-01-21T02:12:21Z") ==
        (json \ "hits")(0)(1))
    }

    it("should process namespace and page_id -specifying queries") {
      val q = ("q" -> "namespace:0 page_id:12") ~ ("fields" -> "rev_id")
      val json = retrieve(d.address, q)
      assert(JString("+namespace:0 +page_id:12") ==
        (json \ "parsed_query"))
      assert(JInt(1) ==
        (json \ "hits_all"))
      assert(JString("18201") == (json \ "hits")(0)(0))
    }

  }

  describe("SearcherDaemon with three small documents") {

    val d = n3daemon("""233192	10	0	'Accessiblecomputing'	980043141	u'*'	False	99	u'RoseParks'	0:1:u'This subject covers\\n\\n* AssistiveTechnology\\n\\n* AccessibleSoftware\\n\\n* AccessibleWeb\\n\\n* LegalIssuesInAccessibleComputing\\n\\n'
18201	12	0	'Anarchism'	1014649222	u'Automated conversion'	True	None	u'Conversion script'	9230:1:u'[[talk:Anarchism|'	9252:1:u']]'	9260:1:u'[[Anarchism'	9276:1:u'|/Todo]]'	9292:1:u'talk:'	9304:-1:u'/Talk'	9464:1:u'\\n'
12345	10	0	'Accessiblecomputing'	1980043141	u'*'	False	99	u'Automated conversion'	0:1:u'[[fr:ABC]]\\n'
""")

    it("should process OR queries") {
      val q = ("q" -> "rev_id:12345 OR page_id:12") ~ ("fields", "rev_id")
      val json = retrieve(d.address, q)
      assert(JInt(2) == (json \ "hits_all"))
      assert(Set(JString("12345"), JString("18201")) ==
        Set((json \ "hits")(0)(0), (json \ "hits")(1)(0)))
    }

    it("should process '?' queries") {
      val q = ("q" -> "namespace:0 AND NOT \\[\\[?") ~ ("fields", "rev_id")
      val json = retrieve(d.address, q)
      assert(JInt(1) == json \ "hits_all")
      assert(JString("233192") == (json \ "hits")(0)(0))
    }

  }

}
