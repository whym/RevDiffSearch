package org.wikimedia.revdiffsearch;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.Set;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriter;

public class DiffDocumentConsumer implements Runnable {
  private final BlockingQueue<Document> prodq;
  private final BlockingQueue<Document> poolq;
  private final IndexWriter writer;
  public DiffDocumentConsumer(IndexWriter writer, BlockingQueue<Document> prodq, BlockingQueue<Document> poolq) {
		this.writer = writer; 
    this.prodq = prodq;
    this.poolq = poolq;
  }

  public void run() {
		try {
			while ( true ) {
			Document doc = this.prodq.take();
				writer.addDocument(doc);
				this.poolq.put(doc);
			}
		} catch ( InterruptedException e ) {
			System.out.println(e);
		} catch ( IOException e ) {
			System.out.println(e);
		}
  }
}
/*
 * Local variables:
 * tab-width: 2
 * c-basic-offset: 2
 * indent-tabs-mode: t
 * End:
 */
