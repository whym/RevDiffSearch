package org.wikimedia.diffdb;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.List;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriter;

public class DiffDocumentConsumer implements Runnable {
  private final BlockingQueue<Document> prodq;
  private final BlockingQueue<Document> poolq;
  private final IndexWriter writer;
	private final List<Runnable> producers;
  public DiffDocumentConsumer(IndexWriter writer, BlockingQueue<Document> prodq, BlockingQueue<Document> poolq, List<Runnable> producers) {
		this.writer = writer;
    this.prodq = prodq;
    this.poolq = poolq;
		this.producers = producers;
  }
  public void run() {
		try {
			// FIXME: currently a consumer needs to be started after having at least one producer is running, otherwise it immediately finishes. it should wait.
			while ( this.prodq.size() > 0  ||  this.producers.size() > 0 ) {
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
