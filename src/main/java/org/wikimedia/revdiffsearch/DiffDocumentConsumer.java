package org.wikimedia.revdiffsearch;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.Set;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;

public class DiffDocumentConsumer implements Runnable {
  private final BlockingQueue<Document> prodq;
  private final BlockingQueue<Document> poolq;
	private final boolean overwrite;
  private final IndexWriter writer;
  public DiffDocumentConsumer(IndexWriter writer, BlockingQueue<Document> prodq, BlockingQueue<Document> poolq, boolean overwrite) {
		this.writer = writer; 
    this.prodq = prodq;
    this.poolq = poolq;
		this.overwrite = overwrite;
  }

  public void run() {
		try {
			while ( true ) {
				Document doc = this.prodq.take();
				if ( this.overwrite ) {
					writer.updateDocument(new Term("rev_id",doc.getField("rev_id").stringValue()), doc);
				} else {
					writer.addDocument(doc);
				}
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
