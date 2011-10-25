package org.wikimedia.diffdb;

import java.io.IOException;

import org.apache.lucene.index.IndexWriter;

public class CallbackImpl implements Callback {
	@Override
	public void reportProgress(IndexWriter writer) throws IOException {
		// TODO Auto-generated method stub
		if ((writer.numDocs() % 10000) == 0) {
			System.out.println(writer.numDocs() + " documents indexed.");
		}
	}

}