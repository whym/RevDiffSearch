package org.wikimedia.diffdb;

import java.io.IOException;

import org.apache.lucene.index.IndexWriter;

public interface Callback {

	void reportProgress(IndexWriter writer) throws IOException;
}
