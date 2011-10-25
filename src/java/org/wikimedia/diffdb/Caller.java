package org.wikimedia.diffdb;

import java.io.IOException;

import org.apache.lucene.index.IndexWriter;

public class Caller {
	public Callback callback = null;
	public static IndexWriter writer = null;

	public void register(Callback callback) {
		this.callback = callback;
	}

	public void execute() throws IOException {
		this.callback.reportProgress(writer);
	}

	public Caller(IndexWriter writer) {
		Caller.writer = writer;
	}

	public static void main(String args[]) throws IOException {
		Caller caller = new Caller(writer);
		Callback callback = new CallbackImpl();
		caller.register(callback);
		caller.execute();
	}
}