package org.wikimedia.diffdb;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Hashtable;
import java.util.Iterator;

public class BufferedReaderIterable implements Iterable<Hashtable<Integer, String>> {

	private BufferedReader mine;
	private Iterator<Hashtable<Integer, String>> i;

	public BufferedReaderIterable(BufferedReader br) {
		i = new BufferedReaderIterator(br);
	}

	public BufferedReaderIterable(File f) throws FileNotFoundException {
		mine = new BufferedReader(new FileReader(f));
		i = new BufferedReaderIterator(mine);
	}

	public Iterator<Hashtable<Integer, String>> iterator() {
		return i;
	}

	private class BufferedReaderIterator implements Iterator<Hashtable<Integer, String>> {
		private BufferedReader br;
		private String line;
		private String[] tmp;
		private Hashtable<Integer, String> diff;

		public BufferedReaderIterator(BufferedReader aBR) {
			(br = aBR).getClass();
			advance();
		}

		public boolean hasNext() {
			return line != null;
		}

		public Hashtable<Integer, String> next() {
			Hashtable<Integer, String> retval = diff;
			advance();
			return retval;
		}

		public void remove() {
			throw new UnsupportedOperationException(
					"Remove not supported on BufferedReader iteration.");
		}

		private void advance() {
			try {
				line = br.readLine();
				tmp = line.split(",");
				
				for (int i=0; i<KeyMap.length; i++){
					diff.put(i, tmp[i]);
				}
				
			} catch (IOException e) { /* TODO */
			}
			if (line == null && mine != null) {
				try {
					mine.close();
				} catch (IOException e) { /* Ignore - probably should log an error */
				}
				mine = null;
			}
		}
	}
}
