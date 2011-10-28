package org.wikimedia.diffdb;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Hashtable;
import java.util.Iterator;

public class BufferedReaderIterable implements Iterable<Hashtable<String, String>> {

	private BufferedReader mine;
	private Iterator<Hashtable<String, String>> i;

	public BufferedReaderIterable(BufferedReader br) {
		i = new BufferedReaderIterator(br);
	}

	public BufferedReaderIterable(File f) throws FileNotFoundException {
		mine = new BufferedReader(new FileReader(f));
		i = new BufferedReaderIterator(mine);
	}

	public Iterator<Hashtable<String, String>> iterator() {
		return i;
	}

	private class BufferedReaderIterator implements Iterator<Hashtable<String, String>> {
		private BufferedReader br;
		private String line;
		private String[] tmp;
		private Hashtable<String, String> diff;

		public BufferedReaderIterator(BufferedReader aBR) {
			(br = aBR).getClass();
			advance();
		}

		public boolean hasNext() {
			return line != null;
		}

		public Hashtable<String, String> next() {
			Hashtable<String, String> retval = diff;
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
				KeyMap km = new KeyMap();
				for (int i=0; i<km.length; i++){
					diff.put(km.map.get(i), tmp[i]);
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
