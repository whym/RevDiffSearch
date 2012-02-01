package org.wikimedia.revdiffsearch;

/*This program is free software: you can redistribute it and/or modify
 it under the terms of the GNU General Public License as published by
 the Free Software Foundation, either version 3 of the License, or
 (at your option) any later version.

 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU General Public License for more details.

 You should have received a copy of the GNU General Public License
 along with this program.  If not, see <http://www.gnu.org/licenses/>.

 Author: Asterios Katsifodimos (http://www.asteriosk.gr)
 Author: Diederik van Liere (made compatible with Lucene 3.4)
 */
import java.io.File;
import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Arrays;
import java.util.ArrayList;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LogDocMergePolicy;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

public class IndexMerger {

	/** Index all text files under a directory. */
	public static void main(String[] args) {

		if (args.length != 2) {
			System.out.println("Usage: java -jar IndexMerger.jar "
					+ "merged_index_dir existing_index_dir1 existing_index_dir2 ...");
			System.out.println(" merged_index_dir: A directory where the merged "
                         + "index will be stored");
			System.out.println("   e.g. merged_indexes");
			System.out.println(" existing_indexes_dir: A directory where the "
                         + "indexes that have to merged exist");
			System.out.println("   e.g. indexes/");
			System.out.println("   e.g.         index1");
			System.out.println("   e.g.         index2");
			System.out.println("   e.g.         index3");
			System.exit(1);
		}

    int ramBufferSizeMB = 1024;
    int ngram = 3;
		File INDEX_DIR = new File(args[0]);
    boolean optimize = true;
    {
      String s;
      if ( (s = System.getProperty("optimize")) != null ) {
        optimize = "true".equals(s);
      }
      if ( (s = System.getProperty("ngram")) != null ) {
        ngram = Integer.parseInt(s);
      }
      if ( (s = System.getProperty("ramBufferSize")) != null ) {
        ramBufferSizeMB = Integer.parseInt(s);
      }
    }

		INDEX_DIR.mkdir();

		Date start = new Date();

		try {
			IndexWriterConfig cfg = new IndexWriterConfig(Version.LUCENE_35,
                                                    new SimpleNGramAnalyzer(ngram));
			LogDocMergePolicy lmp = new LogDocMergePolicy();
			lmp.setMergeFactor(1000);
			cfg.setRAMBufferSizeMB(ramBufferSizeMB);
			cfg.setMergePolicy(lmp);

			IndexWriter writer = new IndexWriter(FSDirectory.open(INDEX_DIR), cfg);

			// IndexWriter writer = new IndexWriter(INDEX_DIR,
			// new StandardAnalyzer(Version.LUCENE_35),
			// true);
			// writer.setMergeFactor(1000);
			// writer.setRAMBufferSizeMB(50);

			List<Directory> indexes = new ArrayList<Directory>();

			for ( String indexdir: Arrays.asList(args).subList(1, args.length)) {
				System.out.println("Adding: " + indexdir);
				indexes.add(FSDirectory.open(new File(indexdir)));
			}

			System.out.print("Merging added indexes...");
			writer.addIndexes(indexes.toArray(new Directory[indexes.size()]));
			System.out.println("done");

      if ( optimize ) {
        System.out.print("Optimizing index...");
        writer.optimize();
      }
			writer.close();
			System.out.println("done");

			Date end = new Date();
			System.out.println("It took: "
					+ ((end.getTime() - start.getTime()) / 1000) + "\"");

		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}