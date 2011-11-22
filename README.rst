==========================================
Wikihadoop Lucene Indexer & Searcher
==========================================

Purpose
=====================

Wikihadoop [#]_ is a tool to create a database of the differences between two revisions for Wikipedia articles. While knowing who adds / removes certain content is very useful it is still cumbersome to search through the data.
Hence, we developed a Lucene indexer that takes as input the diffdb created by Wikihadoop and creates an index that is searachable using Lucene.
This indexer assumes the input files to be formatted as explained in [#]_.

.. [#] https://github.com/whym/wikihadoop
.. [#] http://meta.wikimedia.org/wiki/WSoR_datasets/revision_diff

You invoke diffindexer on the command line using the following command:

CLASSPATH=$CLASSPATH:/home/yusuke/diffdbtest/lib/diffdb.jar:/home/yusuke/diffdbtest/lib/lucene-core-3.4-SNAPSHOT.jar:/home/yusuke/diffdbtest/lib/lucene-analyzers-3.4-SNAPSHOT.jar:/home/yusuke/diffdbtest/lib/commons-lang3-3.0.1.jar java org.wikimedia.diffdb.Searcher -index /home/yusuke/diffdbtest/index





Requirements
=====================
* Apache Lucene 3.4.0 (lucene-analyzers-3.4-\*.jar and lucene-core-3.4-\*.jar)
* Apache Commons Lang 3.0.1

.. Local variables:
.. mode: rst
.. End:
