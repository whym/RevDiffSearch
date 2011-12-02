==========================================
Wikihadoop Lucene Indexer & Searcher
==========================================

Purpose
=====================

*Search* is one of the most useful means to look into a huge amount of text data.  When we deal with hundreds of millions of revisions in Wikipedia, we want to find an answer to questions like ''when did this template start to be popular in this wiki?'' and `a lot more`_.  However, it is almost impossible without a search capability over revision diffs.

WikiHadoop [#]_ is a tool to create a database of the differences between two revisions for Wikipedia articles. While knowing who adds / removes certain content is very useful it is still cumbersome to search through the data.

Hence, we developed a Lucene indexer that takes as input the diffdb created by Wikihadoop and creates an index that is searachable using Lucene.
This indexer assumes the input files to be formatted as explained in [#]_.

**Note that this software is under-development.**  Most parts are not well documented and the architecture frequently changes.  Any feedback will be welcomed at Issues_.

.. _WikiHadoop: https://github.com/whym/wikihadoop
.. _Issues: https://github.com/whym/diffindexer/issues
.. _a lot more: http://meta.wikimedia.org/wiki/Research:MDM_-_The_Magical_Difference_Machine
.. [#] http://meta.wikimedia.org/wiki/WSoR_datasets/revision_diff

How to use
=====================
We use `Apache Maven`_ to compile this software in to a jar file.  The jar file can be created by running the command ``mvn dependency:unpack-dependencies && mvn package`` in the top-level directory.

You invoke the indexer on the command line using the following command[#]_: ::

 CLASSPATH=$CLASSPATH:target/diffdb-0.1.jar java org.wikimedia.diffdb.Indexer ~/diffdbtest/index ~/diffdbtest/data/diffs

and then the interactive searcher with the following command: ::

 CLASSPATH=$CLASSPATH:target/diffdb-0.1.jar java org.wikimedia.diffdb.Searcher -index ~/diffdbtest/index

Requirements
=====================
* Apache Maven
* Apache Lucene 3.4.0 (lucene-analyzers-3.4-\*.jar and lucene-core-3.4-\*.jar)
* Apache Commons Lang 3.0.1
* opencsv
* junit
* netty

Architecture
=====================
(to be written)

* N-gram based indexing and search
* Search result refinement with grep
* Searcher daemon

.. _Apache Maven: http://maven.apache.org/
.. [#] ~/diffdbtest/data/diffs must contains the revision diff files explained at http://meta.wikimedia.org/wiki/WSoR_datasets/revision_diff

.. Local variables:
.. mode: rst
.. End:
