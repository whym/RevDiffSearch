==========================
RevDiwikiffSearch
==========================
-----------------------------------------------------------
Lucene indexer & searcher of revision diffs for WikiHadoop
-----------------------------------------------------------

Purpose
=====================

*Search* is one of the most useful means to look into a huge amount of text data.  When we deal with hundreds of millions of revisions in Wikipedia, we want to find an answer to questions like ''when did this template start to be popular in this wiki?'' and `a lot more`_.  However, it is almost impossible without a search capability over revision diffs.  `WikiHadoop`_ is a tool to create a database of the differences between two revisions for Wikipedia articles. While knowing who adds / removes certain content is very useful it is still cumbersome to search through the data.

Hence, we developed a `Lucene`_-based search system that takes as input the revision diffs created by Wikihadoop and creates an index that is searachable using Lucene.
The indexer assumes the input files to be formatted as explained in [#]_.

**Note that this software is under-development.**  Most parts are not well documented and the architecture frequently changes.  Any feedback will be welcomed at Issues_.

.. _WikiHadoop: https://github.com/whym/wikihadoop
.. _Lucene: http://lucene.apache.org
.. _Issues: https://github.com/whym/RevDiffSearch/issues
.. _a lot more: http://meta.wikimedia.org/wiki/Research:MDM_-_The_Magical_Difference_Machine
.. [#] http://meta.wikimedia.org/wiki/WSoR_datasets/revision_diff

How to use
=====================
We use `Apache Maven`_ to compile this software in to a jar file.  The jar file can be created by running the command ``mvn dependency:unpack-dependencies && mvn package`` in the top-level directory.

You invoke the indexer on the command line using the following command [#]_: ::

 CLASSPATH=$CLASSPATH:target/revdiffsearch-0.1.jar java org.wikimedia.revdiffsearch.Indexer ~/diffdbtest/index ~/diffdbtest/data/diffs

then the searcher daemon with the following command: ::

 CLASSPATH=$CLASSPATH:target/revdiffsearch-0.1.jar java org.wikimedia.revdiffsearch.SearcherDaemon -index ~/diffdbtest/index

and then you can issue a query with an accompanying script to see which revisions are matched and when they are dated: ::

 ./query.py "Welcome to Wikipedia" -R -o monthly_hits.csv

With the parameters above, the script will find revisions containing "Welcome to Wikipedia" as added text.  You can also search for other fields.  See below for a more detailed format of the query format.

Requirements
=====================

* Apache Maven
* Apache Lucene 3.5.0
* Apache Commons Lang 3.0.1
* opencsv
* junit
* netty

Query format
=====================

Following fields can be searched over.  When multiple fields are
specified, the searcher will retrieve revisions containing all fields
as specified.

* rev_id
* page_id
* namespace
* title
* timestamp
* comment
* minor
* user_id
* user_text
* added_size
* removed_size
* added
* removed
* action

For example, to find the revisions that contains the string 'Welcome
to Wikipedia' and were made within January 2006 and January 2007, you
will use ::

 added:"Welcome to Wikipedia" timestamp:[2002-01 TO 2003-01]

This query format is used when using ``query.py`` with a
``--advanced`` flag turned on, or directly connecting to the
SearcherDaemon via telnet.  By default ``query.py`` use a command line
argument as a phrase query to the ``added`` field.

See `Lucene's Query Parser Syntax`_ for more details.

(to be expanded)

Configurations
=====================

(to be expanded)

* Type of analysis to convert a document to the index representation including the value of N in N-gram indexing
* Number of threads used in indexing

Architecture
=====================
(to be written)

* N-gram based indexing and search
* Search result refinement with grep
* Searcher daemon

.. _Apache Maven: http://maven.apache.org/
.. _Lucene's Query Parser Syntax: http://lucene.apache.org/java/3_5_0/queryparsersyntax.html
.. [#] ~/diffdbtest/data/diffs must contains the revision diff files explained at http://meta.wikimedia.org/wiki/WSoR_datasets/revision_diff

.. Local variables:
.. mode: rst
.. End:
