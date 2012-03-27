==========================
RevDiffSearch
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

 CLASSPATH=$CLASSPATH:target/revdiffsearch-0.1.jar java org.wikimedia.revdiffsearch.SearcherDaemon ~/diffdbtest/index

and then you can issue a query with an accompanying script to see which revisions are matched and when they are dated: ::

 ./trend_query.py "Welcome to Wikipedia" -R -o monthly_hits.csv

With the parameters above, the script will find revisions containing "Welcome to Wikipedia" as added text.  You can also search for other fields.  See below for a more detailed format of the query format.

.. [#] ~/diffdbtest/data/diffs must contains the revision diff files explained at http://meta.wikimedia.org/wiki/WSoR_datasets/revision_diff

Requirements
=====================

* Apache Maven
* Apache Lucene 3.5.0
* Apache Commons Lang 3.0.1
* opencsv
* junit
* netty

Command line usage
=====================

Query options
--------------------------
Here are some of the command line options for the ``trend_query.py`` script. ::
 
 ./trend_query.py 'query_string' [-m N] [-R] [-d] [-a]
 
 -m N will set N as the maximum number of hits returned (10000 by default).
 -R   will add revision IDs of those hits.
 -d   will add some debug information as comments starting with '#'.
 -a   will switch to the advanced mode where you can search for any field listed at the Query Format section.
      By default the query string is regarded as the exact string you want to find.

Example queries
---------------------------
Here are some of the queries you can make with the ``trend_query.py`` script.

Number of occurrences of the string ''welcome'' for each month [#]_:
  ::
  
  ./trend_query.py 'welcome' -o result.tsv
Revision IDs of additions that contains no wiki syntax such as ``[[ABC]]``, ``[http://example.com Example]`` or ``{{ABC}}``, and were made between 2011-11-01 and 2011-12-01 [#]_ [#]_:
  ::
  
  ./trend_query.py -a 'added_size:0 AND NOT \[\[?? AND NOT \[??? AND NOT \{\{??' -s 2011-11-01 -e 2011-12-01 -D -R > nowiki_201111.tsv

(to be expanded)

.. [#] Note that the match will be decided with no consideration of word boundary. For example, the query '``welcome``' matches to 'Welcome to Wikipedia' and 'such behavior is unwelcome'.
.. [#] Note that this query uses four-letter patterns, assuming a 4-gram index.  When the index is created with a 3-gram analyzer, use ``NOT \[\[? AND NOT \[?? AND NOT \{\{?`` instead.
.. [#] '``?``' is a wildcard that matches to an arbitrary character.

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

This query format is used when using ``trend_query.py`` with a
``--advanced`` flag turned on, or directly connecting to the
SearcherDaemon via telnet.  By default ``trend_query.py`` use a command line
argument as a phrase query to the ``added`` field.

See `Lucene's Query Parser Syntax`_ for more details.

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

.. Local variables:
.. mode: rst
.. End:
