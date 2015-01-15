#!/usr/bin/env python
#-*- encoding: utf-8 -*-

import sys
import codecs

from math import sqrt
import datetime
import MySQLdb
import gensim
import gensim.parsing

def handler():
	return MySQLdb.connect(
		host = "localhost",
		port = 3306,
		user = "twitter",
		passwd = "",
		db = "twitter",
		use_unicode=1
	)

def wob_sql():
  return """\
select
   id
  ,word
  ,hindo
from
  girls_word
where
  hindo between 5 and 72
order by hindo desc
"""

def girls_sql():
  return """\
select
   id
  ,tweet_id
  ,username
  ,wakachi
from
  {0}_timeline
where
  wakachi <> ''
order by
   username asc
  ,tweet_id
"""

if __name__ == '__main__':

	sys.stdout = codecs.getwriter('utf_8')(sys.stdout)

	is_girl = True
	classify = "girls"

	# 単語辞書を取得します
	print "fetch start girls_word >>>\n"
	con = handler().cursor( MySQLdb.cursors.DictCursor )
	con.execute( wob_sql() )
	rows = con.fetchall()
	words = [ r['word'] for r in rows ]


	# 辞書を作成します
	dic = gensim.corpora.Dictionary( [ words ] )

	# Bag of Wordsベクトルを作成します
	layer = 20
	print "Bag of Words layer={0}.>>>\n".format( layer )

	con.execute( girls_sql().format( classify ) )
	tweets = con.fetchall()

	bow = {}
	for t in tweets:
		bow[ t['tweet_id'] ] = dic.doc2bow( t['wakachi'].split() )

	lsi = gensim.models.LsiModel( bow.values(),
					id2word=dic,
					num_topics=layer )

	lsi_docs = []

	for k in bow.keys():
		sparse = lsi[ bow[k] ]
		vec = list( gensim.matutils.corpus2dense( [sparse], num_terms=layer ).T[0] )
		norm = sqrt( sum( num ** 2 for num in vec ) )
		unit_vec = [ num / norm for num in vec ]
		lsi_docs.append( "{0},{1},{2}\n".format( k, is_girl, ','.join( str(x) for x in unit_vec ) ) )

	print "Now, flush!"

	f = open( "{0}.csv".format( classify ), "w" )
	f.write( "id,female,{0}\n".format(
		",".join( [ "v{0}".format( n ) for n in range( layer ) ] ) ) )

	f.writelines( lsi_docs )
	f.close()

