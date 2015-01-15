#!/usr/bin/env python
#-*- encoding: utf-8 -*-

import sys
import codecs

from math import sqrt
import datetime
import MySQLdb
import numpy
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

def tweets_count_sql():
	return """\
select
   count(*) num
from
  user_timeline
where
  wakachi <> ''
  and exists (
    select * from
      latest l
    where
      l.tweet_id < ut.tweet_id
  )
"""

def tweets_sql():
	return """\
select
   ut.id
  ,ut.tweet_id
  ,ut.username
  ,ut.wakachi
from
  user_timeline ut
where
  ut.wakachi <> ''
  and exists (
    select * from
      latest l
    where
      l.tweet_id < ut.tweet_id
  )
order by
  ut.tweet_id asc
limit %s, %s
"""

def create_sql( layer ):
	return """\
create table unknowns (
   id int(11) not null auto_increment
  ,tweet_id varchar(255) not null
  ,is_female tinyint(1)
  ,{0}
  ,primary key ( id )
)
""".format( vector_cre_columns( layer ) )

def vector_cre_columns( layer ):
	buf = []
	for n in range( layer ):
		buf.append( "v{0} float not null".format( n ) )
	return ",".join( buf )

def vector_sel_columns( layer ):
	buf = []
	for n in range( layer ):
		buf.append( "v{0}".format( n ) )
	return ",".join( buf )

def vector_val_columns( layer ):
	buf = []
	for n in range( layer ):
		buf.append( "%s" )
	return ",".join( buf )

def insert_sql( layer ):
	return """\
insert into unknowns (
   tweet_id, {0}
) values (
   %s, {1}
)
""".format( vector_sel_columns( layer ), vector_val_columns( layer ) )

def update_latest_sql():
	return """\
update latest set
   tweet_id = %s
"""

def format_rows( data ):
	rows = []
	for cols in data:
		can_append = True
		for n in range( 1, len(cols) ):
			if numpy.isnan(cols[n]).any():
				can_append = False
				break
		if can_append:
			rows.append( tuple( cols ) )
	return rows

if __name__ == '__main__':

	sys.stdout = codecs.getwriter('utf_8')(sys.stdout)

	# 次元数
	layer = 20
	# 同時処理数
	am = 2000
	# テーブルを再作成
	is_recreate = True
#	is_recreate = False

	latest = ''

	con = handler()
	op = con.cursor()
	cur = con.cursor( MySQLdb.cursors.DictCursor )

	if is_recreate:
		print "create unknowns table >>>\n"
		op.execute( "drop table if exists unknowns;" )
		op.execute( create_sql( layer ) )

	insert = insert_sql( layer )

	# 単語辞書を作成します
	print "fetch start girls_word >>>\n"

	cur.execute( wob_sql() )
	rows = cur.fetchall()
	words = [ r['word'] for r in rows ]
	dic = gensim.corpora.Dictionary( [ words ] )

	# ツイートの総件数
	cur.execute( tweets_count_sql() )
	r = cur.fetchall()
	all_count = r[0]['num']
	print "{0} tweets found.\n".format( all_count )

	end = ( all_count / am ) + ( 0 if all_count % am == 0 else 1 )

	# 2,000ツイートづつベクトルを作成します
	for n in range( end ):
		cur.execute( tweets_sql(), ( n * am, am ) )
		tweets = cur.fetchall()

		# Bag of Wordsベクトルを作成します
		print "Bag of Words >>>\n"
		bow = {}
		for t in tweets:
			sparse = dic.doc2bow( t['wakachi'].split() )
			bow[ t['tweet_id'] ] = sparse

		lsi = gensim.models.LsiModel( bow.values(),
						id2word=dic,
						num_topics=layer )

		lsis = []
		for k in bow.keys():
			sparse = lsi[ bow[k] ]
			vec = list( gensim.matutils.corpus2dense( [sparse], num_terms=layer ).T[0] )
			norm = sqrt( sum( num ** 2 for num in vec ) )
			unit_vec = [ num / norm for num in vec ]

			lsis.append( [k] + unit_vec )

		print "Now, insert start >>>\n"
		try:
			res = op.executemany( insert, format_rows( lsis ) )
		except:
			print lsis
			raise

		con.commit()
		latest = (lsis.pop())[0]
		print "{0} inserted.".format( res )
		print "{0} finished.".format( n * am )

	print "latest is {0}".format( latest )

	op.execute( update_latest_sql(), ( latest, ) )
	con.commit()

	op.close()
	cur.close()
	con.close()
	print "finished."

