#!/usr/bin/env python
#-*- encoding: utf-8 -*-

import sys
import codecs

from math import sqrt
import datetime
import MySQLdb
import numpy
import gensim
from gensim import corpora, models, parsing, similarities
from gensim.models import word2vec

def handler():
	return MySQLdb.connect(
		host = "localhost",
		port = 3306,
		user = "twitter",
		passwd = "",
		db = "twitter",
		charset = "utf8",
		use_unicode = True
	)

def zunda_sql():
	return """\
select
   t.username
  ,t.tweet_id
  ,max(z.jise) jise
  ,max(z.kachi) kachi
  ,t.wakachi
from
  lady_timeline t
  inner join
    lady_timeline_zunda z
    on z.tweet_id = t.tweet_id
where exists (
  select * from
    zlatest l
    where l.tweet_id < t.tweet_id
)
and not exists (
  select * from
    excepts e
    where e.username = t.username
)
group by
   t.tweet_id
  ,t.username
  ,t.wakachi
order by
  t.tweet_id
limit %s, %s
"""

def zunda_count_sql():
	return """\
select
  count(*) num
from
  lady_timeline_zunda z
  inner join
    zlatest l
    on l.tweet_id < z.tweet_id
"""

def update_latest_sql():
	return """\
update zlatest set
   tweet_id = %s
"""

def get_end( cur ):
	# zundaわかちがきの総件数
	cur.execute( zunda_count_sql() )
	r = cur.fetchall()
	all_count = r[0]['num']
	print "{0} zunda separated tweets found.\n".format( all_count )
	return ( all_count / am ) + ( 0 if all_count % am == 0 else 1 )

if __name__ == '__main__':

	reload(sys)
	sys.setdefaultencoding('utf-8')
	sys.stdout = codecs.getwriter('utf-8')(sys.stdout)

	# 同時処理数
	am = 2000
	zunda = "zunda.txt"
	stops = ["あの", "あれ", "ある", "いい", "から", "けど", "ここ", "こと", "この", "これ"
				, "さえ", "しか", "じゃ", "すぐ", "する", "その", "たい", "だけ", "たら", "たり", "つつ", "って", "ても", "てる", "とか", "とも", "どれ"
				, "ない", "なく", "なし", "など", "なら", "なり", "なる", "にて", "ので", "のに", "ほど"
				, "まし", "まで", "また", "もう", "やだ", "やばい", "やら", "よく", "より"
				, "さん", "ちゃん", "です", "ます"
				, "http", "co", "jp", "io", "com", "://", ""]
	stops = map( lambda x: unicode( x, 'utf-8' ), stops )

	con = handler()
	op = con.cursor()
	cur = con.cursor( MySQLdb.cursors.DictCursor )

	# ファイルに書き込む
	f = codecs.open( zunda, mode="w", encoding="utf-8" )

	end = get_end( cur )
	latest = ''

	for n in range( end ):
		cur.execute( zunda_sql(), ( n * am, am ) )
		rows = cur.fetchall()

		for r in rows:
			words = r['wakachi'].split()
			twos = filter( lambda x: len(x) > 1, words )
			stems = filter( lambda x: not(x in stops), twos )

			f.write(
				"@{0} {1} {2} {3}\n".format(
				        r['username'], r['jise'], r['kachi'], ' '.join( stems ) ) )
			latest = r['tweet_id']

	f.close()

	op.execute( update_latest_sql(), ( latest, ) )
	con.commit()

	op.close()
	cur.close()
	con.close()
	print "finished."

