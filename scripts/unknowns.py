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

def unknowns_sql( layer ):
	return """\
select
   tweet_id
  ,'?' as female
  ,{0}
from
  unknowns
limit %s, %s
""".format( vector_sel_columns( layer ) )

def unknowns_count_sql():
	return """\
select
   count(*) num
from
  unknowns
"""

def vector_sel_columns( layer ):
	buf = []
	for n in range( layer ):
		buf.append( "v{0}".format( n ) )
	return ",".join( buf )

def write_arff( f, layer, all_count ):
	f.write( "%% unknowns from MySQL db.\n" )
	f.write( "%% all count = {0}\n\n".format( all_count ) )
	f.write( "@RELATION unknowns\n" )
	f.write( "\n" )
	f.write( "@ATTRIBUTE id	integer\n" )
	f.write( "@ATTRIBUTE female	{True, False}\n" )
	for n in range( layer ):
		f.write( "@ATTRIBUTE v{0}	REAL\n".format( n ) )
	f.write( "\n" )
	f.write( "\n@DATA\n" )

if __name__ == '__main__':

	sys.stdout = codecs.getwriter('utf_8')(sys.stdout)

	# 次元数
	layer = 20
	# 同時処理数
	am = 2000


	con = handler()
	cur = con.cursor()

	select = unknowns_sql( layer )

	# 予測ツイートの総件数
	cur.execute( unknowns_count_sql() )
	r = cur.fetchall()
	all_count = r[0][0]
	print "{0} tweet_ids found.\n".format( all_count )

	f = open( "unknowns.arff", "w" )
	write_arff( f, layer, all_count )

	end = ( all_count / am ) + ( 0 if all_count % am == 0 else 1 )

	# 2,000づつ書き出します
	for n in range( end ):
		cur.execute( select, ( n * am, am ) )
		items = cur.fetchall()
		for item in items:
			r = [ str( c ) for c in list( item ) ]
			f.write( "{0}\n".format( ",".join( r ) ) )

		print "{0} finished.".format( n * am )

	cur.close()
	con.close()
	f.close()
	print "finished."

