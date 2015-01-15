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

def create_sql( layer ):
	return """\
create table vectors (
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
insert into vectors (
   tweet_id, is_female, {0}
) values (
   %s, %s, {1}
)
""".format( vector_sel_columns( layer ), vector_val_columns( layer ) )

def read_csv( classify ):
	rows = []
	f = open( "{0}.csv".format( classify ), 'r' )
	for line in f:
		rows.append( line[:-1].split(',') )
	f.close()

	if rows[0][0] == "id":
		rows.pop(0)
	return rows

def format_rows( data ):
	rows = []
	for cols in data:
		can_append = True
		for n in range( 2, len(cols) ):
			if cols[n] == "nan":
				can_append = False
				break
			cols[n] = float( cols[n] )
		if can_append:
			rows.append( tuple( cols ) )
	return rows

if __name__ == '__main__':

	print "Start >>>\n"

	girls = read_csv( "girls" )

	# テーブルを作成します。
	layer = len( girls[0] ) - 2

	con = handler()
	cur = con.cursor()
	cur.execute( "drop table if exists vectors;" )
	cur.execute( create_sql( layer ) )

	# girlsをinsertします。
	insert = insert_sql( layer )
	cur.executemany( insert, format_rows( girls ) )
	print "girls inserted."

	# othersをinsertします。
	others = read_csv( "others" )
	cur.executemany( insert, format_rows( others ) )
	print "others inserted."

	con.commit()
	cur.close()
	con.close()
	print "all finished."

