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

def insert_sql():
	return """\
insert into forecast (
   id, is_female, is_female_text, tweet_id
) values (
   %s, %s, %s, %s
)
"""

def read_csv( path ):
	rows = []
	f = open( path, 'r' )
	for line in f:
		rows.append( line[:-1].split('\t') )
	f.close()
	return rows

if __name__ == '__main__':

	sys.stdout = codecs.getwriter('utf_8')(sys.stdout)

	con = handler()
	op = con.cursor()

	rows = read_csv( 'random_forest.txt' )

	counter = 0

	formatted = []
	for row in rows:
		cols = []
		cols.append( int( row[0] ) )
		cols.append( int( row[1] ) )
		cols.append( row[2] )
		cols.append( "{0:.0f}".format( float( row[5] ) ) )

		formatted.append( tuple( cols ) )
		if ( counter % 1000 ) == 0:
			op.executemany( insert_sql(), formatted )
			del formatted[:]
		counter += 1

	if formatted != []:
		op.executemany( insert_sql(), formatted )


	con.commit()
	op.close()
	con.close()

	print "text inserted."

