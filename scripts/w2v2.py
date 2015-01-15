#!/usr/bin/env python
#-*- encoding: utf-8 -*-

import sys
import codecs
import argparse
import re

from math import sqrt
import datetime
import numpy
import gensim
from gensim import corpora, models, parsing, similarities
from gensim.models import word2vec


if __name__ == '__main__':

	reload(sys)
	sys.setdefaultencoding('utf-8')
	sys.stdout = codecs.getwriter('utf-8')(sys.stdout)

	# CUI引数を取得
	parser = argparse.ArgumentParser( description='run word2vec. Please give parameters, topN, positive & negative.' )
	parser.add_argument( 'topn', type=int, help='find top N account (N <= 400).' )
	parser.add_argument( '--posi', nargs='*', help='you may give positive elements. ex) golf, dog, outdoor' )
	parser.add_argument( '--nega', nargs='*', help='you may give negative elements. ex) smoking' )
	parser.add_argument( '--text', help='you would like to learn text (path). default: zunda.txt' )
	parser.add_argument( '--model', help='you would like to use model (path). default: zunda.model' )
	parser.add_argument( '-o', '--only', dest='only', default=False, action="store_true", help='output only account. default: True' )
	parser.add_argument( '-s', '--save', dest='save', default=False, action="store_true", help='save model.' )
	args = parser.parse_args()
	posi = [ unicode( a, 'utf-8' ) for a in args.posi ]
	nega = [ unicode( a, 'utf-8' ) for a in args.nega ]
	topn = args.topn
	only = args.only

	print "posi:", ", ".join( posi )
	print "nega:", ", ".join( nega )
	print "top:", topn
	print "account only:", only

	pattern = re.compile(r'^@.+')

	model = None

	if args.text:
		print "use text:", args.text
		sentences = word2vec.Text8Corpus( args.text )
		model = word2vec.Word2Vec( sentences, size=480, window=12, min_count=5, workers=2 )

	zmodel = ''
	if args.model:
		print "use model:", args.model
		zmodel = args.model
		if model is None: model = word2vec.Word2Vec.load( zmodel )
	else:
		zmodel = "zunda.model"

	if args.save:
		print "save model."
		model.save( zmodel )

	result = model.most_similar(positive=posi, negative=nega, topn=400 )

	counter = 1
	counter2 = 1
	print 'rank', 'word', 'simular'
	for r in result:
		if only:
			m = pattern.match( r[0] )
			if m:
				print counter, r[0], r[1]
				counter += 1
		else:
			print counter, r[0], r[1]
			counter += 1

		counter2 += 1
		if counter > topn:
			break

	print "last found at", ( counter2 - 1 )
