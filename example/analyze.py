# -*- coding: utf-8 -*-
'''Analyze web content using our package Distrpy.
	 Web content crawled using Scrapy and saved into
	 MongoDB.
'''
# python 3
import sys; sys.path.append('../src')
from pymongo import MongoClient
from collections import Counter
from distrpy.server.dbconnect import DBConnect
from distrpy.starter import Distrpy
from rediscluster import StrictRedisCluster
from pprint import pprint

import nltk
from nltk import word_tokenize
from nltk.tokenize import RegexpTokenizer
from stop_words import get_stop_words
from nltk.stem.porter import PorterStemmer
from nltk.stem.lancaster import LancasterStemmer
from gensim import corpora, models
import gensim
from datetime import datetime

count = 0
maxcount = 560000

'''	Load content from generator then
		map function and arguments into
		ProcMaster.
'''
def loadContent(WebContentCollection, dp, rc):
    arglist = webDataGenerator(WebContentCollection)
    print("Start mapping=========>")
    flag = dp.map(analyze, arglist, print_results, rc, cb_per_result=True)
    if not flag:
        print ("map failed")
    else:
        print ("map succeed")

''' Get content from MongoDb,
		Find and update all data
		which field 'done' is false.
'''
def webDataGenerator(WebContentCollection):
    totalcount = WebContentCollection.find({'done': False}).count()
    count = 0
    while count < totalcount:
        webContent = WebContentCollection.find_one_and_update({'done': False}, {'$set':{'done':True}})
        count += 1
        if webContent.get('content') and webContent.get('url') and webContent.get('title'):
          yield (webContent.get('content'),webContent.get('url'),webContent.get('title'))
        if count == 50: break;

'''	Callback function.
		Print current webContent's
		number and insert `analyze`
		returned tag result to Redis
		Cluster.
'''
def print_results(results, rc):
    _, results = results
    if 'except' in results:
      print(results['except'])
    else:
      tags, url, title = results['result']
      global count
      count+=1
      pprint(count)
      rc.set(url, title)
      for tag in tags:
          tag = tag.lower()
          rc.sadd("tags", tag) # add tag to set `tags`
          rc.sadd(tag, url) # add current url to set `$tag`
          rc.sadd('tag:'+url, tag) # add all tags into set `$url`
    print("callback finished")


'''	Analyze web content using
		NLTK and LDA algorithm.
'''
def analyze(content, url, title):
	tokenizer = RegexpTokenizer(r'\w+')
	en_stop = get_stop_words('en')
	p_stemmer = LancasterStemmer()

	stop_token = ['The', 'can', 's', 'I', 't', 'am', 'are']
	texts = []
	content_tokens = word_tokenize(content)
	title_tokens = word_tokenize(title)
	content_text = nltk.Text(content_tokens)

	tokens = tokenizer.tokenize(content)

	tokens = [i for i in tokens if not i.isdigit()]  #Remove all numbers
	stopped_tokens = [i for i in tokens if not i in en_stop] #Remove all meaningless words
	stopped_tokens = [i for i in stopped_tokens if not i in stop_token] #Stem tokens
	stemmed_tokens = [p_stemmer.stem(i) for i in stopped_tokens]
	texts.append(stemmed_tokens)

	dictionary = corpora.Dictionary(texts)
	corpus = [dictionary.doc2bow(text) for text in texts]
	ldamodel = gensim.models.ldamodel.LdaModel(corpus, num_topics=1,\
	 id2word = dictionary, passes=20)
	topics = ldamodel.show_topic(0, 3)
	#topics = ldamodel.print_topics(num_topics=1, num_words=3)[0]
	Rtopic = []

	for topicTuple in topics:
		topic, rate = topicTuple
		Rtopic.append(topic)

	if len(Rtopic) == 0:
		Rtopic.append("Not English")
		Rtopic.append("Maybe Chinese?")

	return (Rtopic, url, title)



if __name__ == '__main__':
  PORT = 9999

  client = MongoClient('localhost', 27017)
  db = client.alfred_database
  rc = DBConnect().StrictRedisCluster
  dp = Distrpy(PORT)

  WebContentCollection = db.broad_web_content
  while True:
  	s = datetime.now()
  	loadContent(WebContentCollection, dp, rc)
  	dp.wait_all_job_done()
  	e = datetime.now()
  	print(e-s)
  	if count >= maxcount:break
  print("All job finished.")



