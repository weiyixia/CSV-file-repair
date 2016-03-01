"""
  	Weiyi Xia, xwy0220@gmail.com
	Steve L. Nyemba, steve@the-phi.com

	This program is design to clean up fields that have several parts to it like name, address. The program tries to find/learn about the various representations of a name/address by building a context model.

	Building context allows us to be able to infer names/addresses given a representation. For example:
	- A name like B. Malin is probably Bradley Malin
	- An address like Pleasant Springs Dr, is Probably Pleasant Springs Drive
	From this point on the application of the learner would vary i.e:

		In the context of addresses the learner can replace Dr by Drive,
		In the context of names it will have to be account for context information

	We will have consider generating statistics about the learning:
		- fuzzy match using levenstein distance
		- contextual match

	METHOD:
		We believe that with little data it is possible to make the appropriate general inferences needed. This why we use skip-grams to build out context even though there may not be a large volume of data.
		Children don't have much data about the world but are capable of generalizing concepts in the restricted world they live in. Generalizing this concept and applying it through a broad spectrum of domains allows better understanding & learning.

	DEPENDENCIES:
		pip install python-Levenshtein
		pip install fuzzywuzzy
        pip install numpy
"""


from __future__ import division
from threading import Thread
import re
import numpy as np
from sets import Set
from fuzzywuzzy import fuzz

class ILearnContext(Thread):
	"""
		The context learner will need an already processed  a dataset

		@param sample	sample records (dataset from csv file for example)
		@param field	field index
	"""
	def __init__(self,sample,field):
		Thread.__init__(self)
		self.bags = {}
		self.bag_sizes= []
		self.test = []
		self.organize(sample,field)
	"""
		Organizes a given field index into bags of words.
		The bags of words will serve as basis for generating context (skip-grams)

		@param sample   sample data from which learning is performed
		@param field  index of the field we want to extract concepts from
	"""
	def organize(self,sample,field) :

		for row in sample:
			value = self.getTerms(row[field])
			#
			# @TODO:
			# The minimum we should be working with are bigrams or tri-grams
			# 	- bi-grams and above work for names
			#	- tri-grams and above work for addresses
			# !!! Find a way to make the inference !!!
			#
			if len(value) > 1 :
				id = str(len(value))
				if id not in self.bags :
					self.bags[id] = []
					self.bag_sizes.append(len(value))
				self.bags[id].append(value) ;
			else:
				self.test.append(value)

	"""
		This function expands a field value into it's various terms
		@param value	field value
	"""
	def getTerms(self,value):
		value = re.sub('[^\x00-\x7F,\n,\r,\v,\b]',' ',value).strip()
		value = re.sub('([0-9]+[a-zA-Z]*)|[^a-zA-Z\s:]',' ',value)
		value = [term for term in value.split(' ') if len(term.strip()) > 0]
		return value

	"""
		Builds and returns context (skip-grams) given the size of the n-grams

		@pre size 	in self.bag_sizes
		@param size	n-gram size 
	"""
	def build(self,size):
		#
		# The selected field is broken and grouped into feature size 
		# Selecting a feature size will give access to the records that match that criteria
		#
		# @TODO:
		#	- have a learning algorithm that can determine these sizes given the Central Limit Theorem
		#
		keys = self.bags.keys() ;
		id = [key for key in keys if int(key) == size]
		id = id[0]
		corpus 	= self.bags[id]
		context = []
		MIN_TERMS = 2
		for phrase in corpus:
			if len(phrase) > MIN_TERMS:
				context.append([[term for term in phrase if term!= word] for word in phrase])
			else:
			#
			# Phrases that do not meet the basic context modeling requirements
			# Will be processed differently, but must still be accounted for
			#
				context.append([phrase])
		return context
"""
    This class is designed to perform simple context learning i.e using the skip grams
    It is designed for very restricted domain of application
"""
class SimpleContextLearner(ILearnContext):
    def __init__(self,sample,field):
        ILearnContext.__init__(self,sample,field) ;
        #
        # We need to determine the best size of contexts from which we can learn
        # We use a basic statistical aproach to achieve (Central Limit Theorem)
        #
        ii = [ len(self.bags[id]) for id in self.bags.keys() if int(id) > 2]
	id = self.bags.keys()[ii.index(np.max(ii))]
	self.size = int(id) ;

    def run(self):
        context = self.build(self.size)
        bag = self.bags[str(self.size)]
	N = len(context) #-- same as in bag

	info = {}
	for i in range(0,N):
		Xo = context[i]
		for ii in range(0,N):
			if i == ii or bag[i] == bag[ii]:
				continue ;
			phrase = bag[ii]
			#
			# phrase Xo and bag[ii] are different and thus we can learn from
			# Knowing the phrases are different, we look for commonalities of terms
			#

			Z = [concept for concept in Xo if len(Set(concept) & Set(phrase)) >= len(concept)]
			if len(Z) > 0 :
				#
				# We have viable information in two phrases i.e
				# We will determine exclusive terms to the context:
				#       - Xo
				#       - Yo
				#
				print self.size,Z
				Xo = [concept for concept in Xo if concept not in Z]
				Yo = [concept for concept in context[ii] if concept not in Z]
				r  = []
				for x in Xo:
					for y in Yo:
						xy = Set(x) & Set(y)
						if len(xy) > 0:
							match = list( Set(x) ^ Set(y))
							if match not in r:
								Pm =  fuzz.ratio(list(Set(x)-Set(y)),list(Set(y)-Set(x)))/ 100
								Px =  len(Z) /len(context[i])
								print [Pm,Px, np.mean([Pm,Px])]
								print Set(x) - Set(y)
								print Set(y) - Set(x)
								return 0
                                
								#
								# @TODO:
								#	Make sure the decision to keep a pair is implemented
								#	Also it needs to be persisted instead of so as to shorten the number of iterations
								#
								r.append(match)
				print r

f = open('/Users/steve/Downloads/data/Accreditation_2015_12/Accreditation_2015_12.csv','rU')
data = [line.split(',') for line in f]
thread = SimpleContextLearner(data,2)
thread.start()
