
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
"""


from __future__ import division
from threading import Thread
import re

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
		This function will organize the various bags of words of words that will be used in modeling context
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
		This function will build and return context given the size of the n-grams we are interested in learning from. 
		
		@pre size in self.bag_sizes
		@param size	n-gram size
	"""
	def build(self,size):
		
		keys = self.bags.keys() ;
		id = [key for key in keys if int(key) == size]
		id = id[0]
		corpus 	= self.bags[id]
		context = []
		for phrase in corpus:
			if len(phrase) > 2:
				context.append([[term for term in phrase if term!= word] for word in phrase])
			else:
				context.append([phrase])
			
		return context
