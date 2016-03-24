"""
  	Weiyi Xia, xwy0220@gmail.com
	Steve L. Nyemba, steve@the-phi.com

	This program is design to clean up fields that have several parts to it like name, address. 
	The program tries to find/learn about the various representations of a name/address by building a context model.
	
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
		The idea of building skip-grams to model context is well known in the research community 
			http://homepages.inf.ed.ac.uk/ballison/pdf/lrec_skipgrams.pdf
			https://scholar.google.com
		
		We believe that with little data it is possible to make the appropriate general inferences needed. 
		This why we use skip-grams to build out context even though there may not be a large volume of data.
		Children don't have much data about the world but are capable of generalizing concepts in the restricted world they live in. 
		Generalizing this concept and applying it through a broad spectrum of domains allows better understanding & learning.


	DEPENDENCIES:
		pip install python-Levenshtein
		pip install fuzzywuzzy
		pip install numpy

"""


from __future__ import division
from threading import Thread, RLock
import re
import numpy as np
from sets import Set
from fuzzywuzzy import fuzz, process
from ngram import NGram
from Queue import Queue

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
		info = {}
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
				if str(value) not in info:
					self.bags[id].append(value) ;
				info[str(value)]  = 1
			else:
				self.test.append(value)
		#
		# At this point we have insured that the n-grams don't have duplicates
		# Duplicate data provides very little context for learning
		#
		del info

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
	NUMBER_THREADS = 2
	offset = int(N/NUMBER_THREADS)

	#
	# We launch NUMBER_THREADS to learn in parrallel
	# The results learnt will be accumulated in python Queue
	# NOTE: duplicates have been removed, so there is no need to consolidate results

	#	
	
	self.queue = Queue()
	q = []
	threads = []
	lock = RLock()
	for i in range(0,NUMBER_THREADS):
		xi = i * offset
		yi = i * offset + offset
		if i == NUMBER_THREADS-1:
			yi = N
		print [xi,yi,(yi-xi)]
		thread = Clean( list(context[xi:yi]),bag);
		thread.name = str(i)
		threads.append(thread)
		thread.init(self.queue,lock)
		#thread.setDaemon(True)
		thread.start()
	for thread in threads:
		
		thread.join()
		
		
		if  np.sum([int(thread.isAlive() == False)for thread in threads]) == len(threads):
			break	
	#	if thread.isAlive() == False:
	#		[q.append(thread.info[value]) for value in thread.info]
	#		id = thread.info.keys()[0]
	#		print ['thread - ',thread.name,id,thread.info[id]]	
	
	while self.queue.empty() == False:
		print self.queue.get()
	#for row in q:
	#	print row
		
	
"""
	The plugins determine the context-based operation to be undertaken:
		- cleansing data
		- concept mining
		- ...
"""
class Plugin(Thread):
	def __init__(self,context,bag):
		Thread.__init__(self);
		self.context 	= context ;
		self.bag 	= bag
		self.queue	= None
		self.lock 	= None
	"""
		setting a queue in case the client has chosen to perform multi-threading
		@param queue	python built-in queue
	"""
	def init(self,queue,lock):
		self.queue = queue
		self.lock = lock 
"""
	This class will mine context and attempt to find various representations of a given word

	Use Case
	If a stakeholder wants to cleanup address fields,
	This class should be able to make an inference like Ave is Avenue with a degree of confidence
"""
class Clean(Plugin):
	def __init__(self,context,bag):
		Plugin.__init__(self,context,bag) ;
		self.info = {}
	"""
		@pre len(context) == len(bag)
	"""
	def run(self):
		N = len(self.context)
		
		imatches = []
		found = {}
		Y = range(0,len(self.bag))
		for i in range(0,N):
			Xo_ = list(self.bag[i])	# skip_gram
			#Y = (Set(range(0,N)) - (Set([i]) | Set(imatches)))
			for ii in Y:
				if self.bag[i] == self.bag[ii]:
					imatches.append(ii) ;
					continue
				#
				# We are sure we are not comparing the identical phrase
				# NOTE: Repetition doesn't yield learning, rather context does.
				# Lets determine if there are common terms
				#
				Z = Set(self.bag[i]) & Set(self.bag[ii])
				
				if len(Z) > 0 and len(Xo_) > 0:

					Xo_ 	= Set(Xo_) - Z # - list(Set(bag[i]) - Set(bag[ii]))
					Yo_ 	= Set(self.bag[ii]) - Z #list(Set(bag[ii]) - Set(bag[i]))
					size 	= len(Xo_)
					g = NGram(Yo_)	
					for term in Xo_:
						
						xo = g.search(term)
						if len(xo) > 0 and len(term) < 4:
							xo = xo[0]
						else:
							continue;
						xo = list(xo)
						xo_i = self.bag[i].index(term) 
						yo_i = self.bag[ii].index(xo[0])
						#
						# We have the pair, and we will compute the distance
						#
						ratio = fuzz.ratio(term,xo[0])/100
						is_subset = len(Set(term) & Set(xo[0])) == len(term)
						if is_subset and len(term) < len(xo[0]) and ratio > 0.5 and xo_i ==yo_i:
							
							xo[1] = [ratio,xo_i]
							if (term not in self.info):
								#xo[1] = ratio
								self.info[term] = [term,xo[0]]+xo[1]
							elif term in self.info and ratio > self.info[term][1] :							
								self.info[term] = [term,xo[0]]+xo[1]
							
							
							#imatches.append(ii)
							break;
		#
		# At this point we consolidate all that has been learnt
		# And make it available to the outside word, otherwise client should retrieve it
		#
		self.lock.acquire()
		if self.queue is not None:
			
			for term in self.info:	
				value = ['thread # ',self.name]+list(self.info[term])							
				self.queue.put(value)
		self.lock.release()
				
			
			
				#print term, self.info[term]	
f = open('/Users/steve/Downloads/data/Accreditation_2015_12/Accreditation_2015_12.csv','rU')
data = [line.split(',') for line in f]
thread = SimpleContextLearner(data,2)
thread.start()
