"""
	Repair.py

	This file is designed to repair character delimited files by implementing anomaly detection and using the probability distribution to repair files.
	The dammages of the file are of 2 kinds:
		- Arbitrary delimiter, fixed by finding misplaced fields and merging fields
		- Arbitrary new line, fixed by aggregating records

	Assumptions:
		It is assumed that most of the data in a file are properly structured i.e the mishaps should be marginal
		
	Method:
		The distribution of field counts by records allows us to determine the expected number of fields a record has/should have. This also allows us to know what type of repair a given record will lend itself to.
		The distribution of fields having data or not enables location of misplaced field and determine merger (leftward or rightward)
		There are two types of dammaged records respectively repaired by merger and/or aggregation:
			- Arbitrary delimiters: they have more fields than expected 
			- Arbitrary new line: they will have less fields than expected


	Features:
		The delimiters are automatically detected: COMMA, TAB, PIPE, SEMI-COLON any other delimiter must be manually specified
		Non-ascii characters are automatically removed
		Generated CSV output in UTF-8
	
	TODO:
		Have a mode where the utility scans an entire folder and processes it using some form of semaphore/threads
	Usage:
		python Repair.py --in-path <file> --out-path <folder> [--delimiter <xchar>]

"""
from __future__ import division
from sets import Set
import os
import sys
import shutil
import re
import numpy as np
import threading


class Repair(threading.Thread):
	"""
		The class will be able to determine a delimiter if none is provided.
		The default delimiters it attempts to identify are COMMA, TAB, PIPE SEMI_COLON
	"""
	def __init__(self,path,out,delim = None):
		threading.Thread.__init__(self) ;
		f 	= open(path,'r') ;
		lines = [] #-- it's plural
		line = f.readline()
		
		if delim is None:
		
			delim = [',','\t','|',';'] 
			delim = list(Set(delim) & Set(line)) 
			if len(delim) > 0:
				self.xchar = delim[0] ;
			
		else:
			self.xchar = delim ;
		#
		# This portion of code is designed to determine the proper number of fields a record should have
		# This is limited to a specified number of records because the file is read sequentially we can't randomly sample records
		#
		try:
			LIMIT = 10000
			lines = [f.readline() for i in range(LIMIT)] ;
			m = {}
			self.ncols = 0
			max_value = 0
			for aline in lines :
				i = str(len(aline.split(self.xchar)))
				if i not in m :
					m[i] = 0
				m[i] = m[i] + 1
				
				if m[i] > self.ncols :
					self.ncols = int(i)
			#
			# After sampling LIMIT records and assessing where the number of fields the majority has
			# We compare our findings to the first row, if there's a discrepency then it suggests the first row was broken (Alas)
			# The person exporting the data was too careless to look at what they did ... we got it !!
			#
			if line.split(self.xchar) != self.ncols:
				for aline in lines:
					if aline.split(self.xchar) == self.ncols:
						line = aline ;
						break;
		except:
			self.ncols = None
			pass
		#
		f.close() ;
		#
		# At this point we should have a proper delimiter and an output folder
		#
		
		if os.path.exists(path) == True :
			self.logs 		= []
			self.header 		= self.clean(line).split(self.xchar) 
			
			if self.ncols is None:
				self.ncols = len(self.header) ;
				
			self.passed_count	= 0;
			self.failed_rec 	= [] 	# excess columns ..
			self.partial_rec 	= []	# insuffient columns
			self.px 		= [0 for i in range(self.ncols)]
			self.path		= path
			
			self.out		= out;
			i 			= len( path.split(os.sep)) -1 
			self.name 		=  path.split(os.sep)[i]
			
			
			#
			# Let's check the output folder and structure it accordingly
			# We create the folder structure below, and to be augmented
			#
			if os.path.exists(out) == False:
				folders = [out]+[ "".join([out,os.sep,name]) for name in ['stats','clean','failed']] 
				[os.mkdir(folder) for folder in folders]
			#
			# After creating the structures we need to create the output files for augmentation
			#
			
			self.files = {}
			for name in ['clean','failed','stats'] :
				suffix = "".join([self.name,'_',name])
				path = "".join([self.out,os.sep,name,os.sep,suffix]) ;
				self.files[name]  = path ;
				
				if name != 'stats' and os.path.exists(path) == False:
					f = open(path,'w') ;
					f.write(self.format(self.header)) ;
					f.close();
			
	"""
		This function is designed to remove non-ascii characteers and insure the line has utf-8 encoding
		@param line	unsplit line as read from the file
	"""
	def clean(self,line):
		#if len(re.findall('[\x00-\x1F,\xC0-\xFF,\x7F]',line)) > 0 and self.xchar != '\t':
		#	line = re.sub('[\x00-\x1F,\xC0-\xFF,\x7F,\n,\r,\\,\\\\,\b]',' ',line).strip()			
		
		#line = re.sub('[\n,\r,\v,\b]',' ',line).strip()			
		line =  re.sub('(\n|\r|\v|\b)',' ',line.strip())
		return line.format('utf8');
	
	"""
		This function returns a binary stream i.e the stream will identify fields with data and others without
		@param row in the file
	"""
	def stream(self,row) :
		
		if self.xchar in row:
			row = row.split(self.xchar) ;
		ncols = len(row) ;
		i = 0;
		rstream = []			
		for col in row:
			if col.strip() != '' and len(re.findall('\w+',col)) > 0:
				value = 1  ;
			else:
				value = 0;
			rstream.append( value)
			i = i + 1
		#
		# at this point we can compute ncols, f-ones,entropy
		f_ones= round(np.sum(rstream) / ncols,2) ;
		if f_ones == 0:
			h = 0;
		return rstream;
	"""
		This function is designed to classify a given record based on what we know the number of columns ought to be
		@TODO: 
		In this case we assume the first row would inform us of the norm, but in case it doesn't we should write a more elaborate function to make the determination as to what is the appropriate number of columns
	"""
	
	def classify(self,row):
		if self.xchar in row :
			row = row.split(self.xchar) ;
		if len(row) > self.ncols:
			self.failed_rec.append(row) ;
		else:
			self.partial_rec.append(row) ;
	"""
		This function will attempt to trim records that have extra delimiters their columns is greater than the expectation
		@param failed_line	a record with an extra delimiter
	"""
	def trim(self,failed_line):
		if self.xchar in failed_line:
			row = failed_line.split(self.xchar) ;
		else:
			row = failed_line;
		#	
		RANDOM = 0.5
		print 'self.px is'
		print self.px
		print 'self.passed_count is'
		print self.xchar	
		lpx = np.divide(self.px,float(self.passed_count))
		lpx_min = list(Set([px  for px in lpx if float(px) < RANDOM])) ;
		#lpx_min.sort() ;
		lindex = []
		delta = len(row) - self.ncols
		hx = self.stream(row) ;
		N = len(hx) ;
		N = len(lpx) ;
		i = 0;
		
		
		while i < N:
			
			p = int(lpx[i]) != hx[i] ; 	#-- Identify a disagreement
			q = lpx[i] in lpx_min ;		#-- Probability of the disagreement
			
			if p and q: #-- is there a field in a place where there shouldn't be one
				lindex.append(i) ;
				if len(lindex) == delta:
					break;
			i = i + 1
		#
		# At this point we have the candidate for merging
		# @TODO: Find a better way to determine this shit
		#
		
		#
		# At this point we are rebuilding the fields according the the probabilities observed
		#
		#print failed_line
		for i in lindex:
			value = row[i].strip()
			#
			# Mundane heuristic that will navigate to the lowest probability in hopes to augment it
			#
			if i == 0 or lpx[i+1] < lpx[i]:
				row[i] = self.clean(" ".join([value , row[i+1] ]))
				index = i + 1				
			else:
				row[i-1] = " ".join ([row[i-1],value ])
				index = 1
			del row[index] 
		
		return row
	"""
	"""
	def run(self):
		in_file = open(self.path) ;
		line = in_file.readline() ;
		self.passed_count = 0;
		
		path = self.files['clean'] ;
		out_file = open(path,'a')
		for line in in_file:
			line	= self.clean(line) ;
			stream =  self.stream(line) 
			if len(stream) == self.ncols:
				self.passed_count = self.passed_count + 1
				self.compute(stream) 

				out_file.write(self.format(line.split(self.xchar))) ;
		
				
			else:
				self.classify(line) ;
		out_file.close()
		in_file.close()
		self.repair() ;
	"""
	 This function is designed to post/publish statistics about what was done
	"""
	def post (self,subject,action,value=''):
		f = open(self.files['stats'],'a') 
		f.write(",".join([subject,action,str(value),"\n"]))
		f.close();
	def repair(self):
		N = float(self.passed_count) ;
		header = ['file','status','records']+self.header
		px = [self.name,'passed',self.passed_count]+list(np.divide(self.px,N) )
		
		#f = open(self.files['stats'],'a') ;
		#f.write(self.format(header)) ;
		#f.write(self.format(px)) ;
		
		#f.close() ;
		size = self.passed_count + len(self.failed_rec)+len(self.partial_rec)
		self.post('records','received',size)
		self.post('records','passed',self.passed_count)
		self.post('records','broken',size - self.passed_count) 
		
		path = self.files['clean']
		of = open(path,'a')
		failure = []
		if len(self.failed_rec) > 0:
			#
			# The records here require trimming
			#
			
			i = 0
			for row in self.failed_rec:
				line = self.trim(row) 	
				stream = self.stream(line)
				
				if len(stream) == self.ncols:
					self.compute(stream) ;
					
					of.write(self.format(line)) ;
					
					i = i + 1
				else:
					#
					# utter failures
					failure.append(row);
			
			#
			# post the computation results to the appropriate destitation
			#
			if i > 0 or len(failure) > 0:
				#stats_file = open(self.files['stats'],'a') ;
				if i > 0:								
					#stats_file.write(self.format([self.name,'repaired-class-1', str(i) ])) ;
					self.post('heuristic','repaired',i)
				if len(failure) > 0:
					i = 100* float(len(failure)/len(self.failed_rec))
					#stats_file.write(self.format([self.name,'failed-class-1', str(i) ])) ;
					self.post('heuristic','failed',len(failure))
					fail_file = open(self.files['failed'],'a') ;
					[ fail_file.writelines(line) for line in failure]
					fail_file.close()
				
				#stats_file.close();
			
		if len(self.partial_rec):
			r = self.pad(self.partial_rec) 
			passed = 0;
			failed = 0;
			
			if len(r['repaired']) > 0:
				#
				# This needs to be written somewhere
				#print r['repaired']
				#stream = self.stream(line)
				#self.compute(stream) ;
				[of.write(self.format(line)) for line in r['repaired']] ;
				count = str(len(r['repaired']))+','+str(len(self.partial_rec)  )+','+str(len(r['failed']))
				self.post('aggregate','passed',count) ;
			if len(r['failed']) > 0:
				failures = r['failed'] 
				#i = 100* float(len(failures)/len(self.failed_rec))
				#stats_file.write(self.format([self.name,'failed-class-1', str(i) ])) ;
				fail_file = open(self.files['failed'],'a') ;
				[ fail_file.writelines(line) for line in failure]
				fail_file.close()
				self.post('aggregate','failed',len(r['failed'])) ;
			
		of.close() ;
	"""
		This function is designed to add padding to records tthat fall short on record count
		@param lrows	list of records that fall short on the column count
	"""
	def pad(self,lrows):
		
		cols 	= []
		repaired_ = []
		failure_  = [] ;
		row = []
		for line in lrows:
			if isinstance(line,list) == False:
				line = [line]
			row = row + line
			#row = self.clean(row )
			stream = self.stream(row) ;
			if len(stream) > self.ncols:
				row = self.trim(row) ;
				repaired_.append(row) ;	
				row = []
			elif len(stream) == self.ncols:
				repaired_.append(row) ;
				row = []
		r = {}
		r['repaired'] = repaired_ 
		r['failed'] = row
		return r #repaired_	
	
	"""
		Formatting the to wrap the values in a quoted string
	"""
	def format(self,row):
		return ",".join([ col.strip() for col in row])+"\n"
	
	"""
		This function performs basic counts on the existance of a value in a field or not
		@param x_row 	binary vector representation of a row
	"""
	def compute (self,x_row):
		self.px = np.add(self.px,x_row) ;

if len(sys.argv) > 1:
	param = {} 
	for i in range(1,len(sys.argv)) :
		if re.match('^--(.+)$',sys.argv[i]) is not None:
			id = sys.argv[i].replace('--',''); 
			value = sys.argv[i+ 1] ;
			param[id] = value 
			#i = i + 2 ;
			#if i > len(sys.argv) -1:
			#	break;
	"""
		In order to run with parameters the following must be provided
		--in-path <file path>
		--out-path <out-path>
	"""
	
	missed =  Set(param.keys()) ^ Set(['in-path','out-path']) 
	
	if len(missed) == 0 or ('delimiter' in missed and len(missed) == 1):
		delimiter = None ;
		if 'delimiter' in param:
			delimiter = param['delimiter'] 
		
		thread = Repair(param['in-path'],param['out-path'],delimiter) ;
		thread.start()
