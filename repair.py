"""
Weiyi Xia <xwy0220@gmail.com>
Steve L. Nyemba<steve@the-phi.com>
Version 1.1

This file implements an automatic character delimited file repair approach
based on anomaly detection & ensemble learning across n-features in the
dataset. The file repair engine also provides quantitative assessment of the
data to determine what kind of processing this data would lend itself to.

DESIGN:
	- Filter & repairs both assume the number of features of a dataset
	matters and serves as the basis for determining whether or not the
	records can be used or not.
		a. records with more features than expected have one or
	        more unexpected delimiters
		b. records with less features have an unexpected new
		lines within

	- Sample records from which we derive statistics
		a. The assumption here is that most records are
		properly submitted
		b. A dataset has structural consistency
	
	- From the sample we will derive the following:
		a. Probability/Frequency of a field having data
		b. Field length on average
		c. Field type= {integer, double,date}
	- Additional features will include scrubbing the data for non-ascii
	characters and extra whitespaces. This should in theory allow for a
	clean output that is easy to ingest. 

	The code can be applied in either filter-mode or repair-mode:
		- Filter mode is a passive mode (recommended if data
		  loss is acceptable)
		- Repair mode is designed to minimize data loss

@TODO:
	- Implement input the same way outpout was implemented and find a way
	to tightly couple input/output streams This allows us to be able to
	work from big-table, dropbox, disk, one-drive or google-drive
	transparently
	
In order to execute the program 
	
	from repair import Repair
	thread = Repair('<path-to-file>',<'output-folder'>)
	thread.start()

"""

from __future__ import division
import numpy as np
from threading import Thread
import re
import sys
import os
import uuid

"""

This is the base class that from which all methods of inspecting a record are
derived from.

The Inspect subclasses will assume the sample that is provided has already been
vetted and thus is representative and delimited appropriately

"""
class Inspect(Thread):
	"""

	@param:
		sample:	list of rows, each row is a vector (note a list of
		lists is a matrix)

	"""
	def __init__(self,sample):
		Thread.__init__(self)
		self.sample = self.convert(sample);
		self.ncols = len(sample[0]);
		self.nrows = len(sample)
		
	"""

	This function is design to assert a particular row. The assertion is
	returned as a binary vector designated fields that pass and others that
	don't.

	@param:
		row: a row of a file

	"""
	def inspect(self,row):
		pass
	"""

	This function is designed to convert the sample into usable format
	depending on the field inspection method implemented By default it will
	only return the sample and must be overriden by base classes

	"""
	def convert(self,sample):
		return sample

"""

The idea behind assessing a length from a sample will simply consist in
determining compute the average length and the variance If the variance
is small enough we can assume the length of the field is equal to it's
average.

"""
class InspectFieldLength(Inspect):
	def __init__(self,sample):
		Inspect.__init__(self,sample) ;	
		self.nrows = self.nrows -1 #-- because we skip the header row
	"""
		This function is designed to put the sample data into usable
form : in our case every column will be converted into the length
		@param sample	dataset with values
	"""
	def convert(self,sample):
		return [[len(col.strip()) for col in row]for row in sample]

	def run(self):
		"""

		let's compute average length and variance and the mean.
		The computation of column based sum/variance is performed by
		transposing the data structure because the loop operates on a row
		basis.
		
		In order to begin we will convert the sample to a numeric matrix.

		"""
		self.mean = [np.mean(row) for row in np.array(self.sample[1:]).transpose()]
		self.var = [np.var(row) for row in np.array(self.sample[1:]).transpose()]
		threshold = 0.1
		for i in range(0,self.ncols):
			if self.var[i] > threshold :
				self.mean[i] = 0
			else:
				self.mean[i] = np.round(self.mean[i],0)
		
	"""

	This function is design to assert a particular row. The assertion is
	returned as a binary vector designated fields that pass and others that
	don't.

	@param:
		row: a row of a file

	"""
	def inspect(self,row):
		row = self.convert([row])[0]
		values = [ int(row[i] == self.mean[i]) for i in range(0,self.ncols)]
		#values = [ int(len(row[i].strip())== self.mean[i]) for i in range(0,self.ncols)]
		return values;

"""

This class is designed to inspect the probability of a field having data or not
having data. 

"""
class InspectProbability(Inspect):
	def __init__(self,sample):
		Inspect.__init__(self,sample) ;	
		self.nrows = self.nrows -1 #-- because we skip the header row

	"""

	This function convert a row to a list of binary variables each of
	which represents whether or not the corresponding column in the row is
	empty.

	"""
	def convert(self,sample):
		return [[ int(len(col.strip()) > 0) for col in row] for row in sample]
	
	def run(self):
		self.px = np.divide([np.sum(row) for row in np.array(self.sample[1:]).transpose()],self.nrows)
		self.px_values = list(self.px)
		threshold = 0.5
		for i in range(0,self.ncols):
			if self.px[i] > threshold:
				self.px[i] = 1
			else:
				self.px[i] = 0
	"""
	This function expresses agreement with an arbitrary record that is
	provided i.e zero suggests a disagreement (outliar, not enough
	information ...)
	  
	@pre len(row) == self.ncols

	@param:
		row: a row of a file
	"""
	def inspect(self,row):
		r = self.convert([row])[0]
		return list(np.multiply(self.px,r))

"""

This is a base class of determining field types, it is based on regular
expressions and a computation of probabilities.  Because the computation of
probabilities and inspection are identical we abstract it in this class. The
sub-classes will implement identification specifications using regex.

"""
class InspectFieldType(Inspect):
	def __init__(self,sample):
		Inspect.__init__(self,sample) ;
		self.nrows = self.nrows -1 #-- because we skip the header row
		#self.pattern = None;
	"""

	This function will convert the sample into a binary stream given a
	field is numeric or not.

	@param:
		sample: sample data (matrix)


	"""
	def convert(self,sample):
		pattern = self.getPattern();
		m = {True:1,False:0}
		return [[ m[re.match(pattern,col.strip()) is not None] for col in row] for row in sample]

	def run(self):
		"""

		We will compute the probability/frequencies found given the data converted
		the result will be a binary stream that will serve as a basis for assessment

		"""
		self.px_values = np.divide([ np.sum(row) for row in np.array(self.sample[1:]).transpose()],self.nrows)
		threshold = 0.5
		m = {True:1,False:0}
		self.px = [ m[p > threshold] for p in self.px_values]
		self.px = list(self.px)
		
	"""

	This function will assert if a row is a numeric type or not by
	returning a binary stream.

	"""
	def inspect(self,row):
		row = self.convert([row])[0]
		m = {True:1,False:0}
		return [m[row[i] == self.px[i]] for i in range(0,self.ncols)]
		
"""

This class is designed to inspect numeric types, this will include
integers and doubles

@TODO:
	We have to write perhaps more specific integer assessment and
	double assessment to be used.

"""
class InspectNumericField(InspectFieldType):
	def __init__(self,sample):
		InspectFieldType.__init__(self,sample) ;
	def getPattern(self):
		return '^[0-9]{1,}(\x2E[0-9])*$'

"""

This class is designed to inspect date types in 3 formats formats
	yyyy-dd-mm|dd-mm-yyyy|dd-M-yyyy.
The confirmation of data should be enforced by the length. We assume/conjecture
that a dataset has consistent output when it comes to dates (hopefully)

"""
class InspectDateField(InspectFieldType):
	def __init__(self,sample):
		Inspect.__init__(self,sample) ;
	"""

	This function will return a regex date pattern to be used to identify
	dates.

	"""
	def getPattern(self):
		return '(^\d{1,4}-\d{1,2}-\d{1,2}|^\d{1,2}-\d{1,2}-\d{2,4} | \d{1,2}[A-Z][a-z]-\d{1,4} )'

		
"""

This class is designed to sample the content and derive basic information upon
which meaningful information can be derived (estimating population parameters)

Method:
	- Estimate the size of the file in terms of number of rows/lines and
	extract a fifth (20%) as sample to determine number of columns &
	delimiter.
	- If the first assumption holds we can derive more meaningful
	statistics from this. 

	@TODO:
		Incorporate a mechanism like (bootstrap) where various sizes of
		fractions are tried in order to have confirmation on the basic
		assumption in order to yield better results

"""
class SampleBuilder(Thread):
	def __init__(self,path,size=-1):
		Thread.__init__(self)
		self.xchar = None
		self.ncols = None
		self.nrows = None
		self.FRACTION = 5
		if os.path.exists(path):
				
			#
			# Before we start anything we must have an idea of the number of rows we are dealing with
			# If the calling code has not given us a viable baseline then we should try to infer one (20% of the file)
			#
			if size < 0 :
				self.row_count(path)
				size = int(self.nrows/self.FRACTION)
			
			sample = self.read(path,size)
			#
			# Now that we have been able to determin the number of columns and the delimiter
			# We should create a viable sample that meets the column/delimiters found requirements
			#
			self.row_xchar(sample)
			self.col_count(sample) 
			
			self.sample = self.read(path,size)
			
		else:
			pass
	def row_count (self,path):
		if self.nrows is None:
			f = open(path,'rU')
			self.nrows = np.sum([1 for row in f])
			f.close()
		return self.nrows
	"""

	This function is designed to read content from a n-rows from a file.
	The function is called before delimiter/number od columns are
	determined. After delimiter is delimited and number of columns are
	determined, the read will insure the sample meets the criteria
	
	@param: 
		path: path to the file to sample
	@param:
		size: size of the sample to be read

	"""
	def read(self,path,size):
		f = open(path,'rU') ;
		sample = []
		for row in f:
			if self.xchar is not None and self.ncols is not None:
				row = row.split(self.xchar)
				if len(row) == self.ncols:
					row = self.clean(row) 
				else:
					continue ;
				
			sample.append(row)
			if len(sample) == size :
				break;
				
			
		f.close();
		return sample

	"""

	This function returns the number of columns found in a sample

	"""
	def col_count(self,sample):
		if self.ncols is None:
			m = {}
			i = 0
			for row in sample:
				id = str(len(row.split(self.xchar))) ;
				if id not in m:
					m[id] = 0
				m[id] = m[id] + 1
			
			index = m.values().index( max(m.values()) )
			self.ncols = int(m.keys()[index])
		
		
		return self.ncols;
			
			
			
	"""

	This function is designed to clean a row of data by removing non-ascii
	character.  The submitted row can either be a set of columns or a
	string and it will return the row in the type in which it was received.
	
	@pre
		self.xchar != None and len(sample) > 0
	@param:
		row: row to be cleaned

	"""
	def clean (self,row):
		
		if isinstance(row,list) == False:
			cols = row.split(self.xchar)
		else:
			cols = row ;
		r = [ re.sub('[^\x00-\x7F,\n,\r,\v,\b]',' ',col.strip()) for col in cols]
		
		if isinstance(row,list) == False:
			return (self.xchar.join(r)).format('utf-8') 
		else:
			return r

	"""
	
	This function is designed to find a viable delimiter given the sample,
	The assumption we have made is based upon the central limit theorem i.e
	marginal delimiters will have smaller columns

	"""

	def row_xchar(self,sample):
		if self.xchar is None:
			m = {',':[],'\t':[],'|':[]} 
			delim = m.keys()
			for row in sample:
				for xchar in delim:
					m[xchar].append(len(row.split(xchar)))
			#
			# The delimiter with the smallest least variance
			# This would be troublesome if there many broken records sampled
			#
			m = {id: np.var(m[id]) for id in m.keys() if np.mean(m[id])>1}
			index = m.values().index( min(m.values()))
			self.xchar = m.keys()[index]
		
		return self.xchar

"""

The output class hierarchy will determine where the content will be sent:
	- Disk
	- Cloud dropbox, google-drive, one-drive, s3 , big-table
	- Queue (rabbitmq)

"""
class Output(Thread):
	def __init__(self,filename,folder):
		self.filename	= filename ;
		self.folder	= folder;
		self.key 	= str(uuid.uuid1())
	def write(self,line):
		pass
class Disk(Output):
	def __init__(self,filename,folder):
		Output.__init__(self,filename,folder) ;
		
	def init(self):
		prefix = os.sep.join([self.folder])
		lfolders =[prefix] + [ os.sep.join([prefix,f]) for f in ['passed','fixed','broken','logs']]
		self.files = {}
		for folder in lfolders:
			if os.path.exists(folder) == False:
				print folder
				os.mkdir(folder)
			if folder != prefix:
				path = os.sep.join([folder,self.filename])
				f = open(path,'w') ;
				f.close()
				if re.match('^.*fixed.*$',folder) is not None:
					self.files['fixed'] = path
				elif re.match('^.*passed.*$',folder) is not None:
					self.files['passed'] = path
				elif re.match('^.*broken.*$',folder) is not None:
					self.files['broken'] = path
				elif re.match('^.*logs.*$',folder) is not None:
					self.files['logs'] = path
		
	"""

	This function will write a row to a file, the row would have been formatted prior to being used
	@param:
		id: identifier {passed,fixed,broken,log}
	        row: row to be written

	"""
	def write(self,id,row):
		if id in self.files:
			f = open(self.files[id],'a') ;
			f.write(row)
			f.close();
class Cloud(Disk):
	def __init__(self,filename,token):
		Disk.__init__(self,filename,token) ;
		self.token = token;
		pass
		
"""

This class is designed to perform basic filter operation in order to plainly
separate fields that do not meet the basic requirements in terms of number of
columns Basic filtering is based on distinguishing records on the basis of the
number of columns estimated in the sample

NOTE: This class will assign the clean method from the SamplerBuilder class so
as to use it within it's very own context and thus available thoughout the
class hierarchy i.e Repair class. This is possible because python is not a full
fledged object oriented language ;-) ... call it a clever design hack!!
	

"""
class Filter(Thread):
	def __init__(self,path,ofolder='tmp'):
		Thread.__init__(self)
		thread = SampleBuilder(path,1000) ;
		thread.start() ;
		thread.join() ;
		
		#
		# Let's determine the the filename and build out output structures
		# The files will be output to either disk or cloud ...
		#
		self.sample	= thread.sample ;
		self.ncols	= thread.ncols
		self.xchar	= thread.xchar
		self.filename 	= path.split(os.sep)
		self.path 	= path
		self.clean = thread.clean	#--pointer to the function
		self.logs = {}
		if len(self.filename) == 1:
			self.filename = self.filename[0]
		else:
			i = len(self.filename) -1 ;
			self.filename = self.filename[i]
		#
		# We need to have a handler to post the output stream to either cloud/queue/disk
		# This 
		self.handler = Disk(self.filename,ofolder) ;
		self.handler.init()
	
	def format (self,row):
		return ",".join(row)+'\n' ;

	"""

	The filtering will consist in being able to separate proper files
	against the number of columns

	"""
	def run(self):
		f = open(self.path,'rU') ;
		for row in f:
			row = self.clean(row.split(self.xchar)) ;
			if len(row) == self.ncols:
				self.post('passed',row) ;
			else:
				self.post('broken',row) ;
		f.close()
		#
		# We need to write out the logs at this point
		# The logs capture all that happened and in the class including the findings
		# @TODO: The data grouped here will be part of a report that will be charted
		#
	"""

	This function is designed to log unfit records with records that will
	be ignored

	"""
	def post(self,id,row):
		
		self.handler.write(id,self.format(row) ) ;
		if id not in self.logs:
			self.logs[id]= 0
		self.logs[id] = self.logs[id] + 1
"""

This class is designed to perform record repairs keeping the base class
identical and allowing to assess repairs

NOTE:
	- The base class has taken upon itself to extract the sample
	- The Inspector class hierarchy will use the sample found

"""
class Repair(Filter):
	def __init__(self,path,ofolder='tmp'):
		Filter.__init__(self,path,ofolder) ;
		self.extra 	= []
		self.partial	= []
		self.threads = {'px':InspectProbability(self.sample),'numeric':InspectNumericField(self.sample),'len':InspectFieldLength(self.sample),'date':InspectDateField(self.sample)} ;
		[thread.start() for thread in self.threads.values()]
		self.row_index = 0

	"""

	In addition to capturing and storing records this function will also
	classify broken records.

	"""
	def post(self,id,row):
		Filter.post(self,id,row)		
		if id == 'broken':
			if len(row) > self.ncols:
				
				self.extra.append( self.clean(row))
			else:
					
				self.partial.append(self.clean(row))
		else:
			self.current_row = row
	def run(self):
		Filter.run(self) ;
		ids = self.threads.keys()
		#
		# We need to make sure the threads have finished learning what they need to learn
		# It is only possible to continue if the threads have completed so we can run the repairs
		#
		while True:
			count = [ int(thread.isAlive() == False) for thread in self.threads.values()]
			if sum(count) == len(ids):
				break
		#
		# Now we can under take repairs:
		#	a. Records with extra delimiters will require fields to be merged
		#	b. Partial records will require they be aggregated with other records
		#
		
		if len(self.extra) > 0:
			m = [self.merge(row) for row in self.extra]
			[self.post('fixed',row) for row in m if row is not None]
		
		if len(self.partial) > 0:
			m = [self.aggregate(row) for row in self.partial]
			[self.post('fixed',row) for row in m if row is not None]
		print self.logs

	"""

	The merge operation consists in addressing records with an extra
	delimiter, the function will return the end-result

	@pre
		len(row) > self.ncols
	@param:
		row: row with extra delimiter

	"""
	def merge(self,row):
		#
		# Let's find a record that is out of place, 
		# A merger would require an alpha-numeric field to be involved,
		# Misplaced fields are identified by either a disagreement upon inspection of a field having data or not or an agreement with the wrong data type
		# @TODO: Consider adding inspecting type to make sure typing disagreement
		#
		pn = self.threads['numeric'].inspect(row)
		px = self.threads['px'].inspect(row[0:self.ncols])
		
		for i in range(0,self.ncols):
			
			#if px[i] == pi[i] and pn[i] == 0 :
			if (px[i] == 1 and pn[i] == 0) or px[i] == 0 :
				break
		rmrow = []	#-- right merge row, what the row would be like should it be merged right
		lmrow = []	#-- left merge row, what the row would be like should it be merged left

		
		#if px[i] != pi[i] and pn[i] == 0:
		if (px[i] == 1 and pn[i] == 0) or px[i] == 0 :
			value = row[i].strip()
			
			if i -1 >= 0:
				lmrow = list(row)
				lmrow [i-1]= " ".join([lmrow[i-1].strip(),value])
				lmrow = self.clean(lmrow)
				del lmrow [i]
			if i+1 < self.ncols:
				rmrow = list(row)
				rmrow[i+1] = " ".join([value,rmrow[i].strip()])
				rmrow = self.clean(rmrow)
				del rmrow [i]
		else:
			return None
		#
		# We find the best probabilistic fit for the evaluation we have performed 
		# The best fit is assessed by the sum operator: The evaluation with the most agreement will be the best fit
		#
		if len(rmrow) == 0:
			rvalue = 0
		else:
			rvalue = self.threads['px'].inspect(rmrow[0:self.ncols])[i]
		if len(lmrow) == 0:
			lvalue = 0
		else:
			lvalue = self.threads['px'].inspect(lmrow[0:self.ncols])[i]
		if rvalue > lvalue:
			nrow = rmrow ;
		else:
			nrow = lmrow ;
		
		#
		# At this point we need to inspect if the length of the rows match expectations
		# If not we continue the merge process until the row doesn't meet the preconditions to be processed here
		#
		if len(nrow) == self.ncols :
			#
			# We are settle on the merger and we should return the value
			# But before returning the value we need to make sure we have broader consensus on the repairs
			#
			m = np.sum([thread.inspect(nrow)[i] for thread in self.threads.values()])
			N = len(self.threads)
			threshold = 1/N #-- acceptance criteria
			if m/N > threshold:
				return nrow
			else:
				return None
		elif len(nrow) > self.ncols :
			#
			# At this point we assume there are more unexpected delimiters
			#
			return self.merge(nrow)
		else:
			return None

	"""

	This function is designed to repair records with an arbitrary an
	unexpected new line i.e the number of features would less than
	expectated number of features

	"""
	def aggregate(self,row):
		
		index = [i for i in range(0,len(self.partial)) if self.partial[i] == row]
		index = index[0] + 1
		nrow	= list(row)
		for i in range(index,len(self.partial)) :
			#if row == self.partial[i]:
			#	continue ;
			next_row = list(self.partial[i])
			#value = nrow[len(nrow)-1] +' '+ next_row[0]
			#nrow[len(nrow)-1] = value
			#del next_row[0]
			nrow = nrow + next_row ; #self.partial[i]
			
			if len(nrow) > self.ncols:
				r = self.merge(nrow)
				if r is not None:
					nrow = r
				else:
					del self.partial[0:i]
					return None;
			
			if len(nrow) == self.ncols:
				del self.partial[0:i]
				return nrow 
		return None
				
