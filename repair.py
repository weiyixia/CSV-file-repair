"""
	Steve L. Nyemba<steve@the-phi.com>
	https://the-phi.com, The Phi Technology LLC

	This file implements a character delimited file repair approach based on anomaly detection & ensemble learning across n-features in the dataset
	The reason for this implementation is to streamline data ingestion, in the process of repairing and assessing the data this engine will provide quantitative assessment of the data
	The quantitative assessment of the data is designed to determine what kind of processing this data would lend itself to

	DESIGN:
		- Sample records from which we derive statistics
			a. The assumption here is that most records are properly submitted
			b. A dataset has structural consistency
		- From the sample we will derive the following:
			a. Probability/Frequency of a field having data
			b. Field length on average
			c. Field type= {integer, double,date}
			d. 
		- Additional features will include scrubbing the data for non-ascii characters and extra whitespaces
		This should in theory allow for a clean output that is easy to ingest. 

		The code can be applied in either filter-mode or repair-mode:
			- Filter mode is a passive mode (recommended if data loss is acceptable)
			- Repair mode is designed to minimize data loss

"""

