#Motivation

This engine demonstrates how a learning component can serve as a solution to a problem that would otherwise have been addressed by developing custom parsers to every field and error associated with a dataset. 

**1. Reducing data preparation tasks**

In machine learning most of time is spent on [janitorial tasks] (http://www.pcworld.com/article/3047665/hottest-job-data-scientists-say-theyre-still-mostly-digital-janitors.html]). The file repair utility handles some of that.

**2. Integration of learning components in software engineering**

Writing parsers for every case/field/error and dataset is NEITHER practical NOR scalable. So we propose a learning algorithm/component because it is code written once that can generalize without new code being written

# Character File Repair Utility

This utility repairs character delimited files that have either a misplaced delimiter and or an arbitrary new line where it shouldn't.
The utility leverages machine-learning approaches and can be run in either 

* Filter mode, where the utility removes structurally problematic records
* Repair mode, where the utility will attempt to repair problematic records.

In general character delimited files issues are rooted in arbitrary delimiter or unexpected new lines. Additionally we address the issue of encoding and non-ascii character. The learning algorithm we design focuses on these issues.

<script src="https://google-code-prettify.googlecode.com/svn/loader/run_prettify.js"></script>

#Description

This engine repairs any character delimited file using an approach
based on [anomaly detection](https://en.wikipedia.org/wiki/Anomaly_detection) & [ensemble learning](https://en.wikipedia.org/wiki/Ensemble_learning) across n-features in any
dataset. The file repair engine also provides a quantitative assessment of the
data to determine what kind of processing this data would lend itself to.


#Features:

- Filter or Repair broken records automatically
- Include scrubbing the data for non-ascii
	characters and extra whitespaces. 
- Quantitative assessment of the data processed

	
This engine can be applied in either filter-mode or repair-mode:

- Filter mode is a passive mode (recommended if data loss is acceptable)
- Repair mode is designed to minimize data loss

**Example Usage**

  <code class="prettify">
  import repair
  </code>
  
  <code class="prettify">
  repairThread = repair.Repair('sample-broken.csv')
  </code>
  
  <code class="prettify">
  repairThread.start()
  </code>
<br>

The output is contained in a folder called <i>tmp</i>.
