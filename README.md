# Character File Repair Utility
==
<script src="https://google-code-prettify.googlecode.com/svn/loader/run_prettify.js"></script>

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
