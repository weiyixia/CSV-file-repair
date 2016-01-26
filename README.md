# CSV-file-repair
<script src="https://google-code-prettify.googlecode.com/svn/loader/run_prettify.js"></script>
A tool for automatically repairing a broken delimiter seperated file using statistics of the columns and inference.

It provides statistics of the contents of the file.

An example of usage is:

<code class="prettify">
import repair
</code>

<code class="prettify">
r = repair.Repair('sample-broken.csv')
</code>

<code class="prettify">
r.run()
</code>
<br>

The output is contained in a folder called <i>tmp</i>.
