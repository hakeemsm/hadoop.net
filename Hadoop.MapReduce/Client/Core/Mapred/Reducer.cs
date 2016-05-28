using System.Collections.Generic;
using Org.Apache.Hadoop.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	/// <summary>
	/// Reduces a set of intermediate values which share a key to a smaller set of
	/// values.
	/// </summary>
	/// <remarks>
	/// Reduces a set of intermediate values which share a key to a smaller set of
	/// values.
	/// <p>The number of <code>Reducer</code>s for the job is set by the user via
	/// <see cref="JobConf.SetNumReduceTasks(int)"/>
	/// . <code>Reducer</code> implementations
	/// can access the
	/// <see cref="JobConf"/>
	/// for the job via the
	/// <see cref="JobConfigurable.Configure(JobConf)"/>
	/// method and initialize themselves.
	/// Similarly they can use the
	/// <see cref="System.IDisposable.Close()"/>
	/// method for
	/// de-initialization.</p>
	/// <p><code>Reducer</code> has 3 primary phases:</p>
	/// <ol>
	/// <li>
	/// <b id="Shuffle">Shuffle</b>
	/// <p><code>Reducer</code> is input the grouped output of a
	/// <see cref="Mapper{K1, V1, K2, V2}"/>
	/// .
	/// In the phase the framework, for each <code>Reducer</code>, fetches the
	/// relevant partition of the output of all the <code>Mapper</code>s, via HTTP.
	/// </p>
	/// </li>
	/// <li>
	/// <b id="Sort">Sort</b>
	/// <p>The framework groups <code>Reducer</code> inputs by <code>key</code>s
	/// (since different <code>Mapper</code>s may have output the same key) in this
	/// stage.</p>
	/// <p>The shuffle and sort phases occur simultaneously i.e. while outputs are
	/// being fetched they are merged.</p>
	/// <b id="SecondarySort">SecondarySort</b>
	/// <p>If equivalence rules for keys while grouping the intermediates are
	/// different from those for grouping keys before reduction, then one may
	/// specify a <code>Comparator</code> via
	/// <see cref="JobConf.SetOutputValueGroupingComparator(System.Type{T})"/>
	/// .Since
	/// <see cref="JobConf.SetOutputKeyComparatorClass(System.Type{T})"/>
	/// can be used to
	/// control how intermediate keys are grouped, these can be used in conjunction
	/// to simulate <i>secondary sort on values</i>.</p>
	/// For example, say that you want to find duplicate web pages and tag them
	/// all with the url of the "best" known example. You would set up the job
	/// like:
	/// <ul>
	/// <li>Map Input Key: url</li>
	/// <li>Map Input Value: document</li>
	/// <li>Map Output Key: document checksum, url pagerank</li>
	/// <li>Map Output Value: url</li>
	/// <li>Partitioner: by checksum</li>
	/// <li>OutputKeyComparator: by checksum and then decreasing pagerank</li>
	/// <li>OutputValueGroupingComparator: by checksum</li>
	/// </ul>
	/// </li>
	/// <li>
	/// <b id="Reduce">Reduce</b>
	/// <p>In this phase the
	/// <see cref="Reducer{K2, V2, K3, V3}.Reduce(object, System.Collections.IEnumerator{E}, OutputCollector{K, V}, Reporter)
	/// 	"/>
	/// method is called for each <code>&lt;key, (list of values)&gt;</code> pair in
	/// the grouped inputs.</p>
	/// <p>The output of the reduce task is typically written to the
	/// <see cref="Org.Apache.Hadoop.FS.FileSystem"/>
	/// via
	/// <see cref="OutputCollector{K, V}.Collect(object, object)"/>
	/// .</p>
	/// </li>
	/// </ol>
	/// <p>The output of the <code>Reducer</code> is <b>not re-sorted</b>.</p>
	/// <p>Example:</p>
	/// <p><blockquote><pre>
	/// public class MyReducer&lt;K extends WritableComparable, V extends Writable&gt;
	/// extends MapReduceBase implements Reducer&lt;K, V, K, V&gt; {
	/// static enum MyCounters { NUM_RECORDS }
	/// private String reduceTaskId;
	/// private int noKeys = 0;
	/// public void configure(JobConf job) {
	/// reduceTaskId = job.get(JobContext.TASK_ATTEMPT_ID);
	/// }
	/// public void reduce(K key, Iterator&lt;V&gt; values,
	/// OutputCollector&lt;K, V&gt; output,
	/// Reporter reporter)
	/// throws IOException {
	/// // Process
	/// int noValues = 0;
	/// while (values.hasNext()) {
	/// V value = values.next();
	/// // Increment the no. of values for this key
	/// ++noValues;
	/// // Process the &lt;key, value&gt; pair (assume this takes a while)
	/// // ...
	/// // ...
	/// // Let the framework know that we are alive, and kicking!
	/// if ((noValues%10) == 0) {
	/// reporter.progress();
	/// }
	/// // Process some more
	/// // ...
	/// // ...
	/// // Output the &lt;key, value&gt;
	/// output.collect(key, value);
	/// }
	/// // Increment the no. of &lt;key, list of values&gt; pairs processed
	/// ++noKeys;
	/// // Increment counters
	/// reporter.incrCounter(NUM_RECORDS, 1);
	/// // Every 100 keys update application-level status
	/// if ((noKeys%100) == 0) {
	/// reporter.setStatus(reduceTaskId + " processed " + noKeys);
	/// }
	/// }
	/// }
	/// </pre></blockquote>
	/// </remarks>
	/// <seealso cref="Mapper{K1, V1, K2, V2}"/>
	/// <seealso cref="Partitioner{K2, V2}"/>
	/// <seealso cref="Reporter"/>
	/// <seealso cref="MapReduceBase"/>
	public interface Reducer<K2, V2, K3, V3> : JobConfigurable, Closeable
	{
		/// <summary><i>Reduces</i> values for a given key.</summary>
		/// <remarks>
		/// <i>Reduces</i> values for a given key.
		/// <p>The framework calls this method for each
		/// <code>&lt;key, (list of values)&gt;</code> pair in the grouped inputs.
		/// Output values must be of the same type as input values.  Input keys must
		/// not be altered. The framework will <b>reuse</b> the key and value objects
		/// that are passed into the reduce, therefore the application should clone
		/// the objects they want to keep a copy of. In many cases, all values are
		/// combined into zero or one value.
		/// </p>
		/// <p>Output pairs are collected with calls to
		/// <see cref="OutputCollector{K, V}.Collect(object, object)"/>
		/// .</p>
		/// <p>Applications can use the
		/// <see cref="Reporter"/>
		/// provided to report progress
		/// or just indicate that they are alive. In scenarios where the application
		/// takes a significant amount of time to process individual key/value
		/// pairs, this is crucial since the framework might assume that the task has
		/// timed-out and kill that task. The other way of avoiding this is to set
		/// &lt;a href="
		/// <docRoot/>
		/// /../mapred-default.html#mapreduce.task.timeout"&gt;
		/// mapreduce.task.timeout</a> to a high-enough value (or even zero for no
		/// time-outs).</p>
		/// </remarks>
		/// <param name="key">the key.</param>
		/// <param name="values">the list of values to reduce.</param>
		/// <param name="output">to collect keys and combined values.</param>
		/// <param name="reporter">facility to report progress.</param>
		/// <exception cref="System.IO.IOException"/>
		void Reduce(K2 key, IEnumerator<V2> values, OutputCollector<K3, V3> output, Reporter
			 reporter);
	}
}
