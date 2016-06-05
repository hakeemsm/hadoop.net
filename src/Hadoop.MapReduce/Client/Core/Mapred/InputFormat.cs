using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	/// <summary>
	/// <code>InputFormat</code> describes the input-specification for a
	/// Map-Reduce job.
	/// </summary>
	/// <remarks>
	/// <code>InputFormat</code> describes the input-specification for a
	/// Map-Reduce job.
	/// <p>The Map-Reduce framework relies on the <code>InputFormat</code> of the
	/// job to:<p>
	/// <ol>
	/// <li>
	/// Validate the input-specification of the job.
	/// <li>
	/// Split-up the input file(s) into logical
	/// <see cref="InputSplit"/>
	/// s, each of
	/// which is then assigned to an individual
	/// <see cref="Mapper{K1, V1, K2, V2}"/>
	/// .
	/// </li>
	/// <li>
	/// Provide the
	/// <see cref="RecordReader{K, V}"/>
	/// implementation to be used to glean
	/// input records from the logical <code>InputSplit</code> for processing by
	/// the
	/// <see cref="Mapper{K1, V1, K2, V2}"/>
	/// .
	/// </li>
	/// </ol>
	/// <p>The default behavior of file-based
	/// <see cref="InputFormat{K, V}"/>
	/// s, typically
	/// sub-classes of
	/// <see cref="FileInputFormat{K, V}"/>
	/// , is to split the
	/// input into <i>logical</i>
	/// <see cref="InputSplit"/>
	/// s based on the total size, in
	/// bytes, of the input files. However, the
	/// <see cref="Org.Apache.Hadoop.FS.FileSystem"/>
	/// blocksize of
	/// the input files is treated as an upper bound for input splits. A lower bound
	/// on the split size can be set via
	/// &lt;a href="
	/// <docRoot/>
	/// /../hadoop-mapreduce-client/hadoop-mapreduce-client-core/mapred-default.xml#mapreduce.input.fileinputformat.split.minsize"&gt;
	/// mapreduce.input.fileinputformat.split.minsize</a>.</p>
	/// <p>Clearly, logical splits based on input-size is insufficient for many
	/// applications since record boundaries are to be respected. In such cases, the
	/// application has to also implement a
	/// <see cref="RecordReader{K, V}"/>
	/// on whom lies the
	/// responsibilty to respect record-boundaries and present a record-oriented
	/// view of the logical <code>InputSplit</code> to the individual task.
	/// </remarks>
	/// <seealso cref="InputSplit"/>
	/// <seealso cref="RecordReader{K, V}"/>
	/// <seealso cref="JobClient"/>
	/// <seealso cref="FileInputFormat{K, V}"/>
	public interface InputFormat<K, V>
	{
		/// <summary>Logically split the set of input files for the job.</summary>
		/// <remarks>
		/// Logically split the set of input files for the job.
		/// <p>Each
		/// <see cref="InputSplit"/>
		/// is then assigned to an individual
		/// <see cref="Mapper{K1, V1, K2, V2}"/>
		/// for processing.</p>
		/// <p><i>Note</i>: The split is a <i>logical</i> split of the inputs and the
		/// input files are not physically split into chunks. For e.g. a split could
		/// be <i>&lt;input-file-path, start, offset&gt;</i> tuple.
		/// </remarks>
		/// <param name="job">job configuration.</param>
		/// <param name="numSplits">the desired number of splits, a hint.</param>
		/// <returns>
		/// an array of
		/// <see cref="InputSplit"/>
		/// s for the job.
		/// </returns>
		/// <exception cref="System.IO.IOException"/>
		InputSplit[] GetSplits(JobConf job, int numSplits);

		/// <summary>
		/// Get the
		/// <see cref="RecordReader{K, V}"/>
		/// for the given
		/// <see cref="InputSplit"/>
		/// .
		/// <p>It is the responsibility of the <code>RecordReader</code> to respect
		/// record boundaries while processing the logical split to present a
		/// record-oriented view to the individual task.</p>
		/// </summary>
		/// <param name="split">
		/// the
		/// <see cref="InputSplit"/>
		/// </param>
		/// <param name="job">the job that this split belongs to</param>
		/// <returns>
		/// a
		/// <see cref="RecordReader{K, V}"/>
		/// </returns>
		/// <exception cref="System.IO.IOException"/>
		RecordReader<K, V> GetRecordReader(InputSplit split, JobConf job, Reporter reporter
			);
	}
}
