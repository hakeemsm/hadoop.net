using System.Collections.Generic;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce
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
	/// <see cref="Mapper{KEYIN, VALUEIN, KEYOUT, VALUEOUT}"/>
	/// .
	/// </li>
	/// <li>
	/// Provide the
	/// <see cref="RecordReader{KEYIN, VALUEIN}"/>
	/// implementation to be used to glean
	/// input records from the logical <code>InputSplit</code> for processing by
	/// the
	/// <see cref="Mapper{KEYIN, VALUEIN, KEYOUT, VALUEOUT}"/>
	/// .
	/// </li>
	/// </ol>
	/// <p>The default behavior of file-based
	/// <see cref="InputFormat{K, V}"/>
	/// s, typically
	/// sub-classes of
	/// <see cref="Org.Apache.Hadoop.Mapreduce.Lib.Input.FileInputFormat{K, V}"/>
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
	/// applications since record boundaries are to respected. In such cases, the
	/// application has to also implement a
	/// <see cref="RecordReader{KEYIN, VALUEIN}"/>
	/// on whom lies the
	/// responsibility to respect record-boundaries and present a record-oriented
	/// view of the logical <code>InputSplit</code> to the individual task.
	/// </remarks>
	/// <seealso cref="InputSplit"/>
	/// <seealso cref="RecordReader{KEYIN, VALUEIN}"/>
	/// <seealso cref="Org.Apache.Hadoop.Mapreduce.Lib.Input.FileInputFormat{K, V}"/>
	public abstract class InputFormat<K, V>
	{
		/// <summary>Logically split the set of input files for the job.</summary>
		/// <remarks>
		/// Logically split the set of input files for the job.
		/// <p>Each
		/// <see cref="InputSplit"/>
		/// is then assigned to an individual
		/// <see cref="Mapper{KEYIN, VALUEIN, KEYOUT, VALUEOUT}"/>
		/// for processing.</p>
		/// <p><i>Note</i>: The split is a <i>logical</i> split of the inputs and the
		/// input files are not physically split into chunks. For e.g. a split could
		/// be <i>&lt;input-file-path, start, offset&gt;</i> tuple. The InputFormat
		/// also creates the
		/// <see cref="RecordReader{KEYIN, VALUEIN}"/>
		/// to read the
		/// <see cref="InputSplit"/>
		/// .
		/// </remarks>
		/// <param name="context">job configuration.</param>
		/// <returns>
		/// an array of
		/// <see cref="InputSplit"/>
		/// s for the job.
		/// </returns>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public abstract IList<InputSplit> GetSplits(JobContext context);

		/// <summary>Create a record reader for a given split.</summary>
		/// <remarks>
		/// Create a record reader for a given split. The framework will call
		/// <see cref="RecordReader{KEYIN, VALUEIN}.Initialize(InputSplit, TaskAttemptContext)
		/// 	"/>
		/// before
		/// the split is used.
		/// </remarks>
		/// <param name="split">the split to be read</param>
		/// <param name="context">the information about the task</param>
		/// <returns>a new record reader</returns>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public abstract RecordReader<K, V> CreateRecordReader(InputSplit split, TaskAttemptContext
			 context);
	}
}
