using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce
{
	/// <summary>
	/// <code>OutputFormat</code> describes the output-specification for a
	/// Map-Reduce job.
	/// </summary>
	/// <remarks>
	/// <code>OutputFormat</code> describes the output-specification for a
	/// Map-Reduce job.
	/// <p>The Map-Reduce framework relies on the <code>OutputFormat</code> of the
	/// job to:<p>
	/// <ol>
	/// <li>
	/// Validate the output-specification of the job. For e.g. check that the
	/// output directory doesn't already exist.
	/// <li>
	/// Provide the
	/// <see cref="RecordWriter{K, V}"/>
	/// implementation to be used to write out
	/// the output files of the job. Output files are stored in a
	/// <see cref="Org.Apache.Hadoop.FS.FileSystem"/>
	/// .
	/// </li>
	/// </ol>
	/// </remarks>
	/// <seealso cref="RecordWriter{K, V}"/>
	public abstract class OutputFormat<K, V>
	{
		/// <summary>
		/// Get the
		/// <see cref="RecordWriter{K, V}"/>
		/// for the given task.
		/// </summary>
		/// <param name="context">the information about the current task.</param>
		/// <returns>
		/// a
		/// <see cref="RecordWriter{K, V}"/>
		/// to write the output for the job.
		/// </returns>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public abstract RecordWriter<K, V> GetRecordWriter(TaskAttemptContext context);

		/// <summary>Check for validity of the output-specification for the job.</summary>
		/// <remarks>
		/// Check for validity of the output-specification for the job.
		/// <p>This is to validate the output specification for the job when it is
		/// a job is submitted.  Typically checks that it does not already exist,
		/// throwing an exception when it already exists, so that output is not
		/// overwritten.</p>
		/// </remarks>
		/// <param name="context">information about the job</param>
		/// <exception cref="System.IO.IOException">when output should not be attempted</exception>
		/// <exception cref="System.Exception"/>
		public abstract void CheckOutputSpecs(JobContext context);

		/// <summary>Get the output committer for this output format.</summary>
		/// <remarks>
		/// Get the output committer for this output format. This is responsible
		/// for ensuring the output is committed correctly.
		/// </remarks>
		/// <param name="context">the task context</param>
		/// <returns>an output committer</returns>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public abstract OutputCommitter GetOutputCommitter(TaskAttemptContext context);
	}
}
