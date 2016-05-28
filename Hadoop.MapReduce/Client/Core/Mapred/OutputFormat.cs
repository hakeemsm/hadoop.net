using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
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
	/// <seealso cref="JobConf"/>
	public interface OutputFormat<K, V>
	{
		/// <summary>
		/// Get the
		/// <see cref="RecordWriter{K, V}"/>
		/// for the given job.
		/// </summary>
		/// <param name="ignored"/>
		/// <param name="job">configuration for the job whose output is being written.</param>
		/// <param name="name">the unique name for this part of the output.</param>
		/// <param name="progress">mechanism for reporting progress while writing to file.</param>
		/// <returns>
		/// a
		/// <see cref="RecordWriter{K, V}"/>
		/// to write the output for the job.
		/// </returns>
		/// <exception cref="System.IO.IOException"/>
		RecordWriter<K, V> GetRecordWriter(FileSystem ignored, JobConf job, string name, 
			Progressable progress);

		/// <summary>Check for validity of the output-specification for the job.</summary>
		/// <remarks>
		/// Check for validity of the output-specification for the job.
		/// <p>This is to validate the output specification for the job when it is
		/// a job is submitted.  Typically checks that it does not already exist,
		/// throwing an exception when it already exists, so that output is not
		/// overwritten.</p>
		/// </remarks>
		/// <param name="ignored"/>
		/// <param name="job">job configuration.</param>
		/// <exception cref="System.IO.IOException">when output should not be attempted</exception>
		void CheckOutputSpecs(FileSystem ignored, JobConf job);
	}
}
