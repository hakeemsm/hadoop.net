using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Common
{
	/// <summary>
	/// Interface which implementations of
	/// <see cref="Org.Apache.Hadoop.Hdfs.Server.Namenode.JournalManager"/>
	/// can use to report
	/// errors on underlying storage directories. This avoids a circular dependency
	/// between journal managers and the storage which instantiates them.
	/// </summary>
	public interface StorageErrorReporter
	{
		/// <summary>Indicate that some error occurred on the given file.</summary>
		/// <param name="f">the file which had an error.</param>
		void ReportErrorOnFile(FilePath f);
	}
}
