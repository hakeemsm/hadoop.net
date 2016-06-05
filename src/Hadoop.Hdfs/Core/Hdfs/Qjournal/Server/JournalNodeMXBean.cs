using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Qjournal.Server
{
	/// <summary>This is the JMX management interface for JournalNode information</summary>
	public interface JournalNodeMXBean
	{
		/// <summary>Get status information (e.g., whether formatted) of JournalNode's journals.
		/// 	</summary>
		/// <returns>A string presenting status for each journal</returns>
		string GetJournalsStatus();
	}
}
