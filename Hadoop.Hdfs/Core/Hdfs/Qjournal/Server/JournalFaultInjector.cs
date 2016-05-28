using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Qjournal.Server
{
	/// <summary>Used for injecting faults in QuorumJournalManager tests.</summary>
	/// <remarks>
	/// Used for injecting faults in QuorumJournalManager tests.
	/// Calls into this are a no-op in production code.
	/// </remarks>
	public class JournalFaultInjector
	{
		public static JournalFaultInjector instance = new JournalFaultInjector();

		public static JournalFaultInjector Get()
		{
			return instance;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void BeforePersistPaxosData()
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void AfterPersistPaxosData()
		{
		}
	}
}
