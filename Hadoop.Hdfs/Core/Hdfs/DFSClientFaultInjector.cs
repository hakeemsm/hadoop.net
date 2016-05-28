using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	/// <summary>Used for injecting faults in DFSClient and DFSOutputStream tests.</summary>
	/// <remarks>
	/// Used for injecting faults in DFSClient and DFSOutputStream tests.
	/// Calls into this are a no-op in production code.
	/// </remarks>
	public class DFSClientFaultInjector
	{
		public static DFSClientFaultInjector instance = new DFSClientFaultInjector();

		public static AtomicLong exceptionNum = new AtomicLong(0);

		public static DFSClientFaultInjector Get()
		{
			return instance;
		}

		public virtual bool CorruptPacket()
		{
			return false;
		}

		public virtual bool UncorruptPacket()
		{
			return false;
		}

		public virtual bool FailPacket()
		{
			return false;
		}

		public virtual void StartFetchFromDatanode()
		{
		}

		public virtual void FetchFromDatanodeException()
		{
		}

		public virtual void ReadFromDatanodeDelay()
		{
		}
	}
}
