using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Datanode
{
	/// <summary>Used for injecting faults in DFSClient and DFSOutputStream tests.</summary>
	/// <remarks>
	/// Used for injecting faults in DFSClient and DFSOutputStream tests.
	/// Calls into this are a no-op in production code.
	/// </remarks>
	public class DataNodeFaultInjector
	{
		public static DataNodeFaultInjector instance = new DataNodeFaultInjector();

		public static DataNodeFaultInjector Get()
		{
			return instance;
		}

		public static void Set(DataNodeFaultInjector injector)
		{
			instance = injector;
		}

		public virtual void GetHdfsBlocksMetadata()
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void WriteBlockAfterFlush()
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void SendShortCircuitShmResponse()
		{
		}

		public virtual bool DropHeartbeatPacket()
		{
			return false;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void NoRegistration()
		{
		}
	}
}
