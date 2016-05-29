using NUnit.Framework;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Server.Api;
using Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Server.Api.Records;
using Org.Apache.Hadoop.Yarn.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Client
{
	public class TestResourceTrackerOnHA : ProtocolHATestBase
	{
		private ResourceTracker resourceTracker = null;

		/// <exception cref="System.Exception"/>
		[SetUp]
		public virtual void Initiate()
		{
			StartHACluster(0, false, true, false);
			this.resourceTracker = GetRMClient();
		}

		[TearDown]
		public virtual void ShutDown()
		{
			if (this.resourceTracker != null)
			{
				RPC.StopProxy(this.resourceTracker);
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestResourceTrackerOnHA()
		{
			NodeId nodeId = NodeId.NewInstance("localhost", 0);
			Resource resource = Resource.NewInstance(2048, 4);
			// make sure registerNodeManager works when failover happens
			RegisterNodeManagerRequest request = RegisterNodeManagerRequest.NewInstance(nodeId
				, 0, resource, YarnVersionInfo.GetVersion(), null, null);
			resourceTracker.RegisterNodeManager(request);
			NUnit.Framework.Assert.IsTrue(WaitForNodeManagerToConnect(10000, nodeId));
			// restart the failover thread, and make sure nodeHeartbeat works
			failoverThread = CreateAndStartFailoverThread();
			NodeStatus status = NodeStatus.NewInstance(NodeId.NewInstance("localhost", 0), 0, 
				null, null, null);
			NodeHeartbeatRequest request2 = NodeHeartbeatRequest.NewInstance(status, null, null
				);
			resourceTracker.NodeHeartbeat(request2);
		}

		/// <exception cref="System.IO.IOException"/>
		private ResourceTracker GetRMClient()
		{
			return ServerRMProxy.CreateRMProxy<ResourceTracker>(this.conf);
		}

		/// <exception cref="System.Exception"/>
		private bool WaitForNodeManagerToConnect(int timeout, NodeId nodeId)
		{
			for (int i = 0; i < timeout / 100; i++)
			{
				if (GetActiveRM().GetRMContext().GetRMNodes().Contains(nodeId))
				{
					return true;
				}
				Sharpen.Thread.Sleep(100);
			}
			return false;
		}
	}
}
