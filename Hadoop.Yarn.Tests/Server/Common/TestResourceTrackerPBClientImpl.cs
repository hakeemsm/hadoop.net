using System.Net;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Factories;
using Org.Apache.Hadoop.Yarn.Factories.Impl.PB;
using Org.Apache.Hadoop.Yarn.Factory.Providers;
using Org.Apache.Hadoop.Yarn.Server.Api;
using Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn
{
	/// <summary>Test ResourceTrackerPBClientImpl.</summary>
	/// <remarks>
	/// Test ResourceTrackerPBClientImpl. this class should have methods
	/// registerNodeManager and newRecordInstance.
	/// </remarks>
	public class TestResourceTrackerPBClientImpl
	{
		private static ResourceTracker client;

		private static Org.Apache.Hadoop.Ipc.Server server;

		private static readonly RecordFactory recordFactory = RecordFactoryProvider.GetRecordFactory
			(null);

		[BeforeClass]
		public static void Start()
		{
			IPEndPoint address = new IPEndPoint(0);
			Configuration configuration = new Configuration();
			ResourceTracker instance = new TestResourceTrackerPBClientImpl.ResourceTrackerTestImpl
				();
			server = RpcServerFactoryPBImpl.Get().GetServer(typeof(ResourceTracker), instance
				, address, configuration, null, 1);
			server.Start();
			client = (ResourceTracker)RpcClientFactoryPBImpl.Get().GetClient(typeof(ResourceTracker
				), 1, NetUtils.GetConnectAddress(server), configuration);
		}

		[AfterClass]
		public static void Stop()
		{
			if (server != null)
			{
				server.Stop();
			}
		}

		/// <summary>Test the method registerNodeManager.</summary>
		/// <remarks>
		/// Test the method registerNodeManager. Method should return a not null
		/// result.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestResourceTrackerPBClientImpl()
		{
			RegisterNodeManagerRequest request = recordFactory.NewRecordInstance<RegisterNodeManagerRequest
				>();
			NUnit.Framework.Assert.IsNotNull(client.RegisterNodeManager(request));
			TestResourceTrackerPBClientImpl.ResourceTrackerTestImpl.exception = true;
			try
			{
				client.RegisterNodeManager(request);
				NUnit.Framework.Assert.Fail("there  should be YarnException");
			}
			catch (YarnException e)
			{
				NUnit.Framework.Assert.IsTrue(e.Message.StartsWith("testMessage"));
			}
			finally
			{
				TestResourceTrackerPBClientImpl.ResourceTrackerTestImpl.exception = false;
			}
		}

		/// <summary>Test the method nodeHeartbeat.</summary>
		/// <remarks>Test the method nodeHeartbeat. Method should return a not null result.</remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNodeHeartbeat()
		{
			NodeHeartbeatRequest request = recordFactory.NewRecordInstance<NodeHeartbeatRequest
				>();
			NUnit.Framework.Assert.IsNotNull(client.NodeHeartbeat(request));
			TestResourceTrackerPBClientImpl.ResourceTrackerTestImpl.exception = true;
			try
			{
				client.NodeHeartbeat(request);
				NUnit.Framework.Assert.Fail("there  should be YarnException");
			}
			catch (YarnException e)
			{
				NUnit.Framework.Assert.IsTrue(e.Message.StartsWith("testMessage"));
			}
			finally
			{
				TestResourceTrackerPBClientImpl.ResourceTrackerTestImpl.exception = false;
			}
		}

		public class ResourceTrackerTestImpl : ResourceTracker
		{
			public static bool exception = false;

			/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
			/// <exception cref="System.IO.IOException"/>
			public virtual RegisterNodeManagerResponse RegisterNodeManager(RegisterNodeManagerRequest
				 request)
			{
				if (exception)
				{
					throw new YarnException("testMessage");
				}
				return recordFactory.NewRecordInstance<RegisterNodeManagerResponse>();
			}

			/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
			/// <exception cref="System.IO.IOException"/>
			public virtual NodeHeartbeatResponse NodeHeartbeat(NodeHeartbeatRequest request)
			{
				if (exception)
				{
					throw new YarnException("testMessage");
				}
				return recordFactory.NewRecordInstance<NodeHeartbeatResponse>();
			}
		}
	}
}
