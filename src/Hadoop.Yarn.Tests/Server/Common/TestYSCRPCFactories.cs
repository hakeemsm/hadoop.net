using System.Net;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Factories.Impl.PB;
using Org.Apache.Hadoop.Yarn.Server.Api;
using Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn
{
	public class TestYSCRPCFactories
	{
		[NUnit.Framework.Test]
		public virtual void Test()
		{
			TestPbServerFactory();
			TestPbClientFactory();
		}

		private void TestPbServerFactory()
		{
			IPEndPoint addr = new IPEndPoint(0);
			Configuration conf = new Configuration();
			ResourceTracker instance = new TestYSCRPCFactories.ResourceTrackerTestImpl(this);
			Org.Apache.Hadoop.Ipc.Server server = null;
			try
			{
				server = RpcServerFactoryPBImpl.Get().GetServer(typeof(ResourceTracker), instance
					, addr, conf, null, 1);
				server.Start();
			}
			catch (YarnRuntimeException e)
			{
				Sharpen.Runtime.PrintStackTrace(e);
				NUnit.Framework.Assert.Fail("Failed to create server");
			}
			finally
			{
				server.Stop();
			}
		}

		private void TestPbClientFactory()
		{
			IPEndPoint addr = new IPEndPoint(0);
			System.Console.Error.WriteLine(addr.GetHostName() + addr.Port);
			Configuration conf = new Configuration();
			ResourceTracker instance = new TestYSCRPCFactories.ResourceTrackerTestImpl(this);
			Org.Apache.Hadoop.Ipc.Server server = null;
			try
			{
				server = RpcServerFactoryPBImpl.Get().GetServer(typeof(ResourceTracker), instance
					, addr, conf, null, 1);
				server.Start();
				System.Console.Error.WriteLine(server.GetListenerAddress());
				System.Console.Error.WriteLine(NetUtils.GetConnectAddress(server));
				ResourceTracker client = null;
				try
				{
					client = (ResourceTracker)RpcClientFactoryPBImpl.Get().GetClient(typeof(ResourceTracker
						), 1, NetUtils.GetConnectAddress(server), conf);
				}
				catch (YarnRuntimeException e)
				{
					Sharpen.Runtime.PrintStackTrace(e);
					NUnit.Framework.Assert.Fail("Failed to create client");
				}
			}
			catch (YarnRuntimeException e)
			{
				Sharpen.Runtime.PrintStackTrace(e);
				NUnit.Framework.Assert.Fail("Failed to create server");
			}
			finally
			{
				server.Stop();
			}
		}

		public class ResourceTrackerTestImpl : ResourceTracker
		{
			/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
			/// <exception cref="System.IO.IOException"/>
			public virtual RegisterNodeManagerResponse RegisterNodeManager(RegisterNodeManagerRequest
				 request)
			{
				// TODO Auto-generated method stub
				return null;
			}

			/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
			/// <exception cref="System.IO.IOException"/>
			public virtual NodeHeartbeatResponse NodeHeartbeat(NodeHeartbeatRequest request)
			{
				// TODO Auto-generated method stub
				return null;
			}

			internal ResourceTrackerTestImpl(TestYSCRPCFactories _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly TestYSCRPCFactories _enclosing;
		}
	}
}
