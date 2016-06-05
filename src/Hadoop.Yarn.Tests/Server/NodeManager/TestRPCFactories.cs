using System.Net;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Factories.Impl.PB;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Api;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Api.Protocolrecords;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager
{
	public class TestRPCFactories
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
			LocalizationProtocol instance = new TestRPCFactories.LocalizationProtocolTestImpl
				(this);
			Org.Apache.Hadoop.Ipc.Server server = null;
			try
			{
				server = RpcServerFactoryPBImpl.Get().GetServer(typeof(LocalizationProtocol), instance
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
				if (server != null)
				{
					server.Stop();
				}
			}
		}

		private void TestPbClientFactory()
		{
			IPEndPoint addr = new IPEndPoint(0);
			System.Console.Error.WriteLine(addr.GetHostName() + addr.Port);
			Configuration conf = new Configuration();
			LocalizationProtocol instance = new TestRPCFactories.LocalizationProtocolTestImpl
				(this);
			Org.Apache.Hadoop.Ipc.Server server = null;
			try
			{
				server = RpcServerFactoryPBImpl.Get().GetServer(typeof(LocalizationProtocol), instance
					, addr, conf, null, 1);
				server.Start();
				System.Console.Error.WriteLine(server.GetListenerAddress());
				System.Console.Error.WriteLine(NetUtils.GetConnectAddress(server));
				try
				{
					LocalizationProtocol client = (LocalizationProtocol)RpcClientFactoryPBImpl.Get().
						GetClient(typeof(LocalizationProtocol), 1, NetUtils.GetConnectAddress(server), conf
						);
					NUnit.Framework.Assert.IsNotNull(client);
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

		public class LocalizationProtocolTestImpl : LocalizationProtocol
		{
			public virtual LocalizerHeartbeatResponse Heartbeat(LocalizerStatus status)
			{
				return null;
			}

			internal LocalizationProtocolTestImpl(TestRPCFactories _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly TestRPCFactories _enclosing;
		}
	}
}
