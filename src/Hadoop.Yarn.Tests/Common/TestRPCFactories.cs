using System.Net;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Yarn.Api;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Factories.Impl.PB;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn
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
			ApplicationMasterProtocol instance = new TestRPCFactories.AMRMProtocolTestImpl(this
				);
			Server server = null;
			try
			{
				server = RpcServerFactoryPBImpl.Get().GetServer(typeof(ApplicationMasterProtocol)
					, instance, addr, conf, null, 1);
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
			ApplicationMasterProtocol instance = new TestRPCFactories.AMRMProtocolTestImpl(this
				);
			Server server = null;
			try
			{
				server = RpcServerFactoryPBImpl.Get().GetServer(typeof(ApplicationMasterProtocol)
					, instance, addr, conf, null, 1);
				server.Start();
				System.Console.Error.WriteLine(server.GetListenerAddress());
				System.Console.Error.WriteLine(NetUtils.GetConnectAddress(server));
				ApplicationMasterProtocol amrmClient = null;
				try
				{
					amrmClient = (ApplicationMasterProtocol)RpcClientFactoryPBImpl.Get().GetClient(typeof(
						ApplicationMasterProtocol), 1, NetUtils.GetConnectAddress(server), conf);
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
				if (server != null)
				{
					server.Stop();
				}
			}
		}

		public class AMRMProtocolTestImpl : ApplicationMasterProtocol
		{
			/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
			/// <exception cref="System.IO.IOException"/>
			public virtual RegisterApplicationMasterResponse RegisterApplicationMaster(RegisterApplicationMasterRequest
				 request)
			{
				// TODO Auto-generated method stub
				return null;
			}

			/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
			/// <exception cref="System.IO.IOException"/>
			public virtual FinishApplicationMasterResponse FinishApplicationMaster(FinishApplicationMasterRequest
				 request)
			{
				// TODO Auto-generated method stub
				return null;
			}

			/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
			/// <exception cref="System.IO.IOException"/>
			public virtual AllocateResponse Allocate(AllocateRequest request)
			{
				// TODO Auto-generated method stub
				return null;
			}

			internal AMRMProtocolTestImpl(TestRPCFactories _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly TestRPCFactories _enclosing;
		}
	}
}
