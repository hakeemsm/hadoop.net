using System.Net;
using Com.Google.Protobuf;
using NUnit.Framework;
using Org.Apache.Commons.Lang;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Ipc.Protobuf;
using Org.Apache.Hadoop.Metrics2;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Test;
using Sharpen;

namespace Org.Apache.Hadoop.Ipc
{
	/// <summary>Test for testing protocol buffer based RPC mechanism.</summary>
	/// <remarks>
	/// Test for testing protocol buffer based RPC mechanism.
	/// This test depends on test.proto definition of types in src/test/proto
	/// and protobuf service definition from src/test/test_rpc_service.proto
	/// </remarks>
	public class TestProtoBufRpc
	{
		public const string Address = "0.0.0.0";

		public const int Port = 0;

		private static IPEndPoint addr;

		private static Configuration conf;

		private static RPC.Server server;

		public interface TestRpcService : TestRpcServiceProtos.TestProtobufRpcProto.BlockingInterface
		{
		}

		public interface TestRpcService2 : TestRpcServiceProtos.TestProtobufRpc2Proto.BlockingInterface
		{
		}

		public class PBServerImpl : TestProtoBufRpc.TestRpcService
		{
			/// <exception cref="Com.Google.Protobuf.ServiceException"/>
			public virtual TestProtos.EmptyResponseProto Ping(RpcController unused, TestProtos.EmptyRequestProto
				 request)
			{
				// Ensure clientId is received
				byte[] clientId = Server.GetClientId();
				NUnit.Framework.Assert.IsNotNull(Server.GetClientId());
				NUnit.Framework.Assert.AreEqual(16, clientId.Length);
				return ((TestProtos.EmptyResponseProto)TestProtos.EmptyResponseProto.NewBuilder()
					.Build());
			}

			/// <exception cref="Com.Google.Protobuf.ServiceException"/>
			public virtual TestProtos.EchoResponseProto Echo(RpcController unused, TestProtos.EchoRequestProto
				 request)
			{
				return ((TestProtos.EchoResponseProto)TestProtos.EchoResponseProto.NewBuilder().SetMessage
					(request.GetMessage()).Build());
			}

			/// <exception cref="Com.Google.Protobuf.ServiceException"/>
			public virtual TestProtos.EmptyResponseProto Error(RpcController unused, TestProtos.EmptyRequestProto
				 request)
			{
				throw new ServiceException("error", new RpcServerException("error"));
			}

			/// <exception cref="Com.Google.Protobuf.ServiceException"/>
			public virtual TestProtos.EmptyResponseProto Error2(RpcController unused, TestProtos.EmptyRequestProto
				 request)
			{
				throw new ServiceException("error", new URISyntaxException(string.Empty, "testException"
					));
			}
		}

		public class PBServer2Impl : TestProtoBufRpc.TestRpcService2
		{
			/// <exception cref="Com.Google.Protobuf.ServiceException"/>
			public virtual TestProtos.EmptyResponseProto Ping2(RpcController unused, TestProtos.EmptyRequestProto
				 request)
			{
				return ((TestProtos.EmptyResponseProto)TestProtos.EmptyResponseProto.NewBuilder()
					.Build());
			}

			/// <exception cref="Com.Google.Protobuf.ServiceException"/>
			public virtual TestProtos.EchoResponseProto Echo2(RpcController unused, TestProtos.EchoRequestProto
				 request)
			{
				return ((TestProtos.EchoResponseProto)TestProtos.EchoResponseProto.NewBuilder().SetMessage
					(request.GetMessage()).Build());
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[SetUp]
		public virtual void SetUp()
		{
			// Setup server for both protocols
			conf = new Configuration();
			conf.SetInt(CommonConfigurationKeys.IpcMaximumDataLength, 1024);
			// Set RPC engine to protobuf RPC engine
			RPC.SetProtocolEngine(conf, typeof(TestProtoBufRpc.TestRpcService), typeof(ProtobufRpcEngine
				));
			// Create server side implementation
			TestProtoBufRpc.PBServerImpl serverImpl = new TestProtoBufRpc.PBServerImpl();
			BlockingService service = TestRpcServiceProtos.TestProtobufRpcProto.NewReflectiveBlockingService
				(serverImpl);
			// Get RPC server for server side implementation
			server = new RPC.Builder(conf).SetProtocol(typeof(TestProtoBufRpc.TestRpcService)
				).SetInstance(service).SetBindAddress(Address).SetPort(Port).Build();
			addr = NetUtils.GetConnectAddress(server);
			// now the second protocol
			TestProtoBufRpc.PBServer2Impl server2Impl = new TestProtoBufRpc.PBServer2Impl();
			BlockingService service2 = TestRpcServiceProtos.TestProtobufRpc2Proto.NewReflectiveBlockingService
				(server2Impl);
			server.AddProtocol(RPC.RpcKind.RpcProtocolBuffer, typeof(TestProtoBufRpc.TestRpcService2
				), service2);
			server.Start();
		}

		/// <exception cref="System.Exception"/>
		[TearDown]
		public virtual void TearDown()
		{
			server.Stop();
		}

		/// <exception cref="System.IO.IOException"/>
		private static TestProtoBufRpc.TestRpcService GetClient()
		{
			// Set RPC engine to protobuf RPC engine
			RPC.SetProtocolEngine(conf, typeof(TestProtoBufRpc.TestRpcService), typeof(ProtobufRpcEngine
				));
			return RPC.GetProxy<TestProtoBufRpc.TestRpcService>(0, addr, conf);
		}

		/// <exception cref="System.IO.IOException"/>
		private static TestProtoBufRpc.TestRpcService2 GetClient2()
		{
			// Set RPC engine to protobuf RPC engine
			RPC.SetProtocolEngine(conf, typeof(TestProtoBufRpc.TestRpcService2), typeof(ProtobufRpcEngine
				));
			return RPC.GetProxy<TestProtoBufRpc.TestRpcService2>(0, addr, conf);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestProtoBufRpc()
		{
			TestProtoBufRpc.TestRpcService client = GetClient();
			TestProtoBufRpc(client);
		}

		// separated test out so that other tests can call it.
		/// <exception cref="System.Exception"/>
		public static void TestProtoBufRpc(TestProtoBufRpc.TestRpcService client)
		{
			// Test ping method
			TestProtos.EmptyRequestProto emptyRequest = ((TestProtos.EmptyRequestProto)TestProtos.EmptyRequestProto
				.NewBuilder().Build());
			client.Ping(null, emptyRequest);
			// Test echo method
			TestProtos.EchoRequestProto echoRequest = ((TestProtos.EchoRequestProto)TestProtos.EchoRequestProto
				.NewBuilder().SetMessage("hello").Build());
			TestProtos.EchoResponseProto echoResponse = client.Echo(null, echoRequest);
			NUnit.Framework.Assert.AreEqual(echoResponse.GetMessage(), "hello");
			// Test error method - error should be thrown as RemoteException
			try
			{
				client.Error(null, emptyRequest);
				NUnit.Framework.Assert.Fail("Expected exception is not thrown");
			}
			catch (ServiceException e)
			{
				RemoteException re = (RemoteException)e.InnerException;
				RpcServerException rse = (RpcServerException)re.UnwrapRemoteException(typeof(RpcServerException
					));
				NUnit.Framework.Assert.IsNotNull(rse);
				NUnit.Framework.Assert.IsTrue(re.GetErrorCode().Equals(RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto
					.ErrorRpcServer));
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestProtoBufRpc2()
		{
			TestProtoBufRpc.TestRpcService2 client = GetClient2();
			// Test ping method
			TestProtos.EmptyRequestProto emptyRequest = ((TestProtos.EmptyRequestProto)TestProtos.EmptyRequestProto
				.NewBuilder().Build());
			client.Ping2(null, emptyRequest);
			// Test echo method
			TestProtos.EchoRequestProto echoRequest = ((TestProtos.EchoRequestProto)TestProtos.EchoRequestProto
				.NewBuilder().SetMessage("hello").Build());
			TestProtos.EchoResponseProto echoResponse = client.Echo2(null, echoRequest);
			NUnit.Framework.Assert.AreEqual(echoResponse.GetMessage(), "hello");
			// Ensure RPC metrics are updated
			MetricsRecordBuilder rpcMetrics = MetricsAsserts.GetMetrics(server.GetRpcMetrics(
				).Name());
			MetricsAsserts.AssertCounterGt("RpcQueueTimeNumOps", 0L, rpcMetrics);
			MetricsAsserts.AssertCounterGt("RpcProcessingTimeNumOps", 0L, rpcMetrics);
			MetricsRecordBuilder rpcDetailedMetrics = MetricsAsserts.GetMetrics(server.GetRpcDetailedMetrics
				().Name());
			MetricsAsserts.AssertCounterGt("Echo2NumOps", 0L, rpcDetailedMetrics);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestProtoBufRandomException()
		{
			TestProtoBufRpc.TestRpcService client = GetClient();
			TestProtos.EmptyRequestProto emptyRequest = ((TestProtos.EmptyRequestProto)TestProtos.EmptyRequestProto
				.NewBuilder().Build());
			try
			{
				client.Error2(null, emptyRequest);
			}
			catch (ServiceException se)
			{
				NUnit.Framework.Assert.IsTrue(se.InnerException is RemoteException);
				RemoteException re = (RemoteException)se.InnerException;
				NUnit.Framework.Assert.IsTrue(re.GetClassName().Equals(typeof(URISyntaxException)
					.FullName));
				NUnit.Framework.Assert.IsTrue(re.Message.Contains("testException"));
				NUnit.Framework.Assert.IsTrue(re.GetErrorCode().Equals(RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto
					.ErrorApplication));
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestExtraLongRpc()
		{
			TestProtoBufRpc.TestRpcService2 client = GetClient2();
			string shortString = StringUtils.Repeat("X", 4);
			TestProtos.EchoRequestProto echoRequest = ((TestProtos.EchoRequestProto)TestProtos.EchoRequestProto
				.NewBuilder().SetMessage(shortString).Build());
			// short message goes through
			TestProtos.EchoResponseProto echoResponse = client.Echo2(null, echoRequest);
			NUnit.Framework.Assert.AreEqual(shortString, echoResponse.GetMessage());
			string longString = StringUtils.Repeat("X", 4096);
			echoRequest = ((TestProtos.EchoRequestProto)TestProtos.EchoRequestProto.NewBuilder
				().SetMessage(longString).Build());
			try
			{
				echoResponse = client.Echo2(null, echoRequest);
				NUnit.Framework.Assert.Fail("expected extra-long RPC to fail");
			}
			catch (ServiceException)
			{
			}
		}
		// expected
	}
}
