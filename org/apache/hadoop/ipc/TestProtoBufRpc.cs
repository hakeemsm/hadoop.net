using Sharpen;

namespace org.apache.hadoop.ipc
{
	/// <summary>Test for testing protocol buffer based RPC mechanism.</summary>
	/// <remarks>
	/// Test for testing protocol buffer based RPC mechanism.
	/// This test depends on test.proto definition of types in src/test/proto
	/// and protobuf service definition from src/test/test_rpc_service.proto
	/// </remarks>
	public class TestProtoBufRpc
	{
		public const string ADDRESS = "0.0.0.0";

		public const int PORT = 0;

		private static java.net.InetSocketAddress addr;

		private static org.apache.hadoop.conf.Configuration conf;

		private static org.apache.hadoop.ipc.RPC.Server server;

		public interface TestRpcService : org.apache.hadoop.ipc.protobuf.TestRpcServiceProtos.TestProtobufRpcProto.BlockingInterface
		{
		}

		public interface TestRpcService2 : org.apache.hadoop.ipc.protobuf.TestRpcServiceProtos.TestProtobufRpc2Proto.BlockingInterface
		{
		}

		public class PBServerImpl : org.apache.hadoop.ipc.TestProtoBufRpc.TestRpcService
		{
			/// <exception cref="com.google.protobuf.ServiceException"/>
			public virtual org.apache.hadoop.ipc.protobuf.TestProtos.EmptyResponseProto ping(
				com.google.protobuf.RpcController unused, org.apache.hadoop.ipc.protobuf.TestProtos.EmptyRequestProto
				 request)
			{
				// Ensure clientId is received
				byte[] clientId = org.apache.hadoop.ipc.Server.getClientId();
				NUnit.Framework.Assert.IsNotNull(org.apache.hadoop.ipc.Server.getClientId());
				NUnit.Framework.Assert.AreEqual(16, clientId.Length);
				return ((org.apache.hadoop.ipc.protobuf.TestProtos.EmptyResponseProto)org.apache.hadoop.ipc.protobuf.TestProtos.EmptyResponseProto
					.newBuilder().build());
			}

			/// <exception cref="com.google.protobuf.ServiceException"/>
			public virtual org.apache.hadoop.ipc.protobuf.TestProtos.EchoResponseProto echo(com.google.protobuf.RpcController
				 unused, org.apache.hadoop.ipc.protobuf.TestProtos.EchoRequestProto request)
			{
				return ((org.apache.hadoop.ipc.protobuf.TestProtos.EchoResponseProto)org.apache.hadoop.ipc.protobuf.TestProtos.EchoResponseProto
					.newBuilder().setMessage(request.getMessage()).build());
			}

			/// <exception cref="com.google.protobuf.ServiceException"/>
			public virtual org.apache.hadoop.ipc.protobuf.TestProtos.EmptyResponseProto error
				(com.google.protobuf.RpcController unused, org.apache.hadoop.ipc.protobuf.TestProtos.EmptyRequestProto
				 request)
			{
				throw new com.google.protobuf.ServiceException("error", new org.apache.hadoop.ipc.RpcServerException
					("error"));
			}

			/// <exception cref="com.google.protobuf.ServiceException"/>
			public virtual org.apache.hadoop.ipc.protobuf.TestProtos.EmptyResponseProto error2
				(com.google.protobuf.RpcController unused, org.apache.hadoop.ipc.protobuf.TestProtos.EmptyRequestProto
				 request)
			{
				throw new com.google.protobuf.ServiceException("error", new java.net.URISyntaxException
					(string.Empty, "testException"));
			}
		}

		public class PBServer2Impl : org.apache.hadoop.ipc.TestProtoBufRpc.TestRpcService2
		{
			/// <exception cref="com.google.protobuf.ServiceException"/>
			public virtual org.apache.hadoop.ipc.protobuf.TestProtos.EmptyResponseProto ping2
				(com.google.protobuf.RpcController unused, org.apache.hadoop.ipc.protobuf.TestProtos.EmptyRequestProto
				 request)
			{
				return ((org.apache.hadoop.ipc.protobuf.TestProtos.EmptyResponseProto)org.apache.hadoop.ipc.protobuf.TestProtos.EmptyResponseProto
					.newBuilder().build());
			}

			/// <exception cref="com.google.protobuf.ServiceException"/>
			public virtual org.apache.hadoop.ipc.protobuf.TestProtos.EchoResponseProto echo2(
				com.google.protobuf.RpcController unused, org.apache.hadoop.ipc.protobuf.TestProtos.EchoRequestProto
				 request)
			{
				return ((org.apache.hadoop.ipc.protobuf.TestProtos.EchoResponseProto)org.apache.hadoop.ipc.protobuf.TestProtos.EchoResponseProto
					.newBuilder().setMessage(request.getMessage()).build());
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.SetUp]
		public virtual void setUp()
		{
			// Setup server for both protocols
			conf = new org.apache.hadoop.conf.Configuration();
			conf.setInt(org.apache.hadoop.fs.CommonConfigurationKeys.IPC_MAXIMUM_DATA_LENGTH, 
				1024);
			// Set RPC engine to protobuf RPC engine
			org.apache.hadoop.ipc.RPC.setProtocolEngine(conf, Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.ipc.TestProtoBufRpc.TestRpcService)), Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.ipc.ProtobufRpcEngine)));
			// Create server side implementation
			org.apache.hadoop.ipc.TestProtoBufRpc.PBServerImpl serverImpl = new org.apache.hadoop.ipc.TestProtoBufRpc.PBServerImpl
				();
			com.google.protobuf.BlockingService service = org.apache.hadoop.ipc.protobuf.TestRpcServiceProtos.TestProtobufRpcProto
				.newReflectiveBlockingService(serverImpl);
			// Get RPC server for server side implementation
			server = new org.apache.hadoop.ipc.RPC.Builder(conf).setProtocol(Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.ipc.TestProtoBufRpc.TestRpcService))).setInstance(service
				).setBindAddress(ADDRESS).setPort(PORT).build();
			addr = org.apache.hadoop.net.NetUtils.getConnectAddress(server);
			// now the second protocol
			org.apache.hadoop.ipc.TestProtoBufRpc.PBServer2Impl server2Impl = new org.apache.hadoop.ipc.TestProtoBufRpc.PBServer2Impl
				();
			com.google.protobuf.BlockingService service2 = org.apache.hadoop.ipc.protobuf.TestRpcServiceProtos.TestProtobufRpc2Proto
				.newReflectiveBlockingService(server2Impl);
			server.addProtocol(org.apache.hadoop.ipc.RPC.RpcKind.RPC_PROTOCOL_BUFFER, Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.ipc.TestProtoBufRpc.TestRpcService2)), service2);
			server.start();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.TearDown]
		public virtual void tearDown()
		{
			server.stop();
		}

		/// <exception cref="System.IO.IOException"/>
		private static org.apache.hadoop.ipc.TestProtoBufRpc.TestRpcService getClient()
		{
			// Set RPC engine to protobuf RPC engine
			org.apache.hadoop.ipc.RPC.setProtocolEngine(conf, Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.ipc.TestProtoBufRpc.TestRpcService)), Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.ipc.ProtobufRpcEngine)));
			return org.apache.hadoop.ipc.RPC.getProxy<org.apache.hadoop.ipc.TestProtoBufRpc.TestRpcService
				>(0, addr, conf);
		}

		/// <exception cref="System.IO.IOException"/>
		private static org.apache.hadoop.ipc.TestProtoBufRpc.TestRpcService2 getClient2()
		{
			// Set RPC engine to protobuf RPC engine
			org.apache.hadoop.ipc.RPC.setProtocolEngine(conf, Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.ipc.TestProtoBufRpc.TestRpcService2)), Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.ipc.ProtobufRpcEngine)));
			return org.apache.hadoop.ipc.RPC.getProxy<org.apache.hadoop.ipc.TestProtoBufRpc.TestRpcService2
				>(0, addr, conf);
		}

		/// <exception cref="System.Exception"/>
		public virtual void testProtoBufRpc()
		{
			org.apache.hadoop.ipc.TestProtoBufRpc.TestRpcService client = getClient();
			testProtoBufRpc(client);
		}

		// separated test out so that other tests can call it.
		/// <exception cref="System.Exception"/>
		public static void testProtoBufRpc(org.apache.hadoop.ipc.TestProtoBufRpc.TestRpcService
			 client)
		{
			// Test ping method
			org.apache.hadoop.ipc.protobuf.TestProtos.EmptyRequestProto emptyRequest = ((org.apache.hadoop.ipc.protobuf.TestProtos.EmptyRequestProto
				)org.apache.hadoop.ipc.protobuf.TestProtos.EmptyRequestProto.newBuilder().build(
				));
			client.ping(null, emptyRequest);
			// Test echo method
			org.apache.hadoop.ipc.protobuf.TestProtos.EchoRequestProto echoRequest = ((org.apache.hadoop.ipc.protobuf.TestProtos.EchoRequestProto
				)org.apache.hadoop.ipc.protobuf.TestProtos.EchoRequestProto.newBuilder().setMessage
				("hello").build());
			org.apache.hadoop.ipc.protobuf.TestProtos.EchoResponseProto echoResponse = client
				.echo(null, echoRequest);
			NUnit.Framework.Assert.AreEqual(echoResponse.getMessage(), "hello");
			// Test error method - error should be thrown as RemoteException
			try
			{
				client.error(null, emptyRequest);
				NUnit.Framework.Assert.Fail("Expected exception is not thrown");
			}
			catch (com.google.protobuf.ServiceException e)
			{
				org.apache.hadoop.ipc.RemoteException re = (org.apache.hadoop.ipc.RemoteException
					)e.InnerException;
				org.apache.hadoop.ipc.RpcServerException rse = (org.apache.hadoop.ipc.RpcServerException
					)re.unwrapRemoteException(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.ipc.RpcServerException
					)));
				NUnit.Framework.Assert.IsNotNull(rse);
				NUnit.Framework.Assert.IsTrue(re.getErrorCode().Equals(org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto
					.ERROR_RPC_SERVER));
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void testProtoBufRpc2()
		{
			org.apache.hadoop.ipc.TestProtoBufRpc.TestRpcService2 client = getClient2();
			// Test ping method
			org.apache.hadoop.ipc.protobuf.TestProtos.EmptyRequestProto emptyRequest = ((org.apache.hadoop.ipc.protobuf.TestProtos.EmptyRequestProto
				)org.apache.hadoop.ipc.protobuf.TestProtos.EmptyRequestProto.newBuilder().build(
				));
			client.ping2(null, emptyRequest);
			// Test echo method
			org.apache.hadoop.ipc.protobuf.TestProtos.EchoRequestProto echoRequest = ((org.apache.hadoop.ipc.protobuf.TestProtos.EchoRequestProto
				)org.apache.hadoop.ipc.protobuf.TestProtos.EchoRequestProto.newBuilder().setMessage
				("hello").build());
			org.apache.hadoop.ipc.protobuf.TestProtos.EchoResponseProto echoResponse = client
				.echo2(null, echoRequest);
			NUnit.Framework.Assert.AreEqual(echoResponse.getMessage(), "hello");
			// Ensure RPC metrics are updated
			org.apache.hadoop.metrics2.MetricsRecordBuilder rpcMetrics = org.apache.hadoop.test.MetricsAsserts.getMetrics
				(server.getRpcMetrics().name());
			org.apache.hadoop.test.MetricsAsserts.assertCounterGt("RpcQueueTimeNumOps", 0L, rpcMetrics
				);
			org.apache.hadoop.test.MetricsAsserts.assertCounterGt("RpcProcessingTimeNumOps", 
				0L, rpcMetrics);
			org.apache.hadoop.metrics2.MetricsRecordBuilder rpcDetailedMetrics = org.apache.hadoop.test.MetricsAsserts.getMetrics
				(server.getRpcDetailedMetrics().name());
			org.apache.hadoop.test.MetricsAsserts.assertCounterGt("Echo2NumOps", 0L, rpcDetailedMetrics
				);
		}

		/// <exception cref="System.Exception"/>
		public virtual void testProtoBufRandomException()
		{
			org.apache.hadoop.ipc.TestProtoBufRpc.TestRpcService client = getClient();
			org.apache.hadoop.ipc.protobuf.TestProtos.EmptyRequestProto emptyRequest = ((org.apache.hadoop.ipc.protobuf.TestProtos.EmptyRequestProto
				)org.apache.hadoop.ipc.protobuf.TestProtos.EmptyRequestProto.newBuilder().build(
				));
			try
			{
				client.error2(null, emptyRequest);
			}
			catch (com.google.protobuf.ServiceException se)
			{
				NUnit.Framework.Assert.IsTrue(se.InnerException is org.apache.hadoop.ipc.RemoteException
					);
				org.apache.hadoop.ipc.RemoteException re = (org.apache.hadoop.ipc.RemoteException
					)se.InnerException;
				NUnit.Framework.Assert.IsTrue(re.getClassName().Equals(Sharpen.Runtime.getClassForType
					(typeof(java.net.URISyntaxException)).getName()));
				NUnit.Framework.Assert.IsTrue(re.Message.contains("testException"));
				NUnit.Framework.Assert.IsTrue(re.getErrorCode().Equals(org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto
					.ERROR_APPLICATION));
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void testExtraLongRpc()
		{
			org.apache.hadoop.ipc.TestProtoBufRpc.TestRpcService2 client = getClient2();
			string shortString = org.apache.commons.lang.StringUtils.repeat("X", 4);
			org.apache.hadoop.ipc.protobuf.TestProtos.EchoRequestProto echoRequest = ((org.apache.hadoop.ipc.protobuf.TestProtos.EchoRequestProto
				)org.apache.hadoop.ipc.protobuf.TestProtos.EchoRequestProto.newBuilder().setMessage
				(shortString).build());
			// short message goes through
			org.apache.hadoop.ipc.protobuf.TestProtos.EchoResponseProto echoResponse = client
				.echo2(null, echoRequest);
			NUnit.Framework.Assert.AreEqual(shortString, echoResponse.getMessage());
			string longString = org.apache.commons.lang.StringUtils.repeat("X", 4096);
			echoRequest = ((org.apache.hadoop.ipc.protobuf.TestProtos.EchoRequestProto)org.apache.hadoop.ipc.protobuf.TestProtos.EchoRequestProto
				.newBuilder().setMessage(longString).build());
			try
			{
				echoResponse = client.echo2(null, echoRequest);
				NUnit.Framework.Assert.Fail("expected extra-long RPC to fail");
			}
			catch (com.google.protobuf.ServiceException)
			{
			}
		}
		// expected
	}
}
