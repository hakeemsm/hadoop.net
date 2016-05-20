using Sharpen;

namespace org.apache.hadoop.ipc
{
	/// <summary>Unit tests for IPC.</summary>
	public class TestIPC
	{
		public static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.ipc.TestIPC)));

		private static org.apache.hadoop.conf.Configuration conf;

		private const int PING_INTERVAL = 1000;

		private const int MIN_SLEEP_TIME = 1000;

		/// <summary>
		/// Flag used to turn off the fault injection behavior
		/// of the various writables.
		/// </summary>
		internal static bool WRITABLE_FAULTS_ENABLED = true;

		internal static int WRITABLE_FAULTS_SLEEP = 0;

		[NUnit.Framework.SetUp]
		public virtual void setupConf()
		{
			conf = new org.apache.hadoop.conf.Configuration();
			org.apache.hadoop.ipc.Client.setPingInterval(conf, PING_INTERVAL);
		}

		private static readonly java.util.Random RANDOM = new java.util.Random();

		private const string ADDRESS = "0.0.0.0";

		/// <summary>Directory where we can count open file descriptors on Linux</summary>
		private static readonly java.io.File FD_DIR = new java.io.File("/proc/self/fd");

		private class TestServer : org.apache.hadoop.ipc.Server
		{
			private java.lang.Runnable callListener;

			private bool sleep;

			private java.lang.Class responseClass;

			/// <exception cref="System.IO.IOException"/>
			public TestServer(int handlerCount, bool sleep)
				: this(handlerCount, sleep, Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.LongWritable
					)), null)
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public TestServer(int handlerCount, bool sleep, java.lang.Class paramClass, java.lang.Class
				 responseClass)
				: base(ADDRESS, 0, paramClass, handlerCount, conf)
			{
				// Tests can set callListener to run a piece of code each time the server
				// receives a call.  This code executes on the server thread, so it has
				// visibility of that thread's thread-local storage.
				this.sleep = sleep;
				this.responseClass = responseClass;
			}

			/// <exception cref="System.IO.IOException"/>
			public override org.apache.hadoop.io.Writable call(org.apache.hadoop.ipc.RPC.RpcKind
				 rpcKind, string protocol, org.apache.hadoop.io.Writable param, long receiveTime
				)
			{
				if (sleep)
				{
					// sleep a bit
					try
					{
						java.lang.Thread.sleep(RANDOM.nextInt(PING_INTERVAL) + MIN_SLEEP_TIME);
					}
					catch (System.Exception)
					{
					}
				}
				if (callListener != null)
				{
					callListener.run();
				}
				if (responseClass != null)
				{
					try
					{
						return responseClass.newInstance();
					}
					catch (System.Exception e)
					{
						throw new System.Exception(e);
					}
				}
				else
				{
					return param;
				}
			}
			// echo param as result
		}

		private class SerialCaller : java.lang.Thread
		{
			private org.apache.hadoop.ipc.Client client;

			private java.net.InetSocketAddress server;

			private int count;

			private bool failed;

			public SerialCaller(org.apache.hadoop.ipc.Client client, java.net.InetSocketAddress
				 server, int count)
			{
				this.client = client;
				this.server = server;
				this.count = count;
			}

			public override void run()
			{
				for (int i = 0; i < count; i++)
				{
					try
					{
						org.apache.hadoop.io.LongWritable param = new org.apache.hadoop.io.LongWritable(RANDOM
							.nextLong());
						org.apache.hadoop.io.LongWritable value = (org.apache.hadoop.io.LongWritable)client
							.call(param, server, null, null, 0, conf);
						if (!param.Equals(value))
						{
							LOG.fatal("Call failed!");
							failed = true;
							break;
						}
					}
					catch (System.Exception e)
					{
						LOG.fatal("Caught: " + org.apache.hadoop.util.StringUtils.stringifyException(e));
						failed = true;
					}
				}
			}
		}

		/// <summary>A RpcInvocationHandler instance for test.</summary>
		/// <remarks>
		/// A RpcInvocationHandler instance for test. Its invoke function uses the same
		/// <see cref="Client"/>
		/// instance, and will fail the first totalRetry times (by
		/// throwing an IOException).
		/// </remarks>
		private class TestInvocationHandler : org.apache.hadoop.ipc.RpcInvocationHandler
		{
			private static int retry = 0;

			private readonly org.apache.hadoop.ipc.Client client;

			private readonly org.apache.hadoop.ipc.Server server;

			private readonly int total;

			internal TestInvocationHandler(org.apache.hadoop.ipc.Client client, org.apache.hadoop.ipc.Server
				 server, int total)
			{
				this.client = client;
				this.server = server;
				this.total = total;
			}

			/// <exception cref="System.Exception"/>
			public virtual object invoke(object proxy, java.lang.reflect.Method method, object
				[] args)
			{
				org.apache.hadoop.io.LongWritable param = new org.apache.hadoop.io.LongWritable(RANDOM
					.nextLong());
				org.apache.hadoop.io.LongWritable value = (org.apache.hadoop.io.LongWritable)client
					.call(param, org.apache.hadoop.net.NetUtils.getConnectAddress(server), null, null
					, 0, conf);
				if (retry++ < total)
				{
					throw new System.IO.IOException("Fake IOException");
				}
				else
				{
					return value;
				}
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void close()
			{
			}

			public virtual org.apache.hadoop.ipc.Client.ConnectionId getConnectionId()
			{
				return null;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual void testSerial()
		{
			internalTestSerial(3, false, 2, 5, 100);
			internalTestSerial(3, true, 2, 5, 10);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual void internalTestSerial(int handlerCount, bool handlerSleep, int clientCount
			, int callerCount, int callCount)
		{
			org.apache.hadoop.ipc.Server server = new org.apache.hadoop.ipc.TestIPC.TestServer
				(handlerCount, handlerSleep);
			java.net.InetSocketAddress addr = org.apache.hadoop.net.NetUtils.getConnectAddress
				(server);
			server.start();
			org.apache.hadoop.ipc.Client[] clients = new org.apache.hadoop.ipc.Client[clientCount
				];
			for (int i = 0; i < clientCount; i++)
			{
				clients[i] = new org.apache.hadoop.ipc.Client(Sharpen.Runtime.getClassForType(typeof(
					org.apache.hadoop.io.LongWritable)), conf);
			}
			org.apache.hadoop.ipc.TestIPC.SerialCaller[] callers = new org.apache.hadoop.ipc.TestIPC.SerialCaller
				[callerCount];
			for (int i_1 = 0; i_1 < callerCount; i_1++)
			{
				callers[i_1] = new org.apache.hadoop.ipc.TestIPC.SerialCaller(clients[i_1 % clientCount
					], addr, callCount);
				callers[i_1].start();
			}
			for (int i_2 = 0; i_2 < callerCount; i_2++)
			{
				callers[i_2].join();
				NUnit.Framework.Assert.IsFalse(callers[i_2].failed);
			}
			for (int i_3 = 0; i_3 < clientCount; i_3++)
			{
				clients[i_3].stop();
			}
			server.stop();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testStandAloneClient()
		{
			org.apache.hadoop.ipc.Client client = new org.apache.hadoop.ipc.Client(Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.io.LongWritable)), conf);
			java.net.InetSocketAddress address = new java.net.InetSocketAddress("127.0.0.1", 
				10);
			try
			{
				client.call(new org.apache.hadoop.io.LongWritable(RANDOM.nextLong()), address, null
					, null, 0, conf);
				NUnit.Framework.Assert.Fail("Expected an exception to have been thrown");
			}
			catch (System.IO.IOException e)
			{
				string message = e.Message;
				string addressText = address.getHostName() + ":" + address.getPort();
				NUnit.Framework.Assert.IsTrue("Did not find " + addressText + " in " + message, message
					.contains(addressText));
				System.Exception cause = e.InnerException;
				NUnit.Framework.Assert.IsNotNull("No nested exception in " + e, cause);
				string causeText = cause.Message;
				NUnit.Framework.Assert.IsTrue("Did not find " + causeText + " in " + message, message
					.contains(causeText));
			}
			finally
			{
				client.stop();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		internal static void maybeThrowIOE()
		{
			if (WRITABLE_FAULTS_ENABLED)
			{
				maybeSleep();
				throw new System.IO.IOException("Injected fault");
			}
		}

		internal static void maybeThrowRTE()
		{
			if (WRITABLE_FAULTS_ENABLED)
			{
				maybeSleep();
				throw new System.Exception("Injected fault");
			}
		}

		private static void maybeSleep()
		{
			if (WRITABLE_FAULTS_SLEEP > 0)
			{
				try
				{
					java.lang.Thread.sleep(WRITABLE_FAULTS_SLEEP);
				}
				catch (System.Exception)
				{
				}
			}
		}

		private class IOEOnReadWritable : org.apache.hadoop.io.LongWritable
		{
			public IOEOnReadWritable()
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public override void readFields(java.io.DataInput @in)
			{
				base.readFields(@in);
				maybeThrowIOE();
			}
		}

		private class RTEOnReadWritable : org.apache.hadoop.io.LongWritable
		{
			public RTEOnReadWritable()
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public override void readFields(java.io.DataInput @in)
			{
				base.readFields(@in);
				maybeThrowRTE();
			}
		}

		private class IOEOnWriteWritable : org.apache.hadoop.io.LongWritable
		{
			public IOEOnWriteWritable()
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public override void write(java.io.DataOutput @out)
			{
				base.write(@out);
				maybeThrowIOE();
			}
		}

		private class RTEOnWriteWritable : org.apache.hadoop.io.LongWritable
		{
			public RTEOnWriteWritable()
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public override void write(java.io.DataOutput @out)
			{
				base.write(@out);
				maybeThrowRTE();
			}
		}

		/// <summary>
		/// Generic test case for exceptions thrown at some point in the IPC
		/// process.
		/// </summary>
		/// <param name="clientParamClass">- client writes this writable for parameter</param>
		/// <param name="serverParamClass">- server reads this writable for parameter</param>
		/// <param name="serverResponseClass">- server writes this writable for response</param>
		/// <param name="clientResponseClass">- client reads this writable for response</param>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="java.lang.InstantiationException"/>
		/// <exception cref="java.lang.IllegalAccessException"/>
		private void doErrorTest(java.lang.Class clientParamClass, java.lang.Class serverParamClass
			, java.lang.Class serverResponseClass, java.lang.Class clientResponseClass)
		{
			// start server
			org.apache.hadoop.ipc.Server server = new org.apache.hadoop.ipc.TestIPC.TestServer
				(1, false, serverParamClass, serverResponseClass);
			java.net.InetSocketAddress addr = org.apache.hadoop.net.NetUtils.getConnectAddress
				(server);
			server.start();
			// start client
			WRITABLE_FAULTS_ENABLED = true;
			org.apache.hadoop.ipc.Client client = new org.apache.hadoop.ipc.Client(clientResponseClass
				, conf);
			try
			{
				org.apache.hadoop.io.LongWritable param = clientParamClass.newInstance();
				try
				{
					client.call(param, addr, null, null, 0, conf);
					NUnit.Framework.Assert.Fail("Expected an exception to have been thrown");
				}
				catch (System.Exception t)
				{
					assertExceptionContains(t, "Injected fault");
				}
				// Doing a second call with faults disabled should return fine --
				// ie the internal state of the client or server should not be broken
				// by the failed call
				WRITABLE_FAULTS_ENABLED = false;
				client.call(param, addr, null, null, 0, conf);
			}
			finally
			{
				client.stop();
				server.stop();
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void testIOEOnClientWriteParam()
		{
			doErrorTest(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.ipc.TestIPC.IOEOnWriteWritable
				)), Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.LongWritable)), 
				Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.LongWritable)), Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.io.LongWritable)));
		}

		/// <exception cref="System.Exception"/>
		public virtual void testRTEOnClientWriteParam()
		{
			doErrorTest(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.ipc.TestIPC.RTEOnWriteWritable
				)), Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.LongWritable)), 
				Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.LongWritable)), Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.io.LongWritable)));
		}

		/// <exception cref="System.Exception"/>
		public virtual void testIOEOnServerReadParam()
		{
			doErrorTest(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.LongWritable
				)), Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.ipc.TestIPC.IOEOnReadWritable
				)), Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.LongWritable)), 
				Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.LongWritable)));
		}

		/// <exception cref="System.Exception"/>
		public virtual void testRTEOnServerReadParam()
		{
			doErrorTest(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.LongWritable
				)), Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.ipc.TestIPC.RTEOnReadWritable
				)), Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.LongWritable)), 
				Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.LongWritable)));
		}

		/// <exception cref="System.Exception"/>
		public virtual void testIOEOnServerWriteResponse()
		{
			doErrorTest(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.LongWritable
				)), Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.LongWritable)), 
				Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.ipc.TestIPC.IOEOnWriteWritable
				)), Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.LongWritable)));
		}

		/// <exception cref="System.Exception"/>
		public virtual void testRTEOnServerWriteResponse()
		{
			doErrorTest(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.LongWritable
				)), Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.LongWritable)), 
				Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.ipc.TestIPC.RTEOnWriteWritable
				)), Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.LongWritable)));
		}

		/// <exception cref="System.Exception"/>
		public virtual void testIOEOnClientReadResponse()
		{
			doErrorTest(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.LongWritable
				)), Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.LongWritable)), 
				Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.LongWritable)), Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.ipc.TestIPC.IOEOnReadWritable)));
		}

		/// <exception cref="System.Exception"/>
		public virtual void testRTEOnClientReadResponse()
		{
			doErrorTest(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.LongWritable
				)), Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.LongWritable)), 
				Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.LongWritable)), Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.ipc.TestIPC.RTEOnReadWritable)));
		}

		/// <summary>
		/// Test case that fails a write, but only after taking enough time
		/// that a ping should have been sent.
		/// </summary>
		/// <remarks>
		/// Test case that fails a write, but only after taking enough time
		/// that a ping should have been sent. This is a reproducer for a
		/// deadlock seen in one iteration of HADOOP-6762.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		public virtual void testIOEOnWriteAfterPingClient()
		{
			// start server
			org.apache.hadoop.ipc.Client.setPingInterval(conf, 100);
			try
			{
				WRITABLE_FAULTS_SLEEP = 1000;
				doErrorTest(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.ipc.TestIPC.IOEOnWriteWritable
					)), Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.LongWritable)), 
					Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.LongWritable)), Sharpen.Runtime.getClassForType
					(typeof(org.apache.hadoop.io.LongWritable)));
			}
			finally
			{
				WRITABLE_FAULTS_SLEEP = 0;
			}
		}

		private static void assertExceptionContains(System.Exception t, string substring)
		{
			string msg = org.apache.hadoop.util.StringUtils.stringifyException(t);
			NUnit.Framework.Assert.IsTrue("Exception should contain substring '" + substring 
				+ "':\n" + msg, msg.contains(substring));
			LOG.info("Got expected exception", t);
		}

		/// <summary>
		/// Test that, if the socket factory throws an IOE, it properly propagates
		/// to the client.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void testSocketFactoryException()
		{
			javax.net.SocketFactory mockFactory = org.mockito.Mockito.mock<javax.net.SocketFactory
				>();
			org.mockito.Mockito.doThrow(new System.IO.IOException("Injected fault")).when(mockFactory
				).createSocket();
			org.apache.hadoop.ipc.Client client = new org.apache.hadoop.ipc.Client(Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.io.LongWritable)), conf, mockFactory);
			java.net.InetSocketAddress address = new java.net.InetSocketAddress("127.0.0.1", 
				10);
			try
			{
				client.call(new org.apache.hadoop.io.LongWritable(RANDOM.nextLong()), address, null
					, null, 0, conf);
				NUnit.Framework.Assert.Fail("Expected an exception to have been thrown");
			}
			catch (System.IO.IOException e)
			{
				NUnit.Framework.Assert.IsTrue(e.Message.contains("Injected fault"));
			}
			finally
			{
				client.stop();
			}
		}

		/// <summary>
		/// Test that, if a RuntimeException is thrown after creating a socket
		/// but before successfully connecting to the IPC server, that the
		/// failure is handled properly.
		/// </summary>
		/// <remarks>
		/// Test that, if a RuntimeException is thrown after creating a socket
		/// but before successfully connecting to the IPC server, that the
		/// failure is handled properly. This is a regression test for
		/// HADOOP-7428.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public virtual void testRTEDuringConnectionSetup()
		{
			// Set up a socket factory which returns sockets which
			// throw an RTE when setSoTimeout is called.
			javax.net.SocketFactory spyFactory = org.mockito.Mockito.spy(org.apache.hadoop.net.NetUtils
				.getDefaultSocketFactory(conf));
			org.mockito.Mockito.doAnswer(new _Answer_527()).when(spyFactory).createSocket();
			org.apache.hadoop.ipc.Server server = new org.apache.hadoop.ipc.TestIPC.TestServer
				(1, true);
			org.apache.hadoop.ipc.Client client = new org.apache.hadoop.ipc.Client(Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.io.LongWritable)), conf, spyFactory);
			server.start();
			try
			{
				// Call should fail due to injected exception.
				java.net.InetSocketAddress address = org.apache.hadoop.net.NetUtils.getConnectAddress
					(server);
				try
				{
					client.call(new org.apache.hadoop.io.LongWritable(RANDOM.nextLong()), address, null
						, null, 0, conf);
					NUnit.Framework.Assert.Fail("Expected an exception to have been thrown");
				}
				catch (System.Exception e)
				{
					LOG.info("caught expected exception", e);
					NUnit.Framework.Assert.IsTrue(org.apache.hadoop.util.StringUtils.stringifyException
						(e).contains("Injected fault"));
				}
				// Resetting to the normal socket behavior should succeed
				// (i.e. it should not have cached a half-constructed connection)
				org.mockito.Mockito.reset(spyFactory);
				client.call(new org.apache.hadoop.io.LongWritable(RANDOM.nextLong()), address, null
					, null, 0, conf);
			}
			finally
			{
				client.stop();
				server.stop();
			}
		}

		private sealed class _Answer_527 : org.mockito.stubbing.Answer<java.net.Socket>
		{
			public _Answer_527()
			{
			}

			/// <exception cref="System.Exception"/>
			public java.net.Socket answer(org.mockito.invocation.InvocationOnMock invocation)
			{
				java.net.Socket s = org.mockito.Mockito.spy((java.net.Socket)invocation.callRealMethod
					());
				org.mockito.Mockito.doThrow(new System.Exception("Injected fault")).when(s).setSoTimeout
					(org.mockito.Matchers.anyInt());
				return s;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testIpcTimeout()
		{
			// start server
			org.apache.hadoop.ipc.Server server = new org.apache.hadoop.ipc.TestIPC.TestServer
				(1, true);
			java.net.InetSocketAddress addr = org.apache.hadoop.net.NetUtils.getConnectAddress
				(server);
			server.start();
			// start client
			org.apache.hadoop.ipc.Client client = new org.apache.hadoop.ipc.Client(Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.io.LongWritable)), conf);
			// set timeout to be less than MIN_SLEEP_TIME
			try
			{
				client.call(new org.apache.hadoop.io.LongWritable(RANDOM.nextLong()), addr, null, 
					null, MIN_SLEEP_TIME / 2, conf);
				NUnit.Framework.Assert.Fail("Expected an exception to have been thrown");
			}
			catch (java.net.SocketTimeoutException e)
			{
				LOG.info("Get a SocketTimeoutException ", e);
			}
			// set timeout to be bigger than 3*ping interval
			client.call(new org.apache.hadoop.io.LongWritable(RANDOM.nextLong()), addr, null, 
				null, 3 * PING_INTERVAL + MIN_SLEEP_TIME, conf);
			client.stop();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testIpcConnectTimeout()
		{
			// start server
			org.apache.hadoop.ipc.Server server = new org.apache.hadoop.ipc.TestIPC.TestServer
				(1, true);
			java.net.InetSocketAddress addr = org.apache.hadoop.net.NetUtils.getConnectAddress
				(server);
			//Intentionally do not start server to get a connection timeout
			// start client
			org.apache.hadoop.ipc.Client.setConnectTimeout(conf, 100);
			org.apache.hadoop.ipc.Client client = new org.apache.hadoop.ipc.Client(Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.io.LongWritable)), conf);
			// set the rpc timeout to twice the MIN_SLEEP_TIME
			try
			{
				client.call(new org.apache.hadoop.io.LongWritable(RANDOM.nextLong()), addr, null, 
					null, MIN_SLEEP_TIME * 2, conf);
				NUnit.Framework.Assert.Fail("Expected an exception to have been thrown");
			}
			catch (java.net.SocketTimeoutException e)
			{
				LOG.info("Get a SocketTimeoutException ", e);
			}
			client.stop();
		}

		/// <summary>Check service class byte in IPC header is correct on wire.</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void testIpcWithServiceClass()
		{
			// start server
			org.apache.hadoop.ipc.Server server = new org.apache.hadoop.ipc.TestIPC.TestServer
				(5, false);
			java.net.InetSocketAddress addr = org.apache.hadoop.net.NetUtils.getConnectAddress
				(server);
			server.start();
			// start client
			org.apache.hadoop.ipc.Client.setConnectTimeout(conf, 10000);
			callAndVerify(server, addr, 0, true);
			// Service Class is low to -128 as byte on wire.
			// -128 shouldn't be casted on wire but -129 should.
			callAndVerify(server, addr, -128, true);
			callAndVerify(server, addr, -129, false);
			// Service Class is up to 127.
			// 127 shouldn't be casted on wire but 128 should.
			callAndVerify(server, addr, 127, true);
			callAndVerify(server, addr, 128, false);
			server.stop();
		}

		private class TestServerQueue : org.apache.hadoop.ipc.Server
		{
			internal readonly java.util.concurrent.CountDownLatch firstCallLatch = new java.util.concurrent.CountDownLatch
				(1);

			internal readonly java.util.concurrent.CountDownLatch callBlockLatch = new java.util.concurrent.CountDownLatch
				(1);

			/// <exception cref="System.IO.IOException"/>
			internal TestServerQueue(int expectedCalls, int readers, int callQ, int handlers, 
				org.apache.hadoop.conf.Configuration conf)
				: base(ADDRESS, 0, Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.LongWritable
					)), handlers, readers, callQ, conf, null, null)
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public override org.apache.hadoop.io.Writable call(org.apache.hadoop.ipc.RPC.RpcKind
				 rpcKind, string protocol, org.apache.hadoop.io.Writable param, long receiveTime
				)
			{
				firstCallLatch.countDown();
				try
				{
					callBlockLatch.await();
				}
				catch (System.Exception e)
				{
					throw new System.IO.IOException(e);
				}
				return param;
			}
		}

		/// <summary>Check that reader queueing works</summary>
		/// <exception cref="java.util.concurrent.BrokenBarrierException"></exception>
		/// <exception cref="System.Exception"></exception>
		public virtual void testIpcWithReaderQueuing()
		{
			// 1 reader, 1 connectionQ slot, 1 callq
			for (int i = 0; i < 10; i++)
			{
				checkBlocking(1, 1, 1);
			}
			// 4 readers, 5 connectionQ slots, 2 callq
			for (int i_1 = 0; i_1 < 10; i_1++)
			{
				checkBlocking(4, 5, 2);
			}
		}

		// goal is to jam a handler with a connection, fill the callq with
		// connections, in turn jamming the readers - then flood the server and
		// ensure that the listener blocks when the reader connection queues fill
		/// <exception cref="System.Exception"/>
		private void checkBlocking(int readers, int readerQ, int callQ)
		{
			int handlers = 1;
			// makes it easier
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			conf.setInt(org.apache.hadoop.fs.CommonConfigurationKeys.IPC_SERVER_RPC_READ_CONNECTION_QUEUE_SIZE_KEY
				, readerQ);
			// send in enough clients to block up the handlers, callq, and readers
			int initialClients = readers + callQ + handlers;
			// max connections we should ever end up accepting at once
			int maxAccept = initialClients + readers * readerQ + 1;
			// 1 = listener
			// stress it with 2X the max
			int clients = maxAccept * 2;
			java.util.concurrent.atomic.AtomicInteger failures = new java.util.concurrent.atomic.AtomicInteger
				(0);
			java.util.concurrent.CountDownLatch callFinishedLatch = new java.util.concurrent.CountDownLatch
				(clients);
			// start server
			org.apache.hadoop.ipc.TestIPC.TestServerQueue server = new org.apache.hadoop.ipc.TestIPC.TestServerQueue
				(clients, readers, callQ, handlers, conf);
			java.net.InetSocketAddress addr = org.apache.hadoop.net.NetUtils.getConnectAddress
				(server);
			server.start();
			org.apache.hadoop.ipc.Client.setConnectTimeout(conf, 10000);
			// instantiate the threads, will start in batches
			java.lang.Thread[] threads = new java.lang.Thread[clients];
			for (int i = 0; i < clients; i++)
			{
				threads[i] = new java.lang.Thread(new _Runnable_704(conf, addr, failures, callFinishedLatch
					));
			}
			// start enough clients to block up the handler, callq, and each reader;
			// let the calls sequentially slot in to avoid some readers blocking
			// and others not blocking in the race to fill the callq
			for (int i_1 = 0; i_1 < initialClients; i_1++)
			{
				threads[i_1].start();
				if (i_1 == 0)
				{
					// let first reader block in a call
					server.firstCallLatch.await();
				}
				else
				{
					if (i_1 <= callQ)
					{
						// let subsequent readers jam the callq, will happen immediately 
						while (server.getCallQueueLen() != i_1)
						{
							java.lang.Thread.sleep(1);
						}
					}
				}
			}
			// additional threads block the readers trying to add to the callq
			// wait till everything is slotted, should happen immediately
			java.lang.Thread.sleep(10);
			if (server.getNumOpenConnections() < initialClients)
			{
				LOG.info("(initial clients) need:" + initialClients + " connections have:" + server
					.getNumOpenConnections());
				java.lang.Thread.sleep(100);
			}
			LOG.info("ipc layer should be blocked");
			NUnit.Framework.Assert.AreEqual(callQ, server.getCallQueueLen());
			NUnit.Framework.Assert.AreEqual(initialClients, server.getNumOpenConnections());
			// now flood the server with the rest of the connections, the reader's
			// connection queues should fill and then the listener should block
			for (int i_2 = initialClients; i_2 < clients; i_2++)
			{
				threads[i_2].start();
			}
			java.lang.Thread.sleep(10);
			if (server.getNumOpenConnections() < maxAccept)
			{
				LOG.info("(max clients) need:" + maxAccept + " connections have:" + server.getNumOpenConnections
					());
				java.lang.Thread.sleep(100);
			}
			// check a few times to make sure we didn't go over
			for (int i_3 = 0; i_3 < 4; i_3++)
			{
				NUnit.Framework.Assert.AreEqual(maxAccept, server.getNumOpenConnections());
				java.lang.Thread.sleep(100);
			}
			// sanity check that no calls have finished
			NUnit.Framework.Assert.AreEqual(clients, callFinishedLatch.getCount());
			LOG.info("releasing the calls");
			server.callBlockLatch.countDown();
			callFinishedLatch.await();
			foreach (java.lang.Thread t in threads)
			{
				t.join();
			}
			NUnit.Framework.Assert.AreEqual(0, failures.get());
			server.stop();
		}

		private sealed class _Runnable_704 : java.lang.Runnable
		{
			public _Runnable_704(org.apache.hadoop.conf.Configuration conf, java.net.InetSocketAddress
				 addr, java.util.concurrent.atomic.AtomicInteger failures, java.util.concurrent.CountDownLatch
				 callFinishedLatch)
			{
				this.conf = conf;
				this.addr = addr;
				this.failures = failures;
				this.callFinishedLatch = callFinishedLatch;
			}

			public void run()
			{
				org.apache.hadoop.ipc.Client client = new org.apache.hadoop.ipc.Client(Sharpen.Runtime.getClassForType
					(typeof(org.apache.hadoop.io.LongWritable)), conf);
				try
				{
					client.call(new org.apache.hadoop.io.LongWritable(java.lang.Thread.currentThread(
						).getId()), addr, null, null, 60000, conf);
				}
				catch (System.Exception e)
				{
					org.apache.hadoop.ipc.TestIPC.LOG.error(e);
					failures.incrementAndGet();
					return;
				}
				finally
				{
					callFinishedLatch.countDown();
					client.stop();
				}
			}

			private readonly org.apache.hadoop.conf.Configuration conf;

			private readonly java.net.InetSocketAddress addr;

			private readonly java.util.concurrent.atomic.AtomicInteger failures;

			private readonly java.util.concurrent.CountDownLatch callFinishedLatch;
		}

		/// <exception cref="System.Exception"/>
		public virtual void testConnectionIdleTimeouts()
		{
			((org.apache.commons.logging.impl.Log4JLogger)org.apache.hadoop.ipc.Server.LOG).getLogger
				().setLevel(org.apache.log4j.Level.DEBUG);
			int maxIdle = 1000;
			int cleanupInterval = maxIdle * 3 / 4;
			// stagger cleanups
			int killMax = 3;
			int clients = 1 + killMax * 2;
			// 1 to block, 2 batches to kill
			conf.setInt(org.apache.hadoop.fs.CommonConfigurationKeysPublic.IPC_CLIENT_CONNECTION_MAXIDLETIME_KEY
				, maxIdle);
			conf.setInt(org.apache.hadoop.fs.CommonConfigurationKeysPublic.IPC_CLIENT_IDLETHRESHOLD_KEY
				, 0);
			conf.setInt(org.apache.hadoop.fs.CommonConfigurationKeysPublic.IPC_CLIENT_KILL_MAX_KEY
				, killMax);
			conf.setInt(org.apache.hadoop.fs.CommonConfigurationKeys.IPC_CLIENT_CONNECTION_IDLESCANINTERVAL_KEY
				, cleanupInterval);
			java.util.concurrent.CyclicBarrier firstCallBarrier = new java.util.concurrent.CyclicBarrier
				(2);
			java.util.concurrent.CyclicBarrier callBarrier = new java.util.concurrent.CyclicBarrier
				(clients);
			java.util.concurrent.CountDownLatch allCallLatch = new java.util.concurrent.CountDownLatch
				(clients);
			java.util.concurrent.atomic.AtomicBoolean error = new java.util.concurrent.atomic.AtomicBoolean
				();
			org.apache.hadoop.ipc.TestIPC.TestServer server = new org.apache.hadoop.ipc.TestIPC.TestServer
				(clients, false);
			java.lang.Thread[] threads = new java.lang.Thread[clients];
			try
			{
				server.callListener = new _Runnable_798(allCallLatch, firstCallBarrier, callBarrier
					, error);
				// block first call
				server.start();
				// start client
				java.util.concurrent.CountDownLatch callReturned = new java.util.concurrent.CountDownLatch
					(clients - 1);
				java.net.InetSocketAddress addr = org.apache.hadoop.net.NetUtils.getConnectAddress
					(server);
				org.apache.hadoop.conf.Configuration clientConf = new org.apache.hadoop.conf.Configuration
					();
				clientConf.setInt(org.apache.hadoop.fs.CommonConfigurationKeysPublic.IPC_CLIENT_CONNECTION_MAXIDLETIME_KEY
					, 10000);
				for (int i = 0; i < clients; i++)
				{
					threads[i] = new java.lang.Thread(new _Runnable_824(clientConf, addr, callReturned
						));
					threads[i].start();
				}
				// all calls blocked in handler so all connections made
				allCallLatch.await();
				NUnit.Framework.Assert.IsFalse(error.get());
				NUnit.Framework.Assert.AreEqual(clients, server.getNumOpenConnections());
				// wake up blocked calls and wait for client call to return, no
				// connections should have closed
				callBarrier.await();
				callReturned.await();
				NUnit.Framework.Assert.AreEqual(clients, server.getNumOpenConnections());
				// server won't close till maxIdle*2, so give scanning thread time to
				// be almost ready to close idle connection.  after which it should
				// close max connections on every cleanupInterval
				java.lang.Thread.sleep(maxIdle * 2 - cleanupInterval);
				for (int i_1 = clients; i_1 > 1; i_1 -= killMax)
				{
					java.lang.Thread.sleep(cleanupInterval);
					NUnit.Framework.Assert.IsFalse(error.get());
					NUnit.Framework.Assert.AreEqual(i_1, server.getNumOpenConnections());
				}
				// connection for the first blocked call should still be open
				java.lang.Thread.sleep(cleanupInterval);
				NUnit.Framework.Assert.IsFalse(error.get());
				NUnit.Framework.Assert.AreEqual(1, server.getNumOpenConnections());
				// wake up call and ensure connection times out
				firstCallBarrier.await();
				java.lang.Thread.sleep(maxIdle * 2);
				NUnit.Framework.Assert.IsFalse(error.get());
				NUnit.Framework.Assert.AreEqual(0, server.getNumOpenConnections());
			}
			finally
			{
				foreach (java.lang.Thread t in threads)
				{
					if (t != null)
					{
						t.interrupt();
						t.join();
					}
					server.stop();
				}
			}
		}

		private sealed class _Runnable_798 : java.lang.Runnable
		{
			public _Runnable_798(java.util.concurrent.CountDownLatch allCallLatch, java.util.concurrent.CyclicBarrier
				 firstCallBarrier, java.util.concurrent.CyclicBarrier callBarrier, java.util.concurrent.atomic.AtomicBoolean
				 error)
			{
				this.allCallLatch = allCallLatch;
				this.firstCallBarrier = firstCallBarrier;
				this.callBarrier = callBarrier;
				this.error = error;
				this.first = new java.util.concurrent.atomic.AtomicBoolean(true);
			}

			internal java.util.concurrent.atomic.AtomicBoolean first;

			public void run()
			{
				try
				{
					allCallLatch.countDown();
					if (this.first.compareAndSet(true, false))
					{
						firstCallBarrier.await();
					}
					else
					{
						callBarrier.await();
					}
				}
				catch (System.Exception t)
				{
					org.apache.hadoop.ipc.TestIPC.LOG.error(t);
					error.set(true);
				}
			}

			private readonly java.util.concurrent.CountDownLatch allCallLatch;

			private readonly java.util.concurrent.CyclicBarrier firstCallBarrier;

			private readonly java.util.concurrent.CyclicBarrier callBarrier;

			private readonly java.util.concurrent.atomic.AtomicBoolean error;
		}

		private sealed class _Runnable_824 : java.lang.Runnable
		{
			public _Runnable_824(org.apache.hadoop.conf.Configuration clientConf, java.net.InetSocketAddress
				 addr, java.util.concurrent.CountDownLatch callReturned)
			{
				this.clientConf = clientConf;
				this.addr = addr;
				this.callReturned = callReturned;
			}

			public void run()
			{
				org.apache.hadoop.ipc.Client client = new org.apache.hadoop.ipc.Client(Sharpen.Runtime.getClassForType
					(typeof(org.apache.hadoop.io.LongWritable)), clientConf);
				try
				{
					client.call(new org.apache.hadoop.io.LongWritable(java.lang.Thread.currentThread(
						).getId()), addr, null, null, 0, clientConf);
					callReturned.countDown();
					java.lang.Thread.sleep(10000);
				}
				catch (System.IO.IOException e)
				{
					org.apache.hadoop.ipc.TestIPC.LOG.error(e);
				}
				catch (System.Exception)
				{
				}
				finally
				{
					client.stop();
				}
			}

			private readonly org.apache.hadoop.conf.Configuration clientConf;

			private readonly java.net.InetSocketAddress addr;

			private readonly java.util.concurrent.CountDownLatch callReturned;
		}

		/// <summary>Make a call from a client and verify if header info is changed in server side
		/// 	</summary>
		/// <exception cref="System.IO.IOException"/>
		private void callAndVerify(org.apache.hadoop.ipc.Server server, java.net.InetSocketAddress
			 addr, int serviceClass, bool noChanged)
		{
			org.apache.hadoop.ipc.Client client = new org.apache.hadoop.ipc.Client(Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.io.LongWritable)), conf);
			client.call(new org.apache.hadoop.io.LongWritable(RANDOM.nextLong()), addr, null, 
				null, MIN_SLEEP_TIME, serviceClass, conf);
			org.apache.hadoop.ipc.Server.Connection connection = server.getConnections()[0];
			int serviceClass2 = connection.getServiceClass();
			NUnit.Framework.Assert.IsFalse(noChanged ^ serviceClass == serviceClass2);
			client.stop();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testIpcAfterStopping()
		{
			// start server
			org.apache.hadoop.ipc.Server server = new org.apache.hadoop.ipc.TestIPC.TestServer
				(5, false);
			java.net.InetSocketAddress addr = org.apache.hadoop.net.NetUtils.getConnectAddress
				(server);
			server.start();
			// start client
			org.apache.hadoop.ipc.Client client = new org.apache.hadoop.ipc.Client(Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.io.LongWritable)), conf);
			client.call(new org.apache.hadoop.io.LongWritable(RANDOM.nextLong()), addr, null, 
				null, MIN_SLEEP_TIME, 0, conf);
			client.stop();
			// This call should throw IOException.
			client.call(new org.apache.hadoop.io.LongWritable(RANDOM.nextLong()), addr, null, 
				null, MIN_SLEEP_TIME, 0, conf);
		}

		/// <summary>
		/// Check that file descriptors aren't leaked by starting
		/// and stopping IPC servers.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void testSocketLeak()
		{
			NUnit.Framework.Assume.assumeTrue(FD_DIR.exists());
			// only run on Linux
			long startFds = countOpenFileDescriptors();
			for (int i = 0; i < 50; i++)
			{
				org.apache.hadoop.ipc.Server server = new org.apache.hadoop.ipc.TestIPC.TestServer
					(1, true);
				server.start();
				server.stop();
			}
			long endFds = countOpenFileDescriptors();
			NUnit.Framework.Assert.IsTrue("Leaked " + (endFds - startFds) + " file descriptors"
				, endFds - startFds < 20);
		}

		/// <summary>
		/// Check if Client is interrupted after handling
		/// InterruptedException during cleanup
		/// </summary>
		public virtual void testInterrupted()
		{
			org.apache.hadoop.ipc.Client client = new org.apache.hadoop.ipc.Client(Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.io.LongWritable)), conf);
			org.apache.hadoop.ipc.Client.getClientExecutor().submit(new _Runnable_946());
			java.lang.Thread.currentThread().interrupt();
			client.stop();
			try
			{
				NUnit.Framework.Assert.IsTrue(java.lang.Thread.currentThread().isInterrupted());
				LOG.info("Expected thread interrupt during client cleanup");
			}
			catch (java.lang.AssertionError)
			{
				LOG.error("The Client did not interrupt after handling an Interrupted Exception");
				NUnit.Framework.Assert.Fail("The Client did not interrupt after handling an Interrupted Exception"
					);
			}
			// Clear Thread interrupt
			java.lang.Thread.interrupted();
		}

		private sealed class _Runnable_946 : java.lang.Runnable
		{
			public _Runnable_946()
			{
			}

			public void run()
			{
				while (true)
				{
				}
			}
		}

		private long countOpenFileDescriptors()
		{
			return FD_DIR.list().Length;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testIpcFromHadoop_0_18_13()
		{
			doIpcVersionTest(org.apache.hadoop.ipc.TestIPC.NetworkTraces.HADOOP_0_18_3_RPC_DUMP
				, org.apache.hadoop.ipc.TestIPC.NetworkTraces.RESPONSE_TO_HADOOP_0_18_3_RPC);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testIpcFromHadoop0_20_3()
		{
			doIpcVersionTest(org.apache.hadoop.ipc.TestIPC.NetworkTraces.HADOOP_0_20_3_RPC_DUMP
				, org.apache.hadoop.ipc.TestIPC.NetworkTraces.RESPONSE_TO_HADOOP_0_20_3_RPC);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testIpcFromHadoop0_21_0()
		{
			doIpcVersionTest(org.apache.hadoop.ipc.TestIPC.NetworkTraces.HADOOP_0_21_0_RPC_DUMP
				, org.apache.hadoop.ipc.TestIPC.NetworkTraces.RESPONSE_TO_HADOOP_0_21_0_RPC);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testHttpGetResponse()
		{
			doIpcVersionTest(Sharpen.Runtime.getBytesForString("GET / HTTP/1.0\r\n\r\n"), Sharpen.Runtime.getBytesForString
				(org.apache.hadoop.ipc.Server.RECEIVED_HTTP_REQ_RESPONSE));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testConnectionRetriesOnSocketTimeoutExceptions()
		{
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			// set max retries to 0
			conf.setInt(org.apache.hadoop.fs.CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SOCKET_TIMEOUTS_KEY
				, 0);
			assertRetriesOnSocketTimeouts(conf, 1);
			// set max retries to 3
			conf.setInt(org.apache.hadoop.fs.CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SOCKET_TIMEOUTS_KEY
				, 3);
			assertRetriesOnSocketTimeouts(conf, 4);
		}

		private class CallInfo
		{
			internal int id = org.apache.hadoop.ipc.RpcConstants.INVALID_CALL_ID;

			internal int retry = org.apache.hadoop.ipc.RpcConstants.INVALID_RETRY_COUNT;
		}

		/// <summary>
		/// Test if
		/// (1) the rpc server uses the call id/retry provided by the rpc client, and
		/// (2) the rpc client receives the same call id/retry from the rpc server.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void testCallIdAndRetry()
		{
			org.apache.hadoop.ipc.TestIPC.CallInfo info = new org.apache.hadoop.ipc.TestIPC.CallInfo
				();
			// Override client to store the call info and check response
			org.apache.hadoop.ipc.Client client = new _Client_1023(info, Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.io.LongWritable)), conf);
			// Attach a listener that tracks every call received by the server.
			org.apache.hadoop.ipc.TestIPC.TestServer server = new org.apache.hadoop.ipc.TestIPC.TestServer
				(1, false);
			server.callListener = new _Runnable_1042(info);
			try
			{
				java.net.InetSocketAddress addr = org.apache.hadoop.net.NetUtils.getConnectAddress
					(server);
				server.start();
				org.apache.hadoop.ipc.TestIPC.SerialCaller caller = new org.apache.hadoop.ipc.TestIPC.SerialCaller
					(client, addr, 10);
				caller.run();
				NUnit.Framework.Assert.IsFalse(caller.failed);
			}
			finally
			{
				client.stop();
				server.stop();
			}
		}

		private sealed class _Client_1023 : org.apache.hadoop.ipc.Client
		{
			public _Client_1023(org.apache.hadoop.ipc.TestIPC.CallInfo info, java.lang.Class 
				baseArg1, org.apache.hadoop.conf.Configuration baseArg2)
				: base(baseArg1, baseArg2)
			{
				this.info = info;
			}

			internal override org.apache.hadoop.ipc.Client.Call createCall(org.apache.hadoop.ipc.RPC.RpcKind
				 rpcKind, org.apache.hadoop.io.Writable rpcRequest)
			{
				org.apache.hadoop.ipc.Client.Call call = base.createCall(rpcKind, rpcRequest);
				info.id = call.id;
				info.retry = call.retry;
				return call;
			}

			/// <exception cref="System.IO.IOException"/>
			internal override void checkResponse(org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto
				 header)
			{
				base.checkResponse(header);
				NUnit.Framework.Assert.AreEqual(info.id, header.getCallId());
				NUnit.Framework.Assert.AreEqual(info.retry, header.getRetryCount());
			}

			private readonly org.apache.hadoop.ipc.TestIPC.CallInfo info;
		}

		private sealed class _Runnable_1042 : java.lang.Runnable
		{
			public _Runnable_1042(org.apache.hadoop.ipc.TestIPC.CallInfo info)
			{
				this.info = info;
			}

			public void run()
			{
				NUnit.Framework.Assert.AreEqual(info.id, org.apache.hadoop.ipc.Server.getCallId()
					);
				NUnit.Framework.Assert.AreEqual(info.retry, org.apache.hadoop.ipc.Server.getCallRetryCount
					());
			}

			private readonly org.apache.hadoop.ipc.TestIPC.CallInfo info;
		}

		/// <summary>A dummy protocol</summary>
		private interface DummyProtocol
		{
			void dummyRun();
		}

		/// <summary>Test the retry count while used in a retry proxy.</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void testRetryProxy()
		{
			org.apache.hadoop.ipc.Client client = new org.apache.hadoop.ipc.Client(Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.io.LongWritable)), conf);
			org.apache.hadoop.ipc.TestIPC.TestServer server = new org.apache.hadoop.ipc.TestIPC.TestServer
				(1, false);
			server.callListener = new _Runnable_1075();
			// try more times, so it is easier to find race condition bug
			// 10000 times runs about 6s on a core i7 machine
			int totalRetry = 10000;
			org.apache.hadoop.ipc.TestIPC.DummyProtocol proxy = (org.apache.hadoop.ipc.TestIPC.DummyProtocol
				)java.lang.reflect.Proxy.newProxyInstance(Sharpen.Runtime.getClassForType(typeof(
				org.apache.hadoop.ipc.TestIPC.DummyProtocol)).getClassLoader(), new java.lang.Class
				[] { Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.ipc.TestIPC.DummyProtocol
				)) }, new org.apache.hadoop.ipc.TestIPC.TestInvocationHandler(client, server, totalRetry
				));
			org.apache.hadoop.ipc.TestIPC.DummyProtocol retryProxy = (org.apache.hadoop.ipc.TestIPC.DummyProtocol
				)org.apache.hadoop.io.retry.RetryProxy.create<org.apache.hadoop.ipc.TestIPC.DummyProtocol
				>(proxy, org.apache.hadoop.io.retry.RetryPolicies.RETRY_FOREVER);
			try
			{
				server.start();
				retryProxy.dummyRun();
				NUnit.Framework.Assert.AreEqual(org.apache.hadoop.ipc.TestIPC.TestInvocationHandler
					.retry, totalRetry + 1);
			}
			finally
			{
				org.apache.hadoop.ipc.Client.setCallIdAndRetryCount(0, 0);
				client.stop();
				server.stop();
			}
		}

		private sealed class _Runnable_1075 : java.lang.Runnable
		{
			public _Runnable_1075()
			{
				this.retryCount = 0;
			}

			private int retryCount;

			public void run()
			{
				NUnit.Framework.Assert.AreEqual(this.retryCount++, org.apache.hadoop.ipc.Server.getCallRetryCount
					());
			}
		}

		/// <summary>Test if the rpc server gets the default retry count (0) from client.</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void testInitialCallRetryCount()
		{
			// Override client to store the call id
			org.apache.hadoop.ipc.Client client = new org.apache.hadoop.ipc.Client(Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.io.LongWritable)), conf);
			// Attach a listener that tracks every call ID received by the server.
			org.apache.hadoop.ipc.TestIPC.TestServer server = new org.apache.hadoop.ipc.TestIPC.TestServer
				(1, false);
			server.callListener = new _Runnable_1114();
			// we have not set the retry count for the client, thus on the server
			// side we should see retry count as 0
			try
			{
				java.net.InetSocketAddress addr = org.apache.hadoop.net.NetUtils.getConnectAddress
					(server);
				server.start();
				org.apache.hadoop.ipc.TestIPC.SerialCaller caller = new org.apache.hadoop.ipc.TestIPC.SerialCaller
					(client, addr, 10);
				caller.run();
				NUnit.Framework.Assert.IsFalse(caller.failed);
			}
			finally
			{
				client.stop();
				server.stop();
			}
		}

		private sealed class _Runnable_1114 : java.lang.Runnable
		{
			public _Runnable_1114()
			{
			}

			public void run()
			{
				NUnit.Framework.Assert.AreEqual(0, org.apache.hadoop.ipc.Server.getCallRetryCount
					());
			}
		}

		/// <summary>Test if the rpc server gets the retry count from client.</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void testCallRetryCount()
		{
			int retryCount = 255;
			// Override client to store the call id
			org.apache.hadoop.ipc.Client client = new org.apache.hadoop.ipc.Client(Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.io.LongWritable)), conf);
			org.apache.hadoop.ipc.Client.setCallIdAndRetryCount(org.apache.hadoop.ipc.Client.
				nextCallId(), 255);
			// Attach a listener that tracks every call ID received by the server.
			org.apache.hadoop.ipc.TestIPC.TestServer server = new org.apache.hadoop.ipc.TestIPC.TestServer
				(1, false);
			server.callListener = new _Runnable_1147(retryCount);
			// we have not set the retry count for the client, thus on the server
			// side we should see retry count as 0
			try
			{
				java.net.InetSocketAddress addr = org.apache.hadoop.net.NetUtils.getConnectAddress
					(server);
				server.start();
				org.apache.hadoop.ipc.TestIPC.SerialCaller caller = new org.apache.hadoop.ipc.TestIPC.SerialCaller
					(client, addr, 10);
				caller.run();
				NUnit.Framework.Assert.IsFalse(caller.failed);
			}
			finally
			{
				client.stop();
				server.stop();
			}
		}

		private sealed class _Runnable_1147 : java.lang.Runnable
		{
			public _Runnable_1147(int retryCount)
			{
				this.retryCount = retryCount;
			}

			public void run()
			{
				NUnit.Framework.Assert.AreEqual(retryCount, org.apache.hadoop.ipc.Server.getCallRetryCount
					());
			}

			private readonly int retryCount;
		}

		/// <summary>
		/// Tests that client generates a unique sequential call ID for each RPC call,
		/// even if multiple threads are using the same client.
		/// </summary>
		/// <exception cref="System.Exception"></exception>
		/// <exception cref="System.IO.IOException"/>
		public virtual void testUniqueSequentialCallIds()
		{
			int serverThreads = 10;
			int callerCount = 100;
			int perCallerCallCount = 100;
			org.apache.hadoop.ipc.TestIPC.TestServer server = new org.apache.hadoop.ipc.TestIPC.TestServer
				(serverThreads, false);
			// Attach a listener that tracks every call ID received by the server.  This
			// list must be synchronized, because multiple server threads will add to it.
			System.Collections.Generic.IList<int> callIds = java.util.Collections.synchronizedList
				(new System.Collections.Generic.List<int>());
			server.callListener = new _Runnable_1183(callIds);
			org.apache.hadoop.ipc.Client client = new org.apache.hadoop.ipc.Client(Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.io.LongWritable)), conf);
			try
			{
				java.net.InetSocketAddress addr = org.apache.hadoop.net.NetUtils.getConnectAddress
					(server);
				server.start();
				org.apache.hadoop.ipc.TestIPC.SerialCaller[] callers = new org.apache.hadoop.ipc.TestIPC.SerialCaller
					[callerCount];
				for (int i = 0; i < callerCount; ++i)
				{
					callers[i] = new org.apache.hadoop.ipc.TestIPC.SerialCaller(client, addr, perCallerCallCount
						);
					callers[i].start();
				}
				for (int i_1 = 0; i_1 < callerCount; ++i_1)
				{
					callers[i_1].join();
					NUnit.Framework.Assert.IsFalse(callers[i_1].failed);
				}
			}
			finally
			{
				client.stop();
				server.stop();
			}
			int expectedCallCount = callerCount * perCallerCallCount;
			NUnit.Framework.Assert.AreEqual(expectedCallCount, callIds.Count);
			// It is not guaranteed that the server executes requests in sequential order
			// of client call ID, so we must sort the call IDs before checking that it
			// contains every expected value.
			callIds.Sort();
			int startID = callIds[0];
			for (int i_2 = 0; i_2 < expectedCallCount; ++i_2)
			{
				NUnit.Framework.Assert.AreEqual(startID + i_2, callIds[i_2]);
			}
		}

		private sealed class _Runnable_1183 : java.lang.Runnable
		{
			public _Runnable_1183(System.Collections.Generic.IList<int> callIds)
			{
				this.callIds = callIds;
			}

			public void run()
			{
				callIds.add(org.apache.hadoop.ipc.Server.getCallId());
			}

			private readonly System.Collections.Generic.IList<int> callIds;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testMaxConnections()
		{
			conf.setInt("ipc.server.max.connections", 5);
			org.apache.hadoop.ipc.Server server = null;
			java.lang.Thread[] connectors = new java.lang.Thread[10];
			try
			{
				server = new org.apache.hadoop.ipc.TestIPC.TestServer(3, false);
				java.net.InetSocketAddress addr = org.apache.hadoop.net.NetUtils.getConnectAddress
					(server);
				server.start();
				NUnit.Framework.Assert.AreEqual(0, server.getNumOpenConnections());
				for (int i = 0; i < 10; i++)
				{
					connectors[i] = new _Thread_1235(addr);
					connectors[i].start();
				}
				java.lang.Thread.sleep(1000);
				// server should only accept up to 5 connections
				NUnit.Framework.Assert.AreEqual(5, server.getNumOpenConnections());
				for (int i_1 = 0; i_1 < 10; i_1++)
				{
					connectors[i_1].join();
				}
			}
			finally
			{
				if (server != null)
				{
					server.stop();
				}
				conf.setInt("ipc.server.max.connections", 0);
			}
		}

		private sealed class _Thread_1235 : java.lang.Thread
		{
			public _Thread_1235(java.net.InetSocketAddress addr)
			{
				this.addr = addr;
			}

			public override void run()
			{
				java.net.Socket sock = null;
				try
				{
					sock = org.apache.hadoop.net.NetUtils.getDefaultSocketFactory(org.apache.hadoop.ipc.TestIPC
						.conf).createSocket();
					org.apache.hadoop.net.NetUtils.connect(sock, addr, 3000);
					try
					{
						java.lang.Thread.sleep(4000);
					}
					catch (System.Exception)
					{
					}
				}
				catch (System.IO.IOException)
				{
				}
				finally
				{
					if (sock != null)
					{
						try
						{
							sock.close();
						}
						catch (System.IO.IOException)
						{
						}
					}
				}
			}

			private readonly java.net.InetSocketAddress addr;
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testClientGetTimeout()
		{
			org.apache.hadoop.conf.Configuration config = new org.apache.hadoop.conf.Configuration
				();
			NUnit.Framework.Assert.AreEqual(org.apache.hadoop.ipc.Client.getTimeout(config), 
				-1);
		}

		/// <exception cref="System.IO.IOException"/>
		private void assertRetriesOnSocketTimeouts(org.apache.hadoop.conf.Configuration conf
			, int maxTimeoutRetries)
		{
			javax.net.SocketFactory mockFactory = org.mockito.Mockito.mock<javax.net.SocketFactory
				>();
			org.mockito.Mockito.doThrow(new org.apache.hadoop.net.ConnectTimeoutException("fake"
				)).when(mockFactory).createSocket();
			org.apache.hadoop.ipc.Client client = new org.apache.hadoop.ipc.Client(Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.io.IntWritable)), conf, mockFactory);
			java.net.InetSocketAddress address = new java.net.InetSocketAddress("127.0.0.1", 
				9090);
			try
			{
				client.call(new org.apache.hadoop.io.IntWritable(RANDOM.nextInt()), address, null
					, null, 0, conf);
				NUnit.Framework.Assert.Fail("Not throwing the SocketTimeoutException");
			}
			catch (java.net.SocketTimeoutException)
			{
				org.mockito.Mockito.verify(mockFactory, org.mockito.Mockito.times(maxTimeoutRetries
					)).createSocket();
			}
			client.stop();
		}

		/// <exception cref="System.IO.IOException"/>
		private void doIpcVersionTest(byte[] requestData, byte[] expectedResponse)
		{
			org.apache.hadoop.ipc.Server server = new org.apache.hadoop.ipc.TestIPC.TestServer
				(1, true);
			java.net.InetSocketAddress addr = org.apache.hadoop.net.NetUtils.getConnectAddress
				(server);
			server.start();
			java.net.Socket socket = new java.net.Socket();
			try
			{
				org.apache.hadoop.net.NetUtils.connect(socket, addr, 5000);
				java.io.OutputStream @out = socket.getOutputStream();
				java.io.InputStream @in = socket.getInputStream();
				@out.write(requestData, 0, requestData.Length);
				@out.flush();
				java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
				org.apache.hadoop.io.IOUtils.copyBytes(@in, baos, 256);
				byte[] responseData = baos.toByteArray();
				NUnit.Framework.Assert.AreEqual(org.apache.hadoop.util.StringUtils.byteToHexString
					(expectedResponse), org.apache.hadoop.util.StringUtils.byteToHexString(responseData
					));
			}
			finally
			{
				org.apache.hadoop.io.IOUtils.closeSocket(socket);
				server.stop();
			}
		}

		/// <summary>
		/// Convert a string of lines that look like:
		/// "68 72 70 63 02 00 00 00  82 00 1d 6f 72 67 2e 61 hrpc....
		/// </summary>
		/// <remarks>
		/// Convert a string of lines that look like:
		/// "68 72 70 63 02 00 00 00  82 00 1d 6f 72 67 2e 61 hrpc.... ...org.a"
		/// .. into an array of bytes.
		/// </remarks>
		private static byte[] hexDumpToBytes(string hexdump)
		{
			int LAST_HEX_COL = 3 * 16;
			java.lang.StringBuilder hexString = new java.lang.StringBuilder();
			foreach (string line in org.apache.hadoop.util.StringUtils.toUpperCase(hexdump).split
				("\n"))
			{
				hexString.Append(Sharpen.Runtime.substring(line, 0, LAST_HEX_COL).Replace(" ", string.Empty
					));
			}
			return org.apache.hadoop.util.StringUtils.hexStringToByte(hexString.ToString());
		}

		/// <summary>Wireshark traces collected from various client versions.</summary>
		/// <remarks>
		/// Wireshark traces collected from various client versions. These enable
		/// us to test that old versions of the IPC stack will receive the correct
		/// responses so that they will throw a meaningful error message back
		/// to the user.
		/// </remarks>
		private abstract class NetworkTraces
		{
			/// <summary>Wireshark dump of an RPC request from Hadoop 0.18.3</summary>
			internal static readonly byte[] HADOOP_0_18_3_RPC_DUMP = hexDumpToBytes("68 72 70 63 02 00 00 00  82 00 1d 6f 72 67 2e 61 hrpc.... ...org.a\n"
				 + "70 61 63 68 65 2e 68 61  64 6f 6f 70 2e 69 6f 2e pache.ha doop.io.\n" + "57 72 69 74 61 62 6c 65  00 30 6f 72 67 2e 61 70 Writable .0org.ap\n"
				 + "61 63 68 65 2e 68 61 64  6f 6f 70 2e 69 6f 2e 4f ache.had oop.io.O\n" + "62 6a 65 63 74 57 72 69  74 61 62 6c 65 24 4e 75 bjectWri table$Nu\n"
				 + "6c 6c 49 6e 73 74 61 6e  63 65 00 2f 6f 72 67 2e llInstan ce./org.\n" + "61 70 61 63 68 65 2e 68  61 64 6f 6f 70 2e 73 65 apache.h adoop.se\n"
				 + "63 75 72 69 74 79 2e 55  73 65 72 47 72 6f 75 70 curity.U serGroup\n" + "49 6e 66 6f 72 6d 61 74  69 6f 6e 00 00 00 6c 00 Informat ion...l.\n"
				 + "00 00 00 00 12 67 65 74  50 72 6f 74 6f 63 6f 6c .....get Protocol\n" + "56 65 72 73 69 6f 6e 00  00 00 02 00 10 6a 61 76 Version. .....jav\n"
				 + "61 2e 6c 61 6e 67 2e 53  74 72 69 6e 67 00 2e 6f a.lang.S tring..o\n" + "72 67 2e 61 70 61 63 68  65 2e 68 61 64 6f 6f 70 rg.apach e.hadoop\n"
				 + "2e 6d 61 70 72 65 64 2e  4a 6f 62 53 75 62 6d 69 .mapred. JobSubmi\n" + "73 73 69 6f 6e 50 72 6f  74 6f 63 6f 6c 00 04 6c ssionPro tocol..l\n"
				 + "6f 6e 67 00 00 00 00 00  00 00 0a                ong..... ...     \n");

			internal const string HADOOP0_18_ERROR_MSG = "Server IPC version " + org.apache.hadoop.ipc.RpcConstants
				.CURRENT_VERSION + " cannot communicate with client version 2";

			/// <summary>
			/// Wireshark dump of the correct response that triggers an error message
			/// on an 0.18.3 client.
			/// </summary>
			internal static readonly byte[] RESPONSE_TO_HADOOP_0_18_3_RPC = com.google.common.primitives.Bytes
				.concat(hexDumpToBytes("00 00 00 00 01 00 00 00  29 6f 72 67 2e 61 70 61 ........ )org.apa\n"
				 + "63 68 65 2e 68 61 64 6f  6f 70 2e 69 70 63 2e 52 che.hado op.ipc.R\n" + "50 43 24 56 65 72 73 69  6f 6e 4d 69 73 6d 61 74 PC$Versi onMismat\n"
				 + "63 68                                            ch               \n"), com.google.common.primitives.Ints
				.toByteArray(HADOOP0_18_ERROR_MSG.Length), Sharpen.Runtime.getBytesForString(HADOOP0_18_ERROR_MSG
				));

			/// <summary>Wireshark dump of an RPC request from Hadoop 0.20.3</summary>
			internal static readonly byte[] HADOOP_0_20_3_RPC_DUMP = hexDumpToBytes("68 72 70 63 03 00 00 00  79 27 6f 72 67 2e 61 70 hrpc.... y'org.ap\n"
				 + "61 63 68 65 2e 68 61 64  6f 6f 70 2e 69 70 63 2e ache.had oop.ipc.\n" + "56 65 72 73 69 6f 6e 65  64 50 72 6f 74 6f 63 6f Versione dProtoco\n"
				 + "6c 01 0a 53 54 52 49 4e  47 5f 55 47 49 04 74 6f l..STRIN G_UGI.to\n" + "64 64 09 04 74 6f 64 64  03 61 64 6d 07 64 69 61 dd..todd .adm.dia\n"
				 + "6c 6f 75 74 05 63 64 72  6f 6d 07 70 6c 75 67 64 lout.cdr om.plugd\n" + "65 76 07 6c 70 61 64 6d  69 6e 05 61 64 6d 69 6e ev.lpadm in.admin\n"
				 + "0a 73 61 6d 62 61 73 68  61 72 65 06 6d 72 74 65 .sambash are.mrte\n" + "73 74 00 00 00 6c 00 00  00 00 00 12 67 65 74 50 st...l.. ....getP\n"
				 + "72 6f 74 6f 63 6f 6c 56  65 72 73 69 6f 6e 00 00 rotocolV ersion..\n" + "00 02 00 10 6a 61 76 61  2e 6c 61 6e 67 2e 53 74 ....java .lang.St\n"
				 + "72 69 6e 67 00 2e 6f 72  67 2e 61 70 61 63 68 65 ring..or g.apache\n" + "2e 68 61 64 6f 6f 70 2e  6d 61 70 72 65 64 2e 4a .hadoop. mapred.J\n"
				 + "6f 62 53 75 62 6d 69 73  73 69 6f 6e 50 72 6f 74 obSubmis sionProt\n" + "6f 63 6f 6c 00 04 6c 6f  6e 67 00 00 00 00 00 00 ocol..lo ng......\n"
				 + "00 14                                            ..               \n");

			internal const string HADOOP0_20_ERROR_MSG = "Server IPC version " + org.apache.hadoop.ipc.RpcConstants
				.CURRENT_VERSION + " cannot communicate with client version 3";

			internal static readonly byte[] RESPONSE_TO_HADOOP_0_20_3_RPC = com.google.common.primitives.Bytes
				.concat(hexDumpToBytes("ff ff ff ff ff ff ff ff  00 00 00 29 6f 72 67 2e ........ ...)org.\n"
				 + "61 70 61 63 68 65 2e 68  61 64 6f 6f 70 2e 69 70 apache.h adoop.ip\n" + "63 2e 52 50 43 24 56 65  72 73 69 6f 6e 4d 69 73 c.RPC$Ve rsionMis\n"
				 + "6d 61 74 63 68                                   match            \n"), com.google.common.primitives.Ints
				.toByteArray(HADOOP0_20_ERROR_MSG.Length), Sharpen.Runtime.getBytesForString(HADOOP0_20_ERROR_MSG
				));

			internal const string HADOOP0_21_ERROR_MSG = "Server IPC version " + org.apache.hadoop.ipc.RpcConstants
				.CURRENT_VERSION + " cannot communicate with client version 4";

			internal static readonly byte[] HADOOP_0_21_0_RPC_DUMP = hexDumpToBytes("68 72 70 63 04 50                                hrpc.P"
				 + "00 00 00 3c 33 6f 72 67  2e 61 70 61 63 68 65 2e ...<3org .apache.\n" + "68 61 64 6f 6f 70 2e 6d  61 70 72 65 64 75 63 65 hadoop.m apreduce\n"
				 + "2e 70 72 6f 74 6f 63 6f  6c 2e 43 6c 69 65 6e 74 .protoco l.Client\n" + "50 72 6f 74 6f 63 6f 6c  01 00 04 74 6f 64 64 00 Protocol ...todd.\n"
				 + "00 00 00 71 00 00 00 00  00 12 67 65 74 50 72 6f ...q.... ..getPro\n" + "74 6f 63 6f 6c 56 65 72  73 69 6f 6e 00 00 00 02 tocolVer sion....\n"
				 + "00 10 6a 61 76 61 2e 6c  61 6e 67 2e 53 74 72 69 ..java.l ang.Stri\n" + "6e 67 00 33 6f 72 67 2e  61 70 61 63 68 65 2e 68 ng.3org. apache.h\n"
				 + "61 64 6f 6f 70 2e 6d 61  70 72 65 64 75 63 65 2e adoop.ma preduce.\n" + "70 72 6f 74 6f 63 6f 6c  2e 43 6c 69 65 6e 74 50 protocol .ClientP\n"
				 + "72 6f 74 6f 63 6f 6c 00  04 6c 6f 6e 67 00 00 00 rotocol. .long...\n" + "00 00 00 00 21                                   ....!            \n"
				);

			internal static readonly byte[] RESPONSE_TO_HADOOP_0_21_0_RPC = com.google.common.primitives.Bytes
				.concat(hexDumpToBytes("ff ff ff ff ff ff ff ff  00 00 00 29 6f 72 67 2e ........ ...)org.\n"
				 + "61 70 61 63 68 65 2e 68  61 64 6f 6f 70 2e 69 70 apache.h adoop.ip\n" + "63 2e 52 50 43 24 56 65  72 73 69 6f 6e 4d 69 73 c.RPC$Ve rsionMis\n"
				 + "6d 61 74 63 68                                   match            \n"), com.google.common.primitives.Ints
				.toByteArray(HADOOP0_21_ERROR_MSG.Length), Sharpen.Runtime.getBytesForString(HADOOP0_21_ERROR_MSG
				));
			// in 0.21 it comes in two separate TCP packets
		}
	}
}
