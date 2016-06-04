using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Reflection;
using System.Text;
using Com.Google.Common.Primitives;
using Javax.Net;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Commons.Logging.Impl;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.IO.Retry;
using Org.Apache.Hadoop.Ipc.Protobuf;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Util;
using Org.Apache.Log4j;
using Org.Mockito;
using Org.Mockito.Invocation;
using Org.Mockito.Stubbing;
using Sharpen;
using Sharpen.Reflect;

namespace Org.Apache.Hadoop.Ipc
{
	/// <summary>Unit tests for IPC.</summary>
	public class TestIPC
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(TestIPC));

		private static Configuration conf;

		private const int PingInterval = 1000;

		private const int MinSleepTime = 1000;

		/// <summary>
		/// Flag used to turn off the fault injection behavior
		/// of the various writables.
		/// </summary>
		internal static bool WritableFaultsEnabled = true;

		internal static int WritableFaultsSleep = 0;

		[SetUp]
		public virtual void SetupConf()
		{
			conf = new Configuration();
			Client.SetPingInterval(conf, PingInterval);
		}

		private static readonly Random Random = new Random();

		private const string Address = "0.0.0.0";

		/// <summary>Directory where we can count open file descriptors on Linux</summary>
		private static readonly FilePath FdDir = new FilePath("/proc/self/fd");

		private class TestServer : Server
		{
			private Runnable callListener;

			private bool sleep;

			private Type responseClass;

			/// <exception cref="System.IO.IOException"/>
			public TestServer(int handlerCount, bool sleep)
				: this(handlerCount, sleep, typeof(LongWritable), null)
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public TestServer(int handlerCount, bool sleep, Type paramClass, Type responseClass
				)
				: base(Address, 0, paramClass, handlerCount, conf)
			{
				// Tests can set callListener to run a piece of code each time the server
				// receives a call.  This code executes on the server thread, so it has
				// visibility of that thread's thread-local storage.
				this.sleep = sleep;
				this.responseClass = responseClass;
			}

			/// <exception cref="System.IO.IOException"/>
			public override Writable Call(RPC.RpcKind rpcKind, string protocol, Writable param
				, long receiveTime)
			{
				if (sleep)
				{
					// sleep a bit
					try
					{
						Sharpen.Thread.Sleep(Random.Next(PingInterval) + MinSleepTime);
					}
					catch (Exception)
					{
					}
				}
				if (callListener != null)
				{
					callListener.Run();
				}
				if (responseClass != null)
				{
					try
					{
						return System.Activator.CreateInstance(responseClass);
					}
					catch (Exception e)
					{
						throw new RuntimeException(e);
					}
				}
				else
				{
					return param;
				}
			}
			// echo param as result
		}

		private class SerialCaller : Sharpen.Thread
		{
			private Client client;

			private IPEndPoint server;

			private int count;

			private bool failed;

			public SerialCaller(Client client, IPEndPoint server, int count)
			{
				this.client = client;
				this.server = server;
				this.count = count;
			}

			public override void Run()
			{
				for (int i = 0; i < count; i++)
				{
					try
					{
						LongWritable param = new LongWritable(Random.NextLong());
						LongWritable value = (LongWritable)client.Call(param, server, null, null, 0, conf
							);
						if (!param.Equals(value))
						{
							Log.Fatal("Call failed!");
							failed = true;
							break;
						}
					}
					catch (Exception e)
					{
						Log.Fatal("Caught: " + StringUtils.StringifyException(e));
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
		private class TestInvocationHandler : RpcInvocationHandler
		{
			private static int retry = 0;

			private readonly Client client;

			private readonly Server server;

			private readonly int total;

			internal TestInvocationHandler(Client client, Server server, int total)
			{
				this.client = client;
				this.server = server;
				this.total = total;
			}

			/// <exception cref="System.Exception"/>
			public virtual object Invoke(object proxy, MethodInfo method, object[] args)
			{
				LongWritable param = new LongWritable(Random.NextLong());
				LongWritable value = (LongWritable)client.Call(param, NetUtils.GetConnectAddress(
					server), null, null, 0, conf);
				if (retry++ < total)
				{
					throw new IOException("Fake IOException");
				}
				else
				{
					return value;
				}
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Close()
			{
			}

			public virtual Client.ConnectionId GetConnectionId()
			{
				return null;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual void TestSerial()
		{
			InternalTestSerial(3, false, 2, 5, 100);
			InternalTestSerial(3, true, 2, 5, 10);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual void InternalTestSerial(int handlerCount, bool handlerSleep, int clientCount
			, int callerCount, int callCount)
		{
			Server server = new TestIPC.TestServer(handlerCount, handlerSleep);
			IPEndPoint addr = NetUtils.GetConnectAddress(server);
			server.Start();
			Client[] clients = new Client[clientCount];
			for (int i = 0; i < clientCount; i++)
			{
				clients[i] = new Client(typeof(LongWritable), conf);
			}
			TestIPC.SerialCaller[] callers = new TestIPC.SerialCaller[callerCount];
			for (int i_1 = 0; i_1 < callerCount; i_1++)
			{
				callers[i_1] = new TestIPC.SerialCaller(clients[i_1 % clientCount], addr, callCount
					);
				callers[i_1].Start();
			}
			for (int i_2 = 0; i_2 < callerCount; i_2++)
			{
				callers[i_2].Join();
				NUnit.Framework.Assert.IsFalse(callers[i_2].failed);
			}
			for (int i_3 = 0; i_3 < clientCount; i_3++)
			{
				clients[i_3].Stop();
			}
			server.Stop();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestStandAloneClient()
		{
			Client client = new Client(typeof(LongWritable), conf);
			IPEndPoint address = new IPEndPoint("127.0.0.1", 10);
			try
			{
				client.Call(new LongWritable(Random.NextLong()), address, null, null, 0, conf);
				NUnit.Framework.Assert.Fail("Expected an exception to have been thrown");
			}
			catch (IOException e)
			{
				string message = e.Message;
				string addressText = address.GetHostName() + ":" + address.Port;
				NUnit.Framework.Assert.IsTrue("Did not find " + addressText + " in " + message, message
					.Contains(addressText));
				Exception cause = e.InnerException;
				NUnit.Framework.Assert.IsNotNull("No nested exception in " + e, cause);
				string causeText = cause.Message;
				NUnit.Framework.Assert.IsTrue("Did not find " + causeText + " in " + message, message
					.Contains(causeText));
			}
			finally
			{
				client.Stop();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		internal static void MaybeThrowIOE()
		{
			if (WritableFaultsEnabled)
			{
				MaybeSleep();
				throw new IOException("Injected fault");
			}
		}

		internal static void MaybeThrowRTE()
		{
			if (WritableFaultsEnabled)
			{
				MaybeSleep();
				throw new RuntimeException("Injected fault");
			}
		}

		private static void MaybeSleep()
		{
			if (WritableFaultsSleep > 0)
			{
				try
				{
					Sharpen.Thread.Sleep(WritableFaultsSleep);
				}
				catch (Exception)
				{
				}
			}
		}

		private class IOEOnReadWritable : LongWritable
		{
			public IOEOnReadWritable()
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public override void ReadFields(BinaryReader @in)
			{
				base.ReadFields(@in);
				MaybeThrowIOE();
			}
		}

		private class RTEOnReadWritable : LongWritable
		{
			public RTEOnReadWritable()
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public override void ReadFields(BinaryReader @in)
			{
				base.ReadFields(@in);
				MaybeThrowRTE();
			}
		}

		private class IOEOnWriteWritable : LongWritable
		{
			public IOEOnWriteWritable()
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public override void Write(DataOutput @out)
			{
				base.Write(@out);
				MaybeThrowIOE();
			}
		}

		private class RTEOnWriteWritable : LongWritable
		{
			public RTEOnWriteWritable()
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public override void Write(DataOutput @out)
			{
				base.Write(@out);
				MaybeThrowRTE();
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
		/// <exception cref="Sharpen.InstantiationException"/>
		/// <exception cref="System.MemberAccessException"/>
		private void DoErrorTest(Type clientParamClass, Type serverParamClass, Type serverResponseClass
			, Type clientResponseClass)
		{
			// start server
			Server server = new TestIPC.TestServer(1, false, serverParamClass, serverResponseClass
				);
			IPEndPoint addr = NetUtils.GetConnectAddress(server);
			server.Start();
			// start client
			WritableFaultsEnabled = true;
			Client client = new Client(clientResponseClass, conf);
			try
			{
				LongWritable param = System.Activator.CreateInstance(clientParamClass);
				try
				{
					client.Call(param, addr, null, null, 0, conf);
					NUnit.Framework.Assert.Fail("Expected an exception to have been thrown");
				}
				catch (Exception t)
				{
					AssertExceptionContains(t, "Injected fault");
				}
				// Doing a second call with faults disabled should return fine --
				// ie the internal state of the client or server should not be broken
				// by the failed call
				WritableFaultsEnabled = false;
				client.Call(param, addr, null, null, 0, conf);
			}
			finally
			{
				client.Stop();
				server.Stop();
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestIOEOnClientWriteParam()
		{
			DoErrorTest(typeof(TestIPC.IOEOnWriteWritable), typeof(LongWritable), typeof(LongWritable
				), typeof(LongWritable));
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestRTEOnClientWriteParam()
		{
			DoErrorTest(typeof(TestIPC.RTEOnWriteWritable), typeof(LongWritable), typeof(LongWritable
				), typeof(LongWritable));
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestIOEOnServerReadParam()
		{
			DoErrorTest(typeof(LongWritable), typeof(TestIPC.IOEOnReadWritable), typeof(LongWritable
				), typeof(LongWritable));
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestRTEOnServerReadParam()
		{
			DoErrorTest(typeof(LongWritable), typeof(TestIPC.RTEOnReadWritable), typeof(LongWritable
				), typeof(LongWritable));
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestIOEOnServerWriteResponse()
		{
			DoErrorTest(typeof(LongWritable), typeof(LongWritable), typeof(TestIPC.IOEOnWriteWritable
				), typeof(LongWritable));
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestRTEOnServerWriteResponse()
		{
			DoErrorTest(typeof(LongWritable), typeof(LongWritable), typeof(TestIPC.RTEOnWriteWritable
				), typeof(LongWritable));
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestIOEOnClientReadResponse()
		{
			DoErrorTest(typeof(LongWritable), typeof(LongWritable), typeof(LongWritable), typeof(
				TestIPC.IOEOnReadWritable));
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestRTEOnClientReadResponse()
		{
			DoErrorTest(typeof(LongWritable), typeof(LongWritable), typeof(LongWritable), typeof(
				TestIPC.RTEOnReadWritable));
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
		public virtual void TestIOEOnWriteAfterPingClient()
		{
			// start server
			Client.SetPingInterval(conf, 100);
			try
			{
				WritableFaultsSleep = 1000;
				DoErrorTest(typeof(TestIPC.IOEOnWriteWritable), typeof(LongWritable), typeof(LongWritable
					), typeof(LongWritable));
			}
			finally
			{
				WritableFaultsSleep = 0;
			}
		}

		private static void AssertExceptionContains(Exception t, string substring)
		{
			string msg = StringUtils.StringifyException(t);
			NUnit.Framework.Assert.IsTrue("Exception should contain substring '" + substring 
				+ "':\n" + msg, msg.Contains(substring));
			Log.Info("Got expected exception", t);
		}

		/// <summary>
		/// Test that, if the socket factory throws an IOE, it properly propagates
		/// to the client.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestSocketFactoryException()
		{
			SocketFactory mockFactory = Org.Mockito.Mockito.Mock<SocketFactory>();
			Org.Mockito.Mockito.DoThrow(new IOException("Injected fault")).When(mockFactory).
				CreateSocket();
			Client client = new Client(typeof(LongWritable), conf, mockFactory);
			IPEndPoint address = new IPEndPoint("127.0.0.1", 10);
			try
			{
				client.Call(new LongWritable(Random.NextLong()), address, null, null, 0, conf);
				NUnit.Framework.Assert.Fail("Expected an exception to have been thrown");
			}
			catch (IOException e)
			{
				NUnit.Framework.Assert.IsTrue(e.Message.Contains("Injected fault"));
			}
			finally
			{
				client.Stop();
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
		public virtual void TestRTEDuringConnectionSetup()
		{
			// Set up a socket factory which returns sockets which
			// throw an RTE when setSoTimeout is called.
			SocketFactory spyFactory = Org.Mockito.Mockito.Spy(NetUtils.GetDefaultSocketFactory
				(conf));
			Org.Mockito.Mockito.DoAnswer(new _Answer_527()).When(spyFactory).CreateSocket();
			Server server = new TestIPC.TestServer(1, true);
			Client client = new Client(typeof(LongWritable), conf, spyFactory);
			server.Start();
			try
			{
				// Call should fail due to injected exception.
				IPEndPoint address = NetUtils.GetConnectAddress(server);
				try
				{
					client.Call(new LongWritable(Random.NextLong()), address, null, null, 0, conf);
					NUnit.Framework.Assert.Fail("Expected an exception to have been thrown");
				}
				catch (Exception e)
				{
					Log.Info("caught expected exception", e);
					NUnit.Framework.Assert.IsTrue(StringUtils.StringifyException(e).Contains("Injected fault"
						));
				}
				// Resetting to the normal socket behavior should succeed
				// (i.e. it should not have cached a half-constructed connection)
				Org.Mockito.Mockito.Reset(spyFactory);
				client.Call(new LongWritable(Random.NextLong()), address, null, null, 0, conf);
			}
			finally
			{
				client.Stop();
				server.Stop();
			}
		}

		private sealed class _Answer_527 : Answer<Socket>
		{
			public _Answer_527()
			{
			}

			/// <exception cref="System.Exception"/>
			public Socket Answer(InvocationOnMock invocation)
			{
				Socket s = Org.Mockito.Mockito.Spy((Socket)invocation.CallRealMethod());
				Org.Mockito.Mockito.DoThrow(new RuntimeException("Injected fault")).When(s).ReceiveTimeout
					 = Matchers.AnyInt();
				return s;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestIpcTimeout()
		{
			// start server
			Server server = new TestIPC.TestServer(1, true);
			IPEndPoint addr = NetUtils.GetConnectAddress(server);
			server.Start();
			// start client
			Client client = new Client(typeof(LongWritable), conf);
			// set timeout to be less than MIN_SLEEP_TIME
			try
			{
				client.Call(new LongWritable(Random.NextLong()), addr, null, null, MinSleepTime /
					 2, conf);
				NUnit.Framework.Assert.Fail("Expected an exception to have been thrown");
			}
			catch (SocketTimeoutException e)
			{
				Log.Info("Get a SocketTimeoutException ", e);
			}
			// set timeout to be bigger than 3*ping interval
			client.Call(new LongWritable(Random.NextLong()), addr, null, null, 3 * PingInterval
				 + MinSleepTime, conf);
			client.Stop();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestIpcConnectTimeout()
		{
			// start server
			Server server = new TestIPC.TestServer(1, true);
			IPEndPoint addr = NetUtils.GetConnectAddress(server);
			//Intentionally do not start server to get a connection timeout
			// start client
			Client.SetConnectTimeout(conf, 100);
			Client client = new Client(typeof(LongWritable), conf);
			// set the rpc timeout to twice the MIN_SLEEP_TIME
			try
			{
				client.Call(new LongWritable(Random.NextLong()), addr, null, null, MinSleepTime *
					 2, conf);
				NUnit.Framework.Assert.Fail("Expected an exception to have been thrown");
			}
			catch (SocketTimeoutException e)
			{
				Log.Info("Get a SocketTimeoutException ", e);
			}
			client.Stop();
		}

		/// <summary>Check service class byte in IPC header is correct on wire.</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestIpcWithServiceClass()
		{
			// start server
			Server server = new TestIPC.TestServer(5, false);
			IPEndPoint addr = NetUtils.GetConnectAddress(server);
			server.Start();
			// start client
			Client.SetConnectTimeout(conf, 10000);
			CallAndVerify(server, addr, 0, true);
			// Service Class is low to -128 as byte on wire.
			// -128 shouldn't be casted on wire but -129 should.
			CallAndVerify(server, addr, -128, true);
			CallAndVerify(server, addr, -129, false);
			// Service Class is up to 127.
			// 127 shouldn't be casted on wire but 128 should.
			CallAndVerify(server, addr, 127, true);
			CallAndVerify(server, addr, 128, false);
			server.Stop();
		}

		private class TestServerQueue : Server
		{
			internal readonly CountDownLatch firstCallLatch = new CountDownLatch(1);

			internal readonly CountDownLatch callBlockLatch = new CountDownLatch(1);

			/// <exception cref="System.IO.IOException"/>
			internal TestServerQueue(int expectedCalls, int readers, int callQ, int handlers, 
				Configuration conf)
				: base(Address, 0, typeof(LongWritable), handlers, readers, callQ, conf, null, null
					)
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public override Writable Call(RPC.RpcKind rpcKind, string protocol, Writable param
				, long receiveTime)
			{
				firstCallLatch.CountDown();
				try
				{
					callBlockLatch.Await();
				}
				catch (Exception e)
				{
					throw new IOException(e);
				}
				return param;
			}
		}

		/// <summary>Check that reader queueing works</summary>
		/// <exception cref="Sharpen.BrokenBarrierException"></exception>
		/// <exception cref="System.Exception"></exception>
		public virtual void TestIpcWithReaderQueuing()
		{
			// 1 reader, 1 connectionQ slot, 1 callq
			for (int i = 0; i < 10; i++)
			{
				CheckBlocking(1, 1, 1);
			}
			// 4 readers, 5 connectionQ slots, 2 callq
			for (int i_1 = 0; i_1 < 10; i_1++)
			{
				CheckBlocking(4, 5, 2);
			}
		}

		// goal is to jam a handler with a connection, fill the callq with
		// connections, in turn jamming the readers - then flood the server and
		// ensure that the listener blocks when the reader connection queues fill
		/// <exception cref="System.Exception"/>
		private void CheckBlocking(int readers, int readerQ, int callQ)
		{
			int handlers = 1;
			// makes it easier
			Configuration conf = new Configuration();
			conf.SetInt(CommonConfigurationKeys.IpcServerRpcReadConnectionQueueSizeKey, readerQ
				);
			// send in enough clients to block up the handlers, callq, and readers
			int initialClients = readers + callQ + handlers;
			// max connections we should ever end up accepting at once
			int maxAccept = initialClients + readers * readerQ + 1;
			// 1 = listener
			// stress it with 2X the max
			int clients = maxAccept * 2;
			AtomicInteger failures = new AtomicInteger(0);
			CountDownLatch callFinishedLatch = new CountDownLatch(clients);
			// start server
			TestIPC.TestServerQueue server = new TestIPC.TestServerQueue(clients, readers, callQ
				, handlers, conf);
			IPEndPoint addr = NetUtils.GetConnectAddress(server);
			server.Start();
			Client.SetConnectTimeout(conf, 10000);
			// instantiate the threads, will start in batches
			Sharpen.Thread[] threads = new Sharpen.Thread[clients];
			for (int i = 0; i < clients; i++)
			{
				threads[i] = new Sharpen.Thread(new _Runnable_704(conf, addr, failures, callFinishedLatch
					));
			}
			// start enough clients to block up the handler, callq, and each reader;
			// let the calls sequentially slot in to avoid some readers blocking
			// and others not blocking in the race to fill the callq
			for (int i_1 = 0; i_1 < initialClients; i_1++)
			{
				threads[i_1].Start();
				if (i_1 == 0)
				{
					// let first reader block in a call
					server.firstCallLatch.Await();
				}
				else
				{
					if (i_1 <= callQ)
					{
						// let subsequent readers jam the callq, will happen immediately 
						while (server.GetCallQueueLen() != i_1)
						{
							Sharpen.Thread.Sleep(1);
						}
					}
				}
			}
			// additional threads block the readers trying to add to the callq
			// wait till everything is slotted, should happen immediately
			Sharpen.Thread.Sleep(10);
			if (server.GetNumOpenConnections() < initialClients)
			{
				Log.Info("(initial clients) need:" + initialClients + " connections have:" + server
					.GetNumOpenConnections());
				Sharpen.Thread.Sleep(100);
			}
			Log.Info("ipc layer should be blocked");
			NUnit.Framework.Assert.AreEqual(callQ, server.GetCallQueueLen());
			NUnit.Framework.Assert.AreEqual(initialClients, server.GetNumOpenConnections());
			// now flood the server with the rest of the connections, the reader's
			// connection queues should fill and then the listener should block
			for (int i_2 = initialClients; i_2 < clients; i_2++)
			{
				threads[i_2].Start();
			}
			Sharpen.Thread.Sleep(10);
			if (server.GetNumOpenConnections() < maxAccept)
			{
				Log.Info("(max clients) need:" + maxAccept + " connections have:" + server.GetNumOpenConnections
					());
				Sharpen.Thread.Sleep(100);
			}
			// check a few times to make sure we didn't go over
			for (int i_3 = 0; i_3 < 4; i_3++)
			{
				NUnit.Framework.Assert.AreEqual(maxAccept, server.GetNumOpenConnections());
				Sharpen.Thread.Sleep(100);
			}
			// sanity check that no calls have finished
			NUnit.Framework.Assert.AreEqual(clients, callFinishedLatch.GetCount());
			Log.Info("releasing the calls");
			server.callBlockLatch.CountDown();
			callFinishedLatch.Await();
			foreach (Sharpen.Thread t in threads)
			{
				t.Join();
			}
			NUnit.Framework.Assert.AreEqual(0, failures.Get());
			server.Stop();
		}

		private sealed class _Runnable_704 : Runnable
		{
			public _Runnable_704(Configuration conf, IPEndPoint addr, AtomicInteger failures, 
				CountDownLatch callFinishedLatch)
			{
				this.conf = conf;
				this.addr = addr;
				this.failures = failures;
				this.callFinishedLatch = callFinishedLatch;
			}

			public void Run()
			{
				Client client = new Client(typeof(LongWritable), conf);
				try
				{
					client.Call(new LongWritable(Sharpen.Thread.CurrentThread().GetId()), addr, null, 
						null, 60000, conf);
				}
				catch (Exception e)
				{
					TestIPC.Log.Error(e);
					failures.IncrementAndGet();
					return;
				}
				finally
				{
					callFinishedLatch.CountDown();
					client.Stop();
				}
			}

			private readonly Configuration conf;

			private readonly IPEndPoint addr;

			private readonly AtomicInteger failures;

			private readonly CountDownLatch callFinishedLatch;
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestConnectionIdleTimeouts()
		{
			((Log4JLogger)Server.Log).GetLogger().SetLevel(Level.Debug);
			int maxIdle = 1000;
			int cleanupInterval = maxIdle * 3 / 4;
			// stagger cleanups
			int killMax = 3;
			int clients = 1 + killMax * 2;
			// 1 to block, 2 batches to kill
			conf.SetInt(CommonConfigurationKeysPublic.IpcClientConnectionMaxidletimeKey, maxIdle
				);
			conf.SetInt(CommonConfigurationKeysPublic.IpcClientIdlethresholdKey, 0);
			conf.SetInt(CommonConfigurationKeysPublic.IpcClientKillMaxKey, killMax);
			conf.SetInt(CommonConfigurationKeys.IpcClientConnectionIdlescanintervalKey, cleanupInterval
				);
			CyclicBarrier firstCallBarrier = new CyclicBarrier(2);
			CyclicBarrier callBarrier = new CyclicBarrier(clients);
			CountDownLatch allCallLatch = new CountDownLatch(clients);
			AtomicBoolean error = new AtomicBoolean();
			TestIPC.TestServer server = new TestIPC.TestServer(clients, false);
			Sharpen.Thread[] threads = new Sharpen.Thread[clients];
			try
			{
				server.callListener = new _Runnable_798(allCallLatch, firstCallBarrier, callBarrier
					, error);
				// block first call
				server.Start();
				// start client
				CountDownLatch callReturned = new CountDownLatch(clients - 1);
				IPEndPoint addr = NetUtils.GetConnectAddress(server);
				Configuration clientConf = new Configuration();
				clientConf.SetInt(CommonConfigurationKeysPublic.IpcClientConnectionMaxidletimeKey
					, 10000);
				for (int i = 0; i < clients; i++)
				{
					threads[i] = new Sharpen.Thread(new _Runnable_824(clientConf, addr, callReturned)
						);
					threads[i].Start();
				}
				// all calls blocked in handler so all connections made
				allCallLatch.Await();
				NUnit.Framework.Assert.IsFalse(error.Get());
				NUnit.Framework.Assert.AreEqual(clients, server.GetNumOpenConnections());
				// wake up blocked calls and wait for client call to return, no
				// connections should have closed
				callBarrier.Await();
				callReturned.Await();
				NUnit.Framework.Assert.AreEqual(clients, server.GetNumOpenConnections());
				// server won't close till maxIdle*2, so give scanning thread time to
				// be almost ready to close idle connection.  after which it should
				// close max connections on every cleanupInterval
				Sharpen.Thread.Sleep(maxIdle * 2 - cleanupInterval);
				for (int i_1 = clients; i_1 > 1; i_1 -= killMax)
				{
					Sharpen.Thread.Sleep(cleanupInterval);
					NUnit.Framework.Assert.IsFalse(error.Get());
					NUnit.Framework.Assert.AreEqual(i_1, server.GetNumOpenConnections());
				}
				// connection for the first blocked call should still be open
				Sharpen.Thread.Sleep(cleanupInterval);
				NUnit.Framework.Assert.IsFalse(error.Get());
				NUnit.Framework.Assert.AreEqual(1, server.GetNumOpenConnections());
				// wake up call and ensure connection times out
				firstCallBarrier.Await();
				Sharpen.Thread.Sleep(maxIdle * 2);
				NUnit.Framework.Assert.IsFalse(error.Get());
				NUnit.Framework.Assert.AreEqual(0, server.GetNumOpenConnections());
			}
			finally
			{
				foreach (Sharpen.Thread t in threads)
				{
					if (t != null)
					{
						t.Interrupt();
						t.Join();
					}
					server.Stop();
				}
			}
		}

		private sealed class _Runnable_798 : Runnable
		{
			public _Runnable_798(CountDownLatch allCallLatch, CyclicBarrier firstCallBarrier, 
				CyclicBarrier callBarrier, AtomicBoolean error)
			{
				this.allCallLatch = allCallLatch;
				this.firstCallBarrier = firstCallBarrier;
				this.callBarrier = callBarrier;
				this.error = error;
				this.first = new AtomicBoolean(true);
			}

			internal AtomicBoolean first;

			public void Run()
			{
				try
				{
					allCallLatch.CountDown();
					if (this.first.CompareAndSet(true, false))
					{
						firstCallBarrier.Await();
					}
					else
					{
						callBarrier.Await();
					}
				}
				catch (Exception t)
				{
					TestIPC.Log.Error(t);
					error.Set(true);
				}
			}

			private readonly CountDownLatch allCallLatch;

			private readonly CyclicBarrier firstCallBarrier;

			private readonly CyclicBarrier callBarrier;

			private readonly AtomicBoolean error;
		}

		private sealed class _Runnable_824 : Runnable
		{
			public _Runnable_824(Configuration clientConf, IPEndPoint addr, CountDownLatch callReturned
				)
			{
				this.clientConf = clientConf;
				this.addr = addr;
				this.callReturned = callReturned;
			}

			public void Run()
			{
				Client client = new Client(typeof(LongWritable), clientConf);
				try
				{
					client.Call(new LongWritable(Sharpen.Thread.CurrentThread().GetId()), addr, null, 
						null, 0, clientConf);
					callReturned.CountDown();
					Sharpen.Thread.Sleep(10000);
				}
				catch (IOException e)
				{
					TestIPC.Log.Error(e);
				}
				catch (Exception)
				{
				}
				finally
				{
					client.Stop();
				}
			}

			private readonly Configuration clientConf;

			private readonly IPEndPoint addr;

			private readonly CountDownLatch callReturned;
		}

		/// <summary>Make a call from a client and verify if header info is changed in server side
		/// 	</summary>
		/// <exception cref="System.IO.IOException"/>
		private void CallAndVerify(Server server, IPEndPoint addr, int serviceClass, bool
			 noChanged)
		{
			Client client = new Client(typeof(LongWritable), conf);
			client.Call(new LongWritable(Random.NextLong()), addr, null, null, MinSleepTime, 
				serviceClass, conf);
			Server.Connection connection = server.GetConnections()[0];
			int serviceClass2 = connection.GetServiceClass();
			NUnit.Framework.Assert.IsFalse(noChanged ^ serviceClass == serviceClass2);
			client.Stop();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestIpcAfterStopping()
		{
			// start server
			Server server = new TestIPC.TestServer(5, false);
			IPEndPoint addr = NetUtils.GetConnectAddress(server);
			server.Start();
			// start client
			Client client = new Client(typeof(LongWritable), conf);
			client.Call(new LongWritable(Random.NextLong()), addr, null, null, MinSleepTime, 
				0, conf);
			client.Stop();
			// This call should throw IOException.
			client.Call(new LongWritable(Random.NextLong()), addr, null, null, MinSleepTime, 
				0, conf);
		}

		/// <summary>
		/// Check that file descriptors aren't leaked by starting
		/// and stopping IPC servers.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestSocketLeak()
		{
			Assume.AssumeTrue(FdDir.Exists());
			// only run on Linux
			long startFds = CountOpenFileDescriptors();
			for (int i = 0; i < 50; i++)
			{
				Server server = new TestIPC.TestServer(1, true);
				server.Start();
				server.Stop();
			}
			long endFds = CountOpenFileDescriptors();
			NUnit.Framework.Assert.IsTrue("Leaked " + (endFds - startFds) + " file descriptors"
				, endFds - startFds < 20);
		}

		/// <summary>
		/// Check if Client is interrupted after handling
		/// InterruptedException during cleanup
		/// </summary>
		public virtual void TestInterrupted()
		{
			Client client = new Client(typeof(LongWritable), conf);
			Client.GetClientExecutor().Submit(new _Runnable_946());
			Sharpen.Thread.CurrentThread().Interrupt();
			client.Stop();
			try
			{
				NUnit.Framework.Assert.IsTrue(Sharpen.Thread.CurrentThread().IsInterrupted());
				Log.Info("Expected thread interrupt during client cleanup");
			}
			catch (Exception)
			{
				Log.Error("The Client did not interrupt after handling an Interrupted Exception");
				NUnit.Framework.Assert.Fail("The Client did not interrupt after handling an Interrupted Exception"
					);
			}
			// Clear Thread interrupt
			Sharpen.Thread.Interrupted();
		}

		private sealed class _Runnable_946 : Runnable
		{
			public _Runnable_946()
			{
			}

			public void Run()
			{
				while (true)
				{
				}
			}
		}

		private long CountOpenFileDescriptors()
		{
			return FdDir.List().Length;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestIpcFromHadoop_0_18_13()
		{
			DoIpcVersionTest(TestIPC.NetworkTraces.Hadoop0183RpcDump, TestIPC.NetworkTraces.ResponseToHadoop0183Rpc
				);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestIpcFromHadoop0_20_3()
		{
			DoIpcVersionTest(TestIPC.NetworkTraces.Hadoop0203RpcDump, TestIPC.NetworkTraces.ResponseToHadoop0203Rpc
				);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestIpcFromHadoop0_21_0()
		{
			DoIpcVersionTest(TestIPC.NetworkTraces.Hadoop0210RpcDump, TestIPC.NetworkTraces.ResponseToHadoop0210Rpc
				);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestHttpGetResponse()
		{
			DoIpcVersionTest(Sharpen.Runtime.GetBytesForString("GET / HTTP/1.0\r\n\r\n"), Sharpen.Runtime.GetBytesForString
				(Server.ReceivedHttpReqResponse));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestConnectionRetriesOnSocketTimeoutExceptions()
		{
			Configuration conf = new Configuration();
			// set max retries to 0
			conf.SetInt(CommonConfigurationKeysPublic.IpcClientConnectMaxRetriesOnSocketTimeoutsKey
				, 0);
			AssertRetriesOnSocketTimeouts(conf, 1);
			// set max retries to 3
			conf.SetInt(CommonConfigurationKeysPublic.IpcClientConnectMaxRetriesOnSocketTimeoutsKey
				, 3);
			AssertRetriesOnSocketTimeouts(conf, 4);
		}

		private class CallInfo
		{
			internal int id = RpcConstants.InvalidCallId;

			internal int retry = RpcConstants.InvalidRetryCount;
		}

		/// <summary>
		/// Test if
		/// (1) the rpc server uses the call id/retry provided by the rpc client, and
		/// (2) the rpc client receives the same call id/retry from the rpc server.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestCallIdAndRetry()
		{
			TestIPC.CallInfo info = new TestIPC.CallInfo();
			// Override client to store the call info and check response
			Client client = new _Client_1023(info, typeof(LongWritable), conf);
			// Attach a listener that tracks every call received by the server.
			TestIPC.TestServer server = new TestIPC.TestServer(1, false);
			server.callListener = new _Runnable_1042(info);
			try
			{
				IPEndPoint addr = NetUtils.GetConnectAddress(server);
				server.Start();
				TestIPC.SerialCaller caller = new TestIPC.SerialCaller(client, addr, 10);
				caller.Run();
				NUnit.Framework.Assert.IsFalse(caller.failed);
			}
			finally
			{
				client.Stop();
				server.Stop();
			}
		}

		private sealed class _Client_1023 : Client
		{
			public _Client_1023(TestIPC.CallInfo info, Type baseArg1, Configuration baseArg2)
				: base(baseArg1, baseArg2)
			{
				this.info = info;
			}

			internal override Client.Call CreateCall(RPC.RpcKind rpcKind, Writable rpcRequest
				)
			{
				Client.Call call = base.CreateCall(rpcKind, rpcRequest);
				info.id = call.id;
				info.retry = call.retry;
				return call;
			}

			/// <exception cref="System.IO.IOException"/>
			internal override void CheckResponse(RpcHeaderProtos.RpcResponseHeaderProto header
				)
			{
				base.CheckResponse(header);
				NUnit.Framework.Assert.AreEqual(info.id, header.GetCallId());
				NUnit.Framework.Assert.AreEqual(info.retry, header.GetRetryCount());
			}

			private readonly TestIPC.CallInfo info;
		}

		private sealed class _Runnable_1042 : Runnable
		{
			public _Runnable_1042(TestIPC.CallInfo info)
			{
				this.info = info;
			}

			public void Run()
			{
				NUnit.Framework.Assert.AreEqual(info.id, Server.GetCallId());
				NUnit.Framework.Assert.AreEqual(info.retry, Server.GetCallRetryCount());
			}

			private readonly TestIPC.CallInfo info;
		}

		/// <summary>A dummy protocol</summary>
		private interface DummyProtocol
		{
			void DummyRun();
		}

		/// <summary>Test the retry count while used in a retry proxy.</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestRetryProxy()
		{
			Client client = new Client(typeof(LongWritable), conf);
			TestIPC.TestServer server = new TestIPC.TestServer(1, false);
			server.callListener = new _Runnable_1075();
			// try more times, so it is easier to find race condition bug
			// 10000 times runs about 6s on a core i7 machine
			int totalRetry = 10000;
			TestIPC.DummyProtocol proxy = (TestIPC.DummyProtocol)Proxy.NewProxyInstance(typeof(
				TestIPC.DummyProtocol).GetClassLoader(), new Type[] { typeof(TestIPC.DummyProtocol
				) }, new TestIPC.TestInvocationHandler(client, server, totalRetry));
			TestIPC.DummyProtocol retryProxy = (TestIPC.DummyProtocol)RetryProxy.Create<TestIPC.DummyProtocol
				>(proxy, RetryPolicies.RetryForever);
			try
			{
				server.Start();
				retryProxy.DummyRun();
				NUnit.Framework.Assert.AreEqual(TestIPC.TestInvocationHandler.retry, totalRetry +
					 1);
			}
			finally
			{
				Client.SetCallIdAndRetryCount(0, 0);
				client.Stop();
				server.Stop();
			}
		}

		private sealed class _Runnable_1075 : Runnable
		{
			public _Runnable_1075()
			{
				this.retryCount = 0;
			}

			private int retryCount;

			public void Run()
			{
				NUnit.Framework.Assert.AreEqual(this.retryCount++, Server.GetCallRetryCount());
			}
		}

		/// <summary>Test if the rpc server gets the default retry count (0) from client.</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestInitialCallRetryCount()
		{
			// Override client to store the call id
			Client client = new Client(typeof(LongWritable), conf);
			// Attach a listener that tracks every call ID received by the server.
			TestIPC.TestServer server = new TestIPC.TestServer(1, false);
			server.callListener = new _Runnable_1114();
			// we have not set the retry count for the client, thus on the server
			// side we should see retry count as 0
			try
			{
				IPEndPoint addr = NetUtils.GetConnectAddress(server);
				server.Start();
				TestIPC.SerialCaller caller = new TestIPC.SerialCaller(client, addr, 10);
				caller.Run();
				NUnit.Framework.Assert.IsFalse(caller.failed);
			}
			finally
			{
				client.Stop();
				server.Stop();
			}
		}

		private sealed class _Runnable_1114 : Runnable
		{
			public _Runnable_1114()
			{
			}

			public void Run()
			{
				NUnit.Framework.Assert.AreEqual(0, Server.GetCallRetryCount());
			}
		}

		/// <summary>Test if the rpc server gets the retry count from client.</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestCallRetryCount()
		{
			int retryCount = 255;
			// Override client to store the call id
			Client client = new Client(typeof(LongWritable), conf);
			Client.SetCallIdAndRetryCount(Client.NextCallId(), 255);
			// Attach a listener that tracks every call ID received by the server.
			TestIPC.TestServer server = new TestIPC.TestServer(1, false);
			server.callListener = new _Runnable_1147(retryCount);
			// we have not set the retry count for the client, thus on the server
			// side we should see retry count as 0
			try
			{
				IPEndPoint addr = NetUtils.GetConnectAddress(server);
				server.Start();
				TestIPC.SerialCaller caller = new TestIPC.SerialCaller(client, addr, 10);
				caller.Run();
				NUnit.Framework.Assert.IsFalse(caller.failed);
			}
			finally
			{
				client.Stop();
				server.Stop();
			}
		}

		private sealed class _Runnable_1147 : Runnable
		{
			public _Runnable_1147(int retryCount)
			{
				this.retryCount = retryCount;
			}

			public void Run()
			{
				NUnit.Framework.Assert.AreEqual(retryCount, Server.GetCallRetryCount());
			}

			private readonly int retryCount;
		}

		/// <summary>
		/// Tests that client generates a unique sequential call ID for each RPC call,
		/// even if multiple threads are using the same client.
		/// </summary>
		/// <exception cref="System.Exception"></exception>
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestUniqueSequentialCallIds()
		{
			int serverThreads = 10;
			int callerCount = 100;
			int perCallerCallCount = 100;
			TestIPC.TestServer server = new TestIPC.TestServer(serverThreads, false);
			// Attach a listener that tracks every call ID received by the server.  This
			// list must be synchronized, because multiple server threads will add to it.
			IList<int> callIds = Collections.SynchronizedList(new AList<int>());
			server.callListener = new _Runnable_1183(callIds);
			Client client = new Client(typeof(LongWritable), conf);
			try
			{
				IPEndPoint addr = NetUtils.GetConnectAddress(server);
				server.Start();
				TestIPC.SerialCaller[] callers = new TestIPC.SerialCaller[callerCount];
				for (int i = 0; i < callerCount; ++i)
				{
					callers[i] = new TestIPC.SerialCaller(client, addr, perCallerCallCount);
					callers[i].Start();
				}
				for (int i_1 = 0; i_1 < callerCount; ++i_1)
				{
					callers[i_1].Join();
					NUnit.Framework.Assert.IsFalse(callers[i_1].failed);
				}
			}
			finally
			{
				client.Stop();
				server.Stop();
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

		private sealed class _Runnable_1183 : Runnable
		{
			public _Runnable_1183(IList<int> callIds)
			{
				this.callIds = callIds;
			}

			public void Run()
			{
				callIds.AddItem(Server.GetCallId());
			}

			private readonly IList<int> callIds;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestMaxConnections()
		{
			conf.SetInt("ipc.server.max.connections", 5);
			Server server = null;
			Sharpen.Thread[] connectors = new Sharpen.Thread[10];
			try
			{
				server = new TestIPC.TestServer(3, false);
				IPEndPoint addr = NetUtils.GetConnectAddress(server);
				server.Start();
				NUnit.Framework.Assert.AreEqual(0, server.GetNumOpenConnections());
				for (int i = 0; i < 10; i++)
				{
					connectors[i] = new _Thread_1235(addr);
					connectors[i].Start();
				}
				Sharpen.Thread.Sleep(1000);
				// server should only accept up to 5 connections
				NUnit.Framework.Assert.AreEqual(5, server.GetNumOpenConnections());
				for (int i_1 = 0; i_1 < 10; i_1++)
				{
					connectors[i_1].Join();
				}
			}
			finally
			{
				if (server != null)
				{
					server.Stop();
				}
				conf.SetInt("ipc.server.max.connections", 0);
			}
		}

		private sealed class _Thread_1235 : Sharpen.Thread
		{
			public _Thread_1235(IPEndPoint addr)
			{
				this.addr = addr;
			}

			public override void Run()
			{
				Socket sock = null;
				try
				{
					sock = NetUtils.GetDefaultSocketFactory(TestIPC.conf).CreateSocket();
					NetUtils.Connect(sock, addr, 3000);
					try
					{
						Sharpen.Thread.Sleep(4000);
					}
					catch (Exception)
					{
					}
				}
				catch (IOException)
				{
				}
				finally
				{
					if (sock != null)
					{
						try
						{
							sock.Close();
						}
						catch (IOException)
						{
						}
					}
				}
			}

			private readonly IPEndPoint addr;
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestClientGetTimeout()
		{
			Configuration config = new Configuration();
			NUnit.Framework.Assert.AreEqual(Client.GetTimeout(config), -1);
		}

		/// <exception cref="System.IO.IOException"/>
		private void AssertRetriesOnSocketTimeouts(Configuration conf, int maxTimeoutRetries
			)
		{
			SocketFactory mockFactory = Org.Mockito.Mockito.Mock<SocketFactory>();
			Org.Mockito.Mockito.DoThrow(new ConnectTimeoutException("fake")).When(mockFactory
				).CreateSocket();
			Client client = new Client(typeof(IntWritable), conf, mockFactory);
			IPEndPoint address = new IPEndPoint("127.0.0.1", 9090);
			try
			{
				client.Call(new IntWritable(Random.Next()), address, null, null, 0, conf);
				NUnit.Framework.Assert.Fail("Not throwing the SocketTimeoutException");
			}
			catch (SocketTimeoutException)
			{
				Org.Mockito.Mockito.Verify(mockFactory, Org.Mockito.Mockito.Times(maxTimeoutRetries
					)).CreateSocket();
			}
			client.Stop();
		}

		/// <exception cref="System.IO.IOException"/>
		private void DoIpcVersionTest(byte[] requestData, byte[] expectedResponse)
		{
			Server server = new TestIPC.TestServer(1, true);
			IPEndPoint addr = NetUtils.GetConnectAddress(server);
			server.Start();
			Socket socket = new Socket();
			try
			{
				NetUtils.Connect(socket, addr, 5000);
				OutputStream @out = socket.GetOutputStream();
				InputStream @in = socket.GetInputStream();
				@out.Write(requestData, 0, requestData.Length);
				@out.Flush();
				ByteArrayOutputStream baos = new ByteArrayOutputStream();
				IOUtils.CopyBytes(@in, baos, 256);
				byte[] responseData = baos.ToByteArray();
				NUnit.Framework.Assert.AreEqual(StringUtils.ByteToHexString(expectedResponse), StringUtils
					.ByteToHexString(responseData));
			}
			finally
			{
				IOUtils.CloseSocket(socket);
				server.Stop();
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
		private static byte[] HexDumpToBytes(string hexdump)
		{
			int LastHexCol = 3 * 16;
			StringBuilder hexString = new StringBuilder();
			foreach (string line in StringUtils.ToUpperCase(hexdump).Split("\n"))
			{
				hexString.Append(Sharpen.Runtime.Substring(line, 0, LastHexCol).Replace(" ", string.Empty
					));
			}
			return StringUtils.HexStringToByte(hexString.ToString());
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
			internal static readonly byte[] Hadoop0183RpcDump = HexDumpToBytes("68 72 70 63 02 00 00 00  82 00 1d 6f 72 67 2e 61 hrpc.... ...org.a\n"
				 + "70 61 63 68 65 2e 68 61  64 6f 6f 70 2e 69 6f 2e pache.ha doop.io.\n" + "57 72 69 74 61 62 6c 65  00 30 6f 72 67 2e 61 70 Writable .0org.ap\n"
				 + "61 63 68 65 2e 68 61 64  6f 6f 70 2e 69 6f 2e 4f ache.had oop.io.O\n" + "62 6a 65 63 74 57 72 69  74 61 62 6c 65 24 4e 75 bjectWri table$Nu\n"
				 + "6c 6c 49 6e 73 74 61 6e  63 65 00 2f 6f 72 67 2e llInstan ce./org.\n" + "61 70 61 63 68 65 2e 68  61 64 6f 6f 70 2e 73 65 apache.h adoop.se\n"
				 + "63 75 72 69 74 79 2e 55  73 65 72 47 72 6f 75 70 curity.U serGroup\n" + "49 6e 66 6f 72 6d 61 74  69 6f 6e 00 00 00 6c 00 Informat ion...l.\n"
				 + "00 00 00 00 12 67 65 74  50 72 6f 74 6f 63 6f 6c .....get Protocol\n" + "56 65 72 73 69 6f 6e 00  00 00 02 00 10 6a 61 76 Version. .....jav\n"
				 + "61 2e 6c 61 6e 67 2e 53  74 72 69 6e 67 00 2e 6f a.lang.S tring..o\n" + "72 67 2e 61 70 61 63 68  65 2e 68 61 64 6f 6f 70 rg.apach e.hadoop\n"
				 + "2e 6d 61 70 72 65 64 2e  4a 6f 62 53 75 62 6d 69 .mapred. JobSubmi\n" + "73 73 69 6f 6e 50 72 6f  74 6f 63 6f 6c 00 04 6c ssionPro tocol..l\n"
				 + "6f 6e 67 00 00 00 00 00  00 00 0a                ong..... ...     \n");

			internal const string Hadoop018ErrorMsg = "Server IPC version " + RpcConstants.CurrentVersion
				 + " cannot communicate with client version 2";

			/// <summary>
			/// Wireshark dump of the correct response that triggers an error message
			/// on an 0.18.3 client.
			/// </summary>
			internal static readonly byte[] ResponseToHadoop0183Rpc = Bytes.Concat(HexDumpToBytes
				("00 00 00 00 01 00 00 00  29 6f 72 67 2e 61 70 61 ........ )org.apa\n" + "63 68 65 2e 68 61 64 6f  6f 70 2e 69 70 63 2e 52 che.hado op.ipc.R\n"
				 + "50 43 24 56 65 72 73 69  6f 6e 4d 69 73 6d 61 74 PC$Versi onMismat\n" + "63 68                                            ch               \n"
				), Ints.ToByteArray(Hadoop018ErrorMsg.Length), Sharpen.Runtime.GetBytesForString
				(Hadoop018ErrorMsg));

			/// <summary>Wireshark dump of an RPC request from Hadoop 0.20.3</summary>
			internal static readonly byte[] Hadoop0203RpcDump = HexDumpToBytes("68 72 70 63 03 00 00 00  79 27 6f 72 67 2e 61 70 hrpc.... y'org.ap\n"
				 + "61 63 68 65 2e 68 61 64  6f 6f 70 2e 69 70 63 2e ache.had oop.ipc.\n" + "56 65 72 73 69 6f 6e 65  64 50 72 6f 74 6f 63 6f Versione dProtoco\n"
				 + "6c 01 0a 53 54 52 49 4e  47 5f 55 47 49 04 74 6f l..STRIN G_UGI.to\n" + "64 64 09 04 74 6f 64 64  03 61 64 6d 07 64 69 61 dd..todd .adm.dia\n"
				 + "6c 6f 75 74 05 63 64 72  6f 6d 07 70 6c 75 67 64 lout.cdr om.plugd\n" + "65 76 07 6c 70 61 64 6d  69 6e 05 61 64 6d 69 6e ev.lpadm in.admin\n"
				 + "0a 73 61 6d 62 61 73 68  61 72 65 06 6d 72 74 65 .sambash are.mrte\n" + "73 74 00 00 00 6c 00 00  00 00 00 12 67 65 74 50 st...l.. ....getP\n"
				 + "72 6f 74 6f 63 6f 6c 56  65 72 73 69 6f 6e 00 00 rotocolV ersion..\n" + "00 02 00 10 6a 61 76 61  2e 6c 61 6e 67 2e 53 74 ....java .lang.St\n"
				 + "72 69 6e 67 00 2e 6f 72  67 2e 61 70 61 63 68 65 ring..or g.apache\n" + "2e 68 61 64 6f 6f 70 2e  6d 61 70 72 65 64 2e 4a .hadoop. mapred.J\n"
				 + "6f 62 53 75 62 6d 69 73  73 69 6f 6e 50 72 6f 74 obSubmis sionProt\n" + "6f 63 6f 6c 00 04 6c 6f  6e 67 00 00 00 00 00 00 ocol..lo ng......\n"
				 + "00 14                                            ..               \n");

			internal const string Hadoop020ErrorMsg = "Server IPC version " + RpcConstants.CurrentVersion
				 + " cannot communicate with client version 3";

			internal static readonly byte[] ResponseToHadoop0203Rpc = Bytes.Concat(HexDumpToBytes
				("ff ff ff ff ff ff ff ff  00 00 00 29 6f 72 67 2e ........ ...)org.\n" + "61 70 61 63 68 65 2e 68  61 64 6f 6f 70 2e 69 70 apache.h adoop.ip\n"
				 + "63 2e 52 50 43 24 56 65  72 73 69 6f 6e 4d 69 73 c.RPC$Ve rsionMis\n" + "6d 61 74 63 68                                   match            \n"
				), Ints.ToByteArray(Hadoop020ErrorMsg.Length), Sharpen.Runtime.GetBytesForString
				(Hadoop020ErrorMsg));

			internal const string Hadoop021ErrorMsg = "Server IPC version " + RpcConstants.CurrentVersion
				 + " cannot communicate with client version 4";

			internal static readonly byte[] Hadoop0210RpcDump = HexDumpToBytes("68 72 70 63 04 50                                hrpc.P"
				 + "00 00 00 3c 33 6f 72 67  2e 61 70 61 63 68 65 2e ...<3org .apache.\n" + "68 61 64 6f 6f 70 2e 6d  61 70 72 65 64 75 63 65 hadoop.m apreduce\n"
				 + "2e 70 72 6f 74 6f 63 6f  6c 2e 43 6c 69 65 6e 74 .protoco l.Client\n" + "50 72 6f 74 6f 63 6f 6c  01 00 04 74 6f 64 64 00 Protocol ...todd.\n"
				 + "00 00 00 71 00 00 00 00  00 12 67 65 74 50 72 6f ...q.... ..getPro\n" + "74 6f 63 6f 6c 56 65 72  73 69 6f 6e 00 00 00 02 tocolVer sion....\n"
				 + "00 10 6a 61 76 61 2e 6c  61 6e 67 2e 53 74 72 69 ..java.l ang.Stri\n" + "6e 67 00 33 6f 72 67 2e  61 70 61 63 68 65 2e 68 ng.3org. apache.h\n"
				 + "61 64 6f 6f 70 2e 6d 61  70 72 65 64 75 63 65 2e adoop.ma preduce.\n" + "70 72 6f 74 6f 63 6f 6c  2e 43 6c 69 65 6e 74 50 protocol .ClientP\n"
				 + "72 6f 74 6f 63 6f 6c 00  04 6c 6f 6e 67 00 00 00 rotocol. .long...\n" + "00 00 00 00 21                                   ....!            \n"
				);

			internal static readonly byte[] ResponseToHadoop0210Rpc = Bytes.Concat(HexDumpToBytes
				("ff ff ff ff ff ff ff ff  00 00 00 29 6f 72 67 2e ........ ...)org.\n" + "61 70 61 63 68 65 2e 68  61 64 6f 6f 70 2e 69 70 apache.h adoop.ip\n"
				 + "63 2e 52 50 43 24 56 65  72 73 69 6f 6e 4d 69 73 c.RPC$Ve rsionMis\n" + "6d 61 74 63 68                                   match            \n"
				), Ints.ToByteArray(Hadoop021ErrorMsg.Length), Sharpen.Runtime.GetBytesForString
				(Hadoop021ErrorMsg));
			// in 0.21 it comes in two separate TCP packets
		}
	}
}
