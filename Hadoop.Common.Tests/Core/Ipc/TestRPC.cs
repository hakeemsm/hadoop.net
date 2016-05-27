using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Reflection;
using Com.Google.Protobuf;
using Javax.Net;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.IO.Retry;
using Org.Apache.Hadoop.Metrics2;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Authorize;
using Org.Apache.Hadoop.Security.Token;
using Org.Apache.Hadoop.Test;
using Sharpen;
using Sharpen.Management;
using Sharpen.Reflect;

namespace Org.Apache.Hadoop.Ipc
{
	/// <summary>Unit tests for RPC.</summary>
	public class TestRPC
	{
		private const string Address = "0.0.0.0";

		public static readonly Log Log = LogFactory.GetLog(typeof(TestRPC));

		private static Configuration conf;

		[SetUp]
		public virtual void SetupConf()
		{
			conf = new Configuration();
			conf.SetClass("rpc.engine." + typeof(TestRPC.StoppedProtocol).FullName, typeof(TestRPC.StoppedRpcEngine
				), typeof(RpcEngine));
			UserGroupInformation.SetConfiguration(conf);
		}

		internal int datasize = 1024 * 100;

		internal int numThreads = 50;

		public abstract class TestProtocol : VersionedProtocol
		{
			public const long versionID = 1L;

			/// <exception cref="System.IO.IOException"/>
			public abstract void Ping();

			/// <exception cref="System.IO.IOException"/>
			public abstract void SlowPing(bool shouldSlow);

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			public abstract void Sleep(long delay);

			/// <exception cref="System.IO.IOException"/>
			public abstract string Echo(string value);

			/// <exception cref="System.IO.IOException"/>
			public abstract string[] Echo(string[] value);

			/// <exception cref="System.IO.IOException"/>
			public abstract Writable Echo(Writable value);

			/// <exception cref="System.IO.IOException"/>
			public abstract int Add(int v1, int v2);

			/// <exception cref="System.IO.IOException"/>
			public abstract int Add(int[] values);

			/// <exception cref="System.IO.IOException"/>
			public abstract int Error();

			/// <exception cref="System.IO.IOException"/>
			public abstract void TestServerGet();

			/// <exception cref="System.IO.IOException"/>
			public abstract int[] Exchange(int[] values);

			public abstract DescriptorProtos.EnumDescriptorProto ExchangeProto(DescriptorProtos.EnumDescriptorProto
				 arg);
		}

		public static class TestProtocolConstants
		{
		}

		public class TestImpl : TestRPC.TestProtocol
		{
			internal int fastPingCounter = 0;

			public virtual long GetProtocolVersion(string protocol, long clientVersion)
			{
				return TestRPC.TestProtocol.versionID;
			}

			public virtual ProtocolSignature GetProtocolSignature(string protocol, long clientVersion
				, int hashcode)
			{
				return new ProtocolSignature(TestRPC.TestProtocol.versionID, null);
			}

			public override void Ping()
			{
			}

			public override void SlowPing(bool shouldSlow)
			{
				lock (this)
				{
					if (shouldSlow)
					{
						while (fastPingCounter < 2)
						{
							try
							{
								Sharpen.Runtime.Wait(this);
							}
							catch (Exception)
							{
							}
						}
						// slow response until two fast pings happened
						fastPingCounter -= 2;
					}
					else
					{
						fastPingCounter++;
						Sharpen.Runtime.Notify(this);
					}
				}
			}

			/// <exception cref="System.Exception"/>
			public override void Sleep(long delay)
			{
				Sharpen.Thread.Sleep(delay);
			}

			/// <exception cref="System.IO.IOException"/>
			public override string Echo(string value)
			{
				return value;
			}

			/// <exception cref="System.IO.IOException"/>
			public override string[] Echo(string[] values)
			{
				return values;
			}

			public override Writable Echo(Writable writable)
			{
				return writable;
			}

			public override int Add(int v1, int v2)
			{
				return v1 + v2;
			}

			public override int Add(int[] values)
			{
				int sum = 0;
				for (int i = 0; i < values.Length; i++)
				{
					sum += values[i];
				}
				return sum;
			}

			/// <exception cref="System.IO.IOException"/>
			public override int Error()
			{
				throw new IOException("bobo");
			}

			/// <exception cref="System.IO.IOException"/>
			public override void TestServerGet()
			{
				if (!(Server.Get() is RPC.Server))
				{
					throw new IOException("Server.get() failed");
				}
			}

			public override int[] Exchange(int[] values)
			{
				for (int i = 0; i < values.Length; i++)
				{
					values[i] = i;
				}
				return values;
			}

			public override DescriptorProtos.EnumDescriptorProto ExchangeProto(DescriptorProtos.EnumDescriptorProto
				 arg)
			{
				return arg;
			}
		}

		internal class Transactions : Runnable
		{
			internal int datasize;

			internal TestRPC.TestProtocol proxy;

			internal Transactions(TestRPC.TestProtocol proxy, int datasize)
			{
				//
				// an object that does a bunch of transactions
				//
				this.proxy = proxy;
				this.datasize = datasize;
			}

			// do two RPC that transfers data.
			public virtual void Run()
			{
				int[] indata = new int[datasize];
				int[] outdata = null;
				int val = 0;
				try
				{
					outdata = proxy.Exchange(indata);
					val = proxy.Add(1, 2);
				}
				catch (IOException e)
				{
					NUnit.Framework.Assert.IsTrue("Exception from RPC exchange() " + e, false);
				}
				NUnit.Framework.Assert.AreEqual(indata.Length, outdata.Length);
				NUnit.Framework.Assert.AreEqual(3, val);
				for (int i = 0; i < outdata.Length; i++)
				{
					NUnit.Framework.Assert.AreEqual(outdata[i], i);
				}
			}
		}

		internal class SlowRPC : Runnable
		{
			private TestRPC.TestProtocol proxy;

			private volatile bool done;

			internal SlowRPC(TestRPC.TestProtocol proxy)
			{
				//
				// A class that does an RPC but does not read its response.
				//
				this.proxy = proxy;
				done = false;
			}

			internal virtual bool IsDone()
			{
				return done;
			}

			public virtual void Run()
			{
				try
				{
					proxy.SlowPing(true);
					// this would hang until two fast pings happened
					done = true;
				}
				catch (IOException e)
				{
					NUnit.Framework.Assert.IsTrue("SlowRPC ping exception " + e, false);
				}
			}
		}

		/// <summary>A basic interface for testing client-side RPC resource cleanup.</summary>
		private abstract class StoppedProtocol
		{
			public const long versionID = 0;

			public abstract void Stop();
		}

		private static class StoppedProtocolConstants
		{
		}

		/// <summary>A class used for testing cleanup of client side RPC resources.</summary>
		private class StoppedRpcEngine : RpcEngine
		{
			/// <exception cref="System.IO.IOException"/>
			public virtual ProtocolProxy<T> GetProxy<T>(long clientVersion, IPEndPoint addr, 
				UserGroupInformation ticket, Configuration conf, SocketFactory factory, int rpcTimeout
				, RetryPolicy connectionRetryPolicy)
			{
				System.Type protocol = typeof(T);
				return GetProxy(protocol, clientVersion, addr, ticket, conf, factory, rpcTimeout, 
					connectionRetryPolicy, null);
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual ProtocolProxy<T> GetProxy<T>(long clientVersion, IPEndPoint addr, 
				UserGroupInformation ticket, Configuration conf, SocketFactory factory, int rpcTimeout
				, RetryPolicy connectionRetryPolicy, AtomicBoolean fallbackToSimpleAuth)
			{
				System.Type protocol = typeof(T);
				T proxy = (T)Proxy.NewProxyInstance(protocol.GetClassLoader(), new Type[] { protocol
					 }, new TestRPC.StoppedInvocationHandler());
				return new ProtocolProxy<T>(protocol, proxy, false);
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual RPC.Server GetServer<_T0>(Type protocol, object instance, string bindAddress
				, int port, int numHandlers, int numReaders, int queueSizePerHandler, bool verbose
				, Configuration conf, SecretManager<_T0> secretManager, string portRangeConfig)
				where _T0 : TokenIdentifier
			{
				return null;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual ProtocolProxy<ProtocolMetaInfoPB> GetProtocolMetaInfoProxy(Client.ConnectionId
				 connId, Configuration conf, SocketFactory factory)
			{
				throw new NotSupportedException("This proxy is not supported");
			}
		}

		/// <summary>
		/// An invocation handler which does nothing when invoking methods, and just
		/// counts the number of times close() is called.
		/// </summary>
		private class StoppedInvocationHandler : InvocationHandler, IDisposable
		{
			private int closeCalled = 0;

			/// <exception cref="System.Exception"/>
			public virtual object Invoke(object proxy, MethodInfo method, object[] args)
			{
				return null;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Close()
			{
				closeCalled++;
			}

			public virtual int GetCloseCalled()
			{
				return closeCalled;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestConfRpc()
		{
			Server server = new RPC.Builder(conf).SetProtocol(typeof(TestRPC.TestProtocol)).SetInstance
				(new TestRPC.TestImpl()).SetBindAddress(Address).SetPort(0).SetNumHandlers(1).SetVerbose
				(false).Build();
			// Just one handler
			int confQ = conf.GetInt(CommonConfigurationKeys.IpcServerHandlerQueueSizeKey, CommonConfigurationKeys
				.IpcServerHandlerQueueSizeDefault);
			NUnit.Framework.Assert.AreEqual(confQ, server.GetMaxQueueSize());
			int confReaders = conf.GetInt(CommonConfigurationKeys.IpcServerRpcReadThreadsKey, 
				CommonConfigurationKeys.IpcServerRpcReadThreadsDefault);
			NUnit.Framework.Assert.AreEqual(confReaders, server.GetNumReaders());
			server.Stop();
			server = new RPC.Builder(conf).SetProtocol(typeof(TestRPC.TestProtocol)).SetInstance
				(new TestRPC.TestImpl()).SetBindAddress(Address).SetPort(0).SetNumHandlers(1).SetnumReaders
				(3).SetQueueSizePerHandler(200).SetVerbose(false).Build();
			NUnit.Framework.Assert.AreEqual(3, server.GetNumReaders());
			NUnit.Framework.Assert.AreEqual(200, server.GetMaxQueueSize());
			server.Stop();
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestProxyAddress()
		{
			Server server = new RPC.Builder(conf).SetProtocol(typeof(TestRPC.TestProtocol)).SetInstance
				(new TestRPC.TestImpl()).SetBindAddress(Address).SetPort(0).Build();
			TestRPC.TestProtocol proxy = null;
			try
			{
				server.Start();
				IPEndPoint addr = NetUtils.GetConnectAddress(server);
				// create a client
				proxy = RPC.GetProxy<TestRPC.TestProtocol>(TestRPC.TestProtocol.versionID, addr, 
					conf);
				NUnit.Framework.Assert.AreEqual(addr, RPC.GetServerAddress(proxy));
			}
			finally
			{
				server.Stop();
				if (proxy != null)
				{
					RPC.StopProxy(proxy);
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestSlowRpc()
		{
			System.Console.Out.WriteLine("Testing Slow RPC");
			// create a server with two handlers
			Server server = new RPC.Builder(conf).SetProtocol(typeof(TestRPC.TestProtocol)).SetInstance
				(new TestRPC.TestImpl()).SetBindAddress(Address).SetPort(0).SetNumHandlers(2).SetVerbose
				(false).Build();
			TestRPC.TestProtocol proxy = null;
			try
			{
				server.Start();
				IPEndPoint addr = NetUtils.GetConnectAddress(server);
				// create a client
				proxy = RPC.GetProxy<TestRPC.TestProtocol>(TestRPC.TestProtocol.versionID, addr, 
					conf);
				TestRPC.SlowRPC slowrpc = new TestRPC.SlowRPC(proxy);
				Sharpen.Thread thread = new Sharpen.Thread(slowrpc, "SlowRPC");
				thread.Start();
				// send a slow RPC, which won't return until two fast pings
				NUnit.Framework.Assert.IsTrue("Slow RPC should not have finished1.", !slowrpc.IsDone
					());
				proxy.SlowPing(false);
				// first fast ping
				// verify that the first RPC is still stuck
				NUnit.Framework.Assert.IsTrue("Slow RPC should not have finished2.", !slowrpc.IsDone
					());
				proxy.SlowPing(false);
				// second fast ping
				// Now the slow ping should be able to be executed
				while (!slowrpc.IsDone())
				{
					System.Console.Out.WriteLine("Waiting for slow RPC to get done.");
					try
					{
						Sharpen.Thread.Sleep(1000);
					}
					catch (Exception)
					{
					}
				}
			}
			finally
			{
				server.Stop();
				if (proxy != null)
				{
					RPC.StopProxy(proxy);
				}
				System.Console.Out.WriteLine("Down slow rpc testing");
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestCalls()
		{
			TestCallsInternal(conf);
		}

		/// <exception cref="System.IO.IOException"/>
		private void TestCallsInternal(Configuration conf)
		{
			Server server = new RPC.Builder(conf).SetProtocol(typeof(TestRPC.TestProtocol)).SetInstance
				(new TestRPC.TestImpl()).SetBindAddress(Address).SetPort(0).Build();
			TestRPC.TestProtocol proxy = null;
			try
			{
				server.Start();
				IPEndPoint addr = NetUtils.GetConnectAddress(server);
				proxy = RPC.GetProxy<TestRPC.TestProtocol>(TestRPC.TestProtocol.versionID, addr, 
					conf);
				proxy.Ping();
				string stringResult = proxy.Echo("foo");
				NUnit.Framework.Assert.AreEqual(stringResult, "foo");
				stringResult = proxy.Echo((string)null);
				NUnit.Framework.Assert.AreEqual(stringResult, null);
				// Check rpcMetrics 
				MetricsRecordBuilder rb = MetricsAsserts.GetMetrics(server.rpcMetrics.Name());
				MetricsAsserts.AssertCounter("RpcProcessingTimeNumOps", 3L, rb);
				MetricsAsserts.AssertCounterGt("SentBytes", 0L, rb);
				MetricsAsserts.AssertCounterGt("ReceivedBytes", 0L, rb);
				// Number of calls to echo method should be 2
				rb = MetricsAsserts.GetMetrics(server.rpcDetailedMetrics.Name());
				MetricsAsserts.AssertCounter("EchoNumOps", 2L, rb);
				// Number of calls to ping method should be 1
				MetricsAsserts.AssertCounter("PingNumOps", 1L, rb);
				string[] stringResults = proxy.Echo(new string[] { "foo", "bar" });
				NUnit.Framework.Assert.IsTrue(Arrays.Equals(stringResults, new string[] { "foo", 
					"bar" }));
				stringResults = proxy.Echo((string[])null);
				NUnit.Framework.Assert.IsTrue(Arrays.Equals(stringResults, null));
				UTF8 utf8Result = (UTF8)proxy.Echo(new UTF8("hello world"));
				NUnit.Framework.Assert.AreEqual(new UTF8("hello world"), utf8Result);
				utf8Result = (UTF8)proxy.Echo((UTF8)null);
				NUnit.Framework.Assert.AreEqual(null, utf8Result);
				int intResult = proxy.Add(1, 2);
				NUnit.Framework.Assert.AreEqual(intResult, 3);
				intResult = proxy.Add(new int[] { 1, 2 });
				NUnit.Framework.Assert.AreEqual(intResult, 3);
				// Test protobufs
				DescriptorProtos.EnumDescriptorProto sendProto = ((DescriptorProtos.EnumDescriptorProto
					)DescriptorProtos.EnumDescriptorProto.NewBuilder().SetName("test").Build());
				DescriptorProtos.EnumDescriptorProto retProto = proxy.ExchangeProto(sendProto);
				NUnit.Framework.Assert.AreEqual(sendProto, retProto);
				NUnit.Framework.Assert.AreNotSame(sendProto, retProto);
				bool caught = false;
				try
				{
					proxy.Error();
				}
				catch (IOException e)
				{
					if (Log.IsDebugEnabled())
					{
						Log.Debug("Caught " + e);
					}
					caught = true;
				}
				NUnit.Framework.Assert.IsTrue(caught);
				rb = MetricsAsserts.GetMetrics(server.rpcDetailedMetrics.Name());
				MetricsAsserts.AssertCounter("IOExceptionNumOps", 1L, rb);
				proxy.TestServerGet();
				// create multiple threads and make them do large data transfers
				System.Console.Out.WriteLine("Starting multi-threaded RPC test...");
				server.SetSocketSendBufSize(1024);
				Sharpen.Thread[] threadId = new Sharpen.Thread[numThreads];
				for (int i = 0; i < numThreads; i++)
				{
					TestRPC.Transactions trans = new TestRPC.Transactions(proxy, datasize);
					threadId[i] = new Sharpen.Thread(trans, "TransactionThread-" + i);
					threadId[i].Start();
				}
				// wait for all transactions to get over
				System.Console.Out.WriteLine("Waiting for all threads to finish RPCs...");
				for (int i_1 = 0; i_1 < numThreads; i_1++)
				{
					try
					{
						threadId[i_1].Join();
					}
					catch (Exception)
					{
						i_1--;
					}
				}
			}
			finally
			{
				// retry
				server.Stop();
				if (proxy != null)
				{
					RPC.StopProxy(proxy);
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestStandaloneClient()
		{
			try
			{
				TestRPC.TestProtocol proxy = RPC.WaitForProxy<TestRPC.TestProtocol>(TestRPC.TestProtocol
					.versionID, new IPEndPoint(Address, 20), conf, 15000L);
				proxy.Echo(string.Empty);
				NUnit.Framework.Assert.Fail("We should not have reached here");
			}
			catch (ConnectException)
			{
			}
		}

		private const string AclConfig = "test.protocol.acl";

		private class TestPolicyProvider : PolicyProvider
		{
			//this is what we expected
			public override Service[] GetServices()
			{
				return new Service[] { new Service(AclConfig, typeof(TestRPC.TestProtocol)) };
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void DoRPCs(Configuration conf, bool expectFailure)
		{
			Server server = new RPC.Builder(conf).SetProtocol(typeof(TestRPC.TestProtocol)).SetInstance
				(new TestRPC.TestImpl()).SetBindAddress(Address).SetPort(0).SetNumHandlers(5).SetVerbose
				(true).Build();
			server.RefreshServiceAcl(conf, new TestRPC.TestPolicyProvider());
			TestRPC.TestProtocol proxy = null;
			server.Start();
			IPEndPoint addr = NetUtils.GetConnectAddress(server);
			try
			{
				proxy = RPC.GetProxy<TestRPC.TestProtocol>(TestRPC.TestProtocol.versionID, addr, 
					conf);
				proxy.Ping();
				if (expectFailure)
				{
					NUnit.Framework.Assert.Fail("Expect RPC.getProxy to fail with AuthorizationException!"
						);
				}
			}
			catch (RemoteException e)
			{
				if (expectFailure)
				{
					NUnit.Framework.Assert.IsTrue(e.UnwrapRemoteException() is AuthorizationException
						);
				}
				else
				{
					throw;
				}
			}
			finally
			{
				server.Stop();
				if (proxy != null)
				{
					RPC.StopProxy(proxy);
				}
				MetricsRecordBuilder rb = MetricsAsserts.GetMetrics(server.rpcMetrics.Name());
				if (expectFailure)
				{
					MetricsAsserts.AssertCounter("RpcAuthorizationFailures", 1L, rb);
				}
				else
				{
					MetricsAsserts.AssertCounter("RpcAuthorizationSuccesses", 1L, rb);
				}
				//since we don't have authentication turned ON, we should see 
				// 0 for the authentication successes and 0 for failure
				MetricsAsserts.AssertCounter("RpcAuthenticationFailures", 0L, rb);
				MetricsAsserts.AssertCounter("RpcAuthenticationSuccesses", 0L, rb);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestServerAddress()
		{
			Server server = new RPC.Builder(conf).SetProtocol(typeof(TestRPC.TestProtocol)).SetInstance
				(new TestRPC.TestImpl()).SetBindAddress(Address).SetPort(0).SetNumHandlers(5).SetVerbose
				(true).Build();
			IPEndPoint bindAddr = null;
			try
			{
				bindAddr = NetUtils.GetConnectAddress(server);
			}
			finally
			{
				server.Stop();
			}
			NUnit.Framework.Assert.AreEqual(Sharpen.Runtime.GetLocalHost(), bindAddr.Address);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestAuthorization()
		{
			Configuration conf = new Configuration();
			conf.SetBoolean(CommonConfigurationKeys.HadoopSecurityAuthorization, true);
			// Expect to succeed
			conf.Set(AclConfig, "*");
			DoRPCs(conf, false);
			// Reset authorization to expect failure
			conf.Set(AclConfig, "invalid invalid");
			DoRPCs(conf, true);
			conf.SetInt(CommonConfigurationKeys.IpcServerRpcReadThreadsKey, 2);
			// Expect to succeed
			conf.Set(AclConfig, "*");
			DoRPCs(conf, false);
			// Reset authorization to expect failure
			conf.Set(AclConfig, "invalid invalid");
			DoRPCs(conf, true);
		}

		/// <summary>Switch off setting socketTimeout values on RPC sockets.</summary>
		/// <remarks>
		/// Switch off setting socketTimeout values on RPC sockets.
		/// Verify that RPC calls still work ok.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestNoPings()
		{
			Configuration conf = new Configuration();
			conf.SetBoolean("ipc.client.ping", false);
			new TestRPC().TestCallsInternal(conf);
			conf.SetInt(CommonConfigurationKeys.IpcServerRpcReadThreadsKey, 2);
			new TestRPC().TestCallsInternal(conf);
		}

		/// <summary>Test stopping a non-registered proxy</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestStopNonRegisteredProxy()
		{
			RPC.StopProxy(null);
		}

		/// <summary>
		/// Test that the mockProtocol helper returns mock proxies that can
		/// be stopped without error.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestStopMockObject()
		{
			RPC.StopProxy(MockitoUtil.MockProtocol<TestRPC.TestProtocol>());
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestStopProxy()
		{
			TestRPC.StoppedProtocol proxy = RPC.GetProxy<TestRPC.StoppedProtocol>(TestRPC.StoppedProtocol
				.versionID, null, conf);
			TestRPC.StoppedInvocationHandler invocationHandler = (TestRPC.StoppedInvocationHandler
				)Proxy.GetInvocationHandler(proxy);
			NUnit.Framework.Assert.AreEqual(0, invocationHandler.GetCloseCalled());
			RPC.StopProxy(proxy);
			NUnit.Framework.Assert.AreEqual(1, invocationHandler.GetCloseCalled());
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestWrappedStopProxy()
		{
			TestRPC.StoppedProtocol wrappedProxy = RPC.GetProxy<TestRPC.StoppedProtocol>(TestRPC.StoppedProtocol
				.versionID, null, conf);
			TestRPC.StoppedInvocationHandler invocationHandler = (TestRPC.StoppedInvocationHandler
				)Proxy.GetInvocationHandler(wrappedProxy);
			TestRPC.StoppedProtocol proxy = (TestRPC.StoppedProtocol)RetryProxy.Create<TestRPC.StoppedProtocol
				>(wrappedProxy, RetryPolicies.RetryForever);
			NUnit.Framework.Assert.AreEqual(0, invocationHandler.GetCloseCalled());
			RPC.StopProxy(proxy);
			NUnit.Framework.Assert.AreEqual(1, invocationHandler.GetCloseCalled());
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestErrorMsgForInsecureClient()
		{
			Configuration serverConf = new Configuration(conf);
			SecurityUtil.SetAuthenticationMethod(UserGroupInformation.AuthenticationMethod.Kerberos
				, serverConf);
			UserGroupInformation.SetConfiguration(serverConf);
			Server server = new RPC.Builder(serverConf).SetProtocol(typeof(TestRPC.TestProtocol
				)).SetInstance(new TestRPC.TestImpl()).SetBindAddress(Address).SetPort(0).SetNumHandlers
				(5).SetVerbose(true).Build();
			server.Start();
			UserGroupInformation.SetConfiguration(conf);
			bool succeeded = false;
			IPEndPoint addr = NetUtils.GetConnectAddress(server);
			TestRPC.TestProtocol proxy = null;
			try
			{
				proxy = RPC.GetProxy<TestRPC.TestProtocol>(TestRPC.TestProtocol.versionID, addr, 
					conf);
				proxy.Echo(string.Empty);
			}
			catch (RemoteException e)
			{
				Log.Info("LOGGING MESSAGE: " + e.GetLocalizedMessage());
				NUnit.Framework.Assert.IsTrue(e.UnwrapRemoteException() is AccessControlException
					);
				succeeded = true;
			}
			finally
			{
				server.Stop();
				if (proxy != null)
				{
					RPC.StopProxy(proxy);
				}
			}
			NUnit.Framework.Assert.IsTrue(succeeded);
			conf.SetInt(CommonConfigurationKeys.IpcServerRpcReadThreadsKey, 2);
			UserGroupInformation.SetConfiguration(serverConf);
			Server multiServer = new RPC.Builder(serverConf).SetProtocol(typeof(TestRPC.TestProtocol
				)).SetInstance(new TestRPC.TestImpl()).SetBindAddress(Address).SetPort(0).SetNumHandlers
				(5).SetVerbose(true).Build();
			multiServer.Start();
			succeeded = false;
			IPEndPoint mulitServerAddr = NetUtils.GetConnectAddress(multiServer);
			proxy = null;
			try
			{
				UserGroupInformation.SetConfiguration(conf);
				proxy = RPC.GetProxy<TestRPC.TestProtocol>(TestRPC.TestProtocol.versionID, mulitServerAddr
					, conf);
				proxy.Echo(string.Empty);
			}
			catch (RemoteException e)
			{
				Log.Info("LOGGING MESSAGE: " + e.GetLocalizedMessage());
				NUnit.Framework.Assert.IsTrue(e.UnwrapRemoteException() is AccessControlException
					);
				succeeded = true;
			}
			finally
			{
				multiServer.Stop();
				if (proxy != null)
				{
					RPC.StopProxy(proxy);
				}
			}
			NUnit.Framework.Assert.IsTrue(succeeded);
		}

		/// <summary>
		/// Count the number of threads that have a stack frame containing
		/// the given string
		/// </summary>
		private static int CountThreads(string search)
		{
			ThreadMXBean threadBean = ManagementFactory.GetThreadMXBean();
			int count = 0;
			ThreadInfo[] infos = threadBean.GetThreadInfo(threadBean.GetAllThreadIds(), 20);
			foreach (ThreadInfo info in infos)
			{
				if (info == null)
				{
					continue;
				}
				foreach (StackTraceElement elem in info.GetStackTrace())
				{
					if (elem.GetClassName().Contains(search))
					{
						count++;
						break;
					}
				}
			}
			return count;
		}

		/// <summary>Test that server.stop() properly stops all threads</summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestStopsAllThreads()
		{
			int threadsBefore = CountThreads("Server$Listener$Reader");
			NUnit.Framework.Assert.AreEqual("Expect no Reader threads running before test", 0
				, threadsBefore);
			Server server = new RPC.Builder(conf).SetProtocol(typeof(TestRPC.TestProtocol)).SetInstance
				(new TestRPC.TestImpl()).SetBindAddress(Address).SetPort(0).SetNumHandlers(5).SetVerbose
				(true).Build();
			server.Start();
			try
			{
				// Wait for at least one reader thread to start
				int threadsRunning = 0;
				long totalSleepTime = 0;
				do
				{
					totalSleepTime += 10;
					Sharpen.Thread.Sleep(10);
					threadsRunning = CountThreads("Server$Listener$Reader");
				}
				while (threadsRunning == 0 && totalSleepTime < 5000);
				// Validate that at least one thread started (we didn't timeout)
				threadsRunning = CountThreads("Server$Listener$Reader");
				NUnit.Framework.Assert.IsTrue(threadsRunning > 0);
			}
			finally
			{
				server.Stop();
			}
			int threadsAfter = CountThreads("Server$Listener$Reader");
			NUnit.Framework.Assert.AreEqual("Expect no Reader threads left running after test"
				, 0, threadsAfter);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestRPCBuilder()
		{
			// Test mandatory field conf
			try
			{
				new RPC.Builder(null).SetProtocol(typeof(TestRPC.TestProtocol)).SetInstance(new TestRPC.TestImpl
					()).SetBindAddress(Address).SetPort(0).SetNumHandlers(5).SetVerbose(true).Build(
					);
				NUnit.Framework.Assert.Fail("Didn't throw HadoopIllegalArgumentException");
			}
			catch (Exception e)
			{
				if (!(e is HadoopIllegalArgumentException))
				{
					NUnit.Framework.Assert.Fail("Expecting HadoopIllegalArgumentException but caught "
						 + e);
				}
			}
			// Test mandatory field protocol
			try
			{
				new RPC.Builder(conf).SetInstance(new TestRPC.TestImpl()).SetBindAddress(Address)
					.SetPort(0).SetNumHandlers(5).SetVerbose(true).Build();
				NUnit.Framework.Assert.Fail("Didn't throw HadoopIllegalArgumentException");
			}
			catch (Exception e)
			{
				if (!(e is HadoopIllegalArgumentException))
				{
					NUnit.Framework.Assert.Fail("Expecting HadoopIllegalArgumentException but caught "
						 + e);
				}
			}
			// Test mandatory field instance
			try
			{
				new RPC.Builder(conf).SetProtocol(typeof(TestRPC.TestProtocol)).SetBindAddress(Address
					).SetPort(0).SetNumHandlers(5).SetVerbose(true).Build();
				NUnit.Framework.Assert.Fail("Didn't throw HadoopIllegalArgumentException");
			}
			catch (Exception e)
			{
				if (!(e is HadoopIllegalArgumentException))
				{
					NUnit.Framework.Assert.Fail("Expecting HadoopIllegalArgumentException but caught "
						 + e);
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestRPCInterruptedSimple()
		{
			Configuration conf = new Configuration();
			Server server = new RPC.Builder(conf).SetProtocol(typeof(TestRPC.TestProtocol)).SetInstance
				(new TestRPC.TestImpl()).SetBindAddress(Address).SetPort(0).SetNumHandlers(5).SetVerbose
				(true).SetSecretManager(null).Build();
			server.Start();
			IPEndPoint addr = NetUtils.GetConnectAddress(server);
			TestRPC.TestProtocol proxy = RPC.GetProxy<TestRPC.TestProtocol>(TestRPC.TestProtocol
				.versionID, addr, conf);
			// Connect to the server
			proxy.Ping();
			// Interrupt self, try another call
			Sharpen.Thread.CurrentThread().Interrupt();
			try
			{
				proxy.Ping();
				NUnit.Framework.Assert.Fail("Interruption did not cause IPC to fail");
			}
			catch (IOException ioe)
			{
				if (!ioe.ToString().Contains("InterruptedException"))
				{
					throw;
				}
				// clear interrupt status for future tests
				Sharpen.Thread.Interrupted();
			}
			finally
			{
				server.Stop();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual void TestRPCInterrupted()
		{
			Configuration conf = new Configuration();
			Server server = new RPC.Builder(conf).SetProtocol(typeof(TestRPC.TestProtocol)).SetInstance
				(new TestRPC.TestImpl()).SetBindAddress(Address).SetPort(0).SetNumHandlers(5).SetVerbose
				(true).SetSecretManager(null).Build();
			server.Start();
			int numConcurrentRPC = 200;
			IPEndPoint addr = NetUtils.GetConnectAddress(server);
			CyclicBarrier barrier = new CyclicBarrier(numConcurrentRPC);
			CountDownLatch latch = new CountDownLatch(numConcurrentRPC);
			AtomicBoolean leaderRunning = new AtomicBoolean(true);
			AtomicReference<Exception> error = new AtomicReference<Exception>();
			Sharpen.Thread leaderThread = null;
			for (int i = 0; i < numConcurrentRPC; i++)
			{
				int num = i;
				TestRPC.TestProtocol proxy = RPC.GetProxy<TestRPC.TestProtocol>(TestRPC.TestProtocol
					.versionID, addr, conf);
				Sharpen.Thread rpcThread = new Sharpen.Thread(new _Runnable_915(barrier, num, leaderRunning
					, proxy, error, latch));
				rpcThread.Start();
				if (leaderThread == null)
				{
					leaderThread = rpcThread;
				}
			}
			// let threads get past the barrier
			Sharpen.Thread.Sleep(1000);
			// stop a single thread
			while (leaderRunning.Get())
			{
				leaderThread.Interrupt();
			}
			latch.Await();
			// should not cause any other thread to get an error
			NUnit.Framework.Assert.IsTrue("rpc got exception " + error.Get(), error.Get() == 
				null);
			server.Stop();
		}

		private sealed class _Runnable_915 : Runnable
		{
			public _Runnable_915(CyclicBarrier barrier, int num, AtomicBoolean leaderRunning, 
				TestRPC.TestProtocol proxy, AtomicReference<Exception> error, CountDownLatch latch
				)
			{
				this.barrier = barrier;
				this.num = num;
				this.leaderRunning = leaderRunning;
				this.proxy = proxy;
				this.error = error;
				this.latch = latch;
			}

			public void Run()
			{
				try
				{
					barrier.Await();
					while (num == 0 || leaderRunning.Get())
					{
						proxy.SlowPing(false);
					}
					proxy.SlowPing(false);
				}
				catch (Exception e)
				{
					if (num == 0)
					{
						leaderRunning.Set(false);
					}
					else
					{
						error.Set(e);
					}
					TestRPC.Log.Error(e);
				}
				finally
				{
					latch.CountDown();
				}
			}

			private readonly CyclicBarrier barrier;

			private readonly int num;

			private readonly AtomicBoolean leaderRunning;

			private readonly TestRPC.TestProtocol proxy;

			private readonly AtomicReference<Exception> error;

			private readonly CountDownLatch latch;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestConnectionPing()
		{
			Configuration conf = new Configuration();
			int pingInterval = 50;
			conf.SetBoolean(CommonConfigurationKeys.IpcClientPingKey, true);
			conf.SetInt(CommonConfigurationKeys.IpcPingIntervalKey, pingInterval);
			Server server = new RPC.Builder(conf).SetProtocol(typeof(TestRPC.TestProtocol)).SetInstance
				(new TestRPC.TestImpl()).SetBindAddress(Address).SetPort(0).SetNumHandlers(5).SetVerbose
				(true).Build();
			server.Start();
			TestRPC.TestProtocol proxy = RPC.GetProxy<TestRPC.TestProtocol>(TestRPC.TestProtocol
				.versionID, server.GetListenerAddress(), conf);
			try
			{
				// this call will throw exception if server couldn't decode the ping
				proxy.Sleep(pingInterval * 4);
			}
			finally
			{
				if (proxy != null)
				{
					RPC.StopProxy(proxy);
				}
				server.Stop();
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRpcMetrics()
		{
			Configuration configuration = new Configuration();
			int interval = 1;
			configuration.SetBoolean(CommonConfigurationKeys.RpcMetricsQuantileEnable, true);
			configuration.Set(CommonConfigurationKeys.RpcMetricsPercentilesIntervalsKey, string.Empty
				 + interval);
			Server server = new RPC.Builder(configuration).SetProtocol(typeof(TestRPC.TestProtocol
				)).SetInstance(new TestRPC.TestImpl()).SetBindAddress(Address).SetPort(0).SetNumHandlers
				(5).SetVerbose(true).Build();
			server.Start();
			TestRPC.TestProtocol proxy = RPC.GetProxy<TestRPC.TestProtocol>(TestRPC.TestProtocol
				.versionID, server.GetListenerAddress(), configuration);
			try
			{
				for (int i = 0; i < 1000; i++)
				{
					proxy.Ping();
					proxy.Echo(string.Empty + i);
				}
				MetricsRecordBuilder rpcMetrics = MetricsAsserts.GetMetrics(server.GetRpcMetrics(
					).Name());
				NUnit.Framework.Assert.IsTrue("Expected non-zero rpc queue time", MetricsAsserts.GetLongCounter
					("RpcQueueTimeNumOps", rpcMetrics) > 0);
				NUnit.Framework.Assert.IsTrue("Expected non-zero rpc processing time", MetricsAsserts.GetLongCounter
					("RpcProcessingTimeNumOps", rpcMetrics) > 0);
				MetricsAsserts.AssertQuantileGauges("RpcQueueTime" + interval + "s", rpcMetrics);
				MetricsAsserts.AssertQuantileGauges("RpcProcessingTime" + interval + "s", rpcMetrics
					);
			}
			finally
			{
				if (proxy != null)
				{
					RPC.StopProxy(proxy);
				}
				server.Stop();
			}
		}

		/// <summary>Verify the RPC server can shutdown properly when callQueue is full.</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestRPCServerShutdown()
		{
			int numClients = 3;
			IList<Future<Void>> res = new AList<Future<Void>>();
			ExecutorService executorService = Executors.NewFixedThreadPool(numClients);
			Configuration conf = new Configuration();
			conf.SetInt(CommonConfigurationKeys.IpcClientConnectMaxRetriesKey, 0);
			Server server = new RPC.Builder(conf).SetProtocol(typeof(TestRPC.TestProtocol)).SetInstance
				(new TestRPC.TestImpl()).SetBindAddress(Address).SetPort(0).SetQueueSizePerHandler
				(1).SetNumHandlers(1).SetVerbose(true).Build();
			server.Start();
			TestRPC.TestProtocol proxy = RPC.GetProxy<TestRPC.TestProtocol>(TestRPC.TestProtocol
				.versionID, NetUtils.GetConnectAddress(server), conf);
			try
			{
				// start a sleep RPC call to consume the only handler thread.
				// Start another sleep RPC call to make callQueue full.
				// Start another sleep RPC call to make reader thread block on CallQueue.
				for (int i = 0; i < numClients; i++)
				{
					res.AddItem(executorService.Submit(new _Callable_1046(proxy)));
				}
				while (server.GetCallQueueLen() != 1 && CountThreads(typeof(CallQueueManager).FullName
					) != 1 && CountThreads(typeof(TestRPC.TestProtocol).FullName) != 1)
				{
					Sharpen.Thread.Sleep(100);
				}
			}
			finally
			{
				try
				{
					server.Stop();
					NUnit.Framework.Assert.AreEqual("Not enough clients", numClients, res.Count);
					foreach (Future<Void> f in res)
					{
						try
						{
							f.Get();
							NUnit.Framework.Assert.Fail("Future get should not return");
						}
						catch (ExecutionException e)
						{
							NUnit.Framework.Assert.IsTrue("Unexpected exception: " + e, e.InnerException is IOException
								);
							Log.Info("Expected exception", e.InnerException);
						}
					}
				}
				finally
				{
					RPC.StopProxy(proxy);
					executorService.Shutdown();
				}
			}
		}

		private sealed class _Callable_1046 : Callable<Void>
		{
			public _Callable_1046(TestRPC.TestProtocol proxy)
			{
				this.proxy = proxy;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			public Void Call()
			{
				proxy.Sleep(100000);
				return null;
			}

			private readonly TestRPC.TestProtocol proxy;
		}

		/// <exception cref="System.IO.IOException"/>
		public static void Main(string[] args)
		{
			new TestRPC().TestCallsInternal(conf);
		}
	}
}
