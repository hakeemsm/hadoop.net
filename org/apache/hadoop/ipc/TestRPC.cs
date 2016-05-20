using Sharpen;

namespace org.apache.hadoop.ipc
{
	/// <summary>Unit tests for RPC.</summary>
	public class TestRPC
	{
		private const string ADDRESS = "0.0.0.0";

		public static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.ipc.TestRPC)));

		private static org.apache.hadoop.conf.Configuration conf;

		[NUnit.Framework.SetUp]
		public virtual void setupConf()
		{
			conf = new org.apache.hadoop.conf.Configuration();
			conf.setClass("rpc.engine." + Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.ipc.TestRPC.StoppedProtocol
				)).getName(), Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.ipc.TestRPC.StoppedRpcEngine
				)), Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.ipc.RpcEngine)));
			org.apache.hadoop.security.UserGroupInformation.setConfiguration(conf);
		}

		internal int datasize = 1024 * 100;

		internal int numThreads = 50;

		public abstract class TestProtocol : org.apache.hadoop.ipc.VersionedProtocol
		{
			public const long versionID = 1L;

			/// <exception cref="System.IO.IOException"/>
			public abstract void ping();

			/// <exception cref="System.IO.IOException"/>
			public abstract void slowPing(bool shouldSlow);

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			public abstract void sleep(long delay);

			/// <exception cref="System.IO.IOException"/>
			public abstract string echo(string value);

			/// <exception cref="System.IO.IOException"/>
			public abstract string[] echo(string[] value);

			/// <exception cref="System.IO.IOException"/>
			public abstract org.apache.hadoop.io.Writable echo(org.apache.hadoop.io.Writable 
				value);

			/// <exception cref="System.IO.IOException"/>
			public abstract int add(int v1, int v2);

			/// <exception cref="System.IO.IOException"/>
			public abstract int add(int[] values);

			/// <exception cref="System.IO.IOException"/>
			public abstract int error();

			/// <exception cref="System.IO.IOException"/>
			public abstract void testServerGet();

			/// <exception cref="System.IO.IOException"/>
			public abstract int[] exchange(int[] values);

			public abstract com.google.protobuf.DescriptorProtos.EnumDescriptorProto exchangeProto
				(com.google.protobuf.DescriptorProtos.EnumDescriptorProto arg);
		}

		public static class TestProtocolConstants
		{
		}

		public class TestImpl : org.apache.hadoop.ipc.TestRPC.TestProtocol
		{
			internal int fastPingCounter = 0;

			public virtual long getProtocolVersion(string protocol, long clientVersion)
			{
				return org.apache.hadoop.ipc.TestRPC.TestProtocol.versionID;
			}

			public virtual org.apache.hadoop.ipc.ProtocolSignature getProtocolSignature(string
				 protocol, long clientVersion, int hashcode)
			{
				return new org.apache.hadoop.ipc.ProtocolSignature(org.apache.hadoop.ipc.TestRPC.TestProtocol
					.versionID, null);
			}

			public override void ping()
			{
			}

			public override void slowPing(bool shouldSlow)
			{
				lock (this)
				{
					if (shouldSlow)
					{
						while (fastPingCounter < 2)
						{
							try
							{
								Sharpen.Runtime.wait(this);
							}
							catch (System.Exception)
							{
							}
						}
						// slow response until two fast pings happened
						fastPingCounter -= 2;
					}
					else
					{
						fastPingCounter++;
						Sharpen.Runtime.notify(this);
					}
				}
			}

			/// <exception cref="System.Exception"/>
			public override void sleep(long delay)
			{
				java.lang.Thread.sleep(delay);
			}

			/// <exception cref="System.IO.IOException"/>
			public override string echo(string value)
			{
				return value;
			}

			/// <exception cref="System.IO.IOException"/>
			public override string[] echo(string[] values)
			{
				return values;
			}

			public override org.apache.hadoop.io.Writable echo(org.apache.hadoop.io.Writable 
				writable)
			{
				return writable;
			}

			public override int add(int v1, int v2)
			{
				return v1 + v2;
			}

			public override int add(int[] values)
			{
				int sum = 0;
				for (int i = 0; i < values.Length; i++)
				{
					sum += values[i];
				}
				return sum;
			}

			/// <exception cref="System.IO.IOException"/>
			public override int error()
			{
				throw new System.IO.IOException("bobo");
			}

			/// <exception cref="System.IO.IOException"/>
			public override void testServerGet()
			{
				if (!(org.apache.hadoop.ipc.Server.get() is org.apache.hadoop.ipc.RPC.Server))
				{
					throw new System.IO.IOException("Server.get() failed");
				}
			}

			public override int[] exchange(int[] values)
			{
				for (int i = 0; i < values.Length; i++)
				{
					values[i] = i;
				}
				return values;
			}

			public override com.google.protobuf.DescriptorProtos.EnumDescriptorProto exchangeProto
				(com.google.protobuf.DescriptorProtos.EnumDescriptorProto arg)
			{
				return arg;
			}
		}

		internal class Transactions : java.lang.Runnable
		{
			internal int datasize;

			internal org.apache.hadoop.ipc.TestRPC.TestProtocol proxy;

			internal Transactions(org.apache.hadoop.ipc.TestRPC.TestProtocol proxy, int datasize
				)
			{
				//
				// an object that does a bunch of transactions
				//
				this.proxy = proxy;
				this.datasize = datasize;
			}

			// do two RPC that transfers data.
			public virtual void run()
			{
				int[] indata = new int[datasize];
				int[] outdata = null;
				int val = 0;
				try
				{
					outdata = proxy.exchange(indata);
					val = proxy.add(1, 2);
				}
				catch (System.IO.IOException e)
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

		internal class SlowRPC : java.lang.Runnable
		{
			private org.apache.hadoop.ipc.TestRPC.TestProtocol proxy;

			private volatile bool done;

			internal SlowRPC(org.apache.hadoop.ipc.TestRPC.TestProtocol proxy)
			{
				//
				// A class that does an RPC but does not read its response.
				//
				this.proxy = proxy;
				done = false;
			}

			internal virtual bool isDone()
			{
				return done;
			}

			public virtual void run()
			{
				try
				{
					proxy.slowPing(true);
					// this would hang until two fast pings happened
					done = true;
				}
				catch (System.IO.IOException e)
				{
					NUnit.Framework.Assert.IsTrue("SlowRPC ping exception " + e, false);
				}
			}
		}

		/// <summary>A basic interface for testing client-side RPC resource cleanup.</summary>
		private abstract class StoppedProtocol
		{
			public const long versionID = 0;

			public abstract void stop();
		}

		private static class StoppedProtocolConstants
		{
		}

		/// <summary>A class used for testing cleanup of client side RPC resources.</summary>
		private class StoppedRpcEngine : org.apache.hadoop.ipc.RpcEngine
		{
			/// <exception cref="System.IO.IOException"/>
			public virtual org.apache.hadoop.ipc.ProtocolProxy<T> getProxy<T>(long clientVersion
				, java.net.InetSocketAddress addr, org.apache.hadoop.security.UserGroupInformation
				 ticket, org.apache.hadoop.conf.Configuration conf, javax.net.SocketFactory factory
				, int rpcTimeout, org.apache.hadoop.io.retry.RetryPolicy connectionRetryPolicy)
			{
				System.Type protocol = typeof(T);
				return getProxy(protocol, clientVersion, addr, ticket, conf, factory, rpcTimeout, 
					connectionRetryPolicy, null);
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual org.apache.hadoop.ipc.ProtocolProxy<T> getProxy<T>(long clientVersion
				, java.net.InetSocketAddress addr, org.apache.hadoop.security.UserGroupInformation
				 ticket, org.apache.hadoop.conf.Configuration conf, javax.net.SocketFactory factory
				, int rpcTimeout, org.apache.hadoop.io.retry.RetryPolicy connectionRetryPolicy, 
				java.util.concurrent.atomic.AtomicBoolean fallbackToSimpleAuth)
			{
				System.Type protocol = typeof(T);
				T proxy = (T)java.lang.reflect.Proxy.newProxyInstance(protocol.getClassLoader(), 
					new java.lang.Class[] { protocol }, new org.apache.hadoop.ipc.TestRPC.StoppedInvocationHandler
					());
				return new org.apache.hadoop.ipc.ProtocolProxy<T>(protocol, proxy, false);
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual org.apache.hadoop.ipc.RPC.Server getServer<_T0>(java.lang.Class protocol
				, object instance, string bindAddress, int port, int numHandlers, int numReaders
				, int queueSizePerHandler, bool verbose, org.apache.hadoop.conf.Configuration conf
				, org.apache.hadoop.security.token.SecretManager<_T0> secretManager, string portRangeConfig
				)
				where _T0 : org.apache.hadoop.security.token.TokenIdentifier
			{
				return null;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual org.apache.hadoop.ipc.ProtocolProxy<org.apache.hadoop.ipc.ProtocolMetaInfoPB
				> getProtocolMetaInfoProxy(org.apache.hadoop.ipc.Client.ConnectionId connId, org.apache.hadoop.conf.Configuration
				 conf, javax.net.SocketFactory factory)
			{
				throw new System.NotSupportedException("This proxy is not supported");
			}
		}

		/// <summary>
		/// An invocation handler which does nothing when invoking methods, and just
		/// counts the number of times close() is called.
		/// </summary>
		private class StoppedInvocationHandler : java.lang.reflect.InvocationHandler, java.io.Closeable
		{
			private int closeCalled = 0;

			/// <exception cref="System.Exception"/>
			public virtual object invoke(object proxy, java.lang.reflect.Method method, object
				[] args)
			{
				return null;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void close()
			{
				closeCalled++;
			}

			public virtual int getCloseCalled()
			{
				return closeCalled;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testConfRpc()
		{
			org.apache.hadoop.ipc.Server server = new org.apache.hadoop.ipc.RPC.Builder(conf)
				.setProtocol(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.ipc.TestRPC.TestProtocol
				))).setInstance(new org.apache.hadoop.ipc.TestRPC.TestImpl()).setBindAddress(ADDRESS
				).setPort(0).setNumHandlers(1).setVerbose(false).build();
			// Just one handler
			int confQ = conf.getInt(org.apache.hadoop.fs.CommonConfigurationKeys.IPC_SERVER_HANDLER_QUEUE_SIZE_KEY
				, org.apache.hadoop.fs.CommonConfigurationKeys.IPC_SERVER_HANDLER_QUEUE_SIZE_DEFAULT
				);
			NUnit.Framework.Assert.AreEqual(confQ, server.getMaxQueueSize());
			int confReaders = conf.getInt(org.apache.hadoop.fs.CommonConfigurationKeys.IPC_SERVER_RPC_READ_THREADS_KEY
				, org.apache.hadoop.fs.CommonConfigurationKeys.IPC_SERVER_RPC_READ_THREADS_DEFAULT
				);
			NUnit.Framework.Assert.AreEqual(confReaders, server.getNumReaders());
			server.stop();
			server = new org.apache.hadoop.ipc.RPC.Builder(conf).setProtocol(Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.ipc.TestRPC.TestProtocol))).setInstance(new org.apache.hadoop.ipc.TestRPC.TestImpl
				()).setBindAddress(ADDRESS).setPort(0).setNumHandlers(1).setnumReaders(3).setQueueSizePerHandler
				(200).setVerbose(false).build();
			NUnit.Framework.Assert.AreEqual(3, server.getNumReaders());
			NUnit.Framework.Assert.AreEqual(200, server.getMaxQueueSize());
			server.stop();
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testProxyAddress()
		{
			org.apache.hadoop.ipc.Server server = new org.apache.hadoop.ipc.RPC.Builder(conf)
				.setProtocol(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.ipc.TestRPC.TestProtocol
				))).setInstance(new org.apache.hadoop.ipc.TestRPC.TestImpl()).setBindAddress(ADDRESS
				).setPort(0).build();
			org.apache.hadoop.ipc.TestRPC.TestProtocol proxy = null;
			try
			{
				server.start();
				java.net.InetSocketAddress addr = org.apache.hadoop.net.NetUtils.getConnectAddress
					(server);
				// create a client
				proxy = org.apache.hadoop.ipc.RPC.getProxy<org.apache.hadoop.ipc.TestRPC.TestProtocol
					>(org.apache.hadoop.ipc.TestRPC.TestProtocol.versionID, addr, conf);
				NUnit.Framework.Assert.AreEqual(addr, org.apache.hadoop.ipc.RPC.getServerAddress(
					proxy));
			}
			finally
			{
				server.stop();
				if (proxy != null)
				{
					org.apache.hadoop.ipc.RPC.stopProxy(proxy);
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testSlowRpc()
		{
			System.Console.Out.WriteLine("Testing Slow RPC");
			// create a server with two handlers
			org.apache.hadoop.ipc.Server server = new org.apache.hadoop.ipc.RPC.Builder(conf)
				.setProtocol(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.ipc.TestRPC.TestProtocol
				))).setInstance(new org.apache.hadoop.ipc.TestRPC.TestImpl()).setBindAddress(ADDRESS
				).setPort(0).setNumHandlers(2).setVerbose(false).build();
			org.apache.hadoop.ipc.TestRPC.TestProtocol proxy = null;
			try
			{
				server.start();
				java.net.InetSocketAddress addr = org.apache.hadoop.net.NetUtils.getConnectAddress
					(server);
				// create a client
				proxy = org.apache.hadoop.ipc.RPC.getProxy<org.apache.hadoop.ipc.TestRPC.TestProtocol
					>(org.apache.hadoop.ipc.TestRPC.TestProtocol.versionID, addr, conf);
				org.apache.hadoop.ipc.TestRPC.SlowRPC slowrpc = new org.apache.hadoop.ipc.TestRPC.SlowRPC
					(proxy);
				java.lang.Thread thread = new java.lang.Thread(slowrpc, "SlowRPC");
				thread.start();
				// send a slow RPC, which won't return until two fast pings
				NUnit.Framework.Assert.IsTrue("Slow RPC should not have finished1.", !slowrpc.isDone
					());
				proxy.slowPing(false);
				// first fast ping
				// verify that the first RPC is still stuck
				NUnit.Framework.Assert.IsTrue("Slow RPC should not have finished2.", !slowrpc.isDone
					());
				proxy.slowPing(false);
				// second fast ping
				// Now the slow ping should be able to be executed
				while (!slowrpc.isDone())
				{
					System.Console.Out.WriteLine("Waiting for slow RPC to get done.");
					try
					{
						java.lang.Thread.sleep(1000);
					}
					catch (System.Exception)
					{
					}
				}
			}
			finally
			{
				server.stop();
				if (proxy != null)
				{
					org.apache.hadoop.ipc.RPC.stopProxy(proxy);
				}
				System.Console.Out.WriteLine("Down slow rpc testing");
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testCalls()
		{
			testCallsInternal(conf);
		}

		/// <exception cref="System.IO.IOException"/>
		private void testCallsInternal(org.apache.hadoop.conf.Configuration conf)
		{
			org.apache.hadoop.ipc.Server server = new org.apache.hadoop.ipc.RPC.Builder(conf)
				.setProtocol(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.ipc.TestRPC.TestProtocol
				))).setInstance(new org.apache.hadoop.ipc.TestRPC.TestImpl()).setBindAddress(ADDRESS
				).setPort(0).build();
			org.apache.hadoop.ipc.TestRPC.TestProtocol proxy = null;
			try
			{
				server.start();
				java.net.InetSocketAddress addr = org.apache.hadoop.net.NetUtils.getConnectAddress
					(server);
				proxy = org.apache.hadoop.ipc.RPC.getProxy<org.apache.hadoop.ipc.TestRPC.TestProtocol
					>(org.apache.hadoop.ipc.TestRPC.TestProtocol.versionID, addr, conf);
				proxy.ping();
				string stringResult = proxy.echo("foo");
				NUnit.Framework.Assert.AreEqual(stringResult, "foo");
				stringResult = proxy.echo((string)null);
				NUnit.Framework.Assert.AreEqual(stringResult, null);
				// Check rpcMetrics 
				org.apache.hadoop.metrics2.MetricsRecordBuilder rb = org.apache.hadoop.test.MetricsAsserts.getMetrics
					(server.rpcMetrics.name());
				org.apache.hadoop.test.MetricsAsserts.assertCounter("RpcProcessingTimeNumOps", 3L
					, rb);
				org.apache.hadoop.test.MetricsAsserts.assertCounterGt("SentBytes", 0L, rb);
				org.apache.hadoop.test.MetricsAsserts.assertCounterGt("ReceivedBytes", 0L, rb);
				// Number of calls to echo method should be 2
				rb = org.apache.hadoop.test.MetricsAsserts.getMetrics(server.rpcDetailedMetrics.name
					());
				org.apache.hadoop.test.MetricsAsserts.assertCounter("EchoNumOps", 2L, rb);
				// Number of calls to ping method should be 1
				org.apache.hadoop.test.MetricsAsserts.assertCounter("PingNumOps", 1L, rb);
				string[] stringResults = proxy.echo(new string[] { "foo", "bar" });
				NUnit.Framework.Assert.IsTrue(java.util.Arrays.equals(stringResults, new string[]
					 { "foo", "bar" }));
				stringResults = proxy.echo((string[])null);
				NUnit.Framework.Assert.IsTrue(java.util.Arrays.equals(stringResults, null));
				org.apache.hadoop.io.UTF8 utf8Result = (org.apache.hadoop.io.UTF8)proxy.echo(new 
					org.apache.hadoop.io.UTF8("hello world"));
				NUnit.Framework.Assert.AreEqual(new org.apache.hadoop.io.UTF8("hello world"), utf8Result
					);
				utf8Result = (org.apache.hadoop.io.UTF8)proxy.echo((org.apache.hadoop.io.UTF8)null
					);
				NUnit.Framework.Assert.AreEqual(null, utf8Result);
				int intResult = proxy.add(1, 2);
				NUnit.Framework.Assert.AreEqual(intResult, 3);
				intResult = proxy.add(new int[] { 1, 2 });
				NUnit.Framework.Assert.AreEqual(intResult, 3);
				// Test protobufs
				com.google.protobuf.DescriptorProtos.EnumDescriptorProto sendProto = ((com.google.protobuf.DescriptorProtos.EnumDescriptorProto
					)com.google.protobuf.DescriptorProtos.EnumDescriptorProto.newBuilder().setName("test"
					).build());
				com.google.protobuf.DescriptorProtos.EnumDescriptorProto retProto = proxy.exchangeProto
					(sendProto);
				NUnit.Framework.Assert.AreEqual(sendProto, retProto);
				NUnit.Framework.Assert.AreNotSame(sendProto, retProto);
				bool caught = false;
				try
				{
					proxy.error();
				}
				catch (System.IO.IOException e)
				{
					if (LOG.isDebugEnabled())
					{
						LOG.debug("Caught " + e);
					}
					caught = true;
				}
				NUnit.Framework.Assert.IsTrue(caught);
				rb = org.apache.hadoop.test.MetricsAsserts.getMetrics(server.rpcDetailedMetrics.name
					());
				org.apache.hadoop.test.MetricsAsserts.assertCounter("IOExceptionNumOps", 1L, rb);
				proxy.testServerGet();
				// create multiple threads and make them do large data transfers
				System.Console.Out.WriteLine("Starting multi-threaded RPC test...");
				server.setSocketSendBufSize(1024);
				java.lang.Thread[] threadId = new java.lang.Thread[numThreads];
				for (int i = 0; i < numThreads; i++)
				{
					org.apache.hadoop.ipc.TestRPC.Transactions trans = new org.apache.hadoop.ipc.TestRPC.Transactions
						(proxy, datasize);
					threadId[i] = new java.lang.Thread(trans, "TransactionThread-" + i);
					threadId[i].start();
				}
				// wait for all transactions to get over
				System.Console.Out.WriteLine("Waiting for all threads to finish RPCs...");
				for (int i_1 = 0; i_1 < numThreads; i_1++)
				{
					try
					{
						threadId[i_1].join();
					}
					catch (System.Exception)
					{
						i_1--;
					}
				}
			}
			finally
			{
				// retry
				server.stop();
				if (proxy != null)
				{
					org.apache.hadoop.ipc.RPC.stopProxy(proxy);
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testStandaloneClient()
		{
			try
			{
				org.apache.hadoop.ipc.TestRPC.TestProtocol proxy = org.apache.hadoop.ipc.RPC.waitForProxy
					<org.apache.hadoop.ipc.TestRPC.TestProtocol>(org.apache.hadoop.ipc.TestRPC.TestProtocol
					.versionID, new java.net.InetSocketAddress(ADDRESS, 20), conf, 15000L);
				proxy.echo(string.Empty);
				NUnit.Framework.Assert.Fail("We should not have reached here");
			}
			catch (java.net.ConnectException)
			{
			}
		}

		private const string ACL_CONFIG = "test.protocol.acl";

		private class TestPolicyProvider : org.apache.hadoop.security.authorize.PolicyProvider
		{
			//this is what we expected
			public override org.apache.hadoop.security.authorize.Service[] getServices()
			{
				return new org.apache.hadoop.security.authorize.Service[] { new org.apache.hadoop.security.authorize.Service
					(ACL_CONFIG, Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.ipc.TestRPC.TestProtocol
					))) };
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void doRPCs(org.apache.hadoop.conf.Configuration conf, bool expectFailure
			)
		{
			org.apache.hadoop.ipc.Server server = new org.apache.hadoop.ipc.RPC.Builder(conf)
				.setProtocol(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.ipc.TestRPC.TestProtocol
				))).setInstance(new org.apache.hadoop.ipc.TestRPC.TestImpl()).setBindAddress(ADDRESS
				).setPort(0).setNumHandlers(5).setVerbose(true).build();
			server.refreshServiceAcl(conf, new org.apache.hadoop.ipc.TestRPC.TestPolicyProvider
				());
			org.apache.hadoop.ipc.TestRPC.TestProtocol proxy = null;
			server.start();
			java.net.InetSocketAddress addr = org.apache.hadoop.net.NetUtils.getConnectAddress
				(server);
			try
			{
				proxy = org.apache.hadoop.ipc.RPC.getProxy<org.apache.hadoop.ipc.TestRPC.TestProtocol
					>(org.apache.hadoop.ipc.TestRPC.TestProtocol.versionID, addr, conf);
				proxy.ping();
				if (expectFailure)
				{
					NUnit.Framework.Assert.Fail("Expect RPC.getProxy to fail with AuthorizationException!"
						);
				}
			}
			catch (org.apache.hadoop.ipc.RemoteException e)
			{
				if (expectFailure)
				{
					NUnit.Framework.Assert.IsTrue(e.unwrapRemoteException() is org.apache.hadoop.security.authorize.AuthorizationException
						);
				}
				else
				{
					throw;
				}
			}
			finally
			{
				server.stop();
				if (proxy != null)
				{
					org.apache.hadoop.ipc.RPC.stopProxy(proxy);
				}
				org.apache.hadoop.metrics2.MetricsRecordBuilder rb = org.apache.hadoop.test.MetricsAsserts.getMetrics
					(server.rpcMetrics.name());
				if (expectFailure)
				{
					org.apache.hadoop.test.MetricsAsserts.assertCounter("RpcAuthorizationFailures", 1L
						, rb);
				}
				else
				{
					org.apache.hadoop.test.MetricsAsserts.assertCounter("RpcAuthorizationSuccesses", 
						1L, rb);
				}
				//since we don't have authentication turned ON, we should see 
				// 0 for the authentication successes and 0 for failure
				org.apache.hadoop.test.MetricsAsserts.assertCounter("RpcAuthenticationFailures", 
					0L, rb);
				org.apache.hadoop.test.MetricsAsserts.assertCounter("RpcAuthenticationSuccesses", 
					0L, rb);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testServerAddress()
		{
			org.apache.hadoop.ipc.Server server = new org.apache.hadoop.ipc.RPC.Builder(conf)
				.setProtocol(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.ipc.TestRPC.TestProtocol
				))).setInstance(new org.apache.hadoop.ipc.TestRPC.TestImpl()).setBindAddress(ADDRESS
				).setPort(0).setNumHandlers(5).setVerbose(true).build();
			java.net.InetSocketAddress bindAddr = null;
			try
			{
				bindAddr = org.apache.hadoop.net.NetUtils.getConnectAddress(server);
			}
			finally
			{
				server.stop();
			}
			NUnit.Framework.Assert.AreEqual(java.net.InetAddress.getLocalHost(), bindAddr.getAddress
				());
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testAuthorization()
		{
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			conf.setBoolean(org.apache.hadoop.fs.CommonConfigurationKeys.HADOOP_SECURITY_AUTHORIZATION
				, true);
			// Expect to succeed
			conf.set(ACL_CONFIG, "*");
			doRPCs(conf, false);
			// Reset authorization to expect failure
			conf.set(ACL_CONFIG, "invalid invalid");
			doRPCs(conf, true);
			conf.setInt(org.apache.hadoop.fs.CommonConfigurationKeys.IPC_SERVER_RPC_READ_THREADS_KEY
				, 2);
			// Expect to succeed
			conf.set(ACL_CONFIG, "*");
			doRPCs(conf, false);
			// Reset authorization to expect failure
			conf.set(ACL_CONFIG, "invalid invalid");
			doRPCs(conf, true);
		}

		/// <summary>Switch off setting socketTimeout values on RPC sockets.</summary>
		/// <remarks>
		/// Switch off setting socketTimeout values on RPC sockets.
		/// Verify that RPC calls still work ok.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public virtual void testNoPings()
		{
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			conf.setBoolean("ipc.client.ping", false);
			new org.apache.hadoop.ipc.TestRPC().testCallsInternal(conf);
			conf.setInt(org.apache.hadoop.fs.CommonConfigurationKeys.IPC_SERVER_RPC_READ_THREADS_KEY
				, 2);
			new org.apache.hadoop.ipc.TestRPC().testCallsInternal(conf);
		}

		/// <summary>Test stopping a non-registered proxy</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void testStopNonRegisteredProxy()
		{
			org.apache.hadoop.ipc.RPC.stopProxy(null);
		}

		/// <summary>
		/// Test that the mockProtocol helper returns mock proxies that can
		/// be stopped without error.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testStopMockObject()
		{
			org.apache.hadoop.ipc.RPC.stopProxy(org.apache.hadoop.test.MockitoUtil.mockProtocol
				<org.apache.hadoop.ipc.TestRPC.TestProtocol>());
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testStopProxy()
		{
			org.apache.hadoop.ipc.TestRPC.StoppedProtocol proxy = org.apache.hadoop.ipc.RPC.getProxy
				<org.apache.hadoop.ipc.TestRPC.StoppedProtocol>(org.apache.hadoop.ipc.TestRPC.StoppedProtocol
				.versionID, null, conf);
			org.apache.hadoop.ipc.TestRPC.StoppedInvocationHandler invocationHandler = (org.apache.hadoop.ipc.TestRPC.StoppedInvocationHandler
				)java.lang.reflect.Proxy.getInvocationHandler(proxy);
			NUnit.Framework.Assert.AreEqual(0, invocationHandler.getCloseCalled());
			org.apache.hadoop.ipc.RPC.stopProxy(proxy);
			NUnit.Framework.Assert.AreEqual(1, invocationHandler.getCloseCalled());
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testWrappedStopProxy()
		{
			org.apache.hadoop.ipc.TestRPC.StoppedProtocol wrappedProxy = org.apache.hadoop.ipc.RPC
				.getProxy<org.apache.hadoop.ipc.TestRPC.StoppedProtocol>(org.apache.hadoop.ipc.TestRPC.StoppedProtocol
				.versionID, null, conf);
			org.apache.hadoop.ipc.TestRPC.StoppedInvocationHandler invocationHandler = (org.apache.hadoop.ipc.TestRPC.StoppedInvocationHandler
				)java.lang.reflect.Proxy.getInvocationHandler(wrappedProxy);
			org.apache.hadoop.ipc.TestRPC.StoppedProtocol proxy = (org.apache.hadoop.ipc.TestRPC.StoppedProtocol
				)org.apache.hadoop.io.retry.RetryProxy.create<org.apache.hadoop.ipc.TestRPC.StoppedProtocol
				>(wrappedProxy, org.apache.hadoop.io.retry.RetryPolicies.RETRY_FOREVER);
			NUnit.Framework.Assert.AreEqual(0, invocationHandler.getCloseCalled());
			org.apache.hadoop.ipc.RPC.stopProxy(proxy);
			NUnit.Framework.Assert.AreEqual(1, invocationHandler.getCloseCalled());
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testErrorMsgForInsecureClient()
		{
			org.apache.hadoop.conf.Configuration serverConf = new org.apache.hadoop.conf.Configuration
				(conf);
			org.apache.hadoop.security.SecurityUtil.setAuthenticationMethod(org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod
				.KERBEROS, serverConf);
			org.apache.hadoop.security.UserGroupInformation.setConfiguration(serverConf);
			org.apache.hadoop.ipc.Server server = new org.apache.hadoop.ipc.RPC.Builder(serverConf
				).setProtocol(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.ipc.TestRPC.TestProtocol
				))).setInstance(new org.apache.hadoop.ipc.TestRPC.TestImpl()).setBindAddress(ADDRESS
				).setPort(0).setNumHandlers(5).setVerbose(true).build();
			server.start();
			org.apache.hadoop.security.UserGroupInformation.setConfiguration(conf);
			bool succeeded = false;
			java.net.InetSocketAddress addr = org.apache.hadoop.net.NetUtils.getConnectAddress
				(server);
			org.apache.hadoop.ipc.TestRPC.TestProtocol proxy = null;
			try
			{
				proxy = org.apache.hadoop.ipc.RPC.getProxy<org.apache.hadoop.ipc.TestRPC.TestProtocol
					>(org.apache.hadoop.ipc.TestRPC.TestProtocol.versionID, addr, conf);
				proxy.echo(string.Empty);
			}
			catch (org.apache.hadoop.ipc.RemoteException e)
			{
				LOG.info("LOGGING MESSAGE: " + e.getLocalizedMessage());
				NUnit.Framework.Assert.IsTrue(e.unwrapRemoteException() is org.apache.hadoop.security.AccessControlException
					);
				succeeded = true;
			}
			finally
			{
				server.stop();
				if (proxy != null)
				{
					org.apache.hadoop.ipc.RPC.stopProxy(proxy);
				}
			}
			NUnit.Framework.Assert.IsTrue(succeeded);
			conf.setInt(org.apache.hadoop.fs.CommonConfigurationKeys.IPC_SERVER_RPC_READ_THREADS_KEY
				, 2);
			org.apache.hadoop.security.UserGroupInformation.setConfiguration(serverConf);
			org.apache.hadoop.ipc.Server multiServer = new org.apache.hadoop.ipc.RPC.Builder(
				serverConf).setProtocol(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.ipc.TestRPC.TestProtocol
				))).setInstance(new org.apache.hadoop.ipc.TestRPC.TestImpl()).setBindAddress(ADDRESS
				).setPort(0).setNumHandlers(5).setVerbose(true).build();
			multiServer.start();
			succeeded = false;
			java.net.InetSocketAddress mulitServerAddr = org.apache.hadoop.net.NetUtils.getConnectAddress
				(multiServer);
			proxy = null;
			try
			{
				org.apache.hadoop.security.UserGroupInformation.setConfiguration(conf);
				proxy = org.apache.hadoop.ipc.RPC.getProxy<org.apache.hadoop.ipc.TestRPC.TestProtocol
					>(org.apache.hadoop.ipc.TestRPC.TestProtocol.versionID, mulitServerAddr, conf);
				proxy.echo(string.Empty);
			}
			catch (org.apache.hadoop.ipc.RemoteException e)
			{
				LOG.info("LOGGING MESSAGE: " + e.getLocalizedMessage());
				NUnit.Framework.Assert.IsTrue(e.unwrapRemoteException() is org.apache.hadoop.security.AccessControlException
					);
				succeeded = true;
			}
			finally
			{
				multiServer.stop();
				if (proxy != null)
				{
					org.apache.hadoop.ipc.RPC.stopProxy(proxy);
				}
			}
			NUnit.Framework.Assert.IsTrue(succeeded);
		}

		/// <summary>
		/// Count the number of threads that have a stack frame containing
		/// the given string
		/// </summary>
		private static int countThreads(string search)
		{
			java.lang.management.ThreadMXBean threadBean = java.lang.management.ManagementFactory
				.getThreadMXBean();
			int count = 0;
			java.lang.management.ThreadInfo[] infos = threadBean.getThreadInfo(threadBean.getAllThreadIds
				(), 20);
			foreach (java.lang.management.ThreadInfo info in infos)
			{
				if (info == null)
				{
					continue;
				}
				foreach (java.lang.StackTraceElement elem in info.getStackTrace())
				{
					if (elem.getClassName().contains(search))
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
		public virtual void testStopsAllThreads()
		{
			int threadsBefore = countThreads("Server$Listener$Reader");
			NUnit.Framework.Assert.AreEqual("Expect no Reader threads running before test", 0
				, threadsBefore);
			org.apache.hadoop.ipc.Server server = new org.apache.hadoop.ipc.RPC.Builder(conf)
				.setProtocol(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.ipc.TestRPC.TestProtocol
				))).setInstance(new org.apache.hadoop.ipc.TestRPC.TestImpl()).setBindAddress(ADDRESS
				).setPort(0).setNumHandlers(5).setVerbose(true).build();
			server.start();
			try
			{
				// Wait for at least one reader thread to start
				int threadsRunning = 0;
				long totalSleepTime = 0;
				do
				{
					totalSleepTime += 10;
					java.lang.Thread.sleep(10);
					threadsRunning = countThreads("Server$Listener$Reader");
				}
				while (threadsRunning == 0 && totalSleepTime < 5000);
				// Validate that at least one thread started (we didn't timeout)
				threadsRunning = countThreads("Server$Listener$Reader");
				NUnit.Framework.Assert.IsTrue(threadsRunning > 0);
			}
			finally
			{
				server.stop();
			}
			int threadsAfter = countThreads("Server$Listener$Reader");
			NUnit.Framework.Assert.AreEqual("Expect no Reader threads left running after test"
				, 0, threadsAfter);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testRPCBuilder()
		{
			// Test mandatory field conf
			try
			{
				new org.apache.hadoop.ipc.RPC.Builder(null).setProtocol(Sharpen.Runtime.getClassForType
					(typeof(org.apache.hadoop.ipc.TestRPC.TestProtocol))).setInstance(new org.apache.hadoop.ipc.TestRPC.TestImpl
					()).setBindAddress(ADDRESS).setPort(0).setNumHandlers(5).setVerbose(true).build(
					);
				NUnit.Framework.Assert.Fail("Didn't throw HadoopIllegalArgumentException");
			}
			catch (System.Exception e)
			{
				if (!(e is org.apache.hadoop.HadoopIllegalArgumentException))
				{
					NUnit.Framework.Assert.Fail("Expecting HadoopIllegalArgumentException but caught "
						 + e);
				}
			}
			// Test mandatory field protocol
			try
			{
				new org.apache.hadoop.ipc.RPC.Builder(conf).setInstance(new org.apache.hadoop.ipc.TestRPC.TestImpl
					()).setBindAddress(ADDRESS).setPort(0).setNumHandlers(5).setVerbose(true).build(
					);
				NUnit.Framework.Assert.Fail("Didn't throw HadoopIllegalArgumentException");
			}
			catch (System.Exception e)
			{
				if (!(e is org.apache.hadoop.HadoopIllegalArgumentException))
				{
					NUnit.Framework.Assert.Fail("Expecting HadoopIllegalArgumentException but caught "
						 + e);
				}
			}
			// Test mandatory field instance
			try
			{
				new org.apache.hadoop.ipc.RPC.Builder(conf).setProtocol(Sharpen.Runtime.getClassForType
					(typeof(org.apache.hadoop.ipc.TestRPC.TestProtocol))).setBindAddress(ADDRESS).setPort
					(0).setNumHandlers(5).setVerbose(true).build();
				NUnit.Framework.Assert.Fail("Didn't throw HadoopIllegalArgumentException");
			}
			catch (System.Exception e)
			{
				if (!(e is org.apache.hadoop.HadoopIllegalArgumentException))
				{
					NUnit.Framework.Assert.Fail("Expecting HadoopIllegalArgumentException but caught "
						 + e);
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testRPCInterruptedSimple()
		{
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			org.apache.hadoop.ipc.Server server = new org.apache.hadoop.ipc.RPC.Builder(conf)
				.setProtocol(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.ipc.TestRPC.TestProtocol
				))).setInstance(new org.apache.hadoop.ipc.TestRPC.TestImpl()).setBindAddress(ADDRESS
				).setPort(0).setNumHandlers(5).setVerbose(true).setSecretManager(null).build();
			server.start();
			java.net.InetSocketAddress addr = org.apache.hadoop.net.NetUtils.getConnectAddress
				(server);
			org.apache.hadoop.ipc.TestRPC.TestProtocol proxy = org.apache.hadoop.ipc.RPC.getProxy
				<org.apache.hadoop.ipc.TestRPC.TestProtocol>(org.apache.hadoop.ipc.TestRPC.TestProtocol
				.versionID, addr, conf);
			// Connect to the server
			proxy.ping();
			// Interrupt self, try another call
			java.lang.Thread.currentThread().interrupt();
			try
			{
				proxy.ping();
				NUnit.Framework.Assert.Fail("Interruption did not cause IPC to fail");
			}
			catch (System.IO.IOException ioe)
			{
				if (!ioe.ToString().contains("InterruptedException"))
				{
					throw;
				}
				// clear interrupt status for future tests
				java.lang.Thread.interrupted();
			}
			finally
			{
				server.stop();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual void testRPCInterrupted()
		{
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			org.apache.hadoop.ipc.Server server = new org.apache.hadoop.ipc.RPC.Builder(conf)
				.setProtocol(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.ipc.TestRPC.TestProtocol
				))).setInstance(new org.apache.hadoop.ipc.TestRPC.TestImpl()).setBindAddress(ADDRESS
				).setPort(0).setNumHandlers(5).setVerbose(true).setSecretManager(null).build();
			server.start();
			int numConcurrentRPC = 200;
			java.net.InetSocketAddress addr = org.apache.hadoop.net.NetUtils.getConnectAddress
				(server);
			java.util.concurrent.CyclicBarrier barrier = new java.util.concurrent.CyclicBarrier
				(numConcurrentRPC);
			java.util.concurrent.CountDownLatch latch = new java.util.concurrent.CountDownLatch
				(numConcurrentRPC);
			java.util.concurrent.atomic.AtomicBoolean leaderRunning = new java.util.concurrent.atomic.AtomicBoolean
				(true);
			java.util.concurrent.atomic.AtomicReference<System.Exception> error = new java.util.concurrent.atomic.AtomicReference
				<System.Exception>();
			java.lang.Thread leaderThread = null;
			for (int i = 0; i < numConcurrentRPC; i++)
			{
				int num = i;
				org.apache.hadoop.ipc.TestRPC.TestProtocol proxy = org.apache.hadoop.ipc.RPC.getProxy
					<org.apache.hadoop.ipc.TestRPC.TestProtocol>(org.apache.hadoop.ipc.TestRPC.TestProtocol
					.versionID, addr, conf);
				java.lang.Thread rpcThread = new java.lang.Thread(new _Runnable_915(barrier, num, 
					leaderRunning, proxy, error, latch));
				rpcThread.start();
				if (leaderThread == null)
				{
					leaderThread = rpcThread;
				}
			}
			// let threads get past the barrier
			java.lang.Thread.sleep(1000);
			// stop a single thread
			while (leaderRunning.get())
			{
				leaderThread.interrupt();
			}
			latch.await();
			// should not cause any other thread to get an error
			NUnit.Framework.Assert.IsTrue("rpc got exception " + error.get(), error.get() == 
				null);
			server.stop();
		}

		private sealed class _Runnable_915 : java.lang.Runnable
		{
			public _Runnable_915(java.util.concurrent.CyclicBarrier barrier, int num, java.util.concurrent.atomic.AtomicBoolean
				 leaderRunning, org.apache.hadoop.ipc.TestRPC.TestProtocol proxy, java.util.concurrent.atomic.AtomicReference
				<System.Exception> error, java.util.concurrent.CountDownLatch latch)
			{
				this.barrier = barrier;
				this.num = num;
				this.leaderRunning = leaderRunning;
				this.proxy = proxy;
				this.error = error;
				this.latch = latch;
			}

			public void run()
			{
				try
				{
					barrier.await();
					while (num == 0 || leaderRunning.get())
					{
						proxy.slowPing(false);
					}
					proxy.slowPing(false);
				}
				catch (System.Exception e)
				{
					if (num == 0)
					{
						leaderRunning.set(false);
					}
					else
					{
						error.set(e);
					}
					org.apache.hadoop.ipc.TestRPC.LOG.error(e);
				}
				finally
				{
					latch.countDown();
				}
			}

			private readonly java.util.concurrent.CyclicBarrier barrier;

			private readonly int num;

			private readonly java.util.concurrent.atomic.AtomicBoolean leaderRunning;

			private readonly org.apache.hadoop.ipc.TestRPC.TestProtocol proxy;

			private readonly java.util.concurrent.atomic.AtomicReference<System.Exception> error;

			private readonly java.util.concurrent.CountDownLatch latch;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testConnectionPing()
		{
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			int pingInterval = 50;
			conf.setBoolean(org.apache.hadoop.fs.CommonConfigurationKeys.IPC_CLIENT_PING_KEY, 
				true);
			conf.setInt(org.apache.hadoop.fs.CommonConfigurationKeys.IPC_PING_INTERVAL_KEY, pingInterval
				);
			org.apache.hadoop.ipc.Server server = new org.apache.hadoop.ipc.RPC.Builder(conf)
				.setProtocol(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.ipc.TestRPC.TestProtocol
				))).setInstance(new org.apache.hadoop.ipc.TestRPC.TestImpl()).setBindAddress(ADDRESS
				).setPort(0).setNumHandlers(5).setVerbose(true).build();
			server.start();
			org.apache.hadoop.ipc.TestRPC.TestProtocol proxy = org.apache.hadoop.ipc.RPC.getProxy
				<org.apache.hadoop.ipc.TestRPC.TestProtocol>(org.apache.hadoop.ipc.TestRPC.TestProtocol
				.versionID, server.getListenerAddress(), conf);
			try
			{
				// this call will throw exception if server couldn't decode the ping
				proxy.sleep(pingInterval * 4);
			}
			finally
			{
				if (proxy != null)
				{
					org.apache.hadoop.ipc.RPC.stopProxy(proxy);
				}
				server.stop();
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testRpcMetrics()
		{
			org.apache.hadoop.conf.Configuration configuration = new org.apache.hadoop.conf.Configuration
				();
			int interval = 1;
			configuration.setBoolean(org.apache.hadoop.fs.CommonConfigurationKeys.RPC_METRICS_QUANTILE_ENABLE
				, true);
			configuration.set(org.apache.hadoop.fs.CommonConfigurationKeys.RPC_METRICS_PERCENTILES_INTERVALS_KEY
				, string.Empty + interval);
			org.apache.hadoop.ipc.Server server = new org.apache.hadoop.ipc.RPC.Builder(configuration
				).setProtocol(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.ipc.TestRPC.TestProtocol
				))).setInstance(new org.apache.hadoop.ipc.TestRPC.TestImpl()).setBindAddress(ADDRESS
				).setPort(0).setNumHandlers(5).setVerbose(true).build();
			server.start();
			org.apache.hadoop.ipc.TestRPC.TestProtocol proxy = org.apache.hadoop.ipc.RPC.getProxy
				<org.apache.hadoop.ipc.TestRPC.TestProtocol>(org.apache.hadoop.ipc.TestRPC.TestProtocol
				.versionID, server.getListenerAddress(), configuration);
			try
			{
				for (int i = 0; i < 1000; i++)
				{
					proxy.ping();
					proxy.echo(string.Empty + i);
				}
				org.apache.hadoop.metrics2.MetricsRecordBuilder rpcMetrics = org.apache.hadoop.test.MetricsAsserts.getMetrics
					(server.getRpcMetrics().name());
				NUnit.Framework.Assert.IsTrue("Expected non-zero rpc queue time", org.apache.hadoop.test.MetricsAsserts.getLongCounter
					("RpcQueueTimeNumOps", rpcMetrics) > 0);
				NUnit.Framework.Assert.IsTrue("Expected non-zero rpc processing time", org.apache.hadoop.test.MetricsAsserts.getLongCounter
					("RpcProcessingTimeNumOps", rpcMetrics) > 0);
				org.apache.hadoop.test.MetricsAsserts.assertQuantileGauges("RpcQueueTime" + interval
					 + "s", rpcMetrics);
				org.apache.hadoop.test.MetricsAsserts.assertQuantileGauges("RpcProcessingTime" + 
					interval + "s", rpcMetrics);
			}
			finally
			{
				if (proxy != null)
				{
					org.apache.hadoop.ipc.RPC.stopProxy(proxy);
				}
				server.stop();
			}
		}

		/// <summary>Verify the RPC server can shutdown properly when callQueue is full.</summary>
		/// <exception cref="System.Exception"/>
		public virtual void testRPCServerShutdown()
		{
			int numClients = 3;
			System.Collections.Generic.IList<java.util.concurrent.Future<java.lang.Void>> res
				 = new System.Collections.Generic.List<java.util.concurrent.Future<java.lang.Void
				>>();
			java.util.concurrent.ExecutorService executorService = java.util.concurrent.Executors
				.newFixedThreadPool(numClients);
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			conf.setInt(org.apache.hadoop.fs.CommonConfigurationKeys.IPC_CLIENT_CONNECT_MAX_RETRIES_KEY
				, 0);
			org.apache.hadoop.ipc.Server server = new org.apache.hadoop.ipc.RPC.Builder(conf)
				.setProtocol(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.ipc.TestRPC.TestProtocol
				))).setInstance(new org.apache.hadoop.ipc.TestRPC.TestImpl()).setBindAddress(ADDRESS
				).setPort(0).setQueueSizePerHandler(1).setNumHandlers(1).setVerbose(true).build(
				);
			server.start();
			org.apache.hadoop.ipc.TestRPC.TestProtocol proxy = org.apache.hadoop.ipc.RPC.getProxy
				<org.apache.hadoop.ipc.TestRPC.TestProtocol>(org.apache.hadoop.ipc.TestRPC.TestProtocol
				.versionID, org.apache.hadoop.net.NetUtils.getConnectAddress(server), conf);
			try
			{
				// start a sleep RPC call to consume the only handler thread.
				// Start another sleep RPC call to make callQueue full.
				// Start another sleep RPC call to make reader thread block on CallQueue.
				for (int i = 0; i < numClients; i++)
				{
					res.add(executorService.submit(new _Callable_1046(proxy)));
				}
				while (server.getCallQueueLen() != 1 && countThreads(Sharpen.Runtime.getClassForType
					(typeof(org.apache.hadoop.ipc.CallQueueManager)).getName()) != 1 && countThreads
					(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.ipc.TestRPC.TestProtocol
					)).getName()) != 1)
				{
					java.lang.Thread.sleep(100);
				}
			}
			finally
			{
				try
				{
					server.stop();
					NUnit.Framework.Assert.AreEqual("Not enough clients", numClients, res.Count);
					foreach (java.util.concurrent.Future<java.lang.Void> f in res)
					{
						try
						{
							f.get();
							NUnit.Framework.Assert.Fail("Future get should not return");
						}
						catch (java.util.concurrent.ExecutionException e)
						{
							NUnit.Framework.Assert.IsTrue("Unexpected exception: " + e, e.InnerException is System.IO.IOException
								);
							LOG.info("Expected exception", e.InnerException);
						}
					}
				}
				finally
				{
					org.apache.hadoop.ipc.RPC.stopProxy(proxy);
					executorService.shutdown();
				}
			}
		}

		private sealed class _Callable_1046 : java.util.concurrent.Callable<java.lang.Void
			>
		{
			public _Callable_1046(org.apache.hadoop.ipc.TestRPC.TestProtocol proxy)
			{
				this.proxy = proxy;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			public java.lang.Void call()
			{
				proxy.sleep(100000);
				return null;
			}

			private readonly org.apache.hadoop.ipc.TestRPC.TestProtocol proxy;
		}

		/// <exception cref="System.IO.IOException"/>
		public static void Main(string[] args)
		{
			new org.apache.hadoop.ipc.TestRPC().testCallsInternal(conf);
		}
	}
}
