using System;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.HA;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Zookeeper;
using Org.Apache.Zookeeper.Server.Auth;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Recovery
{
	public class TestZKRMStateStoreZKClientConnections : ClientBaseWithFixes
	{
		private const int ZkOpWaitTime = 3000;

		private const int ZkTimeoutMs = 1000;

		private Log Log = LogFactory.GetLog(typeof(TestZKRMStateStoreZKClientConnections)
			);

		private const string DigestUserPass = "test-user:test-password";

		private const string TestAuthGood = "digest:" + DigestUserPass;

		private static readonly string DigestUserHash;

		static TestZKRMStateStoreZKClientConnections()
		{
			try
			{
				DigestUserHash = DigestAuthenticationProvider.GenerateDigest(DigestUserPass);
			}
			catch (NoSuchAlgorithmException e)
			{
				throw new RuntimeException(e);
			}
		}

		private static readonly string TestAcl = "digest:" + DigestUserHash + ":rwcda";

		internal class TestZKClient
		{
			internal ZKRMStateStore store;

			internal bool forExpire = false;

			internal TestZKRMStateStoreZKClientConnections.TestZKClient.TestForwardingWatcher
				 oldWatcher;

			internal TestZKRMStateStoreZKClientConnections.TestZKClient.TestForwardingWatcher
				 watcher;

			internal CyclicBarrier syncBarrier = new CyclicBarrier(2);

			protected internal class TestZKRMStateStore : ZKRMStateStore
			{
				/// <exception cref="System.Exception"/>
				public TestZKRMStateStore(TestZKClient _enclosing, Configuration conf, string workingZnode
					)
				{
					this._enclosing = _enclosing;
					this.Init(conf);
					this.Start();
					NUnit.Framework.Assert.IsTrue(this.znodeWorkingPath.Equals(workingZnode));
				}

				/// <exception cref="System.IO.IOException"/>
				/// <exception cref="System.Exception"/>
				protected internal override ZooKeeper GetNewZooKeeper()
				{
					this._enclosing.oldWatcher = this._enclosing.watcher;
					this._enclosing.watcher = new TestZKRMStateStoreZKClientConnections.TestZKClient.TestForwardingWatcher
						(this);
					return this._enclosing._enclosing.CreateClient(this._enclosing.watcher, this._enclosing
						._enclosing.hostPort, TestZKRMStateStoreZKClientConnections.ZkTimeoutMs);
				}

				/// <exception cref="System.Exception"/>
				public override void ProcessWatchEvent(ZooKeeper zk, WatchedEvent @event)
				{
					lock (this)
					{
						if (this._enclosing.forExpire)
						{
							// a hack... couldn't find a way to trigger expired event.
							WatchedEvent expriredEvent = new WatchedEvent(Watcher.Event.EventType.None, Watcher.Event.KeeperState
								.Expired, null);
							base.ProcessWatchEvent(zk, expriredEvent);
							this._enclosing.forExpire = false;
							this._enclosing.syncBarrier.Await();
						}
						else
						{
							base.ProcessWatchEvent(zk, @event);
						}
					}
				}

				private readonly TestZKClient _enclosing;
			}

			private class TestForwardingWatcher : ClientBaseWithFixes.CountdownWatcher
			{
				public override void Process(WatchedEvent @event)
				{
					base.Process(@event);
					try
					{
						if (this._enclosing.store != null)
						{
							this._enclosing.store.ProcessWatchEvent(this.client, @event);
						}
					}
					catch (Exception t)
					{
						this._enclosing._enclosing.Log.Error("Failed to process watcher event " + @event 
							+ ": " + StringUtils.StringifyException(t));
					}
				}

				internal TestForwardingWatcher(TestZKClient _enclosing)
				{
					this._enclosing = _enclosing;
				}

				private readonly TestZKClient _enclosing;
			}

			/// <exception cref="System.Exception"/>
			public virtual RMStateStore GetRMStateStore(Configuration conf)
			{
				string workingZnode = "/Test";
				conf.Set(YarnConfiguration.RmZkAddress, this._enclosing.hostPort);
				conf.Set(YarnConfiguration.ZkRmStateStoreParentPath, workingZnode);
				this.store = new TestZKRMStateStoreZKClientConnections.TestZKClient.TestZKRMStateStore
					(this, conf, workingZnode);
				return this.store;
			}

			internal TestZKClient(TestZKRMStateStoreZKClientConnections _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly TestZKRMStateStoreZKClientConnections _enclosing;
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestZKClientRetry()
		{
			TestZKRMStateStoreZKClientConnections.TestZKClient zkClientTester = new TestZKRMStateStoreZKClientConnections.TestZKClient
				(this);
			string path = "/test";
			YarnConfiguration conf = new YarnConfiguration();
			conf.SetInt(YarnConfiguration.RmZkTimeoutMs, ZkTimeoutMs);
			conf.SetLong(YarnConfiguration.RmZkRetryIntervalMs, 100);
			ZKRMStateStore store = (ZKRMStateStore)zkClientTester.GetRMStateStore(conf);
			RMStateStoreTestBase.TestDispatcher dispatcher = new RMStateStoreTestBase.TestDispatcher
				();
			store.SetRMDispatcher(dispatcher);
			AtomicBoolean assertionFailedInThread = new AtomicBoolean(false);
			StopServer();
			Sharpen.Thread clientThread = new _Thread_151(store, path, assertionFailedInThread
				);
			Sharpen.Thread.Sleep(2000);
			StartServer();
			clientThread.Join();
			NUnit.Framework.Assert.IsFalse(assertionFailedInThread.Get());
		}

		private sealed class _Thread_151 : Sharpen.Thread
		{
			public _Thread_151(ZKRMStateStore store, string path, AtomicBoolean assertionFailedInThread
				)
			{
				this.store = store;
				this.path = path;
				this.assertionFailedInThread = assertionFailedInThread;
			}

			public override void Run()
			{
				try
				{
					store.GetDataWithRetries(path, true);
				}
				catch (Exception e)
				{
					Sharpen.Runtime.PrintStackTrace(e);
					assertionFailedInThread.Set(true);
				}
			}

			private readonly ZKRMStateStore store;

			private readonly string path;

			private readonly AtomicBoolean assertionFailedInThread;
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestZKClientDisconnectAndReconnect()
		{
			TestZKRMStateStoreZKClientConnections.TestZKClient zkClientTester = new TestZKRMStateStoreZKClientConnections.TestZKClient
				(this);
			string path = "/test";
			YarnConfiguration conf = new YarnConfiguration();
			conf.SetInt(YarnConfiguration.RmZkTimeoutMs, ZkTimeoutMs);
			ZKRMStateStore store = (ZKRMStateStore)zkClientTester.GetRMStateStore(conf);
			RMStateStoreTestBase.TestDispatcher dispatcher = new RMStateStoreTestBase.TestDispatcher
				();
			store.SetRMDispatcher(dispatcher);
			// trigger watch
			store.CreateWithRetries(path, null, ZooDefs.Ids.OpenAclUnsafe, CreateMode.Persistent
				);
			store.GetDataWithRetries(path, true);
			store.SetDataWithRetries(path, Sharpen.Runtime.GetBytesForString("newBytes"), 0);
			StopServer();
			AtomicBoolean isSucceeded = new AtomicBoolean(false);
			zkClientTester.watcher.WaitForDisconnected(ZkOpWaitTime);
			Sharpen.Thread thread = new _Thread_190(store, path, isSucceeded);
			thread.Start();
			// ZKRMStateStore Session restored
			StartServer();
			zkClientTester.watcher.WaitForConnected(ZkOpWaitTime);
			byte[] ret = null;
			try
			{
				ret = store.GetDataWithRetries(path, true);
			}
			catch (Exception e)
			{
				string error = "ZKRMStateStore Session restore failed";
				Log.Error(error, e);
				NUnit.Framework.Assert.Fail(error);
			}
			NUnit.Framework.Assert.AreEqual("newBytes", Sharpen.Runtime.GetStringForBytes(ret
				));
			thread.Join();
			NUnit.Framework.Assert.IsTrue(isSucceeded.Get());
		}

		private sealed class _Thread_190 : Sharpen.Thread
		{
			public _Thread_190(ZKRMStateStore store, string path, AtomicBoolean isSucceeded)
			{
				this.store = store;
				this.path = path;
				this.isSucceeded = isSucceeded;
			}

			public override void Run()
			{
				try
				{
					store.GetDataWithRetries(path, true);
					isSucceeded.Set(true);
				}
				catch (Exception)
				{
					isSucceeded.Set(false);
				}
			}

			private readonly ZKRMStateStore store;

			private readonly string path;

			private readonly AtomicBoolean isSucceeded;
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestZKSessionTimeout()
		{
			TestZKRMStateStoreZKClientConnections.TestZKClient zkClientTester = new TestZKRMStateStoreZKClientConnections.TestZKClient
				(this);
			string path = "/test";
			YarnConfiguration conf = new YarnConfiguration();
			conf.SetInt(YarnConfiguration.RmZkTimeoutMs, ZkTimeoutMs);
			ZKRMStateStore store = (ZKRMStateStore)zkClientTester.GetRMStateStore(conf);
			RMStateStoreTestBase.TestDispatcher dispatcher = new RMStateStoreTestBase.TestDispatcher
				();
			store.SetRMDispatcher(dispatcher);
			// a hack to trigger expired event
			zkClientTester.forExpire = true;
			// trigger watch
			store.CreateWithRetries(path, null, ZooDefs.Ids.OpenAclUnsafe, CreateMode.Persistent
				);
			store.GetDataWithRetries(path, true);
			store.SetDataWithRetries(path, Sharpen.Runtime.GetBytesForString("bytes"), 0);
			zkClientTester.syncBarrier.Await();
			// after this point, expired event has already been processed.
			try
			{
				byte[] ret = store.GetDataWithRetries(path, false);
				NUnit.Framework.Assert.AreEqual("bytes", Sharpen.Runtime.GetStringForBytes(ret));
			}
			catch (Exception e)
			{
				string error = "New session creation failed";
				Log.Error(error, e);
				NUnit.Framework.Assert.Fail(error);
			}
			// send Disconnected event from old client session to ZKRMStateStore
			// check the current client session is not affected.
			NUnit.Framework.Assert.IsTrue(zkClientTester.oldWatcher != null);
			WatchedEvent disconnectedEvent = new WatchedEvent(Watcher.Event.EventType.None, Watcher.Event.KeeperState
				.Disconnected, null);
			zkClientTester.oldWatcher.Process(disconnectedEvent);
			NUnit.Framework.Assert.IsTrue(store.zkClient != null);
			zkClientTester.watcher.Process(disconnectedEvent);
			NUnit.Framework.Assert.IsTrue(store.zkClient == null);
			WatchedEvent connectedEvent = new WatchedEvent(Watcher.Event.EventType.None, Watcher.Event.KeeperState
				.SyncConnected, null);
			zkClientTester.watcher.Process(connectedEvent);
			NUnit.Framework.Assert.IsTrue(store.zkClient != null);
			NUnit.Framework.Assert.IsTrue(store.zkClient == store.activeZkClient);
		}

		public virtual void TestSetZKAcl()
		{
			TestZKRMStateStoreZKClientConnections.TestZKClient zkClientTester = new TestZKRMStateStoreZKClientConnections.TestZKClient
				(this);
			YarnConfiguration conf = new YarnConfiguration();
			conf.Set(YarnConfiguration.RmZkAcl, "world:anyone:rwca");
			try
			{
				zkClientTester.store.zkClient.Delete(zkClientTester.store.znodeWorkingPath, -1);
				NUnit.Framework.Assert.Fail("Shouldn't be able to delete path");
			}
			catch (Exception)
			{
			}
		}

		/* expected behavior */
		public virtual void TestInvalidZKAclConfiguration()
		{
			TestZKRMStateStoreZKClientConnections.TestZKClient zkClientTester = new TestZKRMStateStoreZKClientConnections.TestZKClient
				(this);
			YarnConfiguration conf = new YarnConfiguration();
			conf.Set(YarnConfiguration.RmZkAcl, "randomstring&*");
			try
			{
				zkClientTester.GetRMStateStore(conf);
				NUnit.Framework.Assert.Fail("ZKRMStateStore created with bad ACL");
			}
			catch (ZKUtil.BadAclFormatException)
			{
			}
			catch (Exception e)
			{
				// expected behavior
				string error = "Incorrect exception on BadAclFormat";
				Log.Error(error, e);
				NUnit.Framework.Assert.Fail(error);
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestZKAuths()
		{
			TestZKRMStateStoreZKClientConnections.TestZKClient zkClientTester = new TestZKRMStateStoreZKClientConnections.TestZKClient
				(this);
			YarnConfiguration conf = new YarnConfiguration();
			conf.SetInt(YarnConfiguration.RmZkNumRetries, 1);
			conf.SetInt(YarnConfiguration.RmZkTimeoutMs, ZkTimeoutMs);
			conf.Set(YarnConfiguration.RmZkAcl, TestAcl);
			conf.Set(YarnConfiguration.RmZkAuth, TestAuthGood);
			zkClientTester.GetRMStateStore(conf);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestZKRetryInterval()
		{
			TestZKRMStateStoreZKClientConnections.TestZKClient zkClientTester = new TestZKRMStateStoreZKClientConnections.TestZKClient
				(this);
			YarnConfiguration conf = new YarnConfiguration();
			ZKRMStateStore store = (ZKRMStateStore)zkClientTester.GetRMStateStore(conf);
			NUnit.Framework.Assert.AreEqual(YarnConfiguration.DefaultRmZkRetryIntervalMs, store
				.zkRetryInterval);
			store.Stop();
			conf.SetBoolean(YarnConfiguration.RmHaEnabled, true);
			store = (ZKRMStateStore)zkClientTester.GetRMStateStore(conf);
			NUnit.Framework.Assert.AreEqual(YarnConfiguration.DefaultRmZkTimeoutMs / YarnConfiguration
				.DefaultZkRmNumRetries, store.zkRetryInterval);
			store.Stop();
		}
	}
}
