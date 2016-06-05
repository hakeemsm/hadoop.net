using System;
using System.IO;
using System.Net;
using NUnit.Framework;
using Org.Apache.Bookkeeper.Util;
using Org.Apache.Commons.Logging;
using Org.Apache.Zookeeper;
using Org.Apache.Zookeeper.Server;
using Sharpen;

namespace Org.Apache.Hadoop.Contrib.Bkjournal
{
	/// <summary>Tests that read, update, clear api from CurrentInprogress</summary>
	public class TestCurrentInprogress
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(TestCurrentInprogress)
			);

		private const string CurrentNodePath = "/test";

		private const string Hostport = "127.0.0.1:2181";

		private const int ConnectionTimeout = 30000;

		private static NIOServerCnxnFactory serverFactory;

		private static ZooKeeperServer zks;

		private static ZooKeeper zkc;

		private static int ZooKeeperDefaultPort = 2181;

		private static FilePath zkTmpDir;

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Zookeeper.KeeperException"/>
		/// <exception cref="System.Exception"/>
		private static ZooKeeper ConnectZooKeeper(string ensemble)
		{
			CountDownLatch latch = new CountDownLatch(1);
			ZooKeeper zkc = new ZooKeeper(Hostport, 3600, new _Watcher_61(latch));
			if (!latch.Await(10, TimeUnit.Seconds))
			{
				throw new IOException("Zookeeper took too long to connect");
			}
			return zkc;
		}

		private sealed class _Watcher_61 : Watcher
		{
			public _Watcher_61(CountDownLatch latch)
			{
				this.latch = latch;
			}

			public override void Process(WatchedEvent @event)
			{
				if (@event.GetState() == Watcher.Event.KeeperState.SyncConnected)
				{
					latch.CountDown();
				}
			}

			private readonly CountDownLatch latch;
		}

		/// <exception cref="System.Exception"/>
		[BeforeClass]
		public static void SetupZooKeeper()
		{
			Log.Info("Starting ZK server");
			zkTmpDir = FilePath.CreateTempFile("zookeeper", "test");
			zkTmpDir.Delete();
			zkTmpDir.Mkdir();
			try
			{
				zks = new ZooKeeperServer(zkTmpDir, zkTmpDir, ZooKeeperDefaultPort);
				serverFactory = new NIOServerCnxnFactory();
				serverFactory.Configure(new IPEndPoint(ZooKeeperDefaultPort), 10);
				serverFactory.Startup(zks);
			}
			catch (Exception e)
			{
				Log.Error("Exception while instantiating ZooKeeper", e);
			}
			bool b = LocalBookKeeper.WaitForServerUp(Hostport, ConnectionTimeout);
			Log.Debug("ZooKeeper server up: " + b);
		}

		[AfterClass]
		public static void ShutDownServer()
		{
			if (null != zks)
			{
				zks.Shutdown();
			}
			zkTmpDir.Delete();
		}

		/// <exception cref="System.Exception"/>
		[SetUp]
		public virtual void Setup()
		{
			zkc = ConnectZooKeeper(Hostport);
		}

		/// <exception cref="System.Exception"/>
		[TearDown]
		public virtual void Teardown()
		{
			if (null != zkc)
			{
				zkc.Close();
			}
		}

		/// <summary>
		/// Tests that read should be able to read the data which updated with update
		/// api
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestReadShouldReturnTheZnodePathAfterUpdate()
		{
			string data = "inprogressNode";
			CurrentInprogress ci = new CurrentInprogress(zkc, CurrentNodePath);
			ci.Init();
			ci.Update(data);
			string inprogressNodePath = ci.Read();
			NUnit.Framework.Assert.AreEqual("Not returning inprogressZnode", "inprogressNode"
				, inprogressNodePath);
		}

		/// <summary>
		/// Tests that read should return null if we clear the updated data in
		/// CurrentInprogress node
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestReadShouldReturnNullAfterClear()
		{
			CurrentInprogress ci = new CurrentInprogress(zkc, CurrentNodePath);
			ci.Init();
			ci.Update("myInprogressZnode");
			ci.Read();
			ci.Clear();
			string inprogressNodePath = ci.Read();
			NUnit.Framework.Assert.AreEqual("Expecting null to be return", null, inprogressNodePath
				);
		}

		/// <summary>
		/// Tests that update should throw IOE, if version number modifies between read
		/// and update
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestUpdateShouldFailWithIOEIfVersionNumberChangedAfterRead()
		{
			CurrentInprogress ci = new CurrentInprogress(zkc, CurrentNodePath);
			ci.Init();
			ci.Update("myInprogressZnode");
			NUnit.Framework.Assert.AreEqual("Not returning myInprogressZnode", "myInprogressZnode"
				, ci.Read());
			// Updating data in-between to change the data to change the version number
			ci.Update("YourInprogressZnode");
			ci.Update("myInprogressZnode");
		}
	}
}
