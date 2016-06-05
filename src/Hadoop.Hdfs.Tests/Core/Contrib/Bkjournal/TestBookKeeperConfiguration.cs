using System;
using System.IO;
using System.Net;
using NUnit.Framework;
using Org.Apache.Bookkeeper.Util;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Org.Apache.Zookeeper;
using Org.Apache.Zookeeper.Server;
using Sharpen;

namespace Org.Apache.Hadoop.Contrib.Bkjournal
{
	public class TestBookKeeperConfiguration
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(TestBookKeeperConfiguration
			));

		private const int ZkSessionTimeout = 5000;

		private const string Hostport = "127.0.0.1:2181";

		private const int ConnectionTimeout = 30000;

		private static NIOServerCnxnFactory serverFactory;

		private static ZooKeeperServer zks;

		private static ZooKeeper zkc;

		private static int ZooKeeperDefaultPort = 2181;

		private static FilePath ZkTmpDir;

		private BookKeeperJournalManager bkjm;

		private const string BkRootPath = "/ledgers";

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Zookeeper.KeeperException"/>
		/// <exception cref="System.Exception"/>
		private static ZooKeeper ConnectZooKeeper(string ensemble)
		{
			CountDownLatch latch = new CountDownLatch(1);
			ZooKeeper zkc = new ZooKeeper(Hostport, ZkSessionTimeout, new _Watcher_66(latch));
			if (!latch.Await(ZkSessionTimeout, TimeUnit.Milliseconds))
			{
				throw new IOException("Zookeeper took too long to connect");
			}
			return zkc;
		}

		private sealed class _Watcher_66 : Watcher
		{
			public _Watcher_66(CountDownLatch latch)
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

		private NamespaceInfo NewNSInfo()
		{
			Random r = new Random();
			return new NamespaceInfo(r.Next(), "testCluster", "TestBPID", -1);
		}

		/// <exception cref="System.Exception"/>
		[BeforeClass]
		public static void SetupZooKeeper()
		{
			// create a ZooKeeper server(dataDir, dataLogDir, port)
			Log.Info("Starting ZK server");
			ZkTmpDir = FilePath.CreateTempFile("zookeeper", "test");
			ZkTmpDir.Delete();
			ZkTmpDir.Mkdir();
			try
			{
				zks = new ZooKeeperServer(ZkTmpDir, ZkTmpDir, ZooKeeperDefaultPort);
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

		/// <exception cref="System.Exception"/>
		[SetUp]
		public virtual void Setup()
		{
			zkc = ConnectZooKeeper(Hostport);
			try
			{
				ZKUtil.DeleteRecursive(zkc, BkRootPath);
			}
			catch (KeeperException.NoNodeException e)
			{
				Log.Debug("Ignoring no node exception on cleanup", e);
			}
			catch (Exception e)
			{
				Log.Error("Exception when deleting bookie root path in zk", e);
			}
		}

		/// <exception cref="System.Exception"/>
		[TearDown]
		public virtual void Teardown()
		{
			if (null != zkc)
			{
				zkc.Close();
			}
			if (null != bkjm)
			{
				bkjm.Close();
			}
		}

		/// <exception cref="System.Exception"/>
		[AfterClass]
		public static void TeardownZooKeeper()
		{
			if (null != zkc)
			{
				zkc.Close();
			}
		}

		/// <summary>
		/// Verify the BKJM is creating the bookie available path configured in
		/// 'dfs.namenode.bookkeeperjournal.zk.availablebookies'
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestWithConfiguringBKAvailablePath()
		{
			// set Bookie available path in the configuration
			string bkAvailablePath = BookKeeperJournalManager.BkjmZkLedgersAvailablePathDefault;
			Configuration conf = new Configuration();
			conf.SetStrings(BookKeeperJournalManager.BkjmZkLedgersAvailablePath, bkAvailablePath
				);
			NUnit.Framework.Assert.IsNull(bkAvailablePath + " already exists", zkc.Exists(bkAvailablePath
				, false));
			NamespaceInfo nsi = NewNSInfo();
			bkjm = new BookKeeperJournalManager(conf, URI.Create("bookkeeper://" + Hostport +
				 "/hdfsjournal-WithBKPath"), nsi);
			bkjm.Format(nsi);
			NUnit.Framework.Assert.IsNotNull("Bookie available path : " + bkAvailablePath + " doesn't exists"
				, zkc.Exists(bkAvailablePath, false));
		}

		/// <summary>
		/// Verify the BKJM is creating the bookie available default path, when there
		/// is no 'dfs.namenode.bookkeeperjournal.zk.availablebookies' configured
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestDefaultBKAvailablePath()
		{
			Configuration conf = new Configuration();
			NUnit.Framework.Assert.IsNull(BkRootPath + " already exists", zkc.Exists(BkRootPath
				, false));
			NamespaceInfo nsi = NewNSInfo();
			bkjm = new BookKeeperJournalManager(conf, URI.Create("bookkeeper://" + Hostport +
				 "/hdfsjournal-DefaultBKPath"), nsi);
			bkjm.Format(nsi);
			NUnit.Framework.Assert.IsNotNull("Bookie available path : " + BkRootPath + " doesn't exists"
				, zkc.Exists(BkRootPath, false));
		}
	}
}
