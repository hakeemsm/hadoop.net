using System;
using System.Collections.Generic;
using System.IO;
using Org.Apache.Bookkeeper.Conf;
using Org.Apache.Bookkeeper.Proto;
using Org.Apache.Bookkeeper.Util;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Zookeeper;
using Sharpen;

namespace Org.Apache.Hadoop.Contrib.Bkjournal
{
	/// <summary>
	/// Utility class for setting up bookkeeper ensembles
	/// and bringing individual bookies up and down
	/// </summary>
	internal class BKJMUtil
	{
		protected internal static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Contrib.Bkjournal.BKJMUtil
			));

		internal int nextPort = 6000;

		private Sharpen.Thread bkthread = null;

		private const string zkEnsemble = "127.0.0.1:2181";

		internal int numBookies;

		/// <exception cref="System.Exception"/>
		internal BKJMUtil(int numBookies)
		{
			// next port for additionally created bookies
			this.numBookies = numBookies;
			bkthread = new _Thread_61(numBookies);
		}

		private sealed class _Thread_61 : Sharpen.Thread
		{
			public _Thread_61(int numBookies)
			{
				this.numBookies = numBookies;
			}

			public override void Run()
			{
				try
				{
					string[] args = new string[1];
					args[0] = numBookies.ToString();
					Org.Apache.Hadoop.Contrib.Bkjournal.BKJMUtil.Log.Info("Starting bk");
					LocalBookKeeper.Main(args);
				}
				catch (Exception)
				{
				}
				catch (Exception e)
				{
					// go away quietly
					Org.Apache.Hadoop.Contrib.Bkjournal.BKJMUtil.Log.Error("Error starting local bk", 
						e);
				}
			}

			private readonly int numBookies;
		}

		/// <exception cref="System.Exception"/>
		internal virtual void Start()
		{
			bkthread.Start();
			if (!LocalBookKeeper.WaitForServerUp(zkEnsemble, 10000))
			{
				throw new Exception("Error starting zookeeper/bookkeeper");
			}
			NUnit.Framework.Assert.AreEqual("Not all bookies started", numBookies, CheckBookiesUp
				(numBookies, 10));
		}

		/// <exception cref="System.Exception"/>
		internal virtual void Teardown()
		{
			if (bkthread != null)
			{
				bkthread.Interrupt();
				bkthread.Join();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Zookeeper.KeeperException"/>
		/// <exception cref="System.Exception"/>
		internal static ZooKeeper ConnectZooKeeper()
		{
			CountDownLatch latch = new CountDownLatch(1);
			ZooKeeper zkc = new ZooKeeper(zkEnsemble, 3600, new _Watcher_97(latch));
			if (!latch.Await(3, TimeUnit.Seconds))
			{
				throw new IOException("Zookeeper took too long to connect");
			}
			return zkc;
		}

		private sealed class _Watcher_97 : Watcher
		{
			public _Watcher_97(CountDownLatch latch)
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
		internal static URI CreateJournalURI(string path)
		{
			return URI.Create("bookkeeper://" + zkEnsemble + path);
		}

		internal static void AddJournalManagerDefinition(Configuration conf)
		{
			conf.Set(DFSConfigKeys.DfsNamenodeEditsPluginPrefix + ".bookkeeper", "org.apache.hadoop.contrib.bkjournal.BookKeeperJournalManager"
				);
		}

		/// <exception cref="System.Exception"/>
		internal virtual BookieServer NewBookie()
		{
			int port = nextPort++;
			ServerConfiguration bookieConf = new ServerConfiguration();
			bookieConf.SetBookiePort(port);
			FilePath tmpdir = FilePath.CreateTempFile("bookie" + Sharpen.Extensions.ToString(
				port) + "_", "test");
			tmpdir.Delete();
			tmpdir.Mkdir();
			bookieConf.SetZkServers(zkEnsemble);
			bookieConf.SetJournalDirName(tmpdir.GetPath());
			bookieConf.SetLedgerDirNames(new string[] { tmpdir.GetPath() });
			BookieServer b = new BookieServer(bookieConf);
			b.Start();
			for (int i = 0; i < 10 && !b.IsRunning(); i++)
			{
				Sharpen.Thread.Sleep(10000);
			}
			if (!b.IsRunning())
			{
				throw new IOException("Bookie would not start");
			}
			return b;
		}

		/// <summary>Check that a number of bookies are available</summary>
		/// <param name="count">number of bookies required</param>
		/// <param name="timeout">number of seconds to wait for bookies to start</param>
		/// <exception cref="System.IO.IOException">if bookies are not started by the time the timeout hits
		/// 	</exception>
		/// <exception cref="System.Exception"/>
		internal virtual int CheckBookiesUp(int count, int timeout)
		{
			ZooKeeper zkc = ConnectZooKeeper();
			try
			{
				int mostRecentSize = 0;
				for (int i = 0; i < timeout; i++)
				{
					try
					{
						IList<string> children = zkc.GetChildren("/ledgers/available", false);
						mostRecentSize = children.Count;
						// Skip 'readonly znode' which is used for keeping R-O bookie details
						if (children.Contains("readonly"))
						{
							mostRecentSize = children.Count - 1;
						}
						if (Log.IsDebugEnabled())
						{
							Log.Debug("Found " + mostRecentSize + " bookies up, " + "waiting for " + count);
							if (Log.IsTraceEnabled())
							{
								foreach (string child in children)
								{
									Log.Trace(" server: " + child);
								}
							}
						}
						if (mostRecentSize == count)
						{
							break;
						}
					}
					catch (KeeperException)
					{
					}
					// ignore
					Sharpen.Thread.Sleep(1000);
				}
				return mostRecentSize;
			}
			finally
			{
				zkc.Close();
			}
		}
	}
}
