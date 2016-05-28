using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Server.Blockmanagement;
using Org.Apache.Hadoop.Hdfs.Server.Datanode;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Test;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode.HA
{
	/// <summary>
	/// Utility class to start an HA cluster, and then start threads
	/// to periodically fail back and forth, accelerate block deletion
	/// processing, etc.
	/// </summary>
	public class HAStressTestHarness
	{
		internal readonly Configuration conf;

		private MiniDFSCluster cluster;

		internal const int BlockSize = 1024;

		internal readonly MultithreadedTestUtil.TestContext testCtx = new MultithreadedTestUtil.TestContext
			();

		public HAStressTestHarness()
		{
			conf = new Configuration();
			conf.SetInt(DFSConfigKeys.DfsBlockSizeKey, BlockSize);
			conf.SetInt(DFSConfigKeys.DfsHeartbeatIntervalKey, 1);
			conf.SetInt(DFSConfigKeys.DfsHaTaileditsPeriodKey, 1);
			// Increase max streams so that we re-replicate quickly.
			conf.SetInt(DFSConfigKeys.DfsNamenodeReplicationMaxStreamsKey, 16);
			conf.SetInt(DFSConfigKeys.DfsNamenodeReplicationStreamsHardLimitKey, 16);
		}

		/// <summary>Start and return the MiniDFSCluster.</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual MiniDFSCluster StartCluster()
		{
			cluster = new MiniDFSCluster.Builder(conf).NnTopology(MiniDFSNNTopology.SimpleHATopology
				()).NumDataNodes(3).Build();
			return cluster;
		}

		/// <summary>
		/// Return a filesystem with client-failover configured for the
		/// cluster.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Sharpen.URISyntaxException"/>
		public virtual FileSystem GetFailoverFs()
		{
			return HATestUtil.ConfigureFailoverFs(cluster, conf);
		}

		/// <summary>
		/// Add a thread which periodically triggers deletion reports,
		/// heartbeats, and NN-side block work.
		/// </summary>
		/// <param name="interval">millisecond period on which to run</param>
		public virtual void AddReplicationTriggerThread(int interval)
		{
			testCtx.AddThread(new _RepeatingTestThread_83(this, interval, testCtx));
		}

		private sealed class _RepeatingTestThread_83 : MultithreadedTestUtil.RepeatingTestThread
		{
			public _RepeatingTestThread_83(HAStressTestHarness _enclosing, int interval, MultithreadedTestUtil.TestContext
				 baseArg1)
				: base(baseArg1)
			{
				this._enclosing = _enclosing;
				this.interval = interval;
			}

			/// <exception cref="System.Exception"/>
			public override void DoAnAction()
			{
				foreach (DataNode dn in this._enclosing.cluster.GetDataNodes())
				{
					DataNodeTestUtils.TriggerDeletionReport(dn);
					DataNodeTestUtils.TriggerHeartbeat(dn);
				}
				for (int i = 0; i < 2; i++)
				{
					NameNode nn = this._enclosing.cluster.GetNameNode(i);
					BlockManagerTestUtil.ComputeAllPendingWork(nn.GetNamesystem().GetBlockManager());
				}
				Sharpen.Thread.Sleep(interval);
			}

			private readonly HAStressTestHarness _enclosing;

			private readonly int interval;
		}

		/// <summary>
		/// Add a thread which periodically triggers failover back and forth between
		/// the two namenodes.
		/// </summary>
		public virtual void AddFailoverThread(int msBetweenFailovers)
		{
			testCtx.AddThread(new _RepeatingTestThread_106(this, msBetweenFailovers, testCtx)
				);
		}

		private sealed class _RepeatingTestThread_106 : MultithreadedTestUtil.RepeatingTestThread
		{
			public _RepeatingTestThread_106(HAStressTestHarness _enclosing, int msBetweenFailovers
				, MultithreadedTestUtil.TestContext baseArg1)
				: base(baseArg1)
			{
				this._enclosing = _enclosing;
				this.msBetweenFailovers = msBetweenFailovers;
			}

			/// <exception cref="System.Exception"/>
			public override void DoAnAction()
			{
				System.Console.Error.WriteLine("==============================\n" + "Failing over from 0->1\n"
					 + "==================================");
				this._enclosing.cluster.TransitionToStandby(0);
				this._enclosing.cluster.TransitionToActive(1);
				Sharpen.Thread.Sleep(msBetweenFailovers);
				System.Console.Error.WriteLine("==============================\n" + "Failing over from 1->0\n"
					 + "==================================");
				this._enclosing.cluster.TransitionToStandby(1);
				this._enclosing.cluster.TransitionToActive(0);
				Sharpen.Thread.Sleep(msBetweenFailovers);
			}

			private readonly HAStressTestHarness _enclosing;

			private readonly int msBetweenFailovers;
		}

		/// <summary>Start all of the threads which have been added.</summary>
		public virtual void StartThreads()
		{
			this.testCtx.StartThreads();
		}

		/// <summary>Stop threads, propagating any exceptions that might have been thrown.</summary>
		/// <exception cref="System.Exception"/>
		public virtual void StopThreads()
		{
			this.testCtx.Stop();
		}

		/// <summary>Shutdown the minicluster, as well as any of the running threads.</summary>
		/// <exception cref="System.Exception"/>
		public virtual void Shutdown()
		{
			this.testCtx.Stop();
			if (cluster != null)
			{
				this.cluster.Shutdown();
				cluster = null;
			}
		}
	}
}
