using System;
using Com.Google.Common.Base;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.HA;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Hdfs.Tools;
using Org.Apache.Hadoop.Test;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode.HA
{
	public class TestDFSZKFailoverController : ClientBaseWithFixes
	{
		private Configuration conf;

		private MiniDFSCluster cluster;

		private MultithreadedTestUtil.TestContext ctx;

		private TestDFSZKFailoverController.ZKFCThread thr1;

		private TestDFSZKFailoverController.ZKFCThread thr2;

		private FileSystem fs;

		static TestDFSZKFailoverController()
		{
			// Make tests run faster by avoiding fsync()
			EditLogFileOutputStream.SetShouldSkipFsyncForTesting(true);
		}

		/// <exception cref="System.Exception"/>
		[SetUp]
		public virtual void Setup()
		{
			conf = new Configuration();
			// Specify the quorum per-nameservice, to ensure that these configs
			// can be nameservice-scoped.
			conf.Set(ZKFailoverController.ZkQuorumKey + ".ns1", hostPort);
			conf.Set(DFSConfigKeys.DfsHaFenceMethodsKey, typeof(TestNodeFencer.AlwaysSucceedFencer
				).FullName);
			conf.SetBoolean(DFSConfigKeys.DfsHaAutoFailoverEnabledKey, true);
			// Turn off IPC client caching, so that the suite can handle
			// the restart of the daemons between test cases.
			conf.SetInt(CommonConfigurationKeysPublic.IpcClientConnectionMaxidletimeKey, 0);
			conf.SetInt(DFSConfigKeys.DfsHaZkfcPortKey + ".ns1.nn1", 10023);
			conf.SetInt(DFSConfigKeys.DfsHaZkfcPortKey + ".ns1.nn2", 10024);
			MiniDFSNNTopology topology = new MiniDFSNNTopology().AddNameservice(new MiniDFSNNTopology.NSConf
				("ns1").AddNN(new MiniDFSNNTopology.NNConf("nn1").SetIpcPort(10021)).AddNN(new MiniDFSNNTopology.NNConf
				("nn2").SetIpcPort(10022)));
			cluster = new MiniDFSCluster.Builder(conf).NnTopology(topology).NumDataNodes(0).Build
				();
			cluster.WaitActive();
			ctx = new MultithreadedTestUtil.TestContext();
			ctx.AddThread(thr1 = new TestDFSZKFailoverController.ZKFCThread(this, ctx, 0));
			NUnit.Framework.Assert.AreEqual(0, thr1.zkfc.Run(new string[] { "-formatZK" }));
			thr1.Start();
			WaitForHAState(0, HAServiceProtocol.HAServiceState.Active);
			ctx.AddThread(thr2 = new TestDFSZKFailoverController.ZKFCThread(this, ctx, 1));
			thr2.Start();
			// Wait for the ZKFCs to fully start up
			ZKFCTestUtil.WaitForHealthState(thr1.zkfc, HealthMonitor.State.ServiceHealthy, ctx
				);
			ZKFCTestUtil.WaitForHealthState(thr2.zkfc, HealthMonitor.State.ServiceHealthy, ctx
				);
			fs = HATestUtil.ConfigureFailoverFs(cluster, conf);
		}

		/// <exception cref="System.Exception"/>
		[TearDown]
		public virtual void Shutdown()
		{
			cluster.Shutdown();
			if (thr1 != null)
			{
				thr1.Interrupt();
			}
			if (thr2 != null)
			{
				thr2.Interrupt();
			}
			if (ctx != null)
			{
				ctx.Stop();
			}
		}

		/// <summary>
		/// Test that automatic failover is triggered by shutting the
		/// active NN down.
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestFailoverAndBackOnNNShutdown()
		{
			Path p1 = new Path("/dir1");
			Path p2 = new Path("/dir2");
			// Write some data on the first NN
			fs.Mkdirs(p1);
			// Shut it down, causing automatic failover
			cluster.ShutdownNameNode(0);
			// Data should still exist. Write some on the new NN
			NUnit.Framework.Assert.IsTrue(fs.Exists(p1));
			fs.Mkdirs(p2);
			NUnit.Framework.Assert.AreEqual(TestNodeFencer.AlwaysSucceedFencer.GetLastFencedService
				().GetAddress(), thr1.zkfc.GetLocalTarget().GetAddress());
			// Start the first node back up
			cluster.RestartNameNode(0);
			// This should have no effect -- the new node should be STANDBY.
			WaitForHAState(0, HAServiceProtocol.HAServiceState.Standby);
			NUnit.Framework.Assert.IsTrue(fs.Exists(p1));
			NUnit.Framework.Assert.IsTrue(fs.Exists(p2));
			// Shut down the second node, which should failback to the first
			cluster.ShutdownNameNode(1);
			WaitForHAState(0, HAServiceProtocol.HAServiceState.Active);
			// First node should see what was written on the second node while it was down.
			NUnit.Framework.Assert.IsTrue(fs.Exists(p1));
			NUnit.Framework.Assert.IsTrue(fs.Exists(p2));
			NUnit.Framework.Assert.AreEqual(TestNodeFencer.AlwaysSucceedFencer.GetLastFencedService
				().GetAddress(), thr2.zkfc.GetLocalTarget().GetAddress());
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestManualFailover()
		{
			thr2.zkfc.GetLocalTarget().GetZKFCProxy(conf, 15000).GracefulFailover();
			WaitForHAState(0, HAServiceProtocol.HAServiceState.Standby);
			WaitForHAState(1, HAServiceProtocol.HAServiceState.Active);
			thr1.zkfc.GetLocalTarget().GetZKFCProxy(conf, 15000).GracefulFailover();
			WaitForHAState(0, HAServiceProtocol.HAServiceState.Active);
			WaitForHAState(1, HAServiceProtocol.HAServiceState.Standby);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestManualFailoverWithDFSHAAdmin()
		{
			DFSHAAdmin tool = new DFSHAAdmin();
			tool.SetConf(conf);
			NUnit.Framework.Assert.AreEqual(0, tool.Run(new string[] { "-failover", "nn1", "nn2"
				 }));
			WaitForHAState(0, HAServiceProtocol.HAServiceState.Standby);
			WaitForHAState(1, HAServiceProtocol.HAServiceState.Active);
			NUnit.Framework.Assert.AreEqual(0, tool.Run(new string[] { "-failover", "nn2", "nn1"
				 }));
			WaitForHAState(0, HAServiceProtocol.HAServiceState.Active);
			WaitForHAState(1, HAServiceProtocol.HAServiceState.Standby);
		}

		/// <exception cref="Sharpen.TimeoutException"/>
		/// <exception cref="System.Exception"/>
		private void WaitForHAState(int nnidx, HAServiceProtocol.HAServiceState state)
		{
			NameNode nn = cluster.GetNameNode(nnidx);
			GenericTestUtils.WaitFor(new _Supplier_190(nn, state), 50, 15000);
		}

		private sealed class _Supplier_190 : Supplier<bool>
		{
			public _Supplier_190(NameNode nn, HAServiceProtocol.HAServiceState state)
			{
				this.nn = nn;
				this.state = state;
			}

			public bool Get()
			{
				try
				{
					return nn.GetRpcServer().GetServiceStatus().GetState() == state;
				}
				catch (Exception e)
				{
					Sharpen.Runtime.PrintStackTrace(e);
					return false;
				}
			}

			private readonly NameNode nn;

			private readonly HAServiceProtocol.HAServiceState state;
		}

		/// <summary>
		/// Test-thread which runs a ZK Failover Controller corresponding
		/// to a given NameNode in the minicluster.
		/// </summary>
		private class ZKFCThread : MultithreadedTestUtil.TestingThread
		{
			private readonly DFSZKFailoverController zkfc;

			public ZKFCThread(TestDFSZKFailoverController _enclosing, MultithreadedTestUtil.TestContext
				 ctx, int idx)
				: base(ctx)
			{
				this._enclosing = _enclosing;
				this.zkfc = DFSZKFailoverController.Create(this._enclosing.cluster.GetConfiguration
					(idx));
			}

			/// <exception cref="System.Exception"/>
			public override void DoWork()
			{
				try
				{
					NUnit.Framework.Assert.AreEqual(0, this.zkfc.Run(new string[0]));
				}
				catch (Exception)
				{
				}
			}

			private readonly TestDFSZKFailoverController _enclosing;
			// Interrupted by main thread, that's OK.
		}
	}
}
