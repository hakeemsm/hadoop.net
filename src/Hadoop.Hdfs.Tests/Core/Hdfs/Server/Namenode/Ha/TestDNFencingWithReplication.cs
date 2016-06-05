using System.IO;
using Com.Google.Common.Base;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Commons.Logging.Impl;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Test;
using Org.Apache.Log4j;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode.HA
{
	/// <summary>
	/// Stress-test for potential bugs when replication is changing
	/// on blocks during a failover.
	/// </summary>
	public class TestDNFencingWithReplication
	{
		static TestDNFencingWithReplication()
		{
			((Log4JLogger)FSNamesystem.auditLog).GetLogger().SetLevel(Level.Warn);
			((Log4JLogger)Org.Apache.Hadoop.Ipc.Server.Log).GetLogger().SetLevel(Level.Fatal);
			((Log4JLogger)LogFactory.GetLog("org.apache.hadoop.io.retry.RetryInvocationHandler"
				)).GetLogger().SetLevel(Level.Fatal);
		}

		private const int NumThreads = 20;

		private const long Runtime = 35000;

		private const int BlockSize = 1024;

		private class ReplicationToggler : MultithreadedTestUtil.RepeatingTestThread
		{
			private readonly FileSystem fs;

			private readonly Path path;

			public ReplicationToggler(MultithreadedTestUtil.TestContext ctx, FileSystem fs, Path
				 p)
				: base(ctx)
			{
				// How long should the test try to run for. In practice
				// it runs for ~20-30s longer than this constant due to startup/
				// shutdown time.
				this.fs = fs;
				this.path = p;
			}

			/// <exception cref="System.Exception"/>
			public override void DoAnAction()
			{
				fs.SetReplication(path, (short)1);
				WaitForReplicas(1);
				fs.SetReplication(path, (short)2);
				WaitForReplicas(2);
			}

			/// <exception cref="System.Exception"/>
			private void WaitForReplicas(int replicas)
			{
				try
				{
					GenericTestUtils.WaitFor(new _Supplier_83(this, replicas), 100, 60000);
				}
				catch (TimeoutException)
				{
					throw new IOException("Timed out waiting for " + replicas + " replicas " + "on path "
						 + path);
				}
			}

			private sealed class _Supplier_83 : Supplier<bool>
			{
				public _Supplier_83(ReplicationToggler _enclosing, int replicas)
				{
					this._enclosing = _enclosing;
					this.replicas = replicas;
				}

				public bool Get()
				{
					try
					{
						BlockLocation[] blocks = this._enclosing.fs.GetFileBlockLocations(this._enclosing
							.path, 0, 10);
						NUnit.Framework.Assert.AreEqual(1, blocks.Length);
						return blocks[0].GetHosts().Length == replicas;
					}
					catch (IOException e)
					{
						throw new RuntimeException(e);
					}
				}

				private readonly ReplicationToggler _enclosing;

				private readonly int replicas;
			}

			public override string ToString()
			{
				return "Toggler for " + path;
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestFencingStress()
		{
			HAStressTestHarness harness = new HAStressTestHarness();
			harness.conf.SetInt(DFSConfigKeys.DfsBlockreportIntervalMsecKey, 1000);
			harness.conf.SetInt(DFSConfigKeys.DfsNamenodeHeartbeatRecheckIntervalKey, 1);
			harness.conf.SetInt(DFSConfigKeys.DfsNamenodeReplicationIntervalKey, 1);
			MiniDFSCluster cluster = harness.StartCluster();
			try
			{
				cluster.WaitActive();
				cluster.TransitionToActive(0);
				FileSystem fs = harness.GetFailoverFs();
				MultithreadedTestUtil.TestContext togglers = new MultithreadedTestUtil.TestContext
					();
				for (int i = 0; i < NumThreads; i++)
				{
					Path p = new Path("/test-" + i);
					DFSTestUtil.CreateFile(fs, p, BlockSize * 10, (short)3, (long)i);
					togglers.AddThread(new TestDNFencingWithReplication.ReplicationToggler(togglers, 
						fs, p));
				}
				// Start a separate thread which will make sure that replication
				// happens quickly by triggering deletion reports and replication
				// work calculation frequently.
				harness.AddReplicationTriggerThread(500);
				harness.AddFailoverThread(5000);
				harness.StartThreads();
				togglers.StartThreads();
				togglers.WaitFor(Runtime);
				togglers.Stop();
				harness.StopThreads();
				// CHeck that the files can be read without throwing
				for (int i_1 = 0; i_1 < NumThreads; i_1++)
				{
					Path p = new Path("/test-" + i_1);
					DFSTestUtil.ReadFile(fs, p);
				}
			}
			finally
			{
				System.Console.Error.WriteLine("===========================\n\n\n\n");
				harness.Shutdown();
			}
		}
	}
}
