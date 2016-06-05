using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using Com.Google.Common.Base;
using Com.Google.Common.Collect;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.HA;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Blockmanagement;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Test;
using Org.Apache.Log4j;
using Org.Mockito.Internal.Util.Reflection;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode.HA
{
	/// <summary>Tests that exercise safemode in an HA cluster.</summary>
	public class TestHASafeMode
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(TestHASafeMode));

		private const int BlockSize = 1024;

		private NameNode nn0;

		private NameNode nn1;

		private FileSystem fs;

		private MiniDFSCluster cluster;

		static TestHASafeMode()
		{
			DFSTestUtil.SetNameNodeLogLevel(Level.All);
			GenericTestUtils.SetLogLevel(FSImage.Log, Level.All);
		}

		/// <exception cref="System.Exception"/>
		[SetUp]
		public virtual void SetupCluster()
		{
			Configuration conf = new Configuration();
			conf.SetInt(DFSConfigKeys.DfsBlockSizeKey, BlockSize);
			conf.SetInt(DFSConfigKeys.DfsHeartbeatIntervalKey, 1);
			conf.SetInt(DFSConfigKeys.DfsHaTaileditsPeriodKey, 1);
			cluster = new MiniDFSCluster.Builder(conf).NnTopology(MiniDFSNNTopology.SimpleHATopology
				()).NumDataNodes(3).WaitSafeMode(false).Build();
			cluster.WaitActive();
			nn0 = cluster.GetNameNode(0);
			nn1 = cluster.GetNameNode(1);
			fs = HATestUtil.ConfigureFailoverFs(cluster, conf);
			cluster.TransitionToActive(0);
		}

		[TearDown]
		public virtual void ShutdownCluster()
		{
			if (cluster != null)
			{
				cluster.Shutdown();
			}
		}

		/// <summary>Make sure the client retries when the active NN is in safemode</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestClientRetrySafeMode()
		{
			IDictionary<Path, bool> results = Collections.SynchronizedMap(new Dictionary<Path
				, bool>());
			Path test = new Path("/test");
			// let nn0 enter safemode
			NameNodeAdapter.EnterSafeMode(nn0, false);
			FSNamesystem.SafeModeInfo safeMode = (FSNamesystem.SafeModeInfo)Whitebox.GetInternalState
				(nn0.GetNamesystem(), "safeMode");
			Whitebox.SetInternalState(safeMode, "extension", Sharpen.Extensions.ValueOf(30000
				));
			Log.Info("enter safemode");
			new _Thread_133(this, test, results).Start();
			// make sure the client's call has actually been handled by the active NN
			NUnit.Framework.Assert.IsFalse("The directory should not be created while NN in safemode"
				, fs.Exists(test));
			Sharpen.Thread.Sleep(1000);
			// let nn0 leave safemode
			NameNodeAdapter.LeaveSafeMode(nn0);
			Log.Info("leave safemode");
			lock (this)
			{
				while (!results.Contains(test))
				{
					Sharpen.Runtime.Wait(this);
				}
				NUnit.Framework.Assert.IsTrue(results[test]);
			}
		}

		private sealed class _Thread_133 : Sharpen.Thread
		{
			public _Thread_133(TestHASafeMode _enclosing, Path test, IDictionary<Path, bool> 
				results)
			{
				this._enclosing = _enclosing;
				this.test = test;
				this.results = results;
			}

			public override void Run()
			{
				try
				{
					bool mkdir = this._enclosing.fs.Mkdirs(test);
					TestHASafeMode.Log.Info("mkdir finished, result is " + mkdir);
					lock (this._enclosing)
					{
						results[test] = mkdir;
						Sharpen.Runtime.NotifyAll(this._enclosing);
					}
				}
				catch (Exception e)
				{
					TestHASafeMode.Log.Info("Got Exception while calling mkdir", e);
				}
			}

			private readonly TestHASafeMode _enclosing;

			private readonly Path test;

			private readonly IDictionary<Path, bool> results;
		}

		/// <exception cref="System.IO.IOException"/>
		private void RestartStandby()
		{
			cluster.ShutdownNameNode(1);
			// Set the safemode extension to be lengthy, so that the tests
			// can check the safemode message after the safemode conditions
			// have been achieved, without being racy.
			cluster.GetConfiguration(1).SetInt(DFSConfigKeys.DfsNamenodeSafemodeExtensionKey, 
				30000);
			cluster.GetConfiguration(1).SetInt(DFSConfigKeys.DfsHaTaileditsPeriodKey, 1);
			cluster.RestartNameNode(1);
			nn1 = cluster.GetNameNode(1);
			NUnit.Framework.Assert.AreEqual(nn1.GetNamesystem().GetTransactionsSinceLastLogRoll
				(), 0L);
		}

		/// <summary>Test case for enter safemode in active namenode, when it is already in startup safemode.
		/// 	</summary>
		/// <remarks>
		/// Test case for enter safemode in active namenode, when it is already in startup safemode.
		/// It is a regression test for HDFS-2747.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestEnterSafeModeInANNShouldNotThrowNPE()
		{
			Banner("Restarting active");
			DFSTestUtil.CreateFile(fs, new Path("/test"), 3 * BlockSize, (short)3, 1L);
			RestartActive();
			nn0.GetRpcServer().TransitionToActive(new HAServiceProtocol.StateChangeRequestInfo
				(HAServiceProtocol.RequestSource.RequestByUser));
			FSNamesystem namesystem = nn0.GetNamesystem();
			string status = namesystem.GetSafemode();
			NUnit.Framework.Assert.IsTrue("Bad safemode status: '" + status + "'", status.StartsWith
				("Safe mode is ON."));
			NameNodeAdapter.EnterSafeMode(nn0, false);
			NUnit.Framework.Assert.IsTrue("Failed to enter into safemode in active", namesystem
				.IsInSafeMode());
			NameNodeAdapter.EnterSafeMode(nn0, false);
			NUnit.Framework.Assert.IsTrue("Failed to enter into safemode in active", namesystem
				.IsInSafeMode());
		}

		/// <summary>Test case for enter safemode in standby namenode, when it is already in startup safemode.
		/// 	</summary>
		/// <remarks>
		/// Test case for enter safemode in standby namenode, when it is already in startup safemode.
		/// It is a regression test for HDFS-2747.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestEnterSafeModeInSBNShouldNotThrowNPE()
		{
			Banner("Starting with NN0 active and NN1 standby, creating some blocks");
			DFSTestUtil.CreateFile(fs, new Path("/test"), 3 * BlockSize, (short)3, 1L);
			// Roll edit log so that, when the SBN restarts, it will load
			// the namespace during startup and enter safemode.
			nn0.GetRpcServer().RollEditLog();
			Banner("Creating some blocks that won't be in the edit log");
			DFSTestUtil.CreateFile(fs, new Path("/test2"), 5 * BlockSize, (short)3, 1L);
			Banner("Deleting the original blocks");
			fs.Delete(new Path("/test"), true);
			Banner("Restarting standby");
			RestartStandby();
			FSNamesystem namesystem = nn1.GetNamesystem();
			string status = namesystem.GetSafemode();
			NUnit.Framework.Assert.IsTrue("Bad safemode status: '" + status + "'", status.StartsWith
				("Safe mode is ON."));
			NameNodeAdapter.EnterSafeMode(nn1, false);
			NUnit.Framework.Assert.IsTrue("Failed to enter into safemode in standby", namesystem
				.IsInSafeMode());
			NameNodeAdapter.EnterSafeMode(nn1, false);
			NUnit.Framework.Assert.IsTrue("Failed to enter into safemode in standby", namesystem
				.IsInSafeMode());
		}

		/// <exception cref="System.IO.IOException"/>
		private void RestartActive()
		{
			cluster.ShutdownNameNode(0);
			// Set the safemode extension to be lengthy, so that the tests
			// can check the safemode message after the safemode conditions
			// have been achieved, without being racy.
			cluster.GetConfiguration(0).SetInt(DFSConfigKeys.DfsNamenodeSafemodeExtensionKey, 
				30000);
			cluster.RestartNameNode(0);
			nn0 = cluster.GetNameNode(0);
		}

		/// <summary>
		/// Tests the case where, while a standby is down, more blocks are
		/// added to the namespace, but not rolled.
		/// </summary>
		/// <remarks>
		/// Tests the case where, while a standby is down, more blocks are
		/// added to the namespace, but not rolled. So, when it starts up,
		/// it receives notification about the new blocks during
		/// the safemode extension period.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestBlocksAddedBeforeStandbyRestart()
		{
			Banner("Starting with NN0 active and NN1 standby, creating some blocks");
			DFSTestUtil.CreateFile(fs, new Path("/test"), 3 * BlockSize, (short)3, 1L);
			// Roll edit log so that, when the SBN restarts, it will load
			// the namespace during startup.
			nn0.GetRpcServer().RollEditLog();
			Banner("Creating some blocks that won't be in the edit log");
			DFSTestUtil.CreateFile(fs, new Path("/test2"), 5 * BlockSize, (short)3, 1L);
			Banner("Restarting standby");
			RestartStandby();
			// We expect it not to be stuck in safemode, since those blocks
			// that are already visible to the SBN should be processed
			// in the initial block reports.
			AssertSafeMode(nn1, 3, 3, 3, 0);
			Banner("Waiting for standby to catch up to active namespace");
			HATestUtil.WaitForStandbyToCatchUp(nn0, nn1);
			AssertSafeMode(nn1, 8, 8, 3, 0);
		}

		/// <summary>
		/// Similar to
		/// <see cref="TestBlocksAddedBeforeStandbyRestart()"/>
		/// except that
		/// the new blocks are allocated after the SBN has restarted. So, the
		/// blocks were not present in the original block reports at startup
		/// but are reported separately by blockReceived calls.
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestBlocksAddedWhileInSafeMode()
		{
			Banner("Starting with NN0 active and NN1 standby, creating some blocks");
			DFSTestUtil.CreateFile(fs, new Path("/test"), 3 * BlockSize, (short)3, 1L);
			// Roll edit log so that, when the SBN restarts, it will load
			// the namespace during startup.
			nn0.GetRpcServer().RollEditLog();
			Banner("Restarting standby");
			RestartStandby();
			AssertSafeMode(nn1, 3, 3, 3, 0);
			// Create a few blocks which will send blockReceived calls to the
			// SBN.
			Banner("Creating some blocks while SBN is in safe mode");
			DFSTestUtil.CreateFile(fs, new Path("/test2"), 5 * BlockSize, (short)3, 1L);
			Banner("Waiting for standby to catch up to active namespace");
			HATestUtil.WaitForStandbyToCatchUp(nn0, nn1);
			AssertSafeMode(nn1, 8, 8, 3, 0);
		}

		/// <summary>
		/// Test for the following case proposed by ATM:
		/// 1.
		/// </summary>
		/// <remarks>
		/// Test for the following case proposed by ATM:
		/// 1. Both NNs are up, one is active. There are 100 blocks. Both are
		/// out of safemode.
		/// 2. 10 block deletions get processed by NN1. NN2 enqueues these DN messages
		/// until it next reads from a checkpointed edits file.
		/// 3. NN2 gets restarted. Its queues are lost.
		/// 4. NN2 comes up, reads from all the finalized edits files. Concludes there
		/// should still be 100 blocks.
		/// 5. NN2 receives a block report from all the DNs, which only accounts for
		/// 90 blocks. It doesn't leave safemode.
		/// 6. NN1 dies or is transitioned to standby.
		/// 7. NN2 is transitioned to active. It reads all the edits from NN1. It now
		/// knows there should only be 90 blocks, but it's still in safemode.
		/// 8. NN2 doesn't ever recheck whether it should leave safemode.
		/// This is essentially the inverse of
		/// <see cref="TestBlocksAddedBeforeStandbyRestart()"/>
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestBlocksRemovedBeforeStandbyRestart()
		{
			Banner("Starting with NN0 active and NN1 standby, creating some blocks");
			DFSTestUtil.CreateFile(fs, new Path("/test"), 5 * BlockSize, (short)3, 1L);
			// Roll edit log so that, when the SBN restarts, it will load
			// the namespace during startup.
			nn0.GetRpcServer().RollEditLog();
			// Delete those blocks again, so they won't get reported to the SBN
			// once it starts up
			Banner("Removing the blocks without rolling the edit log");
			fs.Delete(new Path("/test"), true);
			BlockManagerTestUtil.ComputeAllPendingWork(nn0.GetNamesystem().GetBlockManager());
			cluster.TriggerHeartbeats();
			Banner("Restarting standby");
			RestartStandby();
			AssertSafeMode(nn1, 0, 5, 3, 0);
			Banner("Waiting for standby to catch up to active namespace");
			HATestUtil.WaitForStandbyToCatchUp(nn0, nn1);
			AssertSafeMode(nn1, 0, 0, 3, 0);
		}

		/// <summary>
		/// Similar to
		/// <see cref="TestBlocksRemovedBeforeStandbyRestart()"/>
		/// except that
		/// the blocks are removed after the SBN has restarted. So, the
		/// blocks were present in the original block reports at startup
		/// but are deleted separately later by deletion reports.
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestBlocksRemovedWhileInSafeMode()
		{
			Banner("Starting with NN0 active and NN1 standby, creating some blocks");
			DFSTestUtil.CreateFile(fs, new Path("/test"), 10 * BlockSize, (short)3, 1L);
			// Roll edit log so that, when the SBN restarts, it will load
			// the namespace during startup.
			nn0.GetRpcServer().RollEditLog();
			Banner("Restarting standby");
			RestartStandby();
			// It will initially have all of the blocks necessary.
			AssertSafeMode(nn1, 10, 10, 3, 0);
			// Delete those blocks while the SBN is in safe mode.
			// This doesn't affect the SBN, since deletions are not
			// ACKed when due to block removals.
			Banner("Removing the blocks without rolling the edit log");
			fs.Delete(new Path("/test"), true);
			BlockManagerTestUtil.ComputeAllPendingWork(nn0.GetNamesystem().GetBlockManager());
			Banner("Triggering deletions on DNs and Deletion Reports");
			cluster.TriggerHeartbeats();
			HATestUtil.WaitForDNDeletions(cluster);
			cluster.TriggerDeletionReports();
			AssertSafeMode(nn1, 10, 10, 3, 0);
			// When we catch up to active namespace, it will restore back
			// to 0 blocks.
			Banner("Waiting for standby to catch up to active namespace");
			HATestUtil.WaitForStandbyToCatchUp(nn0, nn1);
			AssertSafeMode(nn1, 0, 0, 3, 0);
		}

		/// <summary>
		/// Tests that the standby node properly tracks the number of total
		/// and safe blocks while it is in safe mode.
		/// </summary>
		/// <remarks>
		/// Tests that the standby node properly tracks the number of total
		/// and safe blocks while it is in safe mode. Since safe-mode only
		/// counts completed blocks, append needs to decrement the total
		/// number of blocks and then re-increment when the file is closed
		/// again.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAppendWhileInSafeMode()
		{
			Banner("Starting with NN0 active and NN1 standby, creating some blocks");
			// Make 4.5 blocks so that append() will re-open an existing block
			// instead of just adding a new one
			DFSTestUtil.CreateFile(fs, new Path("/test"), 4 * BlockSize + BlockSize / 2, (short
				)3, 1L);
			// Roll edit log so that, when the SBN restarts, it will load
			// the namespace during startup.
			nn0.GetRpcServer().RollEditLog();
			Banner("Restarting standby");
			RestartStandby();
			// It will initially have all of the blocks necessary.
			AssertSafeMode(nn1, 5, 5, 3, 0);
			// Append to a block while SBN is in safe mode. This should
			// not affect safemode initially, since the DN message
			// will get queued.
			FSDataOutputStream stm = fs.Append(new Path("/test"));
			try
			{
				AssertSafeMode(nn1, 5, 5, 3, 0);
				// if we roll edits now, the SBN should see that it's under construction
				// and change its total count and safe count down by one, since UC
				// blocks are not counted by safe mode.
				HATestUtil.WaitForStandbyToCatchUp(nn0, nn1);
				AssertSafeMode(nn1, 4, 4, 3, 0);
			}
			finally
			{
				IOUtils.CloseStream(stm);
			}
			// Delete those blocks while the SBN is in safe mode.
			// This will not ACK the deletions to the SBN, so it won't
			// notice until we roll the edit log.
			Banner("Removing the blocks without rolling the edit log");
			fs.Delete(new Path("/test"), true);
			BlockManagerTestUtil.ComputeAllPendingWork(nn0.GetNamesystem().GetBlockManager());
			Banner("Triggering deletions on DNs and Deletion Reports");
			cluster.TriggerHeartbeats();
			HATestUtil.WaitForDNDeletions(cluster);
			cluster.TriggerDeletionReports();
			AssertSafeMode(nn1, 4, 4, 3, 0);
			// When we roll the edit log, the deletions will go through.
			Banner("Waiting for standby to catch up to active namespace");
			HATestUtil.WaitForStandbyToCatchUp(nn0, nn1);
			AssertSafeMode(nn1, 0, 0, 3, 0);
		}

		/// <summary>
		/// Regression test for a bug experienced while developing
		/// HDFS-2742.
		/// </summary>
		/// <remarks>
		/// Regression test for a bug experienced while developing
		/// HDFS-2742. The scenario here is:
		/// - image contains some blocks
		/// - edits log contains at least one block addition, followed
		/// by deletion of more blocks than were added.
		/// - When node starts up, some incorrect accounting of block
		/// totals caused an assertion failure.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestBlocksDeletedInEditLog()
		{
			Banner("Starting with NN0 active and NN1 standby, creating some blocks");
			// Make 4 blocks persisted in the image.
			DFSTestUtil.CreateFile(fs, new Path("/test"), 4 * BlockSize, (short)3, 1L);
			NameNodeAdapter.EnterSafeMode(nn0, false);
			NameNodeAdapter.SaveNamespace(nn0);
			NameNodeAdapter.LeaveSafeMode(nn0);
			// OP_ADD for 2 blocks
			DFSTestUtil.CreateFile(fs, new Path("/test2"), 2 * BlockSize, (short)3, 1L);
			// OP_DELETE for 4 blocks
			fs.Delete(new Path("/test"), true);
			RestartActive();
		}

		private static void AssertSafeMode(NameNode nn, int safe, int total, int numNodes
			, int nodeThresh)
		{
			string status = nn.GetNamesystem().GetSafemode();
			if (safe == total)
			{
				NUnit.Framework.Assert.IsTrue("Bad safemode status: '" + status + "'", status.StartsWith
					("Safe mode is ON. The reported blocks " + safe + " has reached the " + "threshold 0.9990 of total blocks "
					 + total + ". The number of " + "live datanodes " + numNodes + " has reached the minimum number "
					 + nodeThresh + ". In safe mode extension. " + "Safe mode will be turned off automatically"
					));
			}
			else
			{
				int additional = total - safe;
				NUnit.Framework.Assert.IsTrue("Bad safemode status: '" + status + "'", status.StartsWith
					("Safe mode is ON. " + "The reported blocks " + safe + " needs additional " + additional
					 + " blocks"));
			}
		}

		/// <summary>
		/// Set up a namesystem with several edits, both deletions and
		/// additions, and failover to a new NN while that NN is in
		/// safemode.
		/// </summary>
		/// <remarks>
		/// Set up a namesystem with several edits, both deletions and
		/// additions, and failover to a new NN while that NN is in
		/// safemode. Ensure that it will exit safemode.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestComplexFailoverIntoSafemode()
		{
			Banner("Starting with NN0 active and NN1 standby, creating some blocks");
			DFSTestUtil.CreateFile(fs, new Path("/test"), 3 * BlockSize, (short)3, 1L);
			// Roll edit log so that, when the SBN restarts, it will load
			// the namespace during startup and enter safemode.
			nn0.GetRpcServer().RollEditLog();
			Banner("Creating some blocks that won't be in the edit log");
			DFSTestUtil.CreateFile(fs, new Path("/test2"), 5 * BlockSize, (short)3, 1L);
			Banner("Deleting the original blocks");
			fs.Delete(new Path("/test"), true);
			Banner("Restarting standby");
			RestartStandby();
			// We expect it to be on its way out of safemode, since all of the blocks
			// from the edit log have been reported.
			AssertSafeMode(nn1, 3, 3, 3, 0);
			// Initiate a failover into it while it's in safemode
			Banner("Initiating a failover into NN1 in safemode");
			NameNodeAdapter.AbortEditLogs(nn0);
			cluster.TransitionToActive(1);
			AssertSafeMode(nn1, 5, 5, 3, 0);
		}

		/// <summary>
		/// Similar to
		/// <see cref="TestBlocksRemovedWhileInSafeMode()"/>
		/// except that
		/// the OP_DELETE edits arrive at the SBN before the block deletion reports.
		/// The tracking of safe blocks needs to properly account for the removal
		/// of the blocks as well as the safe count. This is a regression test for
		/// HDFS-2742.
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestBlocksRemovedWhileInSafeModeEditsArriveFirst()
		{
			Banner("Starting with NN0 active and NN1 standby, creating some blocks");
			DFSTestUtil.CreateFile(fs, new Path("/test"), 10 * BlockSize, (short)3, 1L);
			// Roll edit log so that, when the SBN restarts, it will load
			// the namespace during startup.
			nn0.GetRpcServer().RollEditLog();
			Banner("Restarting standby");
			RestartStandby();
			// It will initially have all of the blocks necessary.
			string status = nn1.GetNamesystem().GetSafemode();
			NUnit.Framework.Assert.IsTrue("Bad safemode status: '" + status + "'", status.StartsWith
				("Safe mode is ON. The reported blocks 10 has reached the threshold " + "0.9990 of total blocks 10. The number of live datanodes 3 has "
				 + "reached the minimum number 0. In safe mode extension. " + "Safe mode will be turned off automatically"
				));
			// Delete those blocks while the SBN is in safe mode.
			// Immediately roll the edit log before the actual deletions are sent
			// to the DNs.
			Banner("Removing the blocks without rolling the edit log");
			fs.Delete(new Path("/test"), true);
			HATestUtil.WaitForStandbyToCatchUp(nn0, nn1);
			// Should see removal of the blocks as well as their contribution to safe block count.
			AssertSafeMode(nn1, 0, 0, 3, 0);
			Banner("Triggering sending deletions to DNs and Deletion Reports");
			BlockManagerTestUtil.ComputeAllPendingWork(nn0.GetNamesystem().GetBlockManager());
			cluster.TriggerHeartbeats();
			HATestUtil.WaitForDNDeletions(cluster);
			cluster.TriggerDeletionReports();
			// No change in assertion status here, but some of the consistency checks
			// in safemode will fire here if we accidentally decrement safe block count
			// below 0.    
			AssertSafeMode(nn1, 0, 0, 3, 0);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestSafeBlockTracking()
		{
			TestSafeBlockTracking(false);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestSafeBlockTracking2()
		{
			TestSafeBlockTracking(true);
		}

		/// <summary>
		/// Test that the number of safe blocks is accounted correctly even when
		/// blocks move between under-construction state and completed state.
		/// </summary>
		/// <remarks>
		/// Test that the number of safe blocks is accounted correctly even when
		/// blocks move between under-construction state and completed state.
		/// If a FINALIZED report arrives at the SBN before the block is marked
		/// COMPLETE, then when we get the OP_CLOSE we need to count it as "safe"
		/// at that point. This is a regression test for HDFS-2742.
		/// </remarks>
		/// <param name="noFirstBlockReport">
		/// If this is set to true, we shutdown NN1 before
		/// closing the writing streams. In this way, when NN1 restarts, all DNs will
		/// first send it incremental block report before the first full block report.
		/// And NN1 will not treat the full block report as the first block report
		/// in BlockManager#processReport.
		/// </param>
		/// <exception cref="System.Exception"/>
		private void TestSafeBlockTracking(bool noFirstBlockReport)
		{
			Banner("Starting with NN0 active and NN1 standby, creating some " + "UC blocks plus some other blocks to force safemode"
				);
			DFSTestUtil.CreateFile(fs, new Path("/other-blocks"), 10 * BlockSize, (short)3, 1L
				);
			IList<FSDataOutputStream> stms = Lists.NewArrayList();
			try
			{
				for (int i = 0; i < 5; i++)
				{
					FSDataOutputStream stm = fs.Create(new Path("/test-uc-" + i));
					stms.AddItem(stm);
					stm.Write(1);
					stm.Hflush();
				}
				// Roll edit log so that, when the SBN restarts, it will load
				// the namespace during startup and enter safemode.
				nn0.GetRpcServer().RollEditLog();
			}
			finally
			{
				if (noFirstBlockReport)
				{
					cluster.ShutdownNameNode(1);
				}
				foreach (FSDataOutputStream stm in stms)
				{
					IOUtils.CloseStream(stm);
				}
			}
			Banner("Restarting SBN");
			RestartStandby();
			AssertSafeMode(nn1, 10, 10, 3, 0);
			Banner("Allowing SBN to catch up");
			HATestUtil.WaitForStandbyToCatchUp(nn0, nn1);
			AssertSafeMode(nn1, 15, 15, 3, 0);
		}

		/// <summary>Regression test for HDFS-2753.</summary>
		/// <remarks>
		/// Regression test for HDFS-2753. In this bug, the following sequence was
		/// observed:
		/// - Some blocks are written to DNs while the SBN was down. This causes
		/// the blockReceived messages to get queued in the BPServiceActor on the
		/// DN.
		/// - When the SBN returns, the DN re-registers with the SBN, and then
		/// flushes its blockReceived queue to the SBN before it sends its
		/// first block report. This caused the first block report to be
		/// incorrect ignored.
		/// - The SBN would become stuck in safemode.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestBlocksAddedWhileStandbyIsDown()
		{
			DFSTestUtil.CreateFile(fs, new Path("/test"), 3 * BlockSize, (short)3, 1L);
			Banner("Stopping standby");
			cluster.ShutdownNameNode(1);
			DFSTestUtil.CreateFile(fs, new Path("/test2"), 3 * BlockSize, (short)3, 1L);
			Banner("Rolling edit log so standby gets all edits on restart");
			nn0.GetRpcServer().RollEditLog();
			RestartStandby();
			AssertSafeMode(nn1, 6, 6, 3, 0);
		}

		/// <summary>
		/// Regression test for HDFS-2804: standby should not populate replication
		/// queues when exiting safe mode.
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNoPopulatingReplQueuesWhenExitingSafemode()
		{
			DFSTestUtil.CreateFile(fs, new Path("/test"), 15 * BlockSize, (short)3, 1L);
			HATestUtil.WaitForStandbyToCatchUp(nn0, nn1);
			// get some blocks in the SBN's image
			nn1.GetRpcServer().SetSafeMode(HdfsConstants.SafeModeAction.SafemodeEnter, false);
			NameNodeAdapter.SaveNamespace(nn1);
			nn1.GetRpcServer().SetSafeMode(HdfsConstants.SafeModeAction.SafemodeLeave, false);
			// and some blocks in the edit logs
			DFSTestUtil.CreateFile(fs, new Path("/test2"), 15 * BlockSize, (short)3, 1L);
			nn0.GetRpcServer().RollEditLog();
			cluster.StopDataNode(1);
			cluster.ShutdownNameNode(1);
			//Configuration sbConf = cluster.getConfiguration(1);
			//sbConf.setInt(DFSConfigKeys.DFS_NAMENODE_SAFEMODE_EXTENSION_KEY, 1);
			cluster.RestartNameNode(1, false);
			nn1 = cluster.GetNameNode(1);
			GenericTestUtils.WaitFor(new _Supplier_708(this), 100, 10000);
			BlockManagerTestUtil.UpdateState(nn1.GetNamesystem().GetBlockManager());
			NUnit.Framework.Assert.AreEqual(0L, nn1.GetNamesystem().GetUnderReplicatedBlocks(
				));
			NUnit.Framework.Assert.AreEqual(0L, nn1.GetNamesystem().GetPendingReplicationBlocks
				());
		}

		private sealed class _Supplier_708 : Supplier<bool>
		{
			public _Supplier_708(TestHASafeMode _enclosing)
			{
				this._enclosing = _enclosing;
			}

			public bool Get()
			{
				return !this._enclosing.nn1.IsInSafeMode();
			}

			private readonly TestHASafeMode _enclosing;
		}

		/// <summary>
		/// Make sure that when we transition to active in safe mode that we don't
		/// prematurely consider blocks missing just because not all DNs have reported
		/// yet.
		/// </summary>
		/// <remarks>
		/// Make sure that when we transition to active in safe mode that we don't
		/// prematurely consider blocks missing just because not all DNs have reported
		/// yet.
		/// This is a regression test for HDFS-3921.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestNoPopulatingReplQueuesWhenStartingActiveInSafeMode()
		{
			DFSTestUtil.CreateFile(fs, new Path("/test"), 15 * BlockSize, (short)3, 1L);
			// Stop the DN so that when the NN restarts not all blocks wil be reported
			// and the NN won't leave safe mode.
			cluster.StopDataNode(1);
			// Restart the namenode but don't wait for it to hear from all DNs (since
			// one DN is deliberately shut down.)
			cluster.RestartNameNode(0, false);
			cluster.TransitionToActive(0);
			NUnit.Framework.Assert.IsTrue(cluster.GetNameNode(0).IsInSafeMode());
			// We shouldn't yet consider any blocks "missing" since we're in startup
			// safemode, i.e. not all DNs may have reported.
			NUnit.Framework.Assert.AreEqual(0, cluster.GetNamesystem(0).GetMissingBlocksCount
				());
		}

		/// <summary>Print a big banner in the test log to make debug easier.</summary>
		internal static void Banner(string @string)
		{
			Log.Info("\n\n\n\n================================================\n" + @string +
				 "\n" + "==================================================\n\n");
		}

		/// <summary>DFS#isInSafeMode should check the ActiveNNs safemode in HA enabled cluster.
		/// 	</summary>
		/// <remarks>DFS#isInSafeMode should check the ActiveNNs safemode in HA enabled cluster. HDFS-3507
		/// 	</remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestIsInSafemode()
		{
			// Check for the standby nn without client failover.
			NameNode nn2 = cluster.GetNameNode(1);
			NUnit.Framework.Assert.IsTrue("nn2 should be in standby state", nn2.IsStandbyState
				());
			IPEndPoint nameNodeAddress = nn2.GetNameNodeAddress();
			Configuration conf = new Configuration();
			DistributedFileSystem dfs = new DistributedFileSystem();
			try
			{
				dfs.Initialize(URI.Create("hdfs://" + nameNodeAddress.GetHostName() + ":" + nameNodeAddress
					.Port), conf);
				dfs.IsInSafeMode();
				NUnit.Framework.Assert.Fail("StandBy should throw exception for isInSafeMode");
			}
			catch (IOException e)
			{
				if (e is RemoteException)
				{
					IOException sbExcpetion = ((RemoteException)e).UnwrapRemoteException();
					NUnit.Framework.Assert.IsTrue("StandBy nn should not support isInSafeMode", sbExcpetion
						 is StandbyException);
				}
				else
				{
					throw;
				}
			}
			finally
			{
				if (null != dfs)
				{
					dfs.Close();
				}
			}
			// Check with Client FailOver
			cluster.TransitionToStandby(0);
			cluster.TransitionToActive(1);
			cluster.GetNameNodeRpc(1).SetSafeMode(HdfsConstants.SafeModeAction.SafemodeEnter, 
				false);
			DistributedFileSystem dfsWithFailOver = (DistributedFileSystem)fs;
			NUnit.Framework.Assert.IsTrue("ANN should be in SafeMode", dfsWithFailOver.IsInSafeMode
				());
			cluster.GetNameNodeRpc(1).SetSafeMode(HdfsConstants.SafeModeAction.SafemodeLeave, 
				false);
			NUnit.Framework.Assert.IsFalse("ANN should be out of SafeMode", dfsWithFailOver.IsInSafeMode
				());
		}

		/// <summary>Test NN crash and client crash/stuck immediately after block allocation</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestOpenFileWhenNNAndClientCrashAfterAddBlock()
		{
			cluster.GetConfiguration(0).Set(DFSConfigKeys.DfsNamenodeSafemodeThresholdPctKey, 
				"1.0f");
			string testData = "testData";
			// to make sure we write the full block before creating dummy block at NN.
			cluster.GetConfiguration(0).SetInt("io.bytes.per.checksum", testData.Length);
			cluster.RestartNameNode(0);
			try
			{
				cluster.WaitActive();
				cluster.TransitionToActive(0);
				cluster.TransitionToStandby(1);
				DistributedFileSystem dfs = cluster.GetFileSystem(0);
				string pathString = "/tmp1.txt";
				Path filePath = new Path(pathString);
				FSDataOutputStream create = dfs.Create(filePath, FsPermission.GetDefault(), true, 
					1024, (short)3, testData.Length, null);
				create.Write(Sharpen.Runtime.GetBytesForString(testData));
				create.Hflush();
				long fileId = ((DFSOutputStream)create.GetWrappedStream()).GetFileId();
				FileStatus fileStatus = dfs.GetFileStatus(filePath);
				DFSClient client = DFSClientAdapter.GetClient(dfs);
				// add one dummy block at NN, but not write to DataNode
				ExtendedBlock previousBlock = DFSClientAdapter.GetPreviousBlock(client, fileId);
				DFSClientAdapter.GetNamenode(client).AddBlock(pathString, client.GetClientName(), 
					new ExtendedBlock(previousBlock), new DatanodeInfo[0], DFSClientAdapter.GetFileId
					((DFSOutputStream)create.GetWrappedStream()), null);
				cluster.RestartNameNode(0, true);
				cluster.RestartDataNode(0);
				cluster.TransitionToActive(0);
				// let the block reports be processed.
				Sharpen.Thread.Sleep(2000);
				FSDataInputStream @is = dfs.Open(filePath);
				@is.Close();
				dfs.RecoverLease(filePath);
				// initiate recovery
				NUnit.Framework.Assert.IsTrue("Recovery also should be success", dfs.RecoverLease
					(filePath));
			}
			finally
			{
				cluster.Shutdown();
			}
		}
	}
}
