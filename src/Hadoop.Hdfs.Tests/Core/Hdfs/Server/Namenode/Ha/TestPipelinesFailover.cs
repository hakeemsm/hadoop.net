using System;
using System.IO;
using Com.Google.Common.Base;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.ProtocolPB;
using Org.Apache.Hadoop.Hdfs.Server.Blockmanagement;
using Org.Apache.Hadoop.Hdfs.Server.Datanode;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.IO.Retry;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Test;
using Org.Apache.Log4j;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode.HA
{
	/// <summary>Test cases regarding pipeline recovery during NN failover.</summary>
	public class TestPipelinesFailover
	{
		static TestPipelinesFailover()
		{
			GenericTestUtils.SetLogLevel(LogFactory.GetLog(typeof(RetryInvocationHandler)), Level
				.All);
			DFSTestUtil.SetNameNodeLogLevel(Level.All);
		}

		protected internal static readonly Log Log = LogFactory.GetLog(typeof(TestPipelinesFailover
			));

		private static readonly Path TestPath = new Path("/test-file");

		private const int BlockSize = 4096;

		private const int BlockAndAHalf = BlockSize * 3 / 2;

		private const int StressNumThreads = 25;

		private const int StressRuntime = 40000;

		[System.Serializable]
		internal sealed class TestScenario
		{
			public static readonly TestPipelinesFailover.TestScenario GracefulFailover = new 
				TestPipelinesFailover.TestScenario();

			public static readonly TestPipelinesFailover.TestScenario OriginalActiveCrashed = 
				new TestPipelinesFailover.TestScenario();

			/// <exception cref="System.IO.IOException"/>
			internal abstract void Run(MiniDFSCluster cluster);
		}

		internal enum MethodToTestIdempotence
		{
			AllocateBlock,
			CompleteFile
		}

		/// <summary>Tests continuing a write pipeline over a failover.</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestWriteOverGracefulFailover()
		{
			DoWriteOverFailoverTest(TestPipelinesFailover.TestScenario.GracefulFailover, TestPipelinesFailover.MethodToTestIdempotence
				.AllocateBlock);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestAllocateBlockAfterCrashFailover()
		{
			DoWriteOverFailoverTest(TestPipelinesFailover.TestScenario.OriginalActiveCrashed, 
				TestPipelinesFailover.MethodToTestIdempotence.AllocateBlock);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestCompleteFileAfterCrashFailover()
		{
			DoWriteOverFailoverTest(TestPipelinesFailover.TestScenario.OriginalActiveCrashed, 
				TestPipelinesFailover.MethodToTestIdempotence.CompleteFile);
		}

		/// <exception cref="System.Exception"/>
		private void DoWriteOverFailoverTest(TestPipelinesFailover.TestScenario scenario, 
			TestPipelinesFailover.MethodToTestIdempotence methodToTest)
		{
			Configuration conf = new Configuration();
			conf.SetInt(DFSConfigKeys.DfsBlockSizeKey, BlockSize);
			// Don't check replication periodically.
			conf.SetInt(DFSConfigKeys.DfsNamenodeReplicationIntervalKey, 1000);
			FSDataOutputStream stm = null;
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NnTopology(MiniDFSNNTopology
				.SimpleHATopology()).NumDataNodes(3).Build();
			try
			{
				int sizeWritten = 0;
				cluster.WaitActive();
				cluster.TransitionToActive(0);
				Sharpen.Thread.Sleep(500);
				Log.Info("Starting with NN 0 active");
				FileSystem fs = HATestUtil.ConfigureFailoverFs(cluster, conf);
				stm = fs.Create(TestPath);
				// write a block and a half
				AppendTestUtil.Write(stm, 0, BlockAndAHalf);
				sizeWritten += BlockAndAHalf;
				// Make sure all of the blocks are written out before failover.
				stm.Hflush();
				Log.Info("Failing over to NN 1");
				scenario.Run(cluster);
				// NOTE: explicitly do *not* make any further metadata calls
				// to the NN here. The next IPC call should be to allocate the next
				// block. Any other call would notice the failover and not test
				// idempotence of the operation (HDFS-3031)
				FSNamesystem ns1 = cluster.GetNameNode(1).GetNamesystem();
				BlockManagerTestUtil.UpdateState(ns1.GetBlockManager());
				NUnit.Framework.Assert.AreEqual(0, ns1.GetPendingReplicationBlocks());
				NUnit.Framework.Assert.AreEqual(0, ns1.GetCorruptReplicaBlocks());
				NUnit.Framework.Assert.AreEqual(0, ns1.GetMissingBlocksCount());
				// If we're testing allocateBlock()'s idempotence, write another
				// block and a half, so we have to allocate a new block.
				// Otherise, don't write anything, so our next RPC will be
				// completeFile() if we're testing idempotence of that operation.
				if (methodToTest == TestPipelinesFailover.MethodToTestIdempotence.AllocateBlock)
				{
					// write another block and a half
					AppendTestUtil.Write(stm, sizeWritten, BlockAndAHalf);
					sizeWritten += BlockAndAHalf;
				}
				stm.Close();
				stm = null;
				AppendTestUtil.Check(fs, TestPath, sizeWritten);
			}
			finally
			{
				IOUtils.CloseStream(stm);
				cluster.Shutdown();
			}
		}

		/// <summary>
		/// Tests continuing a write pipeline over a failover when a DN fails
		/// after the failover - ensures that updating the pipeline succeeds
		/// even when the pipeline was constructed on a different NN.
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestWriteOverGracefulFailoverWithDnFail()
		{
			DoTestWriteOverFailoverWithDnFail(TestPipelinesFailover.TestScenario.GracefulFailover
				);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestWriteOverCrashFailoverWithDnFail()
		{
			DoTestWriteOverFailoverWithDnFail(TestPipelinesFailover.TestScenario.OriginalActiveCrashed
				);
		}

		/// <exception cref="System.Exception"/>
		private void DoTestWriteOverFailoverWithDnFail(TestPipelinesFailover.TestScenario
			 scenario)
		{
			Configuration conf = new Configuration();
			conf.SetInt(DFSConfigKeys.DfsBlockSizeKey, BlockSize);
			FSDataOutputStream stm = null;
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NnTopology(MiniDFSNNTopology
				.SimpleHATopology()).NumDataNodes(5).Build();
			try
			{
				cluster.WaitActive();
				cluster.TransitionToActive(0);
				Sharpen.Thread.Sleep(500);
				Log.Info("Starting with NN 0 active");
				FileSystem fs = HATestUtil.ConfigureFailoverFs(cluster, conf);
				stm = fs.Create(TestPath);
				// write a block and a half
				AppendTestUtil.Write(stm, 0, BlockAndAHalf);
				// Make sure all the blocks are written before failover
				stm.Hflush();
				Log.Info("Failing over to NN 1");
				scenario.Run(cluster);
				NUnit.Framework.Assert.IsTrue(fs.Exists(TestPath));
				cluster.StopDataNode(0);
				// write another block and a half
				AppendTestUtil.Write(stm, BlockAndAHalf, BlockAndAHalf);
				stm.Hflush();
				Log.Info("Failing back to NN 0");
				cluster.TransitionToStandby(1);
				cluster.TransitionToActive(0);
				cluster.StopDataNode(1);
				AppendTestUtil.Write(stm, BlockAndAHalf * 2, BlockAndAHalf);
				stm.Hflush();
				stm.Close();
				stm = null;
				AppendTestUtil.Check(fs, TestPath, BlockAndAHalf * 3);
			}
			finally
			{
				IOUtils.CloseStream(stm);
				cluster.Shutdown();
			}
		}

		/// <summary>Tests lease recovery if a client crashes.</summary>
		/// <remarks>
		/// Tests lease recovery if a client crashes. This approximates the
		/// use case of HBase WALs being recovered after a NN failover.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		public virtual void TestLeaseRecoveryAfterFailover()
		{
			Configuration conf = new Configuration();
			// Disable permissions so that another user can recover the lease.
			conf.SetBoolean(DFSConfigKeys.DfsPermissionsEnabledKey, false);
			conf.SetInt(DFSConfigKeys.DfsBlockSizeKey, BlockSize);
			FSDataOutputStream stm = null;
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NnTopology(MiniDFSNNTopology
				.SimpleHATopology()).NumDataNodes(3).Build();
			try
			{
				cluster.WaitActive();
				cluster.TransitionToActive(0);
				Sharpen.Thread.Sleep(500);
				Log.Info("Starting with NN 0 active");
				FileSystem fs = HATestUtil.ConfigureFailoverFs(cluster, conf);
				stm = fs.Create(TestPath);
				// write a block and a half
				AppendTestUtil.Write(stm, 0, BlockAndAHalf);
				stm.Hflush();
				Log.Info("Failing over to NN 1");
				cluster.TransitionToStandby(0);
				cluster.TransitionToActive(1);
				NUnit.Framework.Assert.IsTrue(fs.Exists(TestPath));
				FileSystem fsOtherUser = CreateFsAsOtherUser(cluster, conf);
				LoopRecoverLease(fsOtherUser, TestPath);
				AppendTestUtil.Check(fs, TestPath, BlockAndAHalf);
				// Fail back to ensure that the block locations weren't lost on the
				// original node.
				cluster.TransitionToStandby(1);
				cluster.TransitionToActive(0);
				AppendTestUtil.Check(fs, TestPath, BlockAndAHalf);
			}
			finally
			{
				IOUtils.CloseStream(stm);
				cluster.Shutdown();
			}
		}

		/// <summary>
		/// Test the scenario where the NN fails over after issuing a block
		/// synchronization request, but before it is committed.
		/// </summary>
		/// <remarks>
		/// Test the scenario where the NN fails over after issuing a block
		/// synchronization request, but before it is committed. The
		/// DN running the recovery should then fail to commit the synchronization
		/// and a later retry will succeed.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		public virtual void TestFailoverRightBeforeCommitSynchronization()
		{
			Configuration conf = new Configuration();
			// Disable permissions so that another user can recover the lease.
			conf.SetBoolean(DFSConfigKeys.DfsPermissionsEnabledKey, false);
			conf.SetInt(DFSConfigKeys.DfsBlockSizeKey, BlockSize);
			FSDataOutputStream stm = null;
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NnTopology(MiniDFSNNTopology
				.SimpleHATopology()).NumDataNodes(3).Build();
			try
			{
				cluster.WaitActive();
				cluster.TransitionToActive(0);
				Sharpen.Thread.Sleep(500);
				Log.Info("Starting with NN 0 active");
				FileSystem fs = HATestUtil.ConfigureFailoverFs(cluster, conf);
				stm = fs.Create(TestPath);
				// write a half block
				AppendTestUtil.Write(stm, 0, BlockSize / 2);
				stm.Hflush();
				// Look into the block manager on the active node for the block
				// under construction.
				NameNode nn0 = cluster.GetNameNode(0);
				ExtendedBlock blk = DFSTestUtil.GetFirstBlock(fs, TestPath);
				DatanodeDescriptor expectedPrimary = DFSTestUtil.GetExpectedPrimaryNode(nn0, blk);
				Log.Info("Expecting block recovery to be triggered on DN " + expectedPrimary);
				// Find the corresponding DN daemon, and spy on its connection to the
				// active.
				DataNode primaryDN = cluster.GetDataNode(expectedPrimary.GetIpcPort());
				DatanodeProtocolClientSideTranslatorPB nnSpy = DataNodeTestUtils.SpyOnBposToNN(primaryDN
					, nn0);
				// Delay the commitBlockSynchronization call
				GenericTestUtils.DelayAnswer delayer = new GenericTestUtils.DelayAnswer(Log);
				Org.Mockito.Mockito.DoAnswer(delayer).When(nnSpy).CommitBlockSynchronization(Org.Mockito.Mockito
					.Eq(blk), Org.Mockito.Mockito.AnyInt(), Org.Mockito.Mockito.AnyLong(), Org.Mockito.Mockito
					.Eq(true), Org.Mockito.Mockito.Eq(false), (DatanodeID[])Org.Mockito.Mockito.AnyObject
					(), (string[])Org.Mockito.Mockito.AnyObject());
				// new genstamp
				// new length
				// close file
				// delete block
				// new targets
				// new target storages
				DistributedFileSystem fsOtherUser = CreateFsAsOtherUser(cluster, conf);
				NUnit.Framework.Assert.IsFalse(fsOtherUser.RecoverLease(TestPath));
				Log.Info("Waiting for commitBlockSynchronization call from primary");
				delayer.WaitForCall();
				Log.Info("Failing over to NN 1");
				cluster.TransitionToStandby(0);
				cluster.TransitionToActive(1);
				// Let the commitBlockSynchronization call go through, and check that
				// it failed with the correct exception.
				delayer.Proceed();
				delayer.WaitForResult();
				Exception t = delayer.GetThrown();
				if (t == null)
				{
					NUnit.Framework.Assert.Fail("commitBlockSynchronization call did not fail on standby"
						);
				}
				GenericTestUtils.AssertExceptionContains("Operation category WRITE is not supported"
					, t);
				// Now, if we try again to recover the block, it should succeed on the new
				// active.
				LoopRecoverLease(fsOtherUser, TestPath);
				AppendTestUtil.Check(fs, TestPath, BlockSize / 2);
			}
			finally
			{
				IOUtils.CloseStream(stm);
				cluster.Shutdown();
			}
		}

		/// <summary>Stress test for pipeline/lease recovery.</summary>
		/// <remarks>
		/// Stress test for pipeline/lease recovery. Starts a number of
		/// threads, each of which creates a file and has another client
		/// break the lease. While these threads run, failover proceeds
		/// back and forth between two namenodes.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		public virtual void TestPipelineRecoveryStress()
		{
			HAStressTestHarness harness = new HAStressTestHarness();
			// Disable permissions so that another user can recover the lease.
			harness.conf.SetBoolean(DFSConfigKeys.DfsPermissionsEnabledKey, false);
			// This test triggers rapid NN failovers.  The client retry policy uses an
			// exponential backoff.  This can quickly lead to long sleep times and even
			// timeout the whole test.  Cap the sleep time at 1s to prevent this.
			harness.conf.SetInt(DFSConfigKeys.DfsClientFailoverSleeptimeMaxKey, 1000);
			MiniDFSCluster cluster = harness.StartCluster();
			try
			{
				cluster.WaitActive();
				cluster.TransitionToActive(0);
				FileSystem fs = harness.GetFailoverFs();
				DistributedFileSystem fsAsOtherUser = CreateFsAsOtherUser(cluster, harness.conf);
				MultithreadedTestUtil.TestContext testers = new MultithreadedTestUtil.TestContext
					();
				for (int i = 0; i < StressNumThreads; i++)
				{
					Path p = new Path("/test-" + i);
					testers.AddThread(new TestPipelinesFailover.PipelineTestThread(testers, fs, fsAsOtherUser
						, p));
				}
				// Start a separate thread which will make sure that replication
				// happens quickly by triggering deletion reports and replication
				// work calculation frequently.
				harness.AddReplicationTriggerThread(500);
				harness.AddFailoverThread(5000);
				harness.StartThreads();
				testers.StartThreads();
				testers.WaitFor(StressRuntime);
				testers.Stop();
				harness.StopThreads();
			}
			finally
			{
				System.Console.Error.WriteLine("===========================\n\n\n\n");
				harness.Shutdown();
			}
		}

		/// <summary>
		/// Test thread which creates a file, has another fake user recover
		/// the lease on the file, and then ensures that the file's contents
		/// are properly readable.
		/// </summary>
		/// <remarks>
		/// Test thread which creates a file, has another fake user recover
		/// the lease on the file, and then ensures that the file's contents
		/// are properly readable. If any of these steps fails, propagates
		/// an exception back to the test context, causing the test case
		/// to fail.
		/// </remarks>
		private class PipelineTestThread : MultithreadedTestUtil.RepeatingTestThread
		{
			private readonly FileSystem fs;

			private readonly FileSystem fsOtherUser;

			private readonly Path path;

			public PipelineTestThread(MultithreadedTestUtil.TestContext ctx, FileSystem fs, FileSystem
				 fsOtherUser, Path p)
				: base(ctx)
			{
				this.fs = fs;
				this.fsOtherUser = fsOtherUser;
				this.path = p;
			}

			/// <exception cref="System.Exception"/>
			public override void DoAnAction()
			{
				FSDataOutputStream stm = fs.Create(path, true);
				try
				{
					AppendTestUtil.Write(stm, 0, 100);
					stm.Hflush();
					LoopRecoverLease(fsOtherUser, path);
					AppendTestUtil.Check(fs, path, 100);
				}
				finally
				{
					try
					{
						stm.Close();
					}
					catch (IOException)
					{
					}
				}
			}

			// should expect this since we lost the lease
			public override string ToString()
			{
				return "Pipeline test thread for " + path;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		private DistributedFileSystem CreateFsAsOtherUser(MiniDFSCluster cluster, Configuration
			 conf)
		{
			return (DistributedFileSystem)UserGroupInformation.CreateUserForTesting("otheruser"
				, new string[] { "othergroup" }).DoAs(new _PrivilegedExceptionAction_508(cluster
				, conf));
		}

		private sealed class _PrivilegedExceptionAction_508 : PrivilegedExceptionAction<FileSystem
			>
		{
			public _PrivilegedExceptionAction_508(MiniDFSCluster cluster, Configuration conf)
			{
				this.cluster = cluster;
				this.conf = conf;
			}

			/// <exception cref="System.Exception"/>
			public FileSystem Run()
			{
				return HATestUtil.ConfigureFailoverFs(cluster, conf);
			}

			private readonly MiniDFSCluster cluster;

			private readonly Configuration conf;
		}

		/// <summary>Try to recover the lease on the given file for up to 60 seconds.</summary>
		/// <param name="fsOtherUser">the filesystem to use for the recoverLease call</param>
		/// <param name="testPath">the path on which to run lease recovery</param>
		/// <exception cref="Sharpen.TimeoutException">
		/// if lease recover does not succeed within 60
		/// seconds
		/// </exception>
		/// <exception cref="System.Exception">if the thread is interrupted</exception>
		private static void LoopRecoverLease(FileSystem fsOtherUser, Path testPath)
		{
			try
			{
				GenericTestUtils.WaitFor(new _Supplier_529(fsOtherUser, testPath), 1000, 60000);
			}
			catch (TimeoutException)
			{
				throw new TimeoutException("Timed out recovering lease for " + testPath);
			}
		}

		private sealed class _Supplier_529 : Supplier<bool>
		{
			public _Supplier_529(FileSystem fsOtherUser, Path testPath)
			{
				this.fsOtherUser = fsOtherUser;
				this.testPath = testPath;
			}

			public bool Get()
			{
				bool success;
				try
				{
					success = ((DistributedFileSystem)fsOtherUser).RecoverLease(testPath);
				}
				catch (IOException e)
				{
					throw new RuntimeException(e);
				}
				if (!success)
				{
					TestPipelinesFailover.Log.Info("Waiting to recover lease successfully");
				}
				return success;
			}

			private readonly FileSystem fsOtherUser;

			private readonly Path testPath;
		}
	}
}
