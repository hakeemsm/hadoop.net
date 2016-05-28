using System;
using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Base;
using Com.Google.Common.Collect;
using Com.Google.Common.IO;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Hdfs.Util;
using Org.Apache.Hadoop.IO.Compress;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Test;
using Org.Apache.Hadoop.Util;
using Sharpen;
using Sharpen.Management;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode.HA
{
	public class TestStandbyCheckpoints
	{
		private const int NumDirsInLog = 200000;

		protected internal MiniDFSCluster cluster;

		protected internal NameNode nn0;

		protected internal NameNode nn1;

		protected internal FileSystem fs;

		private readonly Random random = new Random();

		protected internal FilePath tmpOivImgDir;

		private static readonly Log Log = LogFactory.GetLog(typeof(TestStandbyCheckpoints
			));

		/// <exception cref="System.Exception"/>
		[SetUp]
		public virtual void SetupCluster()
		{
			Configuration conf = SetupCommonConfig();
			// Dial down the retention of extra edits and checkpoints. This is to
			// help catch regressions of HDFS-4238 (SBN should not purge shared edits)
			conf.SetInt(DFSConfigKeys.DfsNamenodeNumCheckpointsRetainedKey, 1);
			conf.SetInt(DFSConfigKeys.DfsNamenodeNumExtraEditsRetainedKey, 0);
			int retryCount = 0;
			while (true)
			{
				try
				{
					int basePort = 10060 + random.Next(100) * 2;
					MiniDFSNNTopology topology = new MiniDFSNNTopology().AddNameservice(new MiniDFSNNTopology.NSConf
						("ns1").AddNN(new MiniDFSNNTopology.NNConf("nn1").SetHttpPort(basePort)).AddNN(new 
						MiniDFSNNTopology.NNConf("nn2").SetHttpPort(basePort + 1)));
					cluster = new MiniDFSCluster.Builder(conf).NnTopology(topology).NumDataNodes(1).Build
						();
					cluster.WaitActive();
					nn0 = cluster.GetNameNode(0);
					nn1 = cluster.GetNameNode(1);
					fs = HATestUtil.ConfigureFailoverFs(cluster, conf);
					cluster.TransitionToActive(0);
					++retryCount;
					break;
				}
				catch (BindException)
				{
					Log.Info("Set up MiniDFSCluster failed due to port conflicts, retry " + retryCount
						 + " times");
				}
			}
		}

		protected internal virtual Configuration SetupCommonConfig()
		{
			tmpOivImgDir = Files.CreateTempDir();
			Configuration conf = new Configuration();
			conf.SetInt(DFSConfigKeys.DfsNamenodeCheckpointCheckPeriodKey, 1);
			conf.SetInt(DFSConfigKeys.DfsNamenodeCheckpointTxnsKey, 5);
			conf.SetInt(DFSConfigKeys.DfsHaTaileditsPeriodKey, 1);
			conf.Set(DFSConfigKeys.DfsNamenodeLegacyOivImageDirKey, tmpOivImgDir.GetAbsolutePath
				());
			conf.SetBoolean(DFSConfigKeys.DfsImageCompressKey, true);
			conf.Set(DFSConfigKeys.DfsImageCompressionCodecKey, typeof(TestStandbyCheckpoints.SlowCodec
				).GetCanonicalName());
			CompressionCodecFactory.SetCodecClasses(conf, ImmutableList.Of<Type>(typeof(TestStandbyCheckpoints.SlowCodec
				)));
			return conf;
		}

		/// <exception cref="System.IO.IOException"/>
		[TearDown]
		public virtual void ShutdownCluster()
		{
			if (cluster != null)
			{
				cluster.Shutdown();
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestSBNCheckpoints()
		{
			JournalSet standbyJournalSet = NameNodeAdapter.SpyOnJournalSet(nn1);
			DoEdits(0, 10);
			HATestUtil.WaitForStandbyToCatchUp(nn0, nn1);
			// Once the standby catches up, it should notice that it needs to
			// do a checkpoint and save one to its local directories.
			HATestUtil.WaitForCheckpoint(cluster, 1, ImmutableList.Of(12));
			GenericTestUtils.WaitFor(new _Supplier_147(this), 1000, 60000);
			// It should have saved the oiv image too.
			NUnit.Framework.Assert.AreEqual("One file is expected", 1, tmpOivImgDir.List().Length
				);
			// It should also upload it back to the active.
			HATestUtil.WaitForCheckpoint(cluster, 0, ImmutableList.Of(12));
			// The standby should never try to purge edit logs on shared storage.
			Org.Mockito.Mockito.Verify(standbyJournalSet, Org.Mockito.Mockito.Never()).PurgeLogsOlderThan
				(Org.Mockito.Mockito.AnyLong());
		}

		private sealed class _Supplier_147 : Supplier<bool>
		{
			public _Supplier_147(TestStandbyCheckpoints _enclosing)
			{
				this._enclosing = _enclosing;
			}

			public bool Get()
			{
				if (this._enclosing.tmpOivImgDir.List().Length > 0)
				{
					return true;
				}
				else
				{
					return false;
				}
			}

			private readonly TestStandbyCheckpoints _enclosing;
		}

		/// <summary>
		/// Test for the case when both of the NNs in the cluster are
		/// in the standby state, and thus are both creating checkpoints
		/// and uploading them to each other.
		/// </summary>
		/// <remarks>
		/// Test for the case when both of the NNs in the cluster are
		/// in the standby state, and thus are both creating checkpoints
		/// and uploading them to each other.
		/// In this circumstance, they should receive the error from the
		/// other node indicating that the other node already has a
		/// checkpoint for the given txid, but this should not cause
		/// an abort, etc.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		public virtual void TestBothNodesInStandbyState()
		{
			DoEdits(0, 10);
			cluster.TransitionToStandby(0);
			// Transitioning to standby closed the edit log on the active,
			// so the standby will catch up. Then, both will be in standby mode
			// with enough uncheckpointed txns to cause a checkpoint, and they
			// will each try to take a checkpoint and upload to each other.
			HATestUtil.WaitForCheckpoint(cluster, 1, ImmutableList.Of(12));
			HATestUtil.WaitForCheckpoint(cluster, 0, ImmutableList.Of(12));
			NUnit.Framework.Assert.AreEqual(12, nn0.GetNamesystem().GetFSImage().GetMostRecentCheckpointTxId
				());
			NUnit.Framework.Assert.AreEqual(12, nn1.GetNamesystem().GetFSImage().GetMostRecentCheckpointTxId
				());
			IList<FilePath> dirs = Lists.NewArrayList();
			Sharpen.Collections.AddAll(dirs, FSImageTestUtil.GetNameNodeCurrentDirs(cluster, 
				0));
			Sharpen.Collections.AddAll(dirs, FSImageTestUtil.GetNameNodeCurrentDirs(cluster, 
				1));
			FSImageTestUtil.AssertParallelFilesAreIdentical(dirs, ImmutableSet.Of<string>());
		}

		/// <summary>
		/// Test for the case when the SBN is configured to checkpoint based
		/// on a time period, but no transactions are happening on the
		/// active.
		/// </summary>
		/// <remarks>
		/// Test for the case when the SBN is configured to checkpoint based
		/// on a time period, but no transactions are happening on the
		/// active. Thus, it would want to save a second checkpoint at the
		/// same txid, which is a no-op. This test makes sure this doesn't
		/// cause any problem.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		public virtual void TestCheckpointWhenNoNewTransactionsHappened()
		{
			// Checkpoint as fast as we can, in a tight loop.
			cluster.GetConfiguration(1).SetInt(DFSConfigKeys.DfsNamenodeCheckpointPeriodKey, 
				0);
			cluster.RestartNameNode(1);
			nn1 = cluster.GetNameNode(1);
			FSImage spyImage1 = NameNodeAdapter.SpyOnFsImage(nn1);
			// We shouldn't save any checkpoints at txid=0
			Sharpen.Thread.Sleep(1000);
			Org.Mockito.Mockito.Verify(spyImage1, Org.Mockito.Mockito.Never()).SaveNamespace(
				(FSNamesystem)Org.Mockito.Mockito.AnyObject());
			// Roll the primary and wait for the standby to catch up
			HATestUtil.WaitForStandbyToCatchUp(nn0, nn1);
			Sharpen.Thread.Sleep(2000);
			// We should make exactly one checkpoint at this new txid. 
			Org.Mockito.Mockito.Verify(spyImage1, Org.Mockito.Mockito.Times(1)).SaveNamespace
				((FSNamesystem)Org.Mockito.Mockito.AnyObject(), Org.Mockito.Mockito.Eq(NNStorage.NameNodeFile
				.Image), (Canceler)Org.Mockito.Mockito.AnyObject());
		}

		/// <summary>
		/// Test cancellation of ongoing checkpoints when failover happens
		/// mid-checkpoint.
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestCheckpointCancellation()
		{
			cluster.TransitionToStandby(0);
			// Create an edit log in the shared edits dir with a lot
			// of mkdirs operations. This is solely so that the image is
			// large enough to take a non-trivial amount of time to load.
			// (only ~15MB)
			URI sharedUri = cluster.GetSharedEditsDir(0, 1);
			FilePath sharedDir = new FilePath(sharedUri.GetPath(), "current");
			FilePath tmpDir = new FilePath(MiniDFSCluster.GetBaseDirectory(), "testCheckpointCancellation-tmp"
				);
			FSNamesystem fsn = cluster.GetNamesystem(0);
			FSImageTestUtil.CreateAbortedLogWithMkdirs(tmpDir, NumDirsInLog, 3, fsn.GetFSDirectory
				().GetLastInodeId() + 1);
			string fname = NNStorage.GetInProgressEditsFileName(3);
			new FilePath(tmpDir, fname).RenameTo(new FilePath(sharedDir, fname));
			// Checkpoint as fast as we can, in a tight loop.
			cluster.GetConfiguration(1).SetInt(DFSConfigKeys.DfsNamenodeCheckpointPeriodKey, 
				0);
			cluster.RestartNameNode(1);
			nn1 = cluster.GetNameNode(1);
			cluster.TransitionToActive(0);
			bool canceledOne = false;
			for (int i = 0; i < 10 && !canceledOne; i++)
			{
				DoEdits(i * 10, i * 10 + 10);
				cluster.TransitionToStandby(0);
				cluster.TransitionToActive(1);
				cluster.TransitionToStandby(1);
				cluster.TransitionToActive(0);
				canceledOne = StandbyCheckpointer.GetCanceledCount() > 0;
			}
			NUnit.Framework.Assert.IsTrue(canceledOne);
		}

		/// <summary>
		/// Test cancellation of ongoing checkpoints when failover happens
		/// mid-checkpoint during image upload from standby to active NN.
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestCheckpointCancellationDuringUpload()
		{
			// don't compress, we want a big image
			cluster.GetConfiguration(0).SetBoolean(DFSConfigKeys.DfsImageCompressKey, false);
			cluster.GetConfiguration(1).SetBoolean(DFSConfigKeys.DfsImageCompressKey, false);
			// Throttle SBN upload to make it hang during upload to ANN
			cluster.GetConfiguration(1).SetLong(DFSConfigKeys.DfsImageTransferRateKey, 100);
			cluster.RestartNameNode(0);
			cluster.RestartNameNode(1);
			nn0 = cluster.GetNameNode(0);
			nn1 = cluster.GetNameNode(1);
			cluster.TransitionToActive(0);
			DoEdits(0, 100);
			HATestUtil.WaitForStandbyToCatchUp(nn0, nn1);
			HATestUtil.WaitForCheckpoint(cluster, 1, ImmutableList.Of(104));
			cluster.TransitionToStandby(0);
			cluster.TransitionToActive(1);
			// Wait to make sure background TransferFsImageUpload thread was cancelled.
			// This needs to be done before the next test in the suite starts, so that a
			// file descriptor is not held open during the next cluster init.
			cluster.Shutdown();
			cluster = null;
			GenericTestUtils.WaitFor(new _Supplier_312(), 1000, 30000);
			// Assert that former active did not accept the canceled checkpoint file.
			NUnit.Framework.Assert.AreEqual(0, nn0.GetFSImage().GetMostRecentCheckpointTxId()
				);
		}

		private sealed class _Supplier_312 : Supplier<bool>
		{
			public _Supplier_312()
			{
			}

			public bool Get()
			{
				ThreadMXBean threadBean = ManagementFactory.GetThreadMXBean();
				ThreadInfo[] threads = threadBean.GetThreadInfo(threadBean.GetAllThreadIds(), 1);
				foreach (ThreadInfo thread in threads)
				{
					if (thread.GetThreadName().StartsWith("TransferFsImageUpload"))
					{
						return false;
					}
				}
				return true;
			}
		}

		/// <summary>
		/// Make sure that clients will receive StandbyExceptions even when a
		/// checkpoint is in progress on the SBN, and therefore the StandbyCheckpointer
		/// thread will have FSNS lock.
		/// </summary>
		/// <remarks>
		/// Make sure that clients will receive StandbyExceptions even when a
		/// checkpoint is in progress on the SBN, and therefore the StandbyCheckpointer
		/// thread will have FSNS lock. Regression test for HDFS-4591.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		public virtual void TestStandbyExceptionThrownDuringCheckpoint()
		{
			// Set it up so that we know when the SBN checkpoint starts and ends.
			FSImage spyImage1 = NameNodeAdapter.SpyOnFsImage(nn1);
			GenericTestUtils.DelayAnswer answerer = new GenericTestUtils.DelayAnswer(Log);
			Org.Mockito.Mockito.DoAnswer(answerer).When(spyImage1).SaveNamespace(Org.Mockito.Mockito
				.Any<FSNamesystem>(), Org.Mockito.Mockito.Eq(NNStorage.NameNodeFile.Image), Org.Mockito.Mockito
				.Any<Canceler>());
			// Perform some edits and wait for a checkpoint to start on the SBN.
			DoEdits(0, 1000);
			nn0.GetRpcServer().RollEditLog();
			answerer.WaitForCall();
			NUnit.Framework.Assert.IsTrue("SBN is not performing checkpoint but it should be."
				, answerer.GetFireCount() == 1 && answerer.GetResultCount() == 0);
			// Make sure that the lock has actually been taken by the checkpointing
			// thread.
			ThreadUtil.SleepAtLeastIgnoreInterrupts(1000);
			try
			{
				// Perform an RPC to the SBN and make sure it throws a StandbyException.
				nn1.GetRpcServer().GetFileInfo("/");
				NUnit.Framework.Assert.Fail("Should have thrown StandbyException, but instead succeeded."
					);
			}
			catch (StandbyException se)
			{
				GenericTestUtils.AssertExceptionContains("is not supported", se);
			}
			// Make sure new incremental block reports are processed during
			// checkpointing on the SBN.
			NUnit.Framework.Assert.AreEqual(0, cluster.GetNamesystem(1).GetPendingDataNodeMessageCount
				());
			DoCreate();
			Sharpen.Thread.Sleep(1000);
			NUnit.Framework.Assert.IsTrue(cluster.GetNamesystem(1).GetPendingDataNodeMessageCount
				() > 0);
			// Make sure that the checkpoint is still going on, implying that the client
			// RPC to the SBN happened during the checkpoint.
			NUnit.Framework.Assert.IsTrue("SBN should have still been checkpointing.", answerer
				.GetFireCount() == 1 && answerer.GetResultCount() == 0);
			answerer.Proceed();
			answerer.WaitForResult();
			NUnit.Framework.Assert.IsTrue("SBN should have finished checkpointing.", answerer
				.GetFireCount() == 1 && answerer.GetResultCount() == 1);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestReadsAllowedDuringCheckpoint()
		{
			// Set it up so that we know when the SBN checkpoint starts and ends.
			FSImage spyImage1 = NameNodeAdapter.SpyOnFsImage(nn1);
			GenericTestUtils.DelayAnswer answerer = new GenericTestUtils.DelayAnswer(Log);
			Org.Mockito.Mockito.DoAnswer(answerer).When(spyImage1).SaveNamespace(Org.Mockito.Mockito
				.Any<FSNamesystem>(), Org.Mockito.Mockito.Any<NNStorage.NameNodeFile>(), Org.Mockito.Mockito
				.Any<Canceler>());
			// Perform some edits and wait for a checkpoint to start on the SBN.
			DoEdits(0, 1000);
			nn0.GetRpcServer().RollEditLog();
			answerer.WaitForCall();
			NUnit.Framework.Assert.IsTrue("SBN is not performing checkpoint but it should be."
				, answerer.GetFireCount() == 1 && answerer.GetResultCount() == 0);
			// Make sure that the lock has actually been taken by the checkpointing
			// thread.
			ThreadUtil.SleepAtLeastIgnoreInterrupts(1000);
			// Perform an RPC that needs to take the write lock.
			Sharpen.Thread t = new _Thread_404(this);
			t.Start();
			// Make sure that our thread is waiting for the lock.
			ThreadUtil.SleepAtLeastIgnoreInterrupts(1000);
			NUnit.Framework.Assert.IsFalse(nn1.GetNamesystem().GetFsLockForTests().HasQueuedThreads
				());
			NUnit.Framework.Assert.IsFalse(nn1.GetNamesystem().GetFsLockForTests().IsWriteLocked
				());
			NUnit.Framework.Assert.IsTrue(nn1.GetNamesystem().GetCpLockForTests().HasQueuedThreads
				());
			// Get /jmx of the standby NN web UI, which will cause the FSNS read lock to
			// be taken.
			string pageContents = DFSTestUtil.UrlGet(new Uri("http://" + nn1.GetHttpAddress()
				.GetHostName() + ":" + nn1.GetHttpAddress().Port + "/jmx"));
			NUnit.Framework.Assert.IsTrue(pageContents.Contains("NumLiveDataNodes"));
			// Make sure that the checkpoint is still going on, implying that the client
			// RPC to the SBN happened during the checkpoint.
			NUnit.Framework.Assert.IsTrue("SBN should have still been checkpointing.", answerer
				.GetFireCount() == 1 && answerer.GetResultCount() == 0);
			answerer.Proceed();
			answerer.WaitForResult();
			NUnit.Framework.Assert.IsTrue("SBN should have finished checkpointing.", answerer
				.GetFireCount() == 1 && answerer.GetResultCount() == 1);
			t.Join();
		}

		private sealed class _Thread_404 : Sharpen.Thread
		{
			public _Thread_404(TestStandbyCheckpoints _enclosing)
			{
				this._enclosing = _enclosing;
			}

			public override void Run()
			{
				try
				{
					this._enclosing.nn1.GetRpcServer().RestoreFailedStorage("false");
				}
				catch (IOException e)
				{
					Sharpen.Runtime.PrintStackTrace(e);
				}
			}

			private readonly TestStandbyCheckpoints _enclosing;
		}

		/// <exception cref="System.IO.IOException"/>
		private void DoEdits(int start, int stop)
		{
			for (int i = start; i < stop; i++)
			{
				Path p = new Path("/test" + i);
				fs.Mkdirs(p);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void DoCreate()
		{
			Path p = new Path("/testFile");
			fs.Delete(p, false);
			FSDataOutputStream @out = fs.Create(p, (short)1);
			@out.Write(42);
			@out.Close();
		}

		/// <summary>
		/// A codec which just slows down the saving of the image significantly
		/// by sleeping a few milliseconds on every write.
		/// </summary>
		/// <remarks>
		/// A codec which just slows down the saving of the image significantly
		/// by sleeping a few milliseconds on every write. This makes it easy to
		/// catch the standby in the middle of saving a checkpoint.
		/// </remarks>
		public class SlowCodec : GzipCodec
		{
			/// <exception cref="System.IO.IOException"/>
			public override CompressionOutputStream CreateOutputStream(OutputStream @out)
			{
				CompressionOutputStream ret = base.CreateOutputStream(@out);
				CompressionOutputStream spy = Org.Mockito.Mockito.Spy(ret);
				Org.Mockito.Mockito.DoAnswer(new GenericTestUtils.SleepAnswer(5)).When(spy).Write
					(Org.Mockito.Mockito.Any<byte[]>(), Org.Mockito.Mockito.AnyInt(), Org.Mockito.Mockito
					.AnyInt());
				return spy;
			}
		}
	}
}
