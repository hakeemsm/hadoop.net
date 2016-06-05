using System;
using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Util.Concurrent;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Commons.Logging.Impl;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.HA;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Security.Token.Delegation;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Test;
using Org.Apache.Log4j;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode.HA
{
	/// <summary>
	/// Tests state transition from active-&gt;standby, and manual failover
	/// and failback between two namenodes.
	/// </summary>
	public class TestHAStateTransitions
	{
		protected internal static readonly Log Log = LogFactory.GetLog(typeof(TestStandbyIsHot
			));

		private static readonly Path TestDir = new Path("/test");

		private static readonly Path TestFilePath = new Path(TestDir, "foo");

		private static readonly string TestFileStr = TestFilePath.ToUri().GetPath();

		private const string TestFileData = "Hello state transitioning world";

		private static readonly HAServiceProtocol.StateChangeRequestInfo ReqInfo = new HAServiceProtocol.StateChangeRequestInfo
			(HAServiceProtocol.RequestSource.RequestByUserForced);

		static TestHAStateTransitions()
		{
			((Log4JLogger)EditLogTailer.Log).GetLogger().SetLevel(Level.All);
		}

		/// <summary>
		/// Test which takes a single node and flip flops between
		/// active and standby mode, making sure it doesn't
		/// double-play any edits.
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestTransitionActiveToStandby()
		{
			Configuration conf = new Configuration();
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NnTopology(MiniDFSNNTopology
				.SimpleHATopology()).NumDataNodes(1).Build();
			try
			{
				cluster.WaitActive();
				cluster.TransitionToActive(0);
				FileSystem fs = cluster.GetFileSystem(0);
				fs.Mkdirs(TestDir);
				cluster.TransitionToStandby(0);
				try
				{
					fs.Mkdirs(new Path("/x"));
					NUnit.Framework.Assert.Fail("Didn't throw trying to mutate FS in standby state");
				}
				catch (Exception t)
				{
					GenericTestUtils.AssertExceptionContains("Operation category WRITE is not supported"
						, t);
				}
				cluster.TransitionToActive(0);
				// Create a file, then delete the whole directory recursively.
				DFSTestUtil.CreateFile(fs, new Path(TestDir, "foo"), 10, (short)1, 1L);
				fs.Delete(TestDir, true);
				// Now if the standby tries to replay the last segment that it just
				// wrote as active, it would fail since it's trying to create a file
				// in a non-existent directory.
				cluster.TransitionToStandby(0);
				cluster.TransitionToActive(0);
				NUnit.Framework.Assert.IsFalse(fs.Exists(TestDir));
			}
			finally
			{
				cluster.Shutdown();
			}
		}

		private void AddCrmThreads(MiniDFSCluster cluster, List<Sharpen.Thread> crmThreads
			)
		{
			for (int nn = 0; nn <= 1; nn++)
			{
				Sharpen.Thread thread = cluster.GetNameNode(nn).GetNamesystem().GetCacheManager()
					.GetCacheReplicationMonitor();
				if (thread != null)
				{
					crmThreads.AddItem(thread);
				}
			}
		}

		/// <summary>
		/// Test that transitioning a service to the state that it is already
		/// in is a nop, specifically, an exception is not thrown.
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestTransitionToCurrentStateIsANop()
		{
			Configuration conf = new Configuration();
			conf.SetLong(DFSConfigKeys.DfsNamenodePathBasedCacheRefreshIntervalMs, 1L);
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NnTopology(MiniDFSNNTopology
				.SimpleHATopology()).NumDataNodes(1).Build();
			List<Sharpen.Thread> crmThreads = new List<Sharpen.Thread>();
			try
			{
				cluster.WaitActive();
				AddCrmThreads(cluster, crmThreads);
				cluster.TransitionToActive(0);
				AddCrmThreads(cluster, crmThreads);
				cluster.TransitionToActive(0);
				AddCrmThreads(cluster, crmThreads);
				cluster.TransitionToStandby(0);
				AddCrmThreads(cluster, crmThreads);
				cluster.TransitionToStandby(0);
				AddCrmThreads(cluster, crmThreads);
			}
			finally
			{
				cluster.Shutdown();
			}
			// Verify that all cacheReplicationMonitor threads shut down
			foreach (Sharpen.Thread thread in crmThreads)
			{
				Uninterruptibles.JoinUninterruptibly(thread);
			}
		}

		/// <summary>Test manual failover failback for one namespace</summary>
		/// <param name="cluster">single process test cluster</param>
		/// <param name="conf">cluster configuration</param>
		/// <param name="nsIndex">namespace index starting from zero</param>
		/// <exception cref="System.Exception"/>
		private void TestManualFailoverFailback(MiniDFSCluster cluster, Configuration conf
			, int nsIndex)
		{
			int nn0 = 2 * nsIndex;
			int nn1 = 2 * nsIndex + 1;
			cluster.TransitionToActive(nn0);
			Log.Info("Starting with NN 0 active in namespace " + nsIndex);
			FileSystem fs = HATestUtil.ConfigureFailoverFs(cluster, conf);
			fs.Mkdirs(TestDir);
			Log.Info("Failing over to NN 1 in namespace " + nsIndex);
			cluster.TransitionToStandby(nn0);
			cluster.TransitionToActive(nn1);
			NUnit.Framework.Assert.IsTrue(fs.Exists(TestDir));
			DFSTestUtil.WriteFile(fs, TestFilePath, TestFileData);
			Log.Info("Failing over to NN 0 in namespace " + nsIndex);
			cluster.TransitionToStandby(nn1);
			cluster.TransitionToActive(nn0);
			NUnit.Framework.Assert.IsTrue(fs.Exists(TestDir));
			NUnit.Framework.Assert.AreEqual(TestFileData, DFSTestUtil.ReadFile(fs, TestFilePath
				));
			Log.Info("Removing test file");
			fs.Delete(TestDir, true);
			NUnit.Framework.Assert.IsFalse(fs.Exists(TestDir));
			Log.Info("Failing over to NN 1 in namespace " + nsIndex);
			cluster.TransitionToStandby(nn0);
			cluster.TransitionToActive(nn1);
			NUnit.Framework.Assert.IsFalse(fs.Exists(TestDir));
		}

		/// <summary>Tests manual failover back and forth between two NameNodes.</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestManualFailoverAndFailback()
		{
			Configuration conf = new Configuration();
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NnTopology(MiniDFSNNTopology
				.SimpleHATopology()).NumDataNodes(1).Build();
			try
			{
				cluster.WaitActive();
				// test the only namespace
				TestManualFailoverFailback(cluster, conf, 0);
			}
			finally
			{
				cluster.Shutdown();
			}
		}

		/// <summary>
		/// Regression test for HDFS-2693: when doing state transitions, we need to
		/// lock the FSNamesystem so that we don't end up doing any writes while it's
		/// "in between" states.
		/// </summary>
		/// <remarks>
		/// Regression test for HDFS-2693: when doing state transitions, we need to
		/// lock the FSNamesystem so that we don't end up doing any writes while it's
		/// "in between" states.
		/// This test case starts up several client threads which do mutation operations
		/// while flipping a NN back and forth from active to standby.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		public virtual void TestTransitionSynchronization()
		{
			Configuration conf = new Configuration();
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NnTopology(MiniDFSNNTopology
				.SimpleHATopology()).NumDataNodes(0).Build();
			try
			{
				cluster.WaitActive();
				ReentrantReadWriteLock spyLock = NameNodeAdapter.SpyOnFsLock(cluster.GetNameNode(
					0).GetNamesystem());
				Org.Mockito.Mockito.DoAnswer(new GenericTestUtils.SleepAnswer(50)).When(spyLock).
					WriteLock();
				FileSystem fs = HATestUtil.ConfigureFailoverFs(cluster, conf);
				MultithreadedTestUtil.TestContext ctx = new MultithreadedTestUtil.TestContext();
				for (int i = 0; i < 50; i++)
				{
					int finalI = i;
					ctx.AddThread(new _RepeatingTestThread_256(finalI, fs, ctx));
				}
				ctx.AddThread(new _RepeatingTestThread_266(cluster, ctx));
				ctx.StartThreads();
				ctx.WaitFor(20000);
				ctx.Stop();
			}
			finally
			{
				cluster.Shutdown();
			}
		}

		private sealed class _RepeatingTestThread_256 : MultithreadedTestUtil.RepeatingTestThread
		{
			public _RepeatingTestThread_256(int finalI, FileSystem fs, MultithreadedTestUtil.TestContext
				 baseArg1)
				: base(baseArg1)
			{
				this.finalI = finalI;
				this.fs = fs;
			}

			/// <exception cref="System.Exception"/>
			public override void DoAnAction()
			{
				Path p = new Path("/test-" + finalI);
				fs.Mkdirs(p);
				fs.Delete(p, true);
			}

			private readonly int finalI;

			private readonly FileSystem fs;
		}

		private sealed class _RepeatingTestThread_266 : MultithreadedTestUtil.RepeatingTestThread
		{
			public _RepeatingTestThread_266(MiniDFSCluster cluster, MultithreadedTestUtil.TestContext
				 baseArg1)
				: base(baseArg1)
			{
				this.cluster = cluster;
			}

			/// <exception cref="System.Exception"/>
			public override void DoAnAction()
			{
				cluster.TransitionToStandby(0);
				Sharpen.Thread.Sleep(50);
				cluster.TransitionToActive(0);
			}

			private readonly MiniDFSCluster cluster;
		}

		/// <summary>Test for HDFS-2812.</summary>
		/// <remarks>
		/// Test for HDFS-2812. Since lease renewals go from the client
		/// only to the active NN, the SBN will have out-of-date lease
		/// info when it becomes active. We need to make sure we don't
		/// accidentally mark the leases as expired when the failover
		/// proceeds.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		public virtual void TestLeasesRenewedOnTransition()
		{
			Configuration conf = new Configuration();
			conf.SetInt(DFSConfigKeys.DfsHaTaileditsPeriodKey, 1);
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NnTopology(MiniDFSNNTopology
				.SimpleHATopology()).NumDataNodes(1).Build();
			FSDataOutputStream stm = null;
			FileSystem fs = HATestUtil.ConfigureFailoverFs(cluster, conf);
			NameNode nn0 = cluster.GetNameNode(0);
			NameNode nn1 = cluster.GetNameNode(1);
			try
			{
				cluster.WaitActive();
				cluster.TransitionToActive(0);
				Log.Info("Starting with NN 0 active");
				stm = fs.Create(TestFilePath);
				long nn0t0 = NameNodeAdapter.GetLeaseRenewalTime(nn0, TestFileStr);
				NUnit.Framework.Assert.IsTrue(nn0t0 > 0);
				long nn1t0 = NameNodeAdapter.GetLeaseRenewalTime(nn1, TestFileStr);
				NUnit.Framework.Assert.AreEqual("Lease should not yet exist on nn1", -1, nn1t0);
				Sharpen.Thread.Sleep(5);
				// make sure time advances!
				HATestUtil.WaitForStandbyToCatchUp(nn0, nn1);
				long nn1t1 = NameNodeAdapter.GetLeaseRenewalTime(nn1, TestFileStr);
				NUnit.Framework.Assert.IsTrue("Lease should have been created on standby. Time was: "
					 + nn1t1, nn1t1 > nn0t0);
				Sharpen.Thread.Sleep(5);
				// make sure time advances!
				Log.Info("Failing over to NN 1");
				cluster.TransitionToStandby(0);
				cluster.TransitionToActive(1);
				long nn1t2 = NameNodeAdapter.GetLeaseRenewalTime(nn1, TestFileStr);
				NUnit.Framework.Assert.IsTrue("Lease should have been renewed by failover process"
					, nn1t2 > nn1t1);
			}
			finally
			{
				IOUtils.CloseStream(stm);
				cluster.Shutdown();
			}
		}

		/// <summary>Test that delegation tokens continue to work after the failover.</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestDelegationTokensAfterFailover()
		{
			Configuration conf = new Configuration();
			conf.SetBoolean(DFSConfigKeys.DfsNamenodeDelegationTokenAlwaysUseKey, true);
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NnTopology(MiniDFSNNTopology
				.SimpleHATopology()).NumDataNodes(0).Build();
			try
			{
				cluster.WaitActive();
				cluster.TransitionToActive(0);
				NameNode nn1 = cluster.GetNameNode(0);
				NameNode nn2 = cluster.GetNameNode(1);
				string renewer = UserGroupInformation.GetLoginUser().GetUserName();
				Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier> token = nn1.GetRpcServer
					().GetDelegationToken(new Text(renewer));
				Log.Info("Failing over to NN 1");
				cluster.TransitionToStandby(0);
				cluster.TransitionToActive(1);
				nn2.GetRpcServer().RenewDelegationToken(token);
				nn2.GetRpcServer().CancelDelegationToken(token);
				token = nn2.GetRpcServer().GetDelegationToken(new Text(renewer));
				NUnit.Framework.Assert.IsTrue(token != null);
			}
			finally
			{
				cluster.Shutdown();
			}
		}

		/// <summary>
		/// Tests manual failover back and forth between two NameNodes
		/// for federation cluster with two namespaces.
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestManualFailoverFailbackFederationHA()
		{
			Configuration conf = new Configuration();
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NnTopology(MiniDFSNNTopology
				.SimpleHAFederatedTopology(2)).NumDataNodes(1).Build();
			try
			{
				cluster.WaitActive();
				// test for namespace 0
				TestManualFailoverFailback(cluster, conf, 0);
				// test for namespace 1
				TestManualFailoverFailback(cluster, conf, 1);
			}
			finally
			{
				cluster.Shutdown();
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestFailoverWithEmptyInProgressEditLog()
		{
			TestFailoverAfterCrashDuringLogRoll(false);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestFailoverWithEmptyInProgressEditLogWithHeader()
		{
			TestFailoverAfterCrashDuringLogRoll(true);
		}

		/// <exception cref="System.Exception"/>
		private static void TestFailoverAfterCrashDuringLogRoll(bool writeHeader)
		{
			Configuration conf = new Configuration();
			conf.SetInt(DFSConfigKeys.DfsHaTaileditsPeriodKey, int.MaxValue);
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NnTopology(MiniDFSNNTopology
				.SimpleHATopology()).NumDataNodes(0).Build();
			FileSystem fs = HATestUtil.ConfigureFailoverFs(cluster, conf);
			try
			{
				cluster.TransitionToActive(0);
				NameNode nn0 = cluster.GetNameNode(0);
				nn0.GetRpcServer().RollEditLog();
				cluster.ShutdownNameNode(0);
				CreateEmptyInProgressEditLog(cluster, nn0, writeHeader);
				cluster.TransitionToActive(1);
			}
			finally
			{
				IOUtils.Cleanup(Log, fs);
				cluster.Shutdown();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private static void CreateEmptyInProgressEditLog(MiniDFSCluster cluster, NameNode
			 nn, bool writeHeader)
		{
			long txid = nn.GetNamesystem().GetEditLog().GetLastWrittenTxId();
			URI sharedEditsUri = cluster.GetSharedEditsDir(0, 1);
			FilePath sharedEditsDir = new FilePath(sharedEditsUri.GetPath());
			Storage.StorageDirectory storageDir = new Storage.StorageDirectory(sharedEditsDir
				);
			FilePath inProgressFile = NameNodeAdapter.GetInProgressEditsFile(storageDir, txid
				 + 1);
			NUnit.Framework.Assert.IsTrue("Failed to create in-progress edits file", inProgressFile
				.CreateNewFile());
			if (writeHeader)
			{
				DataOutputStream @out = new DataOutputStream(new FileOutputStream(inProgressFile)
					);
				EditLogFileOutputStream.WriteHeader(NameNodeLayoutVersion.CurrentLayoutVersion, @out
					);
				@out.Close();
			}
		}

		/// <summary>
		/// The secret manager needs to start/stop - the invariant should be that
		/// the secret manager runs if and only if the NN is active and not in
		/// safe mode.
		/// </summary>
		/// <remarks>
		/// The secret manager needs to start/stop - the invariant should be that
		/// the secret manager runs if and only if the NN is active and not in
		/// safe mode. As a state diagram, we need to test all of the following
		/// transitions to make sure the secret manager is started when we transition
		/// into state 4, but none of the others.
		/// <pre>
		/// SafeMode     Not SafeMode
		/// Standby   1 <------> 2
		/// ^          ^
		/// |          |
		/// v          v
		/// Active    3 <------> 4
		/// </pre>
		/// </remarks>
		/// <exception cref="System.Exception"/>
		public virtual void TestSecretManagerState()
		{
			Configuration conf = new Configuration();
			conf.SetBoolean(DFSConfigKeys.DfsNamenodeDelegationTokenAlwaysUseKey, true);
			conf.SetInt(DFSConfigKeys.DfsNamenodeDelegationKeyUpdateIntervalKey, 50);
			conf.SetInt(DFSConfigKeys.DfsBlockSizeKey, 1024);
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NnTopology(MiniDFSNNTopology
				.SimpleHATopology()).NumDataNodes(1).WaitSafeMode(false).Build();
			try
			{
				cluster.TransitionToActive(0);
				DFSTestUtil.CreateFile(cluster.GetFileSystem(0), TestFilePath, 6000, (short)1, 1L
					);
				cluster.GetConfiguration(0).SetInt(DFSConfigKeys.DfsNamenodeSafemodeExtensionKey, 
					60000);
				cluster.RestartNameNode(0);
				NameNode nn = cluster.GetNameNode(0);
				Banner("Started in state 1.");
				NUnit.Framework.Assert.IsTrue(nn.IsStandbyState());
				NUnit.Framework.Assert.IsTrue(nn.IsInSafeMode());
				NUnit.Framework.Assert.IsFalse(IsDTRunning(nn));
				Banner("Transition 1->2. Should not start secret manager");
				NameNodeAdapter.LeaveSafeMode(nn);
				NUnit.Framework.Assert.IsTrue(nn.IsStandbyState());
				NUnit.Framework.Assert.IsFalse(nn.IsInSafeMode());
				NUnit.Framework.Assert.IsFalse(IsDTRunning(nn));
				Banner("Transition 2->1. Should not start secret manager.");
				NameNodeAdapter.EnterSafeMode(nn, false);
				NUnit.Framework.Assert.IsTrue(nn.IsStandbyState());
				NUnit.Framework.Assert.IsTrue(nn.IsInSafeMode());
				NUnit.Framework.Assert.IsFalse(IsDTRunning(nn));
				Banner("Transition 1->3. Should not start secret manager.");
				nn.GetRpcServer().TransitionToActive(ReqInfo);
				NUnit.Framework.Assert.IsFalse(nn.IsStandbyState());
				NUnit.Framework.Assert.IsTrue(nn.IsInSafeMode());
				NUnit.Framework.Assert.IsFalse(IsDTRunning(nn));
				Banner("Transition 3->1. Should not start secret manager.");
				nn.GetRpcServer().TransitionToStandby(ReqInfo);
				NUnit.Framework.Assert.IsTrue(nn.IsStandbyState());
				NUnit.Framework.Assert.IsTrue(nn.IsInSafeMode());
				NUnit.Framework.Assert.IsFalse(IsDTRunning(nn));
				Banner("Transition 1->3->4. Should start secret manager.");
				nn.GetRpcServer().TransitionToActive(ReqInfo);
				NameNodeAdapter.LeaveSafeMode(nn);
				NUnit.Framework.Assert.IsFalse(nn.IsStandbyState());
				NUnit.Framework.Assert.IsFalse(nn.IsInSafeMode());
				NUnit.Framework.Assert.IsTrue(IsDTRunning(nn));
				Banner("Transition 4->3. Should stop secret manager");
				NameNodeAdapter.EnterSafeMode(nn, false);
				NUnit.Framework.Assert.IsFalse(nn.IsStandbyState());
				NUnit.Framework.Assert.IsTrue(nn.IsInSafeMode());
				NUnit.Framework.Assert.IsFalse(IsDTRunning(nn));
				Banner("Transition 3->4. Should start secret manager");
				NameNodeAdapter.LeaveSafeMode(nn);
				NUnit.Framework.Assert.IsFalse(nn.IsStandbyState());
				NUnit.Framework.Assert.IsFalse(nn.IsInSafeMode());
				NUnit.Framework.Assert.IsTrue(IsDTRunning(nn));
				for (int i = 0; i < 20; i++)
				{
					// Loop the last check to suss out races.
					Banner("Transition 4->2. Should stop secret manager.");
					nn.GetRpcServer().TransitionToStandby(ReqInfo);
					NUnit.Framework.Assert.IsTrue(nn.IsStandbyState());
					NUnit.Framework.Assert.IsFalse(nn.IsInSafeMode());
					NUnit.Framework.Assert.IsFalse(IsDTRunning(nn));
					Banner("Transition 2->4. Should start secret manager");
					nn.GetRpcServer().TransitionToActive(ReqInfo);
					NUnit.Framework.Assert.IsFalse(nn.IsStandbyState());
					NUnit.Framework.Assert.IsFalse(nn.IsInSafeMode());
					NUnit.Framework.Assert.IsTrue(IsDTRunning(nn));
				}
			}
			finally
			{
				cluster.Shutdown();
			}
		}

		/// <summary>
		/// This test also serves to test
		/// <see cref="Org.Apache.Hadoop.Hdfs.HAUtil.GetProxiesForAllNameNodesInNameservice(Org.Apache.Hadoop.Conf.Configuration, string)
		/// 	"/>
		/// and
		/// <see cref="Org.Apache.Hadoop.Hdfs.DFSUtil.GetRpcAddressesForNameserviceId(Org.Apache.Hadoop.Conf.Configuration, string, string)
		/// 	"/>
		/// by virtue of the fact that it wouldn't work properly if the proxies
		/// returned were not for the correct NNs.
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestIsAtLeastOneActive()
		{
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(new HdfsConfiguration()).NnTopology
				(MiniDFSNNTopology.SimpleHATopology()).NumDataNodes(0).Build();
			try
			{
				Configuration conf = new HdfsConfiguration();
				HATestUtil.SetFailoverConfigurations(cluster, conf);
				IList<ClientProtocol> namenodes = HAUtil.GetProxiesForAllNameNodesInNameservice(conf
					, HATestUtil.GetLogicalHostname(cluster));
				NUnit.Framework.Assert.AreEqual(2, namenodes.Count);
				NUnit.Framework.Assert.IsFalse(HAUtil.IsAtLeastOneActive(namenodes));
				cluster.TransitionToActive(0);
				NUnit.Framework.Assert.IsTrue(HAUtil.IsAtLeastOneActive(namenodes));
				cluster.TransitionToStandby(0);
				NUnit.Framework.Assert.IsFalse(HAUtil.IsAtLeastOneActive(namenodes));
				cluster.TransitionToActive(1);
				NUnit.Framework.Assert.IsTrue(HAUtil.IsAtLeastOneActive(namenodes));
				cluster.TransitionToStandby(1);
				NUnit.Framework.Assert.IsFalse(HAUtil.IsAtLeastOneActive(namenodes));
			}
			finally
			{
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		private bool IsDTRunning(NameNode nn)
		{
			return NameNodeAdapter.GetDtSecretManager(nn.GetNamesystem()).IsRunning();
		}

		/// <summary>Print a big banner in the test log to make debug easier.</summary>
		internal static void Banner(string @string)
		{
			Log.Info("\n\n\n\n================================================\n" + @string +
				 "\n" + "==================================================\n\n");
		}
	}
}
