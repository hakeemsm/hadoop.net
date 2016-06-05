using System;
using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Base;
using Com.Google.Common.Collect;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Blockmanagement;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Test;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	/// <summary>Tests to verify safe mode correctness.</summary>
	public class TestSafeMode
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(TestSafeMode));

		private static readonly Path TestPath = new Path("/test");

		private const int BlockSize = 1024;

		private static readonly string Newline = Runtime.GetProperty("line.separator");

		internal Configuration conf;

		internal MiniDFSCluster cluster;

		internal FileSystem fs;

		internal DistributedFileSystem dfs;

		private const string NnMetrics = "NameNodeActivity";

		/// <exception cref="System.IO.IOException"/>
		[SetUp]
		public virtual void StartUp()
		{
			conf = new HdfsConfiguration();
			conf.SetInt(DFSConfigKeys.DfsBlockSizeKey, BlockSize);
			conf.SetBoolean(DFSConfigKeys.DfsNamenodeAclsEnabledKey, true);
			conf.SetBoolean(DFSConfigKeys.DfsNamenodeXattrsEnabledKey, true);
			cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(1).Build();
			cluster.WaitActive();
			fs = cluster.GetFileSystem();
			dfs = (DistributedFileSystem)fs;
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.TearDown]
		public virtual void TearDown()
		{
			if (fs != null)
			{
				fs.Close();
			}
			if (cluster != null)
			{
				cluster.Shutdown();
			}
		}

		/// <summary>
		/// This test verifies that if SafeMode is manually entered, name-node does not
		/// come out of safe mode even after the startup safe mode conditions are met.
		/// </summary>
		/// <remarks>
		/// This test verifies that if SafeMode is manually entered, name-node does not
		/// come out of safe mode even after the startup safe mode conditions are met.
		/// <ol>
		/// <li>Start cluster with 1 data-node.</li>
		/// <li>Create 2 files with replication 1.</li>
		/// <li>Re-start cluster with 0 data-nodes.
		/// Name-node should stay in automatic safe-mode.</li>
		/// <li>Enter safe mode manually.</li>
		/// <li>Start the data-node.</li>
		/// <li>Wait longer than <tt>dfs.namenode.safemode.extension</tt> and
		/// verify that the name-node is still in safe mode.</li>
		/// </ol>
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestManualSafeMode()
		{
			fs = cluster.GetFileSystem();
			Path file1 = new Path("/tmp/testManualSafeMode/file1");
			Path file2 = new Path("/tmp/testManualSafeMode/file2");
			// create two files with one block each.
			DFSTestUtil.CreateFile(fs, file1, 1000, (short)1, 0);
			DFSTestUtil.CreateFile(fs, file2, 1000, (short)1, 0);
			fs.Close();
			cluster.Shutdown();
			// now bring up just the NameNode.
			cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(0).Format(false).Build();
			cluster.WaitActive();
			dfs = cluster.GetFileSystem();
			NUnit.Framework.Assert.IsTrue("No datanode is started. Should be in SafeMode", dfs
				.SetSafeMode(HdfsConstants.SafeModeAction.SafemodeGet));
			// manually set safemode.
			dfs.SetSafeMode(HdfsConstants.SafeModeAction.SafemodeEnter);
			// now bring up the datanode and wait for it to be active.
			cluster.StartDataNodes(conf, 1, true, null, null);
			cluster.WaitActive();
			// wait longer than dfs.namenode.safemode.extension
			try
			{
				Sharpen.Thread.Sleep(2000);
			}
			catch (Exception)
			{
			}
			NUnit.Framework.Assert.IsTrue("should still be in SafeMode", dfs.SetSafeMode(HdfsConstants.SafeModeAction
				.SafemodeGet));
			NUnit.Framework.Assert.IsFalse("should not be in SafeMode", dfs.SetSafeMode(HdfsConstants.SafeModeAction
				.SafemodeLeave));
		}

		/// <summary>
		/// Test that, if there are no blocks in the filesystem,
		/// the NameNode doesn't enter the "safemode extension" period.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestNoExtensionIfNoBlocks()
		{
			cluster.GetConfiguration(0).SetInt(DFSConfigKeys.DfsNamenodeSafemodeExtensionKey, 
				60000);
			cluster.RestartNameNode();
			// Even though we have safemode extension set high, we should immediately
			// exit safemode on startup because there are no blocks in the namespace.
			string status = cluster.GetNameNode().GetNamesystem().GetSafemode();
			NUnit.Framework.Assert.AreEqual(string.Empty, status);
		}

		/// <summary>
		/// Test that the NN initializes its under-replicated blocks queue
		/// before it is ready to exit safemode (HDFS-1476)
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestInitializeReplQueuesEarly()
		{
			Log.Info("Starting testInitializeReplQueuesEarly");
			// Spray the blocks around the cluster when we add DNs instead of
			// concentrating all blocks on the first node.
			BlockManagerTestUtil.SetWritingPrefersLocalNode(cluster.GetNamesystem().GetBlockManager
				(), false);
			cluster.StartDataNodes(conf, 2, true, HdfsServerConstants.StartupOption.Regular, 
				null);
			cluster.WaitActive();
			Log.Info("Creating files");
			DFSTestUtil.CreateFile(fs, TestPath, 15 * BlockSize, (short)1, 1L);
			Log.Info("Stopping all DataNodes");
			IList<MiniDFSCluster.DataNodeProperties> dnprops = Lists.NewLinkedList();
			dnprops.AddItem(cluster.StopDataNode(0));
			dnprops.AddItem(cluster.StopDataNode(0));
			dnprops.AddItem(cluster.StopDataNode(0));
			cluster.GetConfiguration(0).SetFloat(DFSConfigKeys.DfsNamenodeReplQueueThresholdPctKey
				, 1f / 15f);
			Log.Info("Restarting NameNode");
			cluster.RestartNameNode();
			NameNode nn = cluster.GetNameNode();
			string status = nn.GetNamesystem().GetSafemode();
			NUnit.Framework.Assert.AreEqual("Safe mode is ON. The reported blocks 0 needs additional "
				 + "15 blocks to reach the threshold 0.9990 of total blocks 15." + Newline + "The number of live datanodes 0 has reached the minimum number 0. "
				 + "Safe mode will be turned off automatically once the thresholds " + "have been reached."
				, status);
			NUnit.Framework.Assert.IsFalse("Mis-replicated block queues should not be initialized "
				 + "until threshold is crossed", NameNodeAdapter.SafeModeInitializedReplQueues(nn
				));
			Log.Info("Restarting one DataNode");
			cluster.RestartDataNode(dnprops.Remove(0));
			// Wait for block reports from all attached storages of
			// the restarted DN to come in.
			GenericTestUtils.WaitFor(new _Supplier_214(this), 10, 10000);
			int safe = NameNodeAdapter.GetSafeModeSafeBlocks(nn);
			NUnit.Framework.Assert.IsTrue("Expected first block report to make some blocks safe."
				, safe > 0);
			NUnit.Framework.Assert.IsTrue("Did not expect first block report to make all blocks safe."
				, safe < 15);
			NUnit.Framework.Assert.IsTrue(NameNodeAdapter.SafeModeInitializedReplQueues(nn));
			// Ensure that UnderReplicatedBlocks goes up to 15 - safe. Misreplicated
			// blocks are processed asynchronously so this may take a few seconds.
			// Failure here will manifest as a test timeout.
			BlockManagerTestUtil.UpdateState(nn.GetNamesystem().GetBlockManager());
			long underReplicatedBlocks = nn.GetNamesystem().GetUnderReplicatedBlocks();
			while (underReplicatedBlocks != (15 - safe))
			{
				Log.Info("UnderReplicatedBlocks expected=" + (15 - safe) + ", actual=" + underReplicatedBlocks
					);
				Sharpen.Thread.Sleep(100);
				BlockManagerTestUtil.UpdateState(nn.GetNamesystem().GetBlockManager());
				underReplicatedBlocks = nn.GetNamesystem().GetUnderReplicatedBlocks();
			}
			cluster.RestartDataNodes();
		}

		private sealed class _Supplier_214 : Supplier<bool>
		{
			public _Supplier_214(TestSafeMode _enclosing)
			{
				this._enclosing = _enclosing;
			}

			public bool Get()
			{
				return MetricsAsserts.GetLongCounter("StorageBlockReportOps", MetricsAsserts.GetMetrics
					(TestSafeMode.NnMetrics)) == this._enclosing.cluster.GetStoragesPerDatanode();
			}

			private readonly TestSafeMode _enclosing;
		}

		/// <summary>
		/// Test that, when under-replicated blocks are processed at the end of
		/// safe-mode, blocks currently under construction are not considered
		/// under-construction or missing.
		/// </summary>
		/// <remarks>
		/// Test that, when under-replicated blocks are processed at the end of
		/// safe-mode, blocks currently under construction are not considered
		/// under-construction or missing. Regression test for HDFS-2822.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestRbwBlocksNotConsideredUnderReplicated()
		{
			IList<FSDataOutputStream> stms = Lists.NewArrayList();
			try
			{
				// Create some junk blocks so that the NN doesn't just immediately
				// exit safemode on restart.
				DFSTestUtil.CreateFile(fs, new Path("/junk-blocks"), BlockSize * 4, (short)1, 1L);
				// Create several files which are left open. It's important to
				// create several here, because otherwise the first iteration of the
				// replication monitor will pull them off the replication queue and
				// hide this bug from the test!
				for (int i = 0; i < 10; i++)
				{
					FSDataOutputStream stm = fs.Create(new Path("/append-" + i), true, BlockSize, (short
						)1, BlockSize);
					stms.AddItem(stm);
					stm.Write(1);
					stm.Hflush();
				}
				cluster.RestartNameNode();
				FSNamesystem ns = cluster.GetNameNode(0).GetNamesystem();
				BlockManagerTestUtil.UpdateState(ns.GetBlockManager());
				NUnit.Framework.Assert.AreEqual(0, ns.GetPendingReplicationBlocks());
				NUnit.Framework.Assert.AreEqual(0, ns.GetCorruptReplicaBlocks());
				NUnit.Framework.Assert.AreEqual(0, ns.GetMissingBlocksCount());
			}
			finally
			{
				foreach (FSDataOutputStream stm in stms)
				{
					IOUtils.CloseStream(stm);
				}
				cluster.Shutdown();
			}
		}

		public interface FSRun
		{
			/// <exception cref="System.IO.IOException"/>
			void Run(FileSystem fs);
		}

		/// <summary>
		/// Assert that the given function fails to run due to a safe
		/// mode exception.
		/// </summary>
		public virtual void RunFsFun(string msg, TestSafeMode.FSRun f)
		{
			try
			{
				f.Run(fs);
				NUnit.Framework.Assert.Fail(msg);
			}
			catch (RemoteException re)
			{
				NUnit.Framework.Assert.AreEqual(typeof(SafeModeException).FullName, re.GetClassName
					());
				GenericTestUtils.AssertExceptionContains("Name node is in safe mode", re);
			}
			catch (IOException ioe)
			{
				NUnit.Framework.Assert.Fail(msg + " " + StringUtils.StringifyException(ioe));
			}
		}

		/// <summary>
		/// Run various fs operations while the NN is in safe mode,
		/// assert that they are either allowed or fail as expected.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestOperationsWhileInSafeMode()
		{
			Path file1 = new Path("/file1");
			NUnit.Framework.Assert.IsFalse(dfs.SetSafeMode(HdfsConstants.SafeModeAction.SafemodeGet
				));
			DFSTestUtil.CreateFile(fs, file1, 1024, (short)1, 0);
			NUnit.Framework.Assert.IsTrue("Could not enter SM", dfs.SetSafeMode(HdfsConstants.SafeModeAction
				.SafemodeEnter));
			RunFsFun("Set quota while in SM", new _FSRun_319(file1));
			RunFsFun("Set perm while in SM", new _FSRun_325(file1));
			RunFsFun("Set owner while in SM", new _FSRun_331(file1));
			RunFsFun("Set repl while in SM", new _FSRun_337(file1));
			RunFsFun("Append file while in SM", new _FSRun_343(file1));
			RunFsFun("Truncate file while in SM", new _FSRun_349(file1));
			RunFsFun("Delete file while in SM", new _FSRun_355(file1));
			RunFsFun("Rename file while in SM", new _FSRun_361(file1));
			RunFsFun("Set time while in SM", new _FSRun_367(file1));
			RunFsFun("modifyAclEntries while in SM", new _FSRun_373(file1));
			RunFsFun("removeAclEntries while in SM", new _FSRun_379(file1));
			RunFsFun("removeDefaultAcl while in SM", new _FSRun_385(file1));
			RunFsFun("removeAcl while in SM", new _FSRun_391(file1));
			RunFsFun("setAcl while in SM", new _FSRun_397(file1));
			RunFsFun("setXAttr while in SM", new _FSRun_403(file1));
			RunFsFun("removeXAttr while in SM", new _FSRun_409(file1));
			try
			{
				DFSTestUtil.ReadFile(fs, file1);
			}
			catch (IOException)
			{
				NUnit.Framework.Assert.Fail("Set times failed while in SM");
			}
			try
			{
				fs.GetAclStatus(file1);
			}
			catch (IOException)
			{
				NUnit.Framework.Assert.Fail("getAclStatus failed while in SM");
			}
			// Test access
			UserGroupInformation ugiX = UserGroupInformation.CreateRemoteUser("userX");
			FileSystem myfs = ugiX.DoAs(new _PrivilegedExceptionAction_429(this));
			myfs.Access(file1, FsAction.Read);
			try
			{
				myfs.Access(file1, FsAction.Write);
				NUnit.Framework.Assert.Fail("The access call should have failed.");
			}
			catch (AccessControlException)
			{
			}
			// expected
			NUnit.Framework.Assert.IsFalse("Could not leave SM", dfs.SetSafeMode(HdfsConstants.SafeModeAction
				.SafemodeLeave));
		}

		private sealed class _FSRun_319 : TestSafeMode.FSRun
		{
			public _FSRun_319(Path file1)
			{
				this.file1 = file1;
			}

			/// <exception cref="System.IO.IOException"/>
			public void Run(FileSystem fs)
			{
				((DistributedFileSystem)fs).SetQuota(file1, 1, 1);
			}

			private readonly Path file1;
		}

		private sealed class _FSRun_325 : TestSafeMode.FSRun
		{
			public _FSRun_325(Path file1)
			{
				this.file1 = file1;
			}

			/// <exception cref="System.IO.IOException"/>
			public void Run(FileSystem fs)
			{
				fs.SetPermission(file1, FsPermission.GetDefault());
			}

			private readonly Path file1;
		}

		private sealed class _FSRun_331 : TestSafeMode.FSRun
		{
			public _FSRun_331(Path file1)
			{
				this.file1 = file1;
			}

			/// <exception cref="System.IO.IOException"/>
			public void Run(FileSystem fs)
			{
				fs.SetOwner(file1, "user", "group");
			}

			private readonly Path file1;
		}

		private sealed class _FSRun_337 : TestSafeMode.FSRun
		{
			public _FSRun_337(Path file1)
			{
				this.file1 = file1;
			}

			/// <exception cref="System.IO.IOException"/>
			public void Run(FileSystem fs)
			{
				fs.SetReplication(file1, (short)1);
			}

			private readonly Path file1;
		}

		private sealed class _FSRun_343 : TestSafeMode.FSRun
		{
			public _FSRun_343(Path file1)
			{
				this.file1 = file1;
			}

			/// <exception cref="System.IO.IOException"/>
			public void Run(FileSystem fs)
			{
				DFSTestUtil.AppendFile(fs, file1, "new bytes");
			}

			private readonly Path file1;
		}

		private sealed class _FSRun_349 : TestSafeMode.FSRun
		{
			public _FSRun_349(Path file1)
			{
				this.file1 = file1;
			}

			/// <exception cref="System.IO.IOException"/>
			public void Run(FileSystem fs)
			{
				fs.Truncate(file1, 0);
			}

			private readonly Path file1;
		}

		private sealed class _FSRun_355 : TestSafeMode.FSRun
		{
			public _FSRun_355(Path file1)
			{
				this.file1 = file1;
			}

			/// <exception cref="System.IO.IOException"/>
			public void Run(FileSystem fs)
			{
				fs.Delete(file1, false);
			}

			private readonly Path file1;
		}

		private sealed class _FSRun_361 : TestSafeMode.FSRun
		{
			public _FSRun_361(Path file1)
			{
				this.file1 = file1;
			}

			/// <exception cref="System.IO.IOException"/>
			public void Run(FileSystem fs)
			{
				fs.Rename(file1, new Path("file2"));
			}

			private readonly Path file1;
		}

		private sealed class _FSRun_367 : TestSafeMode.FSRun
		{
			public _FSRun_367(Path file1)
			{
				this.file1 = file1;
			}

			/// <exception cref="System.IO.IOException"/>
			public void Run(FileSystem fs)
			{
				fs.SetTimes(file1, 0, 0);
			}

			private readonly Path file1;
		}

		private sealed class _FSRun_373 : TestSafeMode.FSRun
		{
			public _FSRun_373(Path file1)
			{
				this.file1 = file1;
			}

			/// <exception cref="System.IO.IOException"/>
			public void Run(FileSystem fs)
			{
				fs.ModifyAclEntries(file1, Lists.NewArrayList<AclEntry>());
			}

			private readonly Path file1;
		}

		private sealed class _FSRun_379 : TestSafeMode.FSRun
		{
			public _FSRun_379(Path file1)
			{
				this.file1 = file1;
			}

			/// <exception cref="System.IO.IOException"/>
			public void Run(FileSystem fs)
			{
				fs.RemoveAclEntries(file1, Lists.NewArrayList<AclEntry>());
			}

			private readonly Path file1;
		}

		private sealed class _FSRun_385 : TestSafeMode.FSRun
		{
			public _FSRun_385(Path file1)
			{
				this.file1 = file1;
			}

			/// <exception cref="System.IO.IOException"/>
			public void Run(FileSystem fs)
			{
				fs.RemoveDefaultAcl(file1);
			}

			private readonly Path file1;
		}

		private sealed class _FSRun_391 : TestSafeMode.FSRun
		{
			public _FSRun_391(Path file1)
			{
				this.file1 = file1;
			}

			/// <exception cref="System.IO.IOException"/>
			public void Run(FileSystem fs)
			{
				fs.RemoveAcl(file1);
			}

			private readonly Path file1;
		}

		private sealed class _FSRun_397 : TestSafeMode.FSRun
		{
			public _FSRun_397(Path file1)
			{
				this.file1 = file1;
			}

			/// <exception cref="System.IO.IOException"/>
			public void Run(FileSystem fs)
			{
				fs.SetAcl(file1, Lists.NewArrayList<AclEntry>());
			}

			private readonly Path file1;
		}

		private sealed class _FSRun_403 : TestSafeMode.FSRun
		{
			public _FSRun_403(Path file1)
			{
				this.file1 = file1;
			}

			/// <exception cref="System.IO.IOException"/>
			public void Run(FileSystem fs)
			{
				fs.SetXAttr(file1, "user.a1", null);
			}

			private readonly Path file1;
		}

		private sealed class _FSRun_409 : TestSafeMode.FSRun
		{
			public _FSRun_409(Path file1)
			{
				this.file1 = file1;
			}

			/// <exception cref="System.IO.IOException"/>
			public void Run(FileSystem fs)
			{
				fs.RemoveXAttr(file1, "user.a1");
			}

			private readonly Path file1;
		}

		private sealed class _PrivilegedExceptionAction_429 : PrivilegedExceptionAction<FileSystem
			>
		{
			public _PrivilegedExceptionAction_429(TestSafeMode _enclosing)
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="System.IO.IOException"/>
			public FileSystem Run()
			{
				return FileSystem.Get(this._enclosing.conf);
			}

			private readonly TestSafeMode _enclosing;
		}

		/// <summary>
		/// Verify that the NameNode stays in safemode when dfs.safemode.datanode.min
		/// is set to a number greater than the number of live datanodes.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestDatanodeThreshold()
		{
			cluster.Shutdown();
			Configuration conf = cluster.GetConfiguration(0);
			conf.SetInt(DFSConfigKeys.DfsNamenodeSafemodeExtensionKey, 0);
			conf.SetInt(DFSConfigKeys.DfsNamenodeSafemodeMinDatanodesKey, 1);
			cluster.RestartNameNode();
			fs = cluster.GetFileSystem();
			string tipMsg = cluster.GetNamesystem().GetSafemode();
			NUnit.Framework.Assert.IsTrue("Safemode tip message doesn't look right: " + tipMsg
				, tipMsg.Contains("The number of live datanodes 0 needs an additional " + "1 live datanodes to reach the minimum number 1."
				 + Newline + "Safe mode will be turned off automatically"));
			// Start a datanode
			cluster.StartDataNodes(conf, 1, true, null, null);
			// Wait long enough for safemode check to refire
			try
			{
				Sharpen.Thread.Sleep(1000);
			}
			catch (Exception)
			{
			}
			// We now should be out of safe mode.
			NUnit.Framework.Assert.AreEqual(string.Empty, cluster.GetNamesystem().GetSafemode
				());
		}

		/*
		* Tests some utility methods that surround the SafeMode's state.
		* @throws IOException when there's an issue connecting to the test DFS.
		*/
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestSafeModeUtils()
		{
			dfs = cluster.GetFileSystem();
			// Enter safemode.
			dfs.SetSafeMode(HdfsConstants.SafeModeAction.SafemodeEnter);
			NUnit.Framework.Assert.IsTrue("State was expected to be in safemode.", dfs.IsInSafeMode
				());
			// Exit safemode.
			dfs.SetSafeMode(HdfsConstants.SafeModeAction.SafemodeLeave);
			NUnit.Framework.Assert.IsFalse("State was expected to be out of safemode.", dfs.IsInSafeMode
				());
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestSafeModeWhenZeroBlockLocations()
		{
			try
			{
				Path file1 = new Path("/tmp/testManualSafeMode/file1");
				Path file2 = new Path("/tmp/testManualSafeMode/file2");
				System.Console.Out.WriteLine("Created file1 and file2.");
				// create two files with one block each.
				DFSTestUtil.CreateFile(fs, file1, 1000, (short)1, 0);
				DFSTestUtil.CreateFile(fs, file2, 2000, (short)1, 0);
				CheckGetBlockLocationsWorks(fs, file1);
				NameNode namenode = cluster.GetNameNode();
				// manually set safemode.
				dfs.SetSafeMode(HdfsConstants.SafeModeAction.SafemodeEnter);
				NUnit.Framework.Assert.IsTrue("should still be in SafeMode", namenode.IsInSafeMode
					());
				// getBlock locations should still work since block locations exists
				CheckGetBlockLocationsWorks(fs, file1);
				dfs.SetSafeMode(HdfsConstants.SafeModeAction.SafemodeLeave);
				NUnit.Framework.Assert.IsFalse("should not be in SafeMode", namenode.IsInSafeMode
					());
				// Now 2nd part of the tests where there aren't block locations
				cluster.ShutdownDataNodes();
				cluster.ShutdownNameNode(0);
				// now bring up just the NameNode.
				cluster.RestartNameNode();
				cluster.WaitActive();
				System.Console.Out.WriteLine("Restarted cluster with just the NameNode");
				namenode = cluster.GetNameNode();
				NUnit.Framework.Assert.IsTrue("No datanode is started. Should be in SafeMode", namenode
					.IsInSafeMode());
				FileStatus stat = fs.GetFileStatus(file1);
				try
				{
					fs.GetFileBlockLocations(stat, 0, 1000);
					NUnit.Framework.Assert.IsTrue("Should have got safemode exception", false);
				}
				catch (SafeModeException)
				{
				}
				catch (RemoteException re)
				{
					// as expected 
					if (!re.GetClassName().Equals(typeof(SafeModeException).FullName))
					{
						NUnit.Framework.Assert.IsTrue("Should have got safemode exception", false);
					}
				}
				dfs.SetSafeMode(HdfsConstants.SafeModeAction.SafemodeLeave);
				NUnit.Framework.Assert.IsFalse("Should not be in safemode", namenode.IsInSafeMode
					());
				CheckGetBlockLocationsWorks(fs, file1);
			}
			finally
			{
				if (fs != null)
				{
					fs.Close();
				}
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual void CheckGetBlockLocationsWorks(FileSystem fs, Path fileName)
		{
			FileStatus stat = fs.GetFileStatus(fileName);
			try
			{
				fs.GetFileBlockLocations(stat, 0, 1000);
			}
			catch (SafeModeException)
			{
				NUnit.Framework.Assert.IsTrue("Should have not got safemode exception", false);
			}
			catch (RemoteException)
			{
				NUnit.Framework.Assert.IsTrue("Should have not got safemode exception", false);
			}
		}
	}
}
