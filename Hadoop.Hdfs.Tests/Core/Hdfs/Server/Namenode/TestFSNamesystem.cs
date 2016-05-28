using System.Collections.Generic;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Hdfs.Server.Namenode.HA;
using Org.Mockito.Internal.Util.Reflection;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	public class TestFSNamesystem
	{
		[TearDown]
		public virtual void CleanUp()
		{
			FileUtil.FullyDeleteContents(new FilePath(MiniDFSCluster.GetBaseDirectory()));
		}

		/// <summary>Tests that the namenode edits dirs are gotten with duplicates removed</summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestUniqueEditDirs()
		{
			Configuration config = new Configuration();
			config.Set(DFSConfigKeys.DfsNamenodeEditsDirKey, "file://edits/dir, " + "file://edits/dir1,file://edits/dir1"
				);
			// overlapping internally
			// getNamespaceEditsDirs removes duplicates
			ICollection<URI> editsDirs = FSNamesystem.GetNamespaceEditsDirs(config);
			NUnit.Framework.Assert.AreEqual(2, editsDirs.Count);
		}

		/// <summary>Test that FSNamesystem#clear clears all leases.</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestFSNamespaceClearLeases()
		{
			Configuration conf = new HdfsConfiguration();
			FilePath nameDir = new FilePath(MiniDFSCluster.GetBaseDirectory(), "name");
			conf.Set(DFSConfigKeys.DfsNamenodeNameDirKey, nameDir.GetAbsolutePath());
			NameNode.InitMetrics(conf, HdfsServerConstants.NamenodeRole.Namenode);
			DFSTestUtil.FormatNameNode(conf);
			FSNamesystem fsn = FSNamesystem.LoadFromDisk(conf);
			LeaseManager leaseMan = fsn.GetLeaseManager();
			leaseMan.AddLease("client1", "importantFile");
			NUnit.Framework.Assert.AreEqual(1, leaseMan.CountLease());
			fsn.Clear();
			leaseMan = fsn.GetLeaseManager();
			NUnit.Framework.Assert.AreEqual(0, leaseMan.CountLease());
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestStartupSafemode()
		{
			Configuration conf = new Configuration();
			FSImage fsImage = Org.Mockito.Mockito.Mock<FSImage>();
			FSEditLog fsEditLog = Org.Mockito.Mockito.Mock<FSEditLog>();
			Org.Mockito.Mockito.When(fsImage.GetEditLog()).ThenReturn(fsEditLog);
			FSNamesystem fsn = new FSNamesystem(conf, fsImage);
			fsn.LeaveSafeMode();
			NUnit.Framework.Assert.IsTrue("After leaving safemode FSNamesystem.isInStartupSafeMode still "
				 + "returned true", !fsn.IsInStartupSafeMode());
			NUnit.Framework.Assert.IsTrue("After leaving safemode FSNamesystem.isInSafeMode still returned"
				 + " true", !fsn.IsInSafeMode());
			fsn.EnterSafeMode(true);
			NUnit.Framework.Assert.IsTrue("After entering safemode due to low resources FSNamesystem."
				 + "isInStartupSafeMode still returned true", !fsn.IsInStartupSafeMode());
			NUnit.Framework.Assert.IsTrue("After entering safemode due to low resources FSNamesystem."
				 + "isInSafeMode still returned false", fsn.IsInSafeMode());
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestReplQueuesActiveAfterStartupSafemode()
		{
			Configuration conf = new Configuration();
			FSEditLog fsEditLog = Org.Mockito.Mockito.Mock<FSEditLog>();
			FSImage fsImage = Org.Mockito.Mockito.Mock<FSImage>();
			Org.Mockito.Mockito.When(fsImage.GetEditLog()).ThenReturn(fsEditLog);
			FSNamesystem fsNamesystem = new FSNamesystem(conf, fsImage);
			FSNamesystem fsn = Org.Mockito.Mockito.Spy(fsNamesystem);
			//Make shouldPopulaeReplQueues return true
			HAContext haContext = Org.Mockito.Mockito.Mock<HAContext>();
			HAState haState = Org.Mockito.Mockito.Mock<HAState>();
			Org.Mockito.Mockito.When(haContext.GetState()).ThenReturn(haState);
			Org.Mockito.Mockito.When(haState.ShouldPopulateReplQueues()).ThenReturn(true);
			Whitebox.SetInternalState(fsn, "haContext", haContext);
			//Make NameNode.getNameNodeMetrics() not return null
			NameNode.InitMetrics(conf, HdfsServerConstants.NamenodeRole.Namenode);
			fsn.EnterSafeMode(false);
			NUnit.Framework.Assert.IsTrue("FSNamesystem didn't enter safemode", fsn.IsInSafeMode
				());
			NUnit.Framework.Assert.IsTrue("Replication queues were being populated during very first "
				 + "safemode", !fsn.IsPopulatingReplQueues());
			fsn.LeaveSafeMode();
			NUnit.Framework.Assert.IsTrue("FSNamesystem didn't leave safemode", !fsn.IsInSafeMode
				());
			NUnit.Framework.Assert.IsTrue("Replication queues weren't being populated even after leaving "
				 + "safemode", fsn.IsPopulatingReplQueues());
			fsn.EnterSafeMode(false);
			NUnit.Framework.Assert.IsTrue("FSNamesystem didn't enter safemode", fsn.IsInSafeMode
				());
			NUnit.Framework.Assert.IsTrue("Replication queues weren't being populated after entering "
				 + "safemode 2nd time", fsn.IsPopulatingReplQueues());
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestFsLockFairness()
		{
			Configuration conf = new Configuration();
			FSEditLog fsEditLog = Org.Mockito.Mockito.Mock<FSEditLog>();
			FSImage fsImage = Org.Mockito.Mockito.Mock<FSImage>();
			Org.Mockito.Mockito.When(fsImage.GetEditLog()).ThenReturn(fsEditLog);
			conf.SetBoolean("dfs.namenode.fslock.fair", true);
			FSNamesystem fsNamesystem = new FSNamesystem(conf, fsImage);
			NUnit.Framework.Assert.IsTrue(fsNamesystem.GetFsLockForTests().IsFair());
			conf.SetBoolean("dfs.namenode.fslock.fair", false);
			fsNamesystem = new FSNamesystem(conf, fsImage);
			NUnit.Framework.Assert.IsFalse(fsNamesystem.GetFsLockForTests().IsFair());
		}

		[NUnit.Framework.Test]
		public virtual void TestFSNamesystemLockCompatibility()
		{
			FSNamesystemLock rwLock = new FSNamesystemLock(true);
			NUnit.Framework.Assert.AreEqual(0, rwLock.GetReadHoldCount());
			rwLock.ReadLock().Lock();
			NUnit.Framework.Assert.AreEqual(1, rwLock.GetReadHoldCount());
			rwLock.ReadLock().Lock();
			NUnit.Framework.Assert.AreEqual(2, rwLock.GetReadHoldCount());
			rwLock.ReadLock().Unlock();
			NUnit.Framework.Assert.AreEqual(1, rwLock.GetReadHoldCount());
			rwLock.ReadLock().Unlock();
			NUnit.Framework.Assert.AreEqual(0, rwLock.GetReadHoldCount());
			NUnit.Framework.Assert.IsFalse(rwLock.IsWriteLockedByCurrentThread());
			NUnit.Framework.Assert.AreEqual(0, rwLock.GetWriteHoldCount());
			rwLock.WriteLock().Lock();
			NUnit.Framework.Assert.IsTrue(rwLock.IsWriteLockedByCurrentThread());
			NUnit.Framework.Assert.AreEqual(1, rwLock.GetWriteHoldCount());
			rwLock.WriteLock().Lock();
			NUnit.Framework.Assert.IsTrue(rwLock.IsWriteLockedByCurrentThread());
			NUnit.Framework.Assert.AreEqual(2, rwLock.GetWriteHoldCount());
			rwLock.WriteLock().Unlock();
			NUnit.Framework.Assert.IsTrue(rwLock.IsWriteLockedByCurrentThread());
			NUnit.Framework.Assert.AreEqual(1, rwLock.GetWriteHoldCount());
			rwLock.WriteLock().Unlock();
			NUnit.Framework.Assert.IsFalse(rwLock.IsWriteLockedByCurrentThread());
			NUnit.Framework.Assert.AreEqual(0, rwLock.GetWriteHoldCount());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestReset()
		{
			Configuration conf = new Configuration();
			FSEditLog fsEditLog = Org.Mockito.Mockito.Mock<FSEditLog>();
			FSImage fsImage = Org.Mockito.Mockito.Mock<FSImage>();
			Org.Mockito.Mockito.When(fsImage.GetEditLog()).ThenReturn(fsEditLog);
			FSNamesystem fsn = new FSNamesystem(conf, fsImage);
			fsn.ImageLoadComplete();
			NUnit.Framework.Assert.IsTrue(fsn.IsImageLoaded());
			fsn.Clear();
			NUnit.Framework.Assert.IsFalse(fsn.IsImageLoaded());
			INodeDirectory root = (INodeDirectory)fsn.GetFSDirectory().GetINode("/");
			NUnit.Framework.Assert.IsTrue(root.GetChildrenList(Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot
				.CurrentStateId).IsEmpty());
			fsn.ImageLoadComplete();
			NUnit.Framework.Assert.IsTrue(fsn.IsImageLoaded());
		}
	}
}
