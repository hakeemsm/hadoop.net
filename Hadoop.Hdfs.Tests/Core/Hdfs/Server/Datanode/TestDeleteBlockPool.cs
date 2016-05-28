using System;
using System.IO;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Tools;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Datanode
{
	/// <summary>Tests deleteBlockPool functionality.</summary>
	public class TestDeleteBlockPool
	{
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestDeleteBlockPool()
		{
			// Start cluster with a 2 NN and 2 DN
			Configuration conf = new Configuration();
			MiniDFSCluster cluster = null;
			try
			{
				conf.Set(DFSConfigKeys.DfsNameservices, "namesServerId1,namesServerId2");
				cluster = new MiniDFSCluster.Builder(conf).NnTopology(MiniDFSNNTopology.SimpleFederatedTopology
					(conf.Get(DFSConfigKeys.DfsNameservices))).NumDataNodes(2).Build();
				cluster.WaitActive();
				FileSystem fs1 = cluster.GetFileSystem(0);
				FileSystem fs2 = cluster.GetFileSystem(1);
				DFSTestUtil.CreateFile(fs1, new Path("/alpha"), 1024, (short)2, 54);
				DFSTestUtil.CreateFile(fs2, new Path("/beta"), 1024, (short)2, 54);
				DataNode dn1 = cluster.GetDataNodes()[0];
				DataNode dn2 = cluster.GetDataNodes()[1];
				string bpid1 = cluster.GetNamesystem(0).GetBlockPoolId();
				string bpid2 = cluster.GetNamesystem(1).GetBlockPoolId();
				FilePath dn1StorageDir1 = cluster.GetInstanceStorageDir(0, 0);
				FilePath dn1StorageDir2 = cluster.GetInstanceStorageDir(0, 1);
				FilePath dn2StorageDir1 = cluster.GetInstanceStorageDir(1, 0);
				FilePath dn2StorageDir2 = cluster.GetInstanceStorageDir(1, 1);
				// Although namenode is shutdown, the bp offerservice is still running
				try
				{
					dn1.DeleteBlockPool(bpid1, true);
					NUnit.Framework.Assert.Fail("Must not delete a running block pool");
				}
				catch (IOException)
				{
				}
				Configuration nn1Conf = cluster.GetConfiguration(1);
				nn1Conf.Set(DFSConfigKeys.DfsNameservices, "namesServerId2");
				dn1.RefreshNamenodes(nn1Conf);
				NUnit.Framework.Assert.AreEqual(1, dn1.GetAllBpOs().Length);
				try
				{
					dn1.DeleteBlockPool(bpid1, false);
					NUnit.Framework.Assert.Fail("Must not delete if any block files exist unless " + 
						"force is true");
				}
				catch (IOException)
				{
				}
				VerifyBlockPoolDirectories(true, dn1StorageDir1, bpid1);
				VerifyBlockPoolDirectories(true, dn1StorageDir2, bpid1);
				dn1.DeleteBlockPool(bpid1, true);
				VerifyBlockPoolDirectories(false, dn1StorageDir1, bpid1);
				VerifyBlockPoolDirectories(false, dn1StorageDir2, bpid1);
				fs1.Delete(new Path("/alpha"), true);
				// Wait till all blocks are deleted from the dn2 for bpid1.
				FilePath finalDir1 = MiniDFSCluster.GetFinalizedDir(dn2StorageDir1, bpid1);
				FilePath finalDir2 = MiniDFSCluster.GetFinalizedDir(dn2StorageDir1, bpid2);
				while ((!DatanodeUtil.DirNoFilesRecursive(finalDir1)) || (!DatanodeUtil.DirNoFilesRecursive
					(finalDir2)))
				{
					try
					{
						Sharpen.Thread.Sleep(3000);
					}
					catch (Exception)
					{
					}
				}
				cluster.ShutdownNameNode(0);
				// Although namenode is shutdown, the bp offerservice is still running 
				// on dn2
				try
				{
					dn2.DeleteBlockPool(bpid1, true);
					NUnit.Framework.Assert.Fail("Must not delete a running block pool");
				}
				catch (IOException)
				{
				}
				dn2.RefreshNamenodes(nn1Conf);
				NUnit.Framework.Assert.AreEqual(1, dn2.GetAllBpOs().Length);
				VerifyBlockPoolDirectories(true, dn2StorageDir1, bpid1);
				VerifyBlockPoolDirectories(true, dn2StorageDir2, bpid1);
				// Now deleteBlockPool must succeed with force as false, because no 
				// blocks exist for bpid1 and bpOfferService is also stopped for bpid1.
				dn2.DeleteBlockPool(bpid1, false);
				VerifyBlockPoolDirectories(false, dn2StorageDir1, bpid1);
				VerifyBlockPoolDirectories(false, dn2StorageDir2, bpid1);
				//bpid2 must not be impacted
				VerifyBlockPoolDirectories(true, dn1StorageDir1, bpid2);
				VerifyBlockPoolDirectories(true, dn1StorageDir2, bpid2);
				VerifyBlockPoolDirectories(true, dn2StorageDir1, bpid2);
				VerifyBlockPoolDirectories(true, dn2StorageDir2, bpid2);
				//make sure second block pool is running all fine
				Path gammaFile = new Path("/gamma");
				DFSTestUtil.CreateFile(fs2, gammaFile, 1024, (short)1, 55);
				fs2.SetReplication(gammaFile, (short)2);
				DFSTestUtil.WaitReplication(fs2, gammaFile, (short)2);
			}
			finally
			{
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestDfsAdminDeleteBlockPool()
		{
			Configuration conf = new Configuration();
			MiniDFSCluster cluster = null;
			try
			{
				conf.Set(DFSConfigKeys.DfsNameservices, "namesServerId1,namesServerId2");
				cluster = new MiniDFSCluster.Builder(conf).NnTopology(MiniDFSNNTopology.SimpleFederatedTopology
					(conf.Get(DFSConfigKeys.DfsNameservices))).NumDataNodes(1).Build();
				cluster.WaitActive();
				FileSystem fs1 = cluster.GetFileSystem(0);
				FileSystem fs2 = cluster.GetFileSystem(1);
				DFSTestUtil.CreateFile(fs1, new Path("/alpha"), 1024, (short)1, 54);
				DFSTestUtil.CreateFile(fs2, new Path("/beta"), 1024, (short)1, 54);
				DataNode dn1 = cluster.GetDataNodes()[0];
				string bpid1 = cluster.GetNamesystem(0).GetBlockPoolId();
				string bpid2 = cluster.GetNamesystem(1).GetBlockPoolId();
				FilePath dn1StorageDir1 = cluster.GetInstanceStorageDir(0, 0);
				FilePath dn1StorageDir2 = cluster.GetInstanceStorageDir(0, 1);
				Configuration nn1Conf = cluster.GetConfiguration(0);
				nn1Conf.Set(DFSConfigKeys.DfsNameservices, "namesServerId1");
				dn1.RefreshNamenodes(nn1Conf);
				NUnit.Framework.Assert.AreEqual(1, dn1.GetAllBpOs().Length);
				DFSAdmin admin = new DFSAdmin(nn1Conf);
				string dn1Address = dn1.GetDatanodeId().GetIpAddr() + ":" + dn1.GetIpcPort();
				string[] args = new string[] { "-deleteBlockPool", dn1Address, bpid2 };
				int ret = admin.Run(args);
				NUnit.Framework.Assert.IsFalse(0 == ret);
				VerifyBlockPoolDirectories(true, dn1StorageDir1, bpid2);
				VerifyBlockPoolDirectories(true, dn1StorageDir2, bpid2);
				string[] forceArgs = new string[] { "-deleteBlockPool", dn1Address, bpid2, "force"
					 };
				ret = admin.Run(forceArgs);
				NUnit.Framework.Assert.AreEqual(0, ret);
				VerifyBlockPoolDirectories(false, dn1StorageDir1, bpid2);
				VerifyBlockPoolDirectories(false, dn1StorageDir2, bpid2);
				//bpid1 remains good
				VerifyBlockPoolDirectories(true, dn1StorageDir1, bpid1);
				VerifyBlockPoolDirectories(true, dn1StorageDir2, bpid1);
			}
			finally
			{
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void VerifyBlockPoolDirectories(bool shouldExist, FilePath storageDir, string
			 bpid)
		{
			FilePath bpDir = new FilePath(storageDir, DataStorage.StorageDirCurrent + "/" + bpid
				);
			if (shouldExist == false)
			{
				NUnit.Framework.Assert.IsFalse(bpDir.Exists());
			}
			else
			{
				FilePath bpCurrentDir = new FilePath(bpDir, DataStorage.StorageDirCurrent);
				FilePath finalizedDir = new FilePath(bpCurrentDir, DataStorage.StorageDirFinalized
					);
				FilePath rbwDir = new FilePath(bpCurrentDir, DataStorage.StorageDirRbw);
				FilePath versionFile = new FilePath(bpCurrentDir, "VERSION");
				NUnit.Framework.Assert.IsTrue(finalizedDir.IsDirectory());
				NUnit.Framework.Assert.IsTrue(rbwDir.IsDirectory());
				NUnit.Framework.Assert.IsTrue(versionFile.Exists());
			}
		}
	}
}
