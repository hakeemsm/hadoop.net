using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Base;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Datanode;
using Org.Apache.Hadoop.Hdfs.Server.Datanode.Fsdataset;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Org.Apache.Hadoop.Test;
using Org.Hamcrest.Core;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Blockmanagement
{
	public class TestNameNodePrunesMissingStorages
	{
		internal static readonly Log Log = LogFactory.GetLog(typeof(TestNameNodePrunesMissingStorages
			));

		/// <exception cref="System.IO.IOException"/>
		private static void RunTest(string testCaseName, bool createFiles, int numInitialStorages
			, int expectedStoragesAfterTest)
		{
			Configuration conf = new HdfsConfiguration();
			MiniDFSCluster cluster = null;
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(1).StoragesPerDatanode(numInitialStorages
					).Build();
				cluster.WaitActive();
				DataNode dn0 = cluster.GetDataNodes()[0];
				// Ensure NN knows about the storage.
				DatanodeID dnId = dn0.GetDatanodeId();
				DatanodeDescriptor dnDescriptor = cluster.GetNamesystem().GetBlockManager().GetDatanodeManager
					().GetDatanode(dnId);
				Assert.AssertThat(dnDescriptor.GetStorageInfos().Length, IS.Is(numInitialStorages
					));
				string bpid = cluster.GetNamesystem().GetBlockPoolId();
				DatanodeRegistration dnReg = dn0.GetDNRegistrationForBP(bpid);
				DataNodeTestUtils.TriggerBlockReport(dn0);
				if (createFiles)
				{
					Path path = new Path("/", testCaseName);
					DFSTestUtil.CreateFile(cluster.GetFileSystem(), path, 1024, (short)1, unchecked((
						int)(0x1BAD5EED)));
					DataNodeTestUtils.TriggerBlockReport(dn0);
				}
				// Generate a fake StorageReport that is missing one storage.
				StorageReport[] reports = dn0.GetFSDataset().GetStorageReports(bpid);
				StorageReport[] prunedReports = new StorageReport[numInitialStorages - 1];
				System.Array.Copy(reports, 0, prunedReports, 0, prunedReports.Length);
				// Stop the DataNode and send fake heartbeat with missing storage.
				cluster.StopDataNode(0);
				cluster.GetNameNodeRpc().SendHeartbeat(dnReg, prunedReports, 0L, 0L, 0, 0, 0, null
					);
				// Check that the missing storage was pruned.
				Assert.AssertThat(dnDescriptor.GetStorageInfos().Length, IS.Is(expectedStoragesAfterTest
					));
			}
			finally
			{
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		/// <summary>
		/// Test that the NameNode prunes empty storage volumes that are no longer
		/// reported by the DataNode.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestUnusedStorageIsPruned()
		{
			// Run the test with 1 storage, after the text expect 0 storages.
			RunTest(GenericTestUtils.GetMethodName(), false, 1, 0);
		}

		/// <summary>
		/// Verify that the NameNode does not prune storages with blocks
		/// simply as a result of a heartbeat being sent missing that storage.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestStorageWithBlocksIsNotPruned()
		{
			// Run the test with 1 storage, after the text still expect 1 storage.
			RunTest(GenericTestUtils.GetMethodName(), true, 1, 1);
		}

		/// <summary>
		/// Regression test for HDFS-7960.<p/>
		/// Shutting down a datanode, removing a storage directory, and restarting
		/// the DataNode should not produce zombie storages.
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestRemovingStorageDoesNotProduceZombies()
		{
			Configuration conf = new HdfsConfiguration();
			conf.SetInt(DFSConfigKeys.DfsDatanodeFailedVolumesToleratedKey, 1);
			int NumStoragesPerDn = 2;
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(3).StoragesPerDatanode
				(NumStoragesPerDn).Build();
			try
			{
				cluster.WaitActive();
				foreach (DataNode dn in cluster.GetDataNodes())
				{
					NUnit.Framework.Assert.AreEqual(NumStoragesPerDn, cluster.GetNamesystem().GetBlockManager
						().GetDatanodeManager().GetDatanode(dn.GetDatanodeId()).GetStorageInfos().Length
						);
				}
				// Create a file which will end up on all 3 datanodes.
				Path TestPath = new Path("/foo1");
				DistributedFileSystem fs = cluster.GetFileSystem();
				DFSTestUtil.CreateFile(fs, TestPath, 1024, (short)3, unchecked((int)(0xcafecafe))
					);
				foreach (DataNode dn_1 in cluster.GetDataNodes())
				{
					DataNodeTestUtils.TriggerBlockReport(dn_1);
				}
				ExtendedBlock block = DFSTestUtil.GetFirstBlock(fs, new Path("/foo1"));
				cluster.GetNamesystem().WriteLock();
				string storageIdToRemove;
				string datanodeUuid;
				// Find the first storage which this block is in.
				try
				{
					IEnumerator<DatanodeStorageInfo> storageInfoIter = cluster.GetNamesystem().GetBlockManager
						().GetStorages(block.GetLocalBlock()).GetEnumerator();
					NUnit.Framework.Assert.IsTrue(storageInfoIter.HasNext());
					DatanodeStorageInfo info = storageInfoIter.Next();
					storageIdToRemove = info.GetStorageID();
					datanodeUuid = info.GetDatanodeDescriptor().GetDatanodeUuid();
				}
				finally
				{
					cluster.GetNamesystem().WriteUnlock();
				}
				// Find the DataNode which holds that first storage.
				DataNode datanodeToRemoveStorageFrom;
				int datanodeToRemoveStorageFromIdx = 0;
				while (true)
				{
					if (datanodeToRemoveStorageFromIdx >= cluster.GetDataNodes().Count)
					{
						NUnit.Framework.Assert.Fail("failed to find datanode with uuid " + datanodeUuid);
						datanodeToRemoveStorageFrom = null;
						break;
					}
					DataNode dn_2 = cluster.GetDataNodes()[datanodeToRemoveStorageFromIdx];
					if (dn_2.GetDatanodeUuid().Equals(datanodeUuid))
					{
						datanodeToRemoveStorageFrom = dn_2;
						break;
					}
					datanodeToRemoveStorageFromIdx++;
				}
				// Find the volume within the datanode which holds that first storage.
				IList<FsVolumeSpi> volumes = datanodeToRemoveStorageFrom.GetFSDataset().GetVolumes
					();
				NUnit.Framework.Assert.AreEqual(NumStoragesPerDn, volumes.Count);
				string volumeDirectoryToRemove = null;
				foreach (FsVolumeSpi volume in volumes)
				{
					if (volume.GetStorageID().Equals(storageIdToRemove))
					{
						volumeDirectoryToRemove = volume.GetBasePath();
					}
				}
				// Shut down the datanode and remove the volume.
				// Replace the volume directory with a regular file, which will
				// cause a volume failure.  (If we merely removed the directory,
				// it would be re-initialized with a new storage ID.)
				NUnit.Framework.Assert.IsNotNull(volumeDirectoryToRemove);
				datanodeToRemoveStorageFrom.Shutdown();
				FileUtil.FullyDelete(new FilePath(volumeDirectoryToRemove));
				FileOutputStream fos = new FileOutputStream(volumeDirectoryToRemove);
				try
				{
					fos.Write(1);
				}
				finally
				{
					fos.Close();
				}
				cluster.RestartDataNode(datanodeToRemoveStorageFromIdx);
				// Wait for the NameNode to remove the storage.
				Log.Info("waiting for the datanode to remove " + storageIdToRemove);
				GenericTestUtils.WaitFor(new _Supplier_227(cluster, datanodeToRemoveStorageFrom, 
					storageIdToRemove, NumStoragesPerDn), 10, 30000);
			}
			finally
			{
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		private sealed class _Supplier_227 : Supplier<bool>
		{
			public _Supplier_227(MiniDFSCluster cluster, DataNode datanodeToRemoveStorageFrom
				, string storageIdToRemove, int NumStoragesPerDn)
			{
				this.cluster = cluster;
				this.datanodeToRemoveStorageFrom = datanodeToRemoveStorageFrom;
				this.storageIdToRemove = storageIdToRemove;
				this.NumStoragesPerDn = NumStoragesPerDn;
			}

			public bool Get()
			{
				DatanodeDescriptor dnDescriptor = cluster.GetNamesystem().GetBlockManager().GetDatanodeManager
					().GetDatanode(datanodeToRemoveStorageFrom.GetDatanodeUuid());
				NUnit.Framework.Assert.IsNotNull(dnDescriptor);
				DatanodeStorageInfo[] infos = dnDescriptor.GetStorageInfos();
				foreach (DatanodeStorageInfo info in infos)
				{
					if (info.GetStorageID().Equals(storageIdToRemove))
					{
						TestNameNodePrunesMissingStorages.Log.Info("Still found storage " + storageIdToRemove
							 + " on " + info + ".");
						return false;
					}
				}
				NUnit.Framework.Assert.AreEqual(NumStoragesPerDn - 1, infos.Length);
				return true;
			}

			private readonly MiniDFSCluster cluster;

			private readonly DataNode datanodeToRemoveStorageFrom;

			private readonly string storageIdToRemove;

			private readonly int NumStoragesPerDn;
		}
	}
}
