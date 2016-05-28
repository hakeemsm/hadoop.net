using System;
using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Datanode;
using Org.Apache.Hadoop.Hdfs.Server.Datanode.Fsdataset.Impl;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Security;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	public class TestLeaseRecovery
	{
		internal const int BlockSize = 1024;

		internal const short ReplicationNum = (short)3;

		private const long LeasePeriod = 300L;

		private MiniDFSCluster cluster;

		/// <exception cref="System.IO.IOException"/>
		[TearDown]
		public virtual void Shutdown()
		{
			if (cluster != null)
			{
				cluster.Shutdown();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		internal static void CheckMetaInfo(ExtendedBlock b, DataNode dn)
		{
			TestInterDatanodeProtocol.CheckMetaInfo(b, dn);
		}

		internal static int Min(params int[] x)
		{
			int m = x[0];
			for (int i = 1; i < x.Length; i++)
			{
				if (x[i] < m)
				{
					m = x[i];
				}
			}
			return m;
		}

		internal virtual void WaitLeaseRecovery(MiniDFSCluster cluster)
		{
			cluster.SetLeasePeriod(LeasePeriod, LeasePeriod);
			// wait for the lease to expire
			try
			{
				Sharpen.Thread.Sleep(2 * 3000);
			}
			catch (Exception)
			{
			}
		}

		// 2 heartbeat intervals
		/// <summary>The following test first creates a file with a few blocks.</summary>
		/// <remarks>
		/// The following test first creates a file with a few blocks.
		/// It randomly truncates the replica of the last block stored in each datanode.
		/// Finally, it triggers block synchronization to synchronize all stored block.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestBlockSynchronization()
		{
			int OrgFileSize = 3000;
			Configuration conf = new HdfsConfiguration();
			conf.SetLong(DFSConfigKeys.DfsBlockSizeKey, BlockSize);
			cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(5).Build();
			cluster.WaitActive();
			//create a file
			DistributedFileSystem dfs = cluster.GetFileSystem();
			string filestr = "/foo";
			Path filepath = new Path(filestr);
			DFSTestUtil.CreateFile(dfs, filepath, OrgFileSize, ReplicationNum, 0L);
			NUnit.Framework.Assert.IsTrue(dfs.Exists(filepath));
			DFSTestUtil.WaitReplication(dfs, filepath, ReplicationNum);
			//get block info for the last block
			LocatedBlock locatedblock = TestInterDatanodeProtocol.GetLastLocatedBlock(dfs.dfs
				.GetNamenode(), filestr);
			DatanodeInfo[] datanodeinfos = locatedblock.GetLocations();
			NUnit.Framework.Assert.AreEqual(ReplicationNum, datanodeinfos.Length);
			//connect to data nodes
			DataNode[] datanodes = new DataNode[ReplicationNum];
			for (int i = 0; i < ReplicationNum; i++)
			{
				datanodes[i] = cluster.GetDataNode(datanodeinfos[i].GetIpcPort());
				NUnit.Framework.Assert.IsTrue(datanodes[i] != null);
			}
			//verify Block Info
			ExtendedBlock lastblock = locatedblock.GetBlock();
			DataNode.Log.Info("newblocks=" + lastblock);
			for (int i_1 = 0; i_1 < ReplicationNum; i_1++)
			{
				CheckMetaInfo(lastblock, datanodes[i_1]);
			}
			DataNode.Log.Info("dfs.dfs.clientName=" + dfs.dfs.clientName);
			cluster.GetNameNodeRpc().Append(filestr, dfs.dfs.clientName, new EnumSetWritable<
				CreateFlag>(EnumSet.Of(CreateFlag.Append)));
			// expire lease to trigger block recovery.
			WaitLeaseRecovery(cluster);
			Block[] updatedmetainfo = new Block[ReplicationNum];
			long oldSize = lastblock.GetNumBytes();
			lastblock = TestInterDatanodeProtocol.GetLastLocatedBlock(dfs.dfs.GetNamenode(), 
				filestr).GetBlock();
			long currentGS = lastblock.GetGenerationStamp();
			for (int i_2 = 0; i_2 < ReplicationNum; i_2++)
			{
				updatedmetainfo[i_2] = DataNodeTestUtils.GetFSDataset(datanodes[i_2]).GetStoredBlock
					(lastblock.GetBlockPoolId(), lastblock.GetBlockId());
				NUnit.Framework.Assert.AreEqual(lastblock.GetBlockId(), updatedmetainfo[i_2].GetBlockId
					());
				NUnit.Framework.Assert.AreEqual(oldSize, updatedmetainfo[i_2].GetNumBytes());
				NUnit.Framework.Assert.AreEqual(currentGS, updatedmetainfo[i_2].GetGenerationStamp
					());
			}
			// verify that lease recovery does not occur when namenode is in safemode
			System.Console.Out.WriteLine("Testing that lease recovery cannot happen during safemode."
				);
			filestr = "/foo.safemode";
			filepath = new Path(filestr);
			dfs.Create(filepath, (short)1);
			cluster.GetNameNodeRpc().SetSafeMode(HdfsConstants.SafeModeAction.SafemodeEnter, 
				false);
			NUnit.Framework.Assert.IsTrue(dfs.dfs.Exists(filestr));
			DFSTestUtil.WaitReplication(dfs, filepath, (short)1);
			WaitLeaseRecovery(cluster);
			// verify that we still cannot recover the lease
			LeaseManager lm = NameNodeAdapter.GetLeaseManager(cluster.GetNamesystem());
			NUnit.Framework.Assert.IsTrue("Found " + lm.CountLease() + " lease, expected 1", 
				lm.CountLease() == 1);
			cluster.GetNameNodeRpc().SetSafeMode(HdfsConstants.SafeModeAction.SafemodeLeave, 
				false);
		}

		/// <summary>
		/// Block Recovery when the meta file not having crcs for all chunks in block
		/// file
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestBlockRecoveryWithLessMetafile()
		{
			Configuration conf = new Configuration();
			conf.Set(DFSConfigKeys.DfsBlockLocalPathAccessUserKey, UserGroupInformation.GetCurrentUser
				().GetShortUserName());
			cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(1).Build();
			Path file = new Path("/testRecoveryFile");
			DistributedFileSystem dfs = cluster.GetFileSystem();
			FSDataOutputStream @out = dfs.Create(file);
			int count = 0;
			while (count < 2 * 1024 * 1024)
			{
				@out.WriteBytes("Data");
				count += 4;
			}
			@out.Hsync();
			// abort the original stream
			((DFSOutputStream)@out.GetWrappedStream()).Abort();
			LocatedBlocks locations = cluster.GetNameNodeRpc().GetBlockLocations(file.ToString
				(), 0, count);
			ExtendedBlock block = locations.Get(0).GetBlock();
			DataNode dn = cluster.GetDataNodes()[0];
			BlockLocalPathInfo localPathInfo = dn.GetBlockLocalPathInfo(block, null);
			FilePath metafile = new FilePath(localPathInfo.GetMetaPath());
			NUnit.Framework.Assert.IsTrue(metafile.Exists());
			// reduce the block meta file size
			RandomAccessFile raf = new RandomAccessFile(metafile, "rw");
			raf.SetLength(metafile.Length() - 20);
			raf.Close();
			// restart DN to make replica to RWR
			MiniDFSCluster.DataNodeProperties dnProp = cluster.StopDataNode(0);
			cluster.RestartDataNode(dnProp, true);
			// try to recover the lease
			DistributedFileSystem newdfs = (DistributedFileSystem)FileSystem.NewInstance(cluster
				.GetConfiguration(0));
			count = 0;
			while (++count < 10 && !newdfs.RecoverLease(file))
			{
				Sharpen.Thread.Sleep(1000);
			}
			NUnit.Framework.Assert.IsTrue("File should be closed", newdfs.RecoverLease(file));
		}

		/// <summary>Recover the lease on a file and append file from another client.</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestLeaseRecoveryAndAppend()
		{
			Configuration conf = new Configuration();
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(1).Build();
				Path file = new Path("/testLeaseRecovery");
				DistributedFileSystem dfs = cluster.GetFileSystem();
				// create a file with 0 bytes
				FSDataOutputStream @out = dfs.Create(file);
				@out.Hflush();
				@out.Hsync();
				// abort the original stream
				((DFSOutputStream)@out.GetWrappedStream()).Abort();
				DistributedFileSystem newdfs = (DistributedFileSystem)FileSystem.NewInstance(cluster
					.GetConfiguration(0));
				// Append to a file , whose lease is held by another client should fail
				try
				{
					newdfs.Append(file);
					NUnit.Framework.Assert.Fail("Append to a file(lease is held by another client) should fail"
						);
				}
				catch (RemoteException e)
				{
					NUnit.Framework.Assert.IsTrue(e.Message.Contains("file lease is currently owned")
						);
				}
				// Lease recovery on first try should be successful
				bool recoverLease = newdfs.RecoverLease(file);
				NUnit.Framework.Assert.IsTrue(recoverLease);
				FSDataOutputStream append = newdfs.Append(file);
				append.Write(Sharpen.Runtime.GetBytesForString("test"));
				append.Close();
			}
			finally
			{
				if (cluster != null)
				{
					cluster.Shutdown();
					cluster = null;
				}
			}
		}
	}
}
