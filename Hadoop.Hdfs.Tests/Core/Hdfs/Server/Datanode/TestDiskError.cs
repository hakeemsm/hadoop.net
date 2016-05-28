using System.IO;
using System.Net;
using System.Net.Sockets;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Protocol.Datatransfer;
using Org.Apache.Hadoop.Hdfs.Security.Token.Block;
using Org.Apache.Hadoop.Hdfs.Server.Datanode.Fsdataset;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Datanode
{
	/// <summary>Test that datanodes can correctly handle errors during block read/write.
	/// 	</summary>
	public class TestDiskError
	{
		private FileSystem fs;

		private MiniDFSCluster cluster;

		private Configuration conf;

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public virtual void SetUp()
		{
			conf = new HdfsConfiguration();
			conf.SetLong(DFSConfigKeys.DfsBlockSizeKey, 512L);
			cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(1).Build();
			cluster.WaitActive();
			fs = cluster.GetFileSystem();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.TearDown]
		public virtual void TearDown()
		{
			cluster.Shutdown();
		}

		/// <summary>Test to check that a DN goes down when all its volumes have failed.</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestShutdown()
		{
			if (Runtime.GetProperty("os.name").StartsWith("Windows"))
			{
				return;
			}
			// Bring up two more datanodes
			cluster.StartDataNodes(conf, 2, true, null, null);
			cluster.WaitActive();
			int dnIndex = 0;
			string bpid = cluster.GetNamesystem().GetBlockPoolId();
			FilePath storageDir = cluster.GetInstanceStorageDir(dnIndex, 0);
			FilePath dir1 = MiniDFSCluster.GetRbwDir(storageDir, bpid);
			storageDir = cluster.GetInstanceStorageDir(dnIndex, 1);
			FilePath dir2 = MiniDFSCluster.GetRbwDir(storageDir, bpid);
			try
			{
				// make the data directory of the first datanode to be readonly
				NUnit.Framework.Assert.IsTrue("Couldn't chmod local vol", dir1.SetReadOnly());
				NUnit.Framework.Assert.IsTrue("Couldn't chmod local vol", dir2.SetReadOnly());
				// create files and make sure that first datanode will be down
				DataNode dn = cluster.GetDataNodes()[dnIndex];
				for (int i = 0; dn.IsDatanodeUp(); i++)
				{
					Path fileName = new Path("/test.txt" + i);
					DFSTestUtil.CreateFile(fs, fileName, 1024, (short)2, 1L);
					DFSTestUtil.WaitReplication(fs, fileName, (short)2);
					fs.Delete(fileName, true);
				}
			}
			finally
			{
				// restore its old permission
				FileUtil.SetWritable(dir1, true);
				FileUtil.SetWritable(dir2, true);
			}
		}

		/// <summary>
		/// Test that when there is a failure replicating a block the temporary
		/// and meta files are cleaned up and subsequent replication succeeds.
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestReplicationError()
		{
			// create a file of replication factor of 1
			Path fileName = new Path("/test.txt");
			int fileLen = 1;
			DFSTestUtil.CreateFile(fs, fileName, 1, (short)1, 1L);
			DFSTestUtil.WaitReplication(fs, fileName, (short)1);
			// get the block belonged to the created file
			LocatedBlocks blocks = NameNodeAdapter.GetBlockLocations(cluster.GetNameNode(), fileName
				.ToString(), 0, (long)fileLen);
			NUnit.Framework.Assert.AreEqual("Should only find 1 block", blocks.LocatedBlockCount
				(), 1);
			LocatedBlock block = blocks.Get(0);
			// bring up a second datanode
			cluster.StartDataNodes(conf, 1, true, null, null);
			cluster.WaitActive();
			int sndNode = 1;
			DataNode datanode = cluster.GetDataNodes()[sndNode];
			// replicate the block to the second datanode
			IPEndPoint target = datanode.GetXferAddress();
			Socket s = Sharpen.Extensions.CreateSocket(target.Address, target.Port);
			// write the header.
			DataOutputStream @out = new DataOutputStream(s.GetOutputStream());
			DataChecksum checksum = DataChecksum.NewDataChecksum(DataChecksum.Type.Crc32, 512
				);
			new Sender(@out).WriteBlock(block.GetBlock(), StorageType.Default, BlockTokenSecretManager
				.DummyToken, string.Empty, new DatanodeInfo[0], new StorageType[0], null, BlockConstructionStage
				.PipelineSetupCreate, 1, 0L, 0L, 0L, checksum, CachingStrategy.NewDefaultStrategy
				(), false, false, null);
			@out.Flush();
			// close the connection before sending the content of the block
			@out.Close();
			// the temporary block & meta files should be deleted
			string bpid = cluster.GetNamesystem().GetBlockPoolId();
			FilePath storageDir = cluster.GetInstanceStorageDir(sndNode, 0);
			FilePath dir1 = MiniDFSCluster.GetRbwDir(storageDir, bpid);
			storageDir = cluster.GetInstanceStorageDir(sndNode, 1);
			FilePath dir2 = MiniDFSCluster.GetRbwDir(storageDir, bpid);
			while (dir1.ListFiles().Length != 0 || dir2.ListFiles().Length != 0)
			{
				Sharpen.Thread.Sleep(100);
			}
			// then increase the file's replication factor
			fs.SetReplication(fileName, (short)2);
			// replication should succeed
			DFSTestUtil.WaitReplication(fs, fileName, (short)1);
			// clean up the file
			fs.Delete(fileName, false);
		}

		/// <summary>Check that the permissions of the local DN directories are as expected.</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestLocalDirs()
		{
			Configuration conf = new Configuration();
			string permStr = conf.Get(DFSConfigKeys.DfsDatanodeDataDirPermissionKey);
			FsPermission expected = new FsPermission(permStr);
			// Check permissions on directories in 'dfs.datanode.data.dir'
			FileSystem localFS = FileSystem.GetLocal(conf);
			foreach (DataNode dn in cluster.GetDataNodes())
			{
				foreach (FsVolumeSpi v in dn.GetFSDataset().GetVolumes())
				{
					string dir = v.GetBasePath();
					Path dataDir = new Path(dir);
					FsPermission actual = localFS.GetFileStatus(dataDir).GetPermission();
					NUnit.Framework.Assert.AreEqual("Permission for dir: " + dataDir + ", is " + actual
						 + ", while expected is " + expected, expected, actual);
				}
			}
		}

		/// <summary>
		/// Checks whether
		/// <see cref="DataNode.CheckDiskErrorAsync()"/>
		/// is being called or not.
		/// Before refactoring the code the above function was not getting called
		/// </summary>
		/// <exception cref="System.IO.IOException">, InterruptedException</exception>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestcheckDiskError()
		{
			if (cluster.GetDataNodes().Count <= 0)
			{
				cluster.StartDataNodes(conf, 1, true, null, null);
				cluster.WaitActive();
			}
			DataNode dataNode = cluster.GetDataNodes()[0];
			long slackTime = dataNode.checkDiskErrorInterval / 2;
			//checking for disk error
			dataNode.CheckDiskErrorAsync();
			Sharpen.Thread.Sleep(dataNode.checkDiskErrorInterval);
			long lastDiskErrorCheck = dataNode.GetLastDiskErrorCheck();
			NUnit.Framework.Assert.IsTrue("Disk Error check is not performed within  " + dataNode
				.checkDiskErrorInterval + "  ms", ((Time.MonotonicNow() - lastDiskErrorCheck) < 
				(dataNode.checkDiskErrorInterval + slackTime)));
		}
	}
}
