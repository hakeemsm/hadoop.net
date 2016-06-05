using Javax.Management;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Blockmanagement;
using Sharpen;
using Sharpen.Management;

namespace Org.Apache.Hadoop.Hdfs
{
	/// <summary>
	/// The test makes sure that NameNode detects presense blocks that do not have
	/// any valid replicas.
	/// </summary>
	/// <remarks>
	/// The test makes sure that NameNode detects presense blocks that do not have
	/// any valid replicas. In addition, it verifies that HDFS front page displays
	/// a warning in such a case.
	/// </remarks>
	public class TestMissingBlocksAlert
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Hdfs.TestMissingBlocksAlert
			));

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		/// <exception cref="Javax.Management.MalformedObjectNameException"/>
		/// <exception cref="Javax.Management.AttributeNotFoundException"/>
		/// <exception cref="Javax.Management.MBeanException"/>
		/// <exception cref="Javax.Management.ReflectionException"/>
		/// <exception cref="Javax.Management.InstanceNotFoundException"/>
		[NUnit.Framework.Test]
		public virtual void TestMissingBlocksAlert()
		{
			MiniDFSCluster cluster = null;
			try
			{
				Configuration conf = new HdfsConfiguration();
				//minimize test delay
				conf.SetInt(DFSConfigKeys.DfsNamenodeReplicationIntervalKey, 0);
				conf.SetInt(DFSConfigKeys.DfsClientRetryWindowBase, 10);
				int fileLen = 10 * 1024;
				conf.SetInt(DFSConfigKeys.DfsBlockSizeKey, fileLen / 2);
				//start a cluster with single datanode
				cluster = new MiniDFSCluster.Builder(conf).Build();
				cluster.WaitActive();
				BlockManager bm = cluster.GetNamesystem().GetBlockManager();
				DistributedFileSystem dfs = cluster.GetFileSystem();
				// create a normal file
				DFSTestUtil.CreateFile(dfs, new Path("/testMissingBlocksAlert/file1"), fileLen, (
					short)3, 0);
				Path corruptFile = new Path("/testMissingBlocks/corruptFile");
				DFSTestUtil.CreateFile(dfs, corruptFile, fileLen, (short)3, 0);
				// Corrupt the block
				ExtendedBlock block = DFSTestUtil.GetFirstBlock(dfs, corruptFile);
				NUnit.Framework.Assert.IsTrue(cluster.CorruptReplica(0, block));
				// read the file so that the corrupt block is reported to NN
				FSDataInputStream @in = dfs.Open(corruptFile);
				try
				{
					@in.ReadFully(new byte[fileLen]);
				}
				catch (ChecksumException)
				{
				}
				// checksum error is expected.      
				@in.Close();
				Log.Info("Waiting for missing blocks count to increase...");
				while (dfs.GetMissingBlocksCount() <= 0)
				{
					Sharpen.Thread.Sleep(100);
				}
				NUnit.Framework.Assert.IsTrue(dfs.GetMissingBlocksCount() == 1);
				NUnit.Framework.Assert.AreEqual(4, dfs.GetUnderReplicatedBlocksCount());
				NUnit.Framework.Assert.AreEqual(3, bm.GetUnderReplicatedNotMissingBlocks());
				MBeanServer mbs = ManagementFactory.GetPlatformMBeanServer();
				ObjectName mxbeanName = new ObjectName("Hadoop:service=NameNode,name=NameNodeInfo"
					);
				NUnit.Framework.Assert.AreEqual(1, (long)(long)mbs.GetAttribute(mxbeanName, "NumberOfMissingBlocks"
					));
				// now do the reverse : remove the file expect the number of missing 
				// blocks to go to zero
				dfs.Delete(corruptFile, true);
				Log.Info("Waiting for missing blocks count to be zero...");
				while (dfs.GetMissingBlocksCount() > 0)
				{
					Sharpen.Thread.Sleep(100);
				}
				NUnit.Framework.Assert.AreEqual(2, dfs.GetUnderReplicatedBlocksCount());
				NUnit.Framework.Assert.AreEqual(2, bm.GetUnderReplicatedNotMissingBlocks());
				NUnit.Framework.Assert.AreEqual(0, (long)(long)mbs.GetAttribute(mxbeanName, "NumberOfMissingBlocks"
					));
				Path replOneFile = new Path("/testMissingBlocks/replOneFile");
				DFSTestUtil.CreateFile(dfs, replOneFile, fileLen, (short)1, 0);
				ExtendedBlock replOneBlock = DFSTestUtil.GetFirstBlock(dfs, replOneFile);
				NUnit.Framework.Assert.IsTrue(cluster.CorruptReplica(0, replOneBlock));
				// read the file so that the corrupt block is reported to NN
				@in = dfs.Open(replOneFile);
				try
				{
					@in.ReadFully(new byte[fileLen]);
				}
				catch (ChecksumException)
				{
				}
				// checksum error is expected.
				@in.Close();
				NUnit.Framework.Assert.AreEqual(1, dfs.GetMissingReplOneBlocksCount());
				NUnit.Framework.Assert.AreEqual(1, (long)(long)mbs.GetAttribute(mxbeanName, "NumberOfMissingBlocksWithReplicationFactorOne"
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
	}
}
