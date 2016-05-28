using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Hdfs.Tools;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode.HA
{
	public class TestHAAppend
	{
		internal const int Count = 5;

		/// <exception cref="System.IO.IOException"/>
		internal static FSDataOutputStream CreateAndHflush(FileSystem fs, Path file, byte
			[] data, int length)
		{
			FSDataOutputStream @out = fs.Create(file, false, 4096, (short)3, 1024);
			@out.Write(data, 0, length);
			@out.Hflush();
			return @out;
		}

		/// <summary>
		/// Test to verify the processing of PendingDataNodeMessageQueue in case of
		/// append.
		/// </summary>
		/// <remarks>
		/// Test to verify the processing of PendingDataNodeMessageQueue in case of
		/// append. One block will marked as corrupt if the OP_ADD, OP_UPDATE_BLOCKS
		/// comes in one edit log segment and OP_CLOSE edit comes in next log segment
		/// which is loaded during failover. Regression test for HDFS-3605.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestMultipleAppendsDuringCatchupTailing()
		{
			Configuration conf = new Configuration();
			// Set a length edits tailing period, and explicit rolling, so we can
			// control the ingest of edits by the standby for this test.
			conf.Set(DFSConfigKeys.DfsHaTaileditsPeriodKey, "5000");
			conf.SetInt(DFSConfigKeys.DfsHaLogrollPeriodKey, -1);
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NnTopology(MiniDFSNNTopology
				.SimpleHATopology()).NumDataNodes(3).Build();
			FileSystem fs = null;
			try
			{
				cluster.TransitionToActive(0);
				fs = HATestUtil.ConfigureFailoverFs(cluster, conf);
				Path fileToAppend = new Path("/FileToAppend");
				Path fileToTruncate = new Path("/FileToTruncate");
				byte[] data = new byte[1 << 16];
				DFSUtil.GetRandom().NextBytes(data);
				int[] appendPos = AppendTestUtil.RandomFilePartition(data.Length, Count);
				int[] truncatePos = AppendTestUtil.RandomFilePartition(data.Length, 1);
				// Create file, write some data, and hflush so that the first
				// block is in the edit log prior to roll.
				FSDataOutputStream @out = CreateAndHflush(fs, fileToAppend, data, appendPos[0]);
				FSDataOutputStream out4Truncate = CreateAndHflush(fs, fileToTruncate, data, data.
					Length);
				// Let the StandbyNode catch the creation of the file. 
				cluster.GetNameNode(0).GetRpcServer().RollEditLog();
				cluster.GetNameNode(1).GetNamesystem().GetEditLogTailer().DoTailEdits();
				@out.Close();
				out4Truncate.Close();
				// Append and re-close a few time, so that many block entries are queued.
				for (int i = 0; i < Count; i++)
				{
					int end = i < Count - 1 ? appendPos[i + 1] : data.Length;
					@out = fs.Append(fileToAppend);
					@out.Write(data, appendPos[i], end - appendPos[i]);
					@out.Close();
				}
				bool isTruncateReady = fs.Truncate(fileToTruncate, truncatePos[0]);
				// Ensure that blocks have been reported to the SBN ahead of the edits
				// arriving.
				cluster.TriggerBlockReports();
				// Failover the current standby to active.
				cluster.ShutdownNameNode(0);
				cluster.TransitionToActive(1);
				// Check the FSCK doesn't detect any bad blocks on the SBN.
				int rc = ToolRunner.Run(new DFSck(cluster.GetConfiguration(1)), new string[] { "/"
					, "-files", "-blocks" });
				NUnit.Framework.Assert.AreEqual(0, rc);
				NUnit.Framework.Assert.AreEqual("CorruptBlocks should be empty.", 0, cluster.GetNameNode
					(1).GetNamesystem().GetCorruptReplicaBlocks());
				AppendTestUtil.CheckFullFile(fs, fileToAppend, data.Length, data, fileToAppend.ToString
					());
				if (!isTruncateReady)
				{
					TestFileTruncate.CheckBlockRecovery(fileToTruncate, cluster.GetFileSystem(1));
				}
				AppendTestUtil.CheckFullFile(fs, fileToTruncate, truncatePos[0], data, fileToTruncate
					.ToString());
			}
			finally
			{
				if (null != cluster)
				{
					cluster.Shutdown();
				}
				if (null != fs)
				{
					fs.Close();
				}
			}
		}
	}
}
