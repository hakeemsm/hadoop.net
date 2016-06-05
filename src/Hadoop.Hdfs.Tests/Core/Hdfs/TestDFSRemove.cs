using System.IO;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs.Server.Datanode;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	public class TestDFSRemove
	{
		internal readonly Path dir = new Path("/test/remove/");

		/// <exception cref="System.IO.IOException"/>
		internal virtual void List(FileSystem fs, string name)
		{
			FileSystem.Log.Info("\n\n" + name);
			foreach (FileStatus s in fs.ListStatus(dir))
			{
				FileSystem.Log.Info(string.Empty + s.GetPath());
			}
		}

		/// <exception cref="System.IO.IOException"/>
		internal static void CreateFile(FileSystem fs, Path f)
		{
			DataOutputStream a_out = fs.Create(f);
			a_out.WriteBytes("something");
			a_out.Close();
		}

		/// <exception cref="System.IO.IOException"/>
		internal static long GetTotalDfsUsed(MiniDFSCluster cluster)
		{
			long total = 0;
			foreach (DataNode node in cluster.GetDataNodes())
			{
				total += DataNodeTestUtils.GetFSDataset(node).GetDfsUsed();
			}
			return total;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRemove()
		{
			Configuration conf = new HdfsConfiguration();
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(2).Build();
			try
			{
				FileSystem fs = cluster.GetFileSystem();
				NUnit.Framework.Assert.IsTrue(fs.Mkdirs(dir));
				long dfsUsedStart = GetTotalDfsUsed(cluster);
				{
					// Create 100 files
					int fileCount = 100;
					for (int i = 0; i < fileCount; i++)
					{
						Path a = new Path(dir, "a" + i);
						CreateFile(fs, a);
					}
					long dfsUsedMax = GetTotalDfsUsed(cluster);
					// Remove 100 files
					for (int i_1 = 0; i_1 < fileCount; i_1++)
					{
						Path a = new Path(dir, "a" + i_1);
						fs.Delete(a, false);
					}
					// wait 3 heartbeat intervals, so that all blocks are deleted.
					Sharpen.Thread.Sleep(3 * DFSConfigKeys.DfsHeartbeatIntervalDefault * 1000);
					// all blocks should be gone now.
					long dfsUsedFinal = GetTotalDfsUsed(cluster);
					NUnit.Framework.Assert.AreEqual("All blocks should be gone. start=" + dfsUsedStart
						 + " max=" + dfsUsedMax + " final=" + dfsUsedFinal, dfsUsedStart, dfsUsedFinal);
				}
				fs.Delete(dir, true);
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
