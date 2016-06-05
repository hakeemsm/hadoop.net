using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	public class TestGetFileChecksum
	{
		private const int Blocksize = 1024;

		private const short Replication = 3;

		private Configuration conf;

		private MiniDFSCluster cluster;

		private DistributedFileSystem dfs;

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public virtual void SetUp()
		{
			conf = new Configuration();
			conf.SetLong(DFSConfigKeys.DfsBlockSizeKey, Blocksize);
			cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(Replication).Build();
			cluster.WaitActive();
			dfs = cluster.GetFileSystem();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.TearDown]
		public virtual void TearDown()
		{
			if (cluster != null)
			{
				cluster.Shutdown();
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestGetFileChecksum(Path foo, int appendLength)
		{
			int appendRounds = 16;
			FileChecksum[] fc = new FileChecksum[appendRounds + 1];
			DFSTestUtil.CreateFile(dfs, foo, appendLength, Replication, 0L);
			fc[0] = dfs.GetFileChecksum(foo);
			for (int i = 0; i < appendRounds; i++)
			{
				DFSTestUtil.AppendFile(dfs, foo, appendLength);
				fc[i + 1] = dfs.GetFileChecksum(foo);
			}
			for (int i_1 = 0; i_1 < appendRounds + 1; i_1++)
			{
				FileChecksum checksum = dfs.GetFileChecksum(foo, appendLength * (i_1 + 1));
				NUnit.Framework.Assert.IsTrue(checksum.Equals(fc[i_1]));
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGetFileChecksum()
		{
			TestGetFileChecksum(new Path("/foo"), Blocksize / 4);
			TestGetFileChecksum(new Path("/bar"), Blocksize / 4 - 1);
		}
	}
}
