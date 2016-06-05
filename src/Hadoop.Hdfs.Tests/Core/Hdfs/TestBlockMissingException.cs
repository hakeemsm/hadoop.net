using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	public class TestBlockMissingException
	{
		internal static readonly Log Log = LogFactory.GetLog("org.apache.hadoop.hdfs.TestBlockMissing"
			);

		internal const int NumDatanodes = 3;

		internal Configuration conf;

		internal MiniDFSCluster dfs = null;

		internal DistributedFileSystem fileSys = null;

		/// <summary>Test DFS Raid</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestBlockMissingException()
		{
			Log.Info("Test testBlockMissingException started.");
			long blockSize = 1024L;
			int numBlocks = 4;
			conf = new HdfsConfiguration();
			// Set short retry timeouts so this test runs faster
			conf.SetInt(DFSConfigKeys.DfsClientRetryWindowBase, 10);
			try
			{
				dfs = new MiniDFSCluster.Builder(conf).NumDataNodes(NumDatanodes).Build();
				dfs.WaitActive();
				fileSys = dfs.GetFileSystem();
				Path file1 = new Path("/user/dhruba/raidtest/file1");
				CreateOldFile(fileSys, file1, 1, numBlocks, blockSize);
				// extract block locations from File system. Wait till file is closed.
				LocatedBlocks locations = null;
				locations = fileSys.dfs.GetNamenode().GetBlockLocations(file1.ToString(), 0, numBlocks
					 * blockSize);
				// remove block of file
				Log.Info("Remove first block of file");
				CorruptBlock(file1, locations.Get(0).GetBlock());
				// validate that the system throws BlockMissingException
				ValidateFile(fileSys, file1);
			}
			finally
			{
				if (fileSys != null)
				{
					fileSys.Close();
				}
				if (dfs != null)
				{
					dfs.Shutdown();
				}
			}
			Log.Info("Test testBlockMissingException completed.");
		}

		//
		// creates a file and populate it with data.
		//
		/// <exception cref="System.IO.IOException"/>
		private void CreateOldFile(FileSystem fileSys, Path name, int repl, int numBlocks
			, long blocksize)
		{
			FSDataOutputStream stm = fileSys.Create(name, true, fileSys.GetConf().GetInt(CommonConfigurationKeys
				.IoFileBufferSizeKey, 4096), (short)repl, blocksize);
			// fill data into file
			byte[] b = new byte[(int)blocksize];
			for (int i = 0; i < numBlocks; i++)
			{
				stm.Write(b);
			}
			stm.Close();
		}

		//
		// validates that file encounters BlockMissingException
		//
		/// <exception cref="System.IO.IOException"/>
		private void ValidateFile(FileSystem fileSys, Path name)
		{
			FSDataInputStream stm = fileSys.Open(name);
			byte[] b = new byte[4192];
			int num = 0;
			bool gotException = false;
			try
			{
				while (num >= 0)
				{
					num = stm.Read(b);
					if (num < 0)
					{
						break;
					}
				}
			}
			catch (BlockMissingException)
			{
				gotException = true;
			}
			stm.Close();
			NUnit.Framework.Assert.IsTrue("Expected BlockMissingException ", gotException);
		}

		//
		// Corrupt specified block of file
		//
		internal virtual void CorruptBlock(Path file, ExtendedBlock blk)
		{
			// Now deliberately remove/truncate data blocks from the file.
			FilePath[] blockFiles = dfs.GetAllBlockFiles(blk);
			foreach (FilePath f in blockFiles)
			{
				f.Delete();
				Log.Info("Deleted block " + f);
			}
		}
	}
}
