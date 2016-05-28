using System;
using System.IO;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Client;
using Org.Apache.Hadoop.Hdfs.Server.Datanode;
using Org.Apache.Hadoop.Test;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>
	/// This class tests that a file system adheres to the limit of
	/// maximum number of files that is configured.
	/// </summary>
	public class TestFileLimit
	{
		internal const long seed = unchecked((long)(0xDEADBEEFL));

		internal const int blockSize = 8192;

		internal bool simulatedStorage = false;

		// creates a zero file.
		/// <exception cref="System.IO.IOException"/>
		private void CreateFile(FileSystem fileSys, Path name)
		{
			FSDataOutputStream stm = fileSys.Create(name, true, fileSys.GetConf().GetInt(CommonConfigurationKeys
				.IoFileBufferSizeKey, 4096), (short)1, blockSize);
			byte[] buffer = new byte[1024];
			Random rand = new Random(seed);
			rand.NextBytes(buffer);
			stm.Write(buffer);
			stm.Close();
		}

		private void WaitForLimit(FSNamesystem namesys, long num)
		{
			// wait for number of blocks to decrease
			while (true)
			{
				long total = namesys.GetBlocksTotal() + namesys.dir.TotalInodes();
				System.Console.Out.WriteLine("Comparing current nodes " + total + " to become " +
					 num);
				if (total == num)
				{
					break;
				}
				try
				{
					Sharpen.Thread.Sleep(1000);
				}
				catch (Exception)
				{
				}
			}
		}

		/// <summary>Test that file data becomes available before file is closed.</summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestFileLimit()
		{
			Configuration conf = new HdfsConfiguration();
			int maxObjects = 5;
			conf.SetLong(DFSConfigKeys.DfsNamenodeMaxObjectsKey, maxObjects);
			conf.SetLong(DFSConfigKeys.DfsBlockreportIntervalMsecKey, 1000L);
			conf.SetInt(DFSConfigKeys.DfsHeartbeatIntervalKey, 1);
			int currentNodes = 0;
			if (simulatedStorage)
			{
				SimulatedFSDataset.SetFactory(conf);
			}
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).Build();
			FileSystem fs = cluster.GetFileSystem();
			FSNamesystem namesys = cluster.GetNamesystem();
			try
			{
				//
				// check that / exists
				//
				Path path = new Path("/");
				NUnit.Framework.Assert.IsTrue("/ should be a directory", fs.GetFileStatus(path).IsDirectory
					());
				currentNodes = 1;
				// root inode
				// verify that we can create the specified number of files. We leave
				// one for the "/". Each file takes an inode and a block.
				//
				for (int i = 0; i < maxObjects / 2; i++)
				{
					Path file = new Path("/filestatus" + i);
					CreateFile(fs, file);
					System.Console.Out.WriteLine("Created file " + file);
					currentNodes += 2;
				}
				// two more objects for this creation.
				// verify that creating another file fails
				bool hitException = false;
				try
				{
					Path file = new Path("/filestatus");
					CreateFile(fs, file);
					System.Console.Out.WriteLine("Created file " + file);
				}
				catch (IOException)
				{
					hitException = true;
				}
				NUnit.Framework.Assert.IsTrue("Was able to exceed file limit", hitException);
				// delete one file
				Path file0 = new Path("/filestatus0");
				fs.Delete(file0, true);
				System.Console.Out.WriteLine("Deleted file " + file0);
				currentNodes -= 2;
				// wait for number of blocks to decrease
				WaitForLimit(namesys, currentNodes);
				// now, we shud be able to create a new file
				CreateFile(fs, file0);
				System.Console.Out.WriteLine("Created file " + file0 + " again.");
				currentNodes += 2;
				// delete the file again
				file0 = new Path("/filestatus0");
				fs.Delete(file0, true);
				System.Console.Out.WriteLine("Deleted file " + file0 + " again.");
				currentNodes -= 2;
				// wait for number of blocks to decrease
				WaitForLimit(namesys, currentNodes);
				// create two directories in place of the file that we deleted
				Path dir = new Path("/dir0/dir1");
				fs.Mkdirs(dir);
				System.Console.Out.WriteLine("Created directories " + dir);
				currentNodes += 2;
				WaitForLimit(namesys, currentNodes);
				// verify that creating another directory fails
				hitException = false;
				try
				{
					fs.Mkdirs(new Path("dir.fail"));
					System.Console.Out.WriteLine("Created directory should not have succeeded.");
				}
				catch (IOException)
				{
					hitException = true;
				}
				NUnit.Framework.Assert.IsTrue("Was able to exceed dir limit", hitException);
			}
			finally
			{
				fs.Close();
				cluster.Shutdown();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestFileLimitSimulated()
		{
			simulatedStorage = true;
			TestFileLimit();
			simulatedStorage = false;
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestMaxBlocksPerFileLimit()
		{
			Configuration conf = new HdfsConfiguration();
			// Make a small block size and a low limit
			long blockSize = 4096;
			long numBlocks = 2;
			conf.SetLong(DFSConfigKeys.DfsBlockSizeKey, blockSize);
			conf.SetLong(DFSConfigKeys.DfsNamenodeMaxBlocksPerFileKey, numBlocks);
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).Build();
			FileSystem fs = cluster.GetFileSystem();
			HdfsDataOutputStream fout = (HdfsDataOutputStream)fs.Create(new Path("/testmaxfilelimit"
				));
			try
			{
				// Write maximum number of blocks
				fout.Write(new byte[(int)blockSize * (int)numBlocks]);
				fout.Hflush();
				// Try to write one more block
				try
				{
					fout.Write(new byte[1]);
					fout.Hflush();
					System.Diagnostics.Debug.Assert(false, "Expected IOException after writing too many blocks"
						);
				}
				catch (IOException e)
				{
					GenericTestUtils.AssertExceptionContains("File has reached the limit" + " on maximum number of"
						, e);
				}
			}
			finally
			{
				cluster.Shutdown();
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestMinBlockSizeLimit()
		{
			long blockSize = 4096;
			Configuration conf = new HdfsConfiguration();
			conf.SetLong(DFSConfigKeys.DfsNamenodeMinBlockSizeKey, blockSize);
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).Build();
			FileSystem fs = cluster.GetFileSystem();
			try
			{
				// Try with min block size
				fs.Create(new Path("/testmblock1"), true, 4096, (short)3, blockSize);
				try
				{
					// Try with min block size - 1
					fs.Create(new Path("/testmblock2"), true, 4096, (short)3, blockSize - 1);
					System.Diagnostics.Debug.Assert(false, "Expected IOException after creating a file with small"
						 + " blocks ");
				}
				catch (IOException e)
				{
					GenericTestUtils.AssertExceptionContains("Specified block size is less", e);
				}
			}
			finally
			{
				cluster.Shutdown();
			}
		}
	}
}
