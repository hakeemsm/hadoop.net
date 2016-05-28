using System.IO;
using System.Net;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	/// <summary>This class tests the decommissioning of nodes.</summary>
	public class TestModTime
	{
		internal const long seed = unchecked((long)(0xDEADBEEFL));

		internal const int blockSize = 8192;

		internal const int fileSize = 16384;

		internal const int numDatanodes = 6;

		internal Random myrand = new Random();

		internal Path hostsFile;

		internal Path excludeFile;

		/// <exception cref="System.IO.IOException"/>
		private void WriteFile(FileSystem fileSys, Path name, int repl)
		{
			// create and write a file that contains three blocks of data
			FSDataOutputStream stm = fileSys.Create(name, true, fileSys.GetConf().GetInt(CommonConfigurationKeys
				.IoFileBufferSizeKey, 4096), (short)repl, blockSize);
			byte[] buffer = new byte[fileSize];
			Random rand = new Random(seed);
			rand.NextBytes(buffer);
			stm.Write(buffer);
			stm.Close();
		}

		/// <exception cref="System.IO.IOException"/>
		private void CleanupFile(FileSystem fileSys, Path name)
		{
			NUnit.Framework.Assert.IsTrue(fileSys.Exists(name));
			fileSys.Delete(name, true);
			NUnit.Framework.Assert.IsTrue(!fileSys.Exists(name));
		}

		private void PrintDatanodeReport(DatanodeInfo[] info)
		{
			System.Console.Out.WriteLine("-------------------------------------------------");
			for (int i = 0; i < info.Length; i++)
			{
				System.Console.Out.WriteLine(info[i].GetDatanodeReport());
				System.Console.Out.WriteLine();
			}
		}

		/// <summary>Tests modification time in DFS.</summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestModTime()
		{
			Configuration conf = new HdfsConfiguration();
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(numDatanodes
				).Build();
			cluster.WaitActive();
			IPEndPoint addr = new IPEndPoint("localhost", cluster.GetNameNodePort());
			DFSClient client = new DFSClient(addr, conf);
			DatanodeInfo[] info = client.DatanodeReport(HdfsConstants.DatanodeReportType.Live
				);
			NUnit.Framework.Assert.AreEqual("Number of Datanodes ", numDatanodes, info.Length
				);
			FileSystem fileSys = cluster.GetFileSystem();
			int replicas = numDatanodes - 1;
			NUnit.Framework.Assert.IsTrue(fileSys is DistributedFileSystem);
			try
			{
				//
				// create file and record ctime and mtime of test file
				//
				System.Console.Out.WriteLine("Creating testdir1 and testdir1/test1.dat.");
				Path dir1 = new Path("testdir1");
				Path file1 = new Path(dir1, "test1.dat");
				WriteFile(fileSys, file1, replicas);
				FileStatus stat = fileSys.GetFileStatus(file1);
				long mtime1 = stat.GetModificationTime();
				NUnit.Framework.Assert.IsTrue(mtime1 != 0);
				//
				// record dir times
				//
				stat = fileSys.GetFileStatus(dir1);
				long mdir1 = stat.GetModificationTime();
				//
				// create second test file
				//
				System.Console.Out.WriteLine("Creating testdir1/test2.dat.");
				Path file2 = new Path(dir1, "test2.dat");
				WriteFile(fileSys, file2, replicas);
				stat = fileSys.GetFileStatus(file2);
				//
				// verify that mod time of dir remains the same
				// as before. modification time of directory has increased.
				//
				stat = fileSys.GetFileStatus(dir1);
				NUnit.Framework.Assert.IsTrue(stat.GetModificationTime() >= mdir1);
				mdir1 = stat.GetModificationTime();
				//
				// create another directory
				//
				Path dir2 = fileSys.MakeQualified(new Path("testdir2/"));
				System.Console.Out.WriteLine("Creating testdir2 " + dir2);
				NUnit.Framework.Assert.IsTrue(fileSys.Mkdirs(dir2));
				stat = fileSys.GetFileStatus(dir2);
				long mdir2 = stat.GetModificationTime();
				//
				// rename file1 from testdir into testdir2
				//
				Path newfile = new Path(dir2, "testnew.dat");
				System.Console.Out.WriteLine("Moving " + file1 + " to " + newfile);
				fileSys.Rename(file1, newfile);
				//
				// verify that modification time of file1 did not change.
				//
				stat = fileSys.GetFileStatus(newfile);
				NUnit.Framework.Assert.IsTrue(stat.GetModificationTime() == mtime1);
				//
				// verify that modification time of  testdir1 and testdir2
				// were changed. 
				//
				stat = fileSys.GetFileStatus(dir1);
				NUnit.Framework.Assert.IsTrue(stat.GetModificationTime() != mdir1);
				mdir1 = stat.GetModificationTime();
				stat = fileSys.GetFileStatus(dir2);
				NUnit.Framework.Assert.IsTrue(stat.GetModificationTime() != mdir2);
				mdir2 = stat.GetModificationTime();
				//
				// delete newfile
				//
				System.Console.Out.WriteLine("Deleting testdir2/testnew.dat.");
				NUnit.Framework.Assert.IsTrue(fileSys.Delete(newfile, true));
				//
				// verify that modification time of testdir1 has not changed.
				//
				stat = fileSys.GetFileStatus(dir1);
				NUnit.Framework.Assert.IsTrue(stat.GetModificationTime() == mdir1);
				//
				// verify that modification time of testdir2 has changed.
				//
				stat = fileSys.GetFileStatus(dir2);
				NUnit.Framework.Assert.IsTrue(stat.GetModificationTime() != mdir2);
				mdir2 = stat.GetModificationTime();
				CleanupFile(fileSys, file2);
				CleanupFile(fileSys, dir1);
				CleanupFile(fileSys, dir2);
			}
			catch (IOException e)
			{
				info = client.DatanodeReport(HdfsConstants.DatanodeReportType.All);
				PrintDatanodeReport(info);
				throw;
			}
			finally
			{
				fileSys.Close();
				cluster.Shutdown();
			}
		}

		/// <summary>
		/// Regression test for HDFS-3864 - NN does not update internal file mtime for
		/// OP_CLOSE when reading from the edit log.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestModTimePersistsAfterRestart()
		{
			long sleepTime = 10;
			// 10 milliseconds
			MiniDFSCluster cluster = null;
			FileSystem fs = null;
			Configuration conf = new HdfsConfiguration();
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).Build();
				fs = cluster.GetFileSystem();
				Path testPath = new Path("/test");
				// Open a file, and get its initial modification time.
				OutputStream @out = fs.Create(testPath);
				long initialModTime = fs.GetFileStatus(testPath).GetModificationTime();
				NUnit.Framework.Assert.IsTrue(initialModTime > 0);
				// Wait and then close the file. Ensure that the mod time goes up.
				ThreadUtil.SleepAtLeastIgnoreInterrupts(sleepTime);
				@out.Close();
				long modTimeAfterClose = fs.GetFileStatus(testPath).GetModificationTime();
				NUnit.Framework.Assert.IsTrue(modTimeAfterClose >= initialModTime + sleepTime);
				// Restart the NN, and make sure that the later mod time is still used.
				cluster.RestartNameNode();
				long modTimeAfterRestart = fs.GetFileStatus(testPath).GetModificationTime();
				NUnit.Framework.Assert.AreEqual(modTimeAfterClose, modTimeAfterRestart);
			}
			finally
			{
				if (fs != null)
				{
					fs.Close();
				}
				if (cluster != null)
				{
					cluster.Shutdown();
				}
			}
		}

		/// <exception cref="System.Exception"/>
		public static void Main(string[] args)
		{
			new Org.Apache.Hadoop.Hdfs.TestModTime().TestModTime();
		}
	}
}
