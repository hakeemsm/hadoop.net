using System.IO;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Commons.Logging.Impl;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Hdfs.Web;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Security;
using Org.Apache.Log4j;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	/// <summary>This class tests the FileStatus API.</summary>
	public class TestFileStatus
	{
		internal const long seed = unchecked((long)(0xDEADBEEFL));

		internal const int blockSize = 8192;

		internal const int fileSize = 16384;

		private static Configuration conf;

		private static MiniDFSCluster cluster;

		private static FileSystem fs;

		private static FileContext fc;

		private static HftpFileSystem hftpfs;

		private static DFSClient dfsClient;

		private static Path file1;

		/// <exception cref="System.Exception"/>
		[BeforeClass]
		public static void TestSetUp()
		{
			conf = new HdfsConfiguration();
			conf.SetInt(DFSConfigKeys.DfsListLimit, 2);
			cluster = new MiniDFSCluster.Builder(conf).Build();
			fs = cluster.GetFileSystem();
			fc = FileContext.GetFileContext(cluster.GetURI(0), conf);
			hftpfs = cluster.GetHftpFileSystem(0);
			dfsClient = new DFSClient(NameNode.GetAddress(conf), conf);
			file1 = new Path("filestatus.dat");
			WriteFile(fs, file1, 1, fileSize, blockSize);
		}

		/// <exception cref="System.Exception"/>
		[AfterClass]
		public static void TestTearDown()
		{
			fs.Close();
			cluster.Shutdown();
		}

		/// <exception cref="System.IO.IOException"/>
		private static void WriteFile(FileSystem fileSys, Path name, int repl, int fileSize
			, int blockSize)
		{
			// Create and write a file that contains three blocks of data
			FSDataOutputStream stm = fileSys.Create(name, true, HdfsConstants.IoFileBufferSize
				, (short)repl, (long)blockSize);
			byte[] buffer = new byte[fileSize];
			Random rand = new Random(seed);
			rand.NextBytes(buffer);
			stm.Write(buffer);
			stm.Close();
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		/// <exception cref="Sharpen.TimeoutException"/>
		private void CheckFile(FileSystem fileSys, Path name, int repl)
		{
			DFSTestUtil.WaitReplication(fileSys, name, (short)repl);
		}

		/// <summary>Test calling getFileInfo directly on the client</summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestGetFileInfo()
		{
			// Check that / exists
			Path path = new Path("/");
			NUnit.Framework.Assert.IsTrue("/ should be a directory", fs.GetFileStatus(path).IsDirectory
				());
			// Make sure getFileInfo returns null for files which do not exist
			HdfsFileStatus fileInfo = dfsClient.GetFileInfo("/noSuchFile");
			NUnit.Framework.Assert.AreEqual("Non-existant file should result in null", null, 
				fileInfo);
			Path path1 = new Path("/name1");
			Path path2 = new Path("/name1/name2");
			NUnit.Framework.Assert.IsTrue(fs.Mkdirs(path1));
			FSDataOutputStream @out = fs.Create(path2, false);
			@out.Close();
			fileInfo = dfsClient.GetFileInfo(path1.ToString());
			NUnit.Framework.Assert.AreEqual(1, fileInfo.GetChildrenNum());
			fileInfo = dfsClient.GetFileInfo(path2.ToString());
			NUnit.Framework.Assert.AreEqual(0, fileInfo.GetChildrenNum());
			// Test getFileInfo throws the right exception given a non-absolute path.
			try
			{
				dfsClient.GetFileInfo("non-absolute");
				NUnit.Framework.Assert.Fail("getFileInfo for a non-absolute path did not throw IOException"
					);
			}
			catch (RemoteException re)
			{
				NUnit.Framework.Assert.IsTrue("Wrong exception for invalid file name", re.ToString
					().Contains("Invalid file name"));
			}
		}

		/// <summary>Test the FileStatus obtained calling getFileStatus on a file</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGetFileStatusOnFile()
		{
			CheckFile(fs, file1, 1);
			// test getFileStatus on a file
			FileStatus status = fs.GetFileStatus(file1);
			NUnit.Framework.Assert.IsFalse(file1 + " should be a file", status.IsDirectory());
			NUnit.Framework.Assert.AreEqual(blockSize, status.GetBlockSize());
			NUnit.Framework.Assert.AreEqual(1, status.GetReplication());
			NUnit.Framework.Assert.AreEqual(fileSize, status.GetLen());
			NUnit.Framework.Assert.AreEqual(file1.MakeQualified(fs.GetUri(), fs.GetWorkingDirectory
				()).ToString(), status.GetPath().ToString());
		}

		/// <summary>Test the FileStatus obtained calling listStatus on a file</summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestListStatusOnFile()
		{
			FileStatus[] stats = fs.ListStatus(file1);
			NUnit.Framework.Assert.AreEqual(1, stats.Length);
			FileStatus status = stats[0];
			NUnit.Framework.Assert.IsFalse(file1 + " should be a file", status.IsDirectory());
			NUnit.Framework.Assert.AreEqual(blockSize, status.GetBlockSize());
			NUnit.Framework.Assert.AreEqual(1, status.GetReplication());
			NUnit.Framework.Assert.AreEqual(fileSize, status.GetLen());
			NUnit.Framework.Assert.AreEqual(file1.MakeQualified(fs.GetUri(), fs.GetWorkingDirectory
				()).ToString(), status.GetPath().ToString());
			RemoteIterator<FileStatus> itor = fc.ListStatus(file1);
			status = itor.Next();
			NUnit.Framework.Assert.AreEqual(stats[0], status);
			NUnit.Framework.Assert.IsFalse(file1 + " should be a file", status.IsDirectory());
		}

		/// <summary>Test getting a FileStatus object using a non-existant path</summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestGetFileStatusOnNonExistantFileDir()
		{
			Path dir = new Path("/test/mkdirs");
			try
			{
				fs.ListStatus(dir);
				NUnit.Framework.Assert.Fail("listStatus of non-existent path should fail");
			}
			catch (FileNotFoundException fe)
			{
				NUnit.Framework.Assert.AreEqual("File " + dir + " does not exist.", fe.Message);
			}
			try
			{
				fc.ListStatus(dir);
				NUnit.Framework.Assert.Fail("listStatus of non-existent path should fail");
			}
			catch (FileNotFoundException fe)
			{
				NUnit.Framework.Assert.AreEqual("File " + dir + " does not exist.", fe.Message);
			}
			try
			{
				fs.GetFileStatus(dir);
				NUnit.Framework.Assert.Fail("getFileStatus of non-existent path should fail");
			}
			catch (FileNotFoundException fe)
			{
				NUnit.Framework.Assert.IsTrue("Exception doesn't indicate non-existant path", fe.
					Message.StartsWith("File does not exist"));
			}
		}

		/// <summary>Test FileStatus objects obtained from a directory</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGetFileStatusOnDir()
		{
			// Create the directory
			Path dir = new Path("/test/mkdirs");
			NUnit.Framework.Assert.IsTrue("mkdir failed", fs.Mkdirs(dir));
			NUnit.Framework.Assert.IsTrue("mkdir failed", fs.Exists(dir));
			// test getFileStatus on an empty directory
			FileStatus status = fs.GetFileStatus(dir);
			NUnit.Framework.Assert.IsTrue(dir + " should be a directory", status.IsDirectory(
				));
			NUnit.Framework.Assert.IsTrue(dir + " should be zero size ", status.GetLen() == 0
				);
			NUnit.Framework.Assert.AreEqual(dir.MakeQualified(fs.GetUri(), fs.GetWorkingDirectory
				()).ToString(), status.GetPath().ToString());
			// test listStatus on an empty directory
			FileStatus[] stats = fs.ListStatus(dir);
			NUnit.Framework.Assert.AreEqual(dir + " should be empty", 0, stats.Length);
			NUnit.Framework.Assert.AreEqual(dir + " should be zero size ", 0, fs.GetContentSummary
				(dir).GetLength());
			NUnit.Framework.Assert.AreEqual(dir + " should be zero size using hftp", 0, hftpfs
				.GetContentSummary(dir).GetLength());
			RemoteIterator<FileStatus> itor = fc.ListStatus(dir);
			NUnit.Framework.Assert.IsFalse(dir + " should be empty", itor.HasNext());
			itor = fs.ListStatusIterator(dir);
			NUnit.Framework.Assert.IsFalse(dir + " should be empty", itor.HasNext());
			// create another file that is smaller than a block.
			Path file2 = new Path(dir, "filestatus2.dat");
			WriteFile(fs, file2, 1, blockSize / 4, blockSize);
			CheckFile(fs, file2, 1);
			// verify file attributes
			status = fs.GetFileStatus(file2);
			NUnit.Framework.Assert.AreEqual(blockSize, status.GetBlockSize());
			NUnit.Framework.Assert.AreEqual(1, status.GetReplication());
			file2 = fs.MakeQualified(file2);
			NUnit.Framework.Assert.AreEqual(file2.ToString(), status.GetPath().ToString());
			// Create another file in the same directory
			Path file3 = new Path(dir, "filestatus3.dat");
			WriteFile(fs, file3, 1, blockSize / 4, blockSize);
			CheckFile(fs, file3, 1);
			file3 = fs.MakeQualified(file3);
			// Verify that the size of the directory increased by the size 
			// of the two files
			int expected = blockSize / 2;
			NUnit.Framework.Assert.AreEqual(dir + " size should be " + expected, expected, fs
				.GetContentSummary(dir).GetLength());
			NUnit.Framework.Assert.AreEqual(dir + " size should be " + expected + " using hftp"
				, expected, hftpfs.GetContentSummary(dir).GetLength());
			// Test listStatus on a non-empty directory
			stats = fs.ListStatus(dir);
			NUnit.Framework.Assert.AreEqual(dir + " should have two entries", 2, stats.Length
				);
			NUnit.Framework.Assert.AreEqual(file2.ToString(), stats[0].GetPath().ToString());
			NUnit.Framework.Assert.AreEqual(file3.ToString(), stats[1].GetPath().ToString());
			itor = fc.ListStatus(dir);
			NUnit.Framework.Assert.AreEqual(file2.ToString(), itor.Next().GetPath().ToString(
				));
			NUnit.Framework.Assert.AreEqual(file3.ToString(), itor.Next().GetPath().ToString(
				));
			NUnit.Framework.Assert.IsFalse("Unexpected addtional file", itor.HasNext());
			itor = fs.ListStatusIterator(dir);
			NUnit.Framework.Assert.AreEqual(file2.ToString(), itor.Next().GetPath().ToString(
				));
			NUnit.Framework.Assert.AreEqual(file3.ToString(), itor.Next().GetPath().ToString(
				));
			NUnit.Framework.Assert.IsFalse("Unexpected addtional file", itor.HasNext());
			// Test iterative listing. Now dir has 2 entries, create one more.
			Path dir3 = fs.MakeQualified(new Path(dir, "dir3"));
			fs.Mkdirs(dir3);
			dir3 = fs.MakeQualified(dir3);
			stats = fs.ListStatus(dir);
			NUnit.Framework.Assert.AreEqual(dir + " should have three entries", 3, stats.Length
				);
			NUnit.Framework.Assert.AreEqual(dir3.ToString(), stats[0].GetPath().ToString());
			NUnit.Framework.Assert.AreEqual(file2.ToString(), stats[1].GetPath().ToString());
			NUnit.Framework.Assert.AreEqual(file3.ToString(), stats[2].GetPath().ToString());
			itor = fc.ListStatus(dir);
			NUnit.Framework.Assert.AreEqual(dir3.ToString(), itor.Next().GetPath().ToString()
				);
			NUnit.Framework.Assert.AreEqual(file2.ToString(), itor.Next().GetPath().ToString(
				));
			NUnit.Framework.Assert.AreEqual(file3.ToString(), itor.Next().GetPath().ToString(
				));
			NUnit.Framework.Assert.IsFalse("Unexpected addtional file", itor.HasNext());
			itor = fs.ListStatusIterator(dir);
			NUnit.Framework.Assert.AreEqual(dir3.ToString(), itor.Next().GetPath().ToString()
				);
			NUnit.Framework.Assert.AreEqual(file2.ToString(), itor.Next().GetPath().ToString(
				));
			NUnit.Framework.Assert.AreEqual(file3.ToString(), itor.Next().GetPath().ToString(
				));
			NUnit.Framework.Assert.IsFalse("Unexpected addtional file", itor.HasNext());
			// Now dir has 3 entries, create two more
			Path dir4 = fs.MakeQualified(new Path(dir, "dir4"));
			fs.Mkdirs(dir4);
			dir4 = fs.MakeQualified(dir4);
			Path dir5 = fs.MakeQualified(new Path(dir, "dir5"));
			fs.Mkdirs(dir5);
			dir5 = fs.MakeQualified(dir5);
			stats = fs.ListStatus(dir);
			NUnit.Framework.Assert.AreEqual(dir + " should have five entries", 5, stats.Length
				);
			NUnit.Framework.Assert.AreEqual(dir3.ToString(), stats[0].GetPath().ToString());
			NUnit.Framework.Assert.AreEqual(dir4.ToString(), stats[1].GetPath().ToString());
			NUnit.Framework.Assert.AreEqual(dir5.ToString(), stats[2].GetPath().ToString());
			NUnit.Framework.Assert.AreEqual(file2.ToString(), stats[3].GetPath().ToString());
			NUnit.Framework.Assert.AreEqual(file3.ToString(), stats[4].GetPath().ToString());
			itor = fc.ListStatus(dir);
			NUnit.Framework.Assert.AreEqual(dir3.ToString(), itor.Next().GetPath().ToString()
				);
			NUnit.Framework.Assert.AreEqual(dir4.ToString(), itor.Next().GetPath().ToString()
				);
			NUnit.Framework.Assert.AreEqual(dir5.ToString(), itor.Next().GetPath().ToString()
				);
			NUnit.Framework.Assert.AreEqual(file2.ToString(), itor.Next().GetPath().ToString(
				));
			NUnit.Framework.Assert.AreEqual(file3.ToString(), itor.Next().GetPath().ToString(
				));
			NUnit.Framework.Assert.IsFalse(itor.HasNext());
			itor = fs.ListStatusIterator(dir);
			NUnit.Framework.Assert.AreEqual(dir3.ToString(), itor.Next().GetPath().ToString()
				);
			NUnit.Framework.Assert.AreEqual(dir4.ToString(), itor.Next().GetPath().ToString()
				);
			NUnit.Framework.Assert.AreEqual(dir5.ToString(), itor.Next().GetPath().ToString()
				);
			NUnit.Framework.Assert.AreEqual(file2.ToString(), itor.Next().GetPath().ToString(
				));
			NUnit.Framework.Assert.AreEqual(file3.ToString(), itor.Next().GetPath().ToString(
				));
			NUnit.Framework.Assert.IsFalse(itor.HasNext());
			{
				//test permission error on hftp 
				fs.SetPermission(dir, new FsPermission((short)0));
				try
				{
					string username = UserGroupInformation.GetCurrentUser().GetShortUserName() + "1";
					HftpFileSystem hftp2 = cluster.GetHftpFileSystemAs(username, conf, 0, "somegroup"
						);
					hftp2.GetContentSummary(dir);
					NUnit.Framework.Assert.Fail();
				}
				catch (IOException ioe)
				{
					FileSystem.Log.Info("GOOD: getting an exception", ioe);
				}
			}
			fs.Delete(dir, true);
		}

		public TestFileStatus()
		{
			{
				((Log4JLogger)LogFactory.GetLog(typeof(FSNamesystem))).GetLogger().SetLevel(Level
					.All);
				((Log4JLogger)FileSystem.Log).GetLogger().SetLevel(Level.All);
			}
		}
	}
}
