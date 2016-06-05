using System.Collections.Generic;
using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Hdfs.Web;
using Org.Apache.Hadoop.Security;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	/// <summary>
	/// Test for
	/// <see cref="Org.Apache.Hadoop.Hdfs.Server.Namenode.ListPathsServlet"/>
	/// that serves the URL
	/// http://&lt;namenodeaddress:httpport?/listPaths
	/// This test does not use the servlet directly. Instead it is based on
	/// <see cref="Org.Apache.Hadoop.Hdfs.Web.HftpFileSystem"/>
	/// , which uses this servlet to implement
	/// <see cref="Org.Apache.Hadoop.Hdfs.Web.HftpFileSystem.ListStatus(Org.Apache.Hadoop.FS.Path)
	/// 	"/>
	/// method.
	/// </summary>
	public class TestListPathServlet
	{
		private static readonly Configuration Conf = new HdfsConfiguration();

		private static MiniDFSCluster cluster;

		private static FileSystem fs;

		private static URI hftpURI;

		private static HftpFileSystem hftpFs;

		private readonly Random r = new Random();

		private readonly IList<string> filelist = new AList<string>();

		/// <exception cref="System.Exception"/>
		[BeforeClass]
		public static void Setup()
		{
			// start a cluster with single datanode
			cluster = new MiniDFSCluster.Builder(Conf).Build();
			cluster.WaitActive();
			fs = cluster.GetFileSystem();
			string str = "hftp://" + Conf.Get(DFSConfigKeys.DfsNamenodeHttpAddressKey);
			hftpURI = new URI(str);
			hftpFs = cluster.GetHftpFileSystem(0);
		}

		[AfterClass]
		public static void Teardown()
		{
			cluster.Shutdown();
		}

		/// <summary>create a file with a length of <code>fileLen</code></summary>
		/// <exception cref="System.IO.IOException"/>
		private void CreateFile(string fileName, long fileLen)
		{
			filelist.AddItem(hftpURI + fileName);
			Path filePath = new Path(fileName);
			DFSTestUtil.CreateFile(fs, filePath, fileLen, (short)1, r.NextLong());
		}

		/// <exception cref="System.IO.IOException"/>
		private void Mkdirs(string dirName)
		{
			filelist.AddItem(hftpURI + dirName);
			fs.Mkdirs(new Path(dirName));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestListStatus()
		{
			// Empty root directory
			CheckStatus("/");
			// Root directory with files and directories
			CreateFile("/a", 1);
			CreateFile("/b", 1);
			Mkdirs("/dir");
			CheckFile(new Path("/a"));
			CheckFile(new Path("/b"));
			CheckStatus("/");
			// A directory with files and directories
			CreateFile("/dir/.a.crc", 1);
			CreateFile("/dir/b", 1);
			Mkdirs("/dir/dir1");
			CheckFile(new Path("/dir/.a.crc"));
			CheckFile(new Path("/dir/b"));
			CheckStatus("/dir");
			// Non existent path
			CheckStatus("/nonexistent");
			CheckStatus("/nonexistent/a");
			string username = UserGroupInformation.GetCurrentUser().GetShortUserName() + "1";
			HftpFileSystem hftp2 = cluster.GetHftpFileSystemAs(username, Conf, 0, "somegroup"
				);
			{
				//test file not found on hftp 
				Path nonexistent = new Path("/nonexistent");
				try
				{
					hftp2.GetFileStatus(nonexistent);
					NUnit.Framework.Assert.Fail();
				}
				catch (IOException ioe)
				{
					FileSystem.Log.Info("GOOD: getting an exception", ioe);
				}
			}
			{
				//test permission error on hftp
				Path dir = new Path("/dir");
				fs.SetPermission(dir, new FsPermission((short)0));
				try
				{
					hftp2.GetFileStatus(new Path(dir, "a"));
					NUnit.Framework.Assert.Fail();
				}
				catch (IOException ioe)
				{
					FileSystem.Log.Info("GOOD: getting an exception", ioe);
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void CheckStatus(string listdir)
		{
			Path listpath = hftpFs.MakeQualified(new Path(listdir));
			listdir = listpath.ToString();
			FileStatus[] statuslist = hftpFs.ListStatus(listpath);
			foreach (string directory in filelist)
			{
				System.Console.Out.WriteLine("dir:" + directory);
			}
			foreach (string file in filelist)
			{
				System.Console.Out.WriteLine("file:" + file);
			}
			foreach (FileStatus status in statuslist)
			{
				System.Console.Out.WriteLine("status:" + status.GetPath().ToString() + " type " +
					 (status.IsDirectory() ? "directory" : (status.IsFile() ? "file" : "symlink")));
			}
			foreach (string file_1 in filelist)
			{
				bool found = false;
				// Consider only file under the list path
				if (!file_1.StartsWith(listpath.ToString()) || file_1.Equals(listpath.ToString()))
				{
					continue;
				}
				foreach (FileStatus status_1 in statuslist)
				{
					if (status_1.GetPath().ToString().Equals(file_1))
					{
						found = true;
						break;
					}
				}
				NUnit.Framework.Assert.IsTrue("Directory/file not returned in list status " + file_1
					, found);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void CheckFile(Path f)
		{
			Path hdfspath = fs.MakeQualified(f);
			FileStatus hdfsstatus = fs.GetFileStatus(hdfspath);
			FileSystem.Log.Info("hdfspath=" + hdfspath);
			Path hftppath = hftpFs.MakeQualified(f);
			FileStatus hftpstatus = hftpFs.GetFileStatus(hftppath);
			FileSystem.Log.Info("hftppath=" + hftppath);
			NUnit.Framework.Assert.AreEqual(hdfspath.ToUri().GetPath(), hdfsstatus.GetPath().
				ToUri().GetPath());
			CheckFileStatus(hdfsstatus, hftpstatus);
		}

		private static void CheckFileStatus(FileStatus expected, FileStatus computed)
		{
			NUnit.Framework.Assert.AreEqual(expected.GetPath().ToUri().GetPath(), computed.GetPath
				().ToUri().GetPath());
			// TODO: test will fail if the following is un-commented. 
			//    Assert.assertEquals(expected.getAccessTime(), computed.getAccessTime());
			//    Assert.assertEquals(expected.getModificationTime(),
			//        computed.getModificationTime());
			NUnit.Framework.Assert.AreEqual(expected.GetBlockSize(), computed.GetBlockSize());
			NUnit.Framework.Assert.AreEqual(expected.GetGroup(), computed.GetGroup());
			NUnit.Framework.Assert.AreEqual(expected.GetLen(), computed.GetLen());
			NUnit.Framework.Assert.AreEqual(expected.GetOwner(), computed.GetOwner());
			NUnit.Framework.Assert.AreEqual(expected.GetPermission(), computed.GetPermission(
				));
			NUnit.Framework.Assert.AreEqual(expected.GetReplication(), computed.GetReplication
				());
		}
	}
}
