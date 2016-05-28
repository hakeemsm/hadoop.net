using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Security;
using Sharpen;

namespace Org.Apache.Hadoop.FS.Viewfs
{
	public class TestViewFsFileStatusHdfs
	{
		internal const string testfilename = "/tmp/testFileStatusSerialziation";

		internal const string someFile = "/hdfstmp/someFileForTestGetFileChecksum";

		private static readonly FileSystemTestHelper fileSystemTestHelper = new FileSystemTestHelper
			();

		private static MiniDFSCluster cluster;

		private static Path defaultWorkingDirectory;

		private static readonly Configuration Conf = new Configuration();

		private static FileSystem fHdfs;

		private static FileSystem vfs;

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Javax.Security.Auth.Login.LoginException"/>
		/// <exception cref="Sharpen.URISyntaxException"/>
		[BeforeClass]
		public static void ClusterSetupAtBegining()
		{
			cluster = new MiniDFSCluster.Builder(Conf).NumDataNodes(2).Build();
			cluster.WaitClusterUp();
			fHdfs = cluster.GetFileSystem();
			defaultWorkingDirectory = fHdfs.MakeQualified(new Path("/user/" + UserGroupInformation
				.GetCurrentUser().GetShortUserName()));
			fHdfs.Mkdirs(defaultWorkingDirectory);
			// Setup the ViewFS to be used for all tests.
			Configuration conf = ViewFileSystemTestSetup.CreateConfig();
			ConfigUtil.AddLink(conf, "/vfstmp", new URI(fHdfs.GetUri() + "/hdfstmp"));
			ConfigUtil.AddLink(conf, "/tmp", new URI(fHdfs.GetUri() + "/tmp"));
			vfs = FileSystem.Get(FsConstants.ViewfsUri, conf);
			NUnit.Framework.Assert.AreEqual(typeof(ViewFileSystem), vfs.GetType());
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Sharpen.URISyntaxException"/>
		[NUnit.Framework.Test]
		public virtual void TestFileStatusSerialziation()
		{
			long len = fileSystemTestHelper.CreateFile(fHdfs, testfilename);
			FileStatus stat = vfs.GetFileStatus(new Path(testfilename));
			NUnit.Framework.Assert.AreEqual(len, stat.GetLen());
			// check serialization/deserialization
			DataOutputBuffer dob = new DataOutputBuffer();
			stat.Write(dob);
			DataInputBuffer dib = new DataInputBuffer();
			dib.Reset(dob.GetData(), 0, dob.GetLength());
			FileStatus deSer = new FileStatus();
			deSer.ReadFields(dib);
			NUnit.Framework.Assert.AreEqual(len, deSer.GetLen());
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Sharpen.URISyntaxException"/>
		[NUnit.Framework.Test]
		public virtual void TestGetFileChecksum()
		{
			// Create two different files in HDFS
			fileSystemTestHelper.CreateFile(fHdfs, someFile);
			FileSystemTestHelper.CreateFile(fHdfs, fileSystemTestHelper.GetTestRootPath(fHdfs
				, someFile + "other"), 1, 512);
			// Get checksum through ViewFS
			FileChecksum viewFSCheckSum = vfs.GetFileChecksum(new Path("/vfstmp/someFileForTestGetFileChecksum"
				));
			// Get checksum through HDFS. 
			FileChecksum hdfsCheckSum = fHdfs.GetFileChecksum(new Path(someFile));
			// Get checksum of different file in HDFS
			FileChecksum otherHdfsFileCheckSum = fHdfs.GetFileChecksum(new Path(someFile + "other"
				));
			// Checksums of the same file (got through HDFS and ViewFS should be same)
			NUnit.Framework.Assert.AreEqual("HDFS and ViewFS checksums were not the same", viewFSCheckSum
				, hdfsCheckSum);
			// Checksum of different files should be different.
			NUnit.Framework.Assert.IsFalse("Some other HDFS file which should not have had the same "
				 + "checksum as viewFS did!", viewFSCheckSum.Equals(otherHdfsFileCheckSum));
		}

		/// <exception cref="System.IO.IOException"/>
		[AfterClass]
		public static void Cleanup()
		{
			fHdfs.Delete(new Path(testfilename), true);
			fHdfs.Delete(new Path(someFile), true);
			fHdfs.Delete(new Path(someFile + "other"), true);
		}
	}
}
