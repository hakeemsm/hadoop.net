using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Sharpen;

namespace Org.Apache.Hadoop.FS.Viewfs
{
	/// <summary>The FileStatus is being serialized in MR as jobs are submitted.</summary>
	/// <remarks>
	/// The FileStatus is being serialized in MR as jobs are submitted.
	/// Since viewfs has overlayed ViewFsFileStatus, we ran into
	/// serialization problems. THis test is test the fix.
	/// </remarks>
	public class TestViewfsFileStatus
	{
		private static readonly FilePath TestDir = new FilePath(Runtime.GetProperty("test.build.data"
			, "/tmp"), typeof(TestViewfsFileStatus).Name);

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Sharpen.URISyntaxException"/>
		[NUnit.Framework.Test]
		public virtual void TestFileStatusSerialziation()
		{
			string testfilename = "testFileStatusSerialziation";
			TestDir.Mkdirs();
			FilePath infile = new FilePath(TestDir, testfilename);
			byte[] content = Sharpen.Runtime.GetBytesForString("dingos");
			FileOutputStream fos = null;
			try
			{
				fos = new FileOutputStream(infile);
				fos.Write(content);
			}
			finally
			{
				if (fos != null)
				{
					fos.Close();
				}
			}
			NUnit.Framework.Assert.AreEqual((long)content.Length, infile.Length());
			Configuration conf = new Configuration();
			ConfigUtil.AddLink(conf, "/foo/bar/baz", TestDir.ToURI());
			FileSystem vfs = FileSystem.Get(FsConstants.ViewfsUri, conf);
			NUnit.Framework.Assert.AreEqual(typeof(ViewFileSystem), vfs.GetType());
			FileStatus stat = vfs.GetFileStatus(new Path("/foo/bar/baz", testfilename));
			NUnit.Framework.Assert.AreEqual(content.Length, stat.GetLen());
			// check serialization/deserialization
			DataOutputBuffer dob = new DataOutputBuffer();
			stat.Write(dob);
			DataInputBuffer dib = new DataInputBuffer();
			dib.Reset(dob.GetData(), 0, dob.GetLength());
			FileStatus deSer = new FileStatus();
			deSer.ReadFields(dib);
			NUnit.Framework.Assert.AreEqual(content.Length, deSer.GetLen());
		}

		// Tests that ViewFileSystem.getFileChecksum calls res.targetFileSystem
		// .getFileChecksum with res.remainingPath and not with f
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestGetFileChecksum()
		{
			FileSystem mockFS = Org.Mockito.Mockito.Mock<FileSystem>();
			InodeTree.ResolveResult<FileSystem> res = new InodeTree.ResolveResult<FileSystem>
				(null, mockFS, null, new Path("someFile"));
			InodeTree<FileSystem> fsState = Org.Mockito.Mockito.Mock<InodeTree>();
			Org.Mockito.Mockito.When(fsState.Resolve("/tmp/someFile", true)).ThenReturn(res);
			ViewFileSystem vfs = Org.Mockito.Mockito.Mock<ViewFileSystem>();
			vfs.fsState = fsState;
			Org.Mockito.Mockito.When(vfs.GetFileChecksum(new Path("/tmp/someFile"))).ThenCallRealMethod
				();
			vfs.GetFileChecksum(new Path("/tmp/someFile"));
			Org.Mockito.Mockito.Verify(mockFS).GetFileChecksum(new Path("someFile"));
		}

		/// <exception cref="System.IO.IOException"/>
		[AfterClass]
		public static void Cleanup()
		{
			FileUtil.FullyDelete(TestDir);
		}
	}
}
