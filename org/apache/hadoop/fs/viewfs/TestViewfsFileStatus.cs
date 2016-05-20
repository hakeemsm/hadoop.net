using Sharpen;

namespace org.apache.hadoop.fs.viewfs
{
	/// <summary>The FileStatus is being serialized in MR as jobs are submitted.</summary>
	/// <remarks>
	/// The FileStatus is being serialized in MR as jobs are submitted.
	/// Since viewfs has overlayed ViewFsFileStatus, we ran into
	/// serialization problems. THis test is test the fix.
	/// </remarks>
	public class TestViewfsFileStatus
	{
		private static readonly java.io.File TEST_DIR = new java.io.File(Sharpen.Runtime.
			getProperty("test.build.data", "/tmp"), Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.viewfs.TestViewfsFileStatus
			)).getSimpleName());

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="java.net.URISyntaxException"/>
		[NUnit.Framework.Test]
		public virtual void testFileStatusSerialziation()
		{
			string testfilename = "testFileStatusSerialziation";
			TEST_DIR.mkdirs();
			java.io.File infile = new java.io.File(TEST_DIR, testfilename);
			byte[] content = Sharpen.Runtime.getBytesForString("dingos");
			java.io.FileOutputStream fos = null;
			try
			{
				fos = new java.io.FileOutputStream(infile);
				fos.write(content);
			}
			finally
			{
				if (fos != null)
				{
					fos.close();
				}
			}
			NUnit.Framework.Assert.AreEqual((long)content.Length, infile.length());
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			org.apache.hadoop.fs.viewfs.ConfigUtil.addLink(conf, "/foo/bar/baz", TEST_DIR.toURI
				());
			org.apache.hadoop.fs.FileSystem vfs = org.apache.hadoop.fs.FileSystem.get(org.apache.hadoop.fs.FsConstants
				.VIEWFS_URI, conf);
			NUnit.Framework.Assert.AreEqual(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.viewfs.ViewFileSystem
				)), Sharpen.Runtime.getClassForObject(vfs));
			org.apache.hadoop.fs.FileStatus stat = vfs.getFileStatus(new org.apache.hadoop.fs.Path
				("/foo/bar/baz", testfilename));
			NUnit.Framework.Assert.AreEqual(content.Length, stat.getLen());
			// check serialization/deserialization
			org.apache.hadoop.io.DataOutputBuffer dob = new org.apache.hadoop.io.DataOutputBuffer
				();
			stat.write(dob);
			org.apache.hadoop.io.DataInputBuffer dib = new org.apache.hadoop.io.DataInputBuffer
				();
			dib.reset(dob.getData(), 0, dob.getLength());
			org.apache.hadoop.fs.FileStatus deSer = new org.apache.hadoop.fs.FileStatus();
			deSer.readFields(dib);
			NUnit.Framework.Assert.AreEqual(content.Length, deSer.getLen());
		}

		// Tests that ViewFileSystem.getFileChecksum calls res.targetFileSystem
		// .getFileChecksum with res.remainingPath and not with f
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testGetFileChecksum()
		{
			org.apache.hadoop.fs.FileSystem mockFS = org.mockito.Mockito.mock<org.apache.hadoop.fs.FileSystem
				>();
			org.apache.hadoop.fs.viewfs.InodeTree.ResolveResult<org.apache.hadoop.fs.FileSystem
				> res = new org.apache.hadoop.fs.viewfs.InodeTree.ResolveResult<org.apache.hadoop.fs.FileSystem
				>(null, mockFS, null, new org.apache.hadoop.fs.Path("someFile"));
			org.apache.hadoop.fs.viewfs.InodeTree<org.apache.hadoop.fs.FileSystem> fsState = 
				org.mockito.Mockito.mock<org.apache.hadoop.fs.viewfs.InodeTree>();
			org.mockito.Mockito.when(fsState.resolve("/tmp/someFile", true)).thenReturn(res);
			org.apache.hadoop.fs.viewfs.ViewFileSystem vfs = org.mockito.Mockito.mock<org.apache.hadoop.fs.viewfs.ViewFileSystem
				>();
			vfs.fsState = fsState;
			org.mockito.Mockito.when(vfs.getFileChecksum(new org.apache.hadoop.fs.Path("/tmp/someFile"
				))).thenCallRealMethod();
			vfs.getFileChecksum(new org.apache.hadoop.fs.Path("/tmp/someFile"));
			org.mockito.Mockito.verify(mockFS).getFileChecksum(new org.apache.hadoop.fs.Path(
				"someFile"));
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.AfterClass]
		public static void cleanup()
		{
			org.apache.hadoop.fs.FileUtil.fullyDelete(TEST_DIR);
		}
	}
}
