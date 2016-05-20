using Sharpen;

namespace org.apache.hadoop.fs.viewfs
{
	/// <summary>Verify that viewfs propagates certain methods to the underlying fs</summary>
	public class TestViewFileSystemDelegation
	{
		internal static org.apache.hadoop.conf.Configuration conf;

		internal static org.apache.hadoop.fs.FileSystem viewFs;

		internal static org.apache.hadoop.fs.viewfs.TestViewFileSystemDelegation.FakeFileSystem
			 fs1;

		internal static org.apache.hadoop.fs.viewfs.TestViewFileSystemDelegation.FakeFileSystem
			 fs2;

		//extends ViewFileSystemTestSetup {
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.BeforeClass]
		public static void setup()
		{
			conf = org.apache.hadoop.fs.viewfs.ViewFileSystemTestSetup.createConfig();
			fs1 = setupFileSystem(new java.net.URI("fs1:/"), Sharpen.Runtime.getClassForType(
				typeof(org.apache.hadoop.fs.viewfs.TestViewFileSystemDelegation.FakeFileSystem))
				);
			fs2 = setupFileSystem(new java.net.URI("fs2:/"), Sharpen.Runtime.getClassForType(
				typeof(org.apache.hadoop.fs.viewfs.TestViewFileSystemDelegation.FakeFileSystem))
				);
			viewFs = org.apache.hadoop.fs.FileSystem.get(org.apache.hadoop.fs.FsConstants.VIEWFS_URI
				, conf);
		}

		/// <exception cref="System.Exception"/>
		internal static org.apache.hadoop.fs.viewfs.TestViewFileSystemDelegation.FakeFileSystem
			 setupFileSystem(java.net.URI uri, java.lang.Class clazz)
		{
			string scheme = uri.getScheme();
			conf.set("fs." + scheme + ".impl", clazz.getName());
			org.apache.hadoop.fs.viewfs.TestViewFileSystemDelegation.FakeFileSystem fs = (org.apache.hadoop.fs.viewfs.TestViewFileSystemDelegation.FakeFileSystem
				)org.apache.hadoop.fs.FileSystem.get(uri, conf);
			NUnit.Framework.Assert.AreEqual(uri, fs.getUri());
			org.apache.hadoop.fs.Path targetPath = new org.apache.hadoop.fs.FileSystemTestHelper
				().getAbsoluteTestRootPath(fs);
			org.apache.hadoop.fs.viewfs.ConfigUtil.addLink(conf, "/mounts/" + scheme, targetPath
				.toUri());
			return fs;
		}

		/// <exception cref="System.Exception"/>
		private static org.apache.hadoop.fs.FileSystem setupMockFileSystem(org.apache.hadoop.conf.Configuration
			 conf, java.net.URI uri)
		{
			string scheme = uri.getScheme();
			conf.set("fs." + scheme + ".impl", Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.viewfs.TestChRootedFileSystem.MockFileSystem
				)).getName());
			org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem.get(uri, conf
				);
			org.apache.hadoop.fs.viewfs.ConfigUtil.addLink(conf, "/mounts/" + scheme, uri);
			return ((org.apache.hadoop.fs.viewfs.TestChRootedFileSystem.MockFileSystem)fs).getRawFileSystem
				();
		}

		[NUnit.Framework.Test]
		public virtual void testSanity()
		{
			NUnit.Framework.Assert.AreEqual("fs1:/", fs1.getUri().ToString());
			NUnit.Framework.Assert.AreEqual("fs2:/", fs2.getUri().ToString());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testVerifyChecksum()
		{
			checkVerifyChecksum(false);
			checkVerifyChecksum(true);
		}

		/// <summary>
		/// Tests that ViewFileSystem dispatches calls for every ACL method through the
		/// mount table to the correct underlying FileSystem with all Path arguments
		/// translated as required.
		/// </summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testAclMethods()
		{
			org.apache.hadoop.conf.Configuration conf = org.apache.hadoop.fs.viewfs.ViewFileSystemTestSetup
				.createConfig();
			org.apache.hadoop.fs.FileSystem mockFs1 = setupMockFileSystem(conf, new java.net.URI
				("mockfs1:/"));
			org.apache.hadoop.fs.FileSystem mockFs2 = setupMockFileSystem(conf, new java.net.URI
				("mockfs2:/"));
			org.apache.hadoop.fs.FileSystem viewFs = org.apache.hadoop.fs.FileSystem.get(org.apache.hadoop.fs.FsConstants
				.VIEWFS_URI, conf);
			org.apache.hadoop.fs.Path viewFsPath1 = new org.apache.hadoop.fs.Path("/mounts/mockfs1/a/b/c"
				);
			org.apache.hadoop.fs.Path mockFsPath1 = new org.apache.hadoop.fs.Path("/a/b/c");
			org.apache.hadoop.fs.Path viewFsPath2 = new org.apache.hadoop.fs.Path("/mounts/mockfs2/d/e/f"
				);
			org.apache.hadoop.fs.Path mockFsPath2 = new org.apache.hadoop.fs.Path("/d/e/f");
			System.Collections.Generic.IList<org.apache.hadoop.fs.permission.AclEntry> entries
				 = java.util.Collections.emptyList();
			viewFs.modifyAclEntries(viewFsPath1, entries);
			org.mockito.Mockito.verify(mockFs1).modifyAclEntries(mockFsPath1, entries);
			viewFs.modifyAclEntries(viewFsPath2, entries);
			org.mockito.Mockito.verify(mockFs2).modifyAclEntries(mockFsPath2, entries);
			viewFs.removeAclEntries(viewFsPath1, entries);
			org.mockito.Mockito.verify(mockFs1).removeAclEntries(mockFsPath1, entries);
			viewFs.removeAclEntries(viewFsPath2, entries);
			org.mockito.Mockito.verify(mockFs2).removeAclEntries(mockFsPath2, entries);
			viewFs.removeDefaultAcl(viewFsPath1);
			org.mockito.Mockito.verify(mockFs1).removeDefaultAcl(mockFsPath1);
			viewFs.removeDefaultAcl(viewFsPath2);
			org.mockito.Mockito.verify(mockFs2).removeDefaultAcl(mockFsPath2);
			viewFs.removeAcl(viewFsPath1);
			org.mockito.Mockito.verify(mockFs1).removeAcl(mockFsPath1);
			viewFs.removeAcl(viewFsPath2);
			org.mockito.Mockito.verify(mockFs2).removeAcl(mockFsPath2);
			viewFs.setAcl(viewFsPath1, entries);
			org.mockito.Mockito.verify(mockFs1).setAcl(mockFsPath1, entries);
			viewFs.setAcl(viewFsPath2, entries);
			org.mockito.Mockito.verify(mockFs2).setAcl(mockFsPath2, entries);
			viewFs.getAclStatus(viewFsPath1);
			org.mockito.Mockito.verify(mockFs1).getAclStatus(mockFsPath1);
			viewFs.getAclStatus(viewFsPath2);
			org.mockito.Mockito.verify(mockFs2).getAclStatus(mockFsPath2);
		}

		internal virtual void checkVerifyChecksum(bool flag)
		{
			viewFs.setVerifyChecksum(flag);
			NUnit.Framework.Assert.AreEqual(flag, fs1.getVerifyChecksum());
			NUnit.Framework.Assert.AreEqual(flag, fs2.getVerifyChecksum());
		}

		internal class FakeFileSystem : org.apache.hadoop.fs.LocalFileSystem
		{
			internal bool verifyChecksum = true;

			internal java.net.URI uri;

			/// <exception cref="System.IO.IOException"/>
			public override void initialize(java.net.URI uri, org.apache.hadoop.conf.Configuration
				 conf)
			{
				base.initialize(uri, conf);
				this.uri = uri;
			}

			public override java.net.URI getUri()
			{
				return uri;
			}

			public override void setVerifyChecksum(bool verifyChecksum)
			{
				this.verifyChecksum = verifyChecksum;
			}

			public virtual bool getVerifyChecksum()
			{
				return verifyChecksum;
			}
		}
	}
}
