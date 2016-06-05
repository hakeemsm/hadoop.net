using System;
using System.Collections.Generic;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Sharpen;

namespace Org.Apache.Hadoop.FS.Viewfs
{
	/// <summary>Verify that viewfs propagates certain methods to the underlying fs</summary>
	public class TestViewFileSystemDelegation
	{
		internal static Configuration conf;

		internal static FileSystem viewFs;

		internal static TestViewFileSystemDelegation.FakeFileSystem fs1;

		internal static TestViewFileSystemDelegation.FakeFileSystem fs2;

		//extends ViewFileSystemTestSetup {
		/// <exception cref="System.Exception"/>
		[BeforeClass]
		public static void Setup()
		{
			conf = ViewFileSystemTestSetup.CreateConfig();
			fs1 = SetupFileSystem(new URI("fs1:/"), typeof(TestViewFileSystemDelegation.FakeFileSystem
				));
			fs2 = SetupFileSystem(new URI("fs2:/"), typeof(TestViewFileSystemDelegation.FakeFileSystem
				));
			viewFs = FileSystem.Get(FsConstants.ViewfsUri, conf);
		}

		/// <exception cref="System.Exception"/>
		internal static TestViewFileSystemDelegation.FakeFileSystem SetupFileSystem(URI uri
			, Type clazz)
		{
			string scheme = uri.GetScheme();
			conf.Set("fs." + scheme + ".impl", clazz.FullName);
			TestViewFileSystemDelegation.FakeFileSystem fs = (TestViewFileSystemDelegation.FakeFileSystem
				)FileSystem.Get(uri, conf);
			Assert.Equal(uri, fs.GetUri());
			Path targetPath = new FileSystemTestHelper().GetAbsoluteTestRootPath(fs);
			ConfigUtil.AddLink(conf, "/mounts/" + scheme, targetPath.ToUri());
			return fs;
		}

		/// <exception cref="System.Exception"/>
		private static FileSystem SetupMockFileSystem(Configuration conf, URI uri)
		{
			string scheme = uri.GetScheme();
			conf.Set("fs." + scheme + ".impl", typeof(TestChRootedFileSystem.MockFileSystem).
				FullName);
			FileSystem fs = FileSystem.Get(uri, conf);
			ConfigUtil.AddLink(conf, "/mounts/" + scheme, uri);
			return ((TestChRootedFileSystem.MockFileSystem)fs).GetRawFileSystem();
		}

		[Fact]
		public virtual void TestSanity()
		{
			Assert.Equal("fs1:/", fs1.GetUri().ToString());
			Assert.Equal("fs2:/", fs2.GetUri().ToString());
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestVerifyChecksum()
		{
			CheckVerifyChecksum(false);
			CheckVerifyChecksum(true);
		}

		/// <summary>
		/// Tests that ViewFileSystem dispatches calls for every ACL method through the
		/// mount table to the correct underlying FileSystem with all Path arguments
		/// translated as required.
		/// </summary>
		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestAclMethods()
		{
			Configuration conf = ViewFileSystemTestSetup.CreateConfig();
			FileSystem mockFs1 = SetupMockFileSystem(conf, new URI("mockfs1:/"));
			FileSystem mockFs2 = SetupMockFileSystem(conf, new URI("mockfs2:/"));
			FileSystem viewFs = FileSystem.Get(FsConstants.ViewfsUri, conf);
			Path viewFsPath1 = new Path("/mounts/mockfs1/a/b/c");
			Path mockFsPath1 = new Path("/a/b/c");
			Path viewFsPath2 = new Path("/mounts/mockfs2/d/e/f");
			Path mockFsPath2 = new Path("/d/e/f");
			IList<AclEntry> entries = Collections.EmptyList();
			viewFs.ModifyAclEntries(viewFsPath1, entries);
			Org.Mockito.Mockito.Verify(mockFs1).ModifyAclEntries(mockFsPath1, entries);
			viewFs.ModifyAclEntries(viewFsPath2, entries);
			Org.Mockito.Mockito.Verify(mockFs2).ModifyAclEntries(mockFsPath2, entries);
			viewFs.RemoveAclEntries(viewFsPath1, entries);
			Org.Mockito.Mockito.Verify(mockFs1).RemoveAclEntries(mockFsPath1, entries);
			viewFs.RemoveAclEntries(viewFsPath2, entries);
			Org.Mockito.Mockito.Verify(mockFs2).RemoveAclEntries(mockFsPath2, entries);
			viewFs.RemoveDefaultAcl(viewFsPath1);
			Org.Mockito.Mockito.Verify(mockFs1).RemoveDefaultAcl(mockFsPath1);
			viewFs.RemoveDefaultAcl(viewFsPath2);
			Org.Mockito.Mockito.Verify(mockFs2).RemoveDefaultAcl(mockFsPath2);
			viewFs.RemoveAcl(viewFsPath1);
			Org.Mockito.Mockito.Verify(mockFs1).RemoveAcl(mockFsPath1);
			viewFs.RemoveAcl(viewFsPath2);
			Org.Mockito.Mockito.Verify(mockFs2).RemoveAcl(mockFsPath2);
			viewFs.SetAcl(viewFsPath1, entries);
			Org.Mockito.Mockito.Verify(mockFs1).SetAcl(mockFsPath1, entries);
			viewFs.SetAcl(viewFsPath2, entries);
			Org.Mockito.Mockito.Verify(mockFs2).SetAcl(mockFsPath2, entries);
			viewFs.GetAclStatus(viewFsPath1);
			Org.Mockito.Mockito.Verify(mockFs1).GetAclStatus(mockFsPath1);
			viewFs.GetAclStatus(viewFsPath2);
			Org.Mockito.Mockito.Verify(mockFs2).GetAclStatus(mockFsPath2);
		}

		internal virtual void CheckVerifyChecksum(bool flag)
		{
			viewFs.SetVerifyChecksum(flag);
			Assert.Equal(flag, fs1.GetVerifyChecksum());
			Assert.Equal(flag, fs2.GetVerifyChecksum());
		}

		internal class FakeFileSystem : LocalFileSystem
		{
			internal bool verifyChecksum = true;

			internal URI uri;

			/// <exception cref="System.IO.IOException"/>
			public override void Initialize(URI uri, Configuration conf)
			{
				base.Initialize(uri, conf);
				this.uri = uri;
			}

			public override URI GetUri()
			{
				return uri;
			}

			public override void SetVerifyChecksum(bool verifyChecksum)
			{
				this.verifyChecksum = verifyChecksum;
			}

			public virtual bool GetVerifyChecksum()
			{
				return verifyChecksum;
			}
		}
	}
}
