using System.Collections.Generic;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Sharpen;

namespace Org.Apache.Hadoop.FS
{
	/// <summary>Tests resolution of AbstractFileSystems for a given path with symlinks.</summary>
	public class TestFileContextResolveAfs
	{
		static TestFileContextResolveAfs()
		{
			FileSystem.EnableSymlinks();
		}

		private static string TestRootDirLocal = Runtime.GetProperty("test.build.data", "/tmp"
			);

		private FileContext fc;

		private FileSystem localFs;

		/// <exception cref="System.IO.IOException"/>
		[SetUp]
		public virtual void Setup()
		{
			fc = FileContext.GetFileContext();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestFileContextResolveAfs()
		{
			Configuration conf = new Configuration();
			localFs = FileSystem.Get(conf);
			Path localPath = new Path(TestRootDirLocal + "/TestFileContextResolveAfs1");
			Path linkPath = localFs.MakeQualified(new Path(TestRootDirLocal, "TestFileContextResolveAfs2"
				));
			localFs.Mkdirs(new Path(TestRootDirLocal));
			localFs.Create(localPath);
			fc.CreateSymlink(localPath, linkPath, true);
			ICollection<AbstractFileSystem> afsList = fc.ResolveAbstractFileSystems(linkPath);
			Assert.Equal(1, afsList.Count);
			localFs.DeleteOnExit(localPath);
			localFs.DeleteOnExit(linkPath);
			localFs.Close();
		}
	}
}
