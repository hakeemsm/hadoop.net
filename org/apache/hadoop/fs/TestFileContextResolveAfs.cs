using Sharpen;

namespace org.apache.hadoop.fs
{
	/// <summary>Tests resolution of AbstractFileSystems for a given path with symlinks.</summary>
	public class TestFileContextResolveAfs
	{
		static TestFileContextResolveAfs()
		{
			org.apache.hadoop.fs.FileSystem.enableSymlinks();
		}

		private static string TEST_ROOT_DIR_LOCAL = Sharpen.Runtime.getProperty("test.build.data"
			, "/tmp");

		private org.apache.hadoop.fs.FileContext fc;

		private org.apache.hadoop.fs.FileSystem localFs;

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.SetUp]
		public virtual void setup()
		{
			fc = org.apache.hadoop.fs.FileContext.getFileContext();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void testFileContextResolveAfs()
		{
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			localFs = org.apache.hadoop.fs.FileSystem.get(conf);
			org.apache.hadoop.fs.Path localPath = new org.apache.hadoop.fs.Path(TEST_ROOT_DIR_LOCAL
				 + "/TestFileContextResolveAfs1");
			org.apache.hadoop.fs.Path linkPath = localFs.makeQualified(new org.apache.hadoop.fs.Path
				(TEST_ROOT_DIR_LOCAL, "TestFileContextResolveAfs2"));
			localFs.mkdirs(new org.apache.hadoop.fs.Path(TEST_ROOT_DIR_LOCAL));
			localFs.create(localPath);
			fc.createSymlink(localPath, linkPath, true);
			System.Collections.Generic.ICollection<org.apache.hadoop.fs.AbstractFileSystem> afsList
				 = fc.resolveAbstractFileSystems(linkPath);
			NUnit.Framework.Assert.AreEqual(1, afsList.Count);
			localFs.deleteOnExit(localPath);
			localFs.deleteOnExit(linkPath);
			localFs.close();
		}
	}
}
