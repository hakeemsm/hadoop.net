using Sharpen;

namespace org.apache.hadoop.fs.viewfs
{
	public class TestViewFsTrash
	{
		internal org.apache.hadoop.fs.FileSystem fsTarget;

		internal org.apache.hadoop.fs.FileSystem fsView;

		internal org.apache.hadoop.conf.Configuration conf;

		internal org.apache.hadoop.fs.FileSystemTestHelper fileSystemTestHelper = new org.apache.hadoop.fs.FileSystemTestHelper
			();

		internal class TestLFS : org.apache.hadoop.fs.LocalFileSystem
		{
			internal org.apache.hadoop.fs.Path home;

			/// <exception cref="System.IO.IOException"/>
			internal TestLFS(TestViewFsTrash _enclosing)
				: this(new org.apache.hadoop.fs.Path(this._enclosing.fileSystemTestHelper.getTestRootDir
					()))
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="System.IO.IOException"/>
			internal TestLFS(TestViewFsTrash _enclosing, org.apache.hadoop.fs.Path home)
				: base()
			{
				this._enclosing = _enclosing;
				// the target file system - the mount will point here
				this.home = home;
			}

			public override org.apache.hadoop.fs.Path getHomeDirectory()
			{
				return this.home;
			}

			private readonly TestViewFsTrash _enclosing;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public virtual void setUp()
		{
			fsTarget = org.apache.hadoop.fs.FileSystem.getLocal(new org.apache.hadoop.conf.Configuration
				());
			fsTarget.mkdirs(new org.apache.hadoop.fs.Path(fileSystemTestHelper.getTestRootPath
				(fsTarget), "dir1"));
			conf = org.apache.hadoop.fs.viewfs.ViewFileSystemTestSetup.createConfig();
			fsView = org.apache.hadoop.fs.viewfs.ViewFileSystemTestSetup.setupForViewFileSystem
				(conf, fileSystemTestHelper, fsTarget);
			conf.set("fs.defaultFS", org.apache.hadoop.fs.FsConstants.VIEWFS_URI.ToString());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.TearDown]
		public virtual void tearDown()
		{
			org.apache.hadoop.fs.viewfs.ViewFileSystemTestSetup.tearDown(fileSystemTestHelper
				, fsTarget);
			fsTarget.delete(new org.apache.hadoop.fs.Path(fsTarget.getHomeDirectory(), ".Trash/Current"
				), true);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testTrash()
		{
			org.apache.hadoop.fs.TestTrash.trashShell(conf, fileSystemTestHelper.getTestRootPath
				(fsView), fsTarget, new org.apache.hadoop.fs.Path(fsTarget.getHomeDirectory(), ".Trash/Current"
				));
		}
	}
}
