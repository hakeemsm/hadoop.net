using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Test;
using Sharpen;

namespace Org.Apache.Hadoop.FS.Http.Client
{
	public class TestHttpFSFileSystemLocalFileSystem : BaseTestHttpFSWith
	{
		private static string PathPrefix;

		static TestHttpFSFileSystemLocalFileSystem()
		{
			new TestDirHelper();
			string prefix = Runtime.GetProperty("test.build.dir", "target/test-dir") + "/local";
			FilePath file = new FilePath(prefix);
			file.Mkdirs();
			PathPrefix = file.GetAbsolutePath();
		}

		public TestHttpFSFileSystemLocalFileSystem(BaseTestHttpFSWith.Operation operation
			)
			: base(operation)
		{
		}

		protected internal override Path GetProxiedFSTestDir()
		{
			return AddPrefix(new Path(TestDirHelper.GetTestDir().GetAbsolutePath()));
		}

		protected internal override string GetProxiedFSURI()
		{
			return "file:///";
		}

		protected internal override Configuration GetProxiedFSConf()
		{
			Configuration conf = new Configuration(false);
			conf.Set(CommonConfigurationKeysPublic.FsDefaultNameKey, GetProxiedFSURI());
			return conf;
		}

		protected internal virtual Path AddPrefix(Path path)
		{
			return Path.MergePaths(new Path(PathPrefix), path);
		}

		/// <exception cref="System.Exception"/>
		protected internal override void TestSetPermission()
		{
			if (Path.Windows)
			{
				FileSystem fs = FileSystem.Get(GetProxiedFSConf());
				Path path = new Path(GetProxiedFSTestDir(), "foodir");
				fs.Mkdirs(path);
				fs = GetHttpFSFileSystem();
				FsPermission permission1 = new FsPermission(FsAction.ReadWrite, FsAction.None, FsAction
					.None);
				fs.SetPermission(path, permission1);
				fs.Close();
				fs = FileSystem.Get(GetProxiedFSConf());
				FileStatus status1 = fs.GetFileStatus(path);
				fs.Close();
				FsPermission permission2 = status1.GetPermission();
				NUnit.Framework.Assert.AreEqual(permission2, permission1);
			}
			else
			{
				// sticky bit not supported on Windows with local file system, so the
				// subclass skips that part of the test
				base.TestSetPermission();
			}
		}
	}
}
