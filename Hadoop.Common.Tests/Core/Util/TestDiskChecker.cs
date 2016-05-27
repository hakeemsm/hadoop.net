using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Test;
using Sharpen;

namespace Org.Apache.Hadoop.Util
{
	public class TestDiskChecker
	{
		internal readonly FsPermission defaultPerm = new FsPermission("755");

		internal readonly FsPermission invalidPerm = new FsPermission("000");

		/// <exception cref="System.Exception"/>
		public virtual void TestMkdirs_dirExists()
		{
			_mkdirs(true, defaultPerm, defaultPerm);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestMkdirs_noDir()
		{
			_mkdirs(false, defaultPerm, defaultPerm);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestMkdirs_dirExists_badUmask()
		{
			_mkdirs(true, defaultPerm, invalidPerm);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestMkdirs_noDir_badUmask()
		{
			_mkdirs(false, defaultPerm, invalidPerm);
		}

		/// <exception cref="System.Exception"/>
		private void _mkdirs(bool exists, FsPermission before, FsPermission after)
		{
			FilePath localDir = MockitoMaker.Make(MockitoMaker.Stub<FilePath>().Returning(exists
				).from.Exists());
			Org.Mockito.Mockito.When(localDir.Mkdir()).ThenReturn(true);
			Path dir = Org.Mockito.Mockito.Mock<Path>();
			// use default stubs
			LocalFileSystem fs = MockitoMaker.Make(MockitoMaker.Stub<LocalFileSystem>().Returning
				(localDir).from.PathToFile(dir));
			FileStatus stat = MockitoMaker.Make(MockitoMaker.Stub<FileStatus>().Returning(after
				).from.GetPermission());
			Org.Mockito.Mockito.When(fs.GetFileStatus(dir)).ThenReturn(stat);
			try
			{
				DiskChecker.MkdirsWithExistsAndPermissionCheck(fs, dir, before);
				if (!exists)
				{
					Org.Mockito.Mockito.Verify(fs).SetPermission(dir, before);
				}
				else
				{
					Org.Mockito.Mockito.Verify(fs).GetFileStatus(dir);
					Org.Mockito.Mockito.Verify(stat).GetPermission();
				}
			}
			catch (DiskChecker.DiskErrorException e)
			{
				if (before != after)
				{
					NUnit.Framework.Assert.IsTrue(e.Message.StartsWith("Incorrect permission"));
				}
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestCheckDir_normal()
		{
			_checkDirs(true, new FsPermission("755"), true);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestCheckDir_notDir()
		{
			_checkDirs(false, new FsPermission("000"), false);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestCheckDir_notReadable()
		{
			_checkDirs(true, new FsPermission("000"), false);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestCheckDir_notWritable()
		{
			_checkDirs(true, new FsPermission("444"), false);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestCheckDir_notListable()
		{
			_checkDirs(true, new FsPermission("666"), false);
		}

		// not listable
		/// <exception cref="System.Exception"/>
		private void _checkDirs(bool isDir, FsPermission perm, bool success)
		{
			FilePath localDir = FilePath.CreateTempFile("test", "tmp");
			if (isDir)
			{
				localDir.Delete();
				localDir.Mkdir();
			}
			Shell.ExecCommand(Shell.GetSetPermissionCommand(string.Format("%04o", perm.ToShort
				()), false, localDir.GetAbsolutePath()));
			try
			{
				DiskChecker.CheckDir(FileSystem.GetLocal(new Configuration()), new Path(localDir.
					GetAbsolutePath()), perm);
				NUnit.Framework.Assert.IsTrue("checkDir success", success);
			}
			catch (DiskChecker.DiskErrorException)
			{
				NUnit.Framework.Assert.IsFalse("checkDir success", success);
			}
			localDir.Delete();
		}

		/// <summary>
		/// These test cases test to test the creation of a local folder with correct
		/// permission for result of mapper.
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestCheckDir_normal_local()
		{
			_checkDirs(true, "755", true);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestCheckDir_notDir_local()
		{
			_checkDirs(false, "000", false);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestCheckDir_notReadable_local()
		{
			_checkDirs(true, "000", false);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestCheckDir_notWritable_local()
		{
			_checkDirs(true, "444", false);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestCheckDir_notListable_local()
		{
			_checkDirs(true, "666", false);
		}

		/// <exception cref="System.Exception"/>
		private void _checkDirs(bool isDir, string perm, bool success)
		{
			FilePath localDir = FilePath.CreateTempFile("test", "tmp");
			if (isDir)
			{
				localDir.Delete();
				localDir.Mkdir();
			}
			Shell.ExecCommand(Shell.GetSetPermissionCommand(perm, false, localDir.GetAbsolutePath
				()));
			try
			{
				DiskChecker.CheckDir(localDir);
				NUnit.Framework.Assert.IsTrue("checkDir success", success);
			}
			catch (DiskChecker.DiskErrorException e)
			{
				Sharpen.Runtime.PrintStackTrace(e);
				NUnit.Framework.Assert.IsFalse("checkDir success", success);
			}
			localDir.Delete();
			System.Console.Out.WriteLine("checkDir success: " + success);
		}
	}
}
