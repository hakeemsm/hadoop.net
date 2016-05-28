using System;
using System.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Security
{
	/// <summary>Unit tests for permission</summary>
	public class TestPermission
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(TestPermission));

		private static readonly Path RootPath = new Path("/data");

		private static readonly Path ChildDir1 = new Path(RootPath, "child1");

		private static readonly Path ChildDir2 = new Path(RootPath, "child2");

		private static readonly Path ChildFile1 = new Path(RootPath, "file1");

		private static readonly Path ChildFile2 = new Path(RootPath, "file2");

		private const int FileLen = 100;

		private static readonly Random Ran = new Random();

		private static readonly string UserName = "user" + Ran.Next();

		private static readonly string[] GroupNames = new string[] { "group1", "group2" };

		/// <exception cref="System.IO.IOException"/>
		internal static FsPermission CheckPermission(FileSystem fs, string path, FsPermission
			 expected)
		{
			FileStatus s = fs.GetFileStatus(new Path(path));
			Log.Info(s.GetPath() + ": " + s.IsDirectory() + " " + s.GetPermission() + ":" + s
				.GetOwner() + ":" + s.GetGroup());
			if (expected != null)
			{
				NUnit.Framework.Assert.AreEqual(expected, s.GetPermission());
				NUnit.Framework.Assert.AreEqual(expected.ToShort(), s.GetPermission().ToShort());
			}
			return s.GetPermission();
		}

		/// <summary>Tests backward compatibility.</summary>
		/// <remarks>
		/// Tests backward compatibility. Configuration can be
		/// either set with old param dfs.umask that takes decimal umasks
		/// or dfs.umaskmode that takes symbolic or octal umask.
		/// </remarks>
		[NUnit.Framework.Test]
		public virtual void TestBackwardCompatibility()
		{
			// Test 1 - old configuration key with decimal 
			// umask value should be handled when set using 
			// FSPermission.setUMask() API
			FsPermission perm = new FsPermission((short)18);
			Configuration conf = new Configuration();
			FsPermission.SetUMask(conf, perm);
			NUnit.Framework.Assert.AreEqual(18, FsPermission.GetUMask(conf).ToShort());
			// Test 2 - old configuration key set with decimal 
			// umask value should be handled
			perm = new FsPermission((short)18);
			conf = new Configuration();
			conf.Set(FsPermission.DeprecatedUmaskLabel, "18");
			NUnit.Framework.Assert.AreEqual(18, FsPermission.GetUMask(conf).ToShort());
			// Test 3 - old configuration key overrides the new one
			conf = new Configuration();
			conf.Set(FsPermission.DeprecatedUmaskLabel, "18");
			conf.Set(FsPermission.UmaskLabel, "000");
			NUnit.Framework.Assert.AreEqual(18, FsPermission.GetUMask(conf).ToShort());
			// Test 4 - new configuration key is handled
			conf = new Configuration();
			conf.Set(FsPermission.UmaskLabel, "022");
			NUnit.Framework.Assert.AreEqual(18, FsPermission.GetUMask(conf).ToShort());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestCreate()
		{
			Configuration conf = new HdfsConfiguration();
			conf.SetBoolean(DFSConfigKeys.DfsPermissionsEnabledKey, true);
			conf.Set(FsPermission.UmaskLabel, "000");
			MiniDFSCluster cluster = null;
			FileSystem fs = null;
			try
			{
				cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(3).Build();
				cluster.WaitActive();
				fs = FileSystem.Get(conf);
				FsPermission rootPerm = CheckPermission(fs, "/", null);
				FsPermission inheritPerm = FsPermission.CreateImmutable((short)(rootPerm.ToShort(
					) | 0xc0));
				FsPermission dirPerm = new FsPermission((short)0x1ff);
				fs.Mkdirs(new Path("/a1/a2/a3"), dirPerm);
				CheckPermission(fs, "/a1", dirPerm);
				CheckPermission(fs, "/a1/a2", dirPerm);
				CheckPermission(fs, "/a1/a2/a3", dirPerm);
				dirPerm = new FsPermission((short)0x53);
				FsPermission permission = FsPermission.CreateImmutable((short)(dirPerm.ToShort() 
					| 0xc0));
				fs.Mkdirs(new Path("/aa/1/aa/2/aa/3"), dirPerm);
				CheckPermission(fs, "/aa/1", permission);
				CheckPermission(fs, "/aa/1/aa/2", permission);
				CheckPermission(fs, "/aa/1/aa/2/aa/3", dirPerm);
				FsPermission filePerm = new FsPermission((short)0x124);
				Path p = new Path("/b1/b2/b3.txt");
				FSDataOutputStream @out = fs.Create(p, filePerm, true, conf.GetInt(CommonConfigurationKeys
					.IoFileBufferSizeKey, 4096), fs.GetDefaultReplication(p), fs.GetDefaultBlockSize
					(p), null);
				@out.Write(123);
				@out.Close();
				CheckPermission(fs, "/b1", inheritPerm);
				CheckPermission(fs, "/b1/b2", inheritPerm);
				CheckPermission(fs, "/b1/b2/b3.txt", filePerm);
				conf.Set(FsPermission.UmaskLabel, "022");
				permission = FsPermission.CreateImmutable((short)0x1b6);
				FileSystem.Mkdirs(fs, new Path("/c1"), new FsPermission(permission));
				FileSystem.Create(fs, new Path("/c1/c2.txt"), new FsPermission(permission));
				CheckPermission(fs, "/c1", permission);
				CheckPermission(fs, "/c1/c2.txt", permission);
			}
			finally
			{
				try
				{
					if (fs != null)
					{
						fs.Close();
					}
				}
				catch (Exception e)
				{
					Log.Error(StringUtils.StringifyException(e));
				}
				try
				{
					if (cluster != null)
					{
						cluster.Shutdown();
					}
				}
				catch (Exception e)
				{
					Log.Error(StringUtils.StringifyException(e));
				}
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestFilePermision()
		{
			Configuration conf = new HdfsConfiguration();
			conf.SetBoolean(DFSConfigKeys.DfsPermissionsEnabledKey, true);
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(3).Build();
			cluster.WaitActive();
			try
			{
				FileSystem nnfs = FileSystem.Get(conf);
				// test permissions on files that do not exist
				NUnit.Framework.Assert.IsFalse(nnfs.Exists(ChildFile1));
				try
				{
					nnfs.SetOwner(ChildFile1, "foo", "bar");
					NUnit.Framework.Assert.IsTrue(false);
				}
				catch (FileNotFoundException e)
				{
					Log.Info("GOOD: got " + e);
				}
				try
				{
					nnfs.SetPermission(ChildFile1, new FsPermission((short)0x1ff));
					NUnit.Framework.Assert.IsTrue(false);
				}
				catch (FileNotFoundException e)
				{
					Log.Info("GOOD: got " + e);
				}
				// make sure nn can take user specified permission (with default fs
				// permission umask applied)
				FSDataOutputStream @out = nnfs.Create(ChildFile1, new FsPermission((short)0x1ff), 
					true, 1024, (short)1, 1024, null);
				FileStatus status = nnfs.GetFileStatus(ChildFile1);
				// FS_PERMISSIONS_UMASK_DEFAULT is 0022
				NUnit.Framework.Assert.IsTrue(status.GetPermission().ToString().Equals("rwxr-xr-x"
					));
				nnfs.Delete(ChildFile1, false);
				// following dir/file creations are legal
				nnfs.Mkdirs(ChildDir1);
				@out = nnfs.Create(ChildFile1);
				status = nnfs.GetFileStatus(ChildFile1);
				NUnit.Framework.Assert.IsTrue(status.GetPermission().ToString().Equals("rw-r--r--"
					));
				byte[] data = new byte[FileLen];
				Ran.NextBytes(data);
				@out.Write(data);
				@out.Close();
				nnfs.SetPermission(ChildFile1, new FsPermission("700"));
				status = nnfs.GetFileStatus(ChildFile1);
				NUnit.Framework.Assert.IsTrue(status.GetPermission().ToString().Equals("rwx------"
					));
				// following read is legal
				byte[] dataIn = new byte[FileLen];
				FSDataInputStream fin = nnfs.Open(ChildFile1);
				int bytesRead = fin.Read(dataIn);
				NUnit.Framework.Assert.IsTrue(bytesRead == FileLen);
				for (int i = 0; i < FileLen; i++)
				{
					NUnit.Framework.Assert.AreEqual(data[i], dataIn[i]);
				}
				// test execution bit support for files
				nnfs.SetPermission(ChildFile1, new FsPermission("755"));
				status = nnfs.GetFileStatus(ChildFile1);
				NUnit.Framework.Assert.IsTrue(status.GetPermission().ToString().Equals("rwxr-xr-x"
					));
				nnfs.SetPermission(ChildFile1, new FsPermission("744"));
				status = nnfs.GetFileStatus(ChildFile1);
				NUnit.Framework.Assert.IsTrue(status.GetPermission().ToString().Equals("rwxr--r--"
					));
				nnfs.SetPermission(ChildFile1, new FsPermission("700"));
				////////////////////////////////////////////////////////////////
				// test illegal file/dir creation
				UserGroupInformation userGroupInfo = UserGroupInformation.CreateUserForTesting(UserName
					, GroupNames);
				FileSystem userfs = DFSTestUtil.GetFileSystemAs(userGroupInfo, conf);
				// make sure mkdir of a existing directory that is not owned by 
				// this user does not throw an exception.
				userfs.Mkdirs(ChildDir1);
				// illegal mkdir
				NUnit.Framework.Assert.IsTrue(!CanMkdirs(userfs, ChildDir2));
				// illegal file creation
				NUnit.Framework.Assert.IsTrue(!CanCreate(userfs, ChildFile2));
				// illegal file open
				NUnit.Framework.Assert.IsTrue(!CanOpen(userfs, ChildFile1));
				nnfs.SetPermission(RootPath, new FsPermission((short)0x1ed));
				nnfs.SetPermission(ChildDir1, new FsPermission("777"));
				nnfs.SetPermission(new Path("/"), new FsPermission((short)0x1ff));
				Path RenamePath = new Path("/foo/bar");
				userfs.Mkdirs(RenamePath);
				NUnit.Framework.Assert.IsTrue(CanRename(userfs, RenamePath, ChildDir1));
			}
			finally
			{
				cluster.Shutdown();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		internal static bool CanMkdirs(FileSystem fs, Path p)
		{
			try
			{
				fs.Mkdirs(p);
				return true;
			}
			catch (AccessControlException)
			{
				return false;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		internal static bool CanCreate(FileSystem fs, Path p)
		{
			try
			{
				fs.Create(p);
				return true;
			}
			catch (AccessControlException)
			{
				return false;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		internal static bool CanOpen(FileSystem fs, Path p)
		{
			try
			{
				fs.Open(p);
				return true;
			}
			catch (AccessControlException)
			{
				return false;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		internal static bool CanRename(FileSystem fs, Path src, Path dst)
		{
			try
			{
				fs.Rename(src, dst);
				return true;
			}
			catch (AccessControlException)
			{
				return false;
			}
		}
	}
}
