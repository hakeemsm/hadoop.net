using System.IO;
using System.Text;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.FS.Permission;
using Sharpen;

namespace Org.Apache.Hadoop.FS
{
	public class TestFileStatus
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(TestFileStatus));

		/// <summary>
		/// Values for creating
		/// <see cref="FileStatus"/>
		/// in some tests
		/// </summary>
		internal const int Length = 1;

		internal const int Replication = 2;

		internal const long Blksize = 3;

		internal const long Mtime = 4;

		internal const long Atime = 5;

		internal const string Owner = "owner";

		internal const string Group = "group";

		internal static readonly FsPermission Permission = FsPermission.ValueOf("-rw-rw-rw-"
			);

		internal static readonly Path Path = new Path("path");

		/// <summary>Check that the write and readField methods work correctly.</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestFileStatusWritable()
		{
			FileStatus[] tests = new FileStatus[] { new FileStatus(1, false, 5, 3, 4, 5, null
				, string.Empty, string.Empty, new Path("/a/b")), new FileStatus(0, false, 1, 2, 
				3, new Path("/")), new FileStatus(1, false, 5, 3, 4, 5, null, string.Empty, string.Empty
				, new Path("/a/b")) };
			Log.Info("Writing FileStatuses to a ByteArrayOutputStream");
			// Writing input list to ByteArrayOutputStream
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			DataOutput @out = new DataOutputStream(baos);
			foreach (FileStatus fs in tests)
			{
				fs.Write(@out);
			}
			Log.Info("Creating ByteArrayInputStream object");
			BinaryReader @in = new DataInputStream(new ByteArrayInputStream(baos.ToByteArray()));
			Log.Info("Testing if read objects are equal to written ones");
			FileStatus dest = new FileStatus();
			int iterator = 0;
			foreach (FileStatus fs_1 in tests)
			{
				dest.ReadFields(@in);
				NUnit.Framework.Assert.AreEqual("Different FileStatuses in iteration " + iterator
					, dest, fs_1);
				iterator++;
			}
		}

		/// <summary>Check that the full parameter constructor works correctly.</summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void ConstructorFull()
		{
			bool isdir = false;
			Path symlink = new Path("symlink");
			FileStatus fileStatus = new FileStatus(Length, isdir, Replication, Blksize, Mtime
				, Atime, Permission, Owner, Group, symlink, Path);
			ValidateAccessors(fileStatus, Length, isdir, Replication, Blksize, Mtime, Atime, 
				Permission, Owner, Group, symlink, Path);
		}

		/// <summary>Check that the non-symlink constructor works correctly.</summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void ConstructorNoSymlink()
		{
			bool isdir = true;
			FileStatus fileStatus = new FileStatus(Length, isdir, Replication, Blksize, Mtime
				, Atime, Permission, Owner, Group, Path);
			ValidateAccessors(fileStatus, Length, isdir, Replication, Blksize, Mtime, Atime, 
				Permission, Owner, Group, null, Path);
		}

		/// <summary>
		/// Check that the constructor without owner, group and permissions works
		/// correctly.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void ConstructorNoOwner()
		{
			bool isdir = true;
			FileStatus fileStatus = new FileStatus(Length, isdir, Replication, Blksize, Mtime
				, Path);
			ValidateAccessors(fileStatus, Length, isdir, Replication, Blksize, Mtime, 0, FsPermission
				.GetDirDefault(), string.Empty, string.Empty, null, Path);
		}

		/// <summary>Check that the no parameter constructor works correctly.</summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void ConstructorBlank()
		{
			FileStatus fileStatus = new FileStatus();
			ValidateAccessors(fileStatus, 0, false, 0, 0, 0, 0, FsPermission.GetFileDefault()
				, string.Empty, string.Empty, null, null);
		}

		/// <summary>Check that FileStatus are equal if their paths are equal.</summary>
		[NUnit.Framework.Test]
		public virtual void TestEquals()
		{
			Path path = new Path("path");
			FileStatus fileStatus1 = new FileStatus(1, true, 1, 1, 1, 1, FsPermission.ValueOf
				("-rw-rw-rw-"), "one", "one", null, path);
			FileStatus fileStatus2 = new FileStatus(2, true, 2, 2, 2, 2, FsPermission.ValueOf
				("---x--x--x"), "two", "two", null, path);
			NUnit.Framework.Assert.AreEqual(fileStatus1, fileStatus2);
		}

		/// <summary>Check that FileStatus are not equal if their paths are not equal.</summary>
		[NUnit.Framework.Test]
		public virtual void TestNotEquals()
		{
			Path path1 = new Path("path1");
			Path path2 = new Path("path2");
			FileStatus fileStatus1 = new FileStatus(1, true, 1, 1, 1, 1, FsPermission.ValueOf
				("-rw-rw-rw-"), "one", "one", null, path1);
			FileStatus fileStatus2 = new FileStatus(1, true, 1, 1, 1, 1, FsPermission.ValueOf
				("-rw-rw-rw-"), "one", "one", null, path2);
			NUnit.Framework.Assert.IsFalse(fileStatus1.Equals(fileStatus2));
			NUnit.Framework.Assert.IsFalse(fileStatus2.Equals(fileStatus1));
		}

		/// <summary>Check that toString produces the expected output for a file.</summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void ToStringFile()
		{
			bool isdir = false;
			FileStatus fileStatus = new FileStatus(Length, isdir, Replication, Blksize, Mtime
				, Atime, Permission, Owner, Group, null, Path);
			ValidateToString(fileStatus);
		}

		/// <summary>Check that toString produces the expected output for a directory.</summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void ToStringDir()
		{
			FileStatus fileStatus = new FileStatus(Length, true, Replication, Blksize, Mtime, 
				Atime, Permission, Owner, Group, null, Path);
			ValidateToString(fileStatus);
		}

		/// <summary>Check that toString produces the expected output for a symlink.</summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void ToStringSymlink()
		{
			bool isdir = false;
			Path symlink = new Path("symlink");
			FileStatus fileStatus = new FileStatus(Length, isdir, Replication, Blksize, Mtime
				, Atime, Permission, Owner, Group, symlink, Path);
			ValidateToString(fileStatus);
		}

		/// <summary>Validate the accessors for FileStatus.</summary>
		/// <param name="fileStatus">FileStatus to checked</param>
		/// <param name="length">expected length</param>
		/// <param name="isdir">expected isDirectory</param>
		/// <param name="replication">expected replication</param>
		/// <param name="blocksize">expected blocksize</param>
		/// <param name="mtime">expected modification time</param>
		/// <param name="atime">expected access time</param>
		/// <param name="permission">expected permission</param>
		/// <param name="owner">expected owner</param>
		/// <param name="group">expected group</param>
		/// <param name="symlink">expected symlink</param>
		/// <param name="path">expected path</param>
		/// <exception cref="System.IO.IOException"/>
		private void ValidateAccessors(FileStatus fileStatus, long length, bool isdir, int
			 replication, long blocksize, long mtime, long atime, FsPermission permission, string
			 owner, string group, Path symlink, Path path)
		{
			NUnit.Framework.Assert.AreEqual(length, fileStatus.GetLen());
			NUnit.Framework.Assert.AreEqual(isdir, fileStatus.IsDirectory());
			NUnit.Framework.Assert.AreEqual(replication, fileStatus.GetReplication());
			NUnit.Framework.Assert.AreEqual(blocksize, fileStatus.GetBlockSize());
			NUnit.Framework.Assert.AreEqual(mtime, fileStatus.GetModificationTime());
			NUnit.Framework.Assert.AreEqual(atime, fileStatus.GetAccessTime());
			NUnit.Framework.Assert.AreEqual(permission, fileStatus.GetPermission());
			NUnit.Framework.Assert.AreEqual(owner, fileStatus.GetOwner());
			NUnit.Framework.Assert.AreEqual(group, fileStatus.GetGroup());
			if (symlink == null)
			{
				NUnit.Framework.Assert.IsFalse(fileStatus.IsSymlink());
			}
			else
			{
				NUnit.Framework.Assert.IsTrue(fileStatus.IsSymlink());
				NUnit.Framework.Assert.AreEqual(symlink, fileStatus.GetSymlink());
			}
			NUnit.Framework.Assert.AreEqual(path, fileStatus.GetPath());
		}

		/// <summary>Validates the toString method for FileStatus.</summary>
		/// <param name="fileStatus">FileStatus to be validated</param>
		/// <exception cref="System.IO.IOException"/>
		private void ValidateToString(FileStatus fileStatus)
		{
			StringBuilder expected = new StringBuilder();
			expected.Append("FileStatus{");
			expected.Append("path=").Append(fileStatus.GetPath()).Append("; ");
			expected.Append("isDirectory=").Append(fileStatus.IsDirectory()).Append("; ");
			if (!fileStatus.IsDirectory())
			{
				expected.Append("length=").Append(fileStatus.GetLen()).Append("; ");
				expected.Append("replication=").Append(fileStatus.GetReplication()).Append("; ");
				expected.Append("blocksize=").Append(fileStatus.GetBlockSize()).Append("; ");
			}
			expected.Append("modification_time=").Append(fileStatus.GetModificationTime()).Append
				("; ");
			expected.Append("access_time=").Append(fileStatus.GetAccessTime()).Append("; ");
			expected.Append("owner=").Append(fileStatus.GetOwner()).Append("; ");
			expected.Append("group=").Append(fileStatus.GetGroup()).Append("; ");
			expected.Append("permission=").Append(fileStatus.GetPermission()).Append("; ");
			if (fileStatus.IsSymlink())
			{
				expected.Append("isSymlink=").Append(true).Append("; ");
				expected.Append("symlink=").Append(fileStatus.GetSymlink()).Append("}");
			}
			else
			{
				expected.Append("isSymlink=").Append(false).Append("}");
			}
			NUnit.Framework.Assert.AreEqual(expected.ToString(), fileStatus.ToString());
		}
	}
}
