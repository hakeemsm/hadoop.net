using Sharpen;

namespace org.apache.hadoop.fs
{
	public class TestFileStatus
	{
		private static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.TestFileStatus
			)));

		/// <summary>
		/// Values for creating
		/// <see cref="FileStatus"/>
		/// in some tests
		/// </summary>
		internal const int LENGTH = 1;

		internal const int REPLICATION = 2;

		internal const long BLKSIZE = 3;

		internal const long MTIME = 4;

		internal const long ATIME = 5;

		internal const string OWNER = "owner";

		internal const string GROUP = "group";

		internal static readonly org.apache.hadoop.fs.permission.FsPermission PERMISSION = 
			org.apache.hadoop.fs.permission.FsPermission.valueOf("-rw-rw-rw-");

		internal static readonly org.apache.hadoop.fs.Path PATH = new org.apache.hadoop.fs.Path
			("path");

		/// <summary>Check that the write and readField methods work correctly.</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testFileStatusWritable()
		{
			org.apache.hadoop.fs.FileStatus[] tests = new org.apache.hadoop.fs.FileStatus[] { 
				new org.apache.hadoop.fs.FileStatus(1, false, 5, 3, 4, 5, null, string.Empty, string.Empty
				, new org.apache.hadoop.fs.Path("/a/b")), new org.apache.hadoop.fs.FileStatus(0, 
				false, 1, 2, 3, new org.apache.hadoop.fs.Path("/")), new org.apache.hadoop.fs.FileStatus
				(1, false, 5, 3, 4, 5, null, string.Empty, string.Empty, new org.apache.hadoop.fs.Path
				("/a/b")) };
			LOG.info("Writing FileStatuses to a ByteArrayOutputStream");
			// Writing input list to ByteArrayOutputStream
			java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
			java.io.DataOutput @out = new java.io.DataOutputStream(baos);
			foreach (org.apache.hadoop.fs.FileStatus fs in tests)
			{
				fs.write(@out);
			}
			LOG.info("Creating ByteArrayInputStream object");
			java.io.DataInput @in = new java.io.DataInputStream(new java.io.ByteArrayInputStream
				(baos.toByteArray()));
			LOG.info("Testing if read objects are equal to written ones");
			org.apache.hadoop.fs.FileStatus dest = new org.apache.hadoop.fs.FileStatus();
			int iterator = 0;
			foreach (org.apache.hadoop.fs.FileStatus fs_1 in tests)
			{
				dest.readFields(@in);
				NUnit.Framework.Assert.AreEqual("Different FileStatuses in iteration " + iterator
					, dest, fs_1);
				iterator++;
			}
		}

		/// <summary>Check that the full parameter constructor works correctly.</summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void constructorFull()
		{
			bool isdir = false;
			org.apache.hadoop.fs.Path symlink = new org.apache.hadoop.fs.Path("symlink");
			org.apache.hadoop.fs.FileStatus fileStatus = new org.apache.hadoop.fs.FileStatus(
				LENGTH, isdir, REPLICATION, BLKSIZE, MTIME, ATIME, PERMISSION, OWNER, GROUP, symlink
				, PATH);
			validateAccessors(fileStatus, LENGTH, isdir, REPLICATION, BLKSIZE, MTIME, ATIME, 
				PERMISSION, OWNER, GROUP, symlink, PATH);
		}

		/// <summary>Check that the non-symlink constructor works correctly.</summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void constructorNoSymlink()
		{
			bool isdir = true;
			org.apache.hadoop.fs.FileStatus fileStatus = new org.apache.hadoop.fs.FileStatus(
				LENGTH, isdir, REPLICATION, BLKSIZE, MTIME, ATIME, PERMISSION, OWNER, GROUP, PATH
				);
			validateAccessors(fileStatus, LENGTH, isdir, REPLICATION, BLKSIZE, MTIME, ATIME, 
				PERMISSION, OWNER, GROUP, null, PATH);
		}

		/// <summary>
		/// Check that the constructor without owner, group and permissions works
		/// correctly.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void constructorNoOwner()
		{
			bool isdir = true;
			org.apache.hadoop.fs.FileStatus fileStatus = new org.apache.hadoop.fs.FileStatus(
				LENGTH, isdir, REPLICATION, BLKSIZE, MTIME, PATH);
			validateAccessors(fileStatus, LENGTH, isdir, REPLICATION, BLKSIZE, MTIME, 0, org.apache.hadoop.fs.permission.FsPermission
				.getDirDefault(), string.Empty, string.Empty, null, PATH);
		}

		/// <summary>Check that the no parameter constructor works correctly.</summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void constructorBlank()
		{
			org.apache.hadoop.fs.FileStatus fileStatus = new org.apache.hadoop.fs.FileStatus(
				);
			validateAccessors(fileStatus, 0, false, 0, 0, 0, 0, org.apache.hadoop.fs.permission.FsPermission
				.getFileDefault(), string.Empty, string.Empty, null, null);
		}

		/// <summary>Check that FileStatus are equal if their paths are equal.</summary>
		[NUnit.Framework.Test]
		public virtual void testEquals()
		{
			org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path("path");
			org.apache.hadoop.fs.FileStatus fileStatus1 = new org.apache.hadoop.fs.FileStatus
				(1, true, 1, 1, 1, 1, org.apache.hadoop.fs.permission.FsPermission.valueOf("-rw-rw-rw-"
				), "one", "one", null, path);
			org.apache.hadoop.fs.FileStatus fileStatus2 = new org.apache.hadoop.fs.FileStatus
				(2, true, 2, 2, 2, 2, org.apache.hadoop.fs.permission.FsPermission.valueOf("---x--x--x"
				), "two", "two", null, path);
			NUnit.Framework.Assert.AreEqual(fileStatus1, fileStatus2);
		}

		/// <summary>Check that FileStatus are not equal if their paths are not equal.</summary>
		[NUnit.Framework.Test]
		public virtual void testNotEquals()
		{
			org.apache.hadoop.fs.Path path1 = new org.apache.hadoop.fs.Path("path1");
			org.apache.hadoop.fs.Path path2 = new org.apache.hadoop.fs.Path("path2");
			org.apache.hadoop.fs.FileStatus fileStatus1 = new org.apache.hadoop.fs.FileStatus
				(1, true, 1, 1, 1, 1, org.apache.hadoop.fs.permission.FsPermission.valueOf("-rw-rw-rw-"
				), "one", "one", null, path1);
			org.apache.hadoop.fs.FileStatus fileStatus2 = new org.apache.hadoop.fs.FileStatus
				(1, true, 1, 1, 1, 1, org.apache.hadoop.fs.permission.FsPermission.valueOf("-rw-rw-rw-"
				), "one", "one", null, path2);
			NUnit.Framework.Assert.IsFalse(fileStatus1.Equals(fileStatus2));
			NUnit.Framework.Assert.IsFalse(fileStatus2.Equals(fileStatus1));
		}

		/// <summary>Check that toString produces the expected output for a file.</summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void toStringFile()
		{
			bool isdir = false;
			org.apache.hadoop.fs.FileStatus fileStatus = new org.apache.hadoop.fs.FileStatus(
				LENGTH, isdir, REPLICATION, BLKSIZE, MTIME, ATIME, PERMISSION, OWNER, GROUP, null
				, PATH);
			validateToString(fileStatus);
		}

		/// <summary>Check that toString produces the expected output for a directory.</summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void toStringDir()
		{
			org.apache.hadoop.fs.FileStatus fileStatus = new org.apache.hadoop.fs.FileStatus(
				LENGTH, true, REPLICATION, BLKSIZE, MTIME, ATIME, PERMISSION, OWNER, GROUP, null
				, PATH);
			validateToString(fileStatus);
		}

		/// <summary>Check that toString produces the expected output for a symlink.</summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void toStringSymlink()
		{
			bool isdir = false;
			org.apache.hadoop.fs.Path symlink = new org.apache.hadoop.fs.Path("symlink");
			org.apache.hadoop.fs.FileStatus fileStatus = new org.apache.hadoop.fs.FileStatus(
				LENGTH, isdir, REPLICATION, BLKSIZE, MTIME, ATIME, PERMISSION, OWNER, GROUP, symlink
				, PATH);
			validateToString(fileStatus);
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
		private void validateAccessors(org.apache.hadoop.fs.FileStatus fileStatus, long length
			, bool isdir, int replication, long blocksize, long mtime, long atime, org.apache.hadoop.fs.permission.FsPermission
			 permission, string owner, string group, org.apache.hadoop.fs.Path symlink, org.apache.hadoop.fs.Path
			 path)
		{
			NUnit.Framework.Assert.AreEqual(length, fileStatus.getLen());
			NUnit.Framework.Assert.AreEqual(isdir, fileStatus.isDirectory());
			NUnit.Framework.Assert.AreEqual(replication, fileStatus.getReplication());
			NUnit.Framework.Assert.AreEqual(blocksize, fileStatus.getBlockSize());
			NUnit.Framework.Assert.AreEqual(mtime, fileStatus.getModificationTime());
			NUnit.Framework.Assert.AreEqual(atime, fileStatus.getAccessTime());
			NUnit.Framework.Assert.AreEqual(permission, fileStatus.getPermission());
			NUnit.Framework.Assert.AreEqual(owner, fileStatus.getOwner());
			NUnit.Framework.Assert.AreEqual(group, fileStatus.getGroup());
			if (symlink == null)
			{
				NUnit.Framework.Assert.IsFalse(fileStatus.isSymlink());
			}
			else
			{
				NUnit.Framework.Assert.IsTrue(fileStatus.isSymlink());
				NUnit.Framework.Assert.AreEqual(symlink, fileStatus.getSymlink());
			}
			NUnit.Framework.Assert.AreEqual(path, fileStatus.getPath());
		}

		/// <summary>Validates the toString method for FileStatus.</summary>
		/// <param name="fileStatus">FileStatus to be validated</param>
		/// <exception cref="System.IO.IOException"/>
		private void validateToString(org.apache.hadoop.fs.FileStatus fileStatus)
		{
			java.lang.StringBuilder expected = new java.lang.StringBuilder();
			expected.Append("FileStatus{");
			expected.Append("path=").Append(fileStatus.getPath()).Append("; ");
			expected.Append("isDirectory=").Append(fileStatus.isDirectory()).Append("; ");
			if (!fileStatus.isDirectory())
			{
				expected.Append("length=").Append(fileStatus.getLen()).Append("; ");
				expected.Append("replication=").Append(fileStatus.getReplication()).Append("; ");
				expected.Append("blocksize=").Append(fileStatus.getBlockSize()).Append("; ");
			}
			expected.Append("modification_time=").Append(fileStatus.getModificationTime()).Append
				("; ");
			expected.Append("access_time=").Append(fileStatus.getAccessTime()).Append("; ");
			expected.Append("owner=").Append(fileStatus.getOwner()).Append("; ");
			expected.Append("group=").Append(fileStatus.getGroup()).Append("; ");
			expected.Append("permission=").Append(fileStatus.getPermission()).Append("; ");
			if (fileStatus.isSymlink())
			{
				expected.Append("isSymlink=").Append(true).Append("; ");
				expected.Append("symlink=").Append(fileStatus.getSymlink()).Append("}");
			}
			else
			{
				expected.Append("isSymlink=").Append(false).Append("}");
			}
			NUnit.Framework.Assert.AreEqual(expected.ToString(), fileStatus.ToString());
		}
	}
}
