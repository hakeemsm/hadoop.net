using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Security;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>Helper methods useful for writing ACL tests.</summary>
	public sealed class AclTestHelpers
	{
		/// <summary>Create a new AclEntry with scope, type and permission (no name).</summary>
		/// <param name="scope">AclEntryScope scope of the ACL entry</param>
		/// <param name="type">AclEntryType ACL entry type</param>
		/// <param name="permission">FsAction set of permissions in the ACL entry</param>
		/// <returns>AclEntry new AclEntry</returns>
		public static Org.Apache.Hadoop.FS.Permission.AclEntry AclEntry(AclEntryScope scope
			, AclEntryType type, FsAction permission)
		{
			return new AclEntry.Builder().SetScope(scope).SetType(type).SetPermission(permission
				).Build();
		}

		/// <summary>Create a new AclEntry with scope, type, name and permission.</summary>
		/// <param name="scope">AclEntryScope scope of the ACL entry</param>
		/// <param name="type">AclEntryType ACL entry type</param>
		/// <param name="name">String optional ACL entry name</param>
		/// <param name="permission">FsAction set of permissions in the ACL entry</param>
		/// <returns>AclEntry new AclEntry</returns>
		public static Org.Apache.Hadoop.FS.Permission.AclEntry AclEntry(AclEntryScope scope
			, AclEntryType type, string name, FsAction permission)
		{
			return new AclEntry.Builder().SetScope(scope).SetType(type).SetName(name).SetPermission
				(permission).Build();
		}

		/// <summary>Create a new AclEntry with scope, type and name (no permission).</summary>
		/// <param name="scope">AclEntryScope scope of the ACL entry</param>
		/// <param name="type">AclEntryType ACL entry type</param>
		/// <param name="name">String optional ACL entry name</param>
		/// <returns>AclEntry new AclEntry</returns>
		public static Org.Apache.Hadoop.FS.Permission.AclEntry AclEntry(AclEntryScope scope
			, AclEntryType type, string name)
		{
			return new AclEntry.Builder().SetScope(scope).SetType(type).SetName(name).Build();
		}

		/// <summary>Create a new AclEntry with scope and type (no name or permission).</summary>
		/// <param name="scope">AclEntryScope scope of the ACL entry</param>
		/// <param name="type">AclEntryType ACL entry type</param>
		/// <returns>AclEntry new AclEntry</returns>
		public static Org.Apache.Hadoop.FS.Permission.AclEntry AclEntry(AclEntryScope scope
			, AclEntryType type)
		{
			return new AclEntry.Builder().SetScope(scope).SetType(type).Build();
		}

		/// <summary>Asserts that permission is denied to the given fs/user for the given file.
		/// 	</summary>
		/// <param name="fs">FileSystem to check</param>
		/// <param name="user">UserGroupInformation owner of fs</param>
		/// <param name="pathToCheck">Path file to check</param>
		/// <exception cref="System.Exception">if there is an unexpected error</exception>
		public static void AssertFilePermissionDenied(FileSystem fs, UserGroupInformation
			 user, Path pathToCheck)
		{
			try
			{
				DFSTestUtil.ReadFileBuffer(fs, pathToCheck);
				NUnit.Framework.Assert.Fail("expected AccessControlException for user " + user + 
					", path = " + pathToCheck);
			}
			catch (AccessControlException)
			{
			}
		}

		// expected
		/// <summary>Asserts that permission is granted to the given fs/user for the given file.
		/// 	</summary>
		/// <param name="fs">FileSystem to check</param>
		/// <param name="user">UserGroupInformation owner of fs</param>
		/// <param name="pathToCheck">Path file to check</param>
		/// <exception cref="System.Exception">if there is an unexpected error</exception>
		public static void AssertFilePermissionGranted(FileSystem fs, UserGroupInformation
			 user, Path pathToCheck)
		{
			try
			{
				DFSTestUtil.ReadFileBuffer(fs, pathToCheck);
			}
			catch (AccessControlException)
			{
				NUnit.Framework.Assert.Fail("expected permission granted for user " + user + ", path = "
					 + pathToCheck);
			}
		}

		/// <summary>Asserts the value of the FsPermission bits on the inode of a specific path.
		/// 	</summary>
		/// <param name="fs">FileSystem to use for check</param>
		/// <param name="pathToCheck">Path inode to check</param>
		/// <param name="perm">short expected permission bits</param>
		/// <exception cref="System.IO.IOException">thrown if there is an I/O error</exception>
		public static void AssertPermission(FileSystem fs, Path pathToCheck, short perm)
		{
			short filteredPerm = (short)(perm & 0x3ff);
			FsPermission fsPermission = fs.GetFileStatus(pathToCheck).GetPermission();
			NUnit.Framework.Assert.AreEqual(filteredPerm, fsPermission.ToShort());
			NUnit.Framework.Assert.AreEqual(((perm & (1 << 12)) != 0), fsPermission.GetAclBit
				());
		}
	}
}
