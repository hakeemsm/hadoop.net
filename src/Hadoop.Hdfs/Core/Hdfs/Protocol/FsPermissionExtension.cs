using Org.Apache.Hadoop.FS.Permission;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Protocol
{
	/// <summary>
	/// HDFS permission subclass used to indicate an ACL is present and/or that the
	/// underlying file/dir is encrypted.
	/// </summary>
	/// <remarks>
	/// HDFS permission subclass used to indicate an ACL is present and/or that the
	/// underlying file/dir is encrypted. The ACL/encrypted bits are not visible
	/// directly to users of
	/// <see cref="Org.Apache.Hadoop.FS.Permission.FsPermission"/>
	/// serialization.  This is
	/// done for backwards compatibility in case any existing clients assume the
	/// value of FsPermission is in a particular range.
	/// </remarks>
	public class FsPermissionExtension : FsPermission
	{
		private const short AclBit = 1 << 12;

		private const short EncryptedBit = 1 << 13;

		private readonly bool aclBit;

		private readonly bool encryptedBit;

		/// <summary>Constructs a new FsPermissionExtension based on the given FsPermission.</summary>
		/// <param name="perm">FsPermission containing permission bits</param>
		public FsPermissionExtension(FsPermission perm, bool hasAcl, bool isEncrypted)
			: base(perm.ToShort())
		{
			aclBit = hasAcl;
			encryptedBit = isEncrypted;
		}

		/// <summary>Creates a new FsPermissionExtension by calling the base class constructor.
		/// 	</summary>
		/// <param name="perm">short containing permission bits</param>
		public FsPermissionExtension(short perm)
			: base(perm)
		{
			aclBit = (perm & AclBit) != 0;
			encryptedBit = (perm & EncryptedBit) != 0;
		}

		public override short ToExtendedShort()
		{
			return (short)(ToShort() | (aclBit ? AclBit : 0) | (encryptedBit ? EncryptedBit : 
				0));
		}

		public override bool GetAclBit()
		{
			return aclBit;
		}

		public override bool GetEncryptedBit()
		{
			return encryptedBit;
		}

		public override bool Equals(object o)
		{
			// This intentionally delegates to the base class.  This is only overridden
			// to suppress a FindBugs warning.
			return base.Equals(o);
		}

		public override int GetHashCode()
		{
			// This intentionally delegates to the base class.  This is only overridden
			// to suppress a FindBugs warning.
			return base.GetHashCode();
		}
	}
}
