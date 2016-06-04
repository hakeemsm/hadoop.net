using System;
using System.IO;
using Hadoop.Common.Core.IO;
using Org.Apache.Hadoop.IO;
using Sharpen;

namespace Org.Apache.Hadoop.FS.Permission
{
	/// <summary>Store permission related information.</summary>
	public class PermissionStatus : IWritable
	{
		private sealed class _WritableFactory_34 : WritableFactory
		{
			public _WritableFactory_34()
			{
			}

			public IWritable NewInstance()
			{
				return new Org.Apache.Hadoop.FS.Permission.PermissionStatus();
			}
		}

		internal static readonly WritableFactory Factory = new _WritableFactory_34();

		static PermissionStatus()
		{
			// register a ctor
			WritableFactories.SetFactory(typeof(Org.Apache.Hadoop.FS.Permission.PermissionStatus
				), Factory);
		}

		/// <summary>
		/// Create an immutable
		/// <see cref="PermissionStatus"/>
		/// object.
		/// </summary>
		public static Org.Apache.Hadoop.FS.Permission.PermissionStatus CreateImmutable(string
			 user, string group, FsPermission permission)
		{
			return new _PermissionStatus_45(user, group, permission);
		}

		private sealed class _PermissionStatus_45 : Org.Apache.Hadoop.FS.Permission.PermissionStatus
		{
			public _PermissionStatus_45(string baseArg1, string baseArg2, FsPermission baseArg3
				)
				: base(baseArg1, baseArg2, baseArg3)
			{
			}

			public override Org.Apache.Hadoop.FS.Permission.PermissionStatus ApplyUMask(FsPermission
				 umask)
			{
				throw new NotSupportedException();
			}

			/// <exception cref="System.IO.IOException"/>
			public override void ReadFields(BinaryReader @in)
			{
				throw new NotSupportedException();
			}
		}

		private string username;

		private string groupname;

		private FsPermission permission;

		private PermissionStatus()
		{
		}

		/// <summary>Constructor</summary>
		public PermissionStatus(string user, string group, FsPermission permission)
		{
			username = user;
			groupname = group;
			this.permission = permission;
		}

		/// <summary>Return user name</summary>
		public virtual string GetUserName()
		{
			return username;
		}

		/// <summary>Return group name</summary>
		public virtual string GetGroupName()
		{
			return groupname;
		}

		/// <summary>Return permission</summary>
		public virtual FsPermission GetPermission()
		{
			return permission;
		}

		/// <summary>Apply umask.</summary>
		/// <seealso cref="FsPermission.ApplyUMask(FsPermission)"/>
		public virtual Org.Apache.Hadoop.FS.Permission.PermissionStatus ApplyUMask(FsPermission
			 umask)
		{
			permission = permission.ApplyUMask(umask);
			return this;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void ReadFields(BinaryReader @in)
		{
			username = Text.ReadString(@in, Text.DefaultMaxLen);
			groupname = Text.ReadString(@in, Text.DefaultMaxLen);
			permission = FsPermission.Read(@in);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void Write(DataOutput @out)
		{
			Write(@out, username, groupname, permission);
		}

		/// <summary>
		/// Create and initialize a
		/// <see cref="PermissionStatus"/>
		/// from
		/// <see cref="System.IO.BinaryReader"/>
		/// .
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public static Org.Apache.Hadoop.FS.Permission.PermissionStatus Read(BinaryReader @in
			)
		{
			Org.Apache.Hadoop.FS.Permission.PermissionStatus p = new Org.Apache.Hadoop.FS.Permission.PermissionStatus
				();
			p.ReadFields(@in);
			return p;
		}

		/// <summary>
		/// Serialize a
		/// <see cref="PermissionStatus"/>
		/// from its base components.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public static void Write(DataOutput @out, string username, string groupname, FsPermission
			 permission)
		{
			Text.WriteString(@out, username, Text.DefaultMaxLen);
			Text.WriteString(@out, groupname, Text.DefaultMaxLen);
			permission.Write(@out);
		}

		public override string ToString()
		{
			return username + ":" + groupname + ":" + permission;
		}
	}
}
