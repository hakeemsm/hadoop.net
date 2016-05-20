using Sharpen;

namespace org.apache.hadoop.fs.permission
{
	/// <summary>Store permission related information.</summary>
	public class PermissionStatus : org.apache.hadoop.io.Writable
	{
		private sealed class _WritableFactory_34 : org.apache.hadoop.io.WritableFactory
		{
			public _WritableFactory_34()
			{
			}

			public org.apache.hadoop.io.Writable newInstance()
			{
				return new org.apache.hadoop.fs.permission.PermissionStatus();
			}
		}

		internal static readonly org.apache.hadoop.io.WritableFactory FACTORY = new _WritableFactory_34
			();

		static PermissionStatus()
		{
			// register a ctor
			org.apache.hadoop.io.WritableFactories.setFactory(Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.fs.permission.PermissionStatus)), FACTORY);
		}

		/// <summary>
		/// Create an immutable
		/// <see cref="PermissionStatus"/>
		/// object.
		/// </summary>
		public static org.apache.hadoop.fs.permission.PermissionStatus createImmutable(string
			 user, string group, org.apache.hadoop.fs.permission.FsPermission permission)
		{
			return new _PermissionStatus_45(user, group, permission);
		}

		private sealed class _PermissionStatus_45 : org.apache.hadoop.fs.permission.PermissionStatus
		{
			public _PermissionStatus_45(string baseArg1, string baseArg2, org.apache.hadoop.fs.permission.FsPermission
				 baseArg3)
				: base(baseArg1, baseArg2, baseArg3)
			{
			}

			public override org.apache.hadoop.fs.permission.PermissionStatus applyUMask(org.apache.hadoop.fs.permission.FsPermission
				 umask)
			{
				throw new System.NotSupportedException();
			}

			/// <exception cref="System.IO.IOException"/>
			public override void readFields(java.io.DataInput @in)
			{
				throw new System.NotSupportedException();
			}
		}

		private string username;

		private string groupname;

		private org.apache.hadoop.fs.permission.FsPermission permission;

		private PermissionStatus()
		{
		}

		/// <summary>Constructor</summary>
		public PermissionStatus(string user, string group, org.apache.hadoop.fs.permission.FsPermission
			 permission)
		{
			username = user;
			groupname = group;
			this.permission = permission;
		}

		/// <summary>Return user name</summary>
		public virtual string getUserName()
		{
			return username;
		}

		/// <summary>Return group name</summary>
		public virtual string getGroupName()
		{
			return groupname;
		}

		/// <summary>Return permission</summary>
		public virtual org.apache.hadoop.fs.permission.FsPermission getPermission()
		{
			return permission;
		}

		/// <summary>Apply umask.</summary>
		/// <seealso cref="FsPermission.applyUMask(FsPermission)"/>
		public virtual org.apache.hadoop.fs.permission.PermissionStatus applyUMask(org.apache.hadoop.fs.permission.FsPermission
			 umask)
		{
			permission = permission.applyUMask(umask);
			return this;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void readFields(java.io.DataInput @in)
		{
			username = org.apache.hadoop.io.Text.readString(@in, org.apache.hadoop.io.Text.DEFAULT_MAX_LEN
				);
			groupname = org.apache.hadoop.io.Text.readString(@in, org.apache.hadoop.io.Text.DEFAULT_MAX_LEN
				);
			permission = org.apache.hadoop.fs.permission.FsPermission.read(@in);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void write(java.io.DataOutput @out)
		{
			write(@out, username, groupname, permission);
		}

		/// <summary>
		/// Create and initialize a
		/// <see cref="PermissionStatus"/>
		/// from
		/// <see cref="java.io.DataInput"/>
		/// .
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public static org.apache.hadoop.fs.permission.PermissionStatus read(java.io.DataInput
			 @in)
		{
			org.apache.hadoop.fs.permission.PermissionStatus p = new org.apache.hadoop.fs.permission.PermissionStatus
				();
			p.readFields(@in);
			return p;
		}

		/// <summary>
		/// Serialize a
		/// <see cref="PermissionStatus"/>
		/// from its base components.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public static void write(java.io.DataOutput @out, string username, string groupname
			, org.apache.hadoop.fs.permission.FsPermission permission)
		{
			org.apache.hadoop.io.Text.writeString(@out, username, org.apache.hadoop.io.Text.DEFAULT_MAX_LEN
				);
			org.apache.hadoop.io.Text.writeString(@out, groupname, org.apache.hadoop.io.Text.
				DEFAULT_MAX_LEN);
			permission.write(@out);
		}

		public override string ToString()
		{
			return username + ":" + groupname + ":" + permission;
		}
	}
}
