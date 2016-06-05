using System;
using System.IO;
using System.Text;
using Hadoop.Common.Core.Conf;
using Hadoop.Common.Core.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;


namespace Org.Apache.Hadoop.FS.Permission
{
	/// <summary>A class for file/directory permissions.</summary>
	public class FsPermission : IWritable
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.FS.Permission.FsPermission
			));

		private sealed class _WritableFactory_42 : WritableFactory
		{
			public _WritableFactory_42()
			{
			}

			public IWritable NewInstance()
			{
				return new Org.Apache.Hadoop.FS.Permission.FsPermission();
			}
		}

		internal static readonly WritableFactory Factory = new _WritableFactory_42();

		static FsPermission()
		{
			// register a ctor
			WritableFactories.SetFactory(typeof(Org.Apache.Hadoop.FS.Permission.FsPermission)
				, Factory);
			WritableFactories.SetFactory(typeof(FsPermission.ImmutableFsPermission), Factory);
		}

		/// <summary>Maximum acceptable length of a permission string to parse</summary>
		public const int MaxPermissionLength = 10;

		/// <summary>
		/// Create an immutable
		/// <see cref="FsPermission"/>
		/// object.
		/// </summary>
		public static Org.Apache.Hadoop.FS.Permission.FsPermission CreateImmutable(short 
			permission)
		{
			return new FsPermission.ImmutableFsPermission(permission);
		}

		private FsAction useraction = null;

		private FsAction groupaction = null;

		private FsAction otheraction = null;

		private bool stickyBit = false;

		private FsPermission()
		{
		}

		/// <summary>
		/// Construct by the given
		/// <see cref="FsAction"/>
		/// .
		/// </summary>
		/// <param name="u">user action</param>
		/// <param name="g">group action</param>
		/// <param name="o">other action</param>
		public FsPermission(FsAction u, FsAction g, FsAction o)
			: this(u, g, o, false)
		{
		}

		public FsPermission(FsAction u, FsAction g, FsAction o, bool sb)
		{
			//POSIX permission style
			Set(u, g, o, sb);
		}

		/// <summary>Construct by the given mode.</summary>
		/// <param name="mode"/>
		/// <seealso cref="ToShort()"/>
		public FsPermission(short mode)
		{
			FromShort(mode);
		}

		/// <summary>Copy constructor</summary>
		/// <param name="other">other permission</param>
		public FsPermission(Org.Apache.Hadoop.FS.Permission.FsPermission other)
		{
			this.useraction = other.useraction;
			this.groupaction = other.groupaction;
			this.otheraction = other.otheraction;
			this.stickyBit = other.stickyBit;
		}

		/// <summary>Construct by given mode, either in octal or symbolic format.</summary>
		/// <param name="mode">mode as a string, either in octal or symbolic format</param>
		/// <exception cref="System.ArgumentException">if <code>mode</code> is invalid</exception>
		public FsPermission(string mode)
			: this(new UmaskParser(mode).GetUMask())
		{
		}

		/// <summary>
		/// Return user
		/// <see cref="FsAction"/>
		/// .
		/// </summary>
		public virtual FsAction GetUserAction()
		{
			return useraction;
		}

		/// <summary>
		/// Return group
		/// <see cref="FsAction"/>
		/// .
		/// </summary>
		public virtual FsAction GetGroupAction()
		{
			return groupaction;
		}

		/// <summary>
		/// Return other
		/// <see cref="FsAction"/>
		/// .
		/// </summary>
		public virtual FsAction GetOtherAction()
		{
			return otheraction;
		}

		private void Set(FsAction u, FsAction g, FsAction o, bool sb)
		{
			useraction = u;
			groupaction = g;
			otheraction = o;
			stickyBit = sb;
		}

		public virtual void FromShort(short n)
		{
			FsAction[] v = FsactionValues;
			Set(v[((short)(((ushort)n) >> 6)) & 7], v[((short)(((ushort)n) >> 3)) & 7], v[n &
				 7], ((((short)(((ushort)n) >> 9)) & 1) == 1));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void Write(BinaryWriter @out)
		{
			@out.WriteShort(ToShort());
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void ReadFields(BinaryReader @in)
		{
			FromShort(@in.ReadShort());
		}

		/// <summary>
		/// Create and initialize a
		/// <see cref="FsPermission"/>
		/// from
		/// <see cref="System.IO.BinaryReader"/>
		/// .
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public static Org.Apache.Hadoop.FS.Permission.FsPermission Read(BinaryReader @in)
		{
			Org.Apache.Hadoop.FS.Permission.FsPermission p = new Org.Apache.Hadoop.FS.Permission.FsPermission
				();
			p.ReadFields(@in);
			return p;
		}

		/// <summary>Encode the object to a short.</summary>
		public virtual short ToShort()
		{
			int s = (stickyBit ? 1 << 9 : 0) | ((int)(useraction) << 6) | ((int)(groupaction)
				 << 3) | (int)(otheraction);
			return (short)s;
		}

		/// <summary>Encodes the object to a short.</summary>
		/// <remarks>
		/// Encodes the object to a short.  Unlike
		/// <see cref="ToShort()"/>
		/// , this method may
		/// return values outside the fixed range 00000 - 01777 if extended features
		/// are encoded into this permission, such as the ACL bit.
		/// </remarks>
		/// <returns>short extended short representation of this permission</returns>
		public virtual short ToExtendedShort()
		{
			return ToShort();
		}

		public override bool Equals(object obj)
		{
			if (obj is Org.Apache.Hadoop.FS.Permission.FsPermission)
			{
				Org.Apache.Hadoop.FS.Permission.FsPermission that = (Org.Apache.Hadoop.FS.Permission.FsPermission
					)obj;
				return this.useraction == that.useraction && this.groupaction == that.groupaction
					 && this.otheraction == that.otheraction && this.stickyBit == that.stickyBit;
			}
			return false;
		}

		public override int GetHashCode()
		{
			return ToShort();
		}

		public override string ToString()
		{
			string str = useraction.Symbol + groupaction.Symbol + otheraction.Symbol;
			if (stickyBit)
			{
				StringBuilder str2 = new StringBuilder(str);
				str2.Replace(str2.Length - 1, str2.Length, otheraction.Implies(FsAction.Execute) ? 
					"t" : "T");
				str = str2.ToString();
			}
			return str;
		}

		/// <summary>Apply a umask to this permission and return a new one.</summary>
		/// <remarks>
		/// Apply a umask to this permission and return a new one.
		/// The umask is used by create, mkdir, and other Hadoop filesystem operations.
		/// The mode argument for these operations is modified by removing the bits
		/// which are set in the umask.  Thus, the umask limits the permissions which
		/// newly created files and directories get.
		/// </remarks>
		/// <param name="umask">The umask to use</param>
		/// <returns>The effective permission</returns>
		public virtual Org.Apache.Hadoop.FS.Permission.FsPermission ApplyUMask(Org.Apache.Hadoop.FS.Permission.FsPermission
			 umask)
		{
			return new Org.Apache.Hadoop.FS.Permission.FsPermission(useraction.And(umask.useraction
				.Not()), groupaction.And(umask.groupaction.Not()), otheraction.And(umask.otheraction
				.Not()));
		}

		/// <summary>
		/// umask property label deprecated key and code in getUMask method
		/// to accommodate it may be removed in version .23
		/// </summary>
		public const string DeprecatedUmaskLabel = "dfs.umask";

		public const string UmaskLabel = CommonConfigurationKeys.FsPermissionsUmaskKey;

		public const int DefaultUmask = CommonConfigurationKeys.FsPermissionsUmaskDefault;

		private static readonly FsAction[] FsactionValues = FsAction.Values();

		/// <summary>
		/// Get the user file creation mask (umask)
		/// <c>UMASK_LABEL</c>
		/// config param has umask value that is either symbolic
		/// or octal.
		/// Symbolic umask is applied relative to file mode creation mask;
		/// the permission op characters '+' clears the corresponding bit in the mask,
		/// '-' sets bits in the mask.
		/// Octal umask, the specified bits are set in the file mode creation mask.
		/// <c>DEPRECATED_UMASK_LABEL</c>
		/// config param has umask value set to decimal.
		/// </summary>
		public static Org.Apache.Hadoop.FS.Permission.FsPermission GetUMask(Configuration
			 conf)
		{
			int umask = DefaultUmask;
			// To ensure backward compatibility first use the deprecated key.
			// If the deprecated key is not present then check for the new key
			if (conf != null)
			{
				string confUmask = conf.Get(UmaskLabel);
				int oldUmask = conf.GetInt(DeprecatedUmaskLabel, int.MinValue);
				try
				{
					if (confUmask != null)
					{
						umask = new UmaskParser(confUmask).GetUMask();
					}
				}
				catch (ArgumentException iae)
				{
					// Provide more explanation for user-facing message
					string type = iae is FormatException ? "decimal" : "octal or symbolic";
					string error = "Unable to parse configuration " + UmaskLabel + " with value " + confUmask
						 + " as " + type + " umask.";
					Log.Warn(error);
					// If oldUmask is not set, then throw the exception
					if (oldUmask == int.MinValue)
					{
						throw new ArgumentException(error);
					}
				}
				if (oldUmask != int.MinValue)
				{
					// Property was set with old key
					if (umask != oldUmask)
					{
						Log.Warn(DeprecatedUmaskLabel + " configuration key is deprecated. " + "Convert to "
							 + UmaskLabel + ", using octal or symbolic umask " + "specifications.");
						// Old and new umask values do not match - Use old umask
						umask = oldUmask;
					}
				}
			}
			return new Org.Apache.Hadoop.FS.Permission.FsPermission((short)umask);
		}

		public virtual bool GetStickyBit()
		{
			return stickyBit;
		}

		/// <summary>Returns true if there is also an ACL (access control list).</summary>
		/// <returns>boolean true if there is also an ACL (access control list).</returns>
		public virtual bool GetAclBit()
		{
			// File system subclasses that support the ACL bit would override this.
			return false;
		}

		/// <summary>Returns true if the file is encrypted or directory is in an encryption zone
		/// 	</summary>
		public virtual bool GetEncryptedBit()
		{
			return false;
		}

		/// <summary>Set the user file creation mask (umask)</summary>
		public static void SetUMask(Configuration conf, Org.Apache.Hadoop.FS.Permission.FsPermission
			 umask)
		{
			conf.Set(UmaskLabel, string.Format("%1$03o", umask.ToShort()));
			conf.SetInt(DeprecatedUmaskLabel, umask.ToShort());
		}

		/// <summary>Get the default permission for directory and symlink.</summary>
		/// <remarks>
		/// Get the default permission for directory and symlink.
		/// In previous versions, this default permission was also used to
		/// create files, so files created end up with ugo+x permission.
		/// See HADOOP-9155 for detail.
		/// Two new methods are added to solve this, please use
		/// <see cref="GetDirDefault()"/>
		/// for directory, and use
		/// <see cref="GetFileDefault()"/>
		/// for file.
		/// This method is kept for compatibility.
		/// </remarks>
		public static Org.Apache.Hadoop.FS.Permission.FsPermission GetDefault()
		{
			return new Org.Apache.Hadoop.FS.Permission.FsPermission((short)0x1ff);
		}

		/// <summary>Get the default permission for directory.</summary>
		public static Org.Apache.Hadoop.FS.Permission.FsPermission GetDirDefault()
		{
			return new Org.Apache.Hadoop.FS.Permission.FsPermission((short)0x1ff);
		}

		/// <summary>Get the default permission for file.</summary>
		public static Org.Apache.Hadoop.FS.Permission.FsPermission GetFileDefault()
		{
			return new Org.Apache.Hadoop.FS.Permission.FsPermission((short)0x1b6);
		}

		/// <summary>Get the default permission for cache pools.</summary>
		public static Org.Apache.Hadoop.FS.Permission.FsPermission GetCachePoolDefault()
		{
			return new Org.Apache.Hadoop.FS.Permission.FsPermission((short)0x1ed);
		}

		/// <summary>Create a FsPermission from a Unix symbolic permission string</summary>
		/// <param name="unixSymbolicPermission">e.g. "-rw-rw-rw-"</param>
		public static Org.Apache.Hadoop.FS.Permission.FsPermission ValueOf(string unixSymbolicPermission
			)
		{
			if (unixSymbolicPermission == null)
			{
				return null;
			}
			else
			{
				if (unixSymbolicPermission.Length != MaxPermissionLength)
				{
					throw new ArgumentException(string.Format("length != %d(unixSymbolicPermission=%s)"
						, MaxPermissionLength, unixSymbolicPermission));
				}
			}
			int n = 0;
			for (int i = 1; i < unixSymbolicPermission.Length; i++)
			{
				n = n << 1;
				char c = unixSymbolicPermission[i];
				n += (c == '-' || c == 'T' || c == 'S') ? 0 : 1;
			}
			// Add sticky bit value if set
			if (unixSymbolicPermission[9] == 't' || unixSymbolicPermission[9] == 'T')
			{
				n += 0x200;
			}
			return new Org.Apache.Hadoop.FS.Permission.FsPermission((short)n);
		}

		private class ImmutableFsPermission : FsPermission
		{
			public ImmutableFsPermission(short permission)
				: base(permission)
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public override void ReadFields(BinaryReader @in)
			{
				throw new NotSupportedException();
			}
		}
	}
}
