using Sharpen;

namespace org.apache.hadoop.fs.permission
{
	/// <summary>A class for file/directory permissions.</summary>
	public class FsPermission : org.apache.hadoop.io.Writable
	{
		private static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.permission.FsPermission
			)));

		private sealed class _WritableFactory_42 : org.apache.hadoop.io.WritableFactory
		{
			public _WritableFactory_42()
			{
			}

			public org.apache.hadoop.io.Writable newInstance()
			{
				return new org.apache.hadoop.fs.permission.FsPermission();
			}
		}

		internal static readonly org.apache.hadoop.io.WritableFactory FACTORY = new _WritableFactory_42
			();

		static FsPermission()
		{
			// register a ctor
			org.apache.hadoop.io.WritableFactories.setFactory(Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.fs.permission.FsPermission)), FACTORY);
			org.apache.hadoop.io.WritableFactories.setFactory(Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.fs.permission.FsPermission.ImmutableFsPermission)), FACTORY
				);
		}

		/// <summary>Maximum acceptable length of a permission string to parse</summary>
		public const int MAX_PERMISSION_LENGTH = 10;

		/// <summary>
		/// Create an immutable
		/// <see cref="FsPermission"/>
		/// object.
		/// </summary>
		public static org.apache.hadoop.fs.permission.FsPermission createImmutable(short 
			permission)
		{
			return new org.apache.hadoop.fs.permission.FsPermission.ImmutableFsPermission(permission
				);
		}

		private org.apache.hadoop.fs.permission.FsAction useraction = null;

		private org.apache.hadoop.fs.permission.FsAction groupaction = null;

		private org.apache.hadoop.fs.permission.FsAction otheraction = null;

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
		public FsPermission(org.apache.hadoop.fs.permission.FsAction u, org.apache.hadoop.fs.permission.FsAction
			 g, org.apache.hadoop.fs.permission.FsAction o)
			: this(u, g, o, false)
		{
		}

		public FsPermission(org.apache.hadoop.fs.permission.FsAction u, org.apache.hadoop.fs.permission.FsAction
			 g, org.apache.hadoop.fs.permission.FsAction o, bool sb)
		{
			//POSIX permission style
			set(u, g, o, sb);
		}

		/// <summary>Construct by the given mode.</summary>
		/// <param name="mode"/>
		/// <seealso cref="toShort()"/>
		public FsPermission(short mode)
		{
			fromShort(mode);
		}

		/// <summary>Copy constructor</summary>
		/// <param name="other">other permission</param>
		public FsPermission(org.apache.hadoop.fs.permission.FsPermission other)
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
			: this(new org.apache.hadoop.fs.permission.UmaskParser(mode).getUMask())
		{
		}

		/// <summary>
		/// Return user
		/// <see cref="FsAction"/>
		/// .
		/// </summary>
		public virtual org.apache.hadoop.fs.permission.FsAction getUserAction()
		{
			return useraction;
		}

		/// <summary>
		/// Return group
		/// <see cref="FsAction"/>
		/// .
		/// </summary>
		public virtual org.apache.hadoop.fs.permission.FsAction getGroupAction()
		{
			return groupaction;
		}

		/// <summary>
		/// Return other
		/// <see cref="FsAction"/>
		/// .
		/// </summary>
		public virtual org.apache.hadoop.fs.permission.FsAction getOtherAction()
		{
			return otheraction;
		}

		private void set(org.apache.hadoop.fs.permission.FsAction u, org.apache.hadoop.fs.permission.FsAction
			 g, org.apache.hadoop.fs.permission.FsAction o, bool sb)
		{
			useraction = u;
			groupaction = g;
			otheraction = o;
			stickyBit = sb;
		}

		public virtual void fromShort(short n)
		{
			org.apache.hadoop.fs.permission.FsAction[] v = FSACTION_VALUES;
			set(v[((short)(((ushort)n) >> 6)) & 7], v[((short)(((ushort)n) >> 3)) & 7], v[n &
				 7], ((((short)(((ushort)n) >> 9)) & 1) == 1));
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void write(java.io.DataOutput @out)
		{
			@out.writeShort(toShort());
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void readFields(java.io.DataInput @in)
		{
			fromShort(@in.readShort());
		}

		/// <summary>
		/// Create and initialize a
		/// <see cref="FsPermission"/>
		/// from
		/// <see cref="java.io.DataInput"/>
		/// .
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public static org.apache.hadoop.fs.permission.FsPermission read(java.io.DataInput
			 @in)
		{
			org.apache.hadoop.fs.permission.FsPermission p = new org.apache.hadoop.fs.permission.FsPermission
				();
			p.readFields(@in);
			return p;
		}

		/// <summary>Encode the object to a short.</summary>
		public virtual short toShort()
		{
			int s = (stickyBit ? 1 << 9 : 0) | ((int)(useraction) << 6) | ((int)(groupaction)
				 << 3) | (int)(otheraction);
			return (short)s;
		}

		/// <summary>Encodes the object to a short.</summary>
		/// <remarks>
		/// Encodes the object to a short.  Unlike
		/// <see cref="toShort()"/>
		/// , this method may
		/// return values outside the fixed range 00000 - 01777 if extended features
		/// are encoded into this permission, such as the ACL bit.
		/// </remarks>
		/// <returns>short extended short representation of this permission</returns>
		public virtual short toExtendedShort()
		{
			return toShort();
		}

		public override bool Equals(object obj)
		{
			if (obj is org.apache.hadoop.fs.permission.FsPermission)
			{
				org.apache.hadoop.fs.permission.FsPermission that = (org.apache.hadoop.fs.permission.FsPermission
					)obj;
				return this.useraction == that.useraction && this.groupaction == that.groupaction
					 && this.otheraction == that.otheraction && this.stickyBit == that.stickyBit;
			}
			return false;
		}

		public override int GetHashCode()
		{
			return toShort();
		}

		public override string ToString()
		{
			string str = useraction.SYMBOL + groupaction.SYMBOL + otheraction.SYMBOL;
			if (stickyBit)
			{
				java.lang.StringBuilder str2 = new java.lang.StringBuilder(str);
				str2.replace(str2.Length - 1, str2.Length, otheraction.implies(org.apache.hadoop.fs.permission.FsAction
					.EXECUTE) ? "t" : "T");
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
		public virtual org.apache.hadoop.fs.permission.FsPermission applyUMask(org.apache.hadoop.fs.permission.FsPermission
			 umask)
		{
			return new org.apache.hadoop.fs.permission.FsPermission(useraction.and(umask.useraction
				.not()), groupaction.and(umask.groupaction.not()), otheraction.and(umask.otheraction
				.not()));
		}

		/// <summary>
		/// umask property label deprecated key and code in getUMask method
		/// to accommodate it may be removed in version .23
		/// </summary>
		public const string DEPRECATED_UMASK_LABEL = "dfs.umask";

		public const string UMASK_LABEL = org.apache.hadoop.fs.CommonConfigurationKeys.FS_PERMISSIONS_UMASK_KEY;

		public const int DEFAULT_UMASK = org.apache.hadoop.fs.CommonConfigurationKeys.FS_PERMISSIONS_UMASK_DEFAULT;

		private static readonly org.apache.hadoop.fs.permission.FsAction[] FSACTION_VALUES
			 = org.apache.hadoop.fs.permission.FsAction.values();

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
		public static org.apache.hadoop.fs.permission.FsPermission getUMask(org.apache.hadoop.conf.Configuration
			 conf)
		{
			int umask = DEFAULT_UMASK;
			// To ensure backward compatibility first use the deprecated key.
			// If the deprecated key is not present then check for the new key
			if (conf != null)
			{
				string confUmask = conf.get(UMASK_LABEL);
				int oldUmask = conf.getInt(DEPRECATED_UMASK_LABEL, int.MinValue);
				try
				{
					if (confUmask != null)
					{
						umask = new org.apache.hadoop.fs.permission.UmaskParser(confUmask).getUMask();
					}
				}
				catch (System.ArgumentException iae)
				{
					// Provide more explanation for user-facing message
					string type = iae is java.lang.NumberFormatException ? "decimal" : "octal or symbolic";
					string error = "Unable to parse configuration " + UMASK_LABEL + " with value " + 
						confUmask + " as " + type + " umask.";
					LOG.warn(error);
					// If oldUmask is not set, then throw the exception
					if (oldUmask == int.MinValue)
					{
						throw new System.ArgumentException(error);
					}
				}
				if (oldUmask != int.MinValue)
				{
					// Property was set with old key
					if (umask != oldUmask)
					{
						LOG.warn(DEPRECATED_UMASK_LABEL + " configuration key is deprecated. " + "Convert to "
							 + UMASK_LABEL + ", using octal or symbolic umask " + "specifications.");
						// Old and new umask values do not match - Use old umask
						umask = oldUmask;
					}
				}
			}
			return new org.apache.hadoop.fs.permission.FsPermission((short)umask);
		}

		public virtual bool getStickyBit()
		{
			return stickyBit;
		}

		/// <summary>Returns true if there is also an ACL (access control list).</summary>
		/// <returns>boolean true if there is also an ACL (access control list).</returns>
		public virtual bool getAclBit()
		{
			// File system subclasses that support the ACL bit would override this.
			return false;
		}

		/// <summary>Returns true if the file is encrypted or directory is in an encryption zone
		/// 	</summary>
		public virtual bool getEncryptedBit()
		{
			return false;
		}

		/// <summary>Set the user file creation mask (umask)</summary>
		public static void setUMask(org.apache.hadoop.conf.Configuration conf, org.apache.hadoop.fs.permission.FsPermission
			 umask)
		{
			conf.set(UMASK_LABEL, string.format("%1$03o", umask.toShort()));
			conf.setInt(DEPRECATED_UMASK_LABEL, umask.toShort());
		}

		/// <summary>Get the default permission for directory and symlink.</summary>
		/// <remarks>
		/// Get the default permission for directory and symlink.
		/// In previous versions, this default permission was also used to
		/// create files, so files created end up with ugo+x permission.
		/// See HADOOP-9155 for detail.
		/// Two new methods are added to solve this, please use
		/// <see cref="getDirDefault()"/>
		/// for directory, and use
		/// <see cref="getFileDefault()"/>
		/// for file.
		/// This method is kept for compatibility.
		/// </remarks>
		public static org.apache.hadoop.fs.permission.FsPermission getDefault()
		{
			return new org.apache.hadoop.fs.permission.FsPermission((short)0x1ff);
		}

		/// <summary>Get the default permission for directory.</summary>
		public static org.apache.hadoop.fs.permission.FsPermission getDirDefault()
		{
			return new org.apache.hadoop.fs.permission.FsPermission((short)0x1ff);
		}

		/// <summary>Get the default permission for file.</summary>
		public static org.apache.hadoop.fs.permission.FsPermission getFileDefault()
		{
			return new org.apache.hadoop.fs.permission.FsPermission((short)0x1b6);
		}

		/// <summary>Get the default permission for cache pools.</summary>
		public static org.apache.hadoop.fs.permission.FsPermission getCachePoolDefault()
		{
			return new org.apache.hadoop.fs.permission.FsPermission((short)0x1ed);
		}

		/// <summary>Create a FsPermission from a Unix symbolic permission string</summary>
		/// <param name="unixSymbolicPermission">e.g. "-rw-rw-rw-"</param>
		public static org.apache.hadoop.fs.permission.FsPermission valueOf(string unixSymbolicPermission
			)
		{
			if (unixSymbolicPermission == null)
			{
				return null;
			}
			else
			{
				if (unixSymbolicPermission.Length != MAX_PERMISSION_LENGTH)
				{
					throw new System.ArgumentException(string.format("length != %d(unixSymbolicPermission=%s)"
						, MAX_PERMISSION_LENGTH, unixSymbolicPermission));
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
			return new org.apache.hadoop.fs.permission.FsPermission((short)n);
		}

		private class ImmutableFsPermission : org.apache.hadoop.fs.permission.FsPermission
		{
			public ImmutableFsPermission(short permission)
				: base(permission)
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public override void readFields(java.io.DataInput @in)
			{
				throw new System.NotSupportedException();
			}
		}
	}
}
