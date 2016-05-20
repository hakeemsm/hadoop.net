using Sharpen;

namespace org.apache.hadoop.fs.viewfs
{
	/// <summary>
	/// Config variable prefixes for ViewFs -
	/// see
	/// <see cref="ViewFs"/>
	/// for examples.
	/// The mount table is specified in the config using these prefixes.
	/// See
	/// <see cref="ConfigUtil"/>
	/// for convenience lib.
	/// </summary>
	public abstract class Constants
	{
		/// <summary>Prefix for the config variable prefix for the ViewFs mount-table</summary>
		public const string CONFIG_VIEWFS_PREFIX = "fs.viewfs.mounttable";

		/// <summary>
		/// Prefix for the home dir for the mount table - if not specified
		/// then the hadoop default value (/user) is used.
		/// </summary>
		public const string CONFIG_VIEWFS_HOMEDIR = "homedir";

		/// <summary>Config variable name for the default mount table.</summary>
		public const string CONFIG_VIEWFS_DEFAULT_MOUNT_TABLE = "default";

		/// <summary>Config variable full prefix for the default mount table.</summary>
		public const string CONFIG_VIEWFS_PREFIX_DEFAULT_MOUNT_TABLE = CONFIG_VIEWFS_PREFIX
			 + "." + CONFIG_VIEWFS_DEFAULT_MOUNT_TABLE;

		/// <summary>Config variable for specifying a simple link</summary>
		public const string CONFIG_VIEWFS_LINK = "link";

		/// <summary>Config variable for specifying a merge link</summary>
		public const string CONFIG_VIEWFS_LINK_MERGE = "linkMerge";

		/// <summary>
		/// Config variable for specifying a merge of the root of the mount-table
		/// with the root of another file system.
		/// </summary>
		public const string CONFIG_VIEWFS_LINK_MERGE_SLASH = "linkMergeSlash";

		public const org.apache.hadoop.fs.permission.FsPermission PERMISSION_555 = new org.apache.hadoop.fs.permission.FsPermission
			((short)0x16d);
	}

	public static class ConstantsConstants
	{
	}
}
