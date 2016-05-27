using Org.Apache.Hadoop.FS.Permission;
using Sharpen;

namespace Org.Apache.Hadoop.FS.Viewfs
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
		public const string ConfigViewfsPrefix = "fs.viewfs.mounttable";

		/// <summary>
		/// Prefix for the home dir for the mount table - if not specified
		/// then the hadoop default value (/user) is used.
		/// </summary>
		public const string ConfigViewfsHomedir = "homedir";

		/// <summary>Config variable name for the default mount table.</summary>
		public const string ConfigViewfsDefaultMountTable = "default";

		/// <summary>Config variable full prefix for the default mount table.</summary>
		public const string ConfigViewfsPrefixDefaultMountTable = ConfigViewfsPrefix + "."
			 + ConfigViewfsDefaultMountTable;

		/// <summary>Config variable for specifying a simple link</summary>
		public const string ConfigViewfsLink = "link";

		/// <summary>Config variable for specifying a merge link</summary>
		public const string ConfigViewfsLinkMerge = "linkMerge";

		/// <summary>
		/// Config variable for specifying a merge of the root of the mount-table
		/// with the root of another file system.
		/// </summary>
		public const string ConfigViewfsLinkMergeSlash = "linkMergeSlash";

		public const FsPermission Permission555 = new FsPermission((short)0x16d);
	}

	public static class ConstantsConstants
	{
	}
}
