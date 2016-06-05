

namespace Org.Apache.Hadoop.Security
{
	/// <summary>Some constants for IdMapping</summary>
	public class IdMappingConstant
	{
		/// <summary>Do user/group update every 15 minutes by default, minimum 1 minute</summary>
		public const string UsergroupidUpdateMillisKey = "usergroupid.update.millis";

		public const long UsergroupidUpdateMillisDefault = 15 * 60 * 1000;

		public const long UsergroupidUpdateMillisMin = 1 * 60 * 1000;

		public const string UnknownUser = "nobody";

		public const string UnknownGroup = "nobody";

		public const string StaticIdMappingFileKey = "static.id.mapping.file";

		public const string StaticIdMappingFileDefault = "/etc/nfs.map";
		// ms
		// ms
		// Used for finding the configured static mapping file.
	}
}
