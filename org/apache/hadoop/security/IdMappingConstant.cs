using Sharpen;

namespace org.apache.hadoop.security
{
	/// <summary>Some constants for IdMapping</summary>
	public class IdMappingConstant
	{
		/// <summary>Do user/group update every 15 minutes by default, minimum 1 minute</summary>
		public const string USERGROUPID_UPDATE_MILLIS_KEY = "usergroupid.update.millis";

		public const long USERGROUPID_UPDATE_MILLIS_DEFAULT = 15 * 60 * 1000;

		public const long USERGROUPID_UPDATE_MILLIS_MIN = 1 * 60 * 1000;

		public const string UNKNOWN_USER = "nobody";

		public const string UNKNOWN_GROUP = "nobody";

		public const string STATIC_ID_MAPPING_FILE_KEY = "static.id.mapping.file";

		public const string STATIC_ID_MAPPING_FILE_DEFAULT = "/etc/nfs.map";
		// ms
		// ms
		// Used for finding the configured static mapping file.
	}
}
