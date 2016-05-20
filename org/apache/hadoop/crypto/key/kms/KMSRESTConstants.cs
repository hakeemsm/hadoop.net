using Sharpen;

namespace org.apache.hadoop.crypto.key.kms
{
	/// <summary>KMS REST and JSON constants and utility methods for the KMSServer.</summary>
	public class KMSRESTConstants
	{
		public const string SERVICE_VERSION = "/v1";

		public const string KEY_RESOURCE = "key";

		public const string KEYS_RESOURCE = "keys";

		public const string KEYS_METADATA_RESOURCE = KEYS_RESOURCE + "/metadata";

		public const string KEYS_NAMES_RESOURCE = KEYS_RESOURCE + "/names";

		public const string KEY_VERSION_RESOURCE = "keyversion";

		public const string METADATA_SUB_RESOURCE = "_metadata";

		public const string VERSIONS_SUB_RESOURCE = "_versions";

		public const string EEK_SUB_RESOURCE = "_eek";

		public const string CURRENT_VERSION_SUB_RESOURCE = "_currentversion";

		public const string KEY = "key";

		public const string EEK_OP = "eek_op";

		public const string EEK_GENERATE = "generate";

		public const string EEK_DECRYPT = "decrypt";

		public const string EEK_NUM_KEYS = "num_keys";

		public const string IV_FIELD = "iv";

		public const string NAME_FIELD = "name";

		public const string CIPHER_FIELD = "cipher";

		public const string LENGTH_FIELD = "length";

		public const string DESCRIPTION_FIELD = "description";

		public const string ATTRIBUTES_FIELD = "attributes";

		public const string CREATED_FIELD = "created";

		public const string VERSIONS_FIELD = "versions";

		public const string MATERIAL_FIELD = "material";

		public const string VERSION_NAME_FIELD = "versionName";

		public const string ENCRYPTED_KEY_VERSION_FIELD = "encryptedKeyVersion";

		public const string ERROR_EXCEPTION_JSON = "exception";

		public const string ERROR_MESSAGE_JSON = "message";
	}
}
