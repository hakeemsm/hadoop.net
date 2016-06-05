

namespace Org.Apache.Hadoop.Crypto.Key.Kms
{
	/// <summary>KMS REST and JSON constants and utility methods for the KMSServer.</summary>
	public class KMSRESTConstants
	{
		public const string ServiceVersion = "/v1";

		public const string KeyResource = "key";

		public const string KeysResource = "keys";

		public const string KeysMetadataResource = KeysResource + "/metadata";

		public const string KeysNamesResource = KeysResource + "/names";

		public const string KeyVersionResource = "keyversion";

		public const string MetadataSubResource = "_metadata";

		public const string VersionsSubResource = "_versions";

		public const string EekSubResource = "_eek";

		public const string CurrentVersionSubResource = "_currentversion";

		public const string Key = "key";

		public const string EekOp = "eek_op";

		public const string EekGenerate = "generate";

		public const string EekDecrypt = "decrypt";

		public const string EekNumKeys = "num_keys";

		public const string IvField = "iv";

		public const string NameField = "name";

		public const string CipherField = "cipher";

		public const string LengthField = "length";

		public const string DescriptionField = "description";

		public const string AttributesField = "attributes";

		public const string CreatedField = "created";

		public const string VersionsField = "versions";

		public const string MaterialField = "material";

		public const string VersionNameField = "versionName";

		public const string EncryptedKeyVersionField = "encryptedKeyVersion";

		public const string ErrorExceptionJson = "exception";

		public const string ErrorMessageJson = "message";
	}
}
