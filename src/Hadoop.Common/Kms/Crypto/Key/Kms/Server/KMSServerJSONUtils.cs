using System.Collections;
using System.Collections.Generic;
using Org.Apache.Commons.Codec.Binary;
using Org.Apache.Hadoop.Crypto.Key;
using Org.Apache.Hadoop.Crypto.Key.Kms;


namespace Org.Apache.Hadoop.Crypto.Key.Kms.Server
{
	/// <summary>JSON utility methods for the KMS.</summary>
	public class KMSServerJSONUtils
	{
		public static IDictionary ToJSON(KeyProvider.KeyVersion keyVersion)
		{
			IDictionary json = new LinkedHashMap();
			if (keyVersion != null)
			{
				json[KMSRESTConstants.NameField] = keyVersion.GetName();
				json[KMSRESTConstants.VersionNameField] = keyVersion.GetVersionName();
				json[KMSRESTConstants.MaterialField] = Base64.EncodeBase64URLSafeString(keyVersion
					.GetMaterial());
			}
			return json;
		}

		public static IList ToJSON(IList<KeyProvider.KeyVersion> keyVersions)
		{
			IList json = new ArrayList();
			if (keyVersions != null)
			{
				foreach (KeyProvider.KeyVersion version in keyVersions)
				{
					json.AddItem(ToJSON(version));
				}
			}
			return json;
		}

		public static IDictionary ToJSON(KeyProviderCryptoExtension.EncryptedKeyVersion encryptedKeyVersion
			)
		{
			IDictionary json = new LinkedHashMap();
			if (encryptedKeyVersion != null)
			{
				json[KMSRESTConstants.VersionNameField] = encryptedKeyVersion.GetEncryptionKeyVersionName
					();
				json[KMSRESTConstants.IvField] = Base64.EncodeBase64URLSafeString(encryptedKeyVersion
					.GetEncryptedKeyIv());
				json[KMSRESTConstants.EncryptedKeyVersionField] = ToJSON(encryptedKeyVersion.GetEncryptedKeyVersion
					());
			}
			return json;
		}

		public static IDictionary ToJSON(string keyName, KeyProvider.Metadata meta)
		{
			IDictionary json = new LinkedHashMap();
			if (meta != null)
			{
				json[KMSRESTConstants.NameField] = keyName;
				json[KMSRESTConstants.CipherField] = meta.GetCipher();
				json[KMSRESTConstants.LengthField] = meta.GetBitLength();
				json[KMSRESTConstants.DescriptionField] = meta.GetDescription();
				json[KMSRESTConstants.AttributesField] = meta.GetAttributes();
				json[KMSRESTConstants.CreatedField] = meta.GetCreated().GetTime();
				json[KMSRESTConstants.VersionsField] = (long)meta.GetVersions();
			}
			return json;
		}

		public static IList ToJSON(string[] keyNames, KeyProvider.Metadata[] metas)
		{
			IList json = new ArrayList();
			for (int i = 0; i < keyNames.Length; i++)
			{
				json.AddItem(ToJSON(keyNames[i], metas[i]));
			}
			return json;
		}
	}
}
