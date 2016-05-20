using Sharpen;

namespace org.apache.hadoop.crypto.key
{
	public class TestKeyProviderCryptoExtension
	{
		private const string CIPHER = "AES";

		private const string ENCRYPTION_KEY_NAME = "fooKey";

		private static org.apache.hadoop.conf.Configuration conf;

		private static org.apache.hadoop.crypto.key.KeyProvider kp;

		private static org.apache.hadoop.crypto.key.KeyProviderCryptoExtension kpExt;

		private static org.apache.hadoop.crypto.key.KeyProvider.Options options;

		private static org.apache.hadoop.crypto.key.KeyProvider.KeyVersion encryptionKey;

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.BeforeClass]
		public static void setup()
		{
			conf = new org.apache.hadoop.conf.Configuration();
			kp = new org.apache.hadoop.crypto.key.UserProvider.Factory().createProvider(new java.net.URI
				("user:///"), conf);
			kpExt = org.apache.hadoop.crypto.key.KeyProviderCryptoExtension.createKeyProviderCryptoExtension
				(kp);
			options = new org.apache.hadoop.crypto.key.KeyProvider.Options(conf);
			options.setCipher(CIPHER);
			options.setBitLength(128);
			encryptionKey = kp.createKey(ENCRYPTION_KEY_NAME, java.security.SecureRandom.getSeed
				(16), options);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testGenerateEncryptedKey()
		{
			// Generate a new EEK and check it
			org.apache.hadoop.crypto.key.KeyProviderCryptoExtension.EncryptedKeyVersion ek1 = 
				kpExt.generateEncryptedKey(encryptionKey.getName());
			NUnit.Framework.Assert.AreEqual("Version name of EEK should be EEK", org.apache.hadoop.crypto.key.KeyProviderCryptoExtension
				.EEK, ek1.getEncryptedKeyVersion().getVersionName());
			NUnit.Framework.Assert.AreEqual("Name of EEK should be encryption key name", ENCRYPTION_KEY_NAME
				, ek1.getEncryptionKeyName());
			NUnit.Framework.Assert.IsNotNull("Expected encrypted key material", ek1.getEncryptedKeyVersion
				().getMaterial());
			NUnit.Framework.Assert.AreEqual("Length of encryption key material and EEK material should "
				 + "be the same", encryptionKey.getMaterial().Length, ek1.getEncryptedKeyVersion
				().getMaterial().Length);
			// Decrypt EEK into an EK and check it
			org.apache.hadoop.crypto.key.KeyProvider.KeyVersion k1 = kpExt.decryptEncryptedKey
				(ek1);
			NUnit.Framework.Assert.AreEqual(org.apache.hadoop.crypto.key.KeyProviderCryptoExtension
				.EK, k1.getVersionName());
			NUnit.Framework.Assert.AreEqual(encryptionKey.getMaterial().Length, k1.getMaterial
				().Length);
			if (java.util.Arrays.equals(k1.getMaterial(), encryptionKey.getMaterial()))
			{
				NUnit.Framework.Assert.Fail("Encrypted key material should not equal encryption key material"
					);
			}
			if (java.util.Arrays.equals(ek1.getEncryptedKeyVersion().getMaterial(), encryptionKey
				.getMaterial()))
			{
				NUnit.Framework.Assert.Fail("Encrypted key material should not equal decrypted key material"
					);
			}
			// Decrypt it again and it should be the same
			org.apache.hadoop.crypto.key.KeyProvider.KeyVersion k1a = kpExt.decryptEncryptedKey
				(ek1);
			NUnit.Framework.Assert.assertArrayEquals(k1.getMaterial(), k1a.getMaterial());
			// Generate another EEK and make sure it's different from the first
			org.apache.hadoop.crypto.key.KeyProviderCryptoExtension.EncryptedKeyVersion ek2 = 
				kpExt.generateEncryptedKey(encryptionKey.getName());
			org.apache.hadoop.crypto.key.KeyProvider.KeyVersion k2 = kpExt.decryptEncryptedKey
				(ek2);
			if (java.util.Arrays.equals(k1.getMaterial(), k2.getMaterial()))
			{
				NUnit.Framework.Assert.Fail("Generated EEKs should have different material!");
			}
			if (java.util.Arrays.equals(ek1.getEncryptedKeyIv(), ek2.getEncryptedKeyIv()))
			{
				NUnit.Framework.Assert.Fail("Generated EEKs should have different IVs!");
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testEncryptDecrypt()
		{
			// Get an EEK
			org.apache.hadoop.crypto.key.KeyProviderCryptoExtension.EncryptedKeyVersion eek = 
				kpExt.generateEncryptedKey(encryptionKey.getName());
			byte[] encryptedKeyIv = eek.getEncryptedKeyIv();
			byte[] encryptedKeyMaterial = eek.getEncryptedKeyVersion().getMaterial();
			// Decrypt it manually
			javax.crypto.Cipher cipher = javax.crypto.Cipher.getInstance("AES/CTR/NoPadding");
			cipher.init(javax.crypto.Cipher.DECRYPT_MODE, new javax.crypto.spec.SecretKeySpec
				(encryptionKey.getMaterial(), "AES"), new javax.crypto.spec.IvParameterSpec(org.apache.hadoop.crypto.key.KeyProviderCryptoExtension.EncryptedKeyVersion
				.deriveIV(encryptedKeyIv)));
			byte[] manualMaterial = cipher.doFinal(encryptedKeyMaterial);
			// Test the createForDecryption factory method
			org.apache.hadoop.crypto.key.KeyProviderCryptoExtension.EncryptedKeyVersion eek2 = 
				org.apache.hadoop.crypto.key.KeyProviderCryptoExtension.EncryptedKeyVersion.createForDecryption
				(eek.getEncryptionKeyName(), eek.getEncryptionKeyVersionName(), eek.getEncryptedKeyIv
				(), eek.getEncryptedKeyVersion().getMaterial());
			// Decrypt it with the API
			org.apache.hadoop.crypto.key.KeyProvider.KeyVersion decryptedKey = kpExt.decryptEncryptedKey
				(eek2);
			byte[] apiMaterial = decryptedKey.getMaterial();
			NUnit.Framework.Assert.assertArrayEquals("Wrong key material from decryptEncryptedKey"
				, manualMaterial, apiMaterial);
		}
	}
}
