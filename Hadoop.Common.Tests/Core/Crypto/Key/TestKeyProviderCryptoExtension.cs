using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Sharpen;

namespace Org.Apache.Hadoop.Crypto.Key
{
	public class TestKeyProviderCryptoExtension
	{
		private const string Cipher = "AES";

		private const string EncryptionKeyName = "fooKey";

		private static Configuration conf;

		private static KeyProvider kp;

		private static KeyProviderCryptoExtension kpExt;

		private static KeyProvider.Options options;

		private static KeyProvider.KeyVersion encryptionKey;

		/// <exception cref="System.Exception"/>
		[BeforeClass]
		public static void Setup()
		{
			conf = new Configuration();
			kp = new UserProvider.Factory().CreateProvider(new URI("user:///"), conf);
			kpExt = KeyProviderCryptoExtension.CreateKeyProviderCryptoExtension(kp);
			options = new KeyProvider.Options(conf);
			options.SetCipher(Cipher);
			options.SetBitLength(128);
			encryptionKey = kp.CreateKey(EncryptionKeyName, SecureRandom.GetSeed(16), options
				);
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestGenerateEncryptedKey()
		{
			// Generate a new EEK and check it
			KeyProviderCryptoExtension.EncryptedKeyVersion ek1 = kpExt.GenerateEncryptedKey(encryptionKey
				.GetName());
			Assert.Equal("Version name of EEK should be EEK", KeyProviderCryptoExtension
				.Eek, ek1.GetEncryptedKeyVersion().GetVersionName());
			Assert.Equal("Name of EEK should be encryption key name", EncryptionKeyName
				, ek1.GetEncryptionKeyName());
			NUnit.Framework.Assert.IsNotNull("Expected encrypted key material", ek1.GetEncryptedKeyVersion
				().GetMaterial());
			Assert.Equal("Length of encryption key material and EEK material should "
				 + "be the same", encryptionKey.GetMaterial().Length, ek1.GetEncryptedKeyVersion
				().GetMaterial().Length);
			// Decrypt EEK into an EK and check it
			KeyProvider.KeyVersion k1 = kpExt.DecryptEncryptedKey(ek1);
			Assert.Equal(KeyProviderCryptoExtension.Ek, k1.GetVersionName(
				));
			Assert.Equal(encryptionKey.GetMaterial().Length, k1.GetMaterial
				().Length);
			if (Arrays.Equals(k1.GetMaterial(), encryptionKey.GetMaterial()))
			{
				NUnit.Framework.Assert.Fail("Encrypted key material should not equal encryption key material"
					);
			}
			if (Arrays.Equals(ek1.GetEncryptedKeyVersion().GetMaterial(), encryptionKey.GetMaterial
				()))
			{
				NUnit.Framework.Assert.Fail("Encrypted key material should not equal decrypted key material"
					);
			}
			// Decrypt it again and it should be the same
			KeyProvider.KeyVersion k1a = kpExt.DecryptEncryptedKey(ek1);
			Assert.AssertArrayEquals(k1.GetMaterial(), k1a.GetMaterial());
			// Generate another EEK and make sure it's different from the first
			KeyProviderCryptoExtension.EncryptedKeyVersion ek2 = kpExt.GenerateEncryptedKey(encryptionKey
				.GetName());
			KeyProvider.KeyVersion k2 = kpExt.DecryptEncryptedKey(ek2);
			if (Arrays.Equals(k1.GetMaterial(), k2.GetMaterial()))
			{
				NUnit.Framework.Assert.Fail("Generated EEKs should have different material!");
			}
			if (Arrays.Equals(ek1.GetEncryptedKeyIv(), ek2.GetEncryptedKeyIv()))
			{
				NUnit.Framework.Assert.Fail("Generated EEKs should have different IVs!");
			}
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestEncryptDecrypt()
		{
			// Get an EEK
			KeyProviderCryptoExtension.EncryptedKeyVersion eek = kpExt.GenerateEncryptedKey(encryptionKey
				.GetName());
			byte[] encryptedKeyIv = eek.GetEncryptedKeyIv();
			byte[] encryptedKeyMaterial = eek.GetEncryptedKeyVersion().GetMaterial();
			// Decrypt it manually
			Sharpen.Cipher cipher = Sharpen.Cipher.GetInstance("AES/CTR/NoPadding");
			cipher.Init(Sharpen.Cipher.DecryptMode, new SecretKeySpec(encryptionKey.GetMaterial
				(), "AES"), new IvParameterSpec(KeyProviderCryptoExtension.EncryptedKeyVersion.DeriveIV
				(encryptedKeyIv)));
			byte[] manualMaterial = cipher.DoFinal(encryptedKeyMaterial);
			// Test the createForDecryption factory method
			KeyProviderCryptoExtension.EncryptedKeyVersion eek2 = KeyProviderCryptoExtension.EncryptedKeyVersion
				.CreateForDecryption(eek.GetEncryptionKeyName(), eek.GetEncryptionKeyVersionName
				(), eek.GetEncryptedKeyIv(), eek.GetEncryptedKeyVersion().GetMaterial());
			// Decrypt it with the API
			KeyProvider.KeyVersion decryptedKey = kpExt.DecryptEncryptedKey(eek2);
			byte[] apiMaterial = decryptedKey.GetMaterial();
			Assert.AssertArrayEquals("Wrong key material from decryptEncryptedKey", manualMaterial
				, apiMaterial);
		}
	}
}
