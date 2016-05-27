using Com.Google.Common.Base;
using Org.Apache.Hadoop.Crypto;
using Sharpen;

namespace Org.Apache.Hadoop.Crypto.Key
{
	/// <summary>
	/// A KeyProvider with Cryptographic Extensions specifically for generating
	/// and decrypting encrypted encryption keys.
	/// </summary>
	public class KeyProviderCryptoExtension : KeyProviderExtension<KeyProviderCryptoExtension.CryptoExtension
		>
	{
		/// <summary>Designates an encrypted encryption key, or EEK.</summary>
		public const string Eek = "EEK";

		/// <summary>
		/// Designates a decrypted encrypted encryption key, that is, an encryption key
		/// (EK).
		/// </summary>
		public const string Ek = "EK";

		/// <summary>An encrypted encryption key (EEK) and related information.</summary>
		/// <remarks>
		/// An encrypted encryption key (EEK) and related information. An EEK must be
		/// decrypted using the key's encryption key before it can be used.
		/// </remarks>
		public class EncryptedKeyVersion
		{
			private string encryptionKeyName;

			private string encryptionKeyVersionName;

			private byte[] encryptedKeyIv;

			private KeyProvider.KeyVersion encryptedKeyVersion;

			/// <summary>Create a new EncryptedKeyVersion.</summary>
			/// <param name="keyName">
			/// Name of the encryption key used to
			/// encrypt the encrypted key.
			/// </param>
			/// <param name="encryptionKeyVersionName">
			/// Version name of the encryption key used
			/// to encrypt the encrypted key.
			/// </param>
			/// <param name="encryptedKeyIv">
			/// Initialization vector of the encrypted
			/// key. The IV of the encryption key used to
			/// encrypt the encrypted key is derived from
			/// this IV.
			/// </param>
			/// <param name="encryptedKeyVersion">The encrypted encryption key version.</param>
			protected internal EncryptedKeyVersion(string keyName, string encryptionKeyVersionName
				, byte[] encryptedKeyIv, KeyProvider.KeyVersion encryptedKeyVersion)
			{
				this.encryptionKeyName = keyName;
				this.encryptionKeyVersionName = encryptionKeyVersionName;
				this.encryptedKeyIv = encryptedKeyIv;
				this.encryptedKeyVersion = encryptedKeyVersion;
			}

			/// <summary>
			/// Factory method to create a new EncryptedKeyVersion that can then be
			/// passed into
			/// <see cref="#decryptEncryptedKey"/>
			/// . Note that the fields of the
			/// returned EncryptedKeyVersion will only partially be populated; it is not
			/// necessarily suitable for operations besides decryption.
			/// </summary>
			/// <param name="keyName">
			/// Key name of the encryption key use to encrypt the
			/// encrypted key.
			/// </param>
			/// <param name="encryptionKeyVersionName">
			/// Version name of the encryption key used
			/// to encrypt the encrypted key.
			/// </param>
			/// <param name="encryptedKeyIv">
			/// Initialization vector of the encrypted
			/// key. The IV of the encryption key used to
			/// encrypt the encrypted key is derived from
			/// this IV.
			/// </param>
			/// <param name="encryptedKeyMaterial">Key material of the encrypted key.</param>
			/// <returns>EncryptedKeyVersion suitable for decryption.</returns>
			public static KeyProviderCryptoExtension.EncryptedKeyVersion CreateForDecryption(
				string keyName, string encryptionKeyVersionName, byte[] encryptedKeyIv, byte[] encryptedKeyMaterial
				)
			{
				KeyProvider.KeyVersion encryptedKeyVersion = new KeyProvider.KeyVersion(null, Eek
					, encryptedKeyMaterial);
				return new KeyProviderCryptoExtension.EncryptedKeyVersion(keyName, encryptionKeyVersionName
					, encryptedKeyIv, encryptedKeyVersion);
			}

			/// <returns>Name of the encryption key used to encrypt the encrypted key.</returns>
			public virtual string GetEncryptionKeyName()
			{
				return encryptionKeyName;
			}

			/// <returns>
			/// Version name of the encryption key used to encrypt the encrypted
			/// key.
			/// </returns>
			public virtual string GetEncryptionKeyVersionName()
			{
				return encryptionKeyVersionName;
			}

			/// <returns>
			/// Initialization vector of the encrypted key. The IV of the
			/// encryption key used to encrypt the encrypted key is derived from this
			/// IV.
			/// </returns>
			public virtual byte[] GetEncryptedKeyIv()
			{
				return encryptedKeyIv;
			}

			/// <returns>The encrypted encryption key version.</returns>
			public virtual KeyProvider.KeyVersion GetEncryptedKeyVersion()
			{
				return encryptedKeyVersion;
			}

			/// <summary>
			/// Derive the initialization vector (IV) for the encryption key from the IV
			/// of the encrypted key.
			/// </summary>
			/// <remarks>
			/// Derive the initialization vector (IV) for the encryption key from the IV
			/// of the encrypted key. This derived IV is used with the encryption key to
			/// decrypt the encrypted key.
			/// <p/>
			/// The alternative to this is using the same IV for both the encryption key
			/// and the encrypted key. Even a simple symmetric transformation like this
			/// improves security by avoiding IV re-use. IVs will also be fairly unique
			/// among different EEKs.
			/// </remarks>
			/// <param name="encryptedKeyIV">
			/// of the encrypted key (i.e.
			/// <see cref="GetEncryptedKeyIv()"/>
			/// )
			/// </param>
			/// <returns>IV for the encryption key</returns>
			protected internal static byte[] DeriveIV(byte[] encryptedKeyIV)
			{
				byte[] rIv = new byte[encryptedKeyIV.Length];
				// Do a simple XOR transformation to flip all the bits
				for (int i = 0; i < encryptedKeyIV.Length; i++)
				{
					rIv[i] = unchecked((byte)(encryptedKeyIV[i] ^ unchecked((int)(0xff))));
				}
				return rIv;
			}
		}

		/// <summary>
		/// CryptoExtension is a type of Extension that exposes methods to generate
		/// EncryptedKeys and to decrypt the same.
		/// </summary>
		public interface CryptoExtension : KeyProviderExtension.Extension
		{
			/// <summary>
			/// Calls to this method allows the underlying KeyProvider to warm-up any
			/// implementation specific caches used to store the Encrypted Keys.
			/// </summary>
			/// <param name="keyNames">Array of Key Names</param>
			/// <exception cref="System.IO.IOException"/>
			void WarmUpEncryptedKeys(params string[] keyNames);

			/// <summary>Drains the Queue for the provided key.</summary>
			/// <param name="keyName">the key to drain the Queue for</param>
			void Drain(string keyName);

			/// <summary>
			/// Generates a key material and encrypts it using the given key version name
			/// and initialization vector.
			/// </summary>
			/// <remarks>
			/// Generates a key material and encrypts it using the given key version name
			/// and initialization vector. The generated key material is of the same
			/// length as the <code>KeyVersion</code> material of the latest key version
			/// of the key and is encrypted using the same cipher.
			/// <p/>
			/// NOTE: The generated key is not stored by the <code>KeyProvider</code>
			/// </remarks>
			/// <param name="encryptionKeyName">The latest KeyVersion of this key's material will be encrypted.
			/// 	</param>
			/// <returns>
			/// EncryptedKeyVersion with the generated key material, the version
			/// name is 'EEK' (for Encrypted Encryption Key)
			/// </returns>
			/// <exception cref="System.IO.IOException">thrown if the key material could not be generated
			/// 	</exception>
			/// <exception cref="Sharpen.GeneralSecurityException">
			/// thrown if the key material could not be encrypted because of a
			/// cryptographic issue.
			/// </exception>
			KeyProviderCryptoExtension.EncryptedKeyVersion GenerateEncryptedKey(string encryptionKeyName
				);

			/// <summary>
			/// Decrypts an encrypted byte[] key material using the given a key version
			/// name and initialization vector.
			/// </summary>
			/// <param name="encryptedKeyVersion">
			/// contains keyVersionName and IV to decrypt the encrypted key
			/// material
			/// </param>
			/// <returns>
			/// a KeyVersion with the decrypted key material, the version name is
			/// 'EK' (For Encryption Key)
			/// </returns>
			/// <exception cref="System.IO.IOException">thrown if the key material could not be decrypted
			/// 	</exception>
			/// <exception cref="Sharpen.GeneralSecurityException">
			/// thrown if the key material could not be decrypted because of a
			/// cryptographic issue.
			/// </exception>
			KeyProvider.KeyVersion DecryptEncryptedKey(KeyProviderCryptoExtension.EncryptedKeyVersion
				 encryptedKeyVersion);
		}

		private class DefaultCryptoExtension : KeyProviderCryptoExtension.CryptoExtension
		{
			private readonly KeyProvider keyProvider;

			private sealed class _ThreadLocal_236 : ThreadLocal<SecureRandom>
			{
				public _ThreadLocal_236()
				{
				}

				protected override SecureRandom InitialValue()
				{
					return new SecureRandom();
				}
			}

			private static readonly ThreadLocal<SecureRandom> Random = new _ThreadLocal_236();

			private DefaultCryptoExtension(KeyProvider keyProvider)
			{
				this.keyProvider = keyProvider;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="Sharpen.GeneralSecurityException"/>
			public virtual KeyProviderCryptoExtension.EncryptedKeyVersion GenerateEncryptedKey
				(string encryptionKeyName)
			{
				// Fetch the encryption key
				KeyProvider.KeyVersion encryptionKey = keyProvider.GetCurrentKey(encryptionKeyName
					);
				Preconditions.CheckNotNull(encryptionKey, "No KeyVersion exists for key '%s' ", encryptionKeyName
					);
				// Generate random bytes for new key and IV
				CryptoCodec cc = CryptoCodec.GetInstance(keyProvider.GetConf());
				byte[] newKey = new byte[encryptionKey.GetMaterial().Length];
				cc.GenerateSecureRandom(newKey);
				byte[] iv = new byte[cc.GetCipherSuite().GetAlgorithmBlockSize()];
				cc.GenerateSecureRandom(iv);
				// Encryption key IV is derived from new key's IV
				byte[] encryptionIV = KeyProviderCryptoExtension.EncryptedKeyVersion.DeriveIV(iv);
				Encryptor encryptor = cc.CreateEncryptor();
				encryptor.Init(encryptionKey.GetMaterial(), encryptionIV);
				int keyLen = newKey.Length;
				ByteBuffer bbIn = ByteBuffer.AllocateDirect(keyLen);
				ByteBuffer bbOut = ByteBuffer.AllocateDirect(keyLen);
				bbIn.Put(newKey);
				bbIn.Flip();
				encryptor.Encrypt(bbIn, bbOut);
				bbOut.Flip();
				byte[] encryptedKey = new byte[keyLen];
				bbOut.Get(encryptedKey);
				return new KeyProviderCryptoExtension.EncryptedKeyVersion(encryptionKeyName, encryptionKey
					.GetVersionName(), iv, new KeyProvider.KeyVersion(encryptionKey.GetName(), Eek, 
					encryptedKey));
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="Sharpen.GeneralSecurityException"/>
			public virtual KeyProvider.KeyVersion DecryptEncryptedKey(KeyProviderCryptoExtension.EncryptedKeyVersion
				 encryptedKeyVersion)
			{
				// Fetch the encryption key material
				string encryptionKeyVersionName = encryptedKeyVersion.GetEncryptionKeyVersionName
					();
				KeyProvider.KeyVersion encryptionKey = keyProvider.GetKeyVersion(encryptionKeyVersionName
					);
				Preconditions.CheckNotNull(encryptionKey, "KeyVersion name '%s' does not exist", 
					encryptionKeyVersionName);
				Preconditions.CheckArgument(encryptedKeyVersion.GetEncryptedKeyVersion().GetVersionName
					().Equals(KeyProviderCryptoExtension.Eek), "encryptedKey version name must be '%s', is '%s'"
					, KeyProviderCryptoExtension.Eek, encryptedKeyVersion.GetEncryptedKeyVersion().GetVersionName
					());
				// Encryption key IV is determined from encrypted key's IV
				byte[] encryptionIV = KeyProviderCryptoExtension.EncryptedKeyVersion.DeriveIV(encryptedKeyVersion
					.GetEncryptedKeyIv());
				CryptoCodec cc = CryptoCodec.GetInstance(keyProvider.GetConf());
				Decryptor decryptor = cc.CreateDecryptor();
				decryptor.Init(encryptionKey.GetMaterial(), encryptionIV);
				KeyProvider.KeyVersion encryptedKV = encryptedKeyVersion.GetEncryptedKeyVersion();
				int keyLen = encryptedKV.GetMaterial().Length;
				ByteBuffer bbIn = ByteBuffer.AllocateDirect(keyLen);
				ByteBuffer bbOut = ByteBuffer.AllocateDirect(keyLen);
				bbIn.Put(encryptedKV.GetMaterial());
				bbIn.Flip();
				decryptor.Decrypt(bbIn, bbOut);
				bbOut.Flip();
				byte[] decryptedKey = new byte[keyLen];
				bbOut.Get(decryptedKey);
				return new KeyProvider.KeyVersion(encryptionKey.GetName(), Ek, decryptedKey);
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void WarmUpEncryptedKeys(params string[] keyNames)
			{
			}

			// NO-OP since the default version does not cache any keys
			public virtual void Drain(string keyName)
			{
			}
			// NO-OP since the default version does not cache any keys
		}

		/// <summary>
		/// This constructor is to be used by sub classes that provide
		/// delegating/proxying functionality to the
		/// <see cref="KeyProviderCryptoExtension"/>
		/// </summary>
		/// <param name="keyProvider"/>
		/// <param name="extension"/>
		protected internal KeyProviderCryptoExtension(KeyProvider keyProvider, KeyProviderCryptoExtension.CryptoExtension
			 extension)
			: base(keyProvider, extension)
		{
		}

		/// <summary>
		/// Notifies the Underlying CryptoExtension implementation to warm up any
		/// implementation specific caches for the specified KeyVersions
		/// </summary>
		/// <param name="keyNames">Arrays of key Names</param>
		/// <exception cref="System.IO.IOException"/>
		public virtual void WarmUpEncryptedKeys(params string[] keyNames)
		{
			GetExtension().WarmUpEncryptedKeys(keyNames);
		}

		/// <summary>
		/// Generates a key material and encrypts it using the given key version name
		/// and initialization vector.
		/// </summary>
		/// <remarks>
		/// Generates a key material and encrypts it using the given key version name
		/// and initialization vector. The generated key material is of the same
		/// length as the <code>KeyVersion</code> material and is encrypted using the
		/// same cipher.
		/// <p/>
		/// NOTE: The generated key is not stored by the <code>KeyProvider</code>
		/// </remarks>
		/// <param name="encryptionKeyName">
		/// The latest KeyVersion of this key's material will
		/// be encrypted.
		/// </param>
		/// <returns>
		/// EncryptedKeyVersion with the generated key material, the version
		/// name is 'EEK' (for Encrypted Encryption Key)
		/// </returns>
		/// <exception cref="System.IO.IOException">thrown if the key material could not be generated
		/// 	</exception>
		/// <exception cref="Sharpen.GeneralSecurityException">
		/// thrown if the key material could not be
		/// encrypted because of a cryptographic issue.
		/// </exception>
		public virtual KeyProviderCryptoExtension.EncryptedKeyVersion GenerateEncryptedKey
			(string encryptionKeyName)
		{
			return GetExtension().GenerateEncryptedKey(encryptionKeyName);
		}

		/// <summary>
		/// Decrypts an encrypted byte[] key material using the given a key version
		/// name and initialization vector.
		/// </summary>
		/// <param name="encryptedKey">
		/// contains keyVersionName and IV to decrypt the encrypted
		/// key material
		/// </param>
		/// <returns>
		/// a KeyVersion with the decrypted key material, the version name is
		/// 'EK' (For Encryption Key)
		/// </returns>
		/// <exception cref="System.IO.IOException">thrown if the key material could not be decrypted
		/// 	</exception>
		/// <exception cref="Sharpen.GeneralSecurityException">
		/// thrown if the key material could not be
		/// decrypted because of a cryptographic issue.
		/// </exception>
		public virtual KeyProvider.KeyVersion DecryptEncryptedKey(KeyProviderCryptoExtension.EncryptedKeyVersion
			 encryptedKey)
		{
			return GetExtension().DecryptEncryptedKey(encryptedKey);
		}

		/// <summary>
		/// Creates a <code>KeyProviderCryptoExtension</code> using a given
		/// <see cref="KeyProvider"/>
		/// .
		/// <p/>
		/// If the given <code>KeyProvider</code> implements the
		/// <see cref="CryptoExtension"/>
		/// interface the <code>KeyProvider</code> itself
		/// will provide the extension functionality, otherwise a default extension
		/// implementation will be used.
		/// </summary>
		/// <param name="keyProvider">
		/// <code>KeyProvider</code> to use to create the
		/// <code>KeyProviderCryptoExtension</code> extension.
		/// </param>
		/// <returns>
		/// a <code>KeyProviderCryptoExtension</code> instance using the
		/// given <code>KeyProvider</code>.
		/// </returns>
		public static KeyProviderCryptoExtension CreateKeyProviderCryptoExtension(KeyProvider
			 keyProvider)
		{
			KeyProviderCryptoExtension.CryptoExtension cryptoExtension = (keyProvider is KeyProviderCryptoExtension.CryptoExtension
				) ? (KeyProviderCryptoExtension.CryptoExtension)keyProvider : new KeyProviderCryptoExtension.DefaultCryptoExtension
				(keyProvider);
			return new KeyProviderCryptoExtension(keyProvider, cryptoExtension);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Close()
		{
			if (GetKeyProvider() != null)
			{
				GetKeyProvider().Close();
			}
		}
	}
}
