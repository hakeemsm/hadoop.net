using Sharpen;

namespace org.apache.hadoop.crypto.key
{
	/// <summary>
	/// A KeyProvider with Cryptographic Extensions specifically for generating
	/// and decrypting encrypted encryption keys.
	/// </summary>
	public class KeyProviderCryptoExtension : org.apache.hadoop.crypto.key.KeyProviderExtension
		<org.apache.hadoop.crypto.key.KeyProviderCryptoExtension.CryptoExtension>
	{
		/// <summary>Designates an encrypted encryption key, or EEK.</summary>
		public const string EEK = "EEK";

		/// <summary>
		/// Designates a decrypted encrypted encryption key, that is, an encryption key
		/// (EK).
		/// </summary>
		public const string EK = "EK";

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

			private org.apache.hadoop.crypto.key.KeyProvider.KeyVersion encryptedKeyVersion;

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
				, byte[] encryptedKeyIv, org.apache.hadoop.crypto.key.KeyProvider.KeyVersion encryptedKeyVersion
				)
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
			public static org.apache.hadoop.crypto.key.KeyProviderCryptoExtension.EncryptedKeyVersion
				 createForDecryption(string keyName, string encryptionKeyVersionName, byte[] encryptedKeyIv
				, byte[] encryptedKeyMaterial)
			{
				org.apache.hadoop.crypto.key.KeyProvider.KeyVersion encryptedKeyVersion = new org.apache.hadoop.crypto.key.KeyProvider.KeyVersion
					(null, EEK, encryptedKeyMaterial);
				return new org.apache.hadoop.crypto.key.KeyProviderCryptoExtension.EncryptedKeyVersion
					(keyName, encryptionKeyVersionName, encryptedKeyIv, encryptedKeyVersion);
			}

			/// <returns>Name of the encryption key used to encrypt the encrypted key.</returns>
			public virtual string getEncryptionKeyName()
			{
				return encryptionKeyName;
			}

			/// <returns>
			/// Version name of the encryption key used to encrypt the encrypted
			/// key.
			/// </returns>
			public virtual string getEncryptionKeyVersionName()
			{
				return encryptionKeyVersionName;
			}

			/// <returns>
			/// Initialization vector of the encrypted key. The IV of the
			/// encryption key used to encrypt the encrypted key is derived from this
			/// IV.
			/// </returns>
			public virtual byte[] getEncryptedKeyIv()
			{
				return encryptedKeyIv;
			}

			/// <returns>The encrypted encryption key version.</returns>
			public virtual org.apache.hadoop.crypto.key.KeyProvider.KeyVersion getEncryptedKeyVersion
				()
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
			/// <see cref="getEncryptedKeyIv()"/>
			/// )
			/// </param>
			/// <returns>IV for the encryption key</returns>
			protected internal static byte[] deriveIV(byte[] encryptedKeyIV)
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
		public interface CryptoExtension : org.apache.hadoop.crypto.key.KeyProviderExtension.Extension
		{
			/// <summary>
			/// Calls to this method allows the underlying KeyProvider to warm-up any
			/// implementation specific caches used to store the Encrypted Keys.
			/// </summary>
			/// <param name="keyNames">Array of Key Names</param>
			/// <exception cref="System.IO.IOException"/>
			void warmUpEncryptedKeys(params string[] keyNames);

			/// <summary>Drains the Queue for the provided key.</summary>
			/// <param name="keyName">the key to drain the Queue for</param>
			void drain(string keyName);

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
			/// <exception cref="java.security.GeneralSecurityException">
			/// thrown if the key material could not be encrypted because of a
			/// cryptographic issue.
			/// </exception>
			org.apache.hadoop.crypto.key.KeyProviderCryptoExtension.EncryptedKeyVersion generateEncryptedKey
				(string encryptionKeyName);

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
			/// <exception cref="java.security.GeneralSecurityException">
			/// thrown if the key material could not be decrypted because of a
			/// cryptographic issue.
			/// </exception>
			org.apache.hadoop.crypto.key.KeyProvider.KeyVersion decryptEncryptedKey(org.apache.hadoop.crypto.key.KeyProviderCryptoExtension.EncryptedKeyVersion
				 encryptedKeyVersion);
		}

		private class DefaultCryptoExtension : org.apache.hadoop.crypto.key.KeyProviderCryptoExtension.CryptoExtension
		{
			private readonly org.apache.hadoop.crypto.key.KeyProvider keyProvider;

			private sealed class _ThreadLocal_236 : java.lang.ThreadLocal<java.security.SecureRandom
				>
			{
				public _ThreadLocal_236()
				{
				}

				protected override java.security.SecureRandom initialValue()
				{
					return new java.security.SecureRandom();
				}
			}

			private static readonly java.lang.ThreadLocal<java.security.SecureRandom> RANDOM = 
				new _ThreadLocal_236();

			private DefaultCryptoExtension(org.apache.hadoop.crypto.key.KeyProvider keyProvider
				)
			{
				this.keyProvider = keyProvider;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="java.security.GeneralSecurityException"/>
			public virtual org.apache.hadoop.crypto.key.KeyProviderCryptoExtension.EncryptedKeyVersion
				 generateEncryptedKey(string encryptionKeyName)
			{
				// Fetch the encryption key
				org.apache.hadoop.crypto.key.KeyProvider.KeyVersion encryptionKey = keyProvider.getCurrentKey
					(encryptionKeyName);
				com.google.common.@base.Preconditions.checkNotNull(encryptionKey, "No KeyVersion exists for key '%s' "
					, encryptionKeyName);
				// Generate random bytes for new key and IV
				org.apache.hadoop.crypto.CryptoCodec cc = org.apache.hadoop.crypto.CryptoCodec.getInstance
					(keyProvider.getConf());
				byte[] newKey = new byte[encryptionKey.getMaterial().Length];
				cc.generateSecureRandom(newKey);
				byte[] iv = new byte[cc.getCipherSuite().getAlgorithmBlockSize()];
				cc.generateSecureRandom(iv);
				// Encryption key IV is derived from new key's IV
				byte[] encryptionIV = org.apache.hadoop.crypto.key.KeyProviderCryptoExtension.EncryptedKeyVersion
					.deriveIV(iv);
				org.apache.hadoop.crypto.Encryptor encryptor = cc.createEncryptor();
				encryptor.init(encryptionKey.getMaterial(), encryptionIV);
				int keyLen = newKey.Length;
				java.nio.ByteBuffer bbIn = java.nio.ByteBuffer.allocateDirect(keyLen);
				java.nio.ByteBuffer bbOut = java.nio.ByteBuffer.allocateDirect(keyLen);
				bbIn.put(newKey);
				bbIn.flip();
				encryptor.encrypt(bbIn, bbOut);
				bbOut.flip();
				byte[] encryptedKey = new byte[keyLen];
				bbOut.get(encryptedKey);
				return new org.apache.hadoop.crypto.key.KeyProviderCryptoExtension.EncryptedKeyVersion
					(encryptionKeyName, encryptionKey.getVersionName(), iv, new org.apache.hadoop.crypto.key.KeyProvider.KeyVersion
					(encryptionKey.getName(), EEK, encryptedKey));
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="java.security.GeneralSecurityException"/>
			public virtual org.apache.hadoop.crypto.key.KeyProvider.KeyVersion decryptEncryptedKey
				(org.apache.hadoop.crypto.key.KeyProviderCryptoExtension.EncryptedKeyVersion encryptedKeyVersion
				)
			{
				// Fetch the encryption key material
				string encryptionKeyVersionName = encryptedKeyVersion.getEncryptionKeyVersionName
					();
				org.apache.hadoop.crypto.key.KeyProvider.KeyVersion encryptionKey = keyProvider.getKeyVersion
					(encryptionKeyVersionName);
				com.google.common.@base.Preconditions.checkNotNull(encryptionKey, "KeyVersion name '%s' does not exist"
					, encryptionKeyVersionName);
				com.google.common.@base.Preconditions.checkArgument(encryptedKeyVersion.getEncryptedKeyVersion
					().getVersionName().Equals(org.apache.hadoop.crypto.key.KeyProviderCryptoExtension
					.EEK), "encryptedKey version name must be '%s', is '%s'", org.apache.hadoop.crypto.key.KeyProviderCryptoExtension
					.EEK, encryptedKeyVersion.getEncryptedKeyVersion().getVersionName());
				// Encryption key IV is determined from encrypted key's IV
				byte[] encryptionIV = org.apache.hadoop.crypto.key.KeyProviderCryptoExtension.EncryptedKeyVersion
					.deriveIV(encryptedKeyVersion.getEncryptedKeyIv());
				org.apache.hadoop.crypto.CryptoCodec cc = org.apache.hadoop.crypto.CryptoCodec.getInstance
					(keyProvider.getConf());
				org.apache.hadoop.crypto.Decryptor decryptor = cc.createDecryptor();
				decryptor.init(encryptionKey.getMaterial(), encryptionIV);
				org.apache.hadoop.crypto.key.KeyProvider.KeyVersion encryptedKV = encryptedKeyVersion
					.getEncryptedKeyVersion();
				int keyLen = encryptedKV.getMaterial().Length;
				java.nio.ByteBuffer bbIn = java.nio.ByteBuffer.allocateDirect(keyLen);
				java.nio.ByteBuffer bbOut = java.nio.ByteBuffer.allocateDirect(keyLen);
				bbIn.put(encryptedKV.getMaterial());
				bbIn.flip();
				decryptor.decrypt(bbIn, bbOut);
				bbOut.flip();
				byte[] decryptedKey = new byte[keyLen];
				bbOut.get(decryptedKey);
				return new org.apache.hadoop.crypto.key.KeyProvider.KeyVersion(encryptionKey.getName
					(), EK, decryptedKey);
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void warmUpEncryptedKeys(params string[] keyNames)
			{
			}

			// NO-OP since the default version does not cache any keys
			public virtual void drain(string keyName)
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
		protected internal KeyProviderCryptoExtension(org.apache.hadoop.crypto.key.KeyProvider
			 keyProvider, org.apache.hadoop.crypto.key.KeyProviderCryptoExtension.CryptoExtension
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
		public virtual void warmUpEncryptedKeys(params string[] keyNames)
		{
			getExtension().warmUpEncryptedKeys(keyNames);
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
		/// <exception cref="java.security.GeneralSecurityException">
		/// thrown if the key material could not be
		/// encrypted because of a cryptographic issue.
		/// </exception>
		public virtual org.apache.hadoop.crypto.key.KeyProviderCryptoExtension.EncryptedKeyVersion
			 generateEncryptedKey(string encryptionKeyName)
		{
			return getExtension().generateEncryptedKey(encryptionKeyName);
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
		/// <exception cref="java.security.GeneralSecurityException">
		/// thrown if the key material could not be
		/// decrypted because of a cryptographic issue.
		/// </exception>
		public virtual org.apache.hadoop.crypto.key.KeyProvider.KeyVersion decryptEncryptedKey
			(org.apache.hadoop.crypto.key.KeyProviderCryptoExtension.EncryptedKeyVersion encryptedKey
			)
		{
			return getExtension().decryptEncryptedKey(encryptedKey);
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
		public static org.apache.hadoop.crypto.key.KeyProviderCryptoExtension createKeyProviderCryptoExtension
			(org.apache.hadoop.crypto.key.KeyProvider keyProvider)
		{
			org.apache.hadoop.crypto.key.KeyProviderCryptoExtension.CryptoExtension cryptoExtension
				 = (keyProvider is org.apache.hadoop.crypto.key.KeyProviderCryptoExtension.CryptoExtension
				) ? (org.apache.hadoop.crypto.key.KeyProviderCryptoExtension.CryptoExtension)keyProvider
				 : new org.apache.hadoop.crypto.key.KeyProviderCryptoExtension.DefaultCryptoExtension
				(keyProvider);
			return new org.apache.hadoop.crypto.key.KeyProviderCryptoExtension(keyProvider, cryptoExtension
				);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void close()
		{
			if (getKeyProvider() != null)
			{
				getKeyProvider().close();
			}
		}
	}
}
