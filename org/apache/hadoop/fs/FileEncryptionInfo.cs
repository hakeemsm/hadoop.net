using Sharpen;

namespace org.apache.hadoop.fs
{
	/// <summary>
	/// FileEncryptionInfo encapsulates all the encryption-related information for
	/// an encrypted file.
	/// </summary>
	public class FileEncryptionInfo
	{
		private readonly org.apache.hadoop.crypto.CipherSuite cipherSuite;

		private readonly org.apache.hadoop.crypto.CryptoProtocolVersion version;

		private readonly byte[] edek;

		private readonly byte[] iv;

		private readonly string keyName;

		private readonly string ezKeyVersionName;

		/// <summary>Create a FileEncryptionInfo.</summary>
		/// <param name="suite">CipherSuite used to encrypt the file</param>
		/// <param name="edek">encrypted data encryption key (EDEK) of the file</param>
		/// <param name="iv">initialization vector (IV) used to encrypt the file</param>
		/// <param name="keyName">name of the key used for the encryption zone</param>
		/// <param name="ezKeyVersionName">
		/// name of the KeyVersion used to encrypt the
		/// encrypted data encryption key.
		/// </param>
		public FileEncryptionInfo(org.apache.hadoop.crypto.CipherSuite suite, org.apache.hadoop.crypto.CryptoProtocolVersion
			 version, byte[] edek, byte[] iv, string keyName, string ezKeyVersionName)
		{
			com.google.common.@base.Preconditions.checkNotNull(suite);
			com.google.common.@base.Preconditions.checkNotNull(version);
			com.google.common.@base.Preconditions.checkNotNull(edek);
			com.google.common.@base.Preconditions.checkNotNull(iv);
			com.google.common.@base.Preconditions.checkNotNull(keyName);
			com.google.common.@base.Preconditions.checkNotNull(ezKeyVersionName);
			com.google.common.@base.Preconditions.checkArgument(iv.Length == suite.getAlgorithmBlockSize
				(), "Unexpected IV length");
			this.cipherSuite = suite;
			this.version = version;
			this.edek = edek;
			this.iv = iv;
			this.keyName = keyName;
			this.ezKeyVersionName = ezKeyVersionName;
		}

		/// <returns>
		/// 
		/// <see cref="org.apache.hadoop.crypto.CipherSuite"/>
		/// used to encrypt
		/// the file.
		/// </returns>
		public virtual org.apache.hadoop.crypto.CipherSuite getCipherSuite()
		{
			return cipherSuite;
		}

		/// <returns>
		/// 
		/// <see cref="org.apache.hadoop.crypto.CryptoProtocolVersion"/>
		/// to use
		/// to access the file.
		/// </returns>
		public virtual org.apache.hadoop.crypto.CryptoProtocolVersion getCryptoProtocolVersion
			()
		{
			return version;
		}

		/// <returns>encrypted data encryption key (EDEK) for the file</returns>
		public virtual byte[] getEncryptedDataEncryptionKey()
		{
			return edek;
		}

		/// <returns>initialization vector (IV) for the cipher used to encrypt the file</returns>
		public virtual byte[] getIV()
		{
			return iv;
		}

		/// <returns>name of the encryption zone key.</returns>
		public virtual string getKeyName()
		{
			return keyName;
		}

		/// <returns>
		/// name of the encryption zone KeyVersion used to encrypt the
		/// encrypted data encryption key (EDEK).
		/// </returns>
		public virtual string getEzKeyVersionName()
		{
			return ezKeyVersionName;
		}

		public override string ToString()
		{
			java.lang.StringBuilder builder = new java.lang.StringBuilder("{");
			builder.Append("cipherSuite: " + cipherSuite);
			builder.Append(", cryptoProtocolVersion: " + version);
			builder.Append(", edek: " + org.apache.commons.codec.binary.Hex.encodeHexString(edek
				));
			builder.Append(", iv: " + org.apache.commons.codec.binary.Hex.encodeHexString(iv)
				);
			builder.Append(", keyName: " + keyName);
			builder.Append(", ezKeyVersionName: " + ezKeyVersionName);
			builder.Append("}");
			return builder.ToString();
		}
	}
}
