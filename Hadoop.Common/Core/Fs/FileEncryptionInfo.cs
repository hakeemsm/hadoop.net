using System.Text;
using Com.Google.Common.Base;
using Org.Apache.Commons.Codec.Binary;
using Org.Apache.Hadoop.Crypto;
using Sharpen;

namespace Org.Apache.Hadoop.FS
{
	/// <summary>
	/// FileEncryptionInfo encapsulates all the encryption-related information for
	/// an encrypted file.
	/// </summary>
	public class FileEncryptionInfo
	{
		private readonly CipherSuite cipherSuite;

		private readonly CryptoProtocolVersion version;

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
		public FileEncryptionInfo(CipherSuite suite, CryptoProtocolVersion version, byte[]
			 edek, byte[] iv, string keyName, string ezKeyVersionName)
		{
			Preconditions.CheckNotNull(suite);
			Preconditions.CheckNotNull(version);
			Preconditions.CheckNotNull(edek);
			Preconditions.CheckNotNull(iv);
			Preconditions.CheckNotNull(keyName);
			Preconditions.CheckNotNull(ezKeyVersionName);
			Preconditions.CheckArgument(iv.Length == suite.GetAlgorithmBlockSize(), "Unexpected IV length"
				);
			this.cipherSuite = suite;
			this.version = version;
			this.edek = edek;
			this.iv = iv;
			this.keyName = keyName;
			this.ezKeyVersionName = ezKeyVersionName;
		}

		/// <returns>
		/// 
		/// <see cref="Org.Apache.Hadoop.Crypto.CipherSuite"/>
		/// used to encrypt
		/// the file.
		/// </returns>
		public virtual CipherSuite GetCipherSuite()
		{
			return cipherSuite;
		}

		/// <returns>
		/// 
		/// <see cref="Org.Apache.Hadoop.Crypto.CryptoProtocolVersion"/>
		/// to use
		/// to access the file.
		/// </returns>
		public virtual CryptoProtocolVersion GetCryptoProtocolVersion()
		{
			return version;
		}

		/// <returns>encrypted data encryption key (EDEK) for the file</returns>
		public virtual byte[] GetEncryptedDataEncryptionKey()
		{
			return edek;
		}

		/// <returns>initialization vector (IV) for the cipher used to encrypt the file</returns>
		public virtual byte[] GetIV()
		{
			return iv;
		}

		/// <returns>name of the encryption zone key.</returns>
		public virtual string GetKeyName()
		{
			return keyName;
		}

		/// <returns>
		/// name of the encryption zone KeyVersion used to encrypt the
		/// encrypted data encryption key (EDEK).
		/// </returns>
		public virtual string GetEzKeyVersionName()
		{
			return ezKeyVersionName;
		}

		public override string ToString()
		{
			StringBuilder builder = new StringBuilder("{");
			builder.Append("cipherSuite: " + cipherSuite);
			builder.Append(", cryptoProtocolVersion: " + version);
			builder.Append(", edek: " + Hex.EncodeHexString(edek));
			builder.Append(", iv: " + Hex.EncodeHexString(iv));
			builder.Append(", keyName: " + keyName);
			builder.Append(", ezKeyVersionName: " + ezKeyVersionName);
			builder.Append("}");
			return builder.ToString();
		}
	}
}
