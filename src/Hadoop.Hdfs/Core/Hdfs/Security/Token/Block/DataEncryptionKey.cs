using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Security.Token.Block
{
	/// <summary>
	/// A little struct class to contain all fields required to perform encryption of
	/// the DataTransferProtocol.
	/// </summary>
	public class DataEncryptionKey
	{
		public readonly int keyId;

		public readonly string blockPoolId;

		public readonly byte[] nonce;

		public readonly byte[] encryptionKey;

		public readonly long expiryDate;

		public readonly string encryptionAlgorithm;

		public DataEncryptionKey(int keyId, string blockPoolId, byte[] nonce, byte[] encryptionKey
			, long expiryDate, string encryptionAlgorithm)
		{
			this.keyId = keyId;
			this.blockPoolId = blockPoolId;
			this.nonce = nonce;
			this.encryptionKey = encryptionKey;
			this.expiryDate = expiryDate;
			this.encryptionAlgorithm = encryptionAlgorithm;
		}

		public override string ToString()
		{
			return keyId + "/" + blockPoolId + "/" + nonce.Length + "/" + encryptionKey.Length;
		}
	}
}
