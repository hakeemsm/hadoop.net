using Org.Apache.Hadoop.Security.Token.Delegation;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Security.Token.Block
{
	/// <summary>Key used for generating and verifying block tokens</summary>
	public class BlockKey : DelegationKey
	{
		public BlockKey()
			: base()
		{
		}

		public BlockKey(int keyId, long expiryDate, SecretKey key)
			: base(keyId, expiryDate, key)
		{
		}

		public BlockKey(int keyId, long expiryDate, byte[] encodedKey)
			: base(keyId, expiryDate, encodedKey)
		{
		}
	}
}
