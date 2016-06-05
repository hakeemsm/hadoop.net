using Org.Apache.Hadoop.Crypto;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Protocol.Datatransfer.Sasl
{
	public class SaslResponseWithNegotiatedCipherOption
	{
		internal readonly byte[] payload;

		internal readonly CipherOption cipherOption;

		public SaslResponseWithNegotiatedCipherOption(byte[] payload, CipherOption cipherOption
			)
		{
			this.payload = payload;
			this.cipherOption = cipherOption;
		}
	}
}
