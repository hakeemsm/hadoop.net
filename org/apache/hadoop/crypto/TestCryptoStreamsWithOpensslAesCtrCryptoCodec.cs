using Sharpen;

namespace org.apache.hadoop.crypto
{
	public class TestCryptoStreamsWithOpensslAesCtrCryptoCodec : org.apache.hadoop.crypto.TestCryptoStreams
	{
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.BeforeClass]
		public static void init()
		{
			org.apache.hadoop.test.GenericTestUtils.assumeInNativeProfile();
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			codec = org.apache.hadoop.crypto.CryptoCodec.getInstance(conf);
		}
	}
}
