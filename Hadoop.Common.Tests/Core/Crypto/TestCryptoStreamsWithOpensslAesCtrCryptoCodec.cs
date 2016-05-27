using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Test;
using Sharpen;

namespace Org.Apache.Hadoop.Crypto
{
	public class TestCryptoStreamsWithOpensslAesCtrCryptoCodec : TestCryptoStreams
	{
		/// <exception cref="System.Exception"/>
		[BeforeClass]
		public static void Init()
		{
			GenericTestUtils.AssumeInNativeProfile();
			Configuration conf = new Configuration();
			codec = CryptoCodec.GetInstance(conf);
		}
	}
}
