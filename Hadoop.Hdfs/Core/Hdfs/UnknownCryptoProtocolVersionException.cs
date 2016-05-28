using System.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	[System.Serializable]
	public class UnknownCryptoProtocolVersionException : IOException
	{
		private const long serialVersionUID = 8957192l;

		public UnknownCryptoProtocolVersionException()
			: base()
		{
		}

		public UnknownCryptoProtocolVersionException(string msg)
			: base(msg)
		{
		}
	}
}
