using Sharpen;

namespace Org.Apache.Hadoop.Crypto
{
	/// <summary>
	/// Used between client and server to negotiate the
	/// cipher suite, key and iv.
	/// </summary>
	public class CipherOption
	{
		private readonly CipherSuite suite;

		private readonly byte[] inKey;

		private readonly byte[] inIv;

		private readonly byte[] outKey;

		private readonly byte[] outIv;

		public CipherOption(CipherSuite suite)
			: this(suite, null, null, null, null)
		{
		}

		public CipherOption(CipherSuite suite, byte[] inKey, byte[] inIv, byte[] outKey, 
			byte[] outIv)
		{
			this.suite = suite;
			this.inKey = inKey;
			this.inIv = inIv;
			this.outKey = outKey;
			this.outIv = outIv;
		}

		public virtual CipherSuite GetCipherSuite()
		{
			return suite;
		}

		public virtual byte[] GetInKey()
		{
			return inKey;
		}

		public virtual byte[] GetInIv()
		{
			return inIv;
		}

		public virtual byte[] GetOutKey()
		{
			return outKey;
		}

		public virtual byte[] GetOutIv()
		{
			return outIv;
		}
	}
}
