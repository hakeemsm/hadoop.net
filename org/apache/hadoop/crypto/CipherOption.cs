using Sharpen;

namespace org.apache.hadoop.crypto
{
	/// <summary>
	/// Used between client and server to negotiate the
	/// cipher suite, key and iv.
	/// </summary>
	public class CipherOption
	{
		private readonly org.apache.hadoop.crypto.CipherSuite suite;

		private readonly byte[] inKey;

		private readonly byte[] inIv;

		private readonly byte[] outKey;

		private readonly byte[] outIv;

		public CipherOption(org.apache.hadoop.crypto.CipherSuite suite)
			: this(suite, null, null, null, null)
		{
		}

		public CipherOption(org.apache.hadoop.crypto.CipherSuite suite, byte[] inKey, byte
			[] inIv, byte[] outKey, byte[] outIv)
		{
			this.suite = suite;
			this.inKey = inKey;
			this.inIv = inIv;
			this.outKey = outKey;
			this.outIv = outIv;
		}

		public virtual org.apache.hadoop.crypto.CipherSuite getCipherSuite()
		{
			return suite;
		}

		public virtual byte[] getInKey()
		{
			return inKey;
		}

		public virtual byte[] getInIv()
		{
			return inIv;
		}

		public virtual byte[] getOutKey()
		{
			return outKey;
		}

		public virtual byte[] getOutIv()
		{
			return outIv;
		}
	}
}
