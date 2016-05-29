using System.IO;
using Org.Apache.Commons.Codec.Digest;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Sharedcache
{
	public class ChecksumSHA256Impl : SharedCacheChecksum
	{
		/// <exception cref="System.IO.IOException"/>
		public virtual string ComputeChecksum(InputStream @in)
		{
			return DigestUtils.Sha256Hex(@in);
		}
	}
}
