using System.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Sharedcache
{
	public interface SharedCacheChecksum
	{
		/// <summary>Calculate the checksum of the passed input stream.</summary>
		/// <param name="in"><code>InputStream</code> to be checksumed</param>
		/// <returns>the message digest of the input stream</returns>
		/// <exception cref="System.IO.IOException"/>
		string ComputeChecksum(InputStream @in);
	}
}
