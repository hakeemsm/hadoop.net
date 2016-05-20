using Sharpen;

namespace org.apache.hadoop.fs
{
	public interface CanSetDropBehind
	{
		/// <summary>Configure whether the stream should drop the cache.</summary>
		/// <param name="dropCache">
		/// Whether to drop the cache.  null means to use the
		/// default value.
		/// </param>
		/// <exception cref="System.IO.IOException">
		/// If there was an error changing the dropBehind
		/// setting.
		/// UnsupportedOperationException  If this stream doesn't support
		/// setting the drop-behind.
		/// </exception>
		/// <exception cref="System.NotSupportedException"/>
		void setDropBehind(bool dropCache);
	}
}
