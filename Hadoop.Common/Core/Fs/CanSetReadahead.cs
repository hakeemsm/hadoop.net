using Sharpen;

namespace Org.Apache.Hadoop.FS
{
	public interface CanSetReadahead
	{
		/// <summary>Set the readahead on this stream.</summary>
		/// <param name="readahead">The readahead to use.  null means to use the default.</param>
		/// <exception cref="System.IO.IOException">
		/// If there was an error changing the dropBehind
		/// setting.
		/// UnsupportedOperationException  If this stream doesn't support
		/// setting readahead.
		/// </exception>
		/// <exception cref="System.NotSupportedException"/>
		void SetReadahead(long readahead);
	}
}
