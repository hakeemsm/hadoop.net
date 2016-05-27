using Org.Apache.Hadoop.Classification;
using Sharpen;

namespace Org.Apache.Hadoop.FS
{
	/// <summary>Stream that permits seeking.</summary>
	public interface Seekable
	{
		/// <summary>Seek to the given offset from the start of the file.</summary>
		/// <remarks>
		/// Seek to the given offset from the start of the file.
		/// The next read() will be from that location.  Can't
		/// seek past the end of the file.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		void Seek(long pos);

		/// <summary>Return the current offset from the start of the file</summary>
		/// <exception cref="System.IO.IOException"/>
		long GetPos();

		/// <summary>Seeks a different copy of the data.</summary>
		/// <remarks>
		/// Seeks a different copy of the data.  Returns true if
		/// found a new source, false otherwise.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		[InterfaceAudience.Private]
		bool SeekToNewSource(long targetPos);
	}
}
