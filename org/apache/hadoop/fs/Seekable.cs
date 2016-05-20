using Sharpen;

namespace org.apache.hadoop.fs
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
		void seek(long pos);

		/// <summary>Return the current offset from the start of the file</summary>
		/// <exception cref="System.IO.IOException"/>
		long getPos();

		/// <summary>Seeks a different copy of the data.</summary>
		/// <remarks>
		/// Seeks a different copy of the data.  Returns true if
		/// found a new source, false otherwise.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		[org.apache.hadoop.classification.InterfaceAudience.Private]
		bool seekToNewSource(long targetPos);
	}
}
