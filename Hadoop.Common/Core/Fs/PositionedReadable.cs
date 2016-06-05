

namespace Org.Apache.Hadoop.FS
{
	/// <summary>Stream that permits positional reading.</summary>
	public interface PositionedReadable
	{
		/// <summary>
		/// Read upto the specified number of bytes, from a given
		/// position within a file, and return the number of bytes read.
		/// </summary>
		/// <remarks>
		/// Read upto the specified number of bytes, from a given
		/// position within a file, and return the number of bytes read. This does not
		/// change the current offset of a file, and is thread-safe.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		int Read(long position, byte[] buffer, int offset, int length);

		/// <summary>
		/// Read the specified number of bytes, from a given
		/// position within a file.
		/// </summary>
		/// <remarks>
		/// Read the specified number of bytes, from a given
		/// position within a file. This does not
		/// change the current offset of a file, and is thread-safe.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		void ReadFully(long position, byte[] buffer, int offset, int length);

		/// <summary>
		/// Read number of bytes equal to the length of the buffer, from a given
		/// position within a file.
		/// </summary>
		/// <remarks>
		/// Read number of bytes equal to the length of the buffer, from a given
		/// position within a file. This does not
		/// change the current offset of a file, and is thread-safe.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		void ReadFully(long position, byte[] buffer);
	}
}
