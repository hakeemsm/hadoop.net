using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs.Shortcircuit;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	/// <summary>
	/// A BlockReader is responsible for reading a single block
	/// from a single datanode.
	/// </summary>
	public interface BlockReader : ByteBufferReadable
	{
		/* same interface as inputStream java.io.InputStream#read()
		* used by DFSInputStream#read()
		* This violates one rule when there is a checksum error:
		* "Read should not modify user buffer before successful read"
		* because it first reads the data to user buffer and then checks
		* the checksum.
		* Note: this must return -1 on EOF, even in the case of a 0-byte read.
		* See HDFS-5762 for details.
		*/
		/// <exception cref="System.IO.IOException"/>
		int Read(byte[] buf, int off, int len);

		/// <summary>Skip the given number of bytes</summary>
		/// <exception cref="System.IO.IOException"/>
		long Skip(long n);

		/// <summary>
		/// Returns an estimate of the number of bytes that can be read
		/// (or skipped over) from this input stream without performing
		/// network I/O.
		/// </summary>
		/// <remarks>
		/// Returns an estimate of the number of bytes that can be read
		/// (or skipped over) from this input stream without performing
		/// network I/O.
		/// This may return more than what is actually present in the block.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		int Available();

		/// <summary>Close the block reader.</summary>
		/// <exception cref="System.IO.IOException"/>
		void Close();

		/// <summary>
		/// Read exactly the given amount of data, throwing an exception
		/// if EOF is reached before that amount
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		void ReadFully(byte[] buf, int readOffset, int amtToRead);

		/// <summary>
		/// Similar to
		/// <see cref="ReadFully(byte[], int, int)"/>
		/// except that it will
		/// not throw an exception on EOF. However, it differs from the simple
		/// <see cref="Read(byte[], int, int)"/>
		/// call in that it is guaranteed to
		/// read the data if it is available. In other words, if this call
		/// does not throw an exception, then either the buffer has been
		/// filled or the next call will return EOF.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		int ReadAll(byte[] buf, int offset, int len);

		/// <returns>true only if this is a local read.</returns>
		bool IsLocal();

		/// <returns>
		/// true only if this is a short-circuit read.
		/// All short-circuit reads are also local.
		/// </returns>
		bool IsShortCircuit();

		/// <summary>Get a ClientMmap object for this BlockReader.</summary>
		/// <param name="opts">The read options to use.</param>
		/// <returns>
		/// The ClientMmap object, or null if mmap is not
		/// supported.
		/// </returns>
		ClientMmap GetClientMmap(EnumSet<ReadOption> opts);
	}
}
