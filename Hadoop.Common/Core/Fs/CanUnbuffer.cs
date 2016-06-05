

namespace Org.Apache.Hadoop.FS
{
	/// <summary>
	/// FSDataInputStreams implement this interface to indicate that they can clear
	/// their buffers on request.
	/// </summary>
	public interface CanUnbuffer
	{
		/// <summary>Reduce the buffering.</summary>
		/// <remarks>
		/// Reduce the buffering.  This will also free sockets and file descriptors
		/// held by the stream, if possible.
		/// </remarks>
		void Unbuffer();
	}
}
