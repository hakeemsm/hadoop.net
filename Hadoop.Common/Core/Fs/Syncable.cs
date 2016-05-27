using System;
using Sharpen;

namespace Org.Apache.Hadoop.FS
{
	/// <summary>This interface for flush/sync operation.</summary>
	public interface Syncable
	{
		/// <seealso cref="Hflush()"/>
		/// <exception cref="System.IO.IOException"/>
		[System.ObsoleteAttribute(@"As of HADOOP 0.21.0, replaced by hflush")]
		void Sync();

		/// <summary>Flush out the data in client's user buffer.</summary>
		/// <remarks>
		/// Flush out the data in client's user buffer. After the return of
		/// this call, new readers will see the data.
		/// </remarks>
		/// <exception cref="System.IO.IOException">if any error occurs</exception>
		void Hflush();

		/// <summary>
		/// Similar to posix fsync, flush out the data in client's user buffer
		/// all the way to the disk device (but the disk may have it in its cache).
		/// </summary>
		/// <exception cref="System.IO.IOException">if error occurs</exception>
		void Hsync();
	}
}
