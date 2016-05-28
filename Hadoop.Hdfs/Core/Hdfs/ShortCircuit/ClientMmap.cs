using System;
using Org.Apache.Commons.Logging;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Shortcircuit
{
	/// <summary>A reference to a memory-mapped region used by an HDFS client.</summary>
	public class ClientMmap : IDisposable
	{
		internal static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Hdfs.Shortcircuit.ClientMmap
			));

		/// <summary>A reference to the block replica which this mmap relates to.</summary>
		private ShortCircuitReplica replica;

		/// <summary>The java ByteBuffer object.</summary>
		private readonly MappedByteBuffer map;

		/// <summary>
		/// Whether or not this ClientMmap anchors the replica into memory while
		/// it exists.
		/// </summary>
		/// <remarks>
		/// Whether or not this ClientMmap anchors the replica into memory while
		/// it exists.  Closing an anchored ClientMmap unanchors the replica.
		/// </remarks>
		private readonly bool anchored;

		internal ClientMmap(ShortCircuitReplica replica, MappedByteBuffer map, bool anchored
			)
		{
			this.replica = replica;
			this.map = map;
			this.anchored = anchored;
		}

		/// <summary>Close the ClientMmap object.</summary>
		public virtual void Close()
		{
			if (replica != null)
			{
				if (anchored)
				{
					replica.RemoveNoChecksumAnchor();
				}
				replica.Unref();
			}
			replica = null;
		}

		public virtual MappedByteBuffer GetMappedByteBuffer()
		{
			return map;
		}
	}
}
