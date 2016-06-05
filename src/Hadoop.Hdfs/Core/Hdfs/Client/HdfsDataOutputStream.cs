using System.IO;
using Com.Google.Common.Base;
using Org.Apache.Hadoop.Crypto;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Client
{
	/// <summary>
	/// The Hdfs implementation of
	/// <see cref="Org.Apache.Hadoop.FS.FSDataOutputStream"/>
	/// .
	/// </summary>
	public class HdfsDataOutputStream : FSDataOutputStream
	{
		/// <exception cref="System.IO.IOException"/>
		public HdfsDataOutputStream(DFSOutputStream @out, FileSystem.Statistics stats, long
			 startPosition)
			: base(@out, stats, startPosition)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public HdfsDataOutputStream(DFSOutputStream @out, FileSystem.Statistics stats)
			: this(@out, stats, 0L)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public HdfsDataOutputStream(CryptoOutputStream @out, FileSystem.Statistics stats, 
			long startPosition)
			: base(@out, stats, startPosition)
		{
			Preconditions.CheckArgument(@out.GetWrappedStream() is DFSOutputStream, "CryptoOutputStream should wrap a DFSOutputStream"
				);
		}

		/// <exception cref="System.IO.IOException"/>
		public HdfsDataOutputStream(CryptoOutputStream @out, FileSystem.Statistics stats)
			: this(@out, stats, 0L)
		{
		}

		/// <summary>Get the actual number of replicas of the current block.</summary>
		/// <remarks>
		/// Get the actual number of replicas of the current block.
		/// This can be different from the designated replication factor of the file
		/// because the namenode does not maintain replication for the blocks which are
		/// currently being written to. Depending on the configuration, the client may
		/// continue to write to a block even if a few datanodes in the write pipeline
		/// have failed, or the client may add a new datanodes once a datanode has
		/// failed.
		/// </remarks>
		/// <returns>the number of valid replicas of the current block</returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual int GetCurrentBlockReplication()
		{
			lock (this)
			{
				OutputStream wrappedStream = GetWrappedStream();
				if (wrappedStream is CryptoOutputStream)
				{
					wrappedStream = ((CryptoOutputStream)wrappedStream).GetWrappedStream();
				}
				return ((DFSOutputStream)wrappedStream).GetCurrentBlockReplication();
			}
		}

		/// <summary>Sync buffered data to DataNodes (flush to disk devices).</summary>
		/// <param name="syncFlags">Indicate the detailed semantic and actions of the hsync.</param>
		/// <exception cref="System.IO.IOException"/>
		/// <seealso cref="Org.Apache.Hadoop.FS.FSDataOutputStream.Hsync()"/>
		public virtual void Hsync(EnumSet<HdfsDataOutputStream.SyncFlag> syncFlags)
		{
			OutputStream wrappedStream = GetWrappedStream();
			if (wrappedStream is CryptoOutputStream)
			{
				((CryptoOutputStream)wrappedStream).Flush();
				wrappedStream = ((CryptoOutputStream)wrappedStream).GetWrappedStream();
			}
			((DFSOutputStream)wrappedStream).Hsync(syncFlags);
		}

		public enum SyncFlag
		{
			UpdateLength,
			EndBlock
		}
	}
}
