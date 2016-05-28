using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Base;
using Org.Apache.Hadoop.Crypto;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Client
{
	/// <summary>
	/// The Hdfs implementation of
	/// <see cref="Org.Apache.Hadoop.FS.FSDataInputStream"/>
	/// .
	/// </summary>
	public class HdfsDataInputStream : FSDataInputStream
	{
		/// <exception cref="System.IO.IOException"/>
		public HdfsDataInputStream(DFSInputStream @in)
			: base(@in)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public HdfsDataInputStream(CryptoInputStream @in)
			: base(@in)
		{
			Preconditions.CheckArgument(@in.GetWrappedStream() is DFSInputStream, "CryptoInputStream should wrap a DFSInputStream"
				);
		}

		private DFSInputStream GetDFSInputStream()
		{
			if (@in is CryptoInputStream)
			{
				return (DFSInputStream)((CryptoInputStream)@in).GetWrappedStream();
			}
			return (DFSInputStream)@in;
		}

		/// <summary>Get a reference to the wrapped output stream.</summary>
		/// <remarks>
		/// Get a reference to the wrapped output stream. We always want to return the
		/// actual underlying InputStream, even when we're using a CryptoStream. e.g.
		/// in the delegated methods below.
		/// </remarks>
		/// <returns>the underlying output stream</returns>
		public override InputStream GetWrappedStream()
		{
			return @in;
		}

		/// <summary>Get the datanode from which the stream is currently reading.</summary>
		public virtual DatanodeInfo GetCurrentDatanode()
		{
			return GetDFSInputStream().GetCurrentDatanode();
		}

		/// <summary>Get the block containing the target position.</summary>
		public virtual ExtendedBlock GetCurrentBlock()
		{
			return GetDFSInputStream().GetCurrentBlock();
		}

		/// <summary>Get the collection of blocks that has already been located.</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual IList<LocatedBlock> GetAllBlocks()
		{
			return GetDFSInputStream().GetAllBlocks();
		}

		/// <summary>Get the visible length of the file.</summary>
		/// <remarks>
		/// Get the visible length of the file. It will include the length of the last
		/// block even if that is in UnderConstruction state.
		/// </remarks>
		/// <returns>The visible length of the file.</returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual long GetVisibleLength()
		{
			return GetDFSInputStream().GetFileLength();
		}

		/// <summary>Get statistics about the reads which this DFSInputStream has done.</summary>
		/// <remarks>
		/// Get statistics about the reads which this DFSInputStream has done.
		/// Note that because HdfsDataInputStream is buffered, these stats may
		/// be higher than you would expect just by adding up the number of
		/// bytes read through HdfsDataInputStream.
		/// </remarks>
		public virtual DFSInputStream.ReadStatistics GetReadStatistics()
		{
			return GetDFSInputStream().GetReadStatistics();
		}

		public virtual void ClearReadStatistics()
		{
			GetDFSInputStream().ClearReadStatistics();
		}
	}
}
