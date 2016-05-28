using System;
using System.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.IO.Nativeio;
using Org.Jboss.Netty.Handler.Stream;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	public class FadvisedChunkedFile : ChunkedFile
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Mapred.FadvisedChunkedFile
			));

		private readonly bool manageOsCache;

		private readonly int readaheadLength;

		private readonly ReadaheadPool readaheadPool;

		private readonly FileDescriptor fd;

		private readonly string identifier;

		private ReadaheadPool.ReadaheadRequest readaheadRequest;

		/// <exception cref="System.IO.IOException"/>
		public FadvisedChunkedFile(RandomAccessFile file, long position, long count, int 
			chunkSize, bool manageOsCache, int readaheadLength, ReadaheadPool readaheadPool, 
			string identifier)
			: base(file, position, count, chunkSize)
		{
			this.manageOsCache = manageOsCache;
			this.readaheadLength = readaheadLength;
			this.readaheadPool = readaheadPool;
			this.fd = file.GetFD();
			this.identifier = identifier;
		}

		/// <exception cref="System.Exception"/>
		public override object NextChunk()
		{
			if (manageOsCache && readaheadPool != null)
			{
				readaheadRequest = readaheadPool.ReadaheadStream(identifier, fd, GetCurrentOffset
					(), readaheadLength, GetEndOffset(), readaheadRequest);
			}
			return base.NextChunk();
		}

		/// <exception cref="System.Exception"/>
		public override void Close()
		{
			if (readaheadRequest != null)
			{
				readaheadRequest.Cancel();
			}
			if (manageOsCache && GetEndOffset() - GetStartOffset() > 0)
			{
				try
				{
					NativeIO.POSIX.GetCacheManipulator().PosixFadviseIfPossible(identifier, fd, GetStartOffset
						(), GetEndOffset() - GetStartOffset(), NativeIO.POSIX.PosixFadvDontneed);
				}
				catch (Exception t)
				{
					Log.Warn("Failed to manage OS cache for " + identifier, t);
				}
			}
			base.Close();
		}
	}
}
