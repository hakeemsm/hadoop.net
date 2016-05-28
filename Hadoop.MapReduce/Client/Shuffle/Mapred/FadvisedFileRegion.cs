using System;
using System.IO;
using Com.Google.Common.Annotations;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.IO.Nativeio;
using Org.Jboss.Netty.Channel;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	public class FadvisedFileRegion : DefaultFileRegion
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Mapred.FadvisedFileRegion
			));

		private readonly bool manageOsCache;

		private readonly int readaheadLength;

		private readonly ReadaheadPool readaheadPool;

		private readonly FileDescriptor fd;

		private readonly string identifier;

		private readonly long count;

		private readonly long position;

		private readonly int shuffleBufferSize;

		private readonly bool shuffleTransferToAllowed;

		private readonly FileChannel fileChannel;

		private ReadaheadPool.ReadaheadRequest readaheadRequest;

		/// <exception cref="System.IO.IOException"/>
		public FadvisedFileRegion(RandomAccessFile file, long position, long count, bool 
			manageOsCache, int readaheadLength, ReadaheadPool readaheadPool, string identifier
			, int shuffleBufferSize, bool shuffleTransferToAllowed)
			: base(file.GetChannel(), position, count)
		{
			this.manageOsCache = manageOsCache;
			this.readaheadLength = readaheadLength;
			this.readaheadPool = readaheadPool;
			this.fd = file.GetFD();
			this.identifier = identifier;
			this.fileChannel = file.GetChannel();
			this.count = count;
			this.position = position;
			this.shuffleBufferSize = shuffleBufferSize;
			this.shuffleTransferToAllowed = shuffleTransferToAllowed;
		}

		/// <exception cref="System.IO.IOException"/>
		public override long TransferTo(WritableByteChannel target, long position)
		{
			if (manageOsCache && readaheadPool != null)
			{
				readaheadRequest = readaheadPool.ReadaheadStream(identifier, fd, GetPosition() + 
					position, readaheadLength, GetPosition() + GetCount(), readaheadRequest);
			}
			if (this.shuffleTransferToAllowed)
			{
				return base.TransferTo(target, position);
			}
			else
			{
				return CustomShuffleTransfer(target, position);
			}
		}

		/// <summary>This method transfers data using local buffer.</summary>
		/// <remarks>
		/// This method transfers data using local buffer. It transfers data from
		/// a disk to a local buffer in memory, and then it transfers data from the
		/// buffer to the target. This is used only if transferTo is disallowed in
		/// the configuration file. super.TransferTo does not perform well on Windows
		/// due to a small IO request generated. customShuffleTransfer can control
		/// the size of the IO requests by changing the size of the intermediate
		/// buffer.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		[VisibleForTesting]
		internal virtual long CustomShuffleTransfer(WritableByteChannel target, long position
			)
		{
			long actualCount = this.count - position;
			if (actualCount < 0 || position < 0)
			{
				throw new ArgumentException("position out of range: " + position + " (expected: 0 - "
					 + (this.count - 1) + ')');
			}
			if (actualCount == 0)
			{
				return 0L;
			}
			long trans = actualCount;
			int readSize;
			ByteBuffer byteBuffer = ByteBuffer.Allocate(this.shuffleBufferSize);
			while (trans > 0L && (readSize = fileChannel.Read(byteBuffer, this.position + position
				)) > 0)
			{
				//adjust counters and buffer limit
				if (readSize < trans)
				{
					trans -= readSize;
					position += readSize;
					byteBuffer.Flip();
				}
				else
				{
					//We can read more than we need if the actualCount is not multiple 
					//of the byteBuffer size and file is big enough. In that case we cannot
					//use flip method but we need to set buffer limit manually to trans.
					byteBuffer.Limit((int)trans);
					byteBuffer.Position(0);
					position += trans;
					trans = 0;
				}
				//write data to the target
				while (byteBuffer.HasRemaining())
				{
					target.Write(byteBuffer);
				}
				byteBuffer.Clear();
			}
			return actualCount - trans;
		}

		public override void ReleaseExternalResources()
		{
			if (readaheadRequest != null)
			{
				readaheadRequest.Cancel();
			}
			base.ReleaseExternalResources();
		}

		/// <summary>
		/// Call when the transfer completes successfully so we can advise the OS that
		/// we don't need the region to be cached anymore.
		/// </summary>
		public virtual void TransferSuccessful()
		{
			if (manageOsCache && GetCount() > 0)
			{
				try
				{
					NativeIO.POSIX.GetCacheManipulator().PosixFadviseIfPossible(identifier, fd, GetPosition
						(), GetCount(), NativeIO.POSIX.PosixFadvDontneed);
				}
				catch (Exception t)
				{
					Log.Warn("Failed to manage OS cache for " + identifier, t);
				}
			}
		}
	}
}
