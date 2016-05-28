using System;
using System.IO;
using Com.Google.Common.Annotations;
using Com.Google.Common.Base;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Hdfs.Client;
using Org.Apache.Hadoop.Nfs.Nfs3;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Nfs.Nfs3
{
	/// <summary>
	/// WriteCtx saves the context of one write request, such as request, channel,
	/// xid and reply status.
	/// </summary>
	internal class WriteCtx
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Hdfs.Nfs.Nfs3.WriteCtx
			));

		/// <summary>In memory write data has 3 states.</summary>
		/// <remarks>
		/// In memory write data has 3 states. ALLOW_DUMP: not sequential write, still
		/// wait for prerequisite writes. NO_DUMP: sequential write, no need to dump
		/// since it will be written to HDFS soon. DUMPED: already dumped to a file.
		/// </remarks>
		public enum DataState
		{
			AllowDump,
			NoDump,
			Dumped
		}

		private readonly FileHandle handle;

		private readonly long offset;

		private readonly int count;

		/// <summary>
		/// Some clients can send a write that includes previously written data along
		/// with new data.
		/// </summary>
		/// <remarks>
		/// Some clients can send a write that includes previously written data along
		/// with new data. In such case the write request is changed to write from only
		/// the new data.
		/// <c>originalCount</c>
		/// tracks the number of bytes sent in the
		/// request before it was modified to write only the new data.
		/// </remarks>
		/// <seealso cref="OpenFileCtx.AddWritesToCache(Org.Apache.Hadoop.Nfs.Nfs3.Request.WRITE3Request, Org.Jboss.Netty.Channel.Channel, int)
		/// 	">for more details</seealso>
		private readonly int originalCount;

		public const int InvalidOriginalCount = -1;

		public virtual int GetOriginalCount()
		{
			return originalCount;
		}

		private readonly Nfs3Constant.WriteStableHow stableHow;

		private volatile ByteBuffer data;

		private readonly Org.Jboss.Netty.Channel.Channel channel;

		private readonly int xid;

		private bool replied;

		/// <summary>
		/// Data belonging to the same
		/// <see cref="OpenFileCtx"/>
		/// may be dumped to a file.
		/// After being dumped to the file, the corresponding
		/// <see cref="WriteCtx"/>
		/// records
		/// the dump file and the offset.
		/// </summary>
		private RandomAccessFile raf;

		private long dumpFileOffset;

		private volatile WriteCtx.DataState dataState;

		public readonly long startTime;

		public virtual WriteCtx.DataState GetDataState()
		{
			return dataState;
		}

		public virtual void SetDataState(WriteCtx.DataState dataState)
		{
			this.dataState = dataState;
		}

		/// <summary>Writing the data into a local file.</summary>
		/// <remarks>
		/// Writing the data into a local file. After the writing, if
		/// <see cref="dataState"/>
		/// is still ALLOW_DUMP, set
		/// <see cref="data"/>
		/// to null and set
		/// <see cref="dataState"/>
		/// to DUMPED.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		internal virtual long DumpData(FileOutputStream dumpOut, RandomAccessFile raf)
		{
			if (dataState != WriteCtx.DataState.AllowDump)
			{
				if (Log.IsTraceEnabled())
				{
					Log.Trace("No need to dump with status(replied,dataState):" + "(" + replied + ","
						 + dataState + ")");
				}
				return 0;
			}
			// Resized write should not allow dump
			Preconditions.CheckState(originalCount == InvalidOriginalCount);
			this.raf = raf;
			dumpFileOffset = dumpOut.GetChannel().Position();
			dumpOut.Write(((byte[])data.Array()), 0, count);
			if (Log.IsDebugEnabled())
			{
				Log.Debug("After dump, new dumpFileOffset:" + dumpFileOffset);
			}
			// it is possible that while we dump the data, the data is also being
			// written back to HDFS. After dump, if the writing back has not finished
			// yet, we change its flag to DUMPED and set the data to null. Otherwise
			// this WriteCtx instance should have been removed from the buffer.
			if (dataState == WriteCtx.DataState.AllowDump)
			{
				lock (this)
				{
					if (dataState == WriteCtx.DataState.AllowDump)
					{
						data = null;
						dataState = WriteCtx.DataState.Dumped;
						return count;
					}
				}
			}
			return 0;
		}

		internal virtual FileHandle GetHandle()
		{
			return handle;
		}

		internal virtual long GetOffset()
		{
			return offset;
		}

		internal virtual int GetCount()
		{
			return count;
		}

		internal virtual Nfs3Constant.WriteStableHow GetStableHow()
		{
			return stableHow;
		}

		/// <exception cref="System.IO.IOException"/>
		[VisibleForTesting]
		internal virtual ByteBuffer GetData()
		{
			if (dataState != WriteCtx.DataState.Dumped)
			{
				lock (this)
				{
					if (dataState != WriteCtx.DataState.Dumped)
					{
						Preconditions.CheckState(data != null);
						return data;
					}
				}
			}
			// read back from dumped file
			this.LoadData();
			return data;
		}

		/// <exception cref="System.IO.IOException"/>
		private void LoadData()
		{
			Preconditions.CheckState(data == null);
			byte[] rawData = new byte[count];
			raf.Seek(dumpFileOffset);
			int size = raf.Read(rawData, 0, count);
			if (size != count)
			{
				throw new IOException("Data count is " + count + ", but read back " + size + "bytes"
					);
			}
			data = ByteBuffer.Wrap(rawData);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void WriteData(HdfsDataOutputStream fos)
		{
			Preconditions.CheckState(fos != null);
			ByteBuffer dataBuffer;
			try
			{
				dataBuffer = GetData();
			}
			catch (Exception e1)
			{
				Log.Error("Failed to get request data offset:" + offset + " count:" + count + " error:"
					 + e1);
				throw new IOException("Can't get WriteCtx.data");
			}
			byte[] data = ((byte[])dataBuffer.Array());
			int position = dataBuffer.Position();
			int limit = dataBuffer.Limit();
			Preconditions.CheckState(limit - position == count);
			// Modified write has a valid original count
			if (position != 0)
			{
				if (limit != GetOriginalCount())
				{
					throw new IOException("Modified write has differnt original size." + "buff position:"
						 + position + " buff limit:" + limit + ". " + ToString());
				}
			}
			// Now write data
			fos.Write(data, position, count);
		}

		internal virtual Org.Jboss.Netty.Channel.Channel GetChannel()
		{
			return channel;
		}

		internal virtual int GetXid()
		{
			return xid;
		}

		internal virtual bool GetReplied()
		{
			return replied;
		}

		internal virtual void SetReplied(bool replied)
		{
			this.replied = replied;
		}

		internal WriteCtx(FileHandle handle, long offset, int count, int originalCount, Nfs3Constant.WriteStableHow
			 stableHow, ByteBuffer data, Org.Jboss.Netty.Channel.Channel channel, int xid, bool
			 replied, WriteCtx.DataState dataState)
		{
			this.handle = handle;
			this.offset = offset;
			this.count = count;
			this.originalCount = originalCount;
			this.stableHow = stableHow;
			this.data = data;
			this.channel = channel;
			this.xid = xid;
			this.replied = replied;
			this.dataState = dataState;
			raf = null;
			this.startTime = Runtime.NanoTime();
		}

		public override string ToString()
		{
			return "Id:" + handle.GetFileId() + " offset:" + offset + " count:" + count + " originalCount:"
				 + originalCount + " stableHow:" + stableHow + " replied:" + replied + " dataState:"
				 + dataState + " xid:" + xid;
		}
	}
}
