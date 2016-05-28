using System;
using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Annotations;
using Com.Google.Common.Base;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Client;
using Org.Apache.Hadoop.Hdfs.Nfs.Conf;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Nfs.Nfs3;
using Org.Apache.Hadoop.Nfs.Nfs3.Request;
using Org.Apache.Hadoop.Nfs.Nfs3.Response;
using Org.Apache.Hadoop.Oncrpc;
using Org.Apache.Hadoop.Oncrpc.Security;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Nfs.Nfs3
{
	/// <summary>OpenFileCtx saves the context of one HDFS file output stream.</summary>
	/// <remarks>
	/// OpenFileCtx saves the context of one HDFS file output stream. Access to it is
	/// synchronized by its member lock.
	/// </remarks>
	internal class OpenFileCtx
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Hdfs.Nfs.Nfs3.OpenFileCtx
			));

		private static long DumpWriteWaterMark = 1024 * 1024;

		internal enum COMMIT_STATUS
		{
			CommitFinished,
			CommitWait,
			CommitInactiveCtx,
			CommitInactiveWithPendingWrite,
			CommitError,
			CommitDoSync,
			CommitSpecialWait,
			CommitSpecialSuccess
		}

		private readonly DFSClient client;

		private readonly IdMappingServiceProvider iug;

		private volatile bool activeState;

		private volatile bool asyncStatus;

		private volatile long asyncWriteBackStartOffset;

		/// <summary>The current offset of the file in HDFS.</summary>
		/// <remarks>
		/// The current offset of the file in HDFS. All the content before this offset
		/// has been written back to HDFS.
		/// </remarks>
		private AtomicLong nextOffset;

		private readonly HdfsDataOutputStream fos;

		private readonly bool aixCompatMode;

		private Nfs3FileAttributes latestAttr;

		private readonly ConcurrentNavigableMap<OffsetRange, WriteCtx> pendingWrites;

		private readonly ConcurrentNavigableMap<long, OpenFileCtx.CommitCtx> pendingCommits;

		internal class CommitCtx
		{
			private readonly long offset;

			private readonly Org.Jboss.Netty.Channel.Channel channel;

			private readonly int xid;

			private readonly Nfs3FileAttributes preOpAttr;

			public readonly long startTime;

			// Pending writes water mark for dump, 1MB
			// scoped pending writes is sequential
			// scoped pending writes is not sequential 
			// The stream status. False means the stream is closed.
			// The stream write-back status. True means one thread is doing write back.
			// It's updated after each sync to HDFS
			internal virtual long GetOffset()
			{
				return offset;
			}

			internal virtual Org.Jboss.Netty.Channel.Channel GetChannel()
			{
				return channel;
			}

			internal virtual int GetXid()
			{
				return xid;
			}

			internal virtual Nfs3FileAttributes GetPreOpAttr()
			{
				return preOpAttr;
			}

			internal virtual long GetStartTime()
			{
				return startTime;
			}

			internal CommitCtx(long offset, Org.Jboss.Netty.Channel.Channel channel, int xid, 
				Nfs3FileAttributes preOpAttr)
			{
				this.offset = offset;
				this.channel = channel;
				this.xid = xid;
				this.preOpAttr = preOpAttr;
				this.startTime = Runtime.NanoTime();
			}

			public override string ToString()
			{
				return string.Format("offset: %d xid: %d startTime: %d", offset, xid, startTime);
			}
		}

		private long lastAccessTime;

		private volatile bool enabledDump;

		private FileOutputStream dumpOut;

		/// <summary>Tracks the data buffered in memory related to non sequential writes</summary>
		private AtomicLong nonSequentialWriteInMemory;

		private RandomAccessFile raf;

		private readonly string dumpFilePath;

		private Daemon dumpThread;

		private readonly bool uploadLargeFile;

		// The last write, commit request or write-back event. Updating time to keep
		// output steam alive.
		private void UpdateLastAccessTime()
		{
			lastAccessTime = Time.MonotonicNow();
		}

		private bool CheckStreamTimeout(long streamTimeout)
		{
			return Time.MonotonicNow() - lastAccessTime > streamTimeout;
		}

		internal virtual long GetLastAccessTime()
		{
			return lastAccessTime;
		}

		public virtual long GetNextOffset()
		{
			return nextOffset.Get();
		}

		internal virtual bool GetActiveState()
		{
			return this.activeState;
		}

		internal virtual bool HasPendingWork()
		{
			return (pendingWrites.Count != 0 || pendingCommits.Count != 0);
		}

		/// <summary>Increase or decrease the memory occupation of non-sequential writes</summary>
		private long UpdateNonSequentialWriteInMemory(long count)
		{
			long newValue = nonSequentialWriteInMemory.AddAndGet(count);
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Update nonSequentialWriteInMemory by " + count + " new value: " + newValue
					);
			}
			Preconditions.CheckState(newValue >= 0, "nonSequentialWriteInMemory is negative "
				 + newValue + " after update with count " + count);
			return newValue;
		}

		internal OpenFileCtx(HdfsDataOutputStream fos, Nfs3FileAttributes latestAttr, string
			 dumpFilePath, DFSClient client, IdMappingServiceProvider iug)
			: this(fos, latestAttr, dumpFilePath, client, iug, false, new NfsConfiguration())
		{
		}

		internal OpenFileCtx(HdfsDataOutputStream fos, Nfs3FileAttributes latestAttr, string
			 dumpFilePath, DFSClient client, IdMappingServiceProvider iug, bool aixCompatMode
			, NfsConfiguration config)
		{
			this.fos = fos;
			this.latestAttr = latestAttr;
			this.aixCompatMode = aixCompatMode;
			// We use the ReverseComparatorOnMin as the comparator of the map. In this
			// way, we first dump the data with larger offset. In the meanwhile, we
			// retrieve the last element to write back to HDFS.
			pendingWrites = new ConcurrentSkipListMap<OffsetRange, WriteCtx>(OffsetRange.ReverseComparatorOnMin
				);
			pendingCommits = new ConcurrentSkipListMap<long, OpenFileCtx.CommitCtx>();
			UpdateLastAccessTime();
			activeState = true;
			asyncStatus = false;
			asyncWriteBackStartOffset = 0;
			dumpOut = null;
			raf = null;
			nonSequentialWriteInMemory = new AtomicLong(0);
			this.dumpFilePath = dumpFilePath;
			enabledDump = dumpFilePath != null;
			nextOffset = new AtomicLong();
			nextOffset.Set(latestAttr.GetSize());
			try
			{
				System.Diagnostics.Debug.Assert((nextOffset.Get() == this.fos.GetPos()));
			}
			catch (IOException)
			{
			}
			dumpThread = null;
			this.client = client;
			this.iug = iug;
			this.uploadLargeFile = config.GetBoolean(NfsConfigKeys.LargeFileUpload, NfsConfigKeys
				.LargeFileUploadDefault);
		}

		public virtual Nfs3FileAttributes GetLatestAttr()
		{
			return latestAttr;
		}

		// Get flushed offset. Note that flushed data may not be persisted.
		/// <exception cref="System.IO.IOException"/>
		private long GetFlushedOffset()
		{
			return fos.GetPos();
		}

		// Check if need to dump the new writes
		private void WaitForDump()
		{
			if (!enabledDump)
			{
				if (Log.IsDebugEnabled())
				{
					Log.Debug("Do nothing, dump is disabled.");
				}
				return;
			}
			if (nonSequentialWriteInMemory.Get() < DumpWriteWaterMark)
			{
				return;
			}
			// wake up the dumper thread to dump the data
			lock (this)
			{
				if (nonSequentialWriteInMemory.Get() >= DumpWriteWaterMark)
				{
					if (Log.IsDebugEnabled())
					{
						Log.Debug("Asking dumper to dump...");
					}
					if (dumpThread == null)
					{
						dumpThread = new Daemon(new OpenFileCtx.Dumper(this));
						dumpThread.Start();
					}
					else
					{
						Sharpen.Runtime.NotifyAll(this);
					}
				}
				while (nonSequentialWriteInMemory.Get() >= DumpWriteWaterMark)
				{
					try
					{
						Sharpen.Runtime.Wait(this);
					}
					catch (Exception)
					{
					}
				}
			}
		}

		internal class Dumper : Runnable
		{
			/// <summary>Dump data into a file</summary>
			private void Dump()
			{
				// Create dump outputstream for the first time
				if (this._enclosing.dumpOut == null)
				{
					OpenFileCtx.Log.Info("Create dump file: " + this._enclosing.dumpFilePath);
					FilePath dumpFile = new FilePath(this._enclosing.dumpFilePath);
					try
					{
						lock (this)
						{
							// check if alive again
							Preconditions.CheckState(dumpFile.CreateNewFile(), "The dump file should not exist: %s"
								, this._enclosing.dumpFilePath);
							this._enclosing.dumpOut = new FileOutputStream(dumpFile);
						}
					}
					catch (IOException e)
					{
						OpenFileCtx.Log.Error("Got failure when creating dump stream " + this._enclosing.
							dumpFilePath, e);
						this._enclosing.enabledDump = false;
						if (this._enclosing.dumpOut != null)
						{
							try
							{
								this._enclosing.dumpOut.Close();
							}
							catch (IOException)
							{
								OpenFileCtx.Log.Error("Can't close dump stream " + this._enclosing.dumpFilePath, 
									e);
							}
						}
						return;
					}
				}
				// Get raf for the first dump
				if (this._enclosing.raf == null)
				{
					try
					{
						this._enclosing.raf = new RandomAccessFile(this._enclosing.dumpFilePath, "r");
					}
					catch (FileNotFoundException)
					{
						OpenFileCtx.Log.Error("Can't get random access to file " + this._enclosing.dumpFilePath
							);
						// Disable dump
						this._enclosing.enabledDump = false;
						return;
					}
				}
				if (OpenFileCtx.Log.IsDebugEnabled())
				{
					OpenFileCtx.Log.Debug("Start dump. Before dump, nonSequentialWriteInMemory == " +
						 this._enclosing.nonSequentialWriteInMemory.Get());
				}
				IEnumerator<OffsetRange> it = ((NavigableSet<OffsetRange>)this._enclosing.pendingWrites
					.Keys).GetEnumerator();
				while (this._enclosing.activeState && it.HasNext() && this._enclosing.nonSequentialWriteInMemory
					.Get() > 0)
				{
					OffsetRange key = it.Next();
					WriteCtx writeCtx = this._enclosing.pendingWrites[key];
					if (writeCtx == null)
					{
						// This write was just deleted
						continue;
					}
					try
					{
						long dumpedDataSize = writeCtx.DumpData(this._enclosing.dumpOut, this._enclosing.
							raf);
						if (dumpedDataSize > 0)
						{
							this._enclosing.UpdateNonSequentialWriteInMemory(-dumpedDataSize);
						}
					}
					catch (IOException e)
					{
						OpenFileCtx.Log.Error("Dump data failed: " + writeCtx + " with error: " + e + " OpenFileCtx state: "
							 + this._enclosing.activeState);
						// Disable dump
						this._enclosing.enabledDump = false;
						return;
					}
				}
				if (OpenFileCtx.Log.IsDebugEnabled())
				{
					OpenFileCtx.Log.Debug("After dump, nonSequentialWriteInMemory == " + this._enclosing
						.nonSequentialWriteInMemory.Get());
				}
			}

			public virtual void Run()
			{
				while (this._enclosing.activeState && this._enclosing.enabledDump)
				{
					try
					{
						if (this._enclosing.nonSequentialWriteInMemory.Get() >= OpenFileCtx.DumpWriteWaterMark)
						{
							this.Dump();
						}
						lock (this._enclosing)
						{
							if (this._enclosing.nonSequentialWriteInMemory.Get() < OpenFileCtx.DumpWriteWaterMark)
							{
								Sharpen.Runtime.NotifyAll(this._enclosing);
								try
								{
									Sharpen.Runtime.Wait(this._enclosing);
									if (OpenFileCtx.Log.IsDebugEnabled())
									{
										OpenFileCtx.Log.Debug("Dumper woke up");
									}
								}
								catch (Exception)
								{
									OpenFileCtx.Log.Info("Dumper is interrupted, dumpFilePath= " + this._enclosing.dumpFilePath
										);
								}
							}
						}
						if (OpenFileCtx.Log.IsDebugEnabled())
						{
							OpenFileCtx.Log.Debug("Dumper checking OpenFileCtx activeState: " + this._enclosing
								.activeState + " enabledDump: " + this._enclosing.enabledDump);
						}
					}
					catch (Exception t)
					{
						// unblock threads with new request
						lock (this._enclosing)
						{
							Sharpen.Runtime.NotifyAll(this._enclosing);
						}
						OpenFileCtx.Log.Info("Dumper get Throwable: " + t + ". dumpFilePath: " + this._enclosing
							.dumpFilePath, t);
						this._enclosing.activeState = false;
					}
				}
			}

			internal Dumper(OpenFileCtx _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly OpenFileCtx _enclosing;
		}

		private WriteCtx CheckRepeatedWriteRequest(WRITE3Request request, Org.Jboss.Netty.Channel.Channel
			 channel, int xid)
		{
			OffsetRange range = new OffsetRange(request.GetOffset(), request.GetOffset() + request
				.GetCount());
			WriteCtx writeCtx = pendingWrites[range];
			if (writeCtx == null)
			{
				return null;
			}
			else
			{
				if (xid != writeCtx.GetXid())
				{
					Log.Warn("Got a repeated request, same range, with a different xid: " + xid + " xid in old request: "
						 + writeCtx.GetXid());
				}
				//TODO: better handling.
				return writeCtx;
			}
		}

		public virtual void ReceivedNewWrite(DFSClient dfsClient, WRITE3Request request, 
			Org.Jboss.Netty.Channel.Channel channel, int xid, AsyncDataService asyncDataService
			, IdMappingServiceProvider iug)
		{
			if (!activeState)
			{
				Log.Info("OpenFileCtx is inactive, fileId: " + request.GetHandle().GetFileId());
				WccData fileWcc = new WccData(latestAttr.GetWccAttr(), latestAttr);
				WRITE3Response response = new WRITE3Response(Nfs3Status.Nfs3errIo, fileWcc, 0, request
					.GetStableHow(), Nfs3Constant.WriteCommitVerf);
				Nfs3Utils.WriteChannel(channel, response.Serialize(new XDR(), xid, new VerifierNone
					()), xid);
			}
			else
			{
				// Update the write time first
				UpdateLastAccessTime();
				// Handle repeated write requests (same xid or not).
				// If already replied, send reply again. If not replied, drop the
				// repeated request.
				WriteCtx existantWriteCtx = CheckRepeatedWriteRequest(request, channel, xid);
				if (existantWriteCtx != null)
				{
					if (!existantWriteCtx.GetReplied())
					{
						if (Log.IsDebugEnabled())
						{
							Log.Debug("Repeated write request which hasn't been served: xid=" + xid + ", drop it."
								);
						}
					}
					else
					{
						if (Log.IsDebugEnabled())
						{
							Log.Debug("Repeated write request which is already served: xid=" + xid + ", resend response."
								);
						}
						WccData fileWcc = new WccData(latestAttr.GetWccAttr(), latestAttr);
						WRITE3Response response = new WRITE3Response(Nfs3Status.Nfs3Ok, fileWcc, request.
							GetCount(), request.GetStableHow(), Nfs3Constant.WriteCommitVerf);
						Nfs3Utils.WriteChannel(channel, response.Serialize(new XDR(), xid, new VerifierNone
							()), xid);
					}
				}
				else
				{
					// not a repeated write request
					ReceivedNewWriteInternal(dfsClient, request, channel, xid, asyncDataService, iug);
				}
			}
		}

		[VisibleForTesting]
		public static void AlterWriteRequest(WRITE3Request request, long cachedOffset)
		{
			long offset = request.GetOffset();
			int count = request.GetCount();
			long smallerCount = offset + count - cachedOffset;
			if (Log.IsDebugEnabled())
			{
				Log.Debug(string.Format("Got overwrite with appended data (%d-%d)," + " current offset %d,"
					 + " drop the overlapped section (%d-%d)" + " and append new data (%d-%d).", offset
					, (offset + count - 1), cachedOffset, offset, (cachedOffset - 1), cachedOffset, 
					(offset + count - 1)));
			}
			ByteBuffer data = request.GetData();
			Preconditions.CheckState(data.Position() == 0, "The write request data has non-zero position"
				);
			data.Position((int)(cachedOffset - offset));
			Preconditions.CheckState(data.Limit() - data.Position() == smallerCount, "The write request buffer has wrong limit/position regarding count"
				);
			request.SetOffset(cachedOffset);
			request.SetCount((int)smallerCount);
		}

		/// <summary>Creates and adds a WriteCtx into the pendingWrites map.</summary>
		/// <remarks>
		/// Creates and adds a WriteCtx into the pendingWrites map. This is a
		/// synchronized method to handle concurrent writes.
		/// </remarks>
		/// <returns>
		/// A non-null
		/// <see cref="WriteCtx"/>
		/// instance if the incoming write
		/// request's offset &gt;= nextOffset. Otherwise null.
		/// </returns>
		private WriteCtx AddWritesToCache(WRITE3Request request, Org.Jboss.Netty.Channel.Channel
			 channel, int xid)
		{
			lock (this)
			{
				long offset = request.GetOffset();
				int count = request.GetCount();
				long cachedOffset = nextOffset.Get();
				int originalCount = WriteCtx.InvalidOriginalCount;
				if (Log.IsDebugEnabled())
				{
					Log.Debug("requested offset=" + offset + " and current offset=" + cachedOffset);
				}
				// Handle a special case first
				if ((offset < cachedOffset) && (offset + count > cachedOffset))
				{
					// One Linux client behavior: after a file is closed and reopened to
					// write, the client sometimes combines previous written data(could still
					// be in kernel buffer) with newly appended data in one write. This is
					// usually the first write after file reopened. In this
					// case, we log the event and drop the overlapped section.
					Log.Warn(string.Format("Got overwrite with appended data (%d-%d)," + " current offset %d,"
						 + " drop the overlapped section (%d-%d)" + " and append new data (%d-%d).", offset
						, (offset + count - 1), cachedOffset, offset, (cachedOffset - 1), cachedOffset, 
						(offset + count - 1)));
					if (!pendingWrites.IsEmpty())
					{
						Log.Warn("There are other pending writes, fail this jumbo write");
						return null;
					}
					Log.Warn("Modify this write to write only the appended data");
					AlterWriteRequest(request, cachedOffset);
					// Update local variable
					originalCount = count;
					offset = request.GetOffset();
					count = request.GetCount();
				}
				// Fail non-append call
				if (offset < cachedOffset)
				{
					Log.Warn("(offset,count,nextOffset): " + "(" + offset + "," + count + "," + nextOffset
						 + ")");
					return null;
				}
				else
				{
					WriteCtx.DataState dataState = offset == cachedOffset ? WriteCtx.DataState.NoDump
						 : WriteCtx.DataState.AllowDump;
					WriteCtx writeCtx = new WriteCtx(request.GetHandle(), request.GetOffset(), request
						.GetCount(), originalCount, request.GetStableHow(), request.GetData(), channel, 
						xid, false, dataState);
					if (Log.IsDebugEnabled())
					{
						Log.Debug("Add new write to the list with nextOffset " + cachedOffset + " and requested offset="
							 + offset);
					}
					if (writeCtx.GetDataState() == WriteCtx.DataState.AllowDump)
					{
						// update the memory size
						UpdateNonSequentialWriteInMemory(count);
					}
					// check if there is a WriteCtx with the same range in pendingWrites
					WriteCtx oldWriteCtx = CheckRepeatedWriteRequest(request, channel, xid);
					if (oldWriteCtx == null)
					{
						pendingWrites[new OffsetRange(offset, offset + count)] = writeCtx;
						if (Log.IsDebugEnabled())
						{
							Log.Debug("New write buffered with xid " + xid + " nextOffset " + cachedOffset + 
								" req offset=" + offset + " mapsize=" + pendingWrites.Count);
						}
					}
					else
					{
						Log.Warn("Got a repeated request, same range, with xid: " + xid + " nextOffset " 
							+ +cachedOffset + " req offset=" + offset);
					}
					return writeCtx;
				}
			}
		}

		/// <summary>Process an overwrite write request</summary>
		private void ProcessOverWrite(DFSClient dfsClient, WRITE3Request request, Org.Jboss.Netty.Channel.Channel
			 channel, int xid, IdMappingServiceProvider iug)
		{
			WccData wccData = new WccData(latestAttr.GetWccAttr(), null);
			long offset = request.GetOffset();
			int count = request.GetCount();
			Nfs3Constant.WriteStableHow stableHow = request.GetStableHow();
			WRITE3Response response;
			long cachedOffset = nextOffset.Get();
			if (offset + count > cachedOffset)
			{
				Log.Warn("Treat this jumbo write as a real random write, no support.");
				response = new WRITE3Response(Nfs3Status.Nfs3errInval, wccData, 0, Nfs3Constant.WriteStableHow
					.Unstable, Nfs3Constant.WriteCommitVerf);
			}
			else
			{
				if (Log.IsDebugEnabled())
				{
					Log.Debug("Process perfectOverWrite");
				}
				// TODO: let executor handle perfect overwrite
				response = ProcessPerfectOverWrite(dfsClient, offset, count, stableHow, ((byte[])
					request.GetData().Array()), Nfs3Utils.GetFileIdPath(request.GetHandle()), wccData
					, iug);
			}
			UpdateLastAccessTime();
			Nfs3Utils.WriteChannel(channel, response.Serialize(new XDR(), xid, new VerifierNone
				()), xid);
		}

		/// <summary>Check if we can start the write (back to HDFS) now.</summary>
		/// <remarks>
		/// Check if we can start the write (back to HDFS) now. If there is no hole for
		/// writing, and there is no other threads writing (i.e., asyncStatus is
		/// false), start the writing and set asyncStatus to true.
		/// </remarks>
		/// <returns>
		/// True if the new write is sequential and we can start writing
		/// (including the case that there is already a thread writing).
		/// </returns>
		private bool CheckAndStartWrite(AsyncDataService asyncDataService, WriteCtx writeCtx
			)
		{
			lock (this)
			{
				if (writeCtx.GetOffset() == nextOffset.Get())
				{
					if (!asyncStatus)
					{
						if (Log.IsDebugEnabled())
						{
							Log.Debug("Trigger the write back task. Current nextOffset: " + nextOffset.Get());
						}
						asyncStatus = true;
						asyncWriteBackStartOffset = writeCtx.GetOffset();
						asyncDataService.Execute(new AsyncDataService.WriteBackTask(this));
					}
					else
					{
						if (Log.IsDebugEnabled())
						{
							Log.Debug("The write back thread is working.");
						}
					}
					return true;
				}
				else
				{
					return false;
				}
			}
		}

		private void ReceivedNewWriteInternal(DFSClient dfsClient, WRITE3Request request, 
			Org.Jboss.Netty.Channel.Channel channel, int xid, AsyncDataService asyncDataService
			, IdMappingServiceProvider iug)
		{
			Nfs3Constant.WriteStableHow stableHow = request.GetStableHow();
			WccAttr preOpAttr = latestAttr.GetWccAttr();
			int count = request.GetCount();
			WriteCtx writeCtx = AddWritesToCache(request, channel, xid);
			if (writeCtx == null)
			{
				// offset < nextOffset
				ProcessOverWrite(dfsClient, request, channel, xid, iug);
			}
			else
			{
				// The write is added to pendingWrites.
				// Check and start writing back if necessary
				bool startWriting = CheckAndStartWrite(asyncDataService, writeCtx);
				if (!startWriting)
				{
					// offset > nextOffset. check if we need to dump data
					WaitForDump();
					// In test, noticed some Linux client sends a batch (e.g., 1MB)
					// of reordered writes and won't send more writes until it gets
					// responses of the previous batch. So here send response immediately
					// for unstable non-sequential write
					if (stableHow != Nfs3Constant.WriteStableHow.Unstable)
					{
						Log.Info("Have to change stable write to unstable write: " + request.GetStableHow
							());
						stableHow = Nfs3Constant.WriteStableHow.Unstable;
					}
					if (Log.IsDebugEnabled())
					{
						Log.Debug("UNSTABLE write request, send response for offset: " + writeCtx.GetOffset
							());
					}
					WccData fileWcc = new WccData(preOpAttr, latestAttr);
					WRITE3Response response = new WRITE3Response(Nfs3Status.Nfs3Ok, fileWcc, count, stableHow
						, Nfs3Constant.WriteCommitVerf);
					RpcProgramNfs3.metrics.AddWrite(Nfs3Utils.GetElapsedTime(writeCtx.startTime));
					Nfs3Utils.WriteChannel(channel, response.Serialize(new XDR(), xid, new VerifierNone
						()), xid);
					writeCtx.SetReplied(true);
				}
			}
		}

		/// <summary>Honor 2 kinds of overwrites: 1).</summary>
		/// <remarks>
		/// Honor 2 kinds of overwrites: 1). support some application like touch(write
		/// the same content back to change mtime), 2) client somehow sends the same
		/// write again in a different RPC.
		/// </remarks>
		private WRITE3Response ProcessPerfectOverWrite(DFSClient dfsClient, long offset, 
			int count, Nfs3Constant.WriteStableHow stableHow, byte[] data, string path, WccData
			 wccData, IdMappingServiceProvider iug)
		{
			WRITE3Response response;
			// Read the content back
			byte[] readbuffer = new byte[count];
			int readCount = 0;
			FSDataInputStream fis = null;
			try
			{
				// Sync file data and length to avoid partial read failure
				fos.Hsync(EnumSet.Of(HdfsDataOutputStream.SyncFlag.UpdateLength));
			}
			catch (ClosedChannelException)
			{
				Log.Info("The FSDataOutputStream has been closed. " + "Continue processing the perfect overwrite."
					);
			}
			catch (IOException e)
			{
				Log.Info("hsync failed when processing possible perfect overwrite, path=" + path 
					+ " error: " + e);
				return new WRITE3Response(Nfs3Status.Nfs3errIo, wccData, 0, stableHow, Nfs3Constant
					.WriteCommitVerf);
			}
			try
			{
				fis = dfsClient.CreateWrappedInputStream(dfsClient.Open(path));
				readCount = fis.Read(offset, readbuffer, 0, count);
				if (readCount < count)
				{
					Log.Error("Can't read back " + count + " bytes, partial read size: " + readCount);
					return new WRITE3Response(Nfs3Status.Nfs3errIo, wccData, 0, stableHow, Nfs3Constant
						.WriteCommitVerf);
				}
			}
			catch (IOException e)
			{
				Log.Info("Read failed when processing possible perfect overwrite, path=" + path, 
					e);
				return new WRITE3Response(Nfs3Status.Nfs3errIo, wccData, 0, stableHow, Nfs3Constant
					.WriteCommitVerf);
			}
			finally
			{
				IOUtils.Cleanup(Log, fis);
			}
			// Compare with the request
			BytesWritable.Comparator comparator = new BytesWritable.Comparator();
			if (comparator.Compare(readbuffer, 0, readCount, data, 0, count) != 0)
			{
				Log.Info("Perfect overwrite has different content");
				response = new WRITE3Response(Nfs3Status.Nfs3errInval, wccData, 0, stableHow, Nfs3Constant
					.WriteCommitVerf);
			}
			else
			{
				Log.Info("Perfect overwrite has same content," + " updating the mtime, then return success"
					);
				Nfs3FileAttributes postOpAttr = null;
				try
				{
					dfsClient.SetTimes(path, Time.MonotonicNow(), -1);
					postOpAttr = Nfs3Utils.GetFileAttr(dfsClient, path, iug);
				}
				catch (IOException e)
				{
					Log.Info("Got error when processing perfect overwrite, path=" + path + " error: "
						 + e);
					return new WRITE3Response(Nfs3Status.Nfs3errIo, wccData, 0, stableHow, Nfs3Constant
						.WriteCommitVerf);
				}
				wccData.SetPostOpAttr(postOpAttr);
				response = new WRITE3Response(Nfs3Status.Nfs3Ok, wccData, count, stableHow, Nfs3Constant
					.WriteCommitVerf);
			}
			return response;
		}

		/// <summary>Check the commit status with the given offset</summary>
		/// <param name="commitOffset">the offset to commit</param>
		/// <param name="channel">the channel to return response</param>
		/// <param name="xid">the xid of the commit request</param>
		/// <param name="preOpAttr">the preOp attribute</param>
		/// <param name="fromRead">whether the commit is triggered from read request</param>
		/// <returns>
		/// one commit status: COMMIT_FINISHED, COMMIT_WAIT,
		/// COMMIT_INACTIVE_CTX, COMMIT_INACTIVE_WITH_PENDING_WRITE, COMMIT_ERROR
		/// </returns>
		public virtual OpenFileCtx.COMMIT_STATUS CheckCommit(DFSClient dfsClient, long commitOffset
			, Org.Jboss.Netty.Channel.Channel channel, int xid, Nfs3FileAttributes preOpAttr
			, bool fromRead)
		{
			if (!fromRead)
			{
				Preconditions.CheckState(channel != null && preOpAttr != null);
				// Keep stream active
				UpdateLastAccessTime();
			}
			Preconditions.CheckState(commitOffset >= 0);
			OpenFileCtx.COMMIT_STATUS ret = CheckCommitInternal(commitOffset, channel, xid, preOpAttr
				, fromRead);
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Got commit status: " + ret.ToString());
			}
			// Do the sync outside the lock
			if (ret == OpenFileCtx.COMMIT_STATUS.CommitDoSync || ret == OpenFileCtx.COMMIT_STATUS
				.CommitFinished)
			{
				try
				{
					// Sync file data and length
					fos.Hsync(EnumSet.Of(HdfsDataOutputStream.SyncFlag.UpdateLength));
					ret = OpenFileCtx.COMMIT_STATUS.CommitFinished;
				}
				catch (ClosedChannelException)
				{
					// Remove COMMIT_DO_SYNC status 
					// Nothing to do for metadata since attr related change is pass-through
					if (pendingWrites.IsEmpty())
					{
						ret = OpenFileCtx.COMMIT_STATUS.CommitFinished;
					}
					else
					{
						ret = OpenFileCtx.COMMIT_STATUS.CommitError;
					}
				}
				catch (IOException e)
				{
					Log.Error("Got stream error during data sync: " + e);
					// Do nothing. Stream will be closed eventually by StreamMonitor.
					// status = Nfs3Status.NFS3ERR_IO;
					ret = OpenFileCtx.COMMIT_STATUS.CommitError;
				}
			}
			return ret;
		}

		// Check if the to-commit range is sequential
		[VisibleForTesting]
		internal virtual bool CheckSequential(long commitOffset, long nextOffset)
		{
			lock (this)
			{
				Preconditions.CheckState(commitOffset >= nextOffset, "commitOffset " + commitOffset
					 + " less than nextOffset " + nextOffset);
				long offset = nextOffset;
				IEnumerator<OffsetRange> it = pendingWrites.DescendingKeySet().GetEnumerator();
				while (it.HasNext())
				{
					OffsetRange range = it.Next();
					if (range.GetMin() != offset)
					{
						// got a hole
						return false;
					}
					offset = range.GetMax();
					if (offset > commitOffset)
					{
						return true;
					}
				}
				// there is gap between the last pending write and commitOffset
				return false;
			}
		}

		private OpenFileCtx.COMMIT_STATUS HandleSpecialWait(bool fromRead, long commitOffset
			, Org.Jboss.Netty.Channel.Channel channel, int xid, Nfs3FileAttributes preOpAttr
			)
		{
			if (!fromRead)
			{
				// let client retry the same request, add pending commit to sync later
				OpenFileCtx.CommitCtx commitCtx = new OpenFileCtx.CommitCtx(commitOffset, channel
					, xid, preOpAttr);
				pendingCommits[commitOffset] = commitCtx;
			}
			if (Log.IsDebugEnabled())
			{
				Log.Debug("return COMMIT_SPECIAL_WAIT");
			}
			return OpenFileCtx.COMMIT_STATUS.CommitSpecialWait;
		}

		[VisibleForTesting]
		internal virtual OpenFileCtx.COMMIT_STATUS CheckCommitInternal(long commitOffset, 
			Org.Jboss.Netty.Channel.Channel channel, int xid, Nfs3FileAttributes preOpAttr, 
			bool fromRead)
		{
			lock (this)
			{
				if (!activeState)
				{
					if (pendingWrites.IsEmpty())
					{
						return OpenFileCtx.COMMIT_STATUS.CommitInactiveCtx;
					}
					else
					{
						// TODO: return success if already committed
						return OpenFileCtx.COMMIT_STATUS.CommitInactiveWithPendingWrite;
					}
				}
				long flushed = 0;
				try
				{
					flushed = GetFlushedOffset();
				}
				catch (IOException e)
				{
					Log.Error("Can't get flushed offset, error:" + e);
					return OpenFileCtx.COMMIT_STATUS.CommitError;
				}
				if (Log.IsDebugEnabled())
				{
					Log.Debug("getFlushedOffset=" + flushed + " commitOffset=" + commitOffset + "nextOffset="
						 + nextOffset.Get());
				}
				if (pendingWrites.IsEmpty())
				{
					if (aixCompatMode)
					{
						// Note that, there is no guarantee data is synced. Caller should still
						// do a sync here though the output stream might be closed.
						return OpenFileCtx.COMMIT_STATUS.CommitFinished;
					}
					else
					{
						if (flushed < nextOffset.Get())
						{
							if (Log.IsDebugEnabled())
							{
								Log.Debug("get commit while still writing to the requested offset," + " with empty queue"
									);
							}
							return HandleSpecialWait(fromRead, nextOffset.Get(), channel, xid, preOpAttr);
						}
						else
						{
							return OpenFileCtx.COMMIT_STATUS.CommitFinished;
						}
					}
				}
				Preconditions.CheckState(flushed <= nextOffset.Get(), "flushed " + flushed + " is larger than nextOffset "
					 + nextOffset.Get());
				// Handle large file upload
				if (uploadLargeFile && !aixCompatMode)
				{
					long co = (commitOffset > 0) ? commitOffset : pendingWrites.FirstEntry().Key.GetMax
						() - 1;
					if (co <= flushed)
					{
						return OpenFileCtx.COMMIT_STATUS.CommitDoSync;
					}
					else
					{
						if (co < nextOffset.Get())
						{
							if (Log.IsDebugEnabled())
							{
								Log.Debug("get commit while still writing to the requested offset");
							}
							return HandleSpecialWait(fromRead, co, channel, xid, preOpAttr);
						}
						else
						{
							// co >= nextOffset
							if (CheckSequential(co, nextOffset.Get()))
							{
								return HandleSpecialWait(fromRead, co, channel, xid, preOpAttr);
							}
							else
							{
								if (Log.IsDebugEnabled())
								{
									Log.Debug("return COMMIT_SPECIAL_SUCCESS");
								}
								return OpenFileCtx.COMMIT_STATUS.CommitSpecialSuccess;
							}
						}
					}
				}
				if (commitOffset > 0)
				{
					if (aixCompatMode)
					{
						// The AIX NFS client misinterprets RFC-1813 and will always send 4096
						// for the commitOffset even if fewer bytes than that have ever (or will
						// ever) be sent by the client. So, if in AIX compatibility mode, we
						// will always DO_SYNC if the number of bytes to commit have already all
						// been flushed, else we will fall through to the logic below which
						// checks for pending writes in the case that we're being asked to
						// commit more bytes than have so far been flushed. See HDFS-6549 for
						// more info.
						if (commitOffset <= flushed)
						{
							return OpenFileCtx.COMMIT_STATUS.CommitDoSync;
						}
					}
					else
					{
						if (commitOffset > flushed)
						{
							if (!fromRead)
							{
								OpenFileCtx.CommitCtx commitCtx = new OpenFileCtx.CommitCtx(commitOffset, channel
									, xid, preOpAttr);
								pendingCommits[commitOffset] = commitCtx;
							}
							return OpenFileCtx.COMMIT_STATUS.CommitWait;
						}
						else
						{
							return OpenFileCtx.COMMIT_STATUS.CommitDoSync;
						}
					}
				}
				KeyValuePair<OffsetRange, WriteCtx> key = pendingWrites.FirstEntry();
				// Commit whole file, commitOffset == 0
				if (!fromRead)
				{
					// Insert commit
					long maxOffset = key.Key.GetMax() - 1;
					Preconditions.CheckState(maxOffset > 0);
					OpenFileCtx.CommitCtx commitCtx = new OpenFileCtx.CommitCtx(maxOffset, channel, xid
						, preOpAttr);
					pendingCommits[maxOffset] = commitCtx;
				}
				return OpenFileCtx.COMMIT_STATUS.CommitWait;
			}
		}

		/// <summary>Check stream status to decide if it should be closed</summary>
		/// <returns>true, remove stream; false, keep stream</returns>
		public virtual bool StreamCleanup(long fileId, long streamTimeout)
		{
			lock (this)
			{
				Preconditions.CheckState(streamTimeout >= NfsConfigKeys.DfsNfsStreamTimeoutMinDefault
					);
				if (!activeState)
				{
					return true;
				}
				bool flag = false;
				// Check the stream timeout
				if (CheckStreamTimeout(streamTimeout))
				{
					if (Log.IsDebugEnabled())
					{
						Log.Debug("stream can be closed for fileId: " + fileId);
					}
					flag = true;
				}
				return flag;
			}
		}

		/// <summary>
		/// Get (and remove) the next WriteCtx from
		/// <see cref="pendingWrites"/>
		/// if possible.
		/// </summary>
		/// <returns>
		/// Null if
		/// <see cref="pendingWrites"/>
		/// is null, or the next WriteCtx's
		/// offset is larger than nextOffSet.
		/// </returns>
		private WriteCtx OfferNextToWrite()
		{
			lock (this)
			{
				if (pendingWrites.IsEmpty())
				{
					if (Log.IsDebugEnabled())
					{
						Log.Debug("The async write task has no pending writes, fileId: " + latestAttr.GetFileId
							());
					}
					// process pending commit again to handle this race: a commit is added
					// to pendingCommits map just after the last doSingleWrite returns.
					// There is no pending write and the commit should be handled by the
					// last doSingleWrite. Due to the race, the commit is left along and
					// can't be processed until cleanup. Therefore, we should do another
					// processCommits to fix the race issue.
					ProcessCommits(nextOffset.Get());
					// nextOffset has same value as
					// flushedOffset
					this.asyncStatus = false;
					return null;
				}
				KeyValuePair<OffsetRange, WriteCtx> lastEntry = pendingWrites.LastEntry();
				OffsetRange range = lastEntry.Key;
				WriteCtx toWrite = lastEntry.Value;
				if (Log.IsTraceEnabled())
				{
					Log.Trace("range.getMin()=" + range.GetMin() + " nextOffset=" + nextOffset);
				}
				long offset = nextOffset.Get();
				if (range.GetMin() > offset)
				{
					if (Log.IsDebugEnabled())
					{
						Log.Debug("The next sequential write has not arrived yet");
					}
					ProcessCommits(nextOffset.Get());
					// handle race
					this.asyncStatus = false;
				}
				else
				{
					if (range.GetMin() < offset && range.GetMax() > offset)
					{
						// shouldn't happen since we do sync for overlapped concurrent writers
						Log.Warn("Got an overlapping write (" + range.GetMin() + ", " + range.GetMax() + 
							"), nextOffset=" + offset + ". Silently drop it now");
						Sharpen.Collections.Remove(pendingWrites, range);
						ProcessCommits(nextOffset.Get());
					}
					else
					{
						// handle race
						if (Log.IsDebugEnabled())
						{
							Log.Debug("Remove write(" + range.GetMin() + "-" + range.GetMax() + ") from the list"
								);
						}
						// after writing, remove the WriteCtx from cache 
						Sharpen.Collections.Remove(pendingWrites, range);
						// update nextOffset
						nextOffset.AddAndGet(toWrite.GetCount());
						if (Log.IsDebugEnabled())
						{
							Log.Debug("Change nextOffset to " + nextOffset.Get());
						}
						return toWrite;
					}
				}
				return null;
			}
		}

		/// <summary>Invoked by AsyncDataService to write back to HDFS</summary>
		internal virtual void ExecuteWriteBack()
		{
			Preconditions.CheckState(asyncStatus, "openFileCtx has false asyncStatus, fileId: "
				 + latestAttr.GetFileId());
			long startOffset = asyncWriteBackStartOffset;
			try
			{
				while (activeState)
				{
					// asyncStatus could be changed to false in offerNextToWrite()
					WriteCtx toWrite = OfferNextToWrite();
					if (toWrite != null)
					{
						// Do the write
						DoSingleWrite(toWrite);
						UpdateLastAccessTime();
					}
					else
					{
						break;
					}
				}
				if (!activeState && Log.IsDebugEnabled())
				{
					Log.Debug("The openFileCtx is not active anymore, fileId: " + latestAttr.GetFileId
						());
				}
			}
			finally
			{
				// Make sure to reset asyncStatus to false unless a race happens
				lock (this)
				{
					if (startOffset == asyncWriteBackStartOffset)
					{
						asyncStatus = false;
					}
					else
					{
						Log.Info("Another async task is already started before this one" + " is finalized. fileId: "
							 + latestAttr.GetFileId() + " asyncStatus: " + asyncStatus + " original startOffset: "
							 + startOffset + " new startOffset: " + asyncWriteBackStartOffset + ". Won't change asyncStatus here."
							);
					}
				}
			}
		}

		private void ProcessCommits(long offset)
		{
			Preconditions.CheckState(offset > 0);
			long flushedOffset = 0;
			KeyValuePair<long, OpenFileCtx.CommitCtx> entry = null;
			int status = Nfs3Status.Nfs3errIo;
			try
			{
				flushedOffset = GetFlushedOffset();
				entry = pendingCommits.FirstEntry();
				if (entry == null || entry.Value.offset > flushedOffset)
				{
					return;
				}
				// Now do sync for the ready commits
				// Sync file data and length
				fos.Hsync(EnumSet.Of(HdfsDataOutputStream.SyncFlag.UpdateLength));
				status = Nfs3Status.Nfs3Ok;
			}
			catch (ClosedChannelException cce)
			{
				if (!pendingWrites.IsEmpty())
				{
					Log.Error("Can't sync for fileId: " + latestAttr.GetFileId() + ". Channel closed with writes pending."
						, cce);
				}
				status = Nfs3Status.Nfs3errIo;
			}
			catch (IOException e)
			{
				Log.Error("Got stream error during data sync: ", e);
				// Do nothing. Stream will be closed eventually by StreamMonitor.
				status = Nfs3Status.Nfs3errIo;
			}
			// Update latestAttr
			try
			{
				latestAttr = Nfs3Utils.GetFileAttr(client, Nfs3Utils.GetFileIdPath(latestAttr.GetFileId
					()), iug);
			}
			catch (IOException e)
			{
				Log.Error("Can't get new file attr, fileId: " + latestAttr.GetFileId(), e);
				status = Nfs3Status.Nfs3errIo;
			}
			if (latestAttr.GetSize() != offset)
			{
				Log.Error("After sync, the expect file size: " + offset + ", however actual file size is: "
					 + latestAttr.GetSize());
				status = Nfs3Status.Nfs3errIo;
			}
			WccData wccData = new WccData(Nfs3Utils.GetWccAttr(latestAttr), latestAttr);
			// Send response for the ready commits
			while (entry != null && entry.Value.offset <= flushedOffset)
			{
				Sharpen.Collections.Remove(pendingCommits, entry.Key);
				OpenFileCtx.CommitCtx commit = entry.Value;
				COMMIT3Response response = new COMMIT3Response(status, wccData, Nfs3Constant.WriteCommitVerf
					);
				RpcProgramNfs3.metrics.AddCommit(Nfs3Utils.GetElapsedTime(commit.startTime));
				Nfs3Utils.WriteChannelCommit(commit.GetChannel(), response.Serialize(new XDR(), commit
					.GetXid(), new VerifierNone()), commit.GetXid());
				if (Log.IsDebugEnabled())
				{
					Log.Debug("FileId: " + latestAttr.GetFileId() + " Service time: " + Nfs3Utils.GetElapsedTime
						(commit.startTime) + "ns. Sent response for commit: " + commit);
				}
				entry = pendingCommits.FirstEntry();
			}
		}

		private void DoSingleWrite(WriteCtx writeCtx)
		{
			Org.Jboss.Netty.Channel.Channel channel = writeCtx.GetChannel();
			int xid = writeCtx.GetXid();
			long offset = writeCtx.GetOffset();
			int count = writeCtx.GetCount();
			Nfs3Constant.WriteStableHow stableHow = writeCtx.GetStableHow();
			FileHandle handle = writeCtx.GetHandle();
			if (Log.IsDebugEnabled())
			{
				Log.Debug("do write, fileId: " + handle.GetFileId() + " offset: " + offset + " length: "
					 + count + " stableHow: " + stableHow.ToString());
			}
			try
			{
				// The write is not protected by lock. asyncState is used to make sure
				// there is one thread doing write back at any time    
				writeCtx.WriteData(fos);
				RpcProgramNfs3.metrics.IncrBytesWritten(writeCtx.GetCount());
				long flushedOffset = GetFlushedOffset();
				if (flushedOffset != (offset + count))
				{
					throw new IOException("output stream is out of sync, pos=" + flushedOffset + " and nextOffset should be"
						 + (offset + count));
				}
				// Reduce memory occupation size if request was allowed dumped
				if (writeCtx.GetDataState() == WriteCtx.DataState.AllowDump)
				{
					lock (writeCtx)
					{
						if (writeCtx.GetDataState() == WriteCtx.DataState.AllowDump)
						{
							writeCtx.SetDataState(WriteCtx.DataState.NoDump);
							UpdateNonSequentialWriteInMemory(-count);
							if (Log.IsDebugEnabled())
							{
								Log.Debug("After writing " + handle.GetFileId() + " at offset " + offset + ", updated the memory count, new value: "
									 + nonSequentialWriteInMemory.Get());
							}
						}
					}
				}
				if (!writeCtx.GetReplied())
				{
					if (stableHow != Nfs3Constant.WriteStableHow.Unstable)
					{
						Log.Info("Do sync for stable write: " + writeCtx);
						try
						{
							if (stableHow == Nfs3Constant.WriteStableHow.DataSync)
							{
								fos.Hsync();
							}
							else
							{
								Preconditions.CheckState(stableHow == Nfs3Constant.WriteStableHow.FileSync, "Unknown WriteStableHow: "
									 + stableHow);
								// Sync file data and length
								fos.Hsync(EnumSet.Of(HdfsDataOutputStream.SyncFlag.UpdateLength));
							}
						}
						catch (IOException e)
						{
							Log.Error("hsync failed with writeCtx: " + writeCtx, e);
							throw;
						}
					}
					WccAttr preOpAttr = latestAttr.GetWccAttr();
					WccData fileWcc = new WccData(preOpAttr, latestAttr);
					if (writeCtx.GetOriginalCount() != WriteCtx.InvalidOriginalCount)
					{
						Log.Warn("Return original count: " + writeCtx.GetOriginalCount() + " instead of real data count: "
							 + count);
						count = writeCtx.GetOriginalCount();
					}
					WRITE3Response response = new WRITE3Response(Nfs3Status.Nfs3Ok, fileWcc, count, stableHow
						, Nfs3Constant.WriteCommitVerf);
					RpcProgramNfs3.metrics.AddWrite(Nfs3Utils.GetElapsedTime(writeCtx.startTime));
					Nfs3Utils.WriteChannel(channel, response.Serialize(new XDR(), xid, new VerifierNone
						()), xid);
				}
				// Handle the waiting commits without holding any lock
				ProcessCommits(writeCtx.GetOffset() + writeCtx.GetCount());
			}
			catch (IOException e)
			{
				Log.Error("Error writing to fileId " + handle.GetFileId() + " at offset " + offset
					 + " and length " + count, e);
				if (!writeCtx.GetReplied())
				{
					WRITE3Response response = new WRITE3Response(Nfs3Status.Nfs3errIo);
					Nfs3Utils.WriteChannel(channel, response.Serialize(new XDR(), xid, new VerifierNone
						()), xid);
				}
				// Keep stream open. Either client retries or SteamMonitor closes it.
				Log.Info("Clean up open file context for fileId: " + latestAttr.GetFileId());
				Cleanup();
			}
		}

		internal virtual void Cleanup()
		{
			lock (this)
			{
				if (!activeState)
				{
					Log.Info("Current OpenFileCtx is already inactive, no need to cleanup.");
					return;
				}
				activeState = false;
				// stop the dump thread
				if (dumpThread != null && dumpThread.IsAlive())
				{
					dumpThread.Interrupt();
					try
					{
						dumpThread.Join(3000);
					}
					catch (Exception)
					{
					}
				}
				// Close stream
				try
				{
					if (fos != null)
					{
						fos.Close();
					}
				}
				catch (IOException e)
				{
					Log.Info("Can't close stream for fileId: " + latestAttr.GetFileId() + ", error: "
						 + e);
				}
				// Reply error for pending writes
				Log.Info("There are " + pendingWrites.Count + " pending writes.");
				WccAttr preOpAttr = latestAttr.GetWccAttr();
				while (!pendingWrites.IsEmpty())
				{
					OffsetRange key = pendingWrites.FirstKey();
					Log.Info("Fail pending write: (" + key.GetMin() + ", " + key.GetMax() + "), nextOffset="
						 + nextOffset.Get());
					WriteCtx writeCtx = Sharpen.Collections.Remove(pendingWrites, key);
					if (!writeCtx.GetReplied())
					{
						WccData fileWcc = new WccData(preOpAttr, latestAttr);
						WRITE3Response response = new WRITE3Response(Nfs3Status.Nfs3errIo, fileWcc, 0, writeCtx
							.GetStableHow(), Nfs3Constant.WriteCommitVerf);
						Nfs3Utils.WriteChannel(writeCtx.GetChannel(), response.Serialize(new XDR(), writeCtx
							.GetXid(), new VerifierNone()), writeCtx.GetXid());
					}
				}
				// Cleanup dump file
				if (dumpOut != null)
				{
					try
					{
						dumpOut.Close();
					}
					catch (IOException e)
					{
						Log.Error("Failed to close outputstream of dump file" + dumpFilePath, e);
					}
					FilePath dumpFile = new FilePath(dumpFilePath);
					if (dumpFile.Exists() && !dumpFile.Delete())
					{
						Log.Error("Failed to delete dumpfile: " + dumpFile);
					}
				}
				if (raf != null)
				{
					try
					{
						raf.Close();
					}
					catch (IOException e)
					{
						Log.Error("Got exception when closing input stream of dump file.", e);
					}
				}
			}
		}

		[VisibleForTesting]
		internal virtual ConcurrentNavigableMap<OffsetRange, WriteCtx> GetPendingWritesForTest
			()
		{
			return pendingWrites;
		}

		[VisibleForTesting]
		internal virtual ConcurrentNavigableMap<long, OpenFileCtx.CommitCtx> GetPendingCommitsForTest
			()
		{
			return pendingCommits;
		}

		[VisibleForTesting]
		internal virtual long GetNextOffsetForTest()
		{
			return nextOffset.Get();
		}

		[VisibleForTesting]
		internal virtual void SetNextOffsetForTest(long newValue)
		{
			nextOffset.Set(newValue);
		}

		[VisibleForTesting]
		internal virtual void SetActiveStatusForTest(bool activeState)
		{
			this.activeState = activeState;
		}

		public override string ToString()
		{
			return string.Format("activeState: %b asyncStatus: %b nextOffset: %d", activeState
				, asyncStatus, nextOffset.Get());
		}
	}
}
