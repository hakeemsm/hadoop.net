using System.IO;
using Com.Google.Common.Annotations;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>
	/// An implementation of the abstract class
	/// <see cref="EditLogOutputStream"/>
	/// , which
	/// stores edits in a local file.
	/// </summary>
	public class EditLogFileOutputStream : EditLogOutputStream
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Hdfs.Server.Namenode.EditLogFileOutputStream
			));

		public const int MinPreallocationLength = 1024 * 1024;

		private FilePath file;

		private FileOutputStream fp;

		private FileChannel fc;

		private EditsDoubleBuffer doubleBuf;

		internal static readonly ByteBuffer fill = ByteBuffer.AllocateDirect(MinPreallocationLength
			);

		private bool shouldSyncWritesAndSkipFsync = false;

		private static bool shouldSkipFsyncForTests = false;

		static EditLogFileOutputStream()
		{
			// file stream for storing edit logs
			// channel of the file stream for sync
			fill.Position(0);
			for (int i = 0; i < fill.Capacity(); i++)
			{
				fill.Put(FSEditLogOpCodes.OpInvalid.GetOpCode());
			}
		}

		/// <summary>Creates output buffers and file object.</summary>
		/// <param name="conf">Configuration object</param>
		/// <param name="name">File name to store edit log</param>
		/// <param name="size">Size of flush buffer</param>
		/// <exception cref="System.IO.IOException"/>
		public EditLogFileOutputStream(Configuration conf, FilePath name, int size)
			: base()
		{
			shouldSyncWritesAndSkipFsync = conf.GetBoolean(DFSConfigKeys.DfsNamenodeEditsNoeditlogchannelflush
				, DFSConfigKeys.DfsNamenodeEditsNoeditlogchannelflushDefault);
			file = name;
			doubleBuf = new EditsDoubleBuffer(size);
			RandomAccessFile rp;
			if (shouldSyncWritesAndSkipFsync)
			{
				rp = new RandomAccessFile(name, "rws");
			}
			else
			{
				rp = new RandomAccessFile(name, "rw");
			}
			fp = new FileOutputStream(rp.GetFD());
			// open for append
			fc = rp.GetChannel();
			fc.Position(fc.Size());
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Write(FSEditLogOp op)
		{
			doubleBuf.WriteOp(op);
		}

		/// <summary>Write a transaction to the stream.</summary>
		/// <remarks>
		/// Write a transaction to the stream. The serialization format is:
		/// <ul>
		/// <li>the opcode (byte)</li>
		/// <li>the transaction id (long)</li>
		/// <li>the actual Writables for the transaction</li>
		/// </ul>
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public override void WriteRaw(byte[] bytes, int offset, int length)
		{
			doubleBuf.WriteRaw(bytes, offset, length);
		}

		/// <summary>Create empty edits logs file.</summary>
		/// <exception cref="System.IO.IOException"/>
		public override void Create(int layoutVersion)
		{
			fc.Truncate(0);
			fc.Position(0);
			WriteHeader(layoutVersion, doubleBuf.GetCurrentBuf());
			SetReadyToFlush();
			Flush();
		}

		/// <summary>
		/// Write header information for this EditLogFileOutputStream to the provided
		/// DataOutputSream.
		/// </summary>
		/// <param name="layoutVersion">the LayoutVersion of the EditLog</param>
		/// <param name="out">the output stream to write the header to.</param>
		/// <exception cref="System.IO.IOException">in the event of error writing to the stream.
		/// 	</exception>
		[VisibleForTesting]
		public static void WriteHeader(int layoutVersion, DataOutputStream @out)
		{
			@out.WriteInt(layoutVersion);
			LayoutFlags.Write(@out);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Close()
		{
			if (fp == null)
			{
				throw new IOException("Trying to use aborted output stream");
			}
			try
			{
				// close should have been called after all pending transactions
				// have been flushed & synced.
				// if already closed, just skip
				if (doubleBuf != null)
				{
					doubleBuf.Close();
					doubleBuf = null;
				}
				// remove any preallocated padding bytes from the transaction log.
				if (fc != null && fc.IsOpen())
				{
					fc.Truncate(fc.Position());
					fc.Close();
					fc = null;
				}
				fp.Close();
				fp = null;
			}
			finally
			{
				IOUtils.Cleanup(Log, fc, fp);
				doubleBuf = null;
				fc = null;
				fp = null;
			}
			fp = null;
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Abort()
		{
			if (fp == null)
			{
				return;
			}
			IOUtils.Cleanup(Log, fp);
			fp = null;
		}

		/// <summary>All data that has been written to the stream so far will be flushed.</summary>
		/// <remarks>
		/// All data that has been written to the stream so far will be flushed. New
		/// data can be still written to the stream while flushing is performed.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public override void SetReadyToFlush()
		{
			doubleBuf.SetReadyToFlush();
		}

		/// <summary>Flush ready buffer to persistent store.</summary>
		/// <remarks>
		/// Flush ready buffer to persistent store. currentBuffer is not flushed as it
		/// accumulates new log records while readyBuffer will be flushed and synced.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		protected internal override void FlushAndSync(bool durable)
		{
			if (fp == null)
			{
				throw new IOException("Trying to use aborted output stream");
			}
			if (doubleBuf.IsFlushed())
			{
				Log.Info("Nothing to flush");
				return;
			}
			Preallocate();
			// preallocate file if necessary
			doubleBuf.FlushTo(fp);
			if (durable && !shouldSkipFsyncForTests && !shouldSyncWritesAndSkipFsync)
			{
				fc.Force(false);
			}
		}

		// metadata updates not needed
		/// <returns>true if the number of buffered data exceeds the intial buffer size</returns>
		public override bool ShouldForceSync()
		{
			return doubleBuf.ShouldForceSync();
		}

		/// <exception cref="System.IO.IOException"/>
		private void Preallocate()
		{
			long position = fc.Position();
			long size = fc.Size();
			int bufSize = doubleBuf.GetReadyBuf().GetLength();
			long need = bufSize - (size - position);
			if (need <= 0)
			{
				return;
			}
			long oldSize = size;
			long total = 0;
			long fillCapacity = fill.Capacity();
			while (need > 0)
			{
				fill.Position(0);
				IOUtils.WriteFully(fc, fill, size);
				need -= fillCapacity;
				size += fillCapacity;
				total += fillCapacity;
			}
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Preallocated " + total + " bytes at the end of " + "the edit log (offset "
					 + oldSize + ")");
			}
		}

		/// <summary>Returns the file associated with this stream.</summary>
		internal virtual FilePath GetFile()
		{
			return file;
		}

		public override string ToString()
		{
			return "EditLogFileOutputStream(" + file + ")";
		}

		/// <returns>true if this stream is currently open.</returns>
		public virtual bool IsOpen()
		{
			return fp != null;
		}

		[VisibleForTesting]
		public virtual void SetFileChannelForTesting(FileChannel fc)
		{
			this.fc = fc;
		}

		[VisibleForTesting]
		public virtual FileChannel GetFileChannelForTesting()
		{
			return fc;
		}

		/// <summary>
		/// For the purposes of unit tests, we don't need to actually
		/// write durably to disk.
		/// </summary>
		/// <remarks>
		/// For the purposes of unit tests, we don't need to actually
		/// write durably to disk. So, we can skip the fsync() calls
		/// for a speed improvement.
		/// </remarks>
		/// <param name="skip">true if fsync should <em>not</em> be called</param>
		[VisibleForTesting]
		public static void SetShouldSkipFsyncForTesting(bool skip)
		{
			shouldSkipFsyncForTests = skip;
		}
	}
}
