using System;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>
	/// A generic abstract class to support reading edits log data from
	/// persistent storage.
	/// </summary>
	/// <remarks>
	/// A generic abstract class to support reading edits log data from
	/// persistent storage.
	/// It should stream bytes from the storage exactly as they were written
	/// into the #
	/// <see cref="EditLogOutputStream"/>
	/// .
	/// </remarks>
	public abstract class EditLogInputStream : IDisposable
	{
		private FSEditLogOp cachedOp = null;

		/// <summary>Returns the name of the currently active underlying stream.</summary>
		/// <remarks>
		/// Returns the name of the currently active underlying stream.  The default
		/// implementation returns the same value as getName unless overridden by the
		/// subclass.
		/// </remarks>
		/// <returns>String name of the currently active underlying stream</returns>
		public virtual string GetCurrentStreamName()
		{
			return GetName();
		}

		/// <returns>the name of the EditLogInputStream</returns>
		public abstract string GetName();

		/// <returns>the first transaction which will be found in this stream</returns>
		public abstract long GetFirstTxId();

		/// <returns>the last transaction which will be found in this stream</returns>
		public abstract long GetLastTxId();

		/// <summary>Close the stream.</summary>
		/// <exception cref="System.IO.IOException">if an error occurred while closing</exception>
		public abstract void Close();

		/// <summary>Read an operation from the stream</summary>
		/// <returns>an operation from the stream or null if at end of stream</returns>
		/// <exception cref="System.IO.IOException">if there is an error reading from the stream
		/// 	</exception>
		public virtual FSEditLogOp ReadOp()
		{
			FSEditLogOp ret;
			if (cachedOp != null)
			{
				ret = cachedOp;
				cachedOp = null;
				return ret;
			}
			return NextOp();
		}

		/// <summary>
		/// Position the stream so that a valid operation can be read from it with
		/// readOp().
		/// </summary>
		/// <remarks>
		/// Position the stream so that a valid operation can be read from it with
		/// readOp().
		/// This method can be used to skip over corrupted sections of edit logs.
		/// </remarks>
		public virtual void Resync()
		{
			if (cachedOp != null)
			{
				return;
			}
			cachedOp = NextValidOp();
		}

		/// <summary>Get the next operation from the stream storage.</summary>
		/// <returns>an operation from the stream or null if at end of stream</returns>
		/// <exception cref="System.IO.IOException">if there is an error reading from the stream
		/// 	</exception>
		protected internal abstract FSEditLogOp NextOp();

		/// <summary>Go through the next operation from the stream storage.</summary>
		/// <returns>the txid of the next operation.</returns>
		/// <exception cref="System.IO.IOException"/>
		protected internal virtual long ScanNextOp()
		{
			FSEditLogOp next = ReadOp();
			return next != null ? next.txid : HdfsConstants.InvalidTxid;
		}

		/// <summary>Get the next valid operation from the stream storage.</summary>
		/// <remarks>
		/// Get the next valid operation from the stream storage.
		/// This is exactly like nextOp, except that we attempt to skip over damaged
		/// parts of the edit log
		/// </remarks>
		/// <returns>an operation from the stream or null if at end of stream</returns>
		protected internal virtual FSEditLogOp NextValidOp()
		{
			// This is a trivial implementation which just assumes that any errors mean
			// that there is nothing more of value in the log.  Subclasses that support
			// error recovery will want to override this.
			try
			{
				return NextOp();
			}
			catch
			{
				return null;
			}
		}

		/// <summary>
		/// Skip edit log operations up to a given transaction ID, or until the
		/// end of the edit log is reached.
		/// </summary>
		/// <remarks>
		/// Skip edit log operations up to a given transaction ID, or until the
		/// end of the edit log is reached.
		/// After this function returns, the next call to readOp will return either
		/// end-of-file (null) or a transaction with a txid equal to or higher than
		/// the one we asked for.
		/// </remarks>
		/// <param name="txid">The transaction ID to read up until.</param>
		/// <returns>
		/// Returns true if we found a transaction ID greater than
		/// or equal to 'txid' in the log.
		/// </returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual bool SkipUntil(long txid)
		{
			while (true)
			{
				FSEditLogOp op = ReadOp();
				if (op == null)
				{
					return false;
				}
				if (op.GetTransactionId() >= txid)
				{
					cachedOp = op;
					return true;
				}
			}
		}

		/// <summary>return the cachedOp, and reset it to null.</summary>
		internal virtual FSEditLogOp GetCachedOp()
		{
			FSEditLogOp op = this.cachedOp;
			cachedOp = null;
			return op;
		}

		/// <summary>Get the layout version of the data in the stream.</summary>
		/// <returns>the layout version of the ops in the stream.</returns>
		/// <exception cref="System.IO.IOException">if there is an error reading the version</exception>
		public abstract int GetVersion(bool verifyVersion);

		/// <summary>Get the "position" of in the stream.</summary>
		/// <remarks>
		/// Get the "position" of in the stream. This is useful for
		/// debugging and operational purposes.
		/// Different stream types can have a different meaning for
		/// what the position is. For file streams it means the byte offset
		/// from the start of the file.
		/// </remarks>
		/// <returns>the position in the stream</returns>
		public abstract long GetPosition();

		/// <summary>Return the size of the current edits log or -1 if unknown.</summary>
		/// <returns>long size of the current edits log or -1 if unknown</returns>
		/// <exception cref="System.IO.IOException"/>
		public abstract long Length();

		/// <summary>Return true if this stream is in progress, false if it is finalized.</summary>
		public abstract bool IsInProgress();

		/// <summary>Set the maximum opcode size in bytes.</summary>
		public abstract void SetMaxOpSize(int maxOpSize);

		/// <summary>
		/// Returns true if we are currently reading the log from a local disk or an
		/// even faster data source (e.g.
		/// </summary>
		/// <remarks>
		/// Returns true if we are currently reading the log from a local disk or an
		/// even faster data source (e.g. a byte buffer).
		/// </remarks>
		public abstract bool IsLocalLog();
	}
}
