using System;
using System.IO;
using Com.Google.Common.Annotations;
using Com.Google.Common.Base;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Hdfs.Web;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Authentication.Client;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>
	/// An implementation of the abstract class
	/// <see cref="EditLogInputStream"/>
	/// , which
	/// reads edits from a file. That file may be either on the local disk or
	/// accessible via a URL.
	/// </summary>
	public class EditLogFileInputStream : EditLogInputStream
	{
		private readonly EditLogFileInputStream.LogSource log;

		private readonly long firstTxId;

		private readonly long lastTxId;

		private readonly bool isInProgress;

		private int maxOpSize;

		private enum State
		{
			Uninit,
			Open,
			Closed
		}

		private EditLogFileInputStream.State state = EditLogFileInputStream.State.Uninit;

		private InputStream fStream = null;

		private int logVersion = 0;

		private FSEditLogOp.Reader reader = null;

		private FSEditLogLoader.PositionTrackingInputStream tracker = null;

		private DataInputStream dataIn = null;

		internal static readonly Log Log = LogFactory.GetLog(typeof(EditLogInputStream));

		/// <summary>Open an EditLogInputStream for the given file.</summary>
		/// <remarks>
		/// Open an EditLogInputStream for the given file.
		/// The file is pretransactional, so has no txids
		/// </remarks>
		/// <param name="name">filename to open</param>
		/// <exception cref="LogHeaderCorruptException">
		/// if the header is either missing or
		/// appears to be corrupt/truncated
		/// </exception>
		/// <exception cref="System.IO.IOException">
		/// if an actual IO error occurs while reading the
		/// header
		/// </exception>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Server.Namenode.EditLogFileInputStream.LogHeaderCorruptException
		/// 	"/>
		internal EditLogFileInputStream(FilePath name)
			: this(name, HdfsConstants.InvalidTxid, HdfsConstants.InvalidTxid, false)
		{
		}

		/// <summary>Open an EditLogInputStream for the given file.</summary>
		/// <param name="name">filename to open</param>
		/// <param name="firstTxId">first transaction found in file</param>
		/// <param name="lastTxId">last transaction id found in file</param>
		public EditLogFileInputStream(FilePath name, long firstTxId, long lastTxId, bool 
			isInProgress)
			: this(new EditLogFileInputStream.FileLog(name), firstTxId, lastTxId, isInProgress
				)
		{
		}

		/// <summary>Open an EditLogInputStream for the given URL.</summary>
		/// <param name="connectionFactory">the URLConnectionFactory used to create the connection.
		/// 	</param>
		/// <param name="url">the url hosting the log</param>
		/// <param name="startTxId">the expected starting txid</param>
		/// <param name="endTxId">the expected ending txid</param>
		/// <param name="inProgress">whether the log is in-progress</param>
		/// <returns>a stream from which edits may be read</returns>
		public static EditLogInputStream FromUrl(URLConnectionFactory connectionFactory, 
			Uri url, long startTxId, long endTxId, bool inProgress)
		{
			return new Org.Apache.Hadoop.Hdfs.Server.Namenode.EditLogFileInputStream(new EditLogFileInputStream.URLLog
				(connectionFactory, url), startTxId, endTxId, inProgress);
		}

		private EditLogFileInputStream(EditLogFileInputStream.LogSource log, long firstTxId
			, long lastTxId, bool isInProgress)
		{
			this.log = log;
			this.firstTxId = firstTxId;
			this.lastTxId = lastTxId;
			this.isInProgress = isInProgress;
			this.maxOpSize = DFSConfigKeys.DfsNamenodeMaxOpSizeDefault;
		}

		/// <exception cref="Org.Apache.Hadoop.Hdfs.Server.Namenode.EditLogFileInputStream.LogHeaderCorruptException
		/// 	"/>
		/// <exception cref="System.IO.IOException"/>
		private void Init(bool verifyLayoutVersion)
		{
			Preconditions.CheckState(state == EditLogFileInputStream.State.Uninit);
			BufferedInputStream bin = null;
			try
			{
				fStream = log.GetInputStream();
				bin = new BufferedInputStream(fStream);
				tracker = new FSEditLogLoader.PositionTrackingInputStream(bin);
				dataIn = new DataInputStream(tracker);
				try
				{
					logVersion = ReadLogVersion(dataIn, verifyLayoutVersion);
				}
				catch (EOFException)
				{
					throw new EditLogFileInputStream.LogHeaderCorruptException("No header found in log"
						);
				}
				// We assume future layout will also support ADD_LAYOUT_FLAGS
				if (NameNodeLayoutVersion.Supports(LayoutVersion.Feature.AddLayoutFlags, logVersion
					) || logVersion < NameNodeLayoutVersion.CurrentLayoutVersion)
				{
					try
					{
						LayoutFlags.Read(dataIn);
					}
					catch (EOFException)
					{
						throw new EditLogFileInputStream.LogHeaderCorruptException("EOF while reading layout "
							 + "flags from log");
					}
				}
				reader = new FSEditLogOp.Reader(dataIn, tracker, logVersion);
				reader.SetMaxOpSize(maxOpSize);
				state = EditLogFileInputStream.State.Open;
			}
			finally
			{
				if (reader == null)
				{
					IOUtils.Cleanup(Log, dataIn, tracker, bin, fStream);
					state = EditLogFileInputStream.State.Closed;
				}
			}
		}

		public override long GetFirstTxId()
		{
			return firstTxId;
		}

		public override long GetLastTxId()
		{
			return lastTxId;
		}

		public override string GetName()
		{
			return log.GetName();
		}

		/// <exception cref="System.IO.IOException"/>
		private FSEditLogOp NextOpImpl(bool skipBrokenEdits)
		{
			FSEditLogOp op = null;
			switch (state)
			{
				case EditLogFileInputStream.State.Uninit:
				{
					try
					{
						Init(true);
					}
					catch (Exception e)
					{
						Log.Error("caught exception initializing " + this, e);
						if (skipBrokenEdits)
						{
							return null;
						}
						Throwables.PropagateIfPossible<IOException>(e);
					}
					Preconditions.CheckState(state != EditLogFileInputStream.State.Uninit);
					return NextOpImpl(skipBrokenEdits);
				}

				case EditLogFileInputStream.State.Open:
				{
					op = reader.ReadOp(skipBrokenEdits);
					if ((op != null) && (op.HasTransactionId()))
					{
						long txId = op.GetTransactionId();
						if ((txId >= lastTxId) && (lastTxId != HdfsConstants.InvalidTxid))
						{
							//
							// Sometimes, the NameNode crashes while it's writing to the
							// edit log.  In that case, you can end up with an unfinalized edit log
							// which has some garbage at the end.
							// JournalManager#recoverUnfinalizedSegments will finalize these
							// unfinished edit logs, giving them a defined final transaction 
							// ID.  Then they will be renamed, so that any subsequent
							// readers will have this information.
							//
							// Since there may be garbage at the end of these "cleaned up"
							// logs, we want to be sure to skip it here if we've read everything
							// we were supposed to read out of the stream.
							// So we force an EOF on all subsequent reads.
							//
							long skipAmt = log.Length() - tracker.GetPos();
							if (skipAmt > 0)
							{
								if (Log.IsDebugEnabled())
								{
									Log.Debug("skipping " + skipAmt + " bytes at the end " + "of edit log  '" + GetName
										() + "': reached txid " + txId + " out of " + lastTxId);
								}
								tracker.ClearLimit();
								IOUtils.SkipFully(tracker, skipAmt);
							}
						}
					}
					break;
				}

				case EditLogFileInputStream.State.Closed:
				{
					break;
				}
			}
			// return null
			return op;
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override long ScanNextOp()
		{
			Preconditions.CheckState(state == EditLogFileInputStream.State.Open);
			FSEditLogOp cachedNext = GetCachedOp();
			return cachedNext == null ? reader.ScanOp() : cachedNext.txid;
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override FSEditLogOp NextOp()
		{
			return NextOpImpl(false);
		}

		protected internal override FSEditLogOp NextValidOp()
		{
			try
			{
				return NextOpImpl(true);
			}
			catch (Exception e)
			{
				Log.Error("nextValidOp: got exception while reading " + this, e);
				return null;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override int GetVersion(bool verifyVersion)
		{
			if (state == EditLogFileInputStream.State.Uninit)
			{
				Init(verifyVersion);
			}
			return logVersion;
		}

		public override long GetPosition()
		{
			if (state == EditLogFileInputStream.State.Open)
			{
				return tracker.GetPos();
			}
			else
			{
				return 0;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Close()
		{
			if (state == EditLogFileInputStream.State.Open)
			{
				dataIn.Close();
			}
			state = EditLogFileInputStream.State.Closed;
		}

		/// <exception cref="System.IO.IOException"/>
		public override long Length()
		{
			// file size + size of both buffers
			return log.Length();
		}

		public override bool IsInProgress()
		{
			return isInProgress;
		}

		public override string ToString()
		{
			return GetName();
		}

		/// <exception cref="System.IO.IOException"/>
		internal static FSEditLogLoader.EditLogValidation ValidateEditLog(FilePath file)
		{
			Org.Apache.Hadoop.Hdfs.Server.Namenode.EditLogFileInputStream @in;
			try
			{
				@in = new Org.Apache.Hadoop.Hdfs.Server.Namenode.EditLogFileInputStream(file);
				@in.GetVersion(true);
			}
			catch (EditLogFileInputStream.LogHeaderCorruptException e)
			{
				// causes us to read the header
				// If the header is malformed or the wrong value, this indicates a corruption
				Log.Warn("Log file " + file + " has no valid header", e);
				return new FSEditLogLoader.EditLogValidation(0, HdfsConstants.InvalidTxid, true);
			}
			try
			{
				return FSEditLogLoader.ValidateEditLog(@in);
			}
			finally
			{
				IOUtils.CloseStream(@in);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		internal static FSEditLogLoader.EditLogValidation ScanEditLog(FilePath file)
		{
			Org.Apache.Hadoop.Hdfs.Server.Namenode.EditLogFileInputStream @in;
			try
			{
				@in = new Org.Apache.Hadoop.Hdfs.Server.Namenode.EditLogFileInputStream(file);
				// read the header, initialize the inputstream, but do not check the
				// layoutversion
				@in.GetVersion(false);
			}
			catch (EditLogFileInputStream.LogHeaderCorruptException e)
			{
				Log.Warn("Log file " + file + " has no valid header", e);
				return new FSEditLogLoader.EditLogValidation(0, HdfsConstants.InvalidTxid, true);
			}
			long lastPos = 0;
			long lastTxId = HdfsConstants.InvalidTxid;
			long numValid = 0;
			try
			{
				while (true)
				{
					long txid = HdfsConstants.InvalidTxid;
					lastPos = @in.GetPosition();
					try
					{
						if ((txid = @in.ScanNextOp()) == HdfsConstants.InvalidTxid)
						{
							break;
						}
					}
					catch (Exception t)
					{
						FSImage.Log.Warn("Caught exception after scanning through " + numValid + " ops from "
							 + @in + " while determining its valid length. Position was " + lastPos, t);
						@in.Resync();
						FSImage.Log.Warn("After resync, position is " + @in.GetPosition());
						continue;
					}
					if (lastTxId == HdfsConstants.InvalidTxid || txid > lastTxId)
					{
						lastTxId = txid;
					}
					numValid++;
				}
				return new FSEditLogLoader.EditLogValidation(lastPos, lastTxId, false);
			}
			finally
			{
				IOUtils.CloseStream(@in);
			}
		}

		/// <summary>Read the header of fsedit log</summary>
		/// <param name="in">fsedit stream</param>
		/// <returns>the edit log version number</returns>
		/// <exception cref="System.IO.IOException">if error occurs</exception>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Server.Namenode.EditLogFileInputStream.LogHeaderCorruptException
		/// 	"/>
		[VisibleForTesting]
		internal static int ReadLogVersion(DataInputStream @in, bool verifyLayoutVersion)
		{
			int logVersion;
			try
			{
				logVersion = @in.ReadInt();
			}
			catch (EOFException)
			{
				throw new EditLogFileInputStream.LogHeaderCorruptException("Reached EOF when reading log header"
					);
			}
			if (verifyLayoutVersion && (logVersion < HdfsConstants.NamenodeLayoutVersion || logVersion
				 > Storage.LastUpgradableLayoutVersion))
			{
				// future version
				// unsupported
				throw new EditLogFileInputStream.LogHeaderCorruptException("Unexpected version of the file system log file: "
					 + logVersion + ". Current version = " + HdfsConstants.NamenodeLayoutVersion + "."
					);
			}
			return logVersion;
		}

		/// <summary>
		/// Exception indicating that the header of an edits log file is
		/// corrupted.
		/// </summary>
		/// <remarks>
		/// Exception indicating that the header of an edits log file is
		/// corrupted. This can be because the header is not present,
		/// or because the header data is invalid (eg claims to be
		/// over a newer version than the running NameNode)
		/// </remarks>
		[System.Serializable]
		internal class LogHeaderCorruptException : IOException
		{
			private const long serialVersionUID = 1L;

			private LogHeaderCorruptException(string msg)
				: base(msg)
			{
			}
		}

		private interface LogSource
		{
			/// <exception cref="System.IO.IOException"/>
			InputStream GetInputStream();

			long Length();

			string GetName();
		}

		private class FileLog : EditLogFileInputStream.LogSource
		{
			private readonly FilePath file;

			public FileLog(FilePath file)
			{
				this.file = file;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual InputStream GetInputStream()
			{
				return new FileInputStream(file);
			}

			public virtual long Length()
			{
				return file.Length();
			}

			public virtual string GetName()
			{
				return file.GetPath();
			}
		}

		private class URLLog : EditLogFileInputStream.LogSource
		{
			private readonly Uri url;

			private long advertisedSize = -1;

			private const string ContentLength = "Content-Length";

			private readonly URLConnectionFactory connectionFactory;

			private readonly bool isSpnegoEnabled;

			public URLLog(URLConnectionFactory connectionFactory, Uri url)
			{
				this.connectionFactory = connectionFactory;
				this.isSpnegoEnabled = UserGroupInformation.IsSecurityEnabled();
				this.url = url;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual InputStream GetInputStream()
			{
				return SecurityUtil.DoAsCurrentUser(new _PrivilegedExceptionAction_456(this));
			}

			private sealed class _PrivilegedExceptionAction_456 : PrivilegedExceptionAction<InputStream
				>
			{
				public _PrivilegedExceptionAction_456(URLLog _enclosing)
				{
					this._enclosing = _enclosing;
				}

				/// <exception cref="System.IO.IOException"/>
				public InputStream Run()
				{
					HttpURLConnection connection;
					try
					{
						connection = (HttpURLConnection)this._enclosing.connectionFactory.OpenConnection(
							this._enclosing.url, this._enclosing.isSpnegoEnabled);
					}
					catch (AuthenticationException e)
					{
						throw new IOException(e);
					}
					if (connection.GetResponseCode() != HttpURLConnection.HttpOk)
					{
						throw new TransferFsImage.HttpGetFailedException("Fetch of " + this._enclosing.url
							 + " failed with status code " + connection.GetResponseCode() + "\nResponse message:\n"
							 + connection.GetResponseMessage(), connection);
					}
					string contentLength = connection.GetHeaderField(EditLogFileInputStream.URLLog.ContentLength
						);
					if (contentLength != null)
					{
						this._enclosing.advertisedSize = long.Parse(contentLength);
						if (this._enclosing.advertisedSize <= 0)
						{
							throw new IOException("Invalid " + EditLogFileInputStream.URLLog.ContentLength + 
								" header: " + contentLength);
						}
					}
					else
					{
						throw new IOException(EditLogFileInputStream.URLLog.ContentLength + " header is not provided "
							 + "by the server when trying to fetch " + this._enclosing.url);
					}
					return connection.GetInputStream();
				}

				private readonly URLLog _enclosing;
			}

			public virtual long Length()
			{
				return advertisedSize;
			}

			public virtual string GetName()
			{
				return url.ToString();
			}
		}

		public override void SetMaxOpSize(int maxOpSize)
		{
			this.maxOpSize = maxOpSize;
			if (reader != null)
			{
				reader.SetMaxOpSize(maxOpSize);
			}
		}

		public override bool IsLocalLog()
		{
			return log is EditLogFileInputStream.FileLog;
		}
	}
}
