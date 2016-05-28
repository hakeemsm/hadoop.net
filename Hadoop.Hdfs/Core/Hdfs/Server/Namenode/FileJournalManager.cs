using System;
using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Annotations;
using Com.Google.Common.Base;
using Com.Google.Common.Collect;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Org.Apache.Hadoop.IO.Nativeio;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>
	/// Journal manager for the common case of edits files being written
	/// to a storage directory.
	/// </summary>
	/// <remarks>
	/// Journal manager for the common case of edits files being written
	/// to a storage directory.
	/// Note: this class is not thread-safe and should be externally
	/// synchronized.
	/// </remarks>
	public class FileJournalManager : JournalManager
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Hdfs.Server.Namenode.FileJournalManager
			));

		private readonly Configuration conf;

		private readonly Storage.StorageDirectory sd;

		private readonly StorageErrorReporter errorReporter;

		private int outputBufferCapacity = 512 * 1024;

		private static readonly Sharpen.Pattern EditsRegex = Sharpen.Pattern.Compile(NNStorage.NameNodeFile
			.Edits.GetName() + "_(\\d+)-(\\d+)");

		private static readonly Sharpen.Pattern EditsInprogressRegex = Sharpen.Pattern.Compile
			(NNStorage.NameNodeFile.EditsInprogress.GetName() + "_(\\d+)");

		private static readonly Sharpen.Pattern EditsInprogressStaleRegex = Sharpen.Pattern
			.Compile(NNStorage.NameNodeFile.EditsInprogress.GetName() + "_(\\d+).*(\\S+)");

		private FilePath currentInProgress = null;

		[VisibleForTesting]
		internal NNStorageRetentionManager.StoragePurger purger = new NNStorageRetentionManager.DeletionStoragePurger
			();

		public FileJournalManager(Configuration conf, Storage.StorageDirectory sd, StorageErrorReporter
			 errorReporter)
		{
			this.conf = conf;
			this.sd = sd;
			this.errorReporter = errorReporter;
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Close()
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Format(NamespaceInfo ns)
		{
			// Formatting file journals is done by the StorageDirectory
			// format code, since they may share their directory with
			// checkpoints, etc.
			throw new NotSupportedException();
		}

		public virtual bool HasSomeData()
		{
			// Formatting file journals is done by the StorageDirectory
			// format code, since they may share their directory with
			// checkpoints, etc.
			throw new NotSupportedException();
		}

		/// <exception cref="System.IO.IOException"/>
		public override EditLogOutputStream StartLogSegment(long txid, int layoutVersion)
		{
			lock (this)
			{
				try
				{
					currentInProgress = NNStorage.GetInProgressEditsFile(sd, txid);
					EditLogOutputStream stm = new EditLogFileOutputStream(conf, currentInProgress, outputBufferCapacity
						);
					stm.Create(layoutVersion);
					return stm;
				}
				catch (IOException e)
				{
					Log.Warn("Unable to start log segment " + txid + " at " + currentInProgress + ": "
						 + e.GetLocalizedMessage());
					errorReporter.ReportErrorOnFile(currentInProgress);
					throw;
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override void FinalizeLogSegment(long firstTxId, long lastTxId)
		{
			lock (this)
			{
				FilePath inprogressFile = NNStorage.GetInProgressEditsFile(sd, firstTxId);
				FilePath dstFile = NNStorage.GetFinalizedEditsFile(sd, firstTxId, lastTxId);
				Log.Info("Finalizing edits file " + inprogressFile + " -> " + dstFile);
				Preconditions.CheckState(!dstFile.Exists(), "Can't finalize edits file " + inprogressFile
					 + " since finalized file " + "already exists");
				try
				{
					NativeIO.RenameTo(inprogressFile, dstFile);
				}
				catch (IOException e)
				{
					errorReporter.ReportErrorOnFile(dstFile);
					throw new InvalidOperationException("Unable to finalize edits file " + inprogressFile
						, e);
				}
				if (inprogressFile.Equals(currentInProgress))
				{
					currentInProgress = null;
				}
			}
		}

		[VisibleForTesting]
		public virtual Storage.StorageDirectory GetStorageDirectory()
		{
			return sd;
		}

		public override void SetOutputBufferCapacity(int size)
		{
			lock (this)
			{
				this.outputBufferCapacity = size;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void PurgeLogsOlderThan(long minTxIdToKeep)
		{
			Log.Info("Purging logs older than " + minTxIdToKeep);
			FilePath[] files = FileUtil.ListFiles(sd.GetCurrentDir());
			IList<FileJournalManager.EditLogFile> editLogs = MatchEditLogs(files, true);
			foreach (FileJournalManager.EditLogFile log in editLogs)
			{
				if (log.GetFirstTxId() < minTxIdToKeep && log.GetLastTxId() < minTxIdToKeep)
				{
					purger.PurgeLog(log);
				}
			}
		}

		/// <summary>Find all editlog segments starting at or above the given txid.</summary>
		/// <param name="firstTxId">the txnid which to start looking</param>
		/// <param name="inProgressOk">
		/// whether or not to include the in-progress edit log
		/// segment
		/// </param>
		/// <returns>a list of remote edit logs</returns>
		/// <exception cref="System.IO.IOException">if edit logs cannot be listed.</exception>
		public virtual IList<RemoteEditLog> GetRemoteEditLogs(long firstTxId, bool inProgressOk
			)
		{
			FilePath currentDir = sd.GetCurrentDir();
			IList<FileJournalManager.EditLogFile> allLogFiles = MatchEditLogs(currentDir);
			IList<RemoteEditLog> ret = Lists.NewArrayListWithCapacity(allLogFiles.Count);
			foreach (FileJournalManager.EditLogFile elf in allLogFiles)
			{
				if (elf.HasCorruptHeader() || (!inProgressOk && elf.IsInProgress()))
				{
					continue;
				}
				if (elf.IsInProgress())
				{
					try
					{
						elf.ValidateLog();
					}
					catch (IOException e)
					{
						Log.Error("got IOException while trying to validate header of " + elf + ".  Skipping."
							, e);
						continue;
					}
				}
				if (elf.GetFirstTxId() >= firstTxId)
				{
					ret.AddItem(new RemoteEditLog(elf.firstTxId, elf.lastTxId, elf.IsInProgress()));
				}
				else
				{
					if (elf.GetFirstTxId() < firstTxId && firstTxId <= elf.GetLastTxId())
					{
						// If the firstTxId is in the middle of an edit log segment. Return this
						// anyway and let the caller figure out whether it wants to use it.
						ret.AddItem(new RemoteEditLog(elf.firstTxId, elf.lastTxId, elf.IsInProgress()));
					}
				}
			}
			ret.Sort();
			return ret;
		}

		/// <summary>
		/// Discard all editlog segments whose first txid is greater than or equal to
		/// the given txid, by renaming them with suffix ".trash".
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		private void DiscardEditLogSegments(long startTxId)
		{
			FilePath currentDir = sd.GetCurrentDir();
			IList<FileJournalManager.EditLogFile> allLogFiles = MatchEditLogs(currentDir);
			IList<FileJournalManager.EditLogFile> toTrash = Lists.NewArrayList();
			Log.Info("Discard the EditLog files, the given start txid is " + startTxId);
			// go through the editlog files to make sure the startTxId is right at the
			// segment boundary
			foreach (FileJournalManager.EditLogFile elf in allLogFiles)
			{
				if (elf.GetFirstTxId() >= startTxId)
				{
					toTrash.AddItem(elf);
				}
				else
				{
					Preconditions.CheckState(elf.GetLastTxId() < startTxId);
				}
			}
			foreach (FileJournalManager.EditLogFile elf_1 in toTrash)
			{
				// rename these editlog file as .trash
				elf_1.MoveAsideTrashFile(startTxId);
				Log.Info("Trash the EditLog file " + elf_1);
			}
		}

		/// <summary>returns matching edit logs via the log directory.</summary>
		/// <remarks>
		/// returns matching edit logs via the log directory. Simple helper function
		/// that lists the files in the logDir and calls matchEditLogs(File[])
		/// </remarks>
		/// <param name="logDir">directory to match edit logs in</param>
		/// <returns>matched edit logs</returns>
		/// <exception cref="System.IO.IOException">IOException thrown for invalid logDir</exception>
		public static IList<FileJournalManager.EditLogFile> MatchEditLogs(FilePath logDir
			)
		{
			return MatchEditLogs(FileUtil.ListFiles(logDir));
		}

		internal static IList<FileJournalManager.EditLogFile> MatchEditLogs(FilePath[] filesInStorage
			)
		{
			return MatchEditLogs(filesInStorage, false);
		}

		private static IList<FileJournalManager.EditLogFile> MatchEditLogs(FilePath[] filesInStorage
			, bool forPurging)
		{
			IList<FileJournalManager.EditLogFile> ret = Lists.NewArrayList();
			foreach (FilePath f in filesInStorage)
			{
				string name = f.GetName();
				// Check for edits
				Matcher editsMatch = EditsRegex.Matcher(name);
				if (editsMatch.Matches())
				{
					try
					{
						long startTxId = long.Parse(editsMatch.Group(1));
						long endTxId = long.Parse(editsMatch.Group(2));
						ret.AddItem(new FileJournalManager.EditLogFile(f, startTxId, endTxId));
						continue;
					}
					catch (FormatException)
					{
						Log.Error("Edits file " + f + " has improperly formatted " + "transaction ID");
					}
				}
				// skip
				// Check for in-progress edits
				Matcher inProgressEditsMatch = EditsInprogressRegex.Matcher(name);
				if (inProgressEditsMatch.Matches())
				{
					try
					{
						long startTxId = long.Parse(inProgressEditsMatch.Group(1));
						ret.AddItem(new FileJournalManager.EditLogFile(f, startTxId, HdfsConstants.InvalidTxid
							, true));
						continue;
					}
					catch (FormatException)
					{
						Log.Error("In-progress edits file " + f + " has improperly " + "formatted transaction ID"
							);
					}
				}
				// skip
				if (forPurging)
				{
					// Check for in-progress stale edits
					Matcher staleInprogressEditsMatch = EditsInprogressStaleRegex.Matcher(name);
					if (staleInprogressEditsMatch.Matches())
					{
						try
						{
							long startTxId = long.Parse(staleInprogressEditsMatch.Group(1));
							ret.AddItem(new FileJournalManager.EditLogFile(f, startTxId, HdfsConstants.InvalidTxid
								, true));
							continue;
						}
						catch (FormatException)
						{
							Log.Error("In-progress stale edits file " + f + " has improperly " + "formatted transaction ID"
								);
						}
					}
				}
			}
			// skip
			return ret;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void SelectInputStreams(ICollection<EditLogInputStream> streams, long
			 fromTxId, bool inProgressOk)
		{
			lock (this)
			{
				IList<FileJournalManager.EditLogFile> elfs = MatchEditLogs(sd.GetCurrentDir());
				Log.Debug(this + ": selecting input streams starting at " + fromTxId + (inProgressOk
					 ? " (inProgress ok) " : " (excluding inProgress) ") + "from among " + elfs.Count
					 + " candidate file(s)");
				AddStreamsToCollectionFromFiles(elfs, streams, fromTxId, inProgressOk);
			}
		}

		internal static void AddStreamsToCollectionFromFiles(ICollection<FileJournalManager.EditLogFile
			> elfs, ICollection<EditLogInputStream> streams, long fromTxId, bool inProgressOk
			)
		{
			foreach (FileJournalManager.EditLogFile elf in elfs)
			{
				if (elf.IsInProgress())
				{
					if (!inProgressOk)
					{
						Log.Debug("passing over " + elf + " because it is in progress " + "and we are ignoring in-progress logs."
							);
						continue;
					}
					try
					{
						elf.ValidateLog();
					}
					catch (IOException e)
					{
						Log.Error("got IOException while trying to validate header of " + elf + ".  Skipping."
							, e);
						continue;
					}
				}
				if (elf.lastTxId < fromTxId)
				{
					System.Diagnostics.Debug.Assert(elf.lastTxId != HdfsConstants.InvalidTxid);
					Log.Debug("passing over " + elf + " because it ends at " + elf.lastTxId + ", but we only care about transactions "
						 + "as new as " + fromTxId);
					continue;
				}
				EditLogFileInputStream elfis = new EditLogFileInputStream(elf.GetFile(), elf.GetFirstTxId
					(), elf.GetLastTxId(), elf.IsInProgress());
				Log.Debug("selecting edit log stream " + elf);
				streams.AddItem(elfis);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override void RecoverUnfinalizedSegments()
		{
			lock (this)
			{
				FilePath currentDir = sd.GetCurrentDir();
				Log.Info("Recovering unfinalized segments in " + currentDir);
				IList<FileJournalManager.EditLogFile> allLogFiles = MatchEditLogs(currentDir);
				foreach (FileJournalManager.EditLogFile elf in allLogFiles)
				{
					if (elf.GetFile().Equals(currentInProgress))
					{
						continue;
					}
					if (elf.IsInProgress())
					{
						// If the file is zero-length, we likely just crashed after opening the
						// file, but before writing anything to it. Safe to delete it.
						if (elf.GetFile().Length() == 0)
						{
							Log.Info("Deleting zero-length edit log file " + elf);
							if (!elf.GetFile().Delete())
							{
								throw new IOException("Unable to delete file " + elf.GetFile());
							}
							continue;
						}
						elf.ValidateLog();
						if (elf.HasCorruptHeader())
						{
							elf.MoveAsideCorruptFile();
							throw new JournalManager.CorruptionException("In-progress edit log file is corrupt: "
								 + elf);
						}
						if (elf.GetLastTxId() == HdfsConstants.InvalidTxid)
						{
							// If the file has a valid header (isn't corrupt) but contains no
							// transactions, we likely just crashed after opening the file and
							// writing the header, but before syncing any transactions. Safe to
							// delete the file.
							Log.Info("Moving aside edit log file that seems to have zero " + "transactions " 
								+ elf);
							elf.MoveAsideEmptyFile();
							continue;
						}
						FinalizeLogSegment(elf.GetFirstTxId(), elf.GetLastTxId());
					}
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual IList<FileJournalManager.EditLogFile> GetLogFiles(long fromTxId)
		{
			FilePath currentDir = sd.GetCurrentDir();
			IList<FileJournalManager.EditLogFile> allLogFiles = MatchEditLogs(currentDir);
			IList<FileJournalManager.EditLogFile> logFiles = Lists.NewArrayList();
			foreach (FileJournalManager.EditLogFile elf in allLogFiles)
			{
				if (fromTxId <= elf.GetFirstTxId() || elf.ContainsTxId(fromTxId))
				{
					logFiles.AddItem(elf);
				}
			}
			logFiles.Sort(FileJournalManager.EditLogFile.CompareByStartTxid);
			return logFiles;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual FileJournalManager.EditLogFile GetLogFile(long startTxId)
		{
			return GetLogFile(sd.GetCurrentDir(), startTxId);
		}

		/// <exception cref="System.IO.IOException"/>
		public static FileJournalManager.EditLogFile GetLogFile(FilePath dir, long startTxId
			)
		{
			IList<FileJournalManager.EditLogFile> files = MatchEditLogs(dir);
			IList<FileJournalManager.EditLogFile> ret = Lists.NewLinkedList();
			foreach (FileJournalManager.EditLogFile elf in files)
			{
				if (elf.GetFirstTxId() == startTxId)
				{
					ret.AddItem(elf);
				}
			}
			if (ret.IsEmpty())
			{
				// no matches
				return null;
			}
			else
			{
				if (ret.Count == 1)
				{
					return ret[0];
				}
				else
				{
					throw new InvalidOperationException("More than one log segment in " + dir + " starting at txid "
						 + startTxId + ": " + Joiner.On(", ").Join(ret));
				}
			}
		}

		public override string ToString()
		{
			return string.Format("FileJournalManager(root=%s)", sd.GetRoot());
		}

		/// <summary>Record of an edit log that has been located and had its filename parsed.
		/// 	</summary>
		public class EditLogFile
		{
			private FilePath file;

			private readonly long firstTxId;

			private long lastTxId;

			private bool hasCorruptHeader = false;

			private readonly bool isInProgress;

			private sealed class _IComparer_463 : IComparer<FileJournalManager.EditLogFile>
			{
				public _IComparer_463()
				{
				}

				public int Compare(FileJournalManager.EditLogFile a, FileJournalManager.EditLogFile
					 b)
				{
					return ComparisonChain.Start().Compare(a.GetFirstTxId(), b.GetFirstTxId()).Compare
						(a.GetLastTxId(), b.GetLastTxId()).Result();
				}
			}

			internal static readonly IComparer<FileJournalManager.EditLogFile> CompareByStartTxid
				 = new _IComparer_463();

			internal EditLogFile(FilePath file, long firstTxId, long lastTxId)
				: this(file, firstTxId, lastTxId, false)
			{
				System.Diagnostics.Debug.Assert((lastTxId != HdfsConstants.InvalidTxid) && (lastTxId
					 >= firstTxId));
			}

			internal EditLogFile(FilePath file, long firstTxId, long lastTxId, bool isInProgress
				)
			{
				System.Diagnostics.Debug.Assert((lastTxId == HdfsConstants.InvalidTxid && isInProgress
					) || (lastTxId != HdfsConstants.InvalidTxid && lastTxId >= firstTxId));
				System.Diagnostics.Debug.Assert((firstTxId > 0) || (firstTxId == HdfsConstants.InvalidTxid
					));
				System.Diagnostics.Debug.Assert(file != null);
				Preconditions.CheckArgument(!isInProgress || lastTxId == HdfsConstants.InvalidTxid
					);
				this.firstTxId = firstTxId;
				this.lastTxId = lastTxId;
				this.file = file;
				this.isInProgress = isInProgress;
			}

			public virtual long GetFirstTxId()
			{
				return firstTxId;
			}

			public virtual long GetLastTxId()
			{
				return lastTxId;
			}

			internal virtual bool ContainsTxId(long txId)
			{
				return firstTxId <= txId && txId <= lastTxId;
			}

			/// <summary>Find out where the edit log ends.</summary>
			/// <remarks>
			/// Find out where the edit log ends.
			/// This will update the lastTxId of the EditLogFile or
			/// mark it as corrupt if it is.
			/// </remarks>
			/// <exception cref="System.IO.IOException"/>
			public virtual void ValidateLog()
			{
				FSEditLogLoader.EditLogValidation val = EditLogFileInputStream.ValidateEditLog(file
					);
				this.lastTxId = val.GetEndTxId();
				this.hasCorruptHeader = val.HasCorruptHeader();
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void ScanLog()
			{
				FSEditLogLoader.EditLogValidation val = EditLogFileInputStream.ScanEditLog(file);
				this.lastTxId = val.GetEndTxId();
				this.hasCorruptHeader = val.HasCorruptHeader();
			}

			public virtual bool IsInProgress()
			{
				return isInProgress;
			}

			public virtual FilePath GetFile()
			{
				return file;
			}

			internal virtual bool HasCorruptHeader()
			{
				return hasCorruptHeader;
			}

			/// <exception cref="System.IO.IOException"/>
			internal virtual void MoveAsideCorruptFile()
			{
				System.Diagnostics.Debug.Assert(hasCorruptHeader);
				RenameSelf(".corrupt");
			}

			/// <exception cref="System.IO.IOException"/>
			internal virtual void MoveAsideTrashFile(long markerTxid)
			{
				System.Diagnostics.Debug.Assert(this.GetFirstTxId() >= markerTxid);
				RenameSelf(".trash");
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void MoveAsideEmptyFile()
			{
				System.Diagnostics.Debug.Assert(lastTxId == HdfsConstants.InvalidTxid);
				RenameSelf(".empty");
			}

			/// <exception cref="System.IO.IOException"/>
			private void RenameSelf(string newSuffix)
			{
				FilePath src = file;
				FilePath dst = new FilePath(src.GetParent(), src.GetName() + newSuffix);
				// renameTo fails on Windows if the destination file already exists.
				try
				{
					if (dst.Exists())
					{
						if (!dst.Delete())
						{
							throw new IOException("Couldn't delete " + dst);
						}
					}
					NativeIO.RenameTo(src, dst);
				}
				catch (IOException e)
				{
					throw new IOException("Couldn't rename log " + src + " to " + dst, e);
				}
				file = dst;
			}

			public override string ToString()
			{
				return string.Format("EditLogFile(file=%s,first=%019d,last=%019d," + "inProgress=%b,hasCorruptHeader=%b)"
					, file.ToString(), firstTxId, lastTxId, IsInProgress(), hasCorruptHeader);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override void DiscardSegments(long startTxid)
		{
			DiscardEditLogSegments(startTxid);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void DoPreUpgrade()
		{
			Log.Info("Starting upgrade of edits directory " + sd.GetRoot());
			try
			{
				NNUpgradeUtil.DoPreUpgrade(conf, sd);
			}
			catch (IOException ioe)
			{
				Log.Error("Failed to move aside pre-upgrade storage " + "in image directory " + sd
					.GetRoot(), ioe);
				throw;
			}
		}

		/// <summary>
		/// This method assumes that the fields of the
		/// <see cref="Org.Apache.Hadoop.Hdfs.Server.Common.Storage"/>
		/// object have
		/// already been updated to the appropriate new values for the upgrade.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public override void DoUpgrade(Storage storage)
		{
			NNUpgradeUtil.DoUpgrade(sd, storage);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void DoFinalize()
		{
			NNUpgradeUtil.DoFinalize(sd);
		}

		/// <exception cref="System.IO.IOException"/>
		public override bool CanRollBack(StorageInfo storage, StorageInfo prevStorage, int
			 targetLayoutVersion)
		{
			return NNUpgradeUtil.CanRollBack(sd, storage, prevStorage, targetLayoutVersion);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void DoRollback()
		{
			NNUpgradeUtil.DoRollBack(sd);
		}

		/// <exception cref="System.IO.IOException"/>
		public override long GetJournalCTime()
		{
			StorageInfo sInfo = new StorageInfo((HdfsServerConstants.NodeType)null);
			sInfo.ReadProperties(sd);
			return sInfo.GetCTime();
		}
	}
}
