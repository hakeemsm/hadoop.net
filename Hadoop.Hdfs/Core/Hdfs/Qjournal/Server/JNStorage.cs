using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Collect;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Qjournal.Server
{
	/// <summary>
	/// A
	/// <see cref="Org.Apache.Hadoop.Hdfs.Server.Common.Storage"/>
	/// implementation for the
	/// <see cref="JournalNode"/>
	/// .
	/// The JN has a storage directory for each namespace for which it stores
	/// metadata. There is only a single directory per JN in the current design.
	/// </summary>
	internal class JNStorage : Storage
	{
		private readonly FileJournalManager fjm;

		private readonly Storage.StorageDirectory sd;

		private Storage.StorageState state;

		private static readonly IList<Sharpen.Pattern> CurrentDirPurgeRegexes = ImmutableList
			.Of(Sharpen.Pattern.Compile("edits_\\d+-(\\d+)"), Sharpen.Pattern.Compile("edits_inprogress_(\\d+)(?:\\..*)?"
			));

		private static readonly IList<Sharpen.Pattern> PaxosDirPurgeRegexes = ImmutableList
			.Of(Sharpen.Pattern.Compile("(\\d+)"));

		/// <param name="conf">Configuration object</param>
		/// <param name="logDir">the path to the directory in which data will be stored</param>
		/// <param name="errorReporter">a callback to report errors</param>
		/// <exception cref="System.IO.IOException"></exception>
		protected internal JNStorage(Configuration conf, FilePath logDir, HdfsServerConstants.StartupOption
			 startOpt, StorageErrorReporter errorReporter)
			: base(HdfsServerConstants.NodeType.JournalNode)
		{
			sd = new Storage.StorageDirectory(logDir);
			this.AddStorageDir(sd);
			this.fjm = new FileJournalManager(conf, sd, errorReporter);
			AnalyzeAndRecoverStorage(startOpt);
		}

		internal virtual FileJournalManager GetJournalManager()
		{
			return fjm;
		}

		/// <exception cref="System.IO.IOException"/>
		public override bool IsPreUpgradableLayout(Storage.StorageDirectory sd)
		{
			return false;
		}

		/// <summary>Find an edits file spanning the given transaction ID range.</summary>
		/// <remarks>
		/// Find an edits file spanning the given transaction ID range.
		/// If no such file exists, an exception is thrown.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		internal virtual FilePath FindFinalizedEditsFile(long startTxId, long endTxId)
		{
			FilePath ret = new FilePath(sd.GetCurrentDir(), NNStorage.GetFinalizedEditsFileName
				(startTxId, endTxId));
			if (!ret.Exists())
			{
				throw new IOException("No edits file for range " + startTxId + "-" + endTxId);
			}
			return ret;
		}

		/// <returns>
		/// the path for an in-progress edits file starting at the given
		/// transaction ID. This does not verify existence of the file.
		/// </returns>
		internal virtual FilePath GetInProgressEditLog(long startTxId)
		{
			return new FilePath(sd.GetCurrentDir(), NNStorage.GetInProgressEditsFileName(startTxId
				));
		}

		/// <param name="segmentTxId">the first txid of the segment</param>
		/// <param name="epoch">
		/// the epoch number of the writer which is coordinating
		/// recovery
		/// </param>
		/// <returns>
		/// the temporary path in which an edits log should be stored
		/// while it is being downloaded from a remote JournalNode
		/// </returns>
		internal virtual FilePath GetSyncLogTemporaryFile(long segmentTxId, long epoch)
		{
			string name = NNStorage.GetInProgressEditsFileName(segmentTxId) + ".epoch=" + epoch;
			return new FilePath(sd.GetCurrentDir(), name);
		}

		/// <returns>
		/// the path for the file which contains persisted data for the
		/// paxos-like recovery process for the given log segment.
		/// </returns>
		internal virtual FilePath GetPaxosFile(long segmentTxId)
		{
			return new FilePath(GetPaxosDir(), segmentTxId.ToString());
		}

		internal virtual FilePath GetPaxosDir()
		{
			return new FilePath(sd.GetCurrentDir(), "paxos");
		}

		internal virtual FilePath GetRoot()
		{
			return sd.GetRoot();
		}

		/// <summary>
		/// Remove any log files and associated paxos files which are older than
		/// the given txid.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		internal virtual void PurgeDataOlderThan(long minTxIdToKeep)
		{
			PurgeMatching(sd.GetCurrentDir(), CurrentDirPurgeRegexes, minTxIdToKeep);
			PurgeMatching(GetPaxosDir(), PaxosDirPurgeRegexes, minTxIdToKeep);
		}

		/// <summary>Purge files in the given directory which match any of the set of patterns.
		/// 	</summary>
		/// <remarks>
		/// Purge files in the given directory which match any of the set of patterns.
		/// The patterns must have a single numeric capture group which determines
		/// the associated transaction ID of the file. Only those files for which
		/// the transaction ID is less than the <code>minTxIdToKeep</code> parameter
		/// are removed.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		private static void PurgeMatching(FilePath dir, IList<Sharpen.Pattern> patterns, 
			long minTxIdToKeep)
		{
			foreach (FilePath f in FileUtil.ListFiles(dir))
			{
				if (!f.IsFile())
				{
					continue;
				}
				foreach (Sharpen.Pattern p in patterns)
				{
					Matcher matcher = p.Matcher(f.GetName());
					if (matcher.Matches())
					{
						// This parsing will always succeed since the group(1) is
						// /\d+/ in the regex itself.
						long txid = long.Parse(matcher.Group(1));
						if (txid < minTxIdToKeep)
						{
							Log.Info("Purging no-longer needed file " + txid);
							if (!f.Delete())
							{
								Log.Warn("Unable to delete no-longer-needed data " + f);
							}
							break;
						}
					}
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual void Format(NamespaceInfo nsInfo)
		{
			SetStorageInfo(nsInfo);
			Log.Info("Formatting journal " + sd + " with nsid: " + GetNamespaceID());
			// Unlock the directory before formatting, because we will
			// re-analyze it after format(). The analyzeStorage() call
			// below is reponsible for re-locking it. This is a no-op
			// if the storage is not currently locked.
			UnlockAll();
			sd.ClearDirectory();
			WriteProperties(sd);
			CreatePaxosDir();
			AnalyzeStorage();
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual void CreatePaxosDir()
		{
			if (!GetPaxosDir().Mkdirs())
			{
				throw new IOException("Could not create paxos dir: " + GetPaxosDir());
			}
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual void AnalyzeStorage()
		{
			this.state = sd.AnalyzeStorage(HdfsServerConstants.StartupOption.Regular, this);
			if (state == Storage.StorageState.Normal)
			{
				ReadProperties(sd);
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Hdfs.Server.Common.IncorrectVersionException"/
		/// 	>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Server.Common.InconsistentFSStateException
		/// 	"/>
		protected internal override void SetLayoutVersion(Properties props, Storage.StorageDirectory
			 sd)
		{
			int lv = System.Convert.ToInt32(GetProperty(props, sd, "layoutVersion"));
			// For journal node, since it now does not decode but just scan through the
			// edits, it can handle edits with future version in most of the cases.
			// Thus currently we may skip the layoutVersion check here.
			layoutVersion = lv;
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual void AnalyzeAndRecoverStorage(HdfsServerConstants.StartupOption 
			startOpt)
		{
			this.state = sd.AnalyzeStorage(startOpt, this);
			bool needRecover = state != Storage.StorageState.Normal && state != Storage.StorageState
				.NonExistent && state != Storage.StorageState.NotFormatted;
			if (state == Storage.StorageState.Normal && startOpt != HdfsServerConstants.StartupOption
				.Rollback)
			{
				ReadProperties(sd);
			}
			else
			{
				if (needRecover)
				{
					sd.DoRecover(state);
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual void CheckConsistentNamespace(NamespaceInfo nsInfo)
		{
			if (nsInfo.GetNamespaceID() != GetNamespaceID())
			{
				throw new IOException("Incompatible namespaceID for journal " + this.sd + ": NameNode has nsId "
					 + nsInfo.GetNamespaceID() + " but storage has nsId " + GetNamespaceID());
			}
			if (!nsInfo.GetClusterID().Equals(GetClusterID()))
			{
				throw new IOException("Incompatible clusterID for journal " + this.sd + ": NameNode has clusterId '"
					 + nsInfo.GetClusterID() + "' but storage has clusterId '" + GetClusterID() + "'"
					);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void Close()
		{
			Log.Info("Closing journal storage for " + sd);
			UnlockAll();
		}

		public virtual bool IsFormatted()
		{
			return state == Storage.StorageState.Normal;
		}
	}
}
