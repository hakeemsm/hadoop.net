using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Annotations;
using Com.Google.Common.Base;
using Com.Google.Common.Collect;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Datanode
{
	/// <summary>
	/// Manages storage for the set of BlockPoolSlices which share a particular
	/// block pool id, on this DataNode.
	/// </summary>
	/// <remarks>
	/// Manages storage for the set of BlockPoolSlices which share a particular
	/// block pool id, on this DataNode.
	/// This class supports the following functionality:
	/// <ol>
	/// <li> Formatting a new block pool storage</li>
	/// <li> Recovering a storage state to a consistent state (if possible&gt;</li>
	/// <li> Taking a snapshot of the block pool during upgrade</li>
	/// <li> Rolling back a block pool to a previous snapshot</li>
	/// <li> Finalizing block storage by deletion of a snapshot</li>
	/// </ul>
	/// </remarks>
	/// <seealso cref="Org.Apache.Hadoop.Hdfs.Server.Common.Storage"/>
	public class BlockPoolSliceStorage : Storage
	{
		internal const string TrashRootDir = "trash";

		/// <summary>
		/// A marker file that is created on each root directory if a rolling upgrade
		/// is in progress.
		/// </summary>
		/// <remarks>
		/// A marker file that is created on each root directory if a rolling upgrade
		/// is in progress. The NN does not inform the DN when a rolling upgrade is
		/// finalized. All the DN can infer is whether or not a rolling upgrade is
		/// currently in progress. When the rolling upgrade is not in progress:
		/// 1. If the marker file is present, then a rolling upgrade just completed.
		/// If a 'previous' directory exists, it can be deleted now.
		/// 2. If the marker file is absent, then a regular upgrade may be in
		/// progress. Do not delete the 'previous' directory.
		/// </remarks>
		internal const string RollingUpgradeMarkerFile = "RollingUpgradeInProgress";

		private static readonly string BlockPoolIdPatternBase = Sharpen.Pattern.Quote(FilePath
			.separator) + "BP-\\d+-\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}-\\d+" + Sharpen.Pattern
			.Quote(FilePath.separator);

		private static readonly Sharpen.Pattern BlockPoolPathPattern = Sharpen.Pattern.Compile
			("^(.*)(" + BlockPoolIdPatternBase + ")(.*)$");

		private static readonly Sharpen.Pattern BlockPoolCurrentPathPattern = Sharpen.Pattern
			.Compile("^(.*)(" + BlockPoolIdPatternBase + ")(" + StorageDirCurrent + ")(.*)$"
			);

		private static readonly Sharpen.Pattern BlockPoolTrashPathPattern = Sharpen.Pattern
			.Compile("^(.*)(" + BlockPoolIdPatternBase + ")(" + TrashRootDir + ")(.*)$");

		private string blockpoolID = string.Empty;

		private Daemon trashCleaner;

		public BlockPoolSliceStorage(StorageInfo storageInfo, string bpid)
			: base(storageInfo)
		{
			// id of the blockpool
			blockpoolID = bpid;
		}

		/// <summary>
		/// These maps are used as an optimization to avoid one filesystem operation
		/// per storage on each heartbeat response.
		/// </summary>
		private static ICollection<string> storagesWithRollingUpgradeMarker;

		private static ICollection<string> storagesWithoutRollingUpgradeMarker;

		internal BlockPoolSliceStorage(int namespaceID, string bpID, long cTime, string clusterId
			)
			: base(HdfsServerConstants.NodeType.DataNode)
		{
			this.namespaceID = namespaceID;
			this.blockpoolID = bpID;
			this.cTime = cTime;
			this.clusterID = clusterId;
			storagesWithRollingUpgradeMarker = Sharpen.Collections.NewSetFromMap(new ConcurrentHashMap
				<string, bool>());
			storagesWithoutRollingUpgradeMarker = Sharpen.Collections.NewSetFromMap(new ConcurrentHashMap
				<string, bool>());
		}

		private BlockPoolSliceStorage()
			: base(HdfsServerConstants.NodeType.DataNode)
		{
			storagesWithRollingUpgradeMarker = Sharpen.Collections.NewSetFromMap(new ConcurrentHashMap
				<string, bool>());
			storagesWithoutRollingUpgradeMarker = Sharpen.Collections.NewSetFromMap(new ConcurrentHashMap
				<string, bool>());
		}

		// Expose visibility for VolumeBuilder#commit().
		protected internal override void AddStorageDir(Storage.StorageDirectory sd)
		{
			base.AddStorageDir(sd);
		}

		/// <summary>Load one storage directory.</summary>
		/// <remarks>Load one storage directory. Recover from previous transitions if required.
		/// 	</remarks>
		/// <param name="datanode">datanode instance</param>
		/// <param name="nsInfo">namespace information</param>
		/// <param name="dataDir">the root path of the storage directory</param>
		/// <param name="startOpt">startup option</param>
		/// <returns>the StorageDirectory successfully loaded.</returns>
		/// <exception cref="System.IO.IOException"/>
		private Storage.StorageDirectory LoadStorageDirectory(DataNode datanode, NamespaceInfo
			 nsInfo, FilePath dataDir, HdfsServerConstants.StartupOption startOpt)
		{
			Storage.StorageDirectory sd = new Storage.StorageDirectory(dataDir, null, true);
			try
			{
				Storage.StorageState curState = sd.AnalyzeStorage(startOpt, this);
				switch (curState)
				{
					case Storage.StorageState.Normal:
					{
						// sd is locked but not opened
						break;
					}

					case Storage.StorageState.NonExistent:
					{
						Log.Info("Block pool storage directory " + dataDir + " does not exist");
						throw new IOException("Storage directory " + dataDir + " does not exist");
					}

					case Storage.StorageState.NotFormatted:
					{
						// format
						Log.Info("Block pool storage directory " + dataDir + " is not formatted for " + nsInfo
							.GetBlockPoolID());
						Log.Info("Formatting ...");
						Format(sd, nsInfo);
						break;
					}

					default:
					{
						// recovery part is common
						sd.DoRecover(curState);
						break;
					}
				}
				// 2. Do transitions
				// Each storage directory is treated individually.
				// During startup some of them can upgrade or roll back
				// while others could be up-to-date for the regular startup.
				DoTransition(datanode, sd, nsInfo, startOpt);
				if (GetCTime() != nsInfo.GetCTime())
				{
					throw new IOException("Data-node and name-node CTimes must be the same.");
				}
				// 3. Update successfully loaded storage.
				SetServiceLayoutVersion(GetServiceLayoutVersion());
				WriteProperties(sd);
				return sd;
			}
			catch (IOException ioe)
			{
				sd.Unlock();
				throw;
			}
		}

		/// <summary>Analyze and load storage directories.</summary>
		/// <remarks>
		/// Analyze and load storage directories. Recover from previous transitions if
		/// required.
		/// The block pool storages are either all analyzed or none of them is loaded.
		/// Therefore, a failure on loading any block pool storage results a faulty
		/// data volume.
		/// </remarks>
		/// <param name="datanode">Datanode to which this storage belongs to</param>
		/// <param name="nsInfo">namespace information</param>
		/// <param name="dataDirs">storage directories of block pool</param>
		/// <param name="startOpt">startup option</param>
		/// <returns>an array of loaded block pool directories.</returns>
		/// <exception cref="System.IO.IOException">on error</exception>
		internal virtual IList<Storage.StorageDirectory> LoadBpStorageDirectories(DataNode
			 datanode, NamespaceInfo nsInfo, ICollection<FilePath> dataDirs, HdfsServerConstants.StartupOption
			 startOpt)
		{
			IList<Storage.StorageDirectory> succeedDirs = Lists.NewArrayList();
			try
			{
				foreach (FilePath dataDir in dataDirs)
				{
					if (ContainsStorageDir(dataDir))
					{
						throw new IOException("BlockPoolSliceStorage.recoverTransitionRead: " + "attempt to load an used block storage: "
							 + dataDir);
					}
					Storage.StorageDirectory sd = LoadStorageDirectory(datanode, nsInfo, dataDir, startOpt
						);
					succeedDirs.AddItem(sd);
				}
			}
			catch (IOException e)
			{
				Log.Warn("Failed to analyze storage directories for block pool " + nsInfo.GetBlockPoolID
					(), e);
				throw;
			}
			return succeedDirs;
		}

		/// <summary>Analyze storage directories.</summary>
		/// <remarks>
		/// Analyze storage directories. Recover from previous transitions if required.
		/// The block pool storages are either all analyzed or none of them is loaded.
		/// Therefore, a failure on loading any block pool storage results a faulty
		/// data volume.
		/// </remarks>
		/// <param name="datanode">Datanode to which this storage belongs to</param>
		/// <param name="nsInfo">namespace information</param>
		/// <param name="dataDirs">storage directories of block pool</param>
		/// <param name="startOpt">startup option</param>
		/// <exception cref="System.IO.IOException">on error</exception>
		internal virtual void RecoverTransitionRead(DataNode datanode, NamespaceInfo nsInfo
			, ICollection<FilePath> dataDirs, HdfsServerConstants.StartupOption startOpt)
		{
			Log.Info("Analyzing storage directories for bpid " + nsInfo.GetBlockPoolID());
			foreach (Storage.StorageDirectory sd in LoadBpStorageDirectories(datanode, nsInfo
				, dataDirs, startOpt))
			{
				AddStorageDir(sd);
			}
		}

		/// <summary>Format a block pool slice storage.</summary>
		/// <param name="dnCurDir">DataStorage current directory</param>
		/// <param name="nsInfo">the name space info</param>
		/// <exception cref="System.IO.IOException">Signals that an I/O exception has occurred.
		/// 	</exception>
		internal virtual void Format(FilePath dnCurDir, NamespaceInfo nsInfo)
		{
			FilePath curBpDir = GetBpRoot(nsInfo.GetBlockPoolID(), dnCurDir);
			Storage.StorageDirectory bpSdir = new Storage.StorageDirectory(curBpDir);
			Format(bpSdir, nsInfo);
		}

		/// <summary>Format a block pool slice storage.</summary>
		/// <param name="bpSdir">the block pool storage</param>
		/// <param name="nsInfo">the name space info</param>
		/// <exception cref="System.IO.IOException">Signals that an I/O exception has occurred.
		/// 	</exception>
		private void Format(Storage.StorageDirectory bpSdir, NamespaceInfo nsInfo)
		{
			Log.Info("Formatting block pool " + blockpoolID + " directory " + bpSdir.GetCurrentDir
				());
			bpSdir.ClearDirectory();
			// create directory
			this.layoutVersion = HdfsConstants.DatanodeLayoutVersion;
			this.cTime = nsInfo.GetCTime();
			this.namespaceID = nsInfo.GetNamespaceID();
			this.blockpoolID = nsInfo.GetBlockPoolID();
			WriteProperties(bpSdir);
		}

		/// <summary>Remove block pool level storage directory.</summary>
		/// <param name="absPathToRemove">
		/// the absolute path of the root for the block pool
		/// level storage to remove.
		/// </param>
		internal virtual void Remove(FilePath absPathToRemove)
		{
			Preconditions.CheckArgument(absPathToRemove.IsAbsolute());
			Log.Info("Removing block level storage: " + absPathToRemove);
			for (IEnumerator<Storage.StorageDirectory> it = this.storageDirs.GetEnumerator(); 
				it.HasNext(); )
			{
				Storage.StorageDirectory sd = it.Next();
				if (sd.GetRoot().GetAbsoluteFile().Equals(absPathToRemove))
				{
					it.Remove();
					break;
				}
			}
		}

		/// <summary>
		/// Set layoutVersion, namespaceID and blockpoolID into block pool storage
		/// VERSION file
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		protected internal override void SetPropertiesFromFields(Properties props, Storage.StorageDirectory
			 sd)
		{
			props.SetProperty("layoutVersion", layoutVersion.ToString());
			props.SetProperty("namespaceID", namespaceID.ToString());
			props.SetProperty("blockpoolID", blockpoolID);
			props.SetProperty("cTime", cTime.ToString());
		}

		/// <summary>Validate and set block pool ID</summary>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Server.Common.InconsistentFSStateException
		/// 	"/>
		private void SetBlockPoolID(FilePath storage, string bpid)
		{
			if (bpid == null || bpid.Equals(string.Empty))
			{
				throw new InconsistentFSStateException(storage, "file " + StorageFileVersion + " is invalid."
					);
			}
			if (!blockpoolID.Equals(string.Empty) && !blockpoolID.Equals(bpid))
			{
				throw new InconsistentFSStateException(storage, "Unexpected blockpoolID " + bpid 
					+ ". Expected " + blockpoolID);
			}
			blockpoolID = bpid;
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override void SetFieldsFromProperties(Properties props, Storage.StorageDirectory
			 sd)
		{
			SetLayoutVersion(props, sd);
			SetNamespaceID(props, sd);
			SetcTime(props, sd);
			string sbpid = props.GetProperty("blockpoolID");
			SetBlockPoolID(sd.GetRoot(), sbpid);
		}

		/// <summary>
		/// Analyze whether a transition of the BP state is required and
		/// perform it if necessary.
		/// </summary>
		/// <remarks>
		/// Analyze whether a transition of the BP state is required and
		/// perform it if necessary.
		/// <br />
		/// Rollback if previousLV &gt;= LAYOUT_VERSION && prevCTime &lt;= namenode.cTime.
		/// Upgrade if this.LV &gt; LAYOUT_VERSION || this.cTime &lt; namenode.cTime Regular
		/// startup if this.LV = LAYOUT_VERSION && this.cTime = namenode.cTime
		/// </remarks>
		/// <param name="sd">storage directory <SD>/current/<bpid></param>
		/// <param name="nsInfo">namespace info</param>
		/// <param name="startOpt">startup option</param>
		/// <exception cref="System.IO.IOException"/>
		private void DoTransition(DataNode datanode, Storage.StorageDirectory sd, NamespaceInfo
			 nsInfo, HdfsServerConstants.StartupOption startOpt)
		{
			if (startOpt == HdfsServerConstants.StartupOption.Rollback && sd.GetPreviousDir()
				.Exists())
			{
				Preconditions.CheckState(!GetTrashRootDir(sd).Exists(), sd.GetPreviousDir() + " and "
					 + GetTrashRootDir(sd) + " should not " + " both be present.");
				DoRollback(sd, nsInfo);
			}
			else
			{
				// rollback if applicable
				if (startOpt == HdfsServerConstants.StartupOption.Rollback && !sd.GetPreviousDir(
					).Exists())
				{
					// Restore all the files in the trash. The restored files are retained
					// during rolling upgrade rollback. They are deleted during rolling
					// upgrade downgrade.
					int restored = RestoreBlockFilesFromTrash(GetTrashRootDir(sd));
					Log.Info("Restored " + restored + " block files from trash.");
				}
			}
			ReadProperties(sd);
			CheckVersionUpgradable(this.layoutVersion);
			System.Diagnostics.Debug.Assert(this.layoutVersion >= HdfsConstants.DatanodeLayoutVersion
				, "Future version is not allowed");
			if (GetNamespaceID() != nsInfo.GetNamespaceID())
			{
				throw new IOException("Incompatible namespaceIDs in " + sd.GetRoot().GetCanonicalPath
					() + ": namenode namespaceID = " + nsInfo.GetNamespaceID() + "; datanode namespaceID = "
					 + GetNamespaceID());
			}
			if (!blockpoolID.Equals(nsInfo.GetBlockPoolID()))
			{
				throw new IOException("Incompatible blockpoolIDs in " + sd.GetRoot().GetCanonicalPath
					() + ": namenode blockpoolID = " + nsInfo.GetBlockPoolID() + "; datanode blockpoolID = "
					 + blockpoolID);
			}
			if (this.layoutVersion == HdfsConstants.DatanodeLayoutVersion && this.cTime == nsInfo
				.GetCTime())
			{
				return;
			}
			// regular startup
			if (this.layoutVersion > HdfsConstants.DatanodeLayoutVersion)
			{
				int restored = RestoreBlockFilesFromTrash(GetTrashRootDir(sd));
				Log.Info("Restored " + restored + " block files from trash " + "before the layout upgrade. These blocks will be moved to "
					 + "the previous directory during the upgrade");
			}
			if (this.layoutVersion > HdfsConstants.DatanodeLayoutVersion || this.cTime < nsInfo
				.GetCTime())
			{
				DoUpgrade(datanode, sd, nsInfo);
				// upgrade
				return;
			}
			// layoutVersion == LAYOUT_VERSION && this.cTime > nsInfo.cTime
			// must shutdown
			throw new IOException("Datanode state: LV = " + this.GetLayoutVersion() + " CTime = "
				 + this.GetCTime() + " is newer than the namespace state: LV = " + nsInfo.GetLayoutVersion
				() + " CTime = " + nsInfo.GetCTime());
		}

		/// <summary>Upgrade to any release after 0.22 (0.22 included) release e.g.</summary>
		/// <remarks>
		/// Upgrade to any release after 0.22 (0.22 included) release e.g. 0.22 =&gt; 0.23
		/// Upgrade procedure is as follows:
		/// <ol>
		/// <li>If <SD>/current/<bpid>/previous exists then delete it</li>
		/// <li>Rename <SD>/current/<bpid>/current to
		/// <SD>/current/bpid/current/previous.tmp</li>
		/// <li>Create new <SD>current/<bpid>/current directory</li>
		/// <ol>
		/// <li>Hard links for block files are created from previous.tmp to current</li>
		/// <li>Save new version file in current directory</li>
		/// </ol>
		/// <li>Rename previous.tmp to previous</li> </ol>
		/// </remarks>
		/// <param name="bpSd">storage directory <SD>/current/<bpid></param>
		/// <param name="nsInfo">Namespace Info from the namenode</param>
		/// <exception cref="System.IO.IOException">on error</exception>
		internal virtual void DoUpgrade(DataNode datanode, Storage.StorageDirectory bpSd, 
			NamespaceInfo nsInfo)
		{
			// Upgrading is applicable only to release with federation or after
			if (!DataNodeLayoutVersion.Supports(LayoutVersion.Feature.Federation, layoutVersion
				))
			{
				return;
			}
			Log.Info("Upgrading block pool storage directory " + bpSd.GetRoot() + ".\n   old LV = "
				 + this.GetLayoutVersion() + "; old CTime = " + this.GetCTime() + ".\n   new LV = "
				 + HdfsConstants.DatanodeLayoutVersion + "; new CTime = " + nsInfo.GetCTime());
			// get <SD>/previous directory
			string dnRoot = GetDataNodeStorageRoot(bpSd.GetRoot().GetCanonicalPath());
			Storage.StorageDirectory dnSdStorage = new Storage.StorageDirectory(new FilePath(
				dnRoot));
			FilePath dnPrevDir = dnSdStorage.GetPreviousDir();
			// If <SD>/previous directory exists delete it
			if (dnPrevDir.Exists())
			{
				DeleteDir(dnPrevDir);
			}
			FilePath bpCurDir = bpSd.GetCurrentDir();
			FilePath bpPrevDir = bpSd.GetPreviousDir();
			System.Diagnostics.Debug.Assert(bpCurDir.Exists(), "BP level current directory must exist."
				);
			CleanupDetachDir(new FilePath(bpCurDir, DataStorage.StorageDirDetached));
			// 1. Delete <SD>/current/<bpid>/previous dir before upgrading
			if (bpPrevDir.Exists())
			{
				DeleteDir(bpPrevDir);
			}
			FilePath bpTmpDir = bpSd.GetPreviousTmp();
			System.Diagnostics.Debug.Assert(!bpTmpDir.Exists(), "previous.tmp directory must not exist."
				);
			// 2. Rename <SD>/current/<bpid>/current to
			//    <SD>/current/<bpid>/previous.tmp
			Rename(bpCurDir, bpTmpDir);
			// 3. Create new <SD>/current with block files hardlinks and VERSION
			LinkAllBlocks(datanode, bpTmpDir, bpCurDir);
			this.layoutVersion = HdfsConstants.DatanodeLayoutVersion;
			System.Diagnostics.Debug.Assert(this.namespaceID == nsInfo.GetNamespaceID(), "Data-node and name-node layout versions must be the same."
				);
			this.cTime = nsInfo.GetCTime();
			WriteProperties(bpSd);
			// 4.rename <SD>/current/<bpid>/previous.tmp to
			// <SD>/current/<bpid>/previous
			Rename(bpTmpDir, bpPrevDir);
			Log.Info("Upgrade of block pool " + blockpoolID + " at " + bpSd.GetRoot() + " is complete"
				);
		}

		/// <summary>Cleanup the detachDir.</summary>
		/// <remarks>
		/// Cleanup the detachDir.
		/// If the directory is not empty report an error; Otherwise remove the
		/// directory.
		/// </remarks>
		/// <param name="detachDir">detach directory</param>
		/// <exception cref="System.IO.IOException">if the directory is not empty or it can not be removed
		/// 	</exception>
		private void CleanupDetachDir(FilePath detachDir)
		{
			if (!DataNodeLayoutVersion.Supports(LayoutVersion.Feature.AppendRbwDir, layoutVersion
				) && detachDir.Exists() && detachDir.IsDirectory())
			{
				if (FileUtil.List(detachDir).Length != 0)
				{
					throw new IOException("Detached directory " + detachDir + " is not empty. Please manually move each file under this "
						 + "directory to the finalized directory if the finalized " + "directory tree does not have the file."
						);
				}
				else
				{
					if (!detachDir.Delete())
					{
						throw new IOException("Cannot remove directory " + detachDir);
					}
				}
			}
		}

		/// <summary>
		/// Restore all files from the trash directory to their corresponding
		/// locations under current/
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		private int RestoreBlockFilesFromTrash(FilePath trashRoot)
		{
			int filesRestored = 0;
			FilePath[] children = trashRoot.Exists() ? trashRoot.ListFiles() : null;
			if (children == null)
			{
				return 0;
			}
			FilePath restoreDirectory = null;
			foreach (FilePath child in children)
			{
				if (child.IsDirectory())
				{
					// Recurse to process subdirectories.
					filesRestored += RestoreBlockFilesFromTrash(child);
					continue;
				}
				if (restoreDirectory == null)
				{
					restoreDirectory = new FilePath(GetRestoreDirectory(child));
					if (!restoreDirectory.Exists() && !restoreDirectory.Mkdirs())
					{
						throw new IOException("Failed to create directory " + restoreDirectory);
					}
				}
				FilePath newChild = new FilePath(restoreDirectory, child.GetName());
				if (newChild.Exists() && newChild.Length() >= child.Length())
				{
					// Failsafe - we should not hit this case but let's make sure
					// we never overwrite a newer version of a block file with an
					// older version.
					Log.Info("Not overwriting " + newChild + " with smaller file from " + "trash directory. This message can be safely ignored."
						);
				}
				else
				{
					if (!child.RenameTo(newChild))
					{
						throw new IOException("Failed to rename " + child + " to " + newChild);
					}
					else
					{
						++filesRestored;
					}
				}
			}
			FileUtil.FullyDelete(trashRoot);
			return filesRestored;
		}

		/*
		* Roll back to old snapshot at the block pool level
		* If previous directory exists:
		* <ol>
		* <li>Rename <SD>/current/<bpid>/current to removed.tmp</li>
		* <li>Rename * <SD>/current/<bpid>/previous to current</li>
		* <li>Remove removed.tmp</li>
		* </ol>
		*
		* Do nothing if previous directory does not exist.
		* @param bpSd Block pool storage directory at <SD>/current/<bpid>
		*/
		/// <exception cref="System.IO.IOException"/>
		internal virtual void DoRollback(Storage.StorageDirectory bpSd, NamespaceInfo nsInfo
			)
		{
			FilePath prevDir = bpSd.GetPreviousDir();
			// regular startup if previous dir does not exist
			if (!prevDir.Exists())
			{
				return;
			}
			// read attributes out of the VERSION file of previous directory
			Org.Apache.Hadoop.Hdfs.Server.Datanode.BlockPoolSliceStorage prevInfo = new Org.Apache.Hadoop.Hdfs.Server.Datanode.BlockPoolSliceStorage
				();
			prevInfo.ReadPreviousVersionProperties(bpSd);
			// We allow rollback to a state, which is either consistent with
			// the namespace state or can be further upgraded to it.
			// In another word, we can only roll back when ( storedLV >= software LV)
			// && ( DN.previousCTime <= NN.ctime)
			if (!(prevInfo.GetLayoutVersion() >= HdfsConstants.DatanodeLayoutVersion && prevInfo
				.GetCTime() <= nsInfo.GetCTime()))
			{
				// cannot rollback
				throw new InconsistentFSStateException(bpSd.GetRoot(), "Cannot rollback to a newer state.\nDatanode previous state: LV = "
					 + prevInfo.GetLayoutVersion() + " CTime = " + prevInfo.GetCTime() + " is newer than the namespace state: LV = "
					 + HdfsConstants.DatanodeLayoutVersion + " CTime = " + nsInfo.GetCTime());
			}
			Log.Info("Rolling back storage directory " + bpSd.GetRoot() + ".\n   target LV = "
				 + nsInfo.GetLayoutVersion() + "; target CTime = " + nsInfo.GetCTime());
			FilePath tmpDir = bpSd.GetRemovedTmp();
			System.Diagnostics.Debug.Assert(!tmpDir.Exists(), "removed.tmp directory must not exist."
				);
			// 1. rename current to tmp
			FilePath curDir = bpSd.GetCurrentDir();
			System.Diagnostics.Debug.Assert(curDir.Exists(), "Current directory must exist.");
			Rename(curDir, tmpDir);
			// 2. rename previous to current
			Rename(prevDir, curDir);
			// 3. delete removed.tmp dir
			DeleteDir(tmpDir);
			Log.Info("Rollback of " + bpSd.GetRoot() + " is complete");
		}

		/*
		* Finalize the block pool storage by deleting <BP>/previous directory
		* that holds the snapshot.
		*/
		/// <exception cref="System.IO.IOException"/>
		internal virtual void DoFinalize(FilePath dnCurDir)
		{
			FilePath bpRoot = GetBpRoot(blockpoolID, dnCurDir);
			Storage.StorageDirectory bpSd = new Storage.StorageDirectory(bpRoot);
			// block pool level previous directory
			FilePath prevDir = bpSd.GetPreviousDir();
			if (!prevDir.Exists())
			{
				return;
			}
			// already finalized
			string dataDirPath = bpSd.GetRoot().GetCanonicalPath();
			Log.Info("Finalizing upgrade for storage directory " + dataDirPath + ".\n   cur LV = "
				 + this.GetLayoutVersion() + "; cur CTime = " + this.GetCTime());
			System.Diagnostics.Debug.Assert(bpSd.GetCurrentDir().Exists(), "Current directory must exist."
				);
			// rename previous to finalized.tmp
			FilePath tmpDir = bpSd.GetFinalizedTmp();
			Rename(prevDir, tmpDir);
			// delete finalized.tmp dir in a separate thread
			new Daemon(new _Runnable_618(tmpDir, dataDirPath)).Start();
		}

		private sealed class _Runnable_618 : Runnable
		{
			public _Runnable_618(FilePath tmpDir, string dataDirPath)
			{
				this.tmpDir = tmpDir;
				this.dataDirPath = dataDirPath;
			}

			public void Run()
			{
				try
				{
					Storage.DeleteDir(tmpDir);
				}
				catch (IOException ex)
				{
					Storage.Log.Error("Finalize upgrade for " + dataDirPath + " failed.", ex);
				}
				Storage.Log.Info("Finalize upgrade for " + dataDirPath + " is complete.");
			}

			public override string ToString()
			{
				return "Finalize " + dataDirPath;
			}

			private readonly FilePath tmpDir;

			private readonly string dataDirPath;
		}

		/// <summary>Hardlink all finalized and RBW blocks in fromDir to toDir</summary>
		/// <param name="fromDir">directory where the snapshot is stored</param>
		/// <param name="toDir">the current data directory</param>
		/// <exception cref="System.IO.IOException">if error occurs during hardlink</exception>
		private void LinkAllBlocks(DataNode datanode, FilePath fromDir, FilePath toDir)
		{
			// do the link
			int diskLayoutVersion = this.GetLayoutVersion();
			// hardlink finalized blocks in tmpDir
			HardLink hardLink = new HardLink();
			DataStorage.LinkBlocks(datanode, new FilePath(fromDir, DataStorage.StorageDirFinalized
				), new FilePath(toDir, DataStorage.StorageDirFinalized), diskLayoutVersion, hardLink
				);
			DataStorage.LinkBlocks(datanode, new FilePath(fromDir, DataStorage.StorageDirRbw)
				, new FilePath(toDir, DataStorage.StorageDirRbw), diskLayoutVersion, hardLink);
			Log.Info(hardLink.linkStats.Report());
		}

		/// <summary>gets the data node storage directory based on block pool storage</summary>
		private static string GetDataNodeStorageRoot(string bpRoot)
		{
			Matcher matcher = BlockPoolPathPattern.Matcher(bpRoot);
			if (matcher.Matches())
			{
				// return the data node root directory
				return matcher.Group(1);
			}
			return bpRoot;
		}

		public override string ToString()
		{
			return base.ToString() + ";bpid=" + blockpoolID;
		}

		/// <summary>Get a block pool storage root based on data node storage root</summary>
		/// <param name="bpID">block pool ID</param>
		/// <param name="dnCurDir">data node storage root directory</param>
		/// <returns>root directory for block pool storage</returns>
		public static FilePath GetBpRoot(string bpID, FilePath dnCurDir)
		{
			return new FilePath(dnCurDir, bpID);
		}

		/// <exception cref="System.IO.IOException"/>
		public override bool IsPreUpgradableLayout(Storage.StorageDirectory sd)
		{
			return false;
		}

		private FilePath GetTrashRootDir(Storage.StorageDirectory sd)
		{
			return new FilePath(sd.GetRoot(), TrashRootDir);
		}

		/// <summary>Determine whether we can use trash for the given blockFile.</summary>
		/// <remarks>
		/// Determine whether we can use trash for the given blockFile. Trash
		/// is disallowed if a 'previous' directory exists for the
		/// storage directory containing the block.
		/// </remarks>
		[VisibleForTesting]
		public virtual bool IsTrashAllowed(FilePath blockFile)
		{
			Matcher matcher = BlockPoolCurrentPathPattern.Matcher(blockFile.GetParent());
			string previousDir = matcher.ReplaceFirst("$1$2" + StorageDirPrevious);
			return !(new FilePath(previousDir)).Exists();
		}

		/// <summary>
		/// Get a target subdirectory under trash/ for a given block file that is being
		/// deleted.
		/// </summary>
		/// <remarks>
		/// Get a target subdirectory under trash/ for a given block file that is being
		/// deleted.
		/// The subdirectory structure under trash/ mirrors that under current/ to keep
		/// implicit memory of where the files are to be restored (if necessary).
		/// </remarks>
		/// <returns>the trash directory for a given block file that is being deleted.</returns>
		public virtual string GetTrashDirectory(FilePath blockFile)
		{
			if (IsTrashAllowed(blockFile))
			{
				Matcher matcher = BlockPoolCurrentPathPattern.Matcher(blockFile.GetParent());
				string trashDirectory = matcher.ReplaceFirst("$1$2" + TrashRootDir + "$4");
				return trashDirectory;
			}
			return null;
		}

		/// <summary>
		/// Get a target subdirectory under current/ for a given block file that is being
		/// restored from trash.
		/// </summary>
		/// <remarks>
		/// Get a target subdirectory under current/ for a given block file that is being
		/// restored from trash.
		/// The subdirectory structure under trash/ mirrors that under current/ to keep
		/// implicit memory of where the files are to be restored.
		/// </remarks>
		/// <returns>the target directory to restore a previously deleted block file.</returns>
		[VisibleForTesting]
		internal virtual string GetRestoreDirectory(FilePath blockFile)
		{
			Matcher matcher = BlockPoolTrashPathPattern.Matcher(blockFile.GetParent());
			string restoreDirectory = matcher.ReplaceFirst("$1$2" + StorageDirCurrent + "$4");
			Log.Info("Restoring " + blockFile + " to " + restoreDirectory);
			return restoreDirectory;
		}

		/// <summary>Delete all files and directories in the trash directories.</summary>
		public virtual void ClearTrash()
		{
			IList<FilePath> trashRoots = new AList<FilePath>();
			foreach (Storage.StorageDirectory sd in storageDirs)
			{
				FilePath trashRoot = GetTrashRootDir(sd);
				if (trashRoot.Exists() && sd.GetPreviousDir().Exists())
				{
					Log.Error("Trash and PreviousDir shouldn't both exist for storage " + "directory "
						 + sd);
					System.Diagnostics.Debug.Assert(false);
				}
				else
				{
					trashRoots.AddItem(trashRoot);
				}
			}
			StopTrashCleaner();
			trashCleaner = new Daemon(new _Runnable_756(this, trashRoots));
			trashCleaner.Start();
		}

		private sealed class _Runnable_756 : Runnable
		{
			public _Runnable_756(BlockPoolSliceStorage _enclosing, IList<FilePath> trashRoots
				)
			{
				this._enclosing = _enclosing;
				this.trashRoots = trashRoots;
			}

			public void Run()
			{
				foreach (FilePath trashRoot in trashRoots)
				{
					FileUtil.FullyDelete(trashRoot);
					Storage.Log.Info("Cleared trash for storage directory " + trashRoot);
				}
			}

			public override string ToString()
			{
				return "clearTrash() for " + this._enclosing.blockpoolID;
			}

			private readonly BlockPoolSliceStorage _enclosing;

			private readonly IList<FilePath> trashRoots;
		}

		public virtual void StopTrashCleaner()
		{
			if (trashCleaner != null)
			{
				trashCleaner.Interrupt();
			}
		}

		/// <summary>trash is enabled if at least one storage directory contains trash root</summary>
		[VisibleForTesting]
		public virtual bool TrashEnabled()
		{
			foreach (Storage.StorageDirectory sd in storageDirs)
			{
				if (GetTrashRootDir(sd).Exists())
				{
					return true;
				}
			}
			return false;
		}

		/// <summary>
		/// Create a rolling upgrade marker file for each BP storage root, if it
		/// does not exist already.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void SetRollingUpgradeMarkers(IList<Storage.StorageDirectory> dnStorageDirs
			)
		{
			foreach (Storage.StorageDirectory sd in dnStorageDirs)
			{
				FilePath bpRoot = GetBpRoot(blockpoolID, sd.GetCurrentDir());
				FilePath markerFile = new FilePath(bpRoot, RollingUpgradeMarkerFile);
				if (!storagesWithRollingUpgradeMarker.Contains(bpRoot.ToString()))
				{
					if (!markerFile.Exists() && markerFile.CreateNewFile())
					{
						Log.Info("Created " + markerFile);
					}
					else
					{
						Log.Info(markerFile + " already exists.");
					}
					storagesWithRollingUpgradeMarker.AddItem(bpRoot.ToString());
					storagesWithoutRollingUpgradeMarker.Remove(bpRoot.ToString());
				}
			}
		}

		/// <summary>
		/// Check whether the rolling upgrade marker file exists for each BP storage
		/// root.
		/// </summary>
		/// <remarks>
		/// Check whether the rolling upgrade marker file exists for each BP storage
		/// root. If it does exist, then the marker file is cleared and more
		/// importantly the layout upgrade is finalized.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public virtual void ClearRollingUpgradeMarkers(IList<Storage.StorageDirectory> dnStorageDirs
			)
		{
			foreach (Storage.StorageDirectory sd in dnStorageDirs)
			{
				FilePath bpRoot = GetBpRoot(blockpoolID, sd.GetCurrentDir());
				FilePath markerFile = new FilePath(bpRoot, RollingUpgradeMarkerFile);
				if (!storagesWithoutRollingUpgradeMarker.Contains(bpRoot.ToString()))
				{
					if (markerFile.Exists())
					{
						Log.Info("Deleting " + markerFile);
						DoFinalize(sd.GetCurrentDir());
						if (!markerFile.Delete())
						{
							Log.Warn("Failed to delete " + markerFile);
						}
					}
					storagesWithoutRollingUpgradeMarker.AddItem(bpRoot.ToString());
					storagesWithRollingUpgradeMarker.Remove(bpRoot.ToString());
				}
			}
		}
	}
}
