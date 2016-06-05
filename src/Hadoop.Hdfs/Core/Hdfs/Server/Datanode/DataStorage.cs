using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Com.Google.Common.Annotations;
using Com.Google.Common.Collect;
using Com.Google.Common.Util.Concurrent;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.IO.Nativeio;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Datanode
{
	/// <summary>Data storage information file.</summary>
	/// <remarks>
	/// Data storage information file.
	/// <p>
	/// </remarks>
	/// <seealso cref="Org.Apache.Hadoop.Hdfs.Server.Common.Storage"/>
	public class DataStorage : Storage
	{
		public const string BlockSubdirPrefix = "subdir";

		internal const string CopyFilePrefix = "dncp_";

		internal const string StorageDirDetached = "detach";

		public const string StorageDirRbw = "rbw";

		public const string StorageDirFinalized = "finalized";

		public const string StorageDirLazyPersist = "lazypersist";

		public const string StorageDirTmp = "tmp";

		/// <summary>Set of bpids for which 'trash' is currently enabled.</summary>
		/// <remarks>
		/// Set of bpids for which 'trash' is currently enabled.
		/// When trash is enabled block files are moved under a separate
		/// 'trash' folder instead of being deleted right away. This can
		/// be useful during rolling upgrades, for example.
		/// The set is backed by a concurrent HashMap.
		/// Even if trash is enabled, it is not used if a layout upgrade
		/// is in progress for a storage directory i.e. if the previous
		/// directory exists.
		/// </remarks>
		private ICollection<string> trashEnabledBpids;

		/// <summary>Datanode UUID that this storage is currently attached to.</summary>
		/// <remarks>
		/// Datanode UUID that this storage is currently attached to. This
		/// is the same as the legacy StorageID for datanodes that were
		/// upgraded from a pre-UUID version. For compatibility with prior
		/// versions of Datanodes we cannot make this field a UUID.
		/// </remarks>
		private string datanodeUuid = null;

		private bool initialized = false;

		private readonly IDictionary<string, BlockPoolSliceStorage> bpStorageMap = Sharpen.Collections
			.SynchronizedMap(new Dictionary<string, BlockPoolSliceStorage>());

		internal DataStorage()
			: base(HdfsServerConstants.NodeType.DataNode)
		{
			// Flag to ensure we only initialize storage once
			// Maps block pool IDs to block pool storage
			trashEnabledBpids = Sharpen.Collections.NewSetFromMap(new ConcurrentHashMap<string
				, bool>());
		}

		public virtual BlockPoolSliceStorage GetBPStorage(string bpid)
		{
			return bpStorageMap[bpid];
		}

		public DataStorage(StorageInfo storageInfo)
			: base(storageInfo)
		{
		}

		public virtual string GetDatanodeUuid()
		{
			lock (this)
			{
				return datanodeUuid;
			}
		}

		public virtual void SetDatanodeUuid(string newDatanodeUuid)
		{
			lock (this)
			{
				this.datanodeUuid = newDatanodeUuid;
			}
		}

		/// <summary>Create an ID for this storage.</summary>
		/// <returns>true if a new storage ID was generated.</returns>
		public virtual bool CreateStorageID(Storage.StorageDirectory sd, bool regenerateStorageIds
			)
		{
			lock (this)
			{
				string oldStorageID = sd.GetStorageUuid();
				if (oldStorageID == null || regenerateStorageIds)
				{
					sd.SetStorageUuid(DatanodeStorage.GenerateUuid());
					Log.Info("Generated new storageID " + sd.GetStorageUuid() + " for directory " + sd
						.GetRoot() + (oldStorageID == null ? string.Empty : (" to replace " + oldStorageID
						)));
					return true;
				}
				return false;
			}
		}

		/// <summary>Enable trash for the specified block pool storage.</summary>
		/// <remarks>
		/// Enable trash for the specified block pool storage. Even if trash is
		/// enabled by the caller, it is superseded by the 'previous' directory
		/// if a layout upgrade is in progress.
		/// </remarks>
		public virtual void EnableTrash(string bpid)
		{
			if (trashEnabledBpids.AddItem(bpid))
			{
				GetBPStorage(bpid).StopTrashCleaner();
				Log.Info("Enabled trash for bpid " + bpid);
			}
		}

		public virtual void ClearTrash(string bpid)
		{
			if (trashEnabledBpids.Contains(bpid))
			{
				GetBPStorage(bpid).ClearTrash();
				trashEnabledBpids.Remove(bpid);
				Log.Info("Cleared trash for bpid " + bpid);
			}
		}

		public virtual bool TrashEnabled(string bpid)
		{
			return trashEnabledBpids.Contains(bpid);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void SetRollingUpgradeMarker(string bpid)
		{
			GetBPStorage(bpid).SetRollingUpgradeMarkers(storageDirs);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void ClearRollingUpgradeMarker(string bpid)
		{
			GetBPStorage(bpid).ClearRollingUpgradeMarkers(storageDirs);
		}

		/// <summary>
		/// If rolling upgrades are in progress then do not delete block files
		/// immediately.
		/// </summary>
		/// <remarks>
		/// If rolling upgrades are in progress then do not delete block files
		/// immediately. Instead we move the block files to an intermediate
		/// 'trash' directory. If there is a subsequent rollback, then the block
		/// files will be restored from trash.
		/// </remarks>
		/// <returns>
		/// trash directory if rolling upgrade is in progress, null
		/// otherwise.
		/// </returns>
		public virtual string GetTrashDirectoryForBlockFile(string bpid, FilePath blockFile
			)
		{
			if (trashEnabledBpids.Contains(bpid))
			{
				return GetBPStorage(bpid).GetTrashDirectory(blockFile);
			}
			return null;
		}

		/// <summary>
		/// VolumeBuilder holds the metadata (e.g., the storage directories) of the
		/// prepared volume returned from
		/// <see>prepareVolume()</see>
		/// . Calling
		/// <see>build()</see>
		/// to add the metadata to
		/// <see cref="DataStorage"/>
		/// so that this prepared volume can
		/// be active.
		/// </summary>
		public class VolumeBuilder
		{
			private DataStorage storage;

			/// <summary>Volume level storage directory.</summary>
			private Storage.StorageDirectory sd;

			/// <summary>Mapping from block pool ID to an array of storage directories.</summary>
			private IDictionary<string, IList<Storage.StorageDirectory>> bpStorageDirMap = Maps
				.NewHashMap();

			[VisibleForTesting]
			public VolumeBuilder(DataStorage storage, Storage.StorageDirectory sd)
			{
				this.storage = storage;
				this.sd = sd;
			}

			public Storage.StorageDirectory GetStorageDirectory()
			{
				return this.sd;
			}

			private void AddBpStorageDirectories(string bpid, IList<Storage.StorageDirectory>
				 dirs)
			{
				bpStorageDirMap[bpid] = dirs;
			}

			/// <summary>
			/// Add loaded metadata of a data volume to
			/// <see cref="DataStorage"/>
			/// .
			/// </summary>
			public virtual void Build()
			{
				System.Diagnostics.Debug.Assert(this.sd != null);
				lock (storage)
				{
					foreach (KeyValuePair<string, IList<Storage.StorageDirectory>> e in bpStorageDirMap)
					{
						string bpid = e.Key;
						BlockPoolSliceStorage bpStorage = this.storage.bpStorageMap[bpid];
						System.Diagnostics.Debug.Assert(bpStorage != null);
						foreach (Storage.StorageDirectory bpSd in e.Value)
						{
							bpStorage.AddStorageDir(bpSd);
						}
					}
					storage.AddStorageDir(sd);
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private Storage.StorageDirectory LoadStorageDirectory(DataNode datanode, NamespaceInfo
			 nsInfo, FilePath dataDir, HdfsServerConstants.StartupOption startOpt)
		{
			Storage.StorageDirectory sd = new Storage.StorageDirectory(dataDir, null, false);
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
						Log.Info("Storage directory " + dataDir + " does not exist");
						throw new IOException("Storage directory " + dataDir + " does not exist");
					}

					case Storage.StorageState.NotFormatted:
					{
						// format
						Log.Info("Storage directory " + dataDir + " is not formatted for " + nsInfo.GetBlockPoolID
							());
						Log.Info("Formatting ...");
						Format(sd, nsInfo, datanode.GetDatanodeUuid());
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

		/// <summary>Prepare a storage directory.</summary>
		/// <remarks>
		/// Prepare a storage directory. It creates a builder which can be used to add
		/// to the volume. If the volume cannot be added, it is OK to discard the
		/// builder later.
		/// </remarks>
		/// <param name="datanode">DataNode object.</param>
		/// <param name="volume">the root path of a storage directory.</param>
		/// <param name="nsInfos">an array of namespace infos.</param>
		/// <returns>
		/// a VolumeBuilder that holds the metadata of this storage directory
		/// and can be added to DataStorage later.
		/// </returns>
		/// <exception cref="System.IO.IOException">
		/// if encounters I/O errors.
		/// Note that if there is IOException, the state of DataStorage is not modified.
		/// </exception>
		public virtual DataStorage.VolumeBuilder PrepareVolume(DataNode datanode, FilePath
			 volume, IList<NamespaceInfo> nsInfos)
		{
			if (ContainsStorageDir(volume))
			{
				string errorMessage = "Storage directory is in use";
				Log.Warn(errorMessage + ".");
				throw new IOException(errorMessage);
			}
			Storage.StorageDirectory sd = LoadStorageDirectory(datanode, nsInfos[0], volume, 
				HdfsServerConstants.StartupOption.Hotswap);
			DataStorage.VolumeBuilder builder = new DataStorage.VolumeBuilder(this, sd);
			foreach (NamespaceInfo nsInfo in nsInfos)
			{
				IList<FilePath> bpDataDirs = Lists.NewArrayList();
				bpDataDirs.AddItem(BlockPoolSliceStorage.GetBpRoot(nsInfo.GetBlockPoolID(), new FilePath
					(volume, StorageDirCurrent)));
				MakeBlockPoolDataDir(bpDataDirs, null);
				BlockPoolSliceStorage bpStorage;
				string bpid = nsInfo.GetBlockPoolID();
				lock (this)
				{
					bpStorage = this.bpStorageMap[bpid];
					if (bpStorage == null)
					{
						bpStorage = new BlockPoolSliceStorage(nsInfo.GetNamespaceID(), bpid, nsInfo.GetCTime
							(), nsInfo.GetClusterID());
						AddBlockPoolStorage(bpid, bpStorage);
					}
				}
				builder.AddBpStorageDirectories(bpid, bpStorage.LoadBpStorageDirectories(datanode
					, nsInfo, bpDataDirs, HdfsServerConstants.StartupOption.Hotswap));
			}
			return builder;
		}

		/// <summary>Add a list of volumes to be managed by DataStorage.</summary>
		/// <remarks>
		/// Add a list of volumes to be managed by DataStorage. If the volume is empty,
		/// format it, otherwise recover it from previous transitions if required.
		/// </remarks>
		/// <param name="datanode">the reference to DataNode.</param>
		/// <param name="nsInfo">namespace information</param>
		/// <param name="dataDirs">array of data storage directories</param>
		/// <param name="startOpt">startup option</param>
		/// <returns>a list of successfully loaded volumes.</returns>
		/// <exception cref="System.IO.IOException"/>
		[VisibleForTesting]
		internal virtual IList<StorageLocation> AddStorageLocations(DataNode datanode, NamespaceInfo
			 nsInfo, ICollection<StorageLocation> dataDirs, HdfsServerConstants.StartupOption
			 startOpt)
		{
			lock (this)
			{
				string bpid = nsInfo.GetBlockPoolID();
				IList<StorageLocation> successVolumes = Lists.NewArrayList();
				foreach (StorageLocation dataDir in dataDirs)
				{
					FilePath root = dataDir.GetFile();
					if (!ContainsStorageDir(root))
					{
						try
						{
							// It first ensures the datanode level format is completed.
							Storage.StorageDirectory sd = LoadStorageDirectory(datanode, nsInfo, root, startOpt
								);
							AddStorageDir(sd);
						}
						catch (IOException e)
						{
							Log.Warn(e);
							continue;
						}
					}
					else
					{
						Log.Info("Storage directory " + dataDir + " has already been used.");
					}
					IList<FilePath> bpDataDirs = new AList<FilePath>();
					bpDataDirs.AddItem(BlockPoolSliceStorage.GetBpRoot(bpid, new FilePath(root, StorageDirCurrent
						)));
					try
					{
						MakeBlockPoolDataDir(bpDataDirs, null);
						BlockPoolSliceStorage bpStorage = this.bpStorageMap[bpid];
						if (bpStorage == null)
						{
							bpStorage = new BlockPoolSliceStorage(nsInfo.GetNamespaceID(), bpid, nsInfo.GetCTime
								(), nsInfo.GetClusterID());
						}
						bpStorage.RecoverTransitionRead(datanode, nsInfo, bpDataDirs, startOpt);
						AddBlockPoolStorage(bpid, bpStorage);
					}
					catch (IOException e)
					{
						Log.Warn("Failed to add storage for block pool: " + bpid + " : " + e.Message);
						continue;
					}
					successVolumes.AddItem(dataDir);
				}
				return successVolumes;
			}
		}

		/// <summary>Remove storage dirs from DataStorage.</summary>
		/// <remarks>
		/// Remove storage dirs from DataStorage. All storage dirs are removed even when the
		/// IOException is thrown.
		/// </remarks>
		/// <param name="dirsToRemove">a set of storage directories to be removed.</param>
		/// <exception cref="System.IO.IOException">if I/O error when unlocking storage directory.
		/// 	</exception>
		internal virtual void RemoveVolumes(ICollection<FilePath> dirsToRemove)
		{
			lock (this)
			{
				if (dirsToRemove.IsEmpty())
				{
					return;
				}
				StringBuilder errorMsgBuilder = new StringBuilder();
				for (IEnumerator<Storage.StorageDirectory> it = this.storageDirs.GetEnumerator(); 
					it.HasNext(); )
				{
					Storage.StorageDirectory sd = it.Next();
					if (dirsToRemove.Contains(sd.GetRoot()))
					{
						// Remove the block pool level storage first.
						foreach (KeyValuePair<string, BlockPoolSliceStorage> entry in this.bpStorageMap)
						{
							string bpid = entry.Key;
							BlockPoolSliceStorage bpsStorage = entry.Value;
							FilePath bpRoot = BlockPoolSliceStorage.GetBpRoot(bpid, sd.GetCurrentDir());
							bpsStorage.Remove(bpRoot.GetAbsoluteFile());
						}
						it.Remove();
						try
						{
							sd.Unlock();
						}
						catch (IOException e)
						{
							Log.Warn(string.Format("I/O error attempting to unlock storage directory %s.", sd
								.GetRoot()), e);
							errorMsgBuilder.Append(string.Format("Failed to remove %s: %s%n", sd.GetRoot(), e
								.Message));
						}
					}
				}
				if (errorMsgBuilder.Length > 0)
				{
					throw new IOException(errorMsgBuilder.ToString());
				}
			}
		}

		/// <summary>Analyze storage directories for a specific block pool.</summary>
		/// <remarks>
		/// Analyze storage directories for a specific block pool.
		/// Recover from previous transitions if required.
		/// Perform fs state transition if necessary depending on the namespace info.
		/// Read storage info.
		/// <br />
		/// This method should be synchronized between multiple DN threads.  Only the
		/// first DN thread does DN level storage dir recoverTransitionRead.
		/// </remarks>
		/// <param name="datanode">DataNode</param>
		/// <param name="nsInfo">Namespace info of namenode corresponding to the block pool</param>
		/// <param name="dataDirs">Storage directories</param>
		/// <param name="startOpt">startup option</param>
		/// <exception cref="System.IO.IOException">on error</exception>
		internal virtual void RecoverTransitionRead(DataNode datanode, NamespaceInfo nsInfo
			, ICollection<StorageLocation> dataDirs, HdfsServerConstants.StartupOption startOpt
			)
		{
			if (this.initialized)
			{
				Log.Info("DataNode version: " + HdfsConstants.DatanodeLayoutVersion + " and NameNode layout version: "
					 + nsInfo.GetLayoutVersion());
				this.storageDirs = new AList<Storage.StorageDirectory>(dataDirs.Count);
				// mark DN storage is initialized
				this.initialized = true;
			}
			if (AddStorageLocations(datanode, nsInfo, dataDirs, startOpt).IsEmpty())
			{
				throw new IOException("All specified directories are failed to load.");
			}
		}

		/// <summary>Create physical directory for block pools on the data node</summary>
		/// <param name="dataDirs">List of data directories</param>
		/// <param name="conf">Configuration instance to use.</param>
		/// <exception cref="System.IO.IOException">on errors</exception>
		internal static void MakeBlockPoolDataDir(ICollection<FilePath> dataDirs, Configuration
			 conf)
		{
			if (conf == null)
			{
				conf = new HdfsConfiguration();
			}
			LocalFileSystem localFS = FileSystem.GetLocal(conf);
			FsPermission permission = new FsPermission(conf.Get(DFSConfigKeys.DfsDatanodeDataDirPermissionKey
				, DFSConfigKeys.DfsDatanodeDataDirPermissionDefault));
			foreach (FilePath data in dataDirs)
			{
				try
				{
					DiskChecker.CheckDir(localFS, new Path(data.ToURI()), permission);
				}
				catch (IOException e)
				{
					Log.Warn("Invalid directory in: " + data.GetCanonicalPath() + ": " + e.Message);
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual void Format(Storage.StorageDirectory sd, NamespaceInfo nsInfo, string
			 datanodeUuid)
		{
			sd.ClearDirectory();
			// create directory
			this.layoutVersion = HdfsConstants.DatanodeLayoutVersion;
			this.clusterID = nsInfo.GetClusterID();
			this.namespaceID = nsInfo.GetNamespaceID();
			this.cTime = 0;
			SetDatanodeUuid(datanodeUuid);
			if (sd.GetStorageUuid() == null)
			{
				// Assign a new Storage UUID.
				sd.SetStorageUuid(DatanodeStorage.GenerateUuid());
			}
			WriteProperties(sd);
		}

		/*
		* Set ClusterID, StorageID, StorageType, CTime into
		* DataStorage VERSION file.
		* Always called just before writing the properties to
		* the VERSION file.
		*/
		/// <exception cref="System.IO.IOException"/>
		protected internal override void SetPropertiesFromFields(Properties props, Storage.StorageDirectory
			 sd)
		{
			props.SetProperty("storageType", storageType.ToString());
			props.SetProperty("clusterID", clusterID);
			props.SetProperty("cTime", cTime.ToString());
			props.SetProperty("layoutVersion", layoutVersion.ToString());
			props.SetProperty("storageID", sd.GetStorageUuid());
			string datanodeUuid = GetDatanodeUuid();
			if (datanodeUuid != null)
			{
				props.SetProperty("datanodeUuid", datanodeUuid);
			}
			// Set NamespaceID in version before federation
			if (!DataNodeLayoutVersion.Supports(LayoutVersion.Feature.Federation, layoutVersion
				))
			{
				props.SetProperty("namespaceID", namespaceID.ToString());
			}
		}

		/*
		* Read ClusterID, StorageID, StorageType, CTime from
		* DataStorage VERSION file and verify them.
		* Always called just after reading the properties from the VERSION file.
		*/
		/// <exception cref="System.IO.IOException"/>
		protected internal override void SetFieldsFromProperties(Properties props, Storage.StorageDirectory
			 sd)
		{
			SetFieldsFromProperties(props, sd, false, 0);
		}

		/// <exception cref="System.IO.IOException"/>
		private void SetFieldsFromProperties(Properties props, Storage.StorageDirectory sd
			, bool overrideLayoutVersion, int toLayoutVersion)
		{
			if (overrideLayoutVersion)
			{
				this.layoutVersion = toLayoutVersion;
			}
			else
			{
				SetLayoutVersion(props, sd);
			}
			SetcTime(props, sd);
			CheckStorageType(props, sd);
			SetClusterId(props, layoutVersion, sd);
			// Read NamespaceID in version before federation
			if (!DataNodeLayoutVersion.Supports(LayoutVersion.Feature.Federation, layoutVersion
				))
			{
				SetNamespaceID(props, sd);
			}
			// valid storage id, storage id may be empty
			string ssid = props.GetProperty("storageID");
			if (ssid == null)
			{
				throw new InconsistentFSStateException(sd.GetRoot(), "file " + StorageFileVersion
					 + " is invalid.");
			}
			string sid = sd.GetStorageUuid();
			if (!(sid == null || sid.Equals(string.Empty) || ssid.Equals(string.Empty) || sid
				.Equals(ssid)))
			{
				throw new InconsistentFSStateException(sd.GetRoot(), "has incompatible storage Id."
					);
			}
			if (sid == null)
			{
				// update id only if it was null
				sd.SetStorageUuid(ssid);
			}
			// Update the datanode UUID if present.
			if (props.GetProperty("datanodeUuid") != null)
			{
				string dnUuid = props.GetProperty("datanodeUuid");
				if (GetDatanodeUuid() == null)
				{
					SetDatanodeUuid(dnUuid);
				}
				else
				{
					if (string.CompareOrdinal(GetDatanodeUuid(), dnUuid) != 0)
					{
						throw new InconsistentFSStateException(sd.GetRoot(), "Root " + sd.GetRoot() + ": DatanodeUuid="
							 + dnUuid + ", does not match " + GetDatanodeUuid() + " from other" + " StorageDirectory."
							);
					}
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override bool IsPreUpgradableLayout(Storage.StorageDirectory sd)
		{
			FilePath oldF = new FilePath(sd.GetRoot(), "storage");
			if (!oldF.Exists())
			{
				return false;
			}
			// check the layout version inside the storage file
			// Lock and Read old storage file
			RandomAccessFile oldFile = new RandomAccessFile(oldF, "rws");
			FileLock oldLock = oldFile.GetChannel().TryLock();
			try
			{
				oldFile.Seek(0);
				int oldVersion = oldFile.ReadInt();
				if (oldVersion < LastPreUpgradeLayoutVersion)
				{
					return false;
				}
			}
			finally
			{
				oldLock.Release();
				oldFile.Close();
			}
			return true;
		}

		/// <summary>Read VERSION file for rollback</summary>
		/// <exception cref="System.IO.IOException"/>
		internal virtual void ReadProperties(Storage.StorageDirectory sd, int rollbackLayoutVersion
			)
		{
			Properties props = ReadPropertiesFile(sd.GetVersionFile());
			SetFieldsFromProperties(props, sd, true, rollbackLayoutVersion);
		}

		/// <summary>
		/// Analize which and whether a transition of the fs state is required
		/// and perform it if necessary.
		/// </summary>
		/// <remarks>
		/// Analize which and whether a transition of the fs state is required
		/// and perform it if necessary.
		/// Rollback if the rollback startup option was specified.
		/// Upgrade if this.LV &gt; LAYOUT_VERSION
		/// Regular startup if this.LV = LAYOUT_VERSION
		/// </remarks>
		/// <param name="datanode">Datanode to which this storage belongs to</param>
		/// <param name="sd">storage directory</param>
		/// <param name="nsInfo">namespace info</param>
		/// <param name="startOpt">startup option</param>
		/// <exception cref="System.IO.IOException"/>
		private void DoTransition(DataNode datanode, Storage.StorageDirectory sd, NamespaceInfo
			 nsInfo, HdfsServerConstants.StartupOption startOpt)
		{
			if (startOpt == HdfsServerConstants.StartupOption.Rollback)
			{
				DoRollback(sd, nsInfo);
			}
			// rollback if applicable
			ReadProperties(sd);
			CheckVersionUpgradable(this.layoutVersion);
			System.Diagnostics.Debug.Assert(this.layoutVersion >= HdfsConstants.DatanodeLayoutVersion
				, "Future version is not allowed");
			bool federationSupported = DataNodeLayoutVersion.Supports(LayoutVersion.Feature.Federation
				, layoutVersion);
			// For pre-federation version - validate the namespaceID
			if (!federationSupported && GetNamespaceID() != nsInfo.GetNamespaceID())
			{
				throw new IOException("Incompatible namespaceIDs in " + sd.GetRoot().GetCanonicalPath
					() + ": namenode namespaceID = " + nsInfo.GetNamespaceID() + "; datanode namespaceID = "
					 + GetNamespaceID());
			}
			// For version that supports federation, validate clusterID
			if (federationSupported && !GetClusterID().Equals(nsInfo.GetClusterID()))
			{
				throw new IOException("Incompatible clusterIDs in " + sd.GetRoot().GetCanonicalPath
					() + ": namenode clusterID = " + nsInfo.GetClusterID() + "; datanode clusterID = "
					 + GetClusterID());
			}
			// Clusters previously upgraded from layout versions earlier than
			// ADD_DATANODE_AND_STORAGE_UUIDS failed to correctly generate a
			// new storage ID. We check for that and fix it now.
			bool haveValidStorageId = DataNodeLayoutVersion.Supports(LayoutVersion.Feature.AddDatanodeAndStorageUuids
				, layoutVersion) && DatanodeStorage.IsValidStorageId(sd.GetStorageUuid());
			// regular start up.
			if (this.layoutVersion == HdfsConstants.DatanodeLayoutVersion)
			{
				CreateStorageID(sd, !haveValidStorageId);
				return;
			}
			// regular startup
			// do upgrade
			if (this.layoutVersion > HdfsConstants.DatanodeLayoutVersion)
			{
				DoUpgrade(datanode, sd, nsInfo);
				// upgrade
				CreateStorageID(sd, !haveValidStorageId);
				return;
			}
			// layoutVersion < DATANODE_LAYOUT_VERSION. I.e. stored layout version is newer
			// than the version supported by datanode. This should have been caught
			// in readProperties(), even if rollback was not carried out or somehow
			// failed.
			throw new IOException("BUG: The stored LV = " + this.GetLayoutVersion() + " is newer than the supported LV = "
				 + HdfsConstants.DatanodeLayoutVersion);
		}

		/// <summary>
		/// Upgrade -- Move current storage into a backup directory,
		/// and hardlink all its blocks into the new current directory.
		/// </summary>
		/// <remarks>
		/// Upgrade -- Move current storage into a backup directory,
		/// and hardlink all its blocks into the new current directory.
		/// Upgrade from pre-0.22 to 0.22 or later release e.g. 0.19/0.20/ =&gt; 0.22/0.23
		/// <ul>
		/// <li> If <SD>/previous exists then delete it </li>
		/// <li> Rename <SD>/current to <SD>/previous.tmp </li>
		/// <li>Create new <SD>/current/<bpid>/current directory<li>
		/// <ul>
		/// <li> Hard links for block files are created from <SD>/previous.tmp
		/// to <SD>/current/<bpid>/current </li>
		/// <li> Saves new version file in <SD>/current/<bpid>/current directory </li>
		/// </ul>
		/// <li> Rename <SD>/previous.tmp to <SD>/previous </li>
		/// </ul>
		/// There should be only ONE namenode in the cluster for first
		/// time upgrade to 0.22
		/// </remarks>
		/// <param name="sd">storage directory</param>
		/// <exception cref="System.IO.IOException">on error</exception>
		internal virtual void DoUpgrade(DataNode datanode, Storage.StorageDirectory sd, NamespaceInfo
			 nsInfo)
		{
			// If the existing on-disk layout version supportes federation, simply
			// update its layout version.
			if (DataNodeLayoutVersion.Supports(LayoutVersion.Feature.Federation, layoutVersion
				))
			{
				// The VERSION file is already read in. Override the layoutVersion 
				// field and overwrite the file. The upgrade work is handled by
				// {@link BlockPoolSliceStorage#doUpgrade}
				Log.Info("Updating layout version from " + layoutVersion + " to " + HdfsConstants
					.DatanodeLayoutVersion + " for storage " + sd.GetRoot());
				layoutVersion = HdfsConstants.DatanodeLayoutVersion;
				WriteProperties(sd);
				return;
			}
			Log.Info("Upgrading storage directory " + sd.GetRoot() + ".\n   old LV = " + this
				.GetLayoutVersion() + "; old CTime = " + this.GetCTime() + ".\n   new LV = " + HdfsConstants
				.DatanodeLayoutVersion + "; new CTime = " + nsInfo.GetCTime());
			FilePath curDir = sd.GetCurrentDir();
			FilePath prevDir = sd.GetPreviousDir();
			FilePath bbwDir = new FilePath(sd.GetRoot(), Storage.Storage1Bbw);
			System.Diagnostics.Debug.Assert(curDir.Exists(), "Data node current directory must exist."
				);
			// Cleanup directory "detach"
			CleanupDetachDir(new FilePath(curDir, StorageDirDetached));
			// 1. delete <SD>/previous dir before upgrading
			if (prevDir.Exists())
			{
				DeleteDir(prevDir);
			}
			// get previous.tmp directory, <SD>/previous.tmp
			FilePath tmpDir = sd.GetPreviousTmp();
			System.Diagnostics.Debug.Assert(!tmpDir.Exists(), "Data node previous.tmp directory must not exist."
				);
			// 2. Rename <SD>/current to <SD>/previous.tmp
			Rename(curDir, tmpDir);
			// 3. Format BP and hard link blocks from previous directory
			FilePath curBpDir = BlockPoolSliceStorage.GetBpRoot(nsInfo.GetBlockPoolID(), curDir
				);
			BlockPoolSliceStorage bpStorage = new BlockPoolSliceStorage(nsInfo.GetNamespaceID
				(), nsInfo.GetBlockPoolID(), nsInfo.GetCTime(), nsInfo.GetClusterID());
			bpStorage.Format(curDir, nsInfo);
			LinkAllBlocks(datanode, tmpDir, bbwDir, new FilePath(curBpDir, StorageDirCurrent)
				);
			// 4. Write version file under <SD>/current
			layoutVersion = HdfsConstants.DatanodeLayoutVersion;
			clusterID = nsInfo.GetClusterID();
			WriteProperties(sd);
			// 5. Rename <SD>/previous.tmp to <SD>/previous
			Rename(tmpDir, prevDir);
			Log.Info("Upgrade of " + sd.GetRoot() + " is complete");
			AddBlockPoolStorage(nsInfo.GetBlockPoolID(), bpStorage);
		}

		/// <summary>Cleanup the detachDir.</summary>
		/// <remarks>
		/// Cleanup the detachDir.
		/// If the directory is not empty report an error;
		/// Otherwise remove the directory.
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
		/// Rolling back to a snapshot in previous directory by moving it to current
		/// directory.
		/// </summary>
		/// <remarks>
		/// Rolling back to a snapshot in previous directory by moving it to current
		/// directory.
		/// Rollback procedure:
		/// <br />
		/// If previous directory exists:
		/// <ol>
		/// <li> Rename current to removed.tmp </li>
		/// <li> Rename previous to current </li>
		/// <li> Remove removed.tmp </li>
		/// </ol>
		/// If previous directory does not exist and the current version supports
		/// federation, perform a simple rollback of layout version. This does not
		/// involve saving/restoration of actual data.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		internal virtual void DoRollback(Storage.StorageDirectory sd, NamespaceInfo nsInfo
			)
		{
			FilePath prevDir = sd.GetPreviousDir();
			// This is a regular startup or a post-federation rollback
			if (!prevDir.Exists())
			{
				if (DataNodeLayoutVersion.Supports(LayoutVersion.Feature.Federation, HdfsConstants
					.DatanodeLayoutVersion))
				{
					ReadProperties(sd, HdfsConstants.DatanodeLayoutVersion);
					WriteProperties(sd);
					Log.Info("Layout version rolled back to " + HdfsConstants.DatanodeLayoutVersion +
						 " for storage " + sd.GetRoot());
				}
				return;
			}
			DataStorage prevInfo = new DataStorage();
			prevInfo.ReadPreviousVersionProperties(sd);
			// We allow rollback to a state, which is either consistent with
			// the namespace state or can be further upgraded to it.
			if (!(prevInfo.GetLayoutVersion() >= HdfsConstants.DatanodeLayoutVersion && prevInfo
				.GetCTime() <= nsInfo.GetCTime()))
			{
				// cannot rollback
				throw new InconsistentFSStateException(sd.GetRoot(), "Cannot rollback to a newer state.\nDatanode previous state: LV = "
					 + prevInfo.GetLayoutVersion() + " CTime = " + prevInfo.GetCTime() + " is newer than the namespace state: LV = "
					 + HdfsConstants.DatanodeLayoutVersion + " CTime = " + nsInfo.GetCTime());
			}
			Log.Info("Rolling back storage directory " + sd.GetRoot() + ".\n   target LV = " 
				+ HdfsConstants.DatanodeLayoutVersion + "; target CTime = " + nsInfo.GetCTime());
			FilePath tmpDir = sd.GetRemovedTmp();
			System.Diagnostics.Debug.Assert(!tmpDir.Exists(), "removed.tmp directory must not exist."
				);
			// rename current to tmp
			FilePath curDir = sd.GetCurrentDir();
			System.Diagnostics.Debug.Assert(curDir.Exists(), "Current directory must exist.");
			Rename(curDir, tmpDir);
			// rename previous to current
			Rename(prevDir, curDir);
			// delete tmp dir
			DeleteDir(tmpDir);
			Log.Info("Rollback of " + sd.GetRoot() + " is complete");
		}

		/// <summary>Finalize procedure deletes an existing snapshot.</summary>
		/// <remarks>
		/// Finalize procedure deletes an existing snapshot.
		/// <ol>
		/// <li>Rename previous to finalized.tmp directory</li>
		/// <li>Fully delete the finalized.tmp directory</li>
		/// </ol>
		/// Do nothing, if previous directory does not exist
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		internal virtual void DoFinalize(Storage.StorageDirectory sd)
		{
			FilePath prevDir = sd.GetPreviousDir();
			if (!prevDir.Exists())
			{
				return;
			}
			// already discarded
			string dataDirPath = sd.GetRoot().GetCanonicalPath();
			Log.Info("Finalizing upgrade for storage directory " + dataDirPath + ".\n   cur LV = "
				 + this.GetLayoutVersion() + "; cur CTime = " + this.GetCTime());
			System.Diagnostics.Debug.Assert(sd.GetCurrentDir().Exists(), "Current directory must exist."
				);
			FilePath tmpDir = sd.GetFinalizedTmp();
			//finalized.tmp directory
			FilePath bbwDir = new FilePath(sd.GetRoot(), Storage.Storage1Bbw);
			// 1. rename previous to finalized.tmp
			Rename(prevDir, tmpDir);
			// 2. delete finalized.tmp dir in a separate thread
			// Also delete the blocksBeingWritten from HDFS 1.x and earlier, if
			// it exists.
			new Daemon(new _Runnable_918(tmpDir, bbwDir, dataDirPath)).Start();
		}

		private sealed class _Runnable_918 : Runnable
		{
			public _Runnable_918(FilePath tmpDir, FilePath bbwDir, string dataDirPath)
			{
				this.tmpDir = tmpDir;
				this.bbwDir = bbwDir;
				this.dataDirPath = dataDirPath;
			}

			public void Run()
			{
				try
				{
					Storage.DeleteDir(tmpDir);
					if (bbwDir.Exists())
					{
						Storage.DeleteDir(bbwDir);
					}
				}
				catch (IOException ex)
				{
					Storage.Log.Error("Finalize upgrade for " + dataDirPath + " failed", ex);
				}
				Storage.Log.Info("Finalize upgrade for " + dataDirPath + " is complete");
			}

			public override string ToString()
			{
				return "Finalize " + dataDirPath;
			}

			private readonly FilePath tmpDir;

			private readonly FilePath bbwDir;

			private readonly string dataDirPath;
		}

		/*
		* Finalize the upgrade for a block pool
		* This also empties trash created during rolling upgrade and disables
		* trash functionality.
		*/
		/// <exception cref="System.IO.IOException"/>
		internal virtual void FinalizeUpgrade(string bpID)
		{
			// To handle finalizing a snapshot taken at datanode level while
			// upgrading to federation, if datanode level snapshot previous exists, 
			// then finalize it. Else finalize the corresponding BP.
			foreach (Storage.StorageDirectory sd in storageDirs)
			{
				FilePath prevDir = sd.GetPreviousDir();
				if (prevDir.Exists())
				{
					// data node level storage finalize
					DoFinalize(sd);
				}
				else
				{
					// block pool storage finalize using specific bpID
					BlockPoolSliceStorage bpStorage = bpStorageMap[bpID];
					bpStorage.DoFinalize(sd.GetCurrentDir());
				}
			}
		}

		/// <summary>Hardlink all finalized and RBW blocks in fromDir to toDir</summary>
		/// <param name="fromDir">The directory where the 'from' snapshot is stored</param>
		/// <param name="fromBbwDir">
		/// In HDFS 1.x, the directory where blocks
		/// that are under construction are stored.
		/// </param>
		/// <param name="toDir">The current data directory</param>
		/// <exception cref="System.IO.IOException">If error occurs during hardlink</exception>
		private void LinkAllBlocks(DataNode datanode, FilePath fromDir, FilePath fromBbwDir
			, FilePath toDir)
		{
			HardLink hardLink = new HardLink();
			// do the link
			int diskLayoutVersion = this.GetLayoutVersion();
			if (DataNodeLayoutVersion.Supports(LayoutVersion.Feature.AppendRbwDir, diskLayoutVersion
				))
			{
				// hardlink finalized blocks in tmpDir/finalized
				LinkBlocks(datanode, new FilePath(fromDir, StorageDirFinalized), new FilePath(toDir
					, StorageDirFinalized), diskLayoutVersion, hardLink);
				// hardlink rbw blocks in tmpDir/rbw
				LinkBlocks(datanode, new FilePath(fromDir, StorageDirRbw), new FilePath(toDir, StorageDirRbw
					), diskLayoutVersion, hardLink);
			}
			else
			{
				// pre-RBW version
				// hardlink finalized blocks in tmpDir
				LinkBlocks(datanode, fromDir, new FilePath(toDir, StorageDirFinalized), diskLayoutVersion
					, hardLink);
				if (fromBbwDir.Exists())
				{
					/*
					* We need to put the 'blocksBeingWritten' from HDFS 1.x into the rbw
					* directory.  It's a little messy, because the blocksBeingWriten was
					* NOT underneath the 'current' directory in those releases.  See
					* HDFS-3731 for details.
					*/
					LinkBlocks(datanode, fromBbwDir, new FilePath(toDir, StorageDirRbw), diskLayoutVersion
						, hardLink);
				}
			}
			Log.Info(hardLink.linkStats.Report());
		}

		private class LinkArgs
		{
			internal FilePath src;

			internal FilePath dst;

			internal LinkArgs(FilePath src, FilePath dst)
			{
				this.src = src;
				this.dst = dst;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		internal static void LinkBlocks(DataNode datanode, FilePath from, FilePath to, int
			 oldLV, HardLink hl)
		{
			bool upgradeToIdBasedLayout = false;
			// If we are upgrading from a version older than the one where we introduced
			// block ID-based layout AND we're working with the finalized directory,
			// we'll need to upgrade from the old flat layout to the block ID-based one
			if (oldLV > DataNodeLayoutVersion.Feature.BlockidBasedLayout.GetInfo().GetLayoutVersion
				() && to.GetName().Equals(StorageDirFinalized))
			{
				upgradeToIdBasedLayout = true;
			}
			AList<DataStorage.LinkArgs> idBasedLayoutSingleLinks = Lists.NewArrayList();
			LinkBlocksHelper(from, to, oldLV, hl, upgradeToIdBasedLayout, to, idBasedLayoutSingleLinks
				);
			// Detect and remove duplicate entries.
			AList<DataStorage.LinkArgs> duplicates = FindDuplicateEntries(idBasedLayoutSingleLinks
				);
			if (!duplicates.IsEmpty())
			{
				Log.Error("There are " + duplicates.Count + " duplicate block " + "entries within the same volume."
					);
				RemoveDuplicateEntries(idBasedLayoutSingleLinks, duplicates);
			}
			int numLinkWorkers = datanode.GetConf().GetInt(DFSConfigKeys.DfsDatanodeBlockIdLayoutUpgradeThreadsKey
				, DFSConfigKeys.DfsDatanodeBlockIdLayoutUpgradeThreads);
			ExecutorService linkWorkers = Executors.NewFixedThreadPool(numLinkWorkers);
			int step = idBasedLayoutSingleLinks.Count / numLinkWorkers + 1;
			IList<Future<Void>> futures = Lists.NewArrayList();
			for (int i = 0; i < idBasedLayoutSingleLinks.Count; i += step)
			{
				int iCopy = i;
				futures.AddItem(linkWorkers.Submit(new _Callable_1042(iCopy, step, idBasedLayoutSingleLinks
					)));
			}
			linkWorkers.Shutdown();
			foreach (Future<Void> f in futures)
			{
				Futures.Get<IOException>(f);
			}
		}

		private sealed class _Callable_1042 : Callable<Void>
		{
			public _Callable_1042(int iCopy, int step, AList<DataStorage.LinkArgs> idBasedLayoutSingleLinks
				)
			{
				this.iCopy = iCopy;
				this.step = step;
				this.idBasedLayoutSingleLinks = idBasedLayoutSingleLinks;
			}

			/// <exception cref="System.IO.IOException"/>
			public Void Call()
			{
				int upperBound = Math.Min(iCopy + step, idBasedLayoutSingleLinks.Count);
				for (int j = iCopy; j < upperBound; j++)
				{
					DataStorage.LinkArgs cur = idBasedLayoutSingleLinks[j];
					NativeIO.Link(cur.src, cur.dst);
				}
				return null;
			}

			private readonly int iCopy;

			private readonly int step;

			private readonly AList<DataStorage.LinkArgs> idBasedLayoutSingleLinks;
		}

		/// <summary>Find duplicate entries with an array of LinkArgs.</summary>
		/// <remarks>
		/// Find duplicate entries with an array of LinkArgs.
		/// Duplicate entries are entries with the same last path component.
		/// </remarks>
		internal static AList<DataStorage.LinkArgs> FindDuplicateEntries(AList<DataStorage.LinkArgs
			> all)
		{
			// Find duplicates by sorting the list by the final path component.
			all.Sort(new _IComparer_1067());
			AList<DataStorage.LinkArgs> duplicates = Lists.NewArrayList();
			long prevBlockId = null;
			bool prevWasMeta = false;
			bool addedPrev = false;
			for (int i = 0; i < all.Count; i++)
			{
				DataStorage.LinkArgs args = all[i];
				long blockId = Block.GetBlockId(args.src.GetName());
				bool isMeta = Block.IsMetaFilename(args.src.GetName());
				if ((prevBlockId == null) || (prevBlockId != blockId))
				{
					prevBlockId = blockId;
					addedPrev = false;
				}
				else
				{
					if (isMeta == prevWasMeta)
					{
						// If we saw another file for the same block ID previously,
						// and it had the same meta-ness as this file, we have a
						// duplicate.
						duplicates.AddItem(args);
						if (!addedPrev)
						{
							duplicates.AddItem(all[i - 1]);
						}
						addedPrev = true;
					}
					else
					{
						addedPrev = false;
					}
				}
				prevWasMeta = isMeta;
			}
			return duplicates;
		}

		private sealed class _IComparer_1067 : IComparer<DataStorage.LinkArgs>
		{
			public _IComparer_1067()
			{
			}

			/// <summary>
			/// Compare two LinkArgs objects, such that objects with the same
			/// terminal source path components are grouped together.
			/// </summary>
			public int Compare(DataStorage.LinkArgs a, DataStorage.LinkArgs b)
			{
				return ComparisonChain.Start().Compare(a.src.GetName(), b.src.GetName()).Compare(
					a.src, b.src).Compare(a.dst, b.dst).Result();
			}
		}

		/// <summary>Remove duplicate entries from the list.</summary>
		/// <remarks>
		/// Remove duplicate entries from the list.
		/// We do this by choosing:
		/// 1. the entries with the highest genstamp (this takes priority),
		/// 2. the entries with the longest block files,
		/// 3. arbitrarily, if neither #1 nor #2 gives a clear winner.
		/// Block and metadata files form a pair-- if you take a metadata file from
		/// one subdirectory, you must also take the block file from that
		/// subdirectory.
		/// </remarks>
		private static void RemoveDuplicateEntries(AList<DataStorage.LinkArgs> all, AList
			<DataStorage.LinkArgs> duplicates)
		{
			// Maps blockId -> metadata file with highest genstamp
			SortedDictionary<long, IList<DataStorage.LinkArgs>> highestGenstamps = new SortedDictionary
				<long, IList<DataStorage.LinkArgs>>();
			foreach (DataStorage.LinkArgs duplicate in duplicates)
			{
				if (!Block.IsMetaFilename(duplicate.src.GetName()))
				{
					continue;
				}
				long blockId = Block.GetBlockId(duplicate.src.GetName());
				IList<DataStorage.LinkArgs> prevHighest = highestGenstamps[blockId];
				if (prevHighest == null)
				{
					IList<DataStorage.LinkArgs> highest = new List<DataStorage.LinkArgs>();
					highest.AddItem(duplicate);
					highestGenstamps[blockId] = highest;
					continue;
				}
				long prevGenstamp = Block.GetGenerationStamp(prevHighest[0].src.GetName());
				long genstamp = Block.GetGenerationStamp(duplicate.src.GetName());
				if (genstamp < prevGenstamp)
				{
					continue;
				}
				if (genstamp > prevGenstamp)
				{
					prevHighest.Clear();
				}
				prevHighest.AddItem(duplicate);
			}
			// Remove data / metadata entries that don't have the highest genstamp
			// from the duplicates list.
			for (IEnumerator<DataStorage.LinkArgs> iter = duplicates.GetEnumerator(); iter.HasNext
				(); )
			{
				DataStorage.LinkArgs duplicate_1 = iter.Next();
				long blockId = Block.GetBlockId(duplicate_1.src.GetName());
				IList<DataStorage.LinkArgs> highest = highestGenstamps[blockId];
				if (highest != null)
				{
					bool found = false;
					foreach (DataStorage.LinkArgs high in highest)
					{
						if (high.src.GetParent().Equals(duplicate_1.src.GetParent()))
						{
							found = true;
							break;
						}
					}
					if (!found)
					{
						Log.Warn("Unexpectedly low genstamp on " + duplicate_1.src.GetAbsolutePath() + "."
							);
						iter.Remove();
					}
				}
			}
			// Find the longest block files
			// We let the "last guy win" here, since we're only interested in
			// preserving one block file / metadata file pair.
			SortedDictionary<long, DataStorage.LinkArgs> longestBlockFiles = new SortedDictionary
				<long, DataStorage.LinkArgs>();
			foreach (DataStorage.LinkArgs duplicate_2 in duplicates)
			{
				if (Block.IsMetaFilename(duplicate_2.src.GetName()))
				{
					continue;
				}
				long blockId = Block.GetBlockId(duplicate_2.src.GetName());
				DataStorage.LinkArgs prevLongest = longestBlockFiles[blockId];
				if (prevLongest == null)
				{
					longestBlockFiles[blockId] = duplicate_2;
					continue;
				}
				long blockLength = duplicate_2.src.Length();
				long prevBlockLength = prevLongest.src.Length();
				if (blockLength < prevBlockLength)
				{
					Log.Warn("Unexpectedly short length on " + duplicate_2.src.GetAbsolutePath() + "."
						);
					continue;
				}
				if (blockLength > prevBlockLength)
				{
					Log.Warn("Unexpectedly short length on " + prevLongest.src.GetAbsolutePath() + "."
						);
				}
				longestBlockFiles[blockId] = duplicate_2;
			}
			// Remove data / metadata entries that aren't the longest, or weren't
			// arbitrarily selected by us.
			for (IEnumerator<DataStorage.LinkArgs> iter_1 = all.GetEnumerator(); iter_1.HasNext
				(); )
			{
				DataStorage.LinkArgs args = iter_1.Next();
				long blockId = Block.GetBlockId(args.src.GetName());
				DataStorage.LinkArgs bestDuplicate = longestBlockFiles[blockId];
				if (bestDuplicate == null)
				{
					continue;
				}
				// file has no duplicates
				if (!bestDuplicate.src.GetParent().Equals(args.src.GetParent()))
				{
					Log.Warn("Discarding " + args.src.GetAbsolutePath() + ".");
					iter_1.Remove();
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		internal static void LinkBlocksHelper(FilePath from, FilePath to, int oldLV, HardLink
			 hl, bool upgradeToIdBasedLayout, FilePath blockRoot, IList<DataStorage.LinkArgs
			> idBasedLayoutSingleLinks)
		{
			if (!from.Exists())
			{
				return;
			}
			if (!from.IsDirectory())
			{
				if (from.GetName().StartsWith(CopyFilePrefix))
				{
					FileInputStream @in = new FileInputStream(from);
					try
					{
						FileOutputStream @out = new FileOutputStream(to);
						try
						{
							IOUtils.CopyBytes(@in, @out, 16 * 1024);
							hl.linkStats.countPhysicalFileCopies++;
						}
						finally
						{
							@out.Close();
						}
					}
					finally
					{
						@in.Close();
					}
				}
				else
				{
					HardLink.CreateHardLink(from, to);
					hl.linkStats.countSingleLinks++;
				}
				return;
			}
			// from is a directory
			hl.linkStats.countDirs++;
			string[] blockNames = from.List(new _FilenameFilter_1245());
			// If we are upgrading to block ID-based layout, we don't want to recreate
			// any subdirs from the source that contain blocks, since we have a new
			// directory structure
			if (!upgradeToIdBasedLayout || !to.GetName().StartsWith(BlockSubdirPrefix))
			{
				if (!to.Mkdirs())
				{
					throw new IOException("Cannot create directory " + to);
				}
			}
			// Block files just need hard links with the same file names
			// but a different directory
			if (blockNames.Length > 0)
			{
				if (upgradeToIdBasedLayout)
				{
					foreach (string blockName in blockNames)
					{
						long blockId = Block.GetBlockId(blockName);
						FilePath blockLocation = DatanodeUtil.IdToBlockDir(blockRoot, blockId);
						if (!blockLocation.Exists())
						{
							if (!blockLocation.Mkdirs())
							{
								throw new IOException("Failed to mkdirs " + blockLocation);
							}
						}
						idBasedLayoutSingleLinks.AddItem(new DataStorage.LinkArgs(new FilePath(from, blockName
							), new FilePath(blockLocation, blockName)));
						hl.linkStats.countSingleLinks++;
					}
				}
				else
				{
					HardLink.CreateHardLinkMult(from, blockNames, to);
					hl.linkStats.countMultLinks++;
					hl.linkStats.countFilesMultLinks += blockNames.Length;
				}
			}
			else
			{
				hl.linkStats.countEmptyDirs++;
			}
			// Now take care of the rest of the files and subdirectories
			string[] otherNames = from.List(new _FilenameFilter_1287());
			for (int i = 0; i < otherNames.Length; i++)
			{
				LinkBlocksHelper(new FilePath(from, otherNames[i]), new FilePath(to, otherNames[i
					]), oldLV, hl, upgradeToIdBasedLayout, blockRoot, idBasedLayoutSingleLinks);
			}
		}

		private sealed class _FilenameFilter_1245 : FilenameFilter
		{
			public _FilenameFilter_1245()
			{
			}

			public bool Accept(FilePath dir, string name)
			{
				return name.StartsWith(Block.BlockFilePrefix);
			}
		}

		private sealed class _FilenameFilter_1287 : FilenameFilter
		{
			public _FilenameFilter_1287()
			{
			}

			public bool Accept(FilePath dir, string name)
			{
				return name.StartsWith(DataStorage.BlockSubdirPrefix) || name.StartsWith(DataStorage
					.CopyFilePrefix);
			}
		}

		/// <summary>Add bpStorage into bpStorageMap</summary>
		private void AddBlockPoolStorage(string bpID, BlockPoolSliceStorage bpStorage)
		{
			if (!this.bpStorageMap.Contains(bpID))
			{
				this.bpStorageMap[bpID] = bpStorage;
			}
		}

		internal virtual void RemoveBlockPoolStorage(string bpId)
		{
			lock (this)
			{
				Sharpen.Collections.Remove(bpStorageMap, bpId);
			}
		}
	}
}
