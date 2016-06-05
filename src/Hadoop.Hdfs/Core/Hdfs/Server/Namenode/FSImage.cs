using System;
using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Base;
using Com.Google.Common.Collect;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Blockmanagement;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Hdfs.Server.Namenode.Startupprogress;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Org.Apache.Hadoop.Hdfs.Util;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>FSImage handles checkpointing and logging of the namespace edits.</summary>
	public class FSImage : IDisposable
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Hdfs.Server.Namenode.FSImage
			).FullName);

		protected internal FSEditLog editLog = null;

		private bool isUpgradeFinalized = false;

		protected internal NNStorage storage;

		/// <summary>
		/// The last transaction ID that was either loaded from an image
		/// or loaded by loading edits files.
		/// </summary>
		protected internal long lastAppliedTxId = 0;

		private readonly Configuration conf;

		protected internal NNStorageRetentionManager archivalManager;

		private readonly ICollection<long> currentlyCheckpointing = Sharpen.Collections.SynchronizedSet
			<long>(new HashSet<long>());

		/// <summary>Construct an FSImage</summary>
		/// <param name="conf">Configuration</param>
		/// <exception cref="System.IO.IOException">if default directories are invalid.</exception>
		public FSImage(Configuration conf)
			: this(conf, FSNamesystem.GetNamespaceDirs(conf), FSNamesystem.GetNamespaceEditsDirs
				(conf))
		{
		}

		/// <summary>Construct the FSImage.</summary>
		/// <remarks>
		/// Construct the FSImage. Set the default checkpoint directories.
		/// Setup storage and initialize the edit log.
		/// </remarks>
		/// <param name="conf">Configuration</param>
		/// <param name="imageDirs">Directories the image can be stored in.</param>
		/// <param name="editsDirs">Directories the editlog can be stored in.</param>
		/// <exception cref="System.IO.IOException">if directories are invalid.</exception>
		protected internal FSImage(Configuration conf, ICollection<URI> imageDirs, IList<
			URI> editsDirs)
		{
			/* Used to make sure there are no concurrent checkpoints for a given txid
			* The checkpoint here could be one of the following operations.
			* a. checkpoint when NN is in standby.
			* b. admin saveNameSpace operation.
			* c. download checkpoint file from any remote checkpointer.
			*/
			this.conf = conf;
			storage = new NNStorage(conf, imageDirs, editsDirs);
			if (conf.GetBoolean(DFSConfigKeys.DfsNamenodeNameDirRestoreKey, DFSConfigKeys.DfsNamenodeNameDirRestoreDefault
				))
			{
				storage.SetRestoreFailedStorage(true);
			}
			this.editLog = new FSEditLog(conf, storage, editsDirs);
			archivalManager = new NNStorageRetentionManager(conf, storage, editLog);
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual void Format(FSNamesystem fsn, string clusterId)
		{
			long fileCount = fsn.GetTotalFiles();
			// Expect 1 file, which is the root inode
			Preconditions.CheckState(fileCount == 1, "FSImage.format should be called with an uninitialized namesystem, has "
				 + fileCount + " files");
			NamespaceInfo ns = NNStorage.NewNamespaceInfo();
			Log.Info("Allocated new BlockPoolId: " + ns.GetBlockPoolID());
			ns.clusterID = clusterId;
			storage.Format(ns);
			editLog.FormatNonFileJournals(ns);
			SaveFSImageInAllDirs(fsn, 0);
		}

		/// <summary>Check whether the storage directories and non-file journals exist.</summary>
		/// <remarks>
		/// Check whether the storage directories and non-file journals exist.
		/// If running in interactive mode, will prompt the user for each
		/// directory to allow them to format anyway. Otherwise, returns
		/// false, unless 'force' is specified.
		/// </remarks>
		/// <param name="force">if true, format regardless of whether dirs exist</param>
		/// <param name="interactive">prompt the user when a dir exists</param>
		/// <returns>true if formatting should proceed</returns>
		/// <exception cref="System.IO.IOException">if some storage cannot be accessed</exception>
		internal virtual bool ConfirmFormat(bool force, bool interactive)
		{
			IList<Storage.FormatConfirmable> confirms = Lists.NewArrayList();
			foreach (Storage.StorageDirectory sd in storage.DirIterable(null))
			{
				confirms.AddItem(sd);
			}
			Sharpen.Collections.AddAll(confirms, editLog.GetFormatConfirmables());
			return Storage.ConfirmFormat(confirms, force, interactive);
		}

		/// <summary>Analyze storage directories.</summary>
		/// <remarks>
		/// Analyze storage directories.
		/// Recover from previous transitions if required.
		/// Perform fs state transition if necessary depending on the namespace info.
		/// Read storage info.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		/// <returns>true if the image needs to be saved or false otherwise</returns>
		internal virtual bool RecoverTransitionRead(HdfsServerConstants.StartupOption startOpt
			, FSNamesystem target, MetaRecoveryContext recovery)
		{
			System.Diagnostics.Debug.Assert(startOpt != HdfsServerConstants.StartupOption.Format
				, "NameNode formatting should be performed before reading the image");
			ICollection<URI> imageDirs = storage.GetImageDirectories();
			ICollection<URI> editsDirs = editLog.GetEditURIs();
			// none of the data dirs exist
			if ((imageDirs.Count == 0 || editsDirs.Count == 0) && startOpt != HdfsServerConstants.StartupOption
				.Import)
			{
				throw new IOException("All specified directories are not accessible or do not exist."
					);
			}
			// 1. For each data directory calculate its state and 
			// check whether all is consistent before transitioning.
			IDictionary<Storage.StorageDirectory, Storage.StorageState> dataDirStates = new Dictionary
				<Storage.StorageDirectory, Storage.StorageState>();
			bool isFormatted = RecoverStorageDirs(startOpt, storage, dataDirStates);
			if (Log.IsTraceEnabled())
			{
				Log.Trace("Data dir states:\n  " + Joiner.On("\n  ").WithKeyValueSeparator(": ").
					Join(dataDirStates));
			}
			if (!isFormatted && startOpt != HdfsServerConstants.StartupOption.Rollback && startOpt
				 != HdfsServerConstants.StartupOption.Import)
			{
				throw new IOException("NameNode is not formatted.");
			}
			int layoutVersion = storage.GetLayoutVersion();
			if (startOpt == HdfsServerConstants.StartupOption.Metadataversion)
			{
				System.Console.Out.WriteLine("HDFS Image Version: " + layoutVersion);
				System.Console.Out.WriteLine("Software format version: " + HdfsConstants.NamenodeLayoutVersion
					);
				return false;
			}
			if (layoutVersion < Storage.LastPreUpgradeLayoutVersion)
			{
				NNStorage.CheckVersionUpgradable(storage.GetLayoutVersion());
			}
			if (startOpt != HdfsServerConstants.StartupOption.Upgrade && startOpt != HdfsServerConstants.StartupOption
				.Upgradeonly && !HdfsServerConstants.RollingUpgradeStartupOption.Started.Matches
				(startOpt) && layoutVersion < Storage.LastPreUpgradeLayoutVersion && layoutVersion
				 != HdfsConstants.NamenodeLayoutVersion)
			{
				throw new IOException("\nFile system image contains an old layout version " + storage
					.GetLayoutVersion() + ".\nAn upgrade to version " + HdfsConstants.NamenodeLayoutVersion
					 + " is required.\n" + "Please restart NameNode with the \"" + HdfsServerConstants.RollingUpgradeStartupOption
					.Started.GetOptionString() + "\" option if a rolling upgrade is already started;"
					 + " or restart NameNode with the \"" + HdfsServerConstants.StartupOption.Upgrade
					.GetName() + "\" option to start" + " a new upgrade.");
			}
			storage.ProcessStartupOptionsForUpgrade(startOpt, layoutVersion);
			// 2. Format unformatted dirs.
			for (IEnumerator<Storage.StorageDirectory> it = storage.DirIterator(); it.HasNext
				(); )
			{
				Storage.StorageDirectory sd = it.Next();
				Storage.StorageState curState = dataDirStates[sd];
				switch (curState)
				{
					case Storage.StorageState.NonExistent:
					{
						throw new IOException(Storage.StorageState.NonExistent + " state cannot be here");
					}

					case Storage.StorageState.NotFormatted:
					{
						Log.Info("Storage directory " + sd.GetRoot() + " is not formatted.");
						Log.Info("Formatting ...");
						sd.ClearDirectory();
						// create empty currrent dir
						break;
					}

					default:
					{
						break;
					}
				}
			}
			switch (startOpt)
			{
				case HdfsServerConstants.StartupOption.Upgrade:
				case HdfsServerConstants.StartupOption.Upgradeonly:
				{
					// 3. Do transitions
					DoUpgrade(target);
					return false;
				}

				case HdfsServerConstants.StartupOption.Import:
				{
					// upgrade saved image already
					DoImportCheckpoint(target);
					return false;
				}

				case HdfsServerConstants.StartupOption.Rollback:
				{
					// import checkpoint saved image already
					throw new Exception("Rollback is now a standalone command, " + "NameNode should not be starting with this option."
						);
				}

				case HdfsServerConstants.StartupOption.Regular:
				default:
				{
					break;
				}
			}
			// just load the image
			return LoadFSImage(target, startOpt, recovery);
		}

		/// <summary>
		/// For each storage directory, performs recovery of incomplete transitions
		/// (eg.
		/// </summary>
		/// <remarks>
		/// For each storage directory, performs recovery of incomplete transitions
		/// (eg. upgrade, rollback, checkpoint) and inserts the directory's storage
		/// state into the dataDirStates map.
		/// </remarks>
		/// <param name="dataDirStates">output of storage directory states</param>
		/// <returns>true if there is at least one valid formatted storage directory</returns>
		/// <exception cref="System.IO.IOException"/>
		public static bool RecoverStorageDirs(HdfsServerConstants.StartupOption startOpt, 
			NNStorage storage, IDictionary<Storage.StorageDirectory, Storage.StorageState> dataDirStates
			)
		{
			bool isFormatted = false;
			// This loop needs to be over all storage dirs, even shared dirs, to make
			// sure that we properly examine their state, but we make sure we don't
			// mutate the shared dir below in the actual loop.
			for (IEnumerator<Storage.StorageDirectory> it = storage.DirIterator(); it.HasNext
				(); )
			{
				Storage.StorageDirectory sd = it.Next();
				Storage.StorageState curState;
				if (startOpt == HdfsServerConstants.StartupOption.Metadataversion)
				{
					/* All we need is the layout version. */
					storage.ReadProperties(sd);
					return true;
				}
				try
				{
					curState = sd.AnalyzeStorage(startOpt, storage);
					switch (curState)
					{
						case Storage.StorageState.NonExistent:
						{
							// sd is locked but not opened
							// name-node fails if any of the configured storage dirs are missing
							throw new InconsistentFSStateException(sd.GetRoot(), "storage directory does not exist or is not accessible."
								);
						}

						case Storage.StorageState.NotFormatted:
						{
							break;
						}

						case Storage.StorageState.Normal:
						{
							break;
						}

						default:
						{
							// recovery is possible
							sd.DoRecover(curState);
							break;
						}
					}
					if (curState != Storage.StorageState.NotFormatted && startOpt != HdfsServerConstants.StartupOption
						.Rollback)
					{
						// read and verify consistency with other directories
						storage.ReadProperties(sd, startOpt);
						isFormatted = true;
					}
					if (startOpt == HdfsServerConstants.StartupOption.Import && isFormatted)
					{
						// import of a checkpoint is allowed only into empty image directories
						throw new IOException("Cannot import image from a checkpoint. " + " NameNode already contains an image in "
							 + sd.GetRoot());
					}
				}
				catch (IOException ioe)
				{
					sd.Unlock();
					throw;
				}
				dataDirStates[sd] = curState;
			}
			return isFormatted;
		}

		/// <summary>Check if upgrade is in progress.</summary>
		/// <exception cref="System.IO.IOException"/>
		public static void CheckUpgrade(NNStorage storage)
		{
			// Upgrade or rolling upgrade is allowed only if there are 
			// no previous fs states in any of the directories
			for (IEnumerator<Storage.StorageDirectory> it = storage.DirIterator(false); it.HasNext
				(); )
			{
				Storage.StorageDirectory sd = it.Next();
				if (sd.GetPreviousDir().Exists())
				{
					throw new InconsistentFSStateException(sd.GetRoot(), "previous fs state should not exist during upgrade. "
						 + "Finalize or rollback first.");
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual void CheckUpgrade()
		{
			CheckUpgrade(storage);
		}

		/// <returns>
		/// true if there is rollback fsimage (for rolling upgrade) in NameNode
		/// directory.
		/// </returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual bool HasRollbackFSImage()
		{
			FSImageStorageInspector inspector = new FSImageTransactionalStorageInspector(EnumSet
				.Of(NNStorage.NameNodeFile.ImageRollback));
			storage.InspectStorageDirs(inspector);
			try
			{
				IList<FSImageStorageInspector.FSImageFile> images = inspector.GetLatestImages();
				return images != null && !images.IsEmpty();
			}
			catch (FileNotFoundException)
			{
				return false;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual void DoUpgrade(FSNamesystem target)
		{
			CheckUpgrade();
			// load the latest image
			this.LoadFSImage(target, HdfsServerConstants.StartupOption.Upgrade, null);
			// Do upgrade for each directory
			target.CheckRollingUpgrade("upgrade namenode");
			long oldCTime = storage.GetCTime();
			storage.cTime = Time.Now();
			// generate new cTime for the state
			int oldLV = storage.GetLayoutVersion();
			storage.layoutVersion = HdfsConstants.NamenodeLayoutVersion;
			IList<Storage.StorageDirectory> errorSDs = Sharpen.Collections.SynchronizedList(new 
				AList<Storage.StorageDirectory>());
			System.Diagnostics.Debug.Assert(!editLog.IsSegmentOpen(), "Edits log must not be open."
				);
			Log.Info("Starting upgrade of local storage directories." + "\n   old LV = " + oldLV
				 + "; old CTime = " + oldCTime + ".\n   new LV = " + storage.GetLayoutVersion() 
				+ "; new CTime = " + storage.GetCTime());
			// Do upgrade for each directory
			for (IEnumerator<Storage.StorageDirectory> it = storage.DirIterator(false); it.HasNext
				(); )
			{
				Storage.StorageDirectory sd = it.Next();
				try
				{
					NNUpgradeUtil.DoPreUpgrade(conf, sd);
				}
				catch (Exception e)
				{
					Log.Error("Failed to move aside pre-upgrade storage " + "in image directory " + sd
						.GetRoot(), e);
					errorSDs.AddItem(sd);
					continue;
				}
			}
			if (target.IsHaEnabled())
			{
				editLog.DoPreUpgradeOfSharedLog();
			}
			storage.ReportErrorsOnDirectories(errorSDs);
			errorSDs.Clear();
			SaveFSImageInAllDirs(target, editLog.GetLastWrittenTxId());
			// upgrade shared edit storage first
			if (target.IsHaEnabled())
			{
				editLog.DoUpgradeOfSharedLog();
			}
			for (IEnumerator<Storage.StorageDirectory> it_1 = storage.DirIterator(false); it_1
				.HasNext(); )
			{
				Storage.StorageDirectory sd = it_1.Next();
				try
				{
					NNUpgradeUtil.DoUpgrade(sd, storage);
				}
				catch (IOException)
				{
					errorSDs.AddItem(sd);
					continue;
				}
			}
			storage.ReportErrorsOnDirectories(errorSDs);
			isUpgradeFinalized = false;
			if (!storage.GetRemovedStorageDirs().IsEmpty())
			{
				// during upgrade, it's a fatal error to fail any storage directory
				throw new IOException("Upgrade failed in " + storage.GetRemovedStorageDirs().Count
					 + " storage directory(ies), previously logged.");
			}
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual void DoRollback(FSNamesystem fsns)
		{
			// Rollback is allowed only if there is 
			// a previous fs states in at least one of the storage directories.
			// Directories that don't have previous state do not rollback
			bool canRollback = false;
			Org.Apache.Hadoop.Hdfs.Server.Namenode.FSImage prevState = new Org.Apache.Hadoop.Hdfs.Server.Namenode.FSImage
				(conf);
			try
			{
				prevState.GetStorage().layoutVersion = HdfsConstants.NamenodeLayoutVersion;
				for (IEnumerator<Storage.StorageDirectory> it = storage.DirIterator(false); it.HasNext
					(); )
				{
					Storage.StorageDirectory sd = it.Next();
					if (!NNUpgradeUtil.CanRollBack(sd, storage, prevState.GetStorage(), HdfsConstants
						.NamenodeLayoutVersion))
					{
						continue;
					}
					Log.Info("Can perform rollback for " + sd);
					canRollback = true;
				}
				if (fsns.IsHaEnabled())
				{
					// If HA is enabled, check if the shared log can be rolled back as well.
					editLog.InitJournalsForWrite();
					bool canRollBackSharedEditLog = editLog.CanRollBackSharedLog(prevState.GetStorage
						(), HdfsConstants.NamenodeLayoutVersion);
					if (canRollBackSharedEditLog)
					{
						Log.Info("Can perform rollback for shared edit log.");
						canRollback = true;
					}
				}
				if (!canRollback)
				{
					throw new IOException("Cannot rollback. None of the storage " + "directories contain previous fs state."
						);
				}
				// Now that we know all directories are going to be consistent
				// Do rollback for each directory containing previous state
				for (IEnumerator<Storage.StorageDirectory> it_1 = storage.DirIterator(false); it_1
					.HasNext(); )
				{
					Storage.StorageDirectory sd = it_1.Next();
					Log.Info("Rolling back storage directory " + sd.GetRoot() + ".\n   new LV = " + prevState
						.GetStorage().GetLayoutVersion() + "; new CTime = " + prevState.GetStorage().GetCTime
						());
					NNUpgradeUtil.DoRollBack(sd);
				}
				if (fsns.IsHaEnabled())
				{
					// If HA is enabled, try to roll back the shared log as well.
					editLog.DoRollback();
				}
				isUpgradeFinalized = true;
			}
			finally
			{
				prevState.Close();
			}
		}

		/// <summary>Load image from a checkpoint directory and save it into the current one.
		/// 	</summary>
		/// <param name="target">the NameSystem to import into</param>
		/// <exception cref="System.IO.IOException"/>
		internal virtual void DoImportCheckpoint(FSNamesystem target)
		{
			ICollection<URI> checkpointDirs = Org.Apache.Hadoop.Hdfs.Server.Namenode.FSImage.
				GetCheckpointDirs(conf, null);
			IList<URI> checkpointEditsDirs = Org.Apache.Hadoop.Hdfs.Server.Namenode.FSImage.GetCheckpointEditsDirs
				(conf, null);
			if (checkpointDirs == null || checkpointDirs.IsEmpty())
			{
				throw new IOException("Cannot import image from a checkpoint. " + "\"dfs.namenode.checkpoint.dir\" is not set."
					);
			}
			if (checkpointEditsDirs == null || checkpointEditsDirs.IsEmpty())
			{
				throw new IOException("Cannot import image from a checkpoint. " + "\"dfs.namenode.checkpoint.dir\" is not set."
					);
			}
			Org.Apache.Hadoop.Hdfs.Server.Namenode.FSImage realImage = target.GetFSImage();
			Org.Apache.Hadoop.Hdfs.Server.Namenode.FSImage ckptImage = new Org.Apache.Hadoop.Hdfs.Server.Namenode.FSImage
				(conf, checkpointDirs, checkpointEditsDirs);
			// load from the checkpoint dirs
			try
			{
				ckptImage.RecoverTransitionRead(HdfsServerConstants.StartupOption.Regular, target
					, null);
			}
			finally
			{
				ckptImage.Close();
			}
			// return back the real image
			realImage.GetStorage().SetStorageInfo(ckptImage.GetStorage());
			realImage.GetEditLog().SetNextTxId(ckptImage.GetEditLog().GetLastWrittenTxId() + 
				1);
			realImage.InitEditLog(HdfsServerConstants.StartupOption.Import);
			realImage.GetStorage().SetBlockPoolID(ckptImage.GetBlockPoolID());
			// and save it but keep the same checkpointTime
			SaveNamespace(target);
			GetStorage().WriteAll();
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual void FinalizeUpgrade(bool finalizeEditLog)
		{
			Log.Info("Finalizing upgrade for local dirs. " + (storage.GetLayoutVersion() == 0
				 ? string.Empty : "\n   cur LV = " + storage.GetLayoutVersion() + "; cur CTime = "
				 + storage.GetCTime()));
			for (IEnumerator<Storage.StorageDirectory> it = storage.DirIterator(false); it.HasNext
				(); )
			{
				Storage.StorageDirectory sd = it.Next();
				NNUpgradeUtil.DoFinalize(sd);
			}
			if (finalizeEditLog)
			{
				// We only do this in the case that HA is enabled and we're active. In any
				// other case the NN will have done the upgrade of the edits directories
				// already by virtue of the fact that they're local.
				editLog.DoFinalizeOfSharedLog();
			}
			isUpgradeFinalized = true;
		}

		internal virtual bool IsUpgradeFinalized()
		{
			return isUpgradeFinalized;
		}

		public virtual FSEditLog GetEditLog()
		{
			return editLog;
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual void OpenEditLogForWrite()
		{
			System.Diagnostics.Debug.Assert(editLog != null, "editLog must be initialized");
			editLog.OpenForWrite();
			storage.WriteTransactionIdFileToStorage(editLog.GetCurSegmentTxId());
		}

		/// <summary>
		/// Toss the current image and namesystem, reloading from the specified
		/// file.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		internal virtual void ReloadFromImageFile(FilePath file, FSNamesystem target)
		{
			target.Clear();
			Log.Debug("Reloading namespace from " + file);
			LoadFSImage(file, target, null, false);
		}

		/// <summary>
		/// Choose latest image from one of the directories,
		/// load it and merge with the edits.
		/// </summary>
		/// <remarks>
		/// Choose latest image from one of the directories,
		/// load it and merge with the edits.
		/// Saving and loading fsimage should never trigger symlink resolution.
		/// The paths that are persisted do not have *intermediate* symlinks
		/// because intermediate symlinks are resolved at the time files,
		/// directories, and symlinks are created. All paths accessed while
		/// loading or saving fsimage should therefore only see symlinks as
		/// the final path component, and the functions called below do not
		/// resolve symlinks that are the final path component.
		/// </remarks>
		/// <returns>whether the image should be saved</returns>
		/// <exception cref="System.IO.IOException"/>
		private bool LoadFSImage(FSNamesystem target, HdfsServerConstants.StartupOption startOpt
			, MetaRecoveryContext recovery)
		{
			bool rollingRollback = HdfsServerConstants.RollingUpgradeStartupOption.Rollback.Matches
				(startOpt);
			EnumSet<NNStorage.NameNodeFile> nnfs;
			if (rollingRollback)
			{
				// if it is rollback of rolling upgrade, only load from the rollback image
				nnfs = EnumSet.Of(NNStorage.NameNodeFile.ImageRollback);
			}
			else
			{
				// otherwise we can load from both IMAGE and IMAGE_ROLLBACK
				nnfs = EnumSet.Of(NNStorage.NameNodeFile.Image, NNStorage.NameNodeFile.ImageRollback
					);
			}
			FSImageStorageInspector inspector = storage.ReadAndInspectDirs(nnfs, startOpt);
			isUpgradeFinalized = inspector.IsUpgradeFinalized();
			IList<FSImageStorageInspector.FSImageFile> imageFiles = inspector.GetLatestImages
				();
			StartupProgress prog = NameNode.GetStartupProgress();
			prog.BeginPhase(Phase.LoadingFsimage);
			FilePath phaseFile = imageFiles[0].GetFile();
			prog.SetFile(Phase.LoadingFsimage, phaseFile.GetAbsolutePath());
			prog.SetSize(Phase.LoadingFsimage, phaseFile.Length());
			bool needToSave = inspector.NeedToSave();
			IEnumerable<EditLogInputStream> editStreams = null;
			InitEditLog(startOpt);
			if (NameNodeLayoutVersion.Supports(LayoutVersion.Feature.TxidBasedLayout, GetLayoutVersion
				()))
			{
				// If we're open for write, we're either non-HA or we're the active NN, so
				// we better be able to load all the edits. If we're the standby NN, it's
				// OK to not be able to read all of edits right now.
				// In the meanwhile, for HA upgrade, we will still write editlog thus need
				// this toAtLeastTxId to be set to the max-seen txid
				// For rollback in rolling upgrade, we need to set the toAtLeastTxId to
				// the txid right before the upgrade marker.  
				long toAtLeastTxId = editLog.IsOpenForWrite() ? inspector.GetMaxSeenTxId() : 0;
				if (rollingRollback)
				{
					// note that the first image in imageFiles is the special checkpoint
					// for the rolling upgrade
					toAtLeastTxId = imageFiles[0].GetCheckpointTxId() + 2;
				}
				editStreams = editLog.SelectInputStreams(imageFiles[0].GetCheckpointTxId() + 1, toAtLeastTxId
					, recovery, false);
			}
			else
			{
				editStreams = FSImagePreTransactionalStorageInspector.GetEditLogStreams(storage);
			}
			int maxOpSize = conf.GetInt(DFSConfigKeys.DfsNamenodeMaxOpSizeKey, DFSConfigKeys.
				DfsNamenodeMaxOpSizeDefault);
			foreach (EditLogInputStream elis in editStreams)
			{
				elis.SetMaxOpSize(maxOpSize);
			}
			foreach (EditLogInputStream l in editStreams)
			{
				Log.Debug("Planning to load edit log stream: " + l);
			}
			if (!editStreams.GetEnumerator().HasNext())
			{
				Log.Info("No edit log streams selected.");
			}
			FSImageStorageInspector.FSImageFile imageFile = null;
			for (int i = 0; i < imageFiles.Count; i++)
			{
				try
				{
					imageFile = imageFiles[i];
					LoadFSImageFile(target, recovery, imageFile, startOpt);
					break;
				}
				catch (IOException ioe)
				{
					Log.Error("Failed to load image from " + imageFile, ioe);
					target.Clear();
					imageFile = null;
				}
			}
			// Failed to load any images, error out
			if (imageFile == null)
			{
				FSEditLog.CloseAllStreams(editStreams);
				throw new IOException("Failed to load an FSImage file!");
			}
			prog.EndPhase(Phase.LoadingFsimage);
			if (!rollingRollback)
			{
				long txnsAdvanced = LoadEdits(editStreams, target, startOpt, recovery);
				needToSave |= NeedsResaveBasedOnStaleCheckpoint(imageFile.GetFile(), txnsAdvanced
					);
				if (HdfsServerConstants.RollingUpgradeStartupOption.Downgrade.Matches(startOpt))
				{
					// rename rollback image if it is downgrade
					RenameCheckpoint(NNStorage.NameNodeFile.ImageRollback, NNStorage.NameNodeFile.Image
						);
				}
			}
			else
			{
				// Trigger the rollback for rolling upgrade. Here lastAppliedTxId equals
				// to the last txid in rollback fsimage.
				RollingRollback(lastAppliedTxId + 1, imageFiles[0].GetCheckpointTxId());
				needToSave = false;
			}
			editLog.SetNextTxId(lastAppliedTxId + 1);
			return needToSave;
		}

		/// <summary>rollback for rolling upgrade.</summary>
		/// <exception cref="System.IO.IOException"/>
		private void RollingRollback(long discardSegmentTxId, long ckptId)
		{
			// discard discard unnecessary editlog segments starting from the given id
			this.editLog.DiscardSegments(discardSegmentTxId);
			// rename the special checkpoint
			RenameCheckpoint(ckptId, NNStorage.NameNodeFile.ImageRollback, NNStorage.NameNodeFile
				.Image, true);
			// purge all the checkpoints after the marker
			archivalManager.PurgeCheckpoinsAfter(NNStorage.NameNodeFile.Image, ckptId);
			string nameserviceId = DFSUtil.GetNamenodeNameServiceId(conf);
			if (HAUtil.IsHAEnabled(conf, nameserviceId))
			{
				// close the editlog since it is currently open for write
				this.editLog.Close();
				// reopen the editlog for read
				this.editLog.InitSharedJournalsForRead();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual void LoadFSImageFile(FSNamesystem target, MetaRecoveryContext recovery
			, FSImageStorageInspector.FSImageFile imageFile, HdfsServerConstants.StartupOption
			 startupOption)
		{
			Log.Debug("Planning to load image :\n" + imageFile);
			Storage.StorageDirectory sdForProperties = imageFile.sd;
			storage.ReadProperties(sdForProperties, startupOption);
			if (NameNodeLayoutVersion.Supports(LayoutVersion.Feature.TxidBasedLayout, GetLayoutVersion
				()))
			{
				// For txid-based layout, we should have a .md5 file
				// next to the image file
				bool isRollingRollback = HdfsServerConstants.RollingUpgradeStartupOption.Rollback
					.Matches(startupOption);
				LoadFSImage(imageFile.GetFile(), target, recovery, isRollingRollback);
			}
			else
			{
				if (NameNodeLayoutVersion.Supports(LayoutVersion.Feature.FsimageChecksum, GetLayoutVersion
					()))
				{
					// In 0.22, we have the checksum stored in the VERSION file.
					string md5 = storage.GetDeprecatedProperty(NNStorage.DeprecatedMessageDigestProperty
						);
					if (md5 == null)
					{
						throw new InconsistentFSStateException(sdForProperties.GetRoot(), "Message digest property "
							 + NNStorage.DeprecatedMessageDigestProperty + " not set for storage directory "
							 + sdForProperties.GetRoot());
					}
					LoadFSImage(imageFile.GetFile(), new MD5Hash(md5), target, recovery, false);
				}
				else
				{
					// We don't have any record of the md5sum
					LoadFSImage(imageFile.GetFile(), null, target, recovery, false);
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void InitEditLog(HdfsServerConstants.StartupOption startOpt)
		{
			Preconditions.CheckState(GetNamespaceID() != 0, "Must know namespace ID before initting edit log"
				);
			string nameserviceId = DFSUtil.GetNamenodeNameServiceId(conf);
			if (!HAUtil.IsHAEnabled(conf, nameserviceId))
			{
				// If this NN is not HA
				editLog.InitJournalsForWrite();
				editLog.RecoverUnclosedStreams();
			}
			else
			{
				if (HAUtil.IsHAEnabled(conf, nameserviceId) && (startOpt == HdfsServerConstants.StartupOption
					.Upgrade || startOpt == HdfsServerConstants.StartupOption.Upgradeonly || HdfsServerConstants.RollingUpgradeStartupOption
					.Rollback.Matches(startOpt)))
				{
					// This NN is HA, but we're doing an upgrade or a rollback of rolling
					// upgrade so init the edit log for write.
					editLog.InitJournalsForWrite();
					if (startOpt == HdfsServerConstants.StartupOption.Upgrade || startOpt == HdfsServerConstants.StartupOption
						.Upgradeonly)
					{
						long sharedLogCTime = editLog.GetSharedLogCTime();
						if (this.storage.GetCTime() < sharedLogCTime)
						{
							throw new IOException("It looks like the shared log is already " + "being upgraded but this NN has not been upgraded yet. You "
								 + "should restart this NameNode with the '" + HdfsServerConstants.StartupOption
								.Bootstrapstandby.GetName() + "' option to bring " + "this NN in sync with the other."
								);
						}
					}
					editLog.RecoverUnclosedStreams();
				}
				else
				{
					// This NN is HA and we're not doing an upgrade.
					editLog.InitSharedJournalsForRead();
				}
			}
		}

		/// <param name="imageFile">the image file that was loaded</param>
		/// <param name="numEditsLoaded">the number of edits loaded from edits logs</param>
		/// <returns>
		/// true if the NameNode should automatically save the namespace
		/// when it is started, due to the latest checkpoint being too old.
		/// </returns>
		private bool NeedsResaveBasedOnStaleCheckpoint(FilePath imageFile, long numEditsLoaded
			)
		{
			long checkpointPeriod = conf.GetLong(DFSConfigKeys.DfsNamenodeCheckpointPeriodKey
				, DFSConfigKeys.DfsNamenodeCheckpointPeriodDefault);
			long checkpointTxnCount = conf.GetLong(DFSConfigKeys.DfsNamenodeCheckpointTxnsKey
				, DFSConfigKeys.DfsNamenodeCheckpointTxnsDefault);
			long checkpointAge = Time.Now() - imageFile.LastModified();
			return (checkpointAge > checkpointPeriod * 1000) || (numEditsLoaded > checkpointTxnCount
				);
		}

		/// <summary>Load the specified list of edit files into the image.</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual long LoadEdits(IEnumerable<EditLogInputStream> editStreams, FSNamesystem
			 target)
		{
			return LoadEdits(editStreams, target, null, null);
		}

		/// <exception cref="System.IO.IOException"/>
		private long LoadEdits(IEnumerable<EditLogInputStream> editStreams, FSNamesystem 
			target, HdfsServerConstants.StartupOption startOpt, MetaRecoveryContext recovery
			)
		{
			Log.Debug("About to load edits:\n  " + Joiner.On("\n  ").Join(editStreams));
			StartupProgress prog = NameNode.GetStartupProgress();
			prog.BeginPhase(Phase.LoadingEdits);
			long prevLastAppliedTxId = lastAppliedTxId;
			try
			{
				FSEditLogLoader loader = new FSEditLogLoader(target, lastAppliedTxId);
				// Load latest edits
				foreach (EditLogInputStream editIn in editStreams)
				{
					Log.Info("Reading " + editIn + " expecting start txid #" + (lastAppliedTxId + 1));
					try
					{
						loader.LoadFSEdits(editIn, lastAppliedTxId + 1, startOpt, recovery);
					}
					finally
					{
						// Update lastAppliedTxId even in case of error, since some ops may
						// have been successfully applied before the error.
						lastAppliedTxId = loader.GetLastAppliedTxId();
					}
					// If we are in recovery mode, we may have skipped over some txids.
					if (editIn.GetLastTxId() != HdfsConstants.InvalidTxid)
					{
						lastAppliedTxId = editIn.GetLastTxId();
					}
				}
			}
			finally
			{
				FSEditLog.CloseAllStreams(editStreams);
				// update the counts
				UpdateCountForQuota(target.GetBlockManager().GetStoragePolicySuite(), target.dir.
					rootDir);
			}
			prog.EndPhase(Phase.LoadingEdits);
			return lastAppliedTxId - prevLastAppliedTxId;
		}

		/// <summary>Update the count of each directory with quota in the namespace.</summary>
		/// <remarks>
		/// Update the count of each directory with quota in the namespace.
		/// A directory's count is defined as the total number inodes in the tree
		/// rooted at the directory.
		/// This is an update of existing state of the filesystem and does not
		/// throw QuotaExceededException.
		/// </remarks>
		internal static void UpdateCountForQuota(BlockStoragePolicySuite bsps, INodeDirectory
			 root)
		{
			UpdateCountForQuotaRecursively(bsps, root.GetStoragePolicyID(), root, new QuotaCounts.Builder
				().Build());
		}

		private static void UpdateCountForQuotaRecursively(BlockStoragePolicySuite bsps, 
			byte blockStoragePolicyId, INodeDirectory dir, QuotaCounts counts)
		{
			long parentNamespace = counts.GetNameSpace();
			long parentStoragespace = counts.GetStorageSpace();
			EnumCounters<StorageType> parentTypeSpaces = counts.GetTypeSpaces();
			dir.ComputeQuotaUsage4CurrentDirectory(bsps, blockStoragePolicyId, counts);
			foreach (INode child in dir.GetChildrenList(Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot
				.CurrentStateId))
			{
				byte childPolicyId = child.GetStoragePolicyIDForQuota(blockStoragePolicyId);
				if (child.IsDirectory())
				{
					UpdateCountForQuotaRecursively(bsps, childPolicyId, child.AsDirectory(), counts);
				}
				else
				{
					// file or symlink: count here to reduce recursive calls.
					child.ComputeQuotaUsage(bsps, childPolicyId, counts, false, Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot
						.CurrentStateId);
				}
			}
			if (dir.IsQuotaSet())
			{
				// check if quota is violated. It indicates a software bug.
				QuotaCounts q = dir.GetQuotaCounts();
				long @namespace = counts.GetNameSpace() - parentNamespace;
				long nsQuota = q.GetNameSpace();
				if (Quota.IsViolated(nsQuota, @namespace))
				{
					Log.Warn("Namespace quota violation in image for " + dir.GetFullPathName() + " quota = "
						 + nsQuota + " < consumed = " + @namespace);
				}
				long ssConsumed = counts.GetStorageSpace() - parentStoragespace;
				long ssQuota = q.GetStorageSpace();
				if (Quota.IsViolated(ssQuota, ssConsumed))
				{
					Log.Warn("Storagespace quota violation in image for " + dir.GetFullPathName() + " quota = "
						 + ssQuota + " < consumed = " + ssConsumed);
				}
				EnumCounters<StorageType> typeSpaces = counts.GetTypeSpaces();
				foreach (StorageType t in StorageType.GetTypesSupportingQuota())
				{
					long typeSpace = typeSpaces.Get(t) - parentTypeSpaces.Get(t);
					long typeQuota = q.GetTypeSpaces().Get(t);
					if (Quota.IsViolated(typeQuota, typeSpace))
					{
						Log.Warn("Storage type quota violation in image for " + dir.GetFullPathName() + " type = "
							 + t.ToString() + " quota = " + typeQuota + " < consumed " + typeSpace);
					}
				}
				dir.GetDirectoryWithQuotaFeature().SetSpaceConsumed(@namespace, ssConsumed, typeSpaces
					);
			}
		}

		/// <summary>
		/// Load the image namespace from the given image file, verifying
		/// it against the MD5 sum stored in its associated .md5 file.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		private void LoadFSImage(FilePath imageFile, FSNamesystem target, MetaRecoveryContext
			 recovery, bool requireSameLayoutVersion)
		{
			MD5Hash expectedMD5 = MD5FileUtils.ReadStoredMd5ForFile(imageFile);
			if (expectedMD5 == null)
			{
				throw new IOException("No MD5 file found corresponding to image file " + imageFile
					);
			}
			LoadFSImage(imageFile, expectedMD5, target, recovery, requireSameLayoutVersion);
		}

		/// <summary>Load in the filesystem image from file.</summary>
		/// <remarks>
		/// Load in the filesystem image from file. It's a big list of
		/// filenames and blocks.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		private void LoadFSImage(FilePath curFile, MD5Hash expectedMd5, FSNamesystem target
			, MetaRecoveryContext recovery, bool requireSameLayoutVersion)
		{
			// BlockPoolId is required when the FsImageLoader loads the rolling upgrade
			// information. Make sure the ID is properly set.
			target.SetBlockPoolId(this.GetBlockPoolID());
			FSImageFormat.LoaderDelegator loader = FSImageFormat.NewLoader(conf, target);
			loader.Load(curFile, requireSameLayoutVersion);
			// Check that the image digest we loaded matches up with what
			// we expected
			MD5Hash readImageMd5 = loader.GetLoadedImageMd5();
			if (expectedMd5 != null && !expectedMd5.Equals(readImageMd5))
			{
				throw new IOException("Image file " + curFile + " is corrupt with MD5 checksum of "
					 + readImageMd5 + " but expecting " + expectedMd5);
			}
			long txId = loader.GetLoadedImageTxId();
			Log.Info("Loaded image for txid " + txId + " from " + curFile);
			lastAppliedTxId = txId;
			storage.SetMostRecentCheckpointInfo(txId, curFile.LastModified());
		}

		/// <summary>Save the contents of the FS image to the file.</summary>
		/// <exception cref="System.IO.IOException"/>
		internal virtual void SaveFSImage(SaveNamespaceContext context, Storage.StorageDirectory
			 sd, NNStorage.NameNodeFile dstType)
		{
			long txid = context.GetTxId();
			FilePath newFile = NNStorage.GetStorageFile(sd, NNStorage.NameNodeFile.ImageNew, 
				txid);
			FilePath dstFile = NNStorage.GetStorageFile(sd, dstType, txid);
			FSImageFormatProtobuf.Saver saver = new FSImageFormatProtobuf.Saver(context);
			FSImageCompression compression = FSImageCompression.CreateCompression(conf);
			saver.Save(newFile, compression);
			MD5FileUtils.SaveMD5File(dstFile, saver.GetSavedDigest());
			storage.SetMostRecentCheckpointInfo(txid, Time.Now());
		}

		/// <summary>Save FSimage in the legacy format.</summary>
		/// <remarks>
		/// Save FSimage in the legacy format. This is not for NN consumption,
		/// but for tools like OIV.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public virtual void SaveLegacyOIVImage(FSNamesystem source, string targetDir, Canceler
			 canceler)
		{
			FSImageCompression compression = FSImageCompression.CreateCompression(conf);
			long txid = GetLastAppliedOrWrittenTxId();
			SaveNamespaceContext ctx = new SaveNamespaceContext(source, txid, canceler);
			FSImageFormat.Saver saver = new FSImageFormat.Saver(ctx);
			string imageFileName = NNStorage.GetLegacyOIVImageFileName(txid);
			FilePath imageFile = new FilePath(targetDir, imageFileName);
			saver.Save(imageFile, compression);
			archivalManager.PurgeOldLegacyOIVImages(targetDir, txid);
		}

		/// <summary>
		/// FSImageSaver is being run in a separate thread when saving
		/// FSImage.
		/// </summary>
		/// <remarks>
		/// FSImageSaver is being run in a separate thread when saving
		/// FSImage. There is one thread per each copy of the image.
		/// FSImageSaver assumes that it was launched from a thread that holds
		/// FSNamesystem lock and waits for the execution of FSImageSaver thread
		/// to finish.
		/// This way we are guaranteed that the namespace is not being updated
		/// while multiple instances of FSImageSaver are traversing it
		/// and writing it out.
		/// </remarks>
		private class FSImageSaver : Runnable
		{
			private readonly SaveNamespaceContext context;

			private readonly Storage.StorageDirectory sd;

			private readonly NNStorage.NameNodeFile nnf;

			public FSImageSaver(FSImage _enclosing, SaveNamespaceContext context, Storage.StorageDirectory
				 sd, NNStorage.NameNodeFile nnf)
			{
				this._enclosing = _enclosing;
				this.context = context;
				this.sd = sd;
				this.nnf = nnf;
			}

			public virtual void Run()
			{
				try
				{
					this._enclosing.SaveFSImage(this.context, this.sd, this.nnf);
				}
				catch (SaveNamespaceCancelledException snce)
				{
					FSImage.Log.Info("Cancelled image saving for " + this.sd.GetRoot() + ": " + snce.
						Message);
				}
				catch (Exception t)
				{
					// don't report an error on the storage dir!
					FSImage.Log.Error("Unable to save image for " + this.sd.GetRoot(), t);
					this.context.ReportErrorOnStorageDirectory(this.sd);
				}
			}

			public override string ToString()
			{
				return "FSImageSaver for " + this.sd.GetRoot() + " of type " + this.sd.GetStorageDirType
					();
			}

			private readonly FSImage _enclosing;
		}

		private void WaitForThreads(IList<Sharpen.Thread> threads)
		{
			foreach (Sharpen.Thread thread in threads)
			{
				while (thread.IsAlive())
				{
					try
					{
						thread.Join();
					}
					catch (Exception)
					{
						Log.Error("Caught interrupted exception while waiting for thread " + thread.GetName
							() + " to finish. Retrying join");
					}
				}
			}
		}

		/// <summary>Update version of all storage directories.</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void UpdateStorageVersion()
		{
			lock (this)
			{
				storage.WriteAll();
			}
		}

		/// <seealso cref="SaveNamespace(FSNamesystem, NameNodeFile, Org.Apache.Hadoop.Hdfs.Util.Canceler)
		/// 	"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual void SaveNamespace(FSNamesystem source)
		{
			lock (this)
			{
				SaveNamespace(source, NNStorage.NameNodeFile.Image, null);
			}
		}

		/// <summary>
		/// Save the contents of the FS image to a new image file in each of the
		/// current storage directories.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void SaveNamespace(FSNamesystem source, NNStorage.NameNodeFile nnf
			, Canceler canceler)
		{
			lock (this)
			{
				System.Diagnostics.Debug.Assert(editLog != null, "editLog must be initialized");
				Log.Info("Save namespace ...");
				storage.AttemptRestoreRemovedStorage();
				bool editLogWasOpen = editLog.IsSegmentOpen();
				if (editLogWasOpen)
				{
					editLog.EndCurrentLogSegment(true);
				}
				long imageTxId = GetLastAppliedOrWrittenTxId();
				if (!AddToCheckpointing(imageTxId))
				{
					throw new IOException("FS image is being downloaded from another NN at txid " + imageTxId
						);
				}
				try
				{
					try
					{
						SaveFSImageInAllDirs(source, nnf, imageTxId, canceler);
						storage.WriteAll();
					}
					finally
					{
						if (editLogWasOpen)
						{
							editLog.StartLogSegment(imageTxId + 1, true);
							// Take this opportunity to note the current transaction.
							// Even if the namespace save was cancelled, this marker
							// is only used to determine what transaction ID is required
							// for startup. So, it doesn't hurt to update it unnecessarily.
							storage.WriteTransactionIdFileToStorage(imageTxId + 1);
						}
					}
				}
				finally
				{
					RemoveFromCheckpointing(imageTxId);
				}
			}
		}

		/// <seealso cref="SaveFSImageInAllDirs(FSNamesystem, NameNodeFile, long, Org.Apache.Hadoop.Hdfs.Util.Canceler)
		/// 	"/>
		/// <exception cref="System.IO.IOException"/>
		protected internal virtual void SaveFSImageInAllDirs(FSNamesystem source, long txid
			)
		{
			lock (this)
			{
				if (!AddToCheckpointing(txid))
				{
					throw new IOException(("FS image is being downloaded from another NN"));
				}
				try
				{
					SaveFSImageInAllDirs(source, NNStorage.NameNodeFile.Image, txid, null);
				}
				finally
				{
					RemoveFromCheckpointing(txid);
				}
			}
		}

		public virtual bool AddToCheckpointing(long txid)
		{
			return currentlyCheckpointing.AddItem(txid);
		}

		public virtual void RemoveFromCheckpointing(long txid)
		{
			currentlyCheckpointing.Remove(txid);
		}

		/// <exception cref="System.IO.IOException"/>
		private void SaveFSImageInAllDirs(FSNamesystem source, NNStorage.NameNodeFile nnf
			, long txid, Canceler canceler)
		{
			lock (this)
			{
				StartupProgress prog = NameNode.GetStartupProgress();
				prog.BeginPhase(Phase.SavingCheckpoint);
				if (storage.GetNumStorageDirs(NNStorage.NameNodeDirType.Image) == 0)
				{
					throw new IOException("No image directories available!");
				}
				if (canceler == null)
				{
					canceler = new Canceler();
				}
				SaveNamespaceContext ctx = new SaveNamespaceContext(source, txid, canceler);
				try
				{
					IList<Sharpen.Thread> saveThreads = new AList<Sharpen.Thread>();
					// save images into current
					for (IEnumerator<Storage.StorageDirectory> it = storage.DirIterator(NNStorage.NameNodeDirType
						.Image); it.HasNext(); )
					{
						Storage.StorageDirectory sd = it.Next();
						FSImage.FSImageSaver saver = new FSImage.FSImageSaver(this, ctx, sd, nnf);
						Sharpen.Thread saveThread = new Sharpen.Thread(saver, saver.ToString());
						saveThreads.AddItem(saveThread);
						saveThread.Start();
					}
					WaitForThreads(saveThreads);
					saveThreads.Clear();
					storage.ReportErrorsOnDirectories(ctx.GetErrorSDs());
					if (storage.GetNumStorageDirs(NNStorage.NameNodeDirType.Image) == 0)
					{
						throw new IOException("Failed to save in any storage directories while saving namespace."
							);
					}
					if (canceler.IsCancelled())
					{
						DeleteCancelledCheckpoint(txid);
						ctx.CheckCancelled();
						// throws
						System.Diagnostics.Debug.Assert(false, "should have thrown above!");
					}
					RenameCheckpoint(txid, NNStorage.NameNodeFile.ImageNew, nnf, false);
					// Since we now have a new checkpoint, we can clean up some
					// old edit logs and checkpoints.
					PurgeOldStorage(nnf);
				}
				finally
				{
					// Notify any threads waiting on the checkpoint to be canceled
					// that it is complete.
					ctx.MarkComplete();
					ctx = null;
				}
				prog.EndPhase(Phase.SavingCheckpoint);
			}
		}

		/// <summary>
		/// Purge any files in the storage directories that are no longer
		/// necessary.
		/// </summary>
		internal virtual void PurgeOldStorage(NNStorage.NameNodeFile nnf)
		{
			try
			{
				archivalManager.PurgeOldStorage(nnf);
			}
			catch (Exception e)
			{
				Log.Warn("Unable to purge old storage " + nnf.GetName(), e);
			}
		}

		/// <summary>Rename FSImage with the specific txid</summary>
		/// <exception cref="System.IO.IOException"/>
		private void RenameCheckpoint(long txid, NNStorage.NameNodeFile fromNnf, NNStorage.NameNodeFile
			 toNnf, bool renameMD5)
		{
			AList<Storage.StorageDirectory> al = null;
			foreach (Storage.StorageDirectory sd in storage.DirIterable(NNStorage.NameNodeDirType
				.Image))
			{
				try
				{
					RenameImageFileInDir(sd, fromNnf, toNnf, txid, renameMD5);
				}
				catch (IOException ioe)
				{
					Log.Warn("Unable to rename checkpoint in " + sd, ioe);
					if (al == null)
					{
						al = Lists.NewArrayList();
					}
					al.AddItem(sd);
				}
			}
			if (al != null)
			{
				storage.ReportErrorsOnDirectories(al);
			}
		}

		/// <summary>Rename all the fsimage files with the specific NameNodeFile type.</summary>
		/// <remarks>
		/// Rename all the fsimage files with the specific NameNodeFile type. The
		/// associated checksum files will also be renamed.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		internal virtual void RenameCheckpoint(NNStorage.NameNodeFile fromNnf, NNStorage.NameNodeFile
			 toNnf)
		{
			AList<Storage.StorageDirectory> al = null;
			FSImageTransactionalStorageInspector inspector = new FSImageTransactionalStorageInspector
				(EnumSet.Of(fromNnf));
			storage.InspectStorageDirs(inspector);
			foreach (FSImageStorageInspector.FSImageFile image in inspector.GetFoundImages())
			{
				try
				{
					RenameImageFileInDir(image.sd, fromNnf, toNnf, image.txId, true);
				}
				catch (IOException ioe)
				{
					Log.Warn("Unable to rename checkpoint in " + image.sd, ioe);
					if (al == null)
					{
						al = Lists.NewArrayList();
					}
					al.AddItem(image.sd);
				}
			}
			if (al != null)
			{
				storage.ReportErrorsOnDirectories(al);
			}
		}

		/// <summary>
		/// Deletes the checkpoint file in every storage directory,
		/// since the checkpoint was cancelled.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		private void DeleteCancelledCheckpoint(long txid)
		{
			AList<Storage.StorageDirectory> al = Lists.NewArrayList();
			foreach (Storage.StorageDirectory sd in storage.DirIterable(NNStorage.NameNodeDirType
				.Image))
			{
				FilePath ckpt = NNStorage.GetStorageFile(sd, NNStorage.NameNodeFile.ImageNew, txid
					);
				if (ckpt.Exists() && !ckpt.Delete())
				{
					Log.Warn("Unable to delete cancelled checkpoint in " + sd);
					al.AddItem(sd);
				}
			}
			storage.ReportErrorsOnDirectories(al);
		}

		/// <exception cref="System.IO.IOException"/>
		private void RenameImageFileInDir(Storage.StorageDirectory sd, NNStorage.NameNodeFile
			 fromNnf, NNStorage.NameNodeFile toNnf, long txid, bool renameMD5)
		{
			FilePath fromFile = NNStorage.GetStorageFile(sd, fromNnf, txid);
			FilePath toFile = NNStorage.GetStorageFile(sd, toNnf, txid);
			// renameTo fails on Windows if the destination file already exists.
			if (Log.IsDebugEnabled())
			{
				Log.Debug("renaming  " + fromFile.GetAbsolutePath() + " to " + toFile.GetAbsolutePath
					());
			}
			if (!fromFile.RenameTo(toFile))
			{
				if (!toFile.Delete() || !fromFile.RenameTo(toFile))
				{
					throw new IOException("renaming  " + fromFile.GetAbsolutePath() + " to " + toFile
						.GetAbsolutePath() + " FAILED");
				}
			}
			if (renameMD5)
			{
				MD5FileUtils.RenameMD5File(fromFile, toFile);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual CheckpointSignature RollEditLog()
		{
			GetEditLog().RollEditLog();
			// Record this log segment ID in all of the storage directories, so
			// we won't miss this log segment on a restart if the edits directories
			// go missing.
			storage.WriteTransactionIdFileToStorage(GetEditLog().GetCurSegmentTxId());
			return new CheckpointSignature(this);
		}

		/// <summary>Start checkpoint.</summary>
		/// <remarks>
		/// Start checkpoint.
		/// <p>
		/// If backup storage contains image that is newer than or incompatible with
		/// what the active name-node has, then the backup node should shutdown.<br />
		/// If the backup image is older than the active one then it should
		/// be discarded and downloaded from the active node.<br />
		/// If the images are the same then the backup image will be used as current.
		/// </remarks>
		/// <param name="bnReg">the backup node registration.</param>
		/// <param name="nnReg">this (active) name-node registration.</param>
		/// <returns>
		/// 
		/// <see cref="Org.Apache.Hadoop.Hdfs.Server.Protocol.NamenodeCommand"/>
		/// if backup node should shutdown or
		/// <see cref="Org.Apache.Hadoop.Hdfs.Server.Protocol.CheckpointCommand"/>
		/// prescribing what backup node should
		/// do with its image.
		/// </returns>
		/// <exception cref="System.IO.IOException"/>
		internal virtual NamenodeCommand StartCheckpoint(NamenodeRegistration bnReg, NamenodeRegistration
			 nnReg)
		{
			// backup node
			// active name-node
			Log.Info("Start checkpoint at txid " + GetEditLog().GetLastWrittenTxId());
			string msg = null;
			// Verify that checkpoint is allowed
			if (bnReg.GetNamespaceID() != storage.GetNamespaceID())
			{
				msg = "Name node " + bnReg.GetAddress() + " has incompatible namespace id: " + bnReg
					.GetNamespaceID() + " expected: " + storage.GetNamespaceID();
			}
			else
			{
				if (bnReg.IsRole(HdfsServerConstants.NamenodeRole.Namenode))
				{
					msg = "Name node " + bnReg.GetAddress() + " role " + bnReg.GetRole() + ": checkpoint is not allowed.";
				}
				else
				{
					if (bnReg.GetLayoutVersion() < storage.GetLayoutVersion() || (bnReg.GetLayoutVersion
						() == storage.GetLayoutVersion() && bnReg.GetCTime() > storage.GetCTime()))
					{
						// remote node has newer image age
						msg = "Name node " + bnReg.GetAddress() + " has newer image layout version: LV = "
							 + bnReg.GetLayoutVersion() + " cTime = " + bnReg.GetCTime() + ". Current version: LV = "
							 + storage.GetLayoutVersion() + " cTime = " + storage.GetCTime();
					}
				}
			}
			if (msg != null)
			{
				Log.Error(msg);
				return new NamenodeCommand(NamenodeProtocol.ActShutdown);
			}
			bool needToReturnImg = true;
			if (storage.GetNumStorageDirs(NNStorage.NameNodeDirType.Image) == 0)
			{
				// do not return image if there are no image directories
				needToReturnImg = false;
			}
			CheckpointSignature sig = RollEditLog();
			return new CheckpointCommand(sig, needToReturnImg);
		}

		/// <summary>End checkpoint.</summary>
		/// <remarks>
		/// End checkpoint.
		/// <p>
		/// Validate the current storage info with the given signature.
		/// </remarks>
		/// <param name="sig">to validate the current storage info against</param>
		/// <exception cref="System.IO.IOException">if the checkpoint fields are inconsistent
		/// 	</exception>
		internal virtual void EndCheckpoint(CheckpointSignature sig)
		{
			Log.Info("End checkpoint at txid " + GetEditLog().GetLastWrittenTxId());
			sig.ValidateStorageInfo(this);
		}

		/// <summary>
		/// This is called by the 2NN after having downloaded an image, and by
		/// the NN after having received a new image from the 2NN.
		/// </summary>
		/// <remarks>
		/// This is called by the 2NN after having downloaded an image, and by
		/// the NN after having received a new image from the 2NN. It
		/// renames the image from fsimage_N.ckpt to fsimage_N and also
		/// saves the related .md5 file into place.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public virtual void SaveDigestAndRenameCheckpointImage(NNStorage.NameNodeFile nnf
			, long txid, MD5Hash digest)
		{
			lock (this)
			{
				// Write and rename MD5 file
				IList<Storage.StorageDirectory> badSds = Lists.NewArrayList();
				foreach (Storage.StorageDirectory sd in storage.DirIterable(NNStorage.NameNodeDirType
					.Image))
				{
					FilePath imageFile = NNStorage.GetImageFile(sd, nnf, txid);
					try
					{
						MD5FileUtils.SaveMD5File(imageFile, digest);
					}
					catch (IOException)
					{
						badSds.AddItem(sd);
					}
				}
				storage.ReportErrorsOnDirectories(badSds);
				CheckpointFaultInjector.GetInstance().AfterMD5Rename();
				// Rename image from tmp file
				RenameCheckpoint(txid, NNStorage.NameNodeFile.ImageNew, nnf, false);
				// So long as this is the newest image available,
				// advertise it as such to other checkpointers
				// from now on
				if (txid > storage.GetMostRecentCheckpointTxId())
				{
					storage.SetMostRecentCheckpointInfo(txid, Time.Now());
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void Close()
		{
			lock (this)
			{
				if (editLog != null)
				{
					// 2NN doesn't have any edit log
					GetEditLog().Close();
				}
				storage.Close();
			}
		}

		/// <summary>Retrieve checkpoint dirs from configuration.</summary>
		/// <param name="conf">the Configuration</param>
		/// <param name="defaultValue">a default value for the attribute, if null</param>
		/// <returns>
		/// a Collection of URIs representing the values in
		/// dfs.namenode.checkpoint.dir configuration property
		/// </returns>
		internal static ICollection<URI> GetCheckpointDirs(Configuration conf, string defaultValue
			)
		{
			ICollection<string> dirNames = conf.GetTrimmedStringCollection(DFSConfigKeys.DfsNamenodeCheckpointDirKey
				);
			if (dirNames.Count == 0 && defaultValue != null)
			{
				dirNames.AddItem(defaultValue);
			}
			return Org.Apache.Hadoop.Hdfs.Server.Common.Util.StringCollectionAsURIs(dirNames);
		}

		internal static IList<URI> GetCheckpointEditsDirs(Configuration conf, string defaultName
			)
		{
			ICollection<string> dirNames = conf.GetTrimmedStringCollection(DFSConfigKeys.DfsNamenodeCheckpointEditsDirKey
				);
			if (dirNames.Count == 0 && defaultName != null)
			{
				dirNames.AddItem(defaultName);
			}
			return Org.Apache.Hadoop.Hdfs.Server.Common.Util.StringCollectionAsURIs(dirNames);
		}

		public virtual NNStorage GetStorage()
		{
			return storage;
		}

		public virtual int GetLayoutVersion()
		{
			return storage.GetLayoutVersion();
		}

		public virtual int GetNamespaceID()
		{
			return storage.GetNamespaceID();
		}

		public virtual string GetClusterID()
		{
			return storage.GetClusterID();
		}

		public virtual string GetBlockPoolID()
		{
			return storage.GetBlockPoolID();
		}

		public virtual long GetLastAppliedTxId()
		{
			lock (this)
			{
				return lastAppliedTxId;
			}
		}

		public virtual long GetLastAppliedOrWrittenTxId()
		{
			return Math.Max(lastAppliedTxId, editLog != null ? editLog.GetLastWrittenTxId() : 
				0);
		}

		public virtual void UpdateLastAppliedTxIdFromWritten()
		{
			this.lastAppliedTxId = editLog.GetLastWrittenTxId();
		}

		// Should be OK for this to not be synchronized since all of the places which
		// mutate this value are themselves synchronized so it shouldn't be possible
		// to see this flop back and forth. In the worst case this will just return an
		// old value.
		public virtual long GetMostRecentCheckpointTxId()
		{
			return storage.GetMostRecentCheckpointTxId();
		}
	}
}
