using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Com.Google.Common.Annotations;
using Com.Google.Common.Base;
using Com.Google.Common.Collect;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Hdfs.Server.Protocol;
using Org.Apache.Hadoop.Hdfs.Util;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>
	/// NNStorage is responsible for management of the StorageDirectories used by
	/// the NameNode.
	/// </summary>
	public class NNStorage : Storage, IDisposable, StorageErrorReporter
	{
		internal const string DeprecatedMessageDigestProperty = "imageMD5Digest";

		internal const string LocalUriScheme = "file";

		[System.Serializable]
		public sealed class NameNodeFile
		{
			public static readonly NNStorage.NameNodeFile Image = new NNStorage.NameNodeFile(
				"fsimage");

			public static readonly NNStorage.NameNodeFile Time = new NNStorage.NameNodeFile("fstime"
				);

			public static readonly NNStorage.NameNodeFile SeenTxid = new NNStorage.NameNodeFile
				("seen_txid");

			public static readonly NNStorage.NameNodeFile Edits = new NNStorage.NameNodeFile(
				"edits");

			public static readonly NNStorage.NameNodeFile ImageNew = new NNStorage.NameNodeFile
				("fsimage.ckpt");

			public static readonly NNStorage.NameNodeFile ImageRollback = new NNStorage.NameNodeFile
				("fsimage_rollback");

			public static readonly NNStorage.NameNodeFile EditsNew = new NNStorage.NameNodeFile
				("edits.new");

			public static readonly NNStorage.NameNodeFile EditsInprogress = new NNStorage.NameNodeFile
				("edits_inprogress");

			public static readonly NNStorage.NameNodeFile EditsTmp = new NNStorage.NameNodeFile
				("edits_tmp");

			public static readonly NNStorage.NameNodeFile ImageLegacyOiv = new NNStorage.NameNodeFile
				("fsimage_legacy_oiv");

			private string fileName = null;

			private NameNodeFile(string name)
			{
				//
				// The filenames used for storing the images
				//
				// from "old" pre-HDFS-1073 format
				// from "old" pre-HDFS-1073 format
				// For pre-PB format
				this.fileName = name;
			}

			[VisibleForTesting]
			public string GetName()
			{
				return NNStorage.NameNodeFile.fileName;
			}
		}

		/// <summary>
		/// Implementation of StorageDirType specific to namenode storage
		/// A Storage directory could be of type IMAGE which stores only fsimage,
		/// or of type EDITS which stores edits or of type IMAGE_AND_EDITS which
		/// stores both fsimage and edits.
		/// </summary>
		[System.Serializable]
		public sealed class NameNodeDirType : Storage.StorageDirType
		{
			public static readonly NNStorage.NameNodeDirType Undefined = new NNStorage.NameNodeDirType
				();

			public static readonly NNStorage.NameNodeDirType Image = new NNStorage.NameNodeDirType
				();

			public static readonly NNStorage.NameNodeDirType Edits = new NNStorage.NameNodeDirType
				();

			public static readonly NNStorage.NameNodeDirType ImageAndEdits = new NNStorage.NameNodeDirType
				();

			public Storage.StorageDirType GetStorageDirType()
			{
				return this;
			}

			public bool IsOfType(Storage.StorageDirType type)
			{
				if ((this == NNStorage.NameNodeDirType.ImageAndEdits) && (type == NNStorage.NameNodeDirType
					.Image || type == NNStorage.NameNodeDirType.Edits))
				{
					return true;
				}
				return this == type;
			}
		}

		protected internal string blockpoolID = string.Empty;

		/// <summary>flag that controls if we try to restore failed storages</summary>
		private bool restoreFailedStorage = false;

		private readonly object restorationLock = new object();

		private bool disablePreUpgradableLayoutCheck = false;

		/// <summary>
		/// TxId of the last transaction that was included in the most
		/// recent fsimage file.
		/// </summary>
		/// <remarks>
		/// TxId of the last transaction that was included in the most
		/// recent fsimage file. This does not include any transactions
		/// that have since been written to the edit log.
		/// </remarks>
		protected internal volatile long mostRecentCheckpointTxId = HdfsConstants.InvalidTxid;

		/// <summary>Time of the last checkpoint, in milliseconds since the epoch.</summary>
		private long mostRecentCheckpointTime = 0;

		/// <summary>list of failed (and thus removed) storages</summary>
		protected internal readonly IList<Storage.StorageDirectory> removedStorageDirs = 
			new CopyOnWriteArrayList<Storage.StorageDirectory>();

		/// <summary>
		/// Properties from old layout versions that may be needed
		/// during upgrade only.
		/// </summary>
		private Dictionary<string, string> deprecatedProperties;

		/// <summary>Construct the NNStorage.</summary>
		/// <param name="conf">Namenode configuration.</param>
		/// <param name="imageDirs">Directories the image can be stored in.</param>
		/// <param name="editsDirs">Directories the editlog can be stored in.</param>
		/// <exception cref="System.IO.IOException">if any directories are inaccessible.</exception>
		public NNStorage(Configuration conf, ICollection<URI> imageDirs, ICollection<URI>
			 editsDirs)
			: base(HdfsServerConstants.NodeType.NameNode)
		{
			// id of the block pool
			storageDirs = new CopyOnWriteArrayList<Storage.StorageDirectory>();
			// this may modify the editsDirs, so copy before passing in
			SetStorageDirectories(imageDirs, Lists.NewArrayList(editsDirs), FSNamesystem.GetSharedEditsDirs
				(conf));
		}

		/// <exception cref="System.IO.IOException"/>
		public override bool IsPreUpgradableLayout(Storage.StorageDirectory sd)
		{
			// Storage
			if (disablePreUpgradableLayoutCheck)
			{
				return false;
			}
			FilePath oldImageDir = new FilePath(sd.GetRoot(), "image");
			if (!oldImageDir.Exists())
			{
				return false;
			}
			// check the layout version inside the image file
			FilePath oldF = new FilePath(oldImageDir, "fsimage");
			RandomAccessFile oldFile = new RandomAccessFile(oldF, "rws");
			try
			{
				oldFile.Seek(0);
				int oldVersion = oldFile.ReadInt();
				oldFile.Close();
				oldFile = null;
				if (oldVersion < LastPreUpgradeLayoutVersion)
				{
					return false;
				}
			}
			finally
			{
				IOUtils.Cleanup(Log, oldFile);
			}
			return true;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void Close()
		{
			// Closeable
			UnlockAll();
			storageDirs.Clear();
		}

		/// <summary>
		/// Set flag whether an attempt should be made to restore failed storage
		/// directories at the next available oppurtuinity.
		/// </summary>
		/// <param name="val">Whether restoration attempt should be made.</param>
		internal virtual void SetRestoreFailedStorage(bool val)
		{
			Log.Warn("set restore failed storage to " + val);
			restoreFailedStorage = val;
		}

		/// <returns>Whether failed storage directories are to be restored.</returns>
		internal virtual bool GetRestoreFailedStorage()
		{
			return restoreFailedStorage;
		}

		/// <summary>
		/// See if any of removed storages is "writable" again, and can be returned
		/// into service.
		/// </summary>
		internal virtual void AttemptRestoreRemovedStorage()
		{
			// if directory is "alive" - copy the images there...
			if (!restoreFailedStorage || removedStorageDirs.Count == 0)
			{
				return;
			}
			//nothing to restore
			/* We don't want more than one thread trying to restore at a time */
			lock (this.restorationLock)
			{
				Log.Info("NNStorage.attemptRestoreRemovedStorage: check removed(failed) " + "storarge. removedStorages size = "
					 + removedStorageDirs.Count);
				for (IEnumerator<Storage.StorageDirectory> it = this.removedStorageDirs.GetEnumerator
					(); it.HasNext(); )
				{
					Storage.StorageDirectory sd = it.Next();
					FilePath root = sd.GetRoot();
					Log.Info("currently disabled dir " + root.GetAbsolutePath() + "; type=" + sd.GetStorageDirType
						() + ";canwrite=" + FileUtil.CanWrite(root));
					if (root.Exists() && FileUtil.CanWrite(root))
					{
						Log.Info("restoring dir " + sd.GetRoot().GetAbsolutePath());
						this.AddStorageDir(sd);
						// restore
						this.removedStorageDirs.Remove(sd);
					}
				}
			}
		}

		/// <returns>A list of storage directories which are in the errored state.</returns>
		internal virtual IList<Storage.StorageDirectory> GetRemovedStorageDirs()
		{
			return this.removedStorageDirs;
		}

		/// <summary>
		/// See
		/// <see cref="SetStorageDirectories(System.Collections.Generic.ICollection{E}, System.Collections.Generic.ICollection{E}, System.Collections.Generic.ICollection{E})
		/// 	"/>
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		[VisibleForTesting]
		internal virtual void SetStorageDirectories(ICollection<URI> fsNameDirs, ICollection
			<URI> fsEditsDirs)
		{
			lock (this)
			{
				SetStorageDirectories(fsNameDirs, fsEditsDirs, new AList<URI>());
			}
		}

		/// <summary>Set the storage directories which will be used.</summary>
		/// <remarks>
		/// Set the storage directories which will be used. This should only ever be
		/// called from inside NNStorage. However, it needs to remain package private
		/// for testing, as StorageDirectories need to be reinitialised after using
		/// Mockito.spy() on this class, as Mockito doesn't work well with inner
		/// classes, such as StorageDirectory in this case.
		/// Synchronized due to initialization of storageDirs and removedStorageDirs.
		/// </remarks>
		/// <param name="fsNameDirs">Locations to store images.</param>
		/// <param name="fsEditsDirs">Locations to store edit logs.</param>
		/// <exception cref="System.IO.IOException"/>
		[VisibleForTesting]
		internal virtual void SetStorageDirectories(ICollection<URI> fsNameDirs, ICollection
			<URI> fsEditsDirs, ICollection<URI> sharedEditsDirs)
		{
			lock (this)
			{
				this.storageDirs.Clear();
				this.removedStorageDirs.Clear();
				// Add all name dirs with appropriate NameNodeDirType
				foreach (URI dirName in fsNameDirs)
				{
					CheckSchemeConsistency(dirName);
					bool isAlsoEdits = false;
					foreach (URI editsDirName in fsEditsDirs)
					{
						if (editsDirName.CompareTo(dirName) == 0)
						{
							isAlsoEdits = true;
							fsEditsDirs.Remove(editsDirName);
							break;
						}
					}
					NNStorage.NameNodeDirType dirType = (isAlsoEdits) ? NNStorage.NameNodeDirType.ImageAndEdits
						 : NNStorage.NameNodeDirType.Image;
					// Add to the list of storage directories, only if the
					// URI is of type file://
					if (string.CompareOrdinal(dirName.GetScheme(), "file") == 0)
					{
						this.AddStorageDir(new Storage.StorageDirectory(new FilePath(dirName.GetPath()), 
							dirType, sharedEditsDirs.Contains(dirName)));
					}
				}
				// Don't lock the dir if it's shared.
				// Add edits dirs if they are different from name dirs
				foreach (URI dirName_1 in fsEditsDirs)
				{
					CheckSchemeConsistency(dirName_1);
					// Add to the list of storage directories, only if the
					// URI is of type file://
					if (string.CompareOrdinal(dirName_1.GetScheme(), "file") == 0)
					{
						this.AddStorageDir(new Storage.StorageDirectory(new FilePath(dirName_1.GetPath())
							, NNStorage.NameNodeDirType.Edits, sharedEditsDirs.Contains(dirName_1)));
					}
				}
			}
		}

		/// <summary>Return the storage directory corresponding to the passed URI</summary>
		/// <param name="uri">URI of a storage directory</param>
		/// <returns>The matching storage directory or null if none found</returns>
		internal virtual Storage.StorageDirectory GetStorageDirectory(URI uri)
		{
			try
			{
				uri = Util.FileAsURI(new FilePath(uri));
				IEnumerator<Storage.StorageDirectory> it = DirIterator();
				for (; it.HasNext(); )
				{
					Storage.StorageDirectory sd = it.Next();
					if (Util.FileAsURI(sd.GetRoot()).Equals(uri))
					{
						return sd;
					}
				}
			}
			catch (IOException ioe)
			{
				Log.Warn("Error converting file to URI", ioe);
			}
			return null;
		}

		/// <summary>
		/// Checks the consistency of a URI, in particular if the scheme
		/// is specified
		/// </summary>
		/// <param name="u">URI whose consistency is being checked.</param>
		/// <exception cref="System.IO.IOException"/>
		private static void CheckSchemeConsistency(URI u)
		{
			string scheme = u.GetScheme();
			// the URI should have a proper scheme
			if (scheme == null)
			{
				throw new IOException("Undefined scheme for " + u);
			}
		}

		/// <summary>Retrieve current directories of type IMAGE</summary>
		/// <returns>Collection of URI representing image directories</returns>
		/// <exception cref="System.IO.IOException">in case of URI processing error</exception>
		internal virtual ICollection<URI> GetImageDirectories()
		{
			return GetDirectories(NNStorage.NameNodeDirType.Image);
		}

		/// <summary>Retrieve current directories of type EDITS</summary>
		/// <returns>Collection of URI representing edits directories</returns>
		/// <exception cref="System.IO.IOException">in case of URI processing error</exception>
		internal virtual ICollection<URI> GetEditsDirectories()
		{
			return GetDirectories(NNStorage.NameNodeDirType.Edits);
		}

		/// <summary>Return number of storage directories of the given type.</summary>
		/// <param name="dirType">directory type</param>
		/// <returns>number of storage directories of type dirType</returns>
		internal virtual int GetNumStorageDirs(NNStorage.NameNodeDirType dirType)
		{
			if (dirType == null)
			{
				return GetNumStorageDirs();
			}
			IEnumerator<Storage.StorageDirectory> it = DirIterator(dirType);
			int numDirs = 0;
			for (; it.HasNext(); it.Next())
			{
				numDirs++;
			}
			return numDirs;
		}

		/// <summary>Return the list of locations being used for a specific purpose.</summary>
		/// <remarks>
		/// Return the list of locations being used for a specific purpose.
		/// i.e. Image or edit log storage.
		/// </remarks>
		/// <param name="dirType">Purpose of locations requested.</param>
		/// <exception cref="System.IO.IOException"/>
		internal virtual ICollection<URI> GetDirectories(NNStorage.NameNodeDirType dirType
			)
		{
			AList<URI> list = new AList<URI>();
			IEnumerator<Storage.StorageDirectory> it = (dirType == null) ? DirIterator() : DirIterator
				(dirType);
			for (; it.HasNext(); )
			{
				Storage.StorageDirectory sd = it.Next();
				try
				{
					list.AddItem(Util.FileAsURI(sd.GetRoot()));
				}
				catch (IOException e)
				{
					throw new IOException("Exception while processing " + "StorageDirectory " + sd.GetRoot
						(), e);
				}
			}
			return list;
		}

		/// <summary>Determine the last transaction ID noted in this storage directory.</summary>
		/// <remarks>
		/// Determine the last transaction ID noted in this storage directory.
		/// This txid is stored in a special seen_txid file since it might not
		/// correspond to the latest image or edit log. For example, an image-only
		/// directory will have this txid incremented when edits logs roll, even
		/// though the edits logs are in a different directory.
		/// </remarks>
		/// <param name="sd">StorageDirectory to check</param>
		/// <returns>If file exists and can be read, last recorded txid. If not, 0L.</returns>
		/// <exception cref="System.IO.IOException">On errors processing file pointed to by sd
		/// 	</exception>
		internal static long ReadTransactionIdFile(Storage.StorageDirectory sd)
		{
			FilePath txidFile = GetStorageFile(sd, NNStorage.NameNodeFile.SeenTxid);
			return PersistentLongFile.ReadFile(txidFile, 0);
		}

		/// <summary>Write last checkpoint time into a separate file.</summary>
		/// <param name="sd">storage directory</param>
		/// <exception cref="System.IO.IOException"/>
		internal virtual void WriteTransactionIdFile(Storage.StorageDirectory sd, long txid
			)
		{
			Preconditions.CheckArgument(txid >= 0, "bad txid: " + txid);
			FilePath txIdFile = GetStorageFile(sd, NNStorage.NameNodeFile.SeenTxid);
			PersistentLongFile.WriteFile(txIdFile, txid);
		}

		/// <summary>Set the transaction ID and time of the last checkpoint</summary>
		/// <param name="txid">transaction id of the last checkpoint</param>
		/// <param name="time">time of the last checkpoint, in millis since the epoch</param>
		internal virtual void SetMostRecentCheckpointInfo(long txid, long time)
		{
			this.mostRecentCheckpointTxId = txid;
			this.mostRecentCheckpointTime = time;
		}

		/// <returns>the transaction ID of the last checkpoint.</returns>
		public virtual long GetMostRecentCheckpointTxId()
		{
			return mostRecentCheckpointTxId;
		}

		/// <returns>the time of the most recent checkpoint in millis since the epoch.</returns>
		internal virtual long GetMostRecentCheckpointTime()
		{
			return mostRecentCheckpointTime;
		}

		/// <summary>
		/// Write a small file in all available storage directories that
		/// indicates that the namespace has reached some given transaction ID.
		/// </summary>
		/// <remarks>
		/// Write a small file in all available storage directories that
		/// indicates that the namespace has reached some given transaction ID.
		/// This is used when the image is loaded to avoid accidental rollbacks
		/// in the case where an edit log is fully deleted but there is no
		/// checkpoint. See TestNameEditsConfigs.testNameEditsConfigsFailure()
		/// </remarks>
		/// <param name="txid">the txid that has been reached</param>
		public virtual void WriteTransactionIdFileToStorage(long txid)
		{
			// Write txid marker in all storage directories
			foreach (Storage.StorageDirectory sd in storageDirs)
			{
				try
				{
					WriteTransactionIdFile(sd, txid);
				}
				catch (IOException e)
				{
					// Close any edits stream associated with this dir and remove directory
					Log.Warn("writeTransactionIdToStorage failed on " + sd, e);
					ReportErrorsOnDirectory(sd);
				}
			}
		}

		/// <summary>
		/// Return the name of the image file that is uploaded by periodic
		/// checkpointing
		/// </summary>
		/// <returns>List of filenames to save checkpoints to.</returns>
		public virtual FilePath[] GetFsImageNameCheckpoint(long txid)
		{
			AList<FilePath> list = new AList<FilePath>();
			for (IEnumerator<Storage.StorageDirectory> it = DirIterator(NNStorage.NameNodeDirType
				.Image); it.HasNext(); )
			{
				list.AddItem(GetStorageFile(it.Next(), NNStorage.NameNodeFile.ImageNew, txid));
			}
			return Sharpen.Collections.ToArray(list, new FilePath[list.Count]);
		}

		/// <returns>The first image file with the given txid and image type.</returns>
		public virtual FilePath GetFsImageName(long txid, NNStorage.NameNodeFile nnf)
		{
			for (IEnumerator<Storage.StorageDirectory> it = DirIterator(NNStorage.NameNodeDirType
				.Image); it.HasNext(); )
			{
				Storage.StorageDirectory sd = it.Next();
				FilePath fsImage = GetStorageFile(sd, nnf, txid);
				if (FileUtil.CanRead(sd.GetRoot()) && fsImage.Exists())
				{
					return fsImage;
				}
			}
			return null;
		}

		/// <returns>
		/// The first image file whose txid is the same with the given txid and
		/// image type is one of the given types.
		/// </returns>
		public virtual FilePath GetFsImage(long txid, EnumSet<NNStorage.NameNodeFile> nnfs
			)
		{
			for (IEnumerator<Storage.StorageDirectory> it = DirIterator(NNStorage.NameNodeDirType
				.Image); it.HasNext(); )
			{
				Storage.StorageDirectory sd = it.Next();
				foreach (NNStorage.NameNodeFile nnf in nnfs)
				{
					FilePath fsImage = GetStorageFile(sd, nnf, txid);
					if (FileUtil.CanRead(sd.GetRoot()) && fsImage.Exists())
					{
						return fsImage;
					}
				}
			}
			return null;
		}

		public virtual FilePath GetFsImageName(long txid)
		{
			return GetFsImageName(txid, NNStorage.NameNodeFile.Image);
		}

		public virtual FilePath GetHighestFsImageName()
		{
			return GetFsImageName(GetMostRecentCheckpointTxId());
		}

		/// <summary>Create new dfs name directory.</summary>
		/// <remarks>
		/// Create new dfs name directory.  Caution: this destroys all files
		/// in this filesystem.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		private void Format(Storage.StorageDirectory sd)
		{
			sd.ClearDirectory();
			// create currrent dir
			WriteProperties(sd);
			WriteTransactionIdFile(sd, 0);
			Log.Info("Storage directory " + sd.GetRoot() + " has been successfully formatted."
				);
		}

		/// <summary>Format all available storage directories.</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void Format(NamespaceInfo nsInfo)
		{
			Preconditions.CheckArgument(nsInfo.GetLayoutVersion() == 0 || nsInfo.GetLayoutVersion
				() == HdfsConstants.NamenodeLayoutVersion, "Bad layout version: %s", nsInfo.GetLayoutVersion
				());
			this.SetStorageInfo(nsInfo);
			this.blockpoolID = nsInfo.GetBlockPoolID();
			for (IEnumerator<Storage.StorageDirectory> it = DirIterator(); it.HasNext(); )
			{
				Storage.StorageDirectory sd = it.Next();
				Format(sd);
			}
		}

		/// <exception cref="Sharpen.UnknownHostException"/>
		public static NamespaceInfo NewNamespaceInfo()
		{
			return new NamespaceInfo(NewNamespaceID(), NewClusterID(), NewBlockPoolID(), 0L);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void Format()
		{
			this.layoutVersion = HdfsConstants.NamenodeLayoutVersion;
			for (IEnumerator<Storage.StorageDirectory> it = DirIterator(); it.HasNext(); )
			{
				Storage.StorageDirectory sd = it.Next();
				Format(sd);
			}
		}

		/// <summary>Generate new namespaceID.</summary>
		/// <remarks>
		/// Generate new namespaceID.
		/// namespaceID is a persistent attribute of the namespace.
		/// It is generated when the namenode is formatted and remains the same
		/// during the life cycle of the namenode.
		/// When a datanodes register they receive it as the registrationID,
		/// which is checked every time the datanode is communicating with the
		/// namenode. Datanodes that do not 'know' the namespaceID are rejected.
		/// </remarks>
		/// <returns>new namespaceID</returns>
		private static int NewNamespaceID()
		{
			int newID = 0;
			while (newID == 0)
			{
				newID = DFSUtil.GetRandom().Next(unchecked((int)(0x7FFFFFFF)));
			}
			// use 31 bits only
			return newID;
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override void SetFieldsFromProperties(Properties props, Storage.StorageDirectory
			 sd)
		{
			// Storage
			base.SetFieldsFromProperties(props, sd);
			if (layoutVersion == 0)
			{
				throw new IOException("NameNode directory " + sd.GetRoot() + " is not formatted."
					);
			}
			// Set Block pool ID in version with federation support
			if (NameNodeLayoutVersion.Supports(LayoutVersion.Feature.Federation, GetLayoutVersion
				()))
			{
				string sbpid = props.GetProperty("blockpoolID");
				SetBlockPoolID(sd.GetRoot(), sbpid);
			}
			SetDeprecatedPropertiesForUpgrade(props);
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual void ReadProperties(Storage.StorageDirectory sd, HdfsServerConstants.StartupOption
			 startupOption)
		{
			Properties props = ReadPropertiesFile(sd.GetVersionFile());
			if (HdfsServerConstants.RollingUpgradeStartupOption.Rollback.Matches(startupOption
				))
			{
				int lv = System.Convert.ToInt32(GetProperty(props, sd, "layoutVersion"));
				if (lv > GetServiceLayoutVersion())
				{
					// we should not use a newer version for rollingUpgrade rollback
					throw new IncorrectVersionException(GetServiceLayoutVersion(), lv, "storage directory "
						 + sd.GetRoot().GetAbsolutePath());
				}
				props.SetProperty("layoutVersion", Sharpen.Extensions.ToString(HdfsConstants.NamenodeLayoutVersion
					));
			}
			SetFieldsFromProperties(props, sd);
		}

		/// <summary>
		/// Pull any properties out of the VERSION file that are from older
		/// versions of HDFS and only necessary during upgrade.
		/// </summary>
		private void SetDeprecatedPropertiesForUpgrade(Properties props)
		{
			deprecatedProperties = new Dictionary<string, string>();
			string md5 = props.GetProperty(DeprecatedMessageDigestProperty);
			if (md5 != null)
			{
				deprecatedProperties[DeprecatedMessageDigestProperty] = md5;
			}
		}

		/// <summary>Return a property that was stored in an earlier version of HDFS.</summary>
		/// <remarks>
		/// Return a property that was stored in an earlier version of HDFS.
		/// This should only be used during upgrades.
		/// </remarks>
		internal virtual string GetDeprecatedProperty(string prop)
		{
			System.Diagnostics.Debug.Assert(GetLayoutVersion() > HdfsConstants.NamenodeLayoutVersion
				, "getDeprecatedProperty should only be done when loading " + "storage from past versions during upgrade."
				);
			return deprecatedProperties[prop];
		}

		/// <summary>Write version file into the storage directory.</summary>
		/// <remarks>
		/// Write version file into the storage directory.
		/// The version file should always be written last.
		/// Missing or corrupted version file indicates that
		/// the checkpoint is not valid.
		/// </remarks>
		/// <param name="sd">storage directory</param>
		/// <exception cref="System.IO.IOException"/>
		protected internal override void SetPropertiesFromFields(Properties props, Storage.StorageDirectory
			 sd)
		{
			// Storage
			base.SetPropertiesFromFields(props, sd);
			// Set blockpoolID in version with federation support
			if (NameNodeLayoutVersion.Supports(LayoutVersion.Feature.Federation, GetLayoutVersion
				()))
			{
				props.SetProperty("blockpoolID", blockpoolID);
			}
		}

		internal static FilePath GetStorageFile(Storage.StorageDirectory sd, NNStorage.NameNodeFile
			 type, long imageTxId)
		{
			return new FilePath(sd.GetCurrentDir(), string.Format("%s_%019d", type.GetName(), 
				imageTxId));
		}

		/// <summary>
		/// Get a storage file for one of the files that doesn't need a txid associated
		/// (e.g version, seen_txid)
		/// </summary>
		internal static FilePath GetStorageFile(Storage.StorageDirectory sd, NNStorage.NameNodeFile
			 type)
		{
			return new FilePath(sd.GetCurrentDir(), type.GetName());
		}

		[VisibleForTesting]
		public static string GetCheckpointImageFileName(long txid)
		{
			return GetNameNodeFileName(NNStorage.NameNodeFile.ImageNew, txid);
		}

		[VisibleForTesting]
		public static string GetImageFileName(long txid)
		{
			return GetNameNodeFileName(NNStorage.NameNodeFile.Image, txid);
		}

		[VisibleForTesting]
		public static string GetRollbackImageFileName(long txid)
		{
			return GetNameNodeFileName(NNStorage.NameNodeFile.ImageRollback, txid);
		}

		public static string GetLegacyOIVImageFileName(long txid)
		{
			return GetNameNodeFileName(NNStorage.NameNodeFile.ImageLegacyOiv, txid);
		}

		private static string GetNameNodeFileName(NNStorage.NameNodeFile nnf, long txid)
		{
			return string.Format("%s_%019d", nnf.GetName(), txid);
		}

		[VisibleForTesting]
		public static string GetInProgressEditsFileName(long startTxId)
		{
			return GetNameNodeFileName(NNStorage.NameNodeFile.EditsInprogress, startTxId);
		}

		internal static FilePath GetInProgressEditsFile(Storage.StorageDirectory sd, long
			 startTxId)
		{
			return new FilePath(sd.GetCurrentDir(), GetInProgressEditsFileName(startTxId));
		}

		internal static FilePath GetFinalizedEditsFile(Storage.StorageDirectory sd, long 
			startTxId, long endTxId)
		{
			return new FilePath(sd.GetCurrentDir(), GetFinalizedEditsFileName(startTxId, endTxId
				));
		}

		internal static FilePath GetTemporaryEditsFile(Storage.StorageDirectory sd, long 
			startTxId, long endTxId, long timestamp)
		{
			return new FilePath(sd.GetCurrentDir(), GetTemporaryEditsFileName(startTxId, endTxId
				, timestamp));
		}

		internal static FilePath GetImageFile(Storage.StorageDirectory sd, NNStorage.NameNodeFile
			 nnf, long txid)
		{
			return new FilePath(sd.GetCurrentDir(), GetNameNodeFileName(nnf, txid));
		}

		[VisibleForTesting]
		public static string GetFinalizedEditsFileName(long startTxId, long endTxId)
		{
			return string.Format("%s_%019d-%019d", NNStorage.NameNodeFile.Edits.GetName(), startTxId
				, endTxId);
		}

		public static string GetTemporaryEditsFileName(long startTxId, long endTxId, long
			 timestamp)
		{
			return string.Format("%s_%019d-%019d_%019d", NNStorage.NameNodeFile.EditsTmp.GetName
				(), startTxId, endTxId, timestamp);
		}

		/// <summary>Return the first readable finalized edits file for the given txid.</summary>
		/// <exception cref="System.IO.IOException"/>
		internal virtual FilePath FindFinalizedEditsFile(long startTxId, long endTxId)
		{
			FilePath ret = FindFile(NNStorage.NameNodeDirType.Edits, GetFinalizedEditsFileName
				(startTxId, endTxId));
			if (ret == null)
			{
				throw new IOException("No edits file for txid " + startTxId + "-" + endTxId + " exists!"
					);
			}
			return ret;
		}

		/// <summary>
		/// Return the first readable image file for the given txid and image type, or
		/// null if no such image can be found
		/// </summary>
		internal virtual FilePath FindImageFile(NNStorage.NameNodeFile nnf, long txid)
		{
			return FindFile(NNStorage.NameNodeDirType.Image, GetNameNodeFileName(nnf, txid));
		}

		/// <summary>
		/// Return the first readable storage file of the given name
		/// across any of the 'current' directories in SDs of the
		/// given type, or null if no such file exists.
		/// </summary>
		private FilePath FindFile(NNStorage.NameNodeDirType dirType, string name)
		{
			foreach (Storage.StorageDirectory sd in DirIterable(dirType))
			{
				FilePath candidate = new FilePath(sd.GetCurrentDir(), name);
				if (FileUtil.CanRead(sd.GetCurrentDir()) && candidate.Exists())
				{
					return candidate;
				}
			}
			return null;
		}

		/// <summary>Disable the check for pre-upgradable layouts.</summary>
		/// <remarks>Disable the check for pre-upgradable layouts. Needed for BackupImage.</remarks>
		/// <param name="val">Whether to disable the preupgradeable layout check.</param>
		internal virtual void SetDisablePreUpgradableLayoutCheck(bool val)
		{
			disablePreUpgradableLayoutCheck = val;
		}

		/// <summary>Marks a list of directories as having experienced an error.</summary>
		/// <param name="sds">A list of storage directories to mark as errored.</param>
		internal virtual void ReportErrorsOnDirectories(IList<Storage.StorageDirectory> sds
			)
		{
			foreach (Storage.StorageDirectory sd in sds)
			{
				ReportErrorsOnDirectory(sd);
			}
		}

		/// <summary>Reports that a directory has experienced an error.</summary>
		/// <remarks>
		/// Reports that a directory has experienced an error.
		/// Notifies listeners that the directory is no longer
		/// available.
		/// </remarks>
		/// <param name="sd">A storage directory to mark as errored.</param>
		private void ReportErrorsOnDirectory(Storage.StorageDirectory sd)
		{
			Log.Error("Error reported on storage directory " + sd);
			string lsd = ListStorageDirectories();
			Log.Debug("current list of storage dirs:" + lsd);
			Log.Warn("About to remove corresponding storage: " + sd.GetRoot().GetAbsolutePath
				());
			try
			{
				sd.Unlock();
			}
			catch (Exception e)
			{
				Log.Warn("Unable to unlock bad storage directory: " + sd.GetRoot().GetPath(), e);
			}
			if (this.storageDirs.Remove(sd))
			{
				this.removedStorageDirs.AddItem(sd);
			}
			lsd = ListStorageDirectories();
			Log.Debug("at the end current list of storage dirs:" + lsd);
		}

		/// <summary>
		/// Processes the startup options for the clusterid and blockpoolid
		/// for the upgrade.
		/// </summary>
		/// <param name="startOpt">Startup options</param>
		/// <param name="layoutVersion">Layout version for the upgrade</param>
		/// <exception cref="System.IO.IOException"/>
		internal virtual void ProcessStartupOptionsForUpgrade(HdfsServerConstants.StartupOption
			 startOpt, int layoutVersion)
		{
			if (startOpt == HdfsServerConstants.StartupOption.Upgrade || startOpt == HdfsServerConstants.StartupOption
				.Upgradeonly)
			{
				// If upgrade from a release that does not support federation,
				// if clusterId is provided in the startupOptions use it.
				// Else generate a new cluster ID      
				if (!NameNodeLayoutVersion.Supports(LayoutVersion.Feature.Federation, layoutVersion
					))
				{
					if (startOpt.GetClusterId() == null)
					{
						startOpt.SetClusterId(NewClusterID());
					}
					SetClusterID(startOpt.GetClusterId());
					SetBlockPoolID(NewBlockPoolID());
				}
				else
				{
					// Upgrade from one version of federation to another supported
					// version of federation doesn't require clusterID.
					// Warn the user if the current clusterid didn't match with the input
					// clusterid.
					if (startOpt.GetClusterId() != null && !startOpt.GetClusterId().Equals(GetClusterID
						()))
					{
						Log.Warn("Clusterid mismatch - current clusterid: " + GetClusterID() + ", Ignoring given clusterid: "
							 + startOpt.GetClusterId());
					}
				}
				Log.Info("Using clusterid: " + GetClusterID());
			}
		}

		/// <summary>
		/// Report that an IOE has occurred on some file which may
		/// or may not be within one of the NN image storage directories.
		/// </summary>
		public virtual void ReportErrorOnFile(FilePath f)
		{
			// We use getAbsolutePath here instead of getCanonicalPath since we know
			// that there is some IO problem on that drive.
			// getCanonicalPath may need to call stat() or readlink() and it's likely
			// those calls would fail due to the same underlying IO problem.
			string absPath = f.GetAbsolutePath();
			foreach (Storage.StorageDirectory sd in storageDirs)
			{
				string dirPath = sd.GetRoot().GetAbsolutePath();
				if (!dirPath.EndsWith(FilePath.separator))
				{
					dirPath += FilePath.separator;
				}
				if (absPath.StartsWith(dirPath))
				{
					ReportErrorsOnDirectory(sd);
					return;
				}
			}
		}

		/// <summary>Generate new clusterID.</summary>
		/// <remarks>
		/// Generate new clusterID.
		/// clusterID is a persistent attribute of the cluster.
		/// It is generated when the cluster is created and remains the same
		/// during the life cycle of the cluster.  When a new name node is formated, if
		/// this is a new cluster, a new clusterID is geneated and stored.  Subsequent
		/// name node must be given the same ClusterID during its format to be in the
		/// same cluster.
		/// When a datanode register it receive the clusterID and stick with it.
		/// If at any point, name node or data node tries to join another cluster, it
		/// will be rejected.
		/// </remarks>
		/// <returns>new clusterID</returns>
		public static string NewClusterID()
		{
			return "CID-" + UUID.RandomUUID().ToString();
		}

		internal virtual void SetClusterID(string cid)
		{
			clusterID = cid;
		}

		/// <summary>
		/// try to find current cluster id in the VERSION files
		/// returns first cluster id found in any VERSION file
		/// null in case none found
		/// </summary>
		/// <returns>clusterId or null in case no cluster id found</returns>
		public virtual string DetermineClusterId()
		{
			string cid = null;
			IEnumerator<Storage.StorageDirectory> sdit = DirIterator(NNStorage.NameNodeDirType
				.Image);
			while (sdit.HasNext())
			{
				Storage.StorageDirectory sd = sdit.Next();
				try
				{
					Properties props = ReadPropertiesFile(sd.GetVersionFile());
					cid = props.GetProperty("clusterID");
					Log.Info("current cluster id for sd=" + sd.GetCurrentDir() + ";lv=" + layoutVersion
						 + ";cid=" + cid);
					if (cid != null && !cid.Equals(string.Empty))
					{
						return cid;
					}
				}
				catch (Exception e)
				{
					Log.Warn("this sd not available: " + e.GetLocalizedMessage());
				}
			}
			//ignore
			Log.Warn("couldn't find any VERSION file containing valid ClusterId");
			return null;
		}

		/// <summary>Generate new blockpoolID.</summary>
		/// <returns>new blockpoolID</returns>
		/// <exception cref="Sharpen.UnknownHostException"/>
		internal static string NewBlockPoolID()
		{
			string ip = "unknownIP";
			try
			{
				ip = DNS.GetDefaultIP("default");
			}
			catch (UnknownHostException e)
			{
				Log.Warn("Could not find ip address of \"default\" inteface.");
				throw;
			}
			int rand = DFSUtil.GetSecureRandom().Next(int.MaxValue);
			string bpid = "BP-" + rand + "-" + ip + "-" + Time.Now();
			return bpid;
		}

		/// <summary>Validate and set block pool ID</summary>
		public virtual void SetBlockPoolID(string bpid)
		{
			blockpoolID = bpid;
		}

		/// <summary>Validate and set block pool ID</summary>
		/// <exception cref="Org.Apache.Hadoop.Hdfs.Server.Common.InconsistentFSStateException
		/// 	"/>
		private void SetBlockPoolID(FilePath storage, string bpid)
		{
			if (bpid == null || bpid.Equals(string.Empty))
			{
				throw new InconsistentFSStateException(storage, "file " + Storage.StorageFileVersion
					 + " has no block pool Id.");
			}
			if (!blockpoolID.Equals(string.Empty) && !blockpoolID.Equals(bpid))
			{
				throw new InconsistentFSStateException(storage, "Unexepcted blockpoolID " + bpid 
					+ " . Expected " + blockpoolID);
			}
			SetBlockPoolID(bpid);
		}

		public virtual string GetBlockPoolID()
		{
			return blockpoolID;
		}

		/// <summary>
		/// Iterate over all current storage directories, inspecting them
		/// with the given inspector.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		internal virtual void InspectStorageDirs(FSImageStorageInspector inspector)
		{
			// Process each of the storage directories to find the pair of
			// newest image file and edit file
			for (IEnumerator<Storage.StorageDirectory> it = DirIterator(); it.HasNext(); )
			{
				Storage.StorageDirectory sd = it.Next();
				inspector.InspectDirectory(sd);
			}
		}

		/// <summary>
		/// Iterate over all of the storage dirs, reading their contents to determine
		/// their layout versions.
		/// </summary>
		/// <remarks>
		/// Iterate over all of the storage dirs, reading their contents to determine
		/// their layout versions. Returns an FSImageStorageInspector which has
		/// inspected each directory.
		/// <b>Note:</b> this can mutate the storage info fields (ctime, version, etc).
		/// </remarks>
		/// <exception cref="System.IO.IOException">if no valid storage dirs are found or no valid layout version
		/// 	</exception>
		internal virtual FSImageStorageInspector ReadAndInspectDirs(EnumSet<NNStorage.NameNodeFile
			> fileTypes, HdfsServerConstants.StartupOption startupOption)
		{
			int layoutVersion = null;
			bool multipleLV = false;
			StringBuilder layoutVersions = new StringBuilder();
			// First determine what range of layout versions we're going to inspect
			for (IEnumerator<Storage.StorageDirectory> it = DirIterator(false); it.HasNext(); )
			{
				Storage.StorageDirectory sd = it.Next();
				if (!sd.GetVersionFile().Exists())
				{
					FSImage.Log.Warn("Storage directory " + sd + " contains no VERSION file. Skipping..."
						);
					continue;
				}
				ReadProperties(sd, startupOption);
				// sets layoutVersion
				int lv = GetLayoutVersion();
				if (layoutVersion == null)
				{
					layoutVersion = Sharpen.Extensions.ValueOf(lv);
				}
				else
				{
					if (!layoutVersion.Equals(lv))
					{
						multipleLV = true;
					}
				}
				layoutVersions.Append("(").Append(sd.GetRoot()).Append(", ").Append(lv).Append(") "
					);
			}
			if (layoutVersion == null)
			{
				throw new IOException("No storage directories contained VERSION information");
			}
			if (multipleLV)
			{
				throw new IOException("Storage directories contain multiple layout versions: " + 
					layoutVersions);
			}
			// If the storage directories are with the new layout version
			// (ie edits_<txnid>) then use the new inspector, which will ignore
			// the old format dirs.
			FSImageStorageInspector inspector;
			if (NameNodeLayoutVersion.Supports(LayoutVersion.Feature.TxidBasedLayout, GetLayoutVersion
				()))
			{
				inspector = new FSImageTransactionalStorageInspector(fileTypes);
			}
			else
			{
				inspector = new FSImagePreTransactionalStorageInspector();
			}
			InspectStorageDirs(inspector);
			return inspector;
		}

		public virtual NamespaceInfo GetNamespaceInfo()
		{
			return new NamespaceInfo(GetNamespaceID(), GetClusterID(), GetBlockPoolID(), GetCTime
				());
		}
	}
}
