using System;
using System.Collections.Generic;
using System.IO;
using System.Security;
using System.Text;
using Com.Google.Common.Base;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO.Nativeio;
using Org.Apache.Hadoop.Util;
using Sharpen;
using Sharpen.Management;

namespace Org.Apache.Hadoop.Hdfs.Server.Common
{
	/// <summary>Storage information file.</summary>
	/// <remarks>
	/// Storage information file.
	/// <p>
	/// Local storage information is stored in a separate file VERSION.
	/// It contains type of the node,
	/// the storage layout version, the namespace id, and
	/// the fs state creation time.
	/// <p>
	/// Local storage can reside in multiple directories.
	/// Each directory should contain the same VERSION file as the others.
	/// During startup Hadoop servers (name-node and data-nodes) read their local
	/// storage information from them.
	/// <p>
	/// The servers hold a lock for each storage directory while they run so that
	/// other nodes were not able to startup sharing the same storage.
	/// The locks are released when the servers stop (normally or abnormally).
	/// </remarks>
	public abstract class Storage : StorageInfo
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Hdfs.Server.Common.Storage
			).FullName);

		public const int LastPreUpgradeLayoutVersion = -3;

		public const int LastUpgradableLayoutVersion = -16;

		protected internal const string LastUpgradableHadoopVersion = "Hadoop-0.18";

		/// <summary>Layout versions of 0.20.203 release</summary>
		public static readonly int[] LayoutVersions203 = new int[] { -19, -31 };

		public const string StorageFileLock = "in_use.lock";

		public const string StorageDirCurrent = "current";

		public const string StorageDirPrevious = "previous";

		public const string StorageTmpRemoved = "removed.tmp";

		public const string StorageTmpPrevious = "previous.tmp";

		public const string StorageTmpFinalized = "finalized.tmp";

		public const string StorageTmpLastCkpt = "lastcheckpoint.tmp";

		public const string StoragePreviousCkpt = "previous.checkpoint";

		/// <summary>
		/// The blocksBeingWritten directory which was used in some 1.x and earlier
		/// releases.
		/// </summary>
		public const string Storage1Bbw = "blocksBeingWritten";

		public enum StorageState
		{
			NonExistent,
			NotFormatted,
			CompleteUpgrade,
			RecoverUpgrade,
			CompleteFinalize,
			CompleteRollback,
			RecoverRollback,
			CompleteCheckpoint,
			RecoverCheckpoint,
			Normal
		}

		/// <summary>
		/// An interface to denote storage directory type
		/// Implementations can define a type for storage directory by implementing
		/// this interface.
		/// </summary>
		public interface StorageDirType
		{
			// last layout version that did not support upgrades
			// this corresponds to Hadoop-0.18
			Storage.StorageDirType GetStorageDirType();

			bool IsOfType(Storage.StorageDirType type);
		}

		protected internal IList<Storage.StorageDirectory> storageDirs = new AList<Storage.StorageDirectory
			>();

		private class DirIterator : IEnumerator<Storage.StorageDirectory>
		{
			internal readonly Storage.StorageDirType dirType;

			internal readonly bool includeShared;

			internal int prevIndex;

			internal int nextIndex;

			internal DirIterator(Storage _enclosing, Storage.StorageDirType dirType, bool includeShared
				)
			{
				this._enclosing = _enclosing;
				// for remove()
				// for next()
				this.dirType = dirType;
				this.nextIndex = 0;
				this.prevIndex = 0;
				this.includeShared = includeShared;
			}

			public override bool HasNext()
			{
				if (this._enclosing.storageDirs.IsEmpty() || this.nextIndex >= this._enclosing.storageDirs
					.Count)
				{
					return false;
				}
				if (this.dirType != null || !this.includeShared)
				{
					while (this.nextIndex < this._enclosing.storageDirs.Count)
					{
						if (this.ShouldReturnNextDir())
						{
							break;
						}
						this.nextIndex++;
					}
					if (this.nextIndex >= this._enclosing.storageDirs.Count)
					{
						return false;
					}
				}
				return true;
			}

			public override Storage.StorageDirectory Next()
			{
				Storage.StorageDirectory sd = this._enclosing.GetStorageDir(this.nextIndex);
				this.prevIndex = this.nextIndex;
				this.nextIndex++;
				if (this.dirType != null || !this.includeShared)
				{
					while (this.nextIndex < this._enclosing.storageDirs.Count)
					{
						if (this.ShouldReturnNextDir())
						{
							break;
						}
						this.nextIndex++;
					}
				}
				return sd;
			}

			public override void Remove()
			{
				this.nextIndex = this.prevIndex;
				// restore previous state
				this._enclosing.storageDirs.Remove(this.prevIndex);
				// remove last returned element
				this.HasNext();
			}

			// reset nextIndex to correct place
			private bool ShouldReturnNextDir()
			{
				Storage.StorageDirectory sd = this._enclosing.GetStorageDir(this.nextIndex);
				return (this.dirType == null || sd.GetStorageDirType().IsOfType(this.dirType)) &&
					 (this.includeShared || !sd.IsShared());
			}

			private readonly Storage _enclosing;
		}

		/// <returns>
		/// A list of the given File in every available storage directory,
		/// regardless of whether it might exist.
		/// </returns>
		public virtual IList<FilePath> GetFiles(Storage.StorageDirType dirType, string fileName
			)
		{
			AList<FilePath> list = new AList<FilePath>();
			IEnumerator<Storage.StorageDirectory> it = (dirType == null) ? DirIterator() : DirIterator
				(dirType);
			for (; it.HasNext(); )
			{
				list.AddItem(new FilePath(it.Next().GetCurrentDir(), fileName));
			}
			return list;
		}

		/// <summary>
		/// Return default iterator
		/// This iterator returns all entries in storageDirs
		/// </summary>
		public virtual IEnumerator<Storage.StorageDirectory> DirIterator()
		{
			return DirIterator(null);
		}

		/// <summary>
		/// Return iterator based on Storage Directory Type
		/// This iterator selects entries in storageDirs of type dirType and returns
		/// them via the Iterator
		/// </summary>
		public virtual IEnumerator<Storage.StorageDirectory> DirIterator(Storage.StorageDirType
			 dirType)
		{
			return DirIterator(dirType, true);
		}

		/// <summary>Return all entries in storageDirs, potentially excluding shared dirs.</summary>
		/// <param name="includeShared">whether or not to include shared dirs.</param>
		/// <returns>an iterator over the configured storage dirs.</returns>
		public virtual IEnumerator<Storage.StorageDirectory> DirIterator(bool includeShared
			)
		{
			return DirIterator(null, includeShared);
		}

		/// <param name="dirType">all entries will be of this type of dir</param>
		/// <param name="includeShared">
		/// true to include any shared directories,
		/// false otherwise
		/// </param>
		/// <returns>an iterator over the configured storage dirs.</returns>
		public virtual IEnumerator<Storage.StorageDirectory> DirIterator(Storage.StorageDirType
			 dirType, bool includeShared)
		{
			return new Storage.DirIterator(this, dirType, includeShared);
		}

		public virtual IEnumerable<Storage.StorageDirectory> DirIterable(Storage.StorageDirType
			 dirType)
		{
			return new _IEnumerable_234(this, dirType);
		}

		private sealed class _IEnumerable_234 : IEnumerable<Storage.StorageDirectory>
		{
			public _IEnumerable_234(Storage _enclosing, Storage.StorageDirType dirType)
			{
				this._enclosing = _enclosing;
				this.dirType = dirType;
			}

			public override IEnumerator<Storage.StorageDirectory> GetEnumerator()
			{
				return this._enclosing.DirIterator(dirType);
			}

			private readonly Storage _enclosing;

			private readonly Storage.StorageDirType dirType;
		}

		/// <summary>generate storage list (debug line)</summary>
		public virtual string ListStorageDirectories()
		{
			StringBuilder buf = new StringBuilder();
			foreach (Storage.StorageDirectory sd in storageDirs)
			{
				buf.Append(sd.GetRoot() + "(" + sd.GetStorageDirType() + ");");
			}
			return buf.ToString();
		}

		/// <summary>One of the storage directories.</summary>
		public class StorageDirectory : Storage.FormatConfirmable
		{
			internal readonly FilePath root;

			internal readonly bool isShared;

			internal readonly Storage.StorageDirType dirType;

			internal FileLock Lock;

			private string storageUuid = null;

			public StorageDirectory(FilePath dir)
				: this(dir, null, false)
			{
			}

			public StorageDirectory(FilePath dir, Storage.StorageDirType dirType)
				: this(dir, dirType, false)
			{
			}

			// root directory
			// whether or not this dir is shared between two separate NNs for HA, or
			// between multiple block pools in the case of federation.
			// storage dir type
			// storage lock
			// Storage directory identifier.
			// default dirType is null
			public virtual void SetStorageUuid(string storageUuid)
			{
				this.storageUuid = storageUuid;
			}

			public virtual string GetStorageUuid()
			{
				return storageUuid;
			}

			/// <summary>Constructor</summary>
			/// <param name="dir">directory corresponding to the storage</param>
			/// <param name="dirType">storage directory type</param>
			/// <param name="isShared">
			/// whether or not this dir is shared between two NNs. true
			/// disables locking on the storage directory, false enables locking
			/// </param>
			public StorageDirectory(FilePath dir, Storage.StorageDirType dirType, bool isShared
				)
			{
				this.root = dir;
				this.Lock = null;
				this.dirType = dirType;
				this.isShared = isShared;
			}

			/// <summary>Get root directory of this storage</summary>
			public virtual FilePath GetRoot()
			{
				return root;
			}

			/// <summary>Get storage directory type</summary>
			public virtual Storage.StorageDirType GetStorageDirType()
			{
				return dirType;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Read(FilePath from, Storage storage)
			{
				Properties props = ReadPropertiesFile(from);
				storage.SetFieldsFromProperties(props, this);
			}

			/// <summary>Clear and re-create storage directory.</summary>
			/// <remarks>
			/// Clear and re-create storage directory.
			/// <p>
			/// Removes contents of the current directory and creates an empty directory.
			/// This does not fully format storage directory.
			/// It cannot write the version file since it should be written last after
			/// all other storage type dependent files are written.
			/// Derived storage is responsible for setting specific storage values and
			/// writing the version file to disk.
			/// </remarks>
			/// <exception cref="System.IO.IOException"/>
			public virtual void ClearDirectory()
			{
				FilePath curDir = this.GetCurrentDir();
				if (curDir.Exists())
				{
					if (!(FileUtil.FullyDelete(curDir)))
					{
						throw new IOException("Cannot remove current directory: " + curDir);
					}
				}
				if (!curDir.Mkdirs())
				{
					throw new IOException("Cannot create directory " + curDir);
				}
			}

			/// <summary>
			/// Directory
			/// <c>current</c>
			/// contains latest files defining
			/// the file system meta-data.
			/// </summary>
			/// <returns>the directory path</returns>
			public virtual FilePath GetCurrentDir()
			{
				return new FilePath(root, StorageDirCurrent);
			}

			/// <summary>
			/// File
			/// <c>VERSION</c>
			/// contains the following fields:
			/// <ol>
			/// <li>node type</li>
			/// <li>layout version</li>
			/// <li>namespaceID</li>
			/// <li>fs state creation time</li>
			/// <li>other fields specific for this node type</li>
			/// </ol>
			/// The version file is always written last during storage directory updates.
			/// The existence of the version file indicates that all other files have
			/// been successfully written in the storage directory, the storage is valid
			/// and does not need to be recovered.
			/// </summary>
			/// <returns>the version file path</returns>
			public virtual FilePath GetVersionFile()
			{
				return new FilePath(new FilePath(root, StorageDirCurrent), StorageFileVersion);
			}

			/// <summary>
			/// File
			/// <c>VERSION</c>
			/// from the
			/// <c>previous</c>
			/// directory.
			/// </summary>
			/// <returns>the previous version file path</returns>
			public virtual FilePath GetPreviousVersionFile()
			{
				return new FilePath(new FilePath(root, StorageDirPrevious), StorageFileVersion);
			}

			/// <summary>
			/// Directory
			/// <c>previous</c>
			/// contains the previous file system state,
			/// which the system can be rolled back to.
			/// </summary>
			/// <returns>the directory path</returns>
			public virtual FilePath GetPreviousDir()
			{
				return new FilePath(root, StorageDirPrevious);
			}

			/// <summary>
			/// <c>previous.tmp</c>
			/// is a transient directory, which holds
			/// current file system state while the new state is saved into the new
			/// <c>current</c>
			/// during upgrade.
			/// If the saving succeeds
			/// <c>previous.tmp</c>
			/// will be moved to
			/// <c>previous</c>
			/// , otherwise it will be renamed back to
			/// <c>current</c>
			/// by the recovery procedure during startup.
			/// </summary>
			/// <returns>the directory path</returns>
			public virtual FilePath GetPreviousTmp()
			{
				return new FilePath(root, StorageTmpPrevious);
			}

			/// <summary>
			/// <c>removed.tmp</c>
			/// is a transient directory, which holds
			/// current file system state while the previous state is moved into
			/// <c>current</c>
			/// during rollback.
			/// If the moving succeeds
			/// <c>removed.tmp</c>
			/// will be removed,
			/// otherwise it will be renamed back to
			/// <c>current</c>
			/// by the recovery procedure during startup.
			/// </summary>
			/// <returns>the directory path</returns>
			public virtual FilePath GetRemovedTmp()
			{
				return new FilePath(root, StorageTmpRemoved);
			}

			/// <summary>
			/// <c>finalized.tmp</c>
			/// is a transient directory, which holds
			/// the
			/// <c>previous</c>
			/// file system state while it is being removed
			/// in response to the finalize request.
			/// Finalize operation will remove
			/// <c>finalized.tmp</c>
			/// when completed,
			/// otherwise the removal will resume upon the system startup.
			/// </summary>
			/// <returns>the directory path</returns>
			public virtual FilePath GetFinalizedTmp()
			{
				return new FilePath(root, StorageTmpFinalized);
			}

			/// <summary>
			/// <c>lastcheckpoint.tmp</c>
			/// is a transient directory, which holds
			/// current file system state while the new state is saved into the new
			/// <c>current</c>
			/// during regular namespace updates.
			/// If the saving succeeds
			/// <c>lastcheckpoint.tmp</c>
			/// will be moved to
			/// <c>previous.checkpoint</c>
			/// , otherwise it will be renamed back to
			/// <c>current</c>
			/// by the recovery procedure during startup.
			/// </summary>
			/// <returns>the directory path</returns>
			public virtual FilePath GetLastCheckpointTmp()
			{
				return new FilePath(root, StorageTmpLastCkpt);
			}

			/// <summary>
			/// <c>previous.checkpoint</c>
			/// is a directory, which holds the previous
			/// (before the last save) state of the storage directory.
			/// The directory is created as a reference only, it does not play role
			/// in state recovery procedures, and is recycled automatically,
			/// but it may be useful for manual recovery of a stale state of the system.
			/// </summary>
			/// <returns>the directory path</returns>
			public virtual FilePath GetPreviousCheckpoint()
			{
				return new FilePath(root, StoragePreviousCkpt);
			}

			/// <summary>Check consistency of the storage directory</summary>
			/// <param name="startOpt">a startup option.</param>
			/// <returns>
			/// state
			/// <see cref="StorageState"/>
			/// of the storage directory
			/// </returns>
			/// <exception cref="InconsistentFSStateException">
			/// if directory state is not
			/// consistent and cannot be recovered.
			/// </exception>
			/// <exception cref="System.IO.IOException"/>
			public virtual Storage.StorageState AnalyzeStorage(HdfsServerConstants.StartupOption
				 startOpt, Storage storage)
			{
				System.Diagnostics.Debug.Assert(root != null, "root is null");
				bool hadMkdirs = false;
				string rootPath = root.GetCanonicalPath();
				try
				{
					// check that storage exists
					if (!root.Exists())
					{
						// storage directory does not exist
						if (startOpt != HdfsServerConstants.StartupOption.Format && startOpt != HdfsServerConstants.StartupOption
							.Hotswap)
						{
							Log.Warn("Storage directory " + rootPath + " does not exist");
							return Storage.StorageState.NonExistent;
						}
						Log.Info(rootPath + " does not exist. Creating ...");
						if (!root.Mkdirs())
						{
							throw new IOException("Cannot create directory " + rootPath);
						}
						hadMkdirs = true;
					}
					// or is inaccessible
					if (!root.IsDirectory())
					{
						Log.Warn(rootPath + "is not a directory");
						return Storage.StorageState.NonExistent;
					}
					if (!FileUtil.CanWrite(root))
					{
						Log.Warn("Cannot access storage directory " + rootPath);
						return Storage.StorageState.NonExistent;
					}
				}
				catch (SecurityException ex)
				{
					Log.Warn("Cannot access storage directory " + rootPath, ex);
					return Storage.StorageState.NonExistent;
				}
				this.Lock();
				// lock storage if it exists
				// If startOpt is HOTSWAP, it returns NOT_FORMATTED for empty directory,
				// while it also checks the layout version.
				if (startOpt == HdfsServerConstants.StartupOption.Format || (startOpt == HdfsServerConstants.StartupOption
					.Hotswap && hadMkdirs))
				{
					return Storage.StorageState.NotFormatted;
				}
				if (startOpt != HdfsServerConstants.StartupOption.Import)
				{
					storage.CheckOldLayoutStorage(this);
				}
				// check whether current directory is valid
				FilePath versionFile = GetVersionFile();
				bool hasCurrent = versionFile.Exists();
				// check which directories exist
				bool hasPrevious = GetPreviousDir().Exists();
				bool hasPreviousTmp = GetPreviousTmp().Exists();
				bool hasRemovedTmp = GetRemovedTmp().Exists();
				bool hasFinalizedTmp = GetFinalizedTmp().Exists();
				bool hasCheckpointTmp = GetLastCheckpointTmp().Exists();
				if (!(hasPreviousTmp || hasRemovedTmp || hasFinalizedTmp || hasCheckpointTmp))
				{
					// no temp dirs - no recovery
					if (hasCurrent)
					{
						return Storage.StorageState.Normal;
					}
					if (hasPrevious)
					{
						throw new InconsistentFSStateException(root, "version file in current directory is missing."
							);
					}
					return Storage.StorageState.NotFormatted;
				}
				if ((hasPreviousTmp ? 1 : 0) + (hasRemovedTmp ? 1 : 0) + (hasFinalizedTmp ? 1 : 0
					) + (hasCheckpointTmp ? 1 : 0) > 1)
				{
					// more than one temp dirs
					throw new InconsistentFSStateException(root, "too many temporary directories.");
				}
				// # of temp dirs == 1 should either recover or complete a transition
				if (hasCheckpointTmp)
				{
					return hasCurrent ? Storage.StorageState.CompleteCheckpoint : Storage.StorageState
						.RecoverCheckpoint;
				}
				if (hasFinalizedTmp)
				{
					if (hasPrevious)
					{
						throw new InconsistentFSStateException(root, StorageDirPrevious + " and " + StorageTmpFinalized
							 + "cannot exist together.");
					}
					return Storage.StorageState.CompleteFinalize;
				}
				if (hasPreviousTmp)
				{
					if (hasPrevious)
					{
						throw new InconsistentFSStateException(root, StorageDirPrevious + " and " + StorageTmpPrevious
							 + " cannot exist together.");
					}
					if (hasCurrent)
					{
						return Storage.StorageState.CompleteUpgrade;
					}
					return Storage.StorageState.RecoverUpgrade;
				}
				System.Diagnostics.Debug.Assert(hasRemovedTmp, "hasRemovedTmp must be true");
				if (!(hasCurrent ^ hasPrevious))
				{
					throw new InconsistentFSStateException(root, "one and only one directory " + StorageDirCurrent
						 + " or " + StorageDirPrevious + " must be present when " + StorageTmpRemoved + 
						" exists.");
				}
				if (hasCurrent)
				{
					return Storage.StorageState.CompleteRollback;
				}
				return Storage.StorageState.RecoverRollback;
			}

			/// <summary>Complete or recover storage state from previously failed transition.</summary>
			/// <param name="curState">specifies what/how the state should be recovered</param>
			/// <exception cref="System.IO.IOException"/>
			public virtual void DoRecover(Storage.StorageState curState)
			{
				FilePath curDir = GetCurrentDir();
				string rootPath = root.GetCanonicalPath();
				switch (curState)
				{
					case Storage.StorageState.CompleteUpgrade:
					{
						// mv previous.tmp -> previous
						Log.Info("Completing previous upgrade for storage directory " + rootPath);
						Rename(GetPreviousTmp(), GetPreviousDir());
						return;
					}

					case Storage.StorageState.RecoverUpgrade:
					{
						// mv previous.tmp -> current
						Log.Info("Recovering storage directory " + rootPath + " from previous upgrade");
						if (curDir.Exists())
						{
							DeleteDir(curDir);
						}
						Rename(GetPreviousTmp(), curDir);
						return;
					}

					case Storage.StorageState.CompleteRollback:
					{
						// rm removed.tmp
						Log.Info("Completing previous rollback for storage directory " + rootPath);
						DeleteDir(GetRemovedTmp());
						return;
					}

					case Storage.StorageState.RecoverRollback:
					{
						// mv removed.tmp -> current
						Log.Info("Recovering storage directory " + rootPath + " from previous rollback");
						Rename(GetRemovedTmp(), curDir);
						return;
					}

					case Storage.StorageState.CompleteFinalize:
					{
						// rm finalized.tmp
						Log.Info("Completing previous finalize for storage directory " + rootPath);
						DeleteDir(GetFinalizedTmp());
						return;
					}

					case Storage.StorageState.CompleteCheckpoint:
					{
						// mv lastcheckpoint.tmp -> previous.checkpoint
						Log.Info("Completing previous checkpoint for storage directory " + rootPath);
						FilePath prevCkptDir = GetPreviousCheckpoint();
						if (prevCkptDir.Exists())
						{
							DeleteDir(prevCkptDir);
						}
						Rename(GetLastCheckpointTmp(), prevCkptDir);
						return;
					}

					case Storage.StorageState.RecoverCheckpoint:
					{
						// mv lastcheckpoint.tmp -> current
						Log.Info("Recovering storage directory " + rootPath + " from failed checkpoint");
						if (curDir.Exists())
						{
							DeleteDir(curDir);
						}
						Rename(GetLastCheckpointTmp(), curDir);
						return;
					}

					default:
					{
						throw new IOException("Unexpected FS state: " + curState);
					}
				}
			}

			/// <returns>
			/// true if the storage directory should prompt the user prior
			/// to formatting (i.e if the directory appears to contain some data)
			/// </returns>
			/// <exception cref="System.IO.IOException">if the SD cannot be accessed due to an IO error
			/// 	</exception>
			public virtual bool HasSomeData()
			{
				// Its alright for a dir not to exist, or to exist (properly accessible)
				// and be completely empty.
				if (!root.Exists())
				{
					return false;
				}
				if (!root.IsDirectory())
				{
					// a file where you expect a directory should not cause silent
					// formatting
					return true;
				}
				if (FileUtil.ListFiles(root).Length == 0)
				{
					// Empty dir can format without prompt.
					return false;
				}
				return true;
			}

			public virtual bool IsShared()
			{
				return isShared;
			}

			/// <summary>Lock storage to provide exclusive access.</summary>
			/// <remarks>
			/// Lock storage to provide exclusive access.
			/// <p> Locking is not supported by all file systems.
			/// E.g., NFS does not consistently support exclusive locks.
			/// <p> If locking is supported we guarantee exclusive access to the
			/// storage directory. Otherwise, no guarantee is given.
			/// </remarks>
			/// <exception cref="System.IO.IOException">if locking fails</exception>
			public virtual void Lock()
			{
				if (IsShared())
				{
					Log.Info("Locking is disabled for " + this.root);
					return;
				}
				FileLock newLock = TryLock();
				if (newLock == null)
				{
					string msg = "Cannot lock storage " + this.root + ". The directory is already locked";
					Log.Info(msg);
					throw new IOException(msg);
				}
				// Don't overwrite lock until success - this way if we accidentally
				// call lock twice, the internal state won't be cleared by the second
				// (failed) lock attempt
				Lock = newLock;
			}

			/// <summary>Attempts to acquire an exclusive lock on the storage.</summary>
			/// <returns>
			/// A lock object representing the newly-acquired lock or
			/// <code>null</code> if storage is already locked.
			/// </returns>
			/// <exception cref="System.IO.IOException">if locking fails.</exception>
			internal virtual FileLock TryLock()
			{
				bool deletionHookAdded = false;
				FilePath lockF = new FilePath(root, StorageFileLock);
				if (!lockF.Exists())
				{
					lockF.DeleteOnExit();
					deletionHookAdded = true;
				}
				RandomAccessFile file = new RandomAccessFile(lockF, "rws");
				string jvmName = ManagementFactory.GetRuntimeMXBean().GetName();
				FileLock res = null;
				try
				{
					res = file.GetChannel().TryLock();
					if (null == res)
					{
						throw new OverlappingFileLockException();
					}
					file.Write(Sharpen.Runtime.GetBytesForString(jvmName, Charsets.Utf8));
					Log.Info("Lock on " + lockF + " acquired by nodename " + jvmName);
				}
				catch (OverlappingFileLockException oe)
				{
					// Cannot read from the locked file on Windows.
					string lockingJvmName = Path.Windows ? string.Empty : (" " + file.ReadLine());
					Log.Error("It appears that another node " + lockingJvmName + " has already locked the storage directory: "
						 + root, oe);
					file.Close();
					return null;
				}
				catch (IOException e)
				{
					Log.Error("Failed to acquire lock on " + lockF + ". If this storage directory is mounted via NFS, "
						 + "ensure that the appropriate nfs lock services are running.", e);
					file.Close();
					throw;
				}
				if (!deletionHookAdded)
				{
					// If the file existed prior to our startup, we didn't
					// call deleteOnExit above. But since we successfully locked
					// the dir, we can take care of cleaning it up.
					lockF.DeleteOnExit();
				}
				return res;
			}

			/// <summary>Unlock storage.</summary>
			/// <exception cref="System.IO.IOException"/>
			public virtual void Unlock()
			{
				if (this.Lock == null)
				{
					return;
				}
				this.Lock.Release();
				Lock.Channel().Close();
				Lock = null;
			}

			public override string ToString()
			{
				return "Storage Directory " + this.root;
			}

			/// <summary>Check whether underlying file system supports file locking.</summary>
			/// <returns>
			/// <code>true</code> if exclusive locks are supported or
			/// <code>false</code> otherwise.
			/// </returns>
			/// <exception cref="System.IO.IOException"/>
			/// <seealso cref="Lock()"/>
			public virtual bool IsLockSupported()
			{
				FileLock firstLock = null;
				FileLock secondLock = null;
				try
				{
					firstLock = Lock;
					if (firstLock == null)
					{
						firstLock = TryLock();
						if (firstLock == null)
						{
							return true;
						}
					}
					secondLock = TryLock();
					if (secondLock == null)
					{
						return true;
					}
				}
				finally
				{
					if (firstLock != null && firstLock != Lock)
					{
						firstLock.Release();
						firstLock.Channel().Close();
					}
					if (secondLock != null)
					{
						secondLock.Release();
						secondLock.Channel().Close();
					}
				}
				return false;
			}
		}

		/// <summary>Create empty storage info of the specified type</summary>
		protected internal Storage(HdfsServerConstants.NodeType type)
			: base(type)
		{
		}

		protected internal Storage(StorageInfo storageInfo)
			: base(storageInfo)
		{
		}

		public virtual int GetNumStorageDirs()
		{
			return storageDirs.Count;
		}

		public virtual Storage.StorageDirectory GetStorageDir(int idx)
		{
			return storageDirs[idx];
		}

		/// <returns>
		/// the storage directory, with the precondition that this storage
		/// has exactly one storage directory
		/// </returns>
		public virtual Storage.StorageDirectory GetSingularStorageDir()
		{
			Preconditions.CheckState(storageDirs.Count == 1);
			return storageDirs[0];
		}

		protected internal virtual void AddStorageDir(Storage.StorageDirectory sd)
		{
			storageDirs.AddItem(sd);
		}

		/// <summary>
		/// Returns true if the storage directory on the given directory is already
		/// loaded.
		/// </summary>
		/// <param name="root">
		/// the root directory of a
		/// <see cref="StorageDirectory"/>
		/// </param>
		/// <exception cref="System.IO.IOException">if failed to get canonical path.</exception>
		protected internal virtual bool ContainsStorageDir(FilePath root)
		{
			foreach (Storage.StorageDirectory sd in storageDirs)
			{
				if (sd.GetRoot().GetCanonicalPath().Equals(root.GetCanonicalPath()))
				{
					return true;
				}
			}
			return false;
		}

		/// <summary>
		/// Return true if the layout of the given storage directory is from a version
		/// of Hadoop prior to the introduction of the "current" and "previous"
		/// directories which allow upgrade and rollback.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public abstract bool IsPreUpgradableLayout(Storage.StorageDirectory sd);

		/// <summary>
		/// Check if the given storage directory comes from a version of Hadoop
		/// prior to when the directory layout changed (ie 0.13).
		/// </summary>
		/// <remarks>
		/// Check if the given storage directory comes from a version of Hadoop
		/// prior to when the directory layout changed (ie 0.13). If this is
		/// the case, this method throws an IOException.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		private void CheckOldLayoutStorage(Storage.StorageDirectory sd)
		{
			if (IsPreUpgradableLayout(sd))
			{
				CheckVersionUpgradable(0);
			}
		}

		/// <summary>
		/// Checks if the upgrade from
		/// <paramref name="oldVersion"/>
		/// is supported.
		/// </summary>
		/// <param name="oldVersion">
		/// the version of the metadata to check with the current
		/// version
		/// </param>
		/// <exception cref="System.IO.IOException">if upgrade is not supported</exception>
		public static void CheckVersionUpgradable(int oldVersion)
		{
			if (oldVersion > LastUpgradableLayoutVersion)
			{
				string msg = "*********** Upgrade is not supported from this " + " older version "
					 + oldVersion + " of storage to the current version." + " Please upgrade to " + 
					LastUpgradableHadoopVersion + " or a later version and then upgrade to current" 
					+ " version. Old layout version is " + (oldVersion == 0 ? "'too old'" : (string.Empty
					 + oldVersion)) + " and latest layout version this software version can" + " upgrade from is "
					 + LastUpgradableLayoutVersion + ". ************";
				Log.Error(msg);
				throw new IOException(msg);
			}
		}

		/// <summary>
		/// Iterate over each of the
		/// <see cref="FormatConfirmable"/>
		/// objects,
		/// potentially checking with the user whether it should be formatted.
		/// If running in interactive mode, will prompt the user for each
		/// directory to allow them to format anyway. Otherwise, returns
		/// false, unless 'force' is specified.
		/// </summary>
		/// <param name="force">format regardless of whether dirs exist</param>
		/// <param name="interactive">prompt the user when a dir exists</param>
		/// <returns>true if formatting should proceed</returns>
		/// <exception cref="System.IO.IOException">if some storage cannot be accessed</exception>
		public static bool ConfirmFormat<_T0>(IEnumerable<_T0> items, bool force, bool interactive
			)
			where _T0 : Storage.FormatConfirmable
		{
			foreach (Storage.FormatConfirmable item in items)
			{
				if (!item.HasSomeData())
				{
					continue;
				}
				if (force)
				{
					// Don't confirm, always format.
					System.Console.Error.WriteLine("Data exists in " + item + ". Formatting anyway.");
					continue;
				}
				if (!interactive)
				{
					// Don't ask - always don't format
					System.Console.Error.WriteLine("Running in non-interactive mode, and data appears to exist in "
						 + item + ". Not formatting.");
					return false;
				}
				if (!ToolRunner.ConfirmPrompt("Re-format filesystem in " + item + " ?"))
				{
					System.Console.Error.WriteLine("Format aborted in " + item);
					return false;
				}
			}
			return true;
		}

		/// <summary>
		/// Interface for classes which need to have the user confirm their
		/// formatting during NameNode -format and other similar operations.
		/// </summary>
		/// <remarks>
		/// Interface for classes which need to have the user confirm their
		/// formatting during NameNode -format and other similar operations.
		/// This is currently a storage directory or journal manager.
		/// </remarks>
		public interface FormatConfirmable
		{
			/// <returns>
			/// true if the storage seems to have some valid data in it,
			/// and the user should be required to confirm the format. Otherwise,
			/// false.
			/// </returns>
			/// <exception cref="System.IO.IOException">if the storage cannot be accessed at all.
			/// 	</exception>
			bool HasSomeData();

			/// <returns>
			/// a string representation of the formattable item, suitable
			/// for display to the user inside a prompt
			/// </returns>
			string ToString();
		}

		/// <summary>Set common storage fields into the given properties object.</summary>
		/// <remarks>
		/// Set common storage fields into the given properties object.
		/// Should be overloaded if additional fields need to be set.
		/// </remarks>
		/// <param name="props">the Properties object to write into</param>
		/// <exception cref="System.IO.IOException"/>
		protected internal virtual void SetPropertiesFromFields(Properties props, Storage.StorageDirectory
			 sd)
		{
			props.SetProperty("layoutVersion", layoutVersion.ToString());
			props.SetProperty("storageType", storageType.ToString());
			props.SetProperty("namespaceID", namespaceID.ToString());
			// Set clusterID in version with federation support
			if (VersionSupportsFederation(GetServiceLayoutFeatureMap()))
			{
				props.SetProperty("clusterID", clusterID);
			}
			props.SetProperty("cTime", cTime.ToString());
		}

		/// <summary>Write properties to the VERSION file in the given storage directory.</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void WriteProperties(Storage.StorageDirectory sd)
		{
			WriteProperties(sd.GetVersionFile(), sd);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void WriteProperties(FilePath to, Storage.StorageDirectory sd)
		{
			Properties props = new Properties();
			SetPropertiesFromFields(props, sd);
			WriteProperties(to, sd, props);
		}

		/// <exception cref="System.IO.IOException"/>
		public static void WriteProperties(FilePath to, Storage.StorageDirectory sd, Properties
			 props)
		{
			RandomAccessFile file = new RandomAccessFile(to, "rws");
			FileOutputStream @out = null;
			try
			{
				file.Seek(0);
				@out = new FileOutputStream(file.GetFD());
				/*
				* If server is interrupted before this line,
				* the version file will remain unchanged.
				*/
				props.Store(@out, null);
				/*
				* Now the new fields are flushed to the head of the file, but file
				* length can still be larger then required and therefore the file can
				* contain whole or corrupted fields from its old contents in the end.
				* If server is interrupted here and restarted later these extra fields
				* either should not effect server behavior or should be handled
				* by the server correctly.
				*/
				file.SetLength(@out.GetChannel().Position());
			}
			finally
			{
				if (@out != null)
				{
					@out.Close();
				}
				file.Close();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public static void Rename(FilePath from, FilePath to)
		{
			try
			{
				NativeIO.RenameTo(from, to);
			}
			catch (NativeIOException e)
			{
				throw new IOException("Failed to rename " + from.GetCanonicalPath() + " to " + to
					.GetCanonicalPath() + " due to failure in native rename. " + e.ToString());
			}
		}

		/// <summary>Copies a file (usually large) to a new location using native unbuffered IO.
		/// 	</summary>
		/// <remarks>
		/// Copies a file (usually large) to a new location using native unbuffered IO.
		/// <p>
		/// This method copies the contents of the specified source file
		/// to the specified destination file using OS specific unbuffered IO.
		/// The goal is to avoid churning the file system buffer cache when copying
		/// large files.
		/// We can't use FileUtils#copyFile from apache-commons-io because it
		/// is a buffered IO based on FileChannel#transferFrom, which uses MmapByteBuffer
		/// internally.
		/// The directory holding the destination file is created if it does not exist.
		/// If the destination file exists, then this method will delete it first.
		/// <p>
		/// <strong>Note:</strong> Setting <code>preserveFileDate</code> to
		/// <see langword="true"/>
		/// tries to preserve the file's last modified
		/// date/times using
		/// <see cref="Sharpen.FilePath.SetLastModified(long)"/>
		/// , however it is
		/// not guaranteed that the operation will succeed.
		/// If the modification operation fails, no indication is provided.
		/// </remarks>
		/// <param name="srcFile">
		/// an existing file to copy, must not be
		/// <see langword="null"/>
		/// </param>
		/// <param name="destFile">
		/// the new file, must not be
		/// <see langword="null"/>
		/// </param>
		/// <param name="preserveFileDate">
		/// true if the file date of the copy
		/// should be the same as the original
		/// </param>
		/// <exception cref="System.ArgumentNullException">
		/// if source or destination is
		/// <see langword="null"/>
		/// </exception>
		/// <exception cref="System.IO.IOException">if source or destination is invalid</exception>
		/// <exception cref="System.IO.IOException">if an IO error occurs during copying</exception>
		public static void NativeCopyFileUnbuffered(FilePath srcFile, FilePath destFile, 
			bool preserveFileDate)
		{
			if (srcFile == null)
			{
				throw new ArgumentNullException("Source must not be null");
			}
			if (destFile == null)
			{
				throw new ArgumentNullException("Destination must not be null");
			}
			if (srcFile.Exists() == false)
			{
				throw new FileNotFoundException("Source '" + srcFile + "' does not exist");
			}
			if (srcFile.IsDirectory())
			{
				throw new IOException("Source '" + srcFile + "' exists but is a directory");
			}
			if (srcFile.GetCanonicalPath().Equals(destFile.GetCanonicalPath()))
			{
				throw new IOException("Source '" + srcFile + "' and destination '" + destFile + "' are the same"
					);
			}
			FilePath parentFile = destFile.GetParentFile();
			if (parentFile != null)
			{
				if (!parentFile.Mkdirs() && !parentFile.IsDirectory())
				{
					throw new IOException("Destination '" + parentFile + "' directory cannot be created"
						);
				}
			}
			if (destFile.Exists())
			{
				if (FileUtil.CanWrite(destFile) == false)
				{
					throw new IOException("Destination '" + destFile + "' exists but is read-only");
				}
				else
				{
					if (destFile.Delete() == false)
					{
						throw new IOException("Destination '" + destFile + "' exists but cannot be deleted"
							);
					}
				}
			}
			try
			{
				NativeIO.CopyFileUnbuffered(srcFile, destFile);
			}
			catch (NativeIOException e)
			{
				throw new IOException("Failed to copy " + srcFile.GetCanonicalPath() + " to " + destFile
					.GetCanonicalPath() + " due to failure in NativeIO#copyFileUnbuffered(). " + e.ToString
					());
			}
			if (srcFile.Length() != destFile.Length())
			{
				throw new IOException("Failed to copy full contents from '" + srcFile + "' to '" 
					+ destFile + "'");
			}
			if (preserveFileDate)
			{
				if (destFile.SetLastModified(srcFile.LastModified()) == false)
				{
					if (Log.IsDebugEnabled())
					{
						Log.Debug("Failed to preserve last modified date from'" + srcFile + "' to '" + destFile
							 + "'");
					}
				}
			}
		}

		/// <summary>
		/// Recursively delete all the content of the directory first and then
		/// the directory itself from the local filesystem.
		/// </summary>
		/// <param name="dir">The directory to delete</param>
		/// <exception cref="System.IO.IOException"/>
		public static void DeleteDir(FilePath dir)
		{
			if (!FileUtil.FullyDelete(dir))
			{
				throw new IOException("Failed to delete " + dir.GetCanonicalPath());
			}
		}

		/// <summary>Write all data storage files.</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void WriteAll()
		{
			this.layoutVersion = GetServiceLayoutVersion();
			for (IEnumerator<Storage.StorageDirectory> it = storageDirs.GetEnumerator(); it.HasNext
				(); )
			{
				WriteProperties(it.Next());
			}
		}

		/// <summary>Unlock all storage directories.</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void UnlockAll()
		{
			for (IEnumerator<Storage.StorageDirectory> it = storageDirs.GetEnumerator(); it.HasNext
				(); )
			{
				it.Next().Unlock();
			}
		}

		public static string GetBuildVersion()
		{
			return VersionInfo.GetRevision();
		}

		public static string GetRegistrationID(StorageInfo storage)
		{
			return "NS-" + Sharpen.Extensions.ToString(storage.GetNamespaceID()) + "-" + storage
				.GetClusterID() + "-" + System.Convert.ToString(storage.GetCTime());
		}

		public static bool Is203LayoutVersion(int layoutVersion)
		{
			foreach (int lv203 in LayoutVersions203)
			{
				if (lv203 == layoutVersion)
				{
					return true;
				}
			}
			return false;
		}
	}
}
