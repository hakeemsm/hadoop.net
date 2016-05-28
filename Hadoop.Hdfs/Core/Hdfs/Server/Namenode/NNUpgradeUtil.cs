using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Base;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.IO;
using Sharpen;
using Sharpen.File;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	public abstract class NNUpgradeUtil
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(NNUpgradeUtil));

		/// <summary>
		/// Return true if this storage dir can roll back to the previous storage
		/// state, false otherwise.
		/// </summary>
		/// <remarks>
		/// Return true if this storage dir can roll back to the previous storage
		/// state, false otherwise. The NN will refuse to run the rollback operation
		/// unless at least one JM or fsimage storage directory can roll back.
		/// </remarks>
		/// <param name="storage">the storage info for the current state</param>
		/// <param name="prevStorage">the storage info for the previous (unupgraded) state</param>
		/// <param name="targetLayoutVersion">the layout version we intend to roll back to</param>
		/// <returns>true if this JM can roll back, false otherwise.</returns>
		/// <exception cref="System.IO.IOException">in the event of error</exception>
		internal static bool CanRollBack(Storage.StorageDirectory sd, StorageInfo storage
			, StorageInfo prevStorage, int targetLayoutVersion)
		{
			FilePath prevDir = sd.GetPreviousDir();
			if (!prevDir.Exists())
			{
				// use current directory then
				Log.Info("Storage directory " + sd.GetRoot() + " does not contain previous fs state."
					);
				// read and verify consistency with other directories
				storage.ReadProperties(sd);
				return false;
			}
			// read and verify consistency of the prev dir
			prevStorage.ReadPreviousVersionProperties(sd);
			if (prevStorage.GetLayoutVersion() != targetLayoutVersion)
			{
				throw new IOException("Cannot rollback to storage version " + prevStorage.GetLayoutVersion
					() + " using this version of the NameNode, which uses storage version " + targetLayoutVersion
					 + ". " + "Please use the previous version of HDFS to perform the rollback.");
			}
			return true;
		}

		/// <summary>Finalize the upgrade.</summary>
		/// <remarks>
		/// Finalize the upgrade. The previous dir, if any, will be renamed and
		/// removed. After this is completed, rollback is no longer allowed.
		/// </remarks>
		/// <param name="sd">the storage directory to finalize</param>
		/// <exception cref="System.IO.IOException">in the event of error</exception>
		internal static void DoFinalize(Storage.StorageDirectory sd)
		{
			FilePath prevDir = sd.GetPreviousDir();
			if (!prevDir.Exists())
			{
				// already discarded
				Log.Info("Directory " + prevDir + " does not exist.");
				Log.Info("Finalize upgrade for " + sd.GetRoot() + " is not required.");
				return;
			}
			Log.Info("Finalizing upgrade of storage directory " + sd.GetRoot());
			Preconditions.CheckState(sd.GetCurrentDir().Exists(), "Current directory must exist."
				);
			FilePath tmpDir = sd.GetFinalizedTmp();
			// rename previous to tmp and remove
			NNStorage.Rename(prevDir, tmpDir);
			NNStorage.DeleteDir(tmpDir);
			Log.Info("Finalize upgrade for " + sd.GetRoot() + " is complete.");
		}

		/// <summary>
		/// Perform any steps that must succeed across all storage dirs/JournalManagers
		/// involved in an upgrade before proceeding onto the actual upgrade stage.
		/// </summary>
		/// <remarks>
		/// Perform any steps that must succeed across all storage dirs/JournalManagers
		/// involved in an upgrade before proceeding onto the actual upgrade stage. If
		/// a call to any JM's or local storage dir's doPreUpgrade method fails, then
		/// doUpgrade will not be called for any JM. The existing current dir is
		/// renamed to previous.tmp, and then a new, empty current dir is created.
		/// </remarks>
		/// <param name="conf">
		/// configuration for creating
		/// <see cref="EditLogFileOutputStream"/>
		/// </param>
		/// <param name="sd">the storage directory to perform the pre-upgrade procedure.</param>
		/// <exception cref="System.IO.IOException">in the event of error</exception>
		internal static void DoPreUpgrade(Configuration conf, Storage.StorageDirectory sd
			)
		{
			Log.Info("Starting upgrade of storage directory " + sd.GetRoot());
			// rename current to tmp
			RenameCurToTmp(sd);
			FilePath curDir = sd.GetCurrentDir();
			FilePath tmpDir = sd.GetPreviousTmp();
			IList<string> fileNameList = IOUtils.ListDirectory(tmpDir, new _FilenameFilter_121
				(tmpDir));
			foreach (string s in fileNameList)
			{
				FilePath prevFile = new FilePath(tmpDir, s);
				FilePath newFile = new FilePath(curDir, prevFile.GetName());
				Files.CreateLink(newFile.ToPath(), prevFile.ToPath());
			}
		}

		private sealed class _FilenameFilter_121 : FilenameFilter
		{
			public _FilenameFilter_121(FilePath tmpDir)
			{
				this.tmpDir = tmpDir;
			}

			public bool Accept(FilePath dir, string name)
			{
				return dir.Equals(tmpDir) && name.StartsWith(NNStorage.NameNodeFile.Edits.GetName
					());
			}

			private readonly FilePath tmpDir;
		}

		/// <summary>
		/// Rename the existing current dir to previous.tmp, and create a new empty
		/// current dir.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public static void RenameCurToTmp(Storage.StorageDirectory sd)
		{
			FilePath curDir = sd.GetCurrentDir();
			FilePath prevDir = sd.GetPreviousDir();
			FilePath tmpDir = sd.GetPreviousTmp();
			Preconditions.CheckState(curDir.Exists(), "Current directory must exist for preupgrade."
				);
			Preconditions.CheckState(!prevDir.Exists(), "Previous directory must not exist for preupgrade."
				);
			Preconditions.CheckState(!tmpDir.Exists(), "Previous.tmp directory must not exist for preupgrade."
				 + "Consider restarting for recovery.");
			// rename current to tmp
			NNStorage.Rename(curDir, tmpDir);
			if (!curDir.Mkdir())
			{
				throw new IOException("Cannot create directory " + curDir);
			}
		}

		/// <summary>Perform the upgrade of the storage dir to the given storage info.</summary>
		/// <remarks>
		/// Perform the upgrade of the storage dir to the given storage info. The new
		/// storage info is written into the current directory, and the previous.tmp
		/// directory is renamed to previous.
		/// </remarks>
		/// <param name="sd">the storage directory to upgrade</param>
		/// <param name="storage">info about the new upgraded versions.</param>
		/// <exception cref="System.IO.IOException">in the event of error</exception>
		public static void DoUpgrade(Storage.StorageDirectory sd, Storage storage)
		{
			Log.Info("Performing upgrade of storage directory " + sd.GetRoot());
			try
			{
				// Write the version file, since saveFsImage only makes the
				// fsimage_<txid>, and the directory is otherwise empty.
				storage.WriteProperties(sd);
				FilePath prevDir = sd.GetPreviousDir();
				FilePath tmpDir = sd.GetPreviousTmp();
				Preconditions.CheckState(!prevDir.Exists(), "previous directory must not exist for upgrade."
					);
				Preconditions.CheckState(tmpDir.Exists(), "previous.tmp directory must exist for upgrade."
					);
				// rename tmp to previous
				NNStorage.Rename(tmpDir, prevDir);
			}
			catch (IOException ioe)
			{
				Log.Error("Unable to rename temp to previous for " + sd.GetRoot(), ioe);
				throw;
			}
		}

		/// <summary>Perform rollback of the storage dir to the previous state.</summary>
		/// <remarks>
		/// Perform rollback of the storage dir to the previous state. The existing
		/// current dir is removed, and the previous dir is renamed to current.
		/// </remarks>
		/// <param name="sd">the storage directory to roll back.</param>
		/// <exception cref="System.IO.IOException">in the event of error</exception>
		internal static void DoRollBack(Storage.StorageDirectory sd)
		{
			FilePath prevDir = sd.GetPreviousDir();
			if (!prevDir.Exists())
			{
				return;
			}
			FilePath tmpDir = sd.GetRemovedTmp();
			Preconditions.CheckState(!tmpDir.Exists(), "removed.tmp directory must not exist for rollback."
				 + "Consider restarting for recovery.");
			// rename current to tmp
			FilePath curDir = sd.GetCurrentDir();
			Preconditions.CheckState(curDir.Exists(), "Current directory must exist for rollback."
				);
			NNStorage.Rename(curDir, tmpDir);
			// rename previous to current
			NNStorage.Rename(prevDir, curDir);
			// delete tmp dir
			NNStorage.DeleteDir(tmpDir);
			Log.Info("Rollback of " + sd.GetRoot() + " is complete.");
		}
	}
}
