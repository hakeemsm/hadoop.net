using System.Collections.Generic;
using System.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>Inspects a FSImage storage directory in the "old" (pre-HDFS-1073) format.
	/// 	</summary>
	/// <remarks>
	/// Inspects a FSImage storage directory in the "old" (pre-HDFS-1073) format.
	/// This format has the following data files:
	/// - fsimage
	/// - fsimage.ckpt (when checkpoint is being uploaded)
	/// - edits
	/// - edits.new (when logs are "rolled")
	/// </remarks>
	internal class FSImagePreTransactionalStorageInspector : FSImageStorageInspector
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(FSImagePreTransactionalStorageInspector
			));

		private bool hasOutOfDateStorageDirs = false;

		private bool isUpgradeFinalized = true;

		private bool needToSaveAfterRecovery = false;

		private long latestNameCheckpointTime = long.MinValue;

		private long latestEditsCheckpointTime = long.MinValue;

		private Storage.StorageDirectory latestNameSD = null;

		private Storage.StorageDirectory latestEditsSD = null;

		/// <summary>Set to determine if all of storageDirectories share the same checkpoint</summary>
		internal readonly ICollection<long> checkpointTimes = new HashSet<long>();

		private readonly IList<string> imageDirs = new AList<string>();

		private readonly IList<string> editsDirs = new AList<string>();

		/* Flag if there is at least one storage dir that doesn't contain the newest
		* fstime */
		/* Flag set false if there are any "previous" directories found */
		// Track the name and edits dir with the latest times
		/// <exception cref="System.IO.IOException"/>
		internal override void InspectDirectory(Storage.StorageDirectory sd)
		{
			// Was the file just formatted?
			if (!sd.GetVersionFile().Exists())
			{
				hasOutOfDateStorageDirs = true;
				return;
			}
			bool imageExists = false;
			bool editsExists = false;
			// Determine if sd is image, edits or both
			if (sd.GetStorageDirType().IsOfType(NNStorage.NameNodeDirType.Image))
			{
				imageExists = NNStorage.GetStorageFile(sd, NNStorage.NameNodeFile.Image).Exists();
				imageDirs.AddItem(sd.GetRoot().GetCanonicalPath());
			}
			if (sd.GetStorageDirType().IsOfType(NNStorage.NameNodeDirType.Edits))
			{
				editsExists = NNStorage.GetStorageFile(sd, NNStorage.NameNodeFile.Edits).Exists();
				editsDirs.AddItem(sd.GetRoot().GetCanonicalPath());
			}
			long checkpointTime = ReadCheckpointTime(sd);
			checkpointTimes.AddItem(checkpointTime);
			if (sd.GetStorageDirType().IsOfType(NNStorage.NameNodeDirType.Image) && (latestNameCheckpointTime
				 < checkpointTime) && imageExists)
			{
				latestNameCheckpointTime = checkpointTime;
				latestNameSD = sd;
			}
			if (sd.GetStorageDirType().IsOfType(NNStorage.NameNodeDirType.Edits) && (latestEditsCheckpointTime
				 < checkpointTime) && editsExists)
			{
				latestEditsCheckpointTime = checkpointTime;
				latestEditsSD = sd;
			}
			// check that we have a valid, non-default checkpointTime
			if (checkpointTime <= 0L)
			{
				hasOutOfDateStorageDirs = true;
			}
			// set finalized flag
			isUpgradeFinalized = isUpgradeFinalized && !sd.GetPreviousDir().Exists();
		}

		/// <summary>Determine the checkpoint time of the specified StorageDirectory</summary>
		/// <param name="sd">StorageDirectory to check</param>
		/// <returns>If file exists and can be read, last checkpoint time. If not, 0L.</returns>
		/// <exception cref="System.IO.IOException">On errors processing file pointed to by sd
		/// 	</exception>
		internal static long ReadCheckpointTime(Storage.StorageDirectory sd)
		{
			FilePath timeFile = NNStorage.GetStorageFile(sd, NNStorage.NameNodeFile.Time);
			long timeStamp = 0L;
			if (timeFile.Exists() && FileUtil.CanRead(timeFile))
			{
				DataInputStream @in = new DataInputStream(new FileInputStream(timeFile));
				try
				{
					timeStamp = @in.ReadLong();
					@in.Close();
					@in = null;
				}
				finally
				{
					IOUtils.Cleanup(Log, @in);
				}
			}
			return timeStamp;
		}

		internal override bool IsUpgradeFinalized()
		{
			return isUpgradeFinalized;
		}

		/// <exception cref="System.IO.IOException"/>
		internal override IList<FSImageStorageInspector.FSImageFile> GetLatestImages()
		{
			// We should have at least one image and one edits dirs
			if (latestNameSD == null)
			{
				throw new IOException("Image file is not found in " + imageDirs);
			}
			if (latestEditsSD == null)
			{
				throw new IOException("Edits file is not found in " + editsDirs);
			}
			// Make sure we are loading image and edits from same checkpoint
			if (latestNameCheckpointTime > latestEditsCheckpointTime && latestNameSD != latestEditsSD
				 && latestNameSD.GetStorageDirType() == NNStorage.NameNodeDirType.Image && latestEditsSD
				.GetStorageDirType() == NNStorage.NameNodeDirType.Edits)
			{
				// This is a rare failure when NN has image-only and edits-only
				// storage directories, and fails right after saving images,
				// in some of the storage directories, but before purging edits.
				// See -NOTE- in saveNamespace().
				Log.Error("This is a rare failure scenario!!!");
				Log.Error("Image checkpoint time " + latestNameCheckpointTime + " > edits checkpoint time "
					 + latestEditsCheckpointTime);
				Log.Error("Name-node will treat the image as the latest state of " + "the namespace. Old edits will be discarded."
					);
			}
			else
			{
				if (latestNameCheckpointTime != latestEditsCheckpointTime)
				{
					throw new IOException("Inconsistent storage detected, " + "image and edits checkpoint times do not match. "
						 + "image checkpoint time = " + latestNameCheckpointTime + "edits checkpoint time = "
						 + latestEditsCheckpointTime);
				}
			}
			needToSaveAfterRecovery = DoRecovery();
			FSImageStorageInspector.FSImageFile file = new FSImageStorageInspector.FSImageFile
				(latestNameSD, NNStorage.GetStorageFile(latestNameSD, NNStorage.NameNodeFile.Image
				), HdfsConstants.InvalidTxid);
			List<FSImageStorageInspector.FSImageFile> ret = new List<FSImageStorageInspector.FSImageFile
				>();
			ret.AddItem(file);
			return ret;
		}

		internal override bool NeedToSave()
		{
			return hasOutOfDateStorageDirs || checkpointTimes.Count != 1 || latestNameCheckpointTime
				 > latestEditsCheckpointTime || needToSaveAfterRecovery;
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual bool DoRecovery()
		{
			Log.Debug("Performing recovery in " + latestNameSD + " and " + latestEditsSD);
			bool needToSave = false;
			FilePath curFile = NNStorage.GetStorageFile(latestNameSD, NNStorage.NameNodeFile.
				Image);
			FilePath ckptFile = NNStorage.GetStorageFile(latestNameSD, NNStorage.NameNodeFile
				.ImageNew);
			//
			// If we were in the midst of a checkpoint
			//
			if (ckptFile.Exists())
			{
				needToSave = true;
				if (NNStorage.GetStorageFile(latestEditsSD, NNStorage.NameNodeFile.EditsNew).Exists
					())
				{
					//
					// checkpointing migth have uploaded a new
					// merged image, but we discard it here because we are
					// not sure whether the entire merged image was uploaded
					// before the namenode crashed.
					//
					if (!ckptFile.Delete())
					{
						throw new IOException("Unable to delete " + ckptFile);
					}
				}
				else
				{
					//
					// checkpointing was in progress when the namenode
					// shutdown. The fsimage.ckpt was created and the edits.new
					// file was moved to edits. We complete that checkpoint by
					// moving fsimage.new to fsimage. There is no need to 
					// update the fstime file here. renameTo fails on Windows
					// if the destination file already exists.
					//
					if (!ckptFile.RenameTo(curFile))
					{
						if (!curFile.Delete())
						{
							Log.Warn("Unable to delete dir " + curFile + " before rename");
						}
						if (!ckptFile.RenameTo(curFile))
						{
							throw new IOException("Unable to rename " + ckptFile + " to " + curFile);
						}
					}
				}
			}
			return needToSave;
		}

		/// <returns>
		/// a list with the paths to EDITS and EDITS_NEW (if it exists)
		/// in a given storage directory.
		/// </returns>
		internal static IList<FilePath> GetEditsInStorageDir(Storage.StorageDirectory sd)
		{
			AList<FilePath> files = new AList<FilePath>();
			FilePath edits = NNStorage.GetStorageFile(sd, NNStorage.NameNodeFile.Edits);
			System.Diagnostics.Debug.Assert(edits.Exists(), "Expected edits file at " + edits
				);
			files.AddItem(edits);
			FilePath editsNew = NNStorage.GetStorageFile(sd, NNStorage.NameNodeFile.EditsNew);
			if (editsNew.Exists())
			{
				files.AddItem(editsNew);
			}
			return files;
		}

		private IList<FilePath> GetLatestEditsFiles()
		{
			if (latestNameCheckpointTime > latestEditsCheckpointTime)
			{
				// the image is already current, discard edits
				Log.Debug("Name checkpoint time is newer than edits, not loading edits.");
				return Sharpen.Collections.EmptyList();
			}
			return GetEditsInStorageDir(latestEditsSD);
		}

		internal override long GetMaxSeenTxId()
		{
			return 0L;
		}

		/// <exception cref="System.IO.IOException"/>
		internal static IEnumerable<EditLogInputStream> GetEditLogStreams(NNStorage storage
			)
		{
			FSImagePreTransactionalStorageInspector inspector = new FSImagePreTransactionalStorageInspector
				();
			storage.InspectStorageDirs(inspector);
			IList<EditLogInputStream> editStreams = new AList<EditLogInputStream>();
			foreach (FilePath f in inspector.GetLatestEditsFiles())
			{
				editStreams.AddItem(new EditLogFileInputStream(f));
			}
			return editStreams;
		}
	}
}
