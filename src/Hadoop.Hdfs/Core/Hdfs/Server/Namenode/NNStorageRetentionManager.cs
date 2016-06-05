using System;
using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Base;
using Com.Google.Common.Collect;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>
	/// The NNStorageRetentionManager is responsible for inspecting the storage
	/// directories of the NN and enforcing a retention policy on checkpoints
	/// and edit logs.
	/// </summary>
	/// <remarks>
	/// The NNStorageRetentionManager is responsible for inspecting the storage
	/// directories of the NN and enforcing a retention policy on checkpoints
	/// and edit logs.
	/// It delegates the actual removal of files to a StoragePurger
	/// implementation, which might delete the files or instead copy them to
	/// a filer or HDFS for later analysis.
	/// </remarks>
	public class NNStorageRetentionManager
	{
		private readonly int numCheckpointsToRetain;

		private readonly long numExtraEditsToRetain;

		private readonly int maxExtraEditsSegmentsToRetain;

		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Hdfs.Server.Namenode.NNStorageRetentionManager
			));

		private readonly NNStorage storage;

		private readonly NNStorageRetentionManager.StoragePurger purger;

		private readonly LogsPurgeable purgeableLogs;

		public NNStorageRetentionManager(Configuration conf, NNStorage storage, LogsPurgeable
			 purgeableLogs, NNStorageRetentionManager.StoragePurger purger)
		{
			this.numCheckpointsToRetain = conf.GetInt(DFSConfigKeys.DfsNamenodeNumCheckpointsRetainedKey
				, DFSConfigKeys.DfsNamenodeNumCheckpointsRetainedDefault);
			this.numExtraEditsToRetain = conf.GetLong(DFSConfigKeys.DfsNamenodeNumExtraEditsRetainedKey
				, DFSConfigKeys.DfsNamenodeNumExtraEditsRetainedDefault);
			this.maxExtraEditsSegmentsToRetain = conf.GetInt(DFSConfigKeys.DfsNamenodeMaxExtraEditsSegmentsRetainedKey
				, DFSConfigKeys.DfsNamenodeMaxExtraEditsSegmentsRetainedDefault);
			Preconditions.CheckArgument(numCheckpointsToRetain > 0, "Must retain at least one checkpoint"
				);
			Preconditions.CheckArgument(numExtraEditsToRetain >= 0, DFSConfigKeys.DfsNamenodeNumExtraEditsRetainedKey
				 + " must not be negative");
			this.storage = storage;
			this.purgeableLogs = purgeableLogs;
			this.purger = purger;
		}

		public NNStorageRetentionManager(Configuration conf, NNStorage storage, LogsPurgeable
			 purgeableLogs)
			: this(conf, storage, purgeableLogs, new NNStorageRetentionManager.DeletionStoragePurger
				())
		{
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual void PurgeCheckpoints(NNStorage.NameNodeFile nnf)
		{
			PurgeCheckpoinsAfter(nnf, -1);
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual void PurgeCheckpoinsAfter(NNStorage.NameNodeFile nnf, long fromTxId
			)
		{
			FSImageTransactionalStorageInspector inspector = new FSImageTransactionalStorageInspector
				(EnumSet.Of(nnf));
			storage.InspectStorageDirs(inspector);
			foreach (FSImageStorageInspector.FSImageFile image in inspector.GetFoundImages())
			{
				if (image.GetCheckpointTxId() > fromTxId)
				{
					purger.PurgeImage(image);
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual void PurgeOldStorage(NNStorage.NameNodeFile nnf)
		{
			FSImageTransactionalStorageInspector inspector = new FSImageTransactionalStorageInspector
				(EnumSet.Of(nnf));
			storage.InspectStorageDirs(inspector);
			long minImageTxId = GetImageTxIdToRetain(inspector);
			PurgeCheckpointsOlderThan(inspector, minImageTxId);
			if (nnf == NNStorage.NameNodeFile.ImageRollback)
			{
				// do not purge edits for IMAGE_ROLLBACK.
				return;
			}
			// If fsimage_N is the image we want to keep, then we need to keep
			// all txns > N. We can remove anything < N+1, since fsimage_N
			// reflects the state up to and including N. However, we also
			// provide a "cushion" of older txns that we keep, which is
			// handy for HA, where a remote node may not have as many
			// new images.
			//
			// First, determine the target number of extra transactions to retain based
			// on the configured amount.
			long minimumRequiredTxId = minImageTxId + 1;
			long purgeLogsFrom = Math.Max(0, minimumRequiredTxId - numExtraEditsToRetain);
			AList<EditLogInputStream> editLogs = new AList<EditLogInputStream>();
			purgeableLogs.SelectInputStreams(editLogs, purgeLogsFrom, false);
			editLogs.Sort(new _IComparer_138());
			// Remove from consideration any edit logs that are in fact required.
			while (editLogs.Count > 0 && editLogs[editLogs.Count - 1].GetFirstTxId() >= minimumRequiredTxId
				)
			{
				editLogs.Remove(editLogs.Count - 1);
			}
			// Next, adjust the number of transactions to retain if doing so would mean
			// keeping too many segments around.
			while (editLogs.Count > maxExtraEditsSegmentsToRetain)
			{
				purgeLogsFrom = editLogs[0].GetLastTxId() + 1;
				editLogs.Remove(0);
			}
			// Finally, ensure that we're not trying to purge any transactions that we
			// actually need.
			if (purgeLogsFrom > minimumRequiredTxId)
			{
				throw new Exception("Should not purge more edits than required to " + "restore: "
					 + purgeLogsFrom + " should be <= " + minimumRequiredTxId);
			}
			purgeableLogs.PurgeLogsOlderThan(purgeLogsFrom);
		}

		private sealed class _IComparer_138 : IComparer<EditLogInputStream>
		{
			public _IComparer_138()
			{
			}

			public int Compare(EditLogInputStream a, EditLogInputStream b)
			{
				return ComparisonChain.Start().Compare(a.GetFirstTxId(), b.GetFirstTxId()).Compare
					(a.GetLastTxId(), b.GetLastTxId()).Result();
			}
		}

		private void PurgeCheckpointsOlderThan(FSImageTransactionalStorageInspector inspector
			, long minTxId)
		{
			foreach (FSImageStorageInspector.FSImageFile image in inspector.GetFoundImages())
			{
				if (image.GetCheckpointTxId() < minTxId)
				{
					purger.PurgeImage(image);
				}
			}
		}

		/// <param name="inspector">inspector that has already inspected all storage dirs</param>
		/// <returns>
		/// the transaction ID corresponding to the oldest checkpoint
		/// that should be retained.
		/// </returns>
		private long GetImageTxIdToRetain(FSImageTransactionalStorageInspector inspector)
		{
			IList<FSImageStorageInspector.FSImageFile> images = inspector.GetFoundImages();
			TreeSet<long> imageTxIds = Sets.NewTreeSet();
			foreach (FSImageStorageInspector.FSImageFile image in images)
			{
				imageTxIds.AddItem(image.GetCheckpointTxId());
			}
			IList<long> imageTxIdsList = Lists.NewArrayList(imageTxIds);
			if (imageTxIdsList.IsEmpty())
			{
				return 0;
			}
			Sharpen.Collections.Reverse(imageTxIdsList);
			int toRetain = Math.Min(numCheckpointsToRetain, imageTxIdsList.Count);
			long minTxId = imageTxIdsList[toRetain - 1];
			Log.Info("Going to retain " + toRetain + " images with txid >= " + minTxId);
			return minTxId;
		}

		/// <summary>Interface responsible for disposing of old checkpoints and edit logs.</summary>
		internal interface StoragePurger
		{
			void PurgeLog(FileJournalManager.EditLogFile log);

			void PurgeImage(FSImageStorageInspector.FSImageFile image);
		}

		internal class DeletionStoragePurger : NNStorageRetentionManager.StoragePurger
		{
			public virtual void PurgeLog(FileJournalManager.EditLogFile log)
			{
				Log.Info("Purging old edit log " + log);
				DeleteOrWarn(log.GetFile());
			}

			public virtual void PurgeImage(FSImageStorageInspector.FSImageFile image)
			{
				Log.Info("Purging old image " + image);
				DeleteOrWarn(image.GetFile());
				DeleteOrWarn(MD5FileUtils.GetDigestFileForFile(image.GetFile()));
			}

			private static void DeleteOrWarn(FilePath file)
			{
				if (!file.Delete())
				{
					// It's OK if we fail to delete something -- we'll catch it
					// next time we swing through this directory.
					Log.Warn("Could not delete " + file);
				}
			}
		}

		/// <summary>Delete old OIV fsimages.</summary>
		/// <remarks>
		/// Delete old OIV fsimages. Since the target dir is not a full blown
		/// storage directory, we simply list and keep the latest ones. For the
		/// same reason, no storage inspector is used.
		/// </remarks>
		internal virtual void PurgeOldLegacyOIVImages(string dir, long txid)
		{
			FilePath oivImageDir = new FilePath(dir);
			string oivImagePrefix = NNStorage.NameNodeFile.ImageLegacyOiv.GetName();
			string[] filesInStorage;
			// Get the listing
			filesInStorage = oivImageDir.List(new _FilenameFilter_250(oivImagePrefix));
			// Check whether there is any work to do.
			if (filesInStorage.Length <= numCheckpointsToRetain)
			{
				return;
			}
			// Create a sorted list of txids from the file names.
			TreeSet<long> sortedTxIds = new TreeSet<long>();
			foreach (string fName in filesInStorage)
			{
				// Extract the transaction id from the file name.
				long fTxId;
				try
				{
					fTxId = long.Parse(Sharpen.Runtime.Substring(fName, oivImagePrefix.Length + 1));
				}
				catch (FormatException)
				{
					// This should not happen since we have already filtered it.
					// Log and continue.
					Log.Warn("Invalid file name. Skipping " + fName);
					continue;
				}
				sortedTxIds.AddItem(Sharpen.Extensions.ValueOf(fTxId));
			}
			int numFilesToDelete = sortedTxIds.Count - numCheckpointsToRetain;
			IEnumerator<long> iter = sortedTxIds.GetEnumerator();
			while (numFilesToDelete > 0 && iter.HasNext())
			{
				long txIdVal = iter.Next();
				string fileName = NNStorage.GetLegacyOIVImageFileName(txIdVal);
				Log.Info("Deleting " + fileName);
				FilePath fileToDelete = new FilePath(oivImageDir, fileName);
				if (!fileToDelete.Delete())
				{
					// deletion failed.
					Log.Warn("Failed to delete image file: " + fileToDelete);
				}
				numFilesToDelete--;
			}
		}

		private sealed class _FilenameFilter_250 : FilenameFilter
		{
			public _FilenameFilter_250(string oivImagePrefix)
			{
				this.oivImagePrefix = oivImagePrefix;
			}

			public bool Accept(FilePath dir, string name)
			{
				return name.Matches(oivImagePrefix + "_(\\d+)");
			}

			private readonly string oivImagePrefix;
		}
	}
}
