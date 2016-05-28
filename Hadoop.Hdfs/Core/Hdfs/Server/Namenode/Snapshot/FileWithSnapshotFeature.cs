using System;
using System.Collections.Generic;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Blockmanagement;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Hdfs.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot
{
	/// <summary>Feature for file with snapshot-related information.</summary>
	public class FileWithSnapshotFeature : INode.Feature
	{
		private readonly FileDiffList diffs;

		private bool isCurrentFileDeleted = false;

		public FileWithSnapshotFeature(FileDiffList diffs)
		{
			this.diffs = diffs != null ? diffs : new FileDiffList();
		}

		public virtual bool IsCurrentFileDeleted()
		{
			return isCurrentFileDeleted;
		}

		/// <summary>
		/// We need to distinguish two scenarios:
		/// 1) the file is still in the current file directory, it has been modified
		/// before while it is included in some snapshot
		/// 2) the file is not in the current file directory (deleted), but it is in
		/// some snapshot, thus we still keep this inode
		/// For both scenarios the file has snapshot feature.
		/// </summary>
		/// <remarks>
		/// We need to distinguish two scenarios:
		/// 1) the file is still in the current file directory, it has been modified
		/// before while it is included in some snapshot
		/// 2) the file is not in the current file directory (deleted), but it is in
		/// some snapshot, thus we still keep this inode
		/// For both scenarios the file has snapshot feature. We set
		/// <see cref="isCurrentFileDeleted"/>
		/// to true for 2).
		/// </remarks>
		public virtual void DeleteCurrentFile()
		{
			isCurrentFileDeleted = true;
		}

		public virtual FileDiffList GetDiffs()
		{
			return diffs;
		}

		/// <returns>the max replication factor in diffs</returns>
		public virtual short GetMaxBlockRepInDiffs()
		{
			short max = 0;
			foreach (FileDiff d in GetDiffs())
			{
				if (d.snapshotINode != null)
				{
					short replication = d.snapshotINode.GetFileReplication();
					if (replication > max)
					{
						max = replication;
					}
				}
			}
			return max;
		}

		internal virtual bool ChangedBetweenSnapshots(INodeFile file, Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot
			 from, Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot to)
		{
			int[] diffIndexPair = diffs.ChangedBetweenSnapshots(from, to);
			if (diffIndexPair == null)
			{
				return false;
			}
			int earlierDiffIndex = diffIndexPair[0];
			int laterDiffIndex = diffIndexPair[1];
			IList<FileDiff> diffList = diffs.AsList();
			long earlierLength = diffList[earlierDiffIndex].GetFileSize();
			long laterLength = laterDiffIndex == diffList.Count ? file.ComputeFileSize(true, 
				false) : diffList[laterDiffIndex].GetFileSize();
			if (earlierLength != laterLength)
			{
				// file length has been changed
				return true;
			}
			INodeFileAttributes earlierAttr = null;
			// check the metadata
			for (int i = earlierDiffIndex; i < laterDiffIndex; i++)
			{
				FileDiff diff = diffList[i];
				if (diff.snapshotINode != null)
				{
					earlierAttr = diff.snapshotINode;
					break;
				}
			}
			if (earlierAttr == null)
			{
				// no meta-change at all, return false
				return false;
			}
			INodeFileAttributes laterAttr = diffs.GetSnapshotINode(Math.Max(Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot
				.GetSnapshotId(from), Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot.GetSnapshotId
				(to)), file);
			return !earlierAttr.MetadataEquals(laterAttr);
		}

		public virtual string GetDetailedString()
		{
			return (IsCurrentFileDeleted() ? "(DELETED), " : ", ") + diffs;
		}

		public virtual QuotaCounts CleanFile(BlockStoragePolicySuite bsps, INodeFile file
			, int snapshotId, int priorSnapshotId, INode.BlocksMapUpdateInfo collectedBlocks
			, IList<INode> removedINodes)
		{
			if (snapshotId == Org.Apache.Hadoop.Hdfs.Server.Namenode.Snapshot.Snapshot.CurrentStateId)
			{
				// delete the current file while the file has snapshot feature
				if (!IsCurrentFileDeleted())
				{
					file.RecordModification(priorSnapshotId);
					DeleteCurrentFile();
				}
				CollectBlocksAndClear(bsps, file, collectedBlocks, removedINodes);
				return new QuotaCounts.Builder().Build();
			}
			else
			{
				// delete the snapshot
				priorSnapshotId = GetDiffs().UpdatePrior(snapshotId, priorSnapshotId);
				return diffs.DeleteSnapshotDiff(bsps, snapshotId, priorSnapshotId, file, collectedBlocks
					, removedINodes);
			}
		}

		public virtual void ClearDiffs()
		{
			this.diffs.Clear();
		}

		public virtual QuotaCounts UpdateQuotaAndCollectBlocks(BlockStoragePolicySuite bsps
			, INodeFile file, FileDiff removed, INode.BlocksMapUpdateInfo collectedBlocks, IList
			<INode> removedINodes)
		{
			long oldStoragespace = file.StoragespaceConsumed();
			byte storagePolicyID = file.GetStoragePolicyID();
			BlockStoragePolicy bsp = null;
			EnumCounters<StorageType> typeSpaces = new EnumCounters<StorageType>(typeof(StorageType
				));
			if (storagePolicyID != BlockStoragePolicySuite.IdUnspecified)
			{
				bsp = bsps.GetPolicy(file.GetStoragePolicyID());
			}
			if (removed.snapshotINode != null)
			{
				short replication = removed.snapshotINode.GetFileReplication();
				short currentRepl = file.GetBlockReplication();
				if (currentRepl == 0)
				{
					long oldFileSizeNoRep = file.ComputeFileSize(true, true);
					oldStoragespace = oldFileSizeNoRep * replication;
					if (bsp != null)
					{
						IList<StorageType> oldTypeChosen = bsp.ChooseStorageTypes(replication);
						foreach (StorageType t in oldTypeChosen)
						{
							if (t.SupportTypeQuota())
							{
								typeSpaces.Add(t, -oldFileSizeNoRep);
							}
						}
					}
				}
				else
				{
					if (replication > currentRepl)
					{
						long oldFileSizeNoRep = file.StoragespaceConsumedNoReplication();
						oldStoragespace = oldFileSizeNoRep * replication;
						if (bsp != null)
						{
							IList<StorageType> oldTypeChosen = bsp.ChooseStorageTypes(replication);
							foreach (StorageType t in oldTypeChosen)
							{
								if (t.SupportTypeQuota())
								{
									typeSpaces.Add(t, -oldFileSizeNoRep);
								}
							}
							IList<StorageType> newTypeChosen = bsp.ChooseStorageTypes(currentRepl);
							foreach (StorageType t_1 in newTypeChosen)
							{
								if (t_1.SupportTypeQuota())
								{
									typeSpaces.Add(t_1, oldFileSizeNoRep);
								}
							}
						}
					}
				}
				AclFeature aclFeature = removed.GetSnapshotINode().GetAclFeature();
				if (aclFeature != null)
				{
					AclStorage.RemoveAclFeature(aclFeature);
				}
			}
			GetDiffs().CombineAndCollectSnapshotBlocks(bsps, file, removed, collectedBlocks, 
				removedINodes);
			long ssDelta = oldStoragespace - file.StoragespaceConsumed();
			return new QuotaCounts.Builder().StorageSpace(ssDelta).TypeSpaces(typeSpaces).Build
				();
		}

		/// <summary>
		/// If some blocks at the end of the block list no longer belongs to
		/// any inode, collect them and update the block list.
		/// </summary>
		public virtual void CollectBlocksAndClear(BlockStoragePolicySuite bsps, INodeFile
			 file, INode.BlocksMapUpdateInfo info, IList<INode> removedINodes)
		{
			// check if everything is deleted.
			if (IsCurrentFileDeleted() && GetDiffs().AsList().IsEmpty())
			{
				file.DestroyAndCollectBlocks(bsps, info, removedINodes);
				return;
			}
			// find max file size.
			long max;
			FileDiff diff = GetDiffs().GetLast();
			if (IsCurrentFileDeleted())
			{
				max = diff == null ? 0 : diff.GetFileSize();
			}
			else
			{
				max = file.ComputeFileSize();
			}
			// Collect blocks that should be deleted
			FileDiff last = diffs.GetLast();
			BlockInfoContiguous[] snapshotBlocks = last == null ? null : last.GetBlocks();
			if (snapshotBlocks == null)
			{
				file.CollectBlocksBeyondMax(max, info);
			}
			else
			{
				file.CollectBlocksBeyondSnapshot(snapshotBlocks, info);
			}
		}
	}
}
