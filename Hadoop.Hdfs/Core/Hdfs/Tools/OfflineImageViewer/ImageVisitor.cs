using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Tools.OfflineImageViewer
{
	/// <summary>
	/// An implementation of ImageVisitor can traverse the structure of an
	/// Hadoop fsimage and respond to each of the structures within the file.
	/// </summary>
	internal abstract class ImageVisitor
	{
		/// <summary>
		/// Structural elements of an FSImage that may be encountered within the
		/// file.
		/// </summary>
		/// <remarks>
		/// Structural elements of an FSImage that may be encountered within the
		/// file. ImageVisitors are able to handle processing any of these elements.
		/// </remarks>
		public enum ImageElement
		{
			FsImage,
			ImageVersion,
			NamespaceId,
			IsCompressed,
			CompressCodec,
			LayoutVersion,
			NumInodes,
			GenerationStamp,
			GenerationStampV2,
			GenerationStampV1Limit,
			LastAllocatedBlockId,
			Inodes,
			Inode,
			InodePath,
			Replication,
			ModificationTime,
			AccessTime,
			BlockSize,
			NumBlocks,
			Blocks,
			Block,
			BlockId,
			NumBytes,
			NsQuota,
			DsQuota,
			Permissions,
			Symlink,
			NumInodesUnderConstruction,
			InodesUnderConstruction,
			InodeUnderConstruction,
			PreferredBlockSize,
			ClientName,
			ClientMachine,
			UserName,
			GroupName,
			PermissionString,
			CurrentDelegationKeyId,
			NumDelegationKeys,
			DelegationKeys,
			DelegationKey,
			DelegationTokenSequenceNumber,
			NumDelegationTokens,
			DelegationTokens,
			DelegationTokenIdentifier,
			DelegationTokenIdentifierKind,
			DelegationTokenIdentifierSeqno,
			DelegationTokenIdentifierOwner,
			DelegationTokenIdentifierRenewer,
			DelegationTokenIdentifierRealuser,
			DelegationTokenIdentifierIssueDate,
			DelegationTokenIdentifierMaxDate,
			DelegationTokenIdentifierExpiryTime,
			DelegationTokenIdentifierMasterKeyId,
			TransactionId,
			LastInodeId,
			InodeId,
			SnapshotCounter,
			NumSnapshotsTotal,
			NumSnapshots,
			Snapshots,
			Snapshot,
			SnapshotId,
			SnapshotRoot,
			SnapshotQuota,
			NumSnapshotDirDiff,
			SnapshotDirDiffs,
			SnapshotDirDiff,
			SnapshotDiffSnapshotid,
			SnapshotDirDiffChildrenSize,
			SnapshotInodeFileAttributes,
			SnapshotInodeDirectoryAttributes,
			SnapshotDirDiffCreatedlist,
			SnapshotDirDiffCreatedlistSize,
			SnapshotDirDiffCreatedInode,
			SnapshotDirDiffDeletedlist,
			SnapshotDirDiffDeletedlistSize,
			SnapshotDirDiffDeletedInode,
			IsSnapshottableDir,
			IsWithsnapshotDir,
			SnapshotFileDiffs,
			SnapshotFileDiff,
			NumSnapshotFileDiff,
			SnapshotFileSize,
			SnapshotDstSnapshotId,
			SnapshotLastSnapshotId,
			SnapshotRefInodeId,
			SnapshotRefInode,
			CacheNextEntryId,
			CacheNumPools,
			CachePoolName,
			CachePoolOwnerName,
			CachePoolGroupName,
			CachePoolPermissionString,
			CachePoolWeight,
			CacheNumEntries,
			CacheEntryPath,
			CacheEntryReplication,
			CacheEntryPoolName
		}

		/// <summary>Begin visiting the fsimage structure.</summary>
		/// <remarks>
		/// Begin visiting the fsimage structure.  Opportunity to perform
		/// any initialization necessary for the implementing visitor.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		internal abstract void Start();

		/// <summary>Finish visiting the fsimage structure.</summary>
		/// <remarks>
		/// Finish visiting the fsimage structure.  Opportunity to perform any
		/// clean up necessary for the implementing visitor.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		internal abstract void Finish();

		/// <summary>
		/// Finish visiting the fsimage structure after an error has occurred
		/// during the processing.
		/// </summary>
		/// <remarks>
		/// Finish visiting the fsimage structure after an error has occurred
		/// during the processing.  Opportunity to perform any clean up necessary
		/// for the implementing visitor.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		internal abstract void FinishAbnormally();

		/// <summary>Visit non enclosing element of fsimage with specified value.</summary>
		/// <param name="element">FSImage element</param>
		/// <param name="value">Element's value</param>
		/// <exception cref="System.IO.IOException"/>
		internal abstract void Visit(ImageVisitor.ImageElement element, string value);

		// Convenience methods to automatically convert numeric value types to strings
		/// <exception cref="System.IO.IOException"/>
		internal virtual void Visit(ImageVisitor.ImageElement element, int value)
		{
			Visit(element, Sharpen.Extensions.ToString(value));
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual void Visit(ImageVisitor.ImageElement element, long value)
		{
			Visit(element, System.Convert.ToString(value));
		}

		/// <summary>
		/// Begin visiting an element that encloses another element, such as
		/// the beginning of the list of blocks that comprise a file.
		/// </summary>
		/// <param name="element">Element being visited</param>
		/// <exception cref="System.IO.IOException"/>
		internal abstract void VisitEnclosingElement(ImageVisitor.ImageElement element);

		/// <summary>
		/// Begin visiting an element that encloses another element, such as
		/// the beginning of the list of blocks that comprise a file.
		/// </summary>
		/// <remarks>
		/// Begin visiting an element that encloses another element, such as
		/// the beginning of the list of blocks that comprise a file.
		/// Also provide an additional key and value for the element, such as the
		/// number items within the element.
		/// </remarks>
		/// <param name="element">Element being visited</param>
		/// <param name="key">Key describing the element being visited</param>
		/// <param name="value">Value associated with element being visited</param>
		/// <exception cref="System.IO.IOException"/>
		internal abstract void VisitEnclosingElement(ImageVisitor.ImageElement element, ImageVisitor.ImageElement
			 key, string value);

		// Convenience methods to automatically convert value types to strings
		/// <exception cref="System.IO.IOException"/>
		internal virtual void VisitEnclosingElement(ImageVisitor.ImageElement element, ImageVisitor.ImageElement
			 key, int value)
		{
			VisitEnclosingElement(element, key, Sharpen.Extensions.ToString(value));
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual void VisitEnclosingElement(ImageVisitor.ImageElement element, ImageVisitor.ImageElement
			 key, long value)
		{
			VisitEnclosingElement(element, key, System.Convert.ToString(value));
		}

		/// <summary>Leave current enclosing element.</summary>
		/// <remarks>
		/// Leave current enclosing element.  Called, for instance, at the end of
		/// processing the blocks that compromise a file.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		internal abstract void LeaveEnclosingElement();
	}
}
