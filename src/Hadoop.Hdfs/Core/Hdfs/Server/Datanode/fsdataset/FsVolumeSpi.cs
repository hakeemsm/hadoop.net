using System;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Datanode.Fsdataset
{
	/// <summary>This is an interface for the underlying volume.</summary>
	public abstract class FsVolumeSpi
	{
		/// <summary>
		/// Obtain a reference object that had increased 1 reference count of the
		/// volume.
		/// </summary>
		/// <remarks>
		/// Obtain a reference object that had increased 1 reference count of the
		/// volume.
		/// It is caller's responsibility to close
		/// <see cref="FsVolumeReference"/>
		/// to decrease
		/// the reference count on the volume.
		/// </remarks>
		/// <exception cref="Sharpen.ClosedChannelException"/>
		public abstract FsVolumeReference ObtainReference();

		/// <returns>the StorageUuid of the volume</returns>
		public abstract string GetStorageID();

		/// <returns>a list of block pools.</returns>
		public abstract string[] GetBlockPoolList();

		/// <returns>the available storage space in bytes.</returns>
		/// <exception cref="System.IO.IOException"/>
		public abstract long GetAvailable();

		/// <returns>the base path to the volume</returns>
		public abstract string GetBasePath();

		/// <returns>the path to the volume</returns>
		/// <exception cref="System.IO.IOException"/>
		public abstract string GetPath(string bpid);

		/// <returns>the directory for the finalized blocks in the block pool.</returns>
		/// <exception cref="System.IO.IOException"/>
		public abstract FilePath GetFinalizedDir(string bpid);

		public abstract StorageType GetStorageType();

		/// <summary>
		/// Reserve disk space for an RBW block so a writer does not run out of
		/// space before the block is full.
		/// </summary>
		public abstract void ReserveSpaceForRbw(long bytesToReserve);

		/// <summary>Release disk space previously reserved for RBW block.</summary>
		public abstract void ReleaseReservedSpace(long bytesToRelease);

		/// <summary>Returns true if the volume is NOT backed by persistent storage.</summary>
		public abstract bool IsTransientStorage();

		/// <summary>
		/// BlockIterator will return ExtendedBlock entries from a block pool in
		/// this volume.
		/// </summary>
		/// <remarks>
		/// BlockIterator will return ExtendedBlock entries from a block pool in
		/// this volume.  The entries will be returned in sorted order.<p/>
		/// BlockIterator objects themselves do not always have internal
		/// synchronization, so they can only safely be used by a single thread at a
		/// time.<p/>
		/// Closing the iterator does not save it.  You must call save to save it.
		/// </remarks>
		public interface BlockIterator : IDisposable
		{
			/// <summary>
			/// Get the next block.<p/>
			/// Note that this block may be removed in between the time we list it,
			/// and the time the caller tries to use it, or it may represent a stale
			/// entry.
			/// </summary>
			/// <remarks>
			/// Get the next block.<p/>
			/// Note that this block may be removed in between the time we list it,
			/// and the time the caller tries to use it, or it may represent a stale
			/// entry.  Callers should handle the case where the returned block no
			/// longer exists.
			/// </remarks>
			/// <returns>
			/// The next block, or null if there are no
			/// more blocks.  Null if there was an error
			/// determining the next block.
			/// </returns>
			/// <exception cref="System.IO.IOException">
			/// If there was an error getting the next block in
			/// this volume.  In this case, EOF will be set on
			/// the iterator.
			/// </exception>
			ExtendedBlock NextBlock();

			/// <summary>Returns true if we got to the end of the block pool.</summary>
			bool AtEnd();

			/// <summary>Repositions the iterator at the beginning of the block pool.</summary>
			void Rewind();

			/// <summary>Save this block iterator to the underlying volume.</summary>
			/// <remarks>
			/// Save this block iterator to the underlying volume.
			/// Any existing saved block iterator with this name will be overwritten.
			/// maxStalenessMs will not be saved.
			/// </remarks>
			/// <exception cref="System.IO.IOException">
			/// If there was an error when saving the block
			/// iterator.
			/// </exception>
			void Save();

			/// <summary>
			/// Set the maximum staleness of entries that we will return.<p/>
			/// A maximum staleness of 0 means we will never return stale entries; a
			/// larger value will allow us to reduce resource consumption in exchange
			/// for returning more potentially stale entries.
			/// </summary>
			/// <remarks>
			/// Set the maximum staleness of entries that we will return.<p/>
			/// A maximum staleness of 0 means we will never return stale entries; a
			/// larger value will allow us to reduce resource consumption in exchange
			/// for returning more potentially stale entries.  Even with staleness set
			/// to 0, consumers of this API must handle race conditions where block
			/// disappear before they can be processed.
			/// </remarks>
			void SetMaxStalenessMs(long maxStalenessMs);

			/// <summary>
			/// Get the wall-clock time, measured in milliseconds since the Epoch,
			/// when this iterator was created.
			/// </summary>
			long GetIterStartMs();

			/// <summary>
			/// Get the wall-clock time, measured in milliseconds since the Epoch,
			/// when this iterator was last saved.
			/// </summary>
			/// <remarks>
			/// Get the wall-clock time, measured in milliseconds since the Epoch,
			/// when this iterator was last saved.  Returns iterStartMs if the
			/// iterator was never saved.
			/// </remarks>
			long GetLastSavedMs();

			/// <summary>Get the id of the block pool which this iterator traverses.</summary>
			string GetBlockPoolId();
		}

		/// <summary>Create a new block iterator.</summary>
		/// <remarks>
		/// Create a new block iterator.  It will start at the beginning of the
		/// block set.
		/// </remarks>
		/// <param name="bpid">The block pool id to iterate over.</param>
		/// <param name="name">The name of the block iterator to create.</param>
		/// <returns>The new block iterator.</returns>
		public abstract FsVolumeSpi.BlockIterator NewBlockIterator(string bpid, string name
			);

		/// <summary>Load a saved block iterator.</summary>
		/// <param name="bpid">The block pool id to iterate over.</param>
		/// <param name="name">The name of the block iterator to load.</param>
		/// <returns>The saved block iterator.</returns>
		/// <exception cref="System.IO.IOException">
		/// If there was an IO error loading the saved
		/// block iterator.
		/// </exception>
		public abstract FsVolumeSpi.BlockIterator LoadBlockIterator(string bpid, string name
			);

		/// <summary>Get the FSDatasetSpi which this volume is a part of.</summary>
		public abstract FsDatasetSpi GetDataset();
	}

	public static class FsVolumeSpiConstants
	{
	}
}
