using System.Collections.Generic;
using Com.Google.Common.Collect;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs.Security.Token.Block;
using Org.Apache.Hadoop.Hdfs.Server.Blockmanagement;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Protocol
{
	/// <summary>
	/// Associates a block with the Datanodes that contain its replicas
	/// and other block metadata (E.g.
	/// </summary>
	/// <remarks>
	/// Associates a block with the Datanodes that contain its replicas
	/// and other block metadata (E.g. the file offset associated with this
	/// block, whether it is corrupt, a location is cached in memory,
	/// security token, etc).
	/// </remarks>
	public class LocatedBlock
	{
		private readonly ExtendedBlock b;

		private long offset;

		private readonly DatanodeInfoWithStorage[] locs;

		/// <summary>Cached storage ID for each replica</summary>
		private string[] storageIDs;

		/// <summary>Cached storage type for each replica, if reported.</summary>
		private StorageType[] storageTypes;

		private bool corrupt;

		private Org.Apache.Hadoop.Security.Token.Token<BlockTokenIdentifier> blockToken = 
			new Org.Apache.Hadoop.Security.Token.Token<BlockTokenIdentifier>();

		/// <summary>List of cached datanode locations</summary>
		private DatanodeInfo[] cachedLocs;

		private static readonly DatanodeInfoWithStorage[] EmptyLocs = new DatanodeInfoWithStorage
			[0];

		public LocatedBlock(ExtendedBlock b, DatanodeInfo[] locs)
			: this(b, locs, -1, false)
		{
		}

		public LocatedBlock(ExtendedBlock b, DatanodeInfo[] locs, long startOffset, bool 
			corrupt)
			: this(b, locs, null, null, startOffset, corrupt, EmptyLocs)
		{
		}

		public LocatedBlock(ExtendedBlock b, DatanodeStorageInfo[] storages)
			: this(b, storages, -1, false)
		{
		}

		public LocatedBlock(ExtendedBlock b, DatanodeInfo[] locs, string[] storageIDs, StorageType
			[] storageTypes)
			: this(b, locs, storageIDs, storageTypes, -1, false, EmptyLocs)
		{
		}

		public LocatedBlock(ExtendedBlock b, DatanodeStorageInfo[] storages, long startOffset
			, bool corrupt)
			: this(b, DatanodeStorageInfo.ToDatanodeInfos(storages), DatanodeStorageInfo.ToStorageIDs
				(storages), DatanodeStorageInfo.ToStorageTypes(storages), startOffset, corrupt, 
				EmptyLocs)
		{
		}

		public LocatedBlock(ExtendedBlock b, DatanodeInfo[] locs, string[] storageIDs, StorageType
			[] storageTypes, long startOffset, bool corrupt, DatanodeInfo[] cachedLocs)
		{
			// offset of the first byte of the block in the file
			// corrupt flag is true if all of the replicas of a block are corrupt.
			// else false. If block has few corrupt replicas, they are filtered and 
			// their locations are not part of this object
			// Used when there are no locations
			// startOffset is unknown
			// startOffset is unknown
			// startOffset is unknown
			this.b = b;
			this.offset = startOffset;
			this.corrupt = corrupt;
			if (locs == null)
			{
				this.locs = EmptyLocs;
			}
			else
			{
				this.locs = new DatanodeInfoWithStorage[locs.Length];
				for (int i = 0; i < locs.Length; i++)
				{
					DatanodeInfo di = locs[i];
					DatanodeInfoWithStorage storage = new DatanodeInfoWithStorage(di, storageIDs != null
						 ? storageIDs[i] : null, storageTypes != null ? storageTypes[i] : null);
					this.locs[i] = storage;
				}
			}
			this.storageIDs = storageIDs;
			this.storageTypes = storageTypes;
			if (cachedLocs == null || cachedLocs.Length == 0)
			{
				this.cachedLocs = EmptyLocs;
			}
			else
			{
				this.cachedLocs = cachedLocs;
			}
		}

		public virtual Org.Apache.Hadoop.Security.Token.Token<BlockTokenIdentifier> GetBlockToken
			()
		{
			return blockToken;
		}

		public virtual void SetBlockToken(Org.Apache.Hadoop.Security.Token.Token<BlockTokenIdentifier
			> token)
		{
			this.blockToken = token;
		}

		public virtual ExtendedBlock GetBlock()
		{
			return b;
		}

		/// <summary>Returns the locations associated with this block.</summary>
		/// <remarks>
		/// Returns the locations associated with this block. The returned array is not
		/// expected to be modified. If it is, caller must immediately invoke
		/// <see cref="UpdateCachedStorageInfo()"/>
		/// to update the cached Storage ID/Type arrays.
		/// </remarks>
		public virtual DatanodeInfo[] GetLocations()
		{
			return locs;
		}

		public virtual StorageType[] GetStorageTypes()
		{
			return storageTypes;
		}

		public virtual string[] GetStorageIDs()
		{
			return storageIDs;
		}

		/// <summary>Updates the cached StorageID and StorageType information.</summary>
		/// <remarks>
		/// Updates the cached StorageID and StorageType information. Must be
		/// called when the locations array is modified.
		/// </remarks>
		public virtual void UpdateCachedStorageInfo()
		{
			if (storageIDs != null)
			{
				for (int i = 0; i < locs.Length; i++)
				{
					storageIDs[i] = locs[i].GetStorageID();
				}
			}
			if (storageTypes != null)
			{
				for (int i = 0; i < locs.Length; i++)
				{
					storageTypes[i] = locs[i].GetStorageType();
				}
			}
		}

		public virtual long GetStartOffset()
		{
			return offset;
		}

		public virtual long GetBlockSize()
		{
			return b.GetNumBytes();
		}

		internal virtual void SetStartOffset(long value)
		{
			this.offset = value;
		}

		internal virtual void SetCorrupt(bool corrupt)
		{
			this.corrupt = corrupt;
		}

		public virtual bool IsCorrupt()
		{
			return this.corrupt;
		}

		/// <summary>Add a the location of a cached replica of the block.</summary>
		/// <param name="loc">of datanode with the cached replica</param>
		public virtual void AddCachedLoc(DatanodeInfo loc)
		{
			IList<DatanodeInfo> cachedList = Lists.NewArrayList(cachedLocs);
			if (cachedList.Contains(loc))
			{
				return;
			}
			// Try to re-use a DatanodeInfo already in loc
			foreach (DatanodeInfoWithStorage di in locs)
			{
				if (loc.Equals(di))
				{
					cachedList.AddItem(di);
					cachedLocs = Sharpen.Collections.ToArray(cachedList, cachedLocs);
					return;
				}
			}
			// Not present in loc, add it and go
			cachedList.AddItem(loc);
			cachedLocs = Sharpen.Collections.ToArray(cachedList, cachedLocs);
		}

		/// <returns>Datanodes with a cached block replica</returns>
		public virtual DatanodeInfo[] GetCachedLocations()
		{
			return cachedLocs;
		}

		public override string ToString()
		{
			return GetType().Name + "{" + b + "; getBlockSize()=" + GetBlockSize() + "; corrupt="
				 + corrupt + "; offset=" + offset + "; locs=" + Arrays.AsList(locs) + "}";
		}
	}
}
