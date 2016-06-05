using System.Collections.Generic;
using Com.Google.Common.Base;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Hdfs.Server.Blockmanagement;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	/// <summary>
	/// Storing all the
	/// <see cref="INode"/>
	/// s and maintaining the mapping between INode ID
	/// and INode.
	/// </summary>
	public class INodeMap
	{
		internal static Org.Apache.Hadoop.Hdfs.Server.Namenode.INodeMap NewInstance(INodeDirectory
			 rootDir)
		{
			// Compute the map capacity by allocating 1% of total memory
			int capacity = LightWeightGSet.ComputeCapacity(1, "INodeMap");
			GSet<INode, INodeWithAdditionalFields> map = new LightWeightGSet<INode, INodeWithAdditionalFields
				>(capacity);
			map.Put(rootDir);
			return new Org.Apache.Hadoop.Hdfs.Server.Namenode.INodeMap(map);
		}

		/// <summary>Synchronized by external lock.</summary>
		private readonly GSet<INode, INodeWithAdditionalFields> map;

		public virtual IEnumerator<INodeWithAdditionalFields> GetMapIterator()
		{
			return map.GetEnumerator();
		}

		private INodeMap(GSet<INode, INodeWithAdditionalFields> map)
		{
			Preconditions.CheckArgument(map != null);
			this.map = map;
		}

		/// <summary>
		/// Add an
		/// <see cref="INode"/>
		/// into the
		/// <see cref="INode"/>
		/// map. Replace the old value if
		/// necessary.
		/// </summary>
		/// <param name="inode">
		/// The
		/// <see cref="INode"/>
		/// to be added to the map.
		/// </param>
		public void Put(INode inode)
		{
			if (inode is INodeWithAdditionalFields)
			{
				map.Put((INodeWithAdditionalFields)inode);
			}
		}

		/// <summary>
		/// Remove a
		/// <see cref="INode"/>
		/// from the map.
		/// </summary>
		/// <param name="inode">
		/// The
		/// <see cref="INode"/>
		/// to be removed.
		/// </param>
		public void Remove(INode inode)
		{
			map.Remove(inode);
		}

		/// <returns>The size of the map.</returns>
		public virtual int Size()
		{
			return map.Size();
		}

		/// <summary>
		/// Get the
		/// <see cref="INode"/>
		/// with the given id from the map.
		/// </summary>
		/// <param name="id">
		/// ID of the
		/// <see cref="INode"/>
		/// .
		/// </param>
		/// <returns>
		/// The
		/// <see cref="INode"/>
		/// in the map with the given id. Return null if no
		/// such
		/// <see cref="INode"/>
		/// in the map.
		/// </returns>
		public virtual INode Get(long id)
		{
			INode inode = new _INodeWithAdditionalFields_92(id, null, new PermissionStatus(string.Empty
				, string.Empty, new FsPermission((short)0)), 0, 0);
			// Nothing to do
			return map.Get(inode);
		}

		private sealed class _INodeWithAdditionalFields_92 : INodeWithAdditionalFields
		{
			public _INodeWithAdditionalFields_92(long baseArg1, byte[] baseArg2, PermissionStatus
				 baseArg3, long baseArg4, long baseArg5)
				: base(baseArg1, baseArg2, baseArg3, baseArg4, baseArg5)
			{
			}

			internal override void RecordModification(int latestSnapshotId)
			{
			}

			public override void DestroyAndCollectBlocks(BlockStoragePolicySuite bsps, INode.BlocksMapUpdateInfo
				 collectedBlocks, IList<INode> removedINodes)
			{
			}

			public override QuotaCounts ComputeQuotaUsage(BlockStoragePolicySuite bsps, byte 
				blockStoragePolicyId, QuotaCounts counts, bool useCache, int lastSnapshotId)
			{
				return null;
			}

			public override ContentSummaryComputationContext ComputeContentSummary(ContentSummaryComputationContext
				 summary)
			{
				return null;
			}

			public override QuotaCounts CleanSubtree(BlockStoragePolicySuite bsps, int snapshotId
				, int priorSnapshotId, INode.BlocksMapUpdateInfo collectedBlocks, IList<INode> removedINodes
				)
			{
				return null;
			}

			public override byte GetStoragePolicyID()
			{
				return BlockStoragePolicySuite.IdUnspecified;
			}

			public override byte GetLocalStoragePolicyID()
			{
				return BlockStoragePolicySuite.IdUnspecified;
			}
		}

		/// <summary>
		/// Clear the
		/// <see cref="map"/>
		/// </summary>
		public virtual void Clear()
		{
			map.Clear();
		}
	}
}
