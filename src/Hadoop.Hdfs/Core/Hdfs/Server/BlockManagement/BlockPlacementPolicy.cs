using System;
using System.Collections.Generic;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Blockmanagement
{
	/// <summary>
	/// This interface is used for choosing the desired number of targets
	/// for placing block replicas.
	/// </summary>
	public abstract class BlockPlacementPolicy
	{
		internal static readonly Log Log = LogFactory.GetLog(typeof(BlockPlacementPolicy)
			);

		[System.Serializable]
		public class NotEnoughReplicasException : Exception
		{
			private const long serialVersionUID = 1L;

			internal NotEnoughReplicasException(string msg)
				: base(msg)
			{
			}
		}

		/// <summary>
		/// choose <i>numOfReplicas</i> data nodes for <i>writer</i>
		/// to re-replicate a block with size <i>blocksize</i>
		/// If not, return as many as we can.
		/// </summary>
		/// <param name="srcPath">the file to which this chooseTargets is being invoked.</param>
		/// <param name="numOfReplicas">additional number of replicas wanted.</param>
		/// <param name="writer">the writer's machine, null if not in the cluster.</param>
		/// <param name="chosen">datanodes that have been chosen as targets.</param>
		/// <param name="returnChosenNodes">decide if the chosenNodes are returned.</param>
		/// <param name="excludedNodes">datanodes that should not be considered as targets.</param>
		/// <param name="blocksize">size of the data to be written.</param>
		/// <returns>
		/// array of DatanodeDescriptor instances chosen as target
		/// and sorted as a pipeline.
		/// </returns>
		public abstract DatanodeStorageInfo[] ChooseTarget(string srcPath, int numOfReplicas
			, Node writer, IList<DatanodeStorageInfo> chosen, bool returnChosenNodes, ICollection
			<Node> excludedNodes, long blocksize, BlockStoragePolicy storagePolicy);

		/// <summary>
		/// Same as
		/// <see cref="ChooseTarget(string, int, Org.Apache.Hadoop.Net.Node, System.Collections.Generic.ICollection{E}, long, System.Collections.Generic.IList{E}, Org.Apache.Hadoop.Hdfs.Protocol.BlockStoragePolicy)
		/// 	"/>
		/// with added parameter
		/// <c>favoredDatanodes</c>
		/// </summary>
		/// <param name="favoredNodes">
		/// datanodes that should be favored as targets. This
		/// is only a hint and due to cluster state, namenode may not be
		/// able to place the blocks on these datanodes.
		/// </param>
		internal virtual DatanodeStorageInfo[] ChooseTarget(string src, int numOfReplicas
			, Node writer, ICollection<Node> excludedNodes, long blocksize, IList<DatanodeDescriptor
			> favoredNodes, BlockStoragePolicy storagePolicy)
		{
			// This class does not provide the functionality of placing
			// a block in favored datanodes. The implementations of this class
			// are expected to provide this functionality
			return ChooseTarget(src, numOfReplicas, writer, new AList<DatanodeStorageInfo>(numOfReplicas
				), false, excludedNodes, blocksize, storagePolicy);
		}

		/// <summary>
		/// Verify if the block's placement meets requirement of placement policy,
		/// i.e.
		/// </summary>
		/// <remarks>
		/// Verify if the block's placement meets requirement of placement policy,
		/// i.e. replicas are placed on no less than minRacks racks in the system.
		/// </remarks>
		/// <param name="srcPath">the full pathname of the file to be verified</param>
		/// <param name="lBlk">block with locations</param>
		/// <param name="numOfReplicas">replica number of file to be verified</param>
		/// <returns>the result of verification</returns>
		public abstract BlockPlacementStatus VerifyBlockPlacement(string srcPath, LocatedBlock
			 lBlk, int numOfReplicas);

		/// <summary>
		/// Decide whether deleting the specified replica of the block still makes
		/// the block conform to the configured block placement policy.
		/// </summary>
		/// <param name="srcBC">block collection of file to which block-to-be-deleted belongs
		/// 	</param>
		/// <param name="block">The block to be deleted</param>
		/// <param name="replicationFactor">The required number of replicas for this block</param>
		/// <param name="moreThanOne">
		/// The replica locations of this block that are present
		/// on more than one unique racks.
		/// </param>
		/// <param name="exactlyOne">
		/// Replica locations of this block that  are present
		/// on exactly one unique racks.
		/// </param>
		/// <param name="excessTypes">
		/// The excess
		/// <see cref="Org.Apache.Hadoop.FS.StorageType"/>
		/// s according to the
		/// <see cref="Org.Apache.Hadoop.Hdfs.Protocol.BlockStoragePolicy"/>
		/// .
		/// </param>
		/// <returns>the replica that is the best candidate for deletion</returns>
		public abstract DatanodeStorageInfo ChooseReplicaToDelete(BlockCollection srcBC, 
			Block block, short replicationFactor, ICollection<DatanodeStorageInfo> moreThanOne
			, ICollection<DatanodeStorageInfo> exactlyOne, IList<StorageType> excessTypes);

		/// <summary>Used to setup a BlockPlacementPolicy object.</summary>
		/// <remarks>
		/// Used to setup a BlockPlacementPolicy object. This should be defined by
		/// all implementations of a BlockPlacementPolicy.
		/// </remarks>
		/// <param name="conf">the configuration object</param>
		/// <param name="stats">retrieve cluster status from here</param>
		/// <param name="clusterMap">cluster topology</param>
		protected internal abstract void Initialize(Configuration conf, FSClusterStats stats
			, NetworkTopology clusterMap, Host2NodesMap host2datanodeMap);

		/// <summary>
		/// Get an instance of the configured Block Placement Policy based on the
		/// the configuration property
		/// <see cref="Org.Apache.Hadoop.Hdfs.DFSConfigKeys.DfsBlockReplicatorClassnameKey"/>
		/// .
		/// </summary>
		/// <param name="conf">the configuration to be used</param>
		/// <param name="stats">an object that is used to retrieve the load on the cluster</param>
		/// <param name="clusterMap">the network topology of the cluster</param>
		/// <returns>an instance of BlockPlacementPolicy</returns>
		public static BlockPlacementPolicy GetInstance(Configuration conf, FSClusterStats
			 stats, NetworkTopology clusterMap, Host2NodesMap host2datanodeMap)
		{
			Type replicatorClass = conf.GetClass<BlockPlacementPolicy>(DFSConfigKeys.DfsBlockReplicatorClassnameKey
				, DFSConfigKeys.DfsBlockReplicatorClassnameDefault);
			BlockPlacementPolicy replicator = ReflectionUtils.NewInstance(replicatorClass, conf
				);
			replicator.Initialize(conf, stats, clusterMap, host2datanodeMap);
			return replicator;
		}

		/// <summary>Adjust rackmap, moreThanOne, and exactlyOne after removing replica on cur.
		/// 	</summary>
		/// <param name="rackMap">a map from rack to replica</param>
		/// <param name="moreThanOne">
		/// The List of replica nodes on rack which has more than
		/// one replica
		/// </param>
		/// <param name="exactlyOne">The List of replica nodes on rack with only one replica</param>
		/// <param name="cur">current replica to remove</param>
		public virtual void AdjustSetsWithChosenReplica(IDictionary<string, IList<DatanodeStorageInfo
			>> rackMap, IList<DatanodeStorageInfo> moreThanOne, IList<DatanodeStorageInfo> exactlyOne
			, DatanodeStorageInfo cur)
		{
			string rack = GetRack(cur.GetDatanodeDescriptor());
			IList<DatanodeStorageInfo> storages = rackMap[rack];
			storages.Remove(cur);
			if (storages.IsEmpty())
			{
				Sharpen.Collections.Remove(rackMap, rack);
			}
			if (moreThanOne.Remove(cur))
			{
				if (storages.Count == 1)
				{
					DatanodeStorageInfo remaining = storages[0];
					moreThanOne.Remove(remaining);
					exactlyOne.AddItem(remaining);
				}
			}
			else
			{
				exactlyOne.Remove(cur);
			}
		}

		/// <summary>Get rack string from a data node</summary>
		/// <returns>rack of data node</returns>
		protected internal virtual string GetRack(DatanodeInfo datanode)
		{
			return datanode.GetNetworkLocation();
		}

		/// <summary>
		/// Split data nodes into two sets, one set includes nodes on rack with
		/// more than one  replica, the other set contains the remaining nodes.
		/// </summary>
		/// <param name="dataNodes">datanodes to be split into two sets</param>
		/// <param name="rackMap">a map from rack to datanodes</param>
		/// <param name="moreThanOne">contains nodes on rack with more than one replica</param>
		/// <param name="exactlyOne">remains contains the remaining nodes</param>
		public virtual void SplitNodesWithRack(IEnumerable<DatanodeStorageInfo> storages, 
			IDictionary<string, IList<DatanodeStorageInfo>> rackMap, IList<DatanodeStorageInfo
			> moreThanOne, IList<DatanodeStorageInfo> exactlyOne)
		{
			foreach (DatanodeStorageInfo s in storages)
			{
				string rackName = GetRack(s.GetDatanodeDescriptor());
				IList<DatanodeStorageInfo> storageList = rackMap[rackName];
				if (storageList == null)
				{
					storageList = new AList<DatanodeStorageInfo>();
					rackMap[rackName] = storageList;
				}
				storageList.AddItem(s);
			}
			// split nodes into two sets
			foreach (IList<DatanodeStorageInfo> storageList_1 in rackMap.Values)
			{
				if (storageList_1.Count == 1)
				{
					// exactlyOne contains nodes on rack with only one replica
					exactlyOne.AddItem(storageList_1[0]);
				}
				else
				{
					// moreThanOne contains nodes on rack with more than one replica
					Sharpen.Collections.AddAll(moreThanOne, storageList_1);
				}
			}
		}
	}
}
