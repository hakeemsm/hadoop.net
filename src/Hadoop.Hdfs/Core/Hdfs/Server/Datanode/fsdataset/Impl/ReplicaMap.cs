using System;
using System.Collections.Generic;
using Org.Apache.Hadoop;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Datanode;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Datanode.Fsdataset.Impl
{
	/// <summary>Maintains the replica map.</summary>
	internal class ReplicaMap
	{
		private readonly object mutex;

		private readonly IDictionary<string, IDictionary<long, ReplicaInfo>> map = new Dictionary
			<string, IDictionary<long, ReplicaInfo>>();

		internal ReplicaMap(object mutex)
		{
			// Object using which this class is synchronized
			// Map of block pool Id to another map of block Id to ReplicaInfo.
			if (mutex == null)
			{
				throw new HadoopIllegalArgumentException("Object to synchronize on cannot be null"
					);
			}
			this.mutex = mutex;
		}

		internal virtual string[] GetBlockPoolList()
		{
			lock (mutex)
			{
				return Sharpen.Collections.ToArray(map.Keys, new string[map.Keys.Count]);
			}
		}

		private void CheckBlockPool(string bpid)
		{
			if (bpid == null)
			{
				throw new ArgumentException("Block Pool Id is null");
			}
		}

		private void CheckBlock(Block b)
		{
			if (b == null)
			{
				throw new ArgumentException("Block is null");
			}
		}

		/// <summary>
		/// Get the meta information of the replica that matches both block id
		/// and generation stamp
		/// </summary>
		/// <param name="bpid">block pool id</param>
		/// <param name="block">block with its id as the key</param>
		/// <returns>the replica's meta information</returns>
		/// <exception cref="System.ArgumentException">if the input block or block pool is null
		/// 	</exception>
		internal virtual ReplicaInfo Get(string bpid, Block block)
		{
			CheckBlockPool(bpid);
			CheckBlock(block);
			ReplicaInfo replicaInfo = Get(bpid, block.GetBlockId());
			if (replicaInfo != null && block.GetGenerationStamp() == replicaInfo.GetGenerationStamp
				())
			{
				return replicaInfo;
			}
			return null;
		}

		/// <summary>Get the meta information of the replica that matches the block id</summary>
		/// <param name="bpid">block pool id</param>
		/// <param name="blockId">a block's id</param>
		/// <returns>the replica's meta information</returns>
		internal virtual ReplicaInfo Get(string bpid, long blockId)
		{
			CheckBlockPool(bpid);
			lock (mutex)
			{
				IDictionary<long, ReplicaInfo> m = map[bpid];
				return m != null ? m[blockId] : null;
			}
		}

		/// <summary>Add a replica's meta information into the map</summary>
		/// <param name="bpid">block pool id</param>
		/// <param name="replicaInfo">a replica's meta information</param>
		/// <returns>previous meta information of the replica</returns>
		/// <exception cref="System.ArgumentException">if the input parameter is null</exception>
		internal virtual ReplicaInfo Add(string bpid, ReplicaInfo replicaInfo)
		{
			CheckBlockPool(bpid);
			CheckBlock(replicaInfo);
			lock (mutex)
			{
				IDictionary<long, ReplicaInfo> m = map[bpid];
				if (m == null)
				{
					// Add an entry for block pool if it does not exist already
					m = new Dictionary<long, ReplicaInfo>();
					map[bpid] = m;
				}
				return m[replicaInfo.GetBlockId()] = replicaInfo;
			}
		}

		/// <summary>Add all entries from the given replica map into the local replica map.</summary>
		internal virtual void AddAll(Org.Apache.Hadoop.Hdfs.Server.Datanode.Fsdataset.Impl.ReplicaMap
			 other)
		{
			map.PutAll(other.map);
		}

		/// <summary>
		/// Remove the replica's meta information from the map that matches
		/// the input block's id and generation stamp
		/// </summary>
		/// <param name="bpid">block pool id</param>
		/// <param name="block">block with its id as the key</param>
		/// <returns>the removed replica's meta information</returns>
		/// <exception cref="System.ArgumentException">if the input block is null</exception>
		internal virtual ReplicaInfo Remove(string bpid, Block block)
		{
			CheckBlockPool(bpid);
			CheckBlock(block);
			lock (mutex)
			{
				IDictionary<long, ReplicaInfo> m = map[bpid];
				if (m != null)
				{
					long key = Sharpen.Extensions.ValueOf(block.GetBlockId());
					ReplicaInfo replicaInfo = m[key];
					if (replicaInfo != null && block.GetGenerationStamp() == replicaInfo.GetGenerationStamp
						())
					{
						return Sharpen.Collections.Remove(m, key);
					}
				}
			}
			return null;
		}

		/// <summary>Remove the replica's meta information from the map if present</summary>
		/// <param name="bpid">block pool id</param>
		/// <param name="blockId">block id of the replica to be removed</param>
		/// <returns>the removed replica's meta information</returns>
		internal virtual ReplicaInfo Remove(string bpid, long blockId)
		{
			CheckBlockPool(bpid);
			lock (mutex)
			{
				IDictionary<long, ReplicaInfo> m = map[bpid];
				if (m != null)
				{
					return Sharpen.Collections.Remove(m, blockId);
				}
			}
			return null;
		}

		/// <summary>Get the size of the map for given block pool</summary>
		/// <param name="bpid">block pool id</param>
		/// <returns>the number of replicas in the map</returns>
		internal virtual int Size(string bpid)
		{
			IDictionary<long, ReplicaInfo> m = null;
			lock (mutex)
			{
				m = map[bpid];
				return m != null ? m.Count : 0;
			}
		}

		/// <summary>
		/// Get a collection of the replicas for given block pool
		/// This method is <b>not synchronized</b>.
		/// </summary>
		/// <remarks>
		/// Get a collection of the replicas for given block pool
		/// This method is <b>not synchronized</b>. It needs to be synchronized
		/// externally using the mutex, both for getting the replicas
		/// values from the map and iterating over it. Mutex can be accessed using
		/// <see cref="GetMutext()"/>
		/// method.
		/// </remarks>
		/// <param name="bpid">block pool id</param>
		/// <returns>a collection of the replicas belonging to the block pool</returns>
		internal virtual ICollection<ReplicaInfo> Replicas(string bpid)
		{
			IDictionary<long, ReplicaInfo> m = null;
			m = map[bpid];
			return m != null ? m.Values : null;
		}

		internal virtual void InitBlockPool(string bpid)
		{
			CheckBlockPool(bpid);
			lock (mutex)
			{
				IDictionary<long, ReplicaInfo> m = map[bpid];
				if (m == null)
				{
					// Add an entry for block pool if it does not exist already
					m = new Dictionary<long, ReplicaInfo>();
					map[bpid] = m;
				}
			}
		}

		internal virtual void CleanUpBlockPool(string bpid)
		{
			CheckBlockPool(bpid);
			lock (mutex)
			{
				Sharpen.Collections.Remove(map, bpid);
			}
		}

		/// <summary>Give access to mutex used for synchronizing ReplicasMap</summary>
		/// <returns>object used as lock</returns>
		internal virtual object GetMutext()
		{
			return mutex;
		}
	}
}
