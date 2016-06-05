using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Blockmanagement
{
	/// <summary>
	/// A immutable object that stores the number of live replicas and
	/// the number of decommissined Replicas.
	/// </summary>
	public class NumberReplicas
	{
		private int liveReplicas;

		private int decommissionedReplicas;

		private int corruptReplicas;

		private int excessReplicas;

		private int replicasOnStaleNodes;

		internal NumberReplicas()
		{
			Initialize(0, 0, 0, 0, 0);
		}

		internal NumberReplicas(int live, int decommissioned, int corrupt, int excess, int
			 stale)
		{
			Initialize(live, decommissioned, corrupt, excess, stale);
		}

		internal virtual void Initialize(int live, int decommissioned, int corrupt, int excess
			, int stale)
		{
			liveReplicas = live;
			decommissionedReplicas = decommissioned;
			corruptReplicas = corrupt;
			excessReplicas = excess;
			replicasOnStaleNodes = stale;
		}

		public virtual int LiveReplicas()
		{
			return liveReplicas;
		}

		public virtual int DecommissionedReplicas()
		{
			return decommissionedReplicas;
		}

		public virtual int CorruptReplicas()
		{
			return corruptReplicas;
		}

		public virtual int ExcessReplicas()
		{
			return excessReplicas;
		}

		/// <returns>
		/// the number of replicas which are on stale nodes.
		/// This is not mutually exclusive with the other counts -- ie a
		/// replica may count as both "live" and "stale".
		/// </returns>
		public virtual int ReplicasOnStaleNodes()
		{
			return replicasOnStaleNodes;
		}
	}
}
