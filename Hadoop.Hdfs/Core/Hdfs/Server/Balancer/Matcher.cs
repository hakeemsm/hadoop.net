using Org.Apache.Hadoop.Net;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Balancer
{
	/// <summary>A matcher interface for matching nodes.</summary>
	public abstract class Matcher
	{
		/// <summary>Given the cluster topology, does the left node match the right node?</summary>
		public abstract bool Match(NetworkTopology cluster, Node left, Node right);

		private sealed class _Matcher_29 : Matcher
		{
			public _Matcher_29()
			{
			}

			public override bool Match(NetworkTopology cluster, Node left, Node right)
			{
				return cluster.IsOnSameNodeGroup(left, right);
			}

			public override string ToString()
			{
				return "SAME_NODE_GROUP";
			}
		}

		/// <summary>Match datanodes in the same node group.</summary>
		public const Matcher SameNodeGroup = new _Matcher_29();

		private sealed class _Matcher_42 : Matcher
		{
			public _Matcher_42()
			{
			}

			public override bool Match(NetworkTopology cluster, Node left, Node right)
			{
				return cluster.IsOnSameRack(left, right);
			}

			public override string ToString()
			{
				return "SAME_RACK";
			}
		}

		/// <summary>Match datanodes in the same rack.</summary>
		public const Matcher SameRack = new _Matcher_42();

		private sealed class _Matcher_55 : Matcher
		{
			public _Matcher_55()
			{
			}

			public override bool Match(NetworkTopology cluster, Node left, Node right)
			{
				return left != right;
			}

			public override string ToString()
			{
				return "ANY_OTHER";
			}
		}

		/// <summary>Match any datanode with any other datanode.</summary>
		public const Matcher AnyOther = new _Matcher_55();
	}

	public static class MatcherConstants
	{
	}
}
