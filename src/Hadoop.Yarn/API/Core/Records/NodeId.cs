using Org.Apache.Hadoop.Classification;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Records
{
	/// <summary>
	/// <p><code>NodeId</code> is the unique identifier for a node.</p>
	/// <p>It includes the <em>hostname</em> and <em>port</em> to uniquely
	/// identify the node.
	/// </summary>
	/// <remarks>
	/// <p><code>NodeId</code> is the unique identifier for a node.</p>
	/// <p>It includes the <em>hostname</em> and <em>port</em> to uniquely
	/// identify the node. Thus, it is unique across restarts of any
	/// <code>NodeManager</code>.</p>
	/// </remarks>
	public abstract class NodeId : Comparable<NodeId>
	{
		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public static NodeId NewInstance(string host, int port)
		{
			NodeId nodeId = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<NodeId>();
			nodeId.SetHost(host);
			nodeId.SetPort(port);
			nodeId.Build();
			return nodeId;
		}

		/// <summary>Get the <em>hostname</em> of the node.</summary>
		/// <returns><em>hostname</em> of the node</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract string GetHost();

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		protected internal abstract void SetHost(string host);

		/// <summary>Get the <em>port</em> for communicating with the node.</summary>
		/// <returns><em>port</em> for communicating with the node</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract int GetPort();

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		protected internal abstract void SetPort(int port);

		public override string ToString()
		{
			return this.GetHost() + ":" + this.GetPort();
		}

		public override int GetHashCode()
		{
			int prime = 493217;
			int result = 8501;
			result = prime * result + this.GetHost().GetHashCode();
			result = prime * result + this.GetPort();
			return result;
		}

		public override bool Equals(object obj)
		{
			if (this == obj)
			{
				return true;
			}
			if (obj == null)
			{
				return false;
			}
			if (GetType() != obj.GetType())
			{
				return false;
			}
			NodeId other = (NodeId)obj;
			if (!this.GetHost().Equals(other.GetHost()))
			{
				return false;
			}
			if (this.GetPort() != other.GetPort())
			{
				return false;
			}
			return true;
		}

		public virtual int CompareTo(NodeId other)
		{
			int hostCompare = string.CompareOrdinal(this.GetHost(), other.GetHost());
			if (hostCompare == 0)
			{
				if (this.GetPort() > other.GetPort())
				{
					return 1;
				}
				else
				{
					if (this.GetPort() < other.GetPort())
					{
						return -1;
					}
				}
				return 0;
			}
			return hostCompare;
		}

		protected internal abstract void Build();
	}
}
