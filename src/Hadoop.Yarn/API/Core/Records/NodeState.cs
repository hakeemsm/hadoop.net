using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Records
{
	/// <summary><p>State of a <code>Node</code>.</p></summary>
	[System.Serializable]
	public sealed class NodeState
	{
		/// <summary>New node</summary>
		public static readonly NodeState New = new NodeState();

		/// <summary>Running node</summary>
		public static readonly NodeState Running = new NodeState();

		/// <summary>Node is unhealthy</summary>
		public static readonly NodeState Unhealthy = new NodeState();

		/// <summary>Node is out of service</summary>
		public static readonly NodeState Decommissioned = new NodeState();

		/// <summary>Node has not sent a heartbeat for some configured time threshold</summary>
		public static readonly NodeState Lost = new NodeState();

		/// <summary>Node has rebooted</summary>
		public static readonly NodeState Rebooted = new NodeState();

		public bool IsUnusable()
		{
			return (this == NodeState.Unhealthy || this == NodeState.Decommissioned || this ==
				 NodeState.Lost);
		}
	}
}
