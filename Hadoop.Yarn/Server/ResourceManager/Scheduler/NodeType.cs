using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler
{
	/// <summary>Resource classification.</summary>
	[System.Serializable]
	public sealed class NodeType
	{
		public static readonly Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.NodeType
			 NodeLocal = new Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.NodeType
			(0);

		public static readonly Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.NodeType
			 RackLocal = new Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.NodeType
			(1);

		public static readonly Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.NodeType
			 OffSwitch = new Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.NodeType
			(2);

		public int index;

		private NodeType(int index)
		{
			this.index = index;
		}
	}
}
