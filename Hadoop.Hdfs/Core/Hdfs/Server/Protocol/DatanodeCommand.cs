using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Protocol
{
	/// <summary>Base class for data-node command.</summary>
	/// <remarks>
	/// Base class for data-node command.
	/// Issued by the name-node to notify data-nodes what should be done.
	/// </remarks>
	public abstract class DatanodeCommand : ServerCommand
	{
		internal DatanodeCommand(int action)
			: base(action)
		{
		}
	}
}
