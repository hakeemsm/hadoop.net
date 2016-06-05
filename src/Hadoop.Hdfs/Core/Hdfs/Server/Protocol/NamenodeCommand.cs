using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Protocol
{
	/// <summary>Base class for name-node command.</summary>
	/// <remarks>
	/// Base class for name-node command.
	/// Issued by the name-node to notify other name-nodes what should be done.
	/// </remarks>
	public class NamenodeCommand : ServerCommand
	{
		public NamenodeCommand(int action)
			: base(action)
		{
		}
	}
}
