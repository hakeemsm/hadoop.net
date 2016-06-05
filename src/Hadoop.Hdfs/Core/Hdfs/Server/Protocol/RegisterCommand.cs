using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Protocol
{
	/// <summary>A BlockCommand is an instruction to a datanode to register with the namenode.
	/// 	</summary>
	/// <remarks>
	/// A BlockCommand is an instruction to a datanode to register with the namenode.
	/// This command can't be combined with other commands in the same response.
	/// This is because after the datanode processes RegisterCommand, it will skip
	/// the rest of the DatanodeCommands in the same HeartbeatResponse.
	/// </remarks>
	public class RegisterCommand : DatanodeCommand
	{
		public static readonly DatanodeCommand Register = new Org.Apache.Hadoop.Hdfs.Server.Protocol.RegisterCommand
			();

		public RegisterCommand()
			: base(DatanodeProtocol.DnaRegister)
		{
		}
	}
}
