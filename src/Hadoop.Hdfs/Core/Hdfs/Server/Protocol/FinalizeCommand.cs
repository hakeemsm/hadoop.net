using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Protocol
{
	/// <summary>A BlockCommand is an instruction to a datanode to register with the namenode.
	/// 	</summary>
	public class FinalizeCommand : DatanodeCommand
	{
		internal string blockPoolId;

		private FinalizeCommand()
			: base(DatanodeProtocol.DnaFinalize)
		{
		}

		public FinalizeCommand(string bpid)
			: base(DatanodeProtocol.DnaFinalize)
		{
			blockPoolId = bpid;
		}

		public virtual string GetBlockPoolId()
		{
			return blockPoolId;
		}
	}
}
