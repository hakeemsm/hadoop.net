using Org.Apache.Hadoop.Hdfs.Protocol;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Protocol
{
	public class HeartbeatResponse
	{
		/// <summary>Commands returned from the namenode to the datanode</summary>
		private readonly DatanodeCommand[] commands;

		/// <summary>Information about the current HA-related state of the NN</summary>
		private readonly NNHAStatusHeartbeat haStatus;

		private readonly RollingUpgradeStatus rollingUpdateStatus;

		public HeartbeatResponse(DatanodeCommand[] cmds, NNHAStatusHeartbeat haStatus, RollingUpgradeStatus
			 rollingUpdateStatus)
		{
			commands = cmds;
			this.haStatus = haStatus;
			this.rollingUpdateStatus = rollingUpdateStatus;
		}

		public virtual DatanodeCommand[] GetCommands()
		{
			return commands;
		}

		public virtual NNHAStatusHeartbeat GetNameNodeHaState()
		{
			return haStatus;
		}

		public virtual RollingUpgradeStatus GetRollingUpdateStatus()
		{
			return rollingUpdateStatus;
		}
	}
}
