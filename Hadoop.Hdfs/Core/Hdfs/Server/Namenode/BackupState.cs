using System.IO;
using Org.Apache.Hadoop.HA;
using Org.Apache.Hadoop.Hdfs.Server.Namenode.HA;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	public class BackupState : HAState
	{
		public BackupState()
			: base(HAServiceProtocol.HAServiceState.Standby)
		{
		}

		/// <exception cref="Org.Apache.Hadoop.Ipc.StandbyException"/>
		public override void CheckOperation(HAContext context, NameNode.OperationCategory
			 op)
		{
			// HAState
			context.CheckOperation(op);
		}

		public override bool ShouldPopulateReplQueues()
		{
			// HAState
			return false;
		}

		/// <exception cref="Org.Apache.Hadoop.HA.ServiceFailedException"/>
		public override void EnterState(HAContext context)
		{
			// HAState
			try
			{
				context.StartActiveServices();
			}
			catch (IOException e)
			{
				throw new ServiceFailedException("Failed to start backup services", e);
			}
		}

		/// <exception cref="Org.Apache.Hadoop.HA.ServiceFailedException"/>
		public override void ExitState(HAContext context)
		{
			// HAState
			try
			{
				context.StopActiveServices();
			}
			catch (IOException e)
			{
				throw new ServiceFailedException("Failed to stop backup services", e);
			}
		}

		/// <exception cref="Org.Apache.Hadoop.HA.ServiceFailedException"/>
		public override void PrepareToExitState(HAContext context)
		{
			// HAState
			context.PrepareToStopStandbyServices();
		}
	}
}
