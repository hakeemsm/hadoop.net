using System.IO;
using Org.Apache.Hadoop.HA;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode.HA
{
	/// <summary>Active state of the namenode.</summary>
	/// <remarks>
	/// Active state of the namenode. In this state, namenode provides the namenode
	/// service and handles operations of type
	/// <see cref="Org.Apache.Hadoop.Hdfs.Server.Namenode.NameNode.OperationCategory.Write
	/// 	"/>
	/// and
	/// <see cref="Org.Apache.Hadoop.Hdfs.Server.Namenode.NameNode.OperationCategory.Read
	/// 	"/>
	/// .
	/// </remarks>
	public class ActiveState : HAState
	{
		public ActiveState()
			: base(HAServiceProtocol.HAServiceState.Active)
		{
		}

		public override void CheckOperation(HAContext context, NameNode.OperationCategory
			 op)
		{
			return;
		}

		// All operations are allowed in active state
		public override bool ShouldPopulateReplQueues()
		{
			return true;
		}

		/// <exception cref="Org.Apache.Hadoop.HA.ServiceFailedException"/>
		public override void SetState(HAContext context, HAState s)
		{
			if (s == NameNode.StandbyState)
			{
				SetStateInternal(context, s);
				return;
			}
			base.SetState(context, s);
		}

		/// <exception cref="Org.Apache.Hadoop.HA.ServiceFailedException"/>
		public override void EnterState(HAContext context)
		{
			try
			{
				context.StartActiveServices();
			}
			catch (IOException e)
			{
				throw new ServiceFailedException("Failed to start active services", e);
			}
		}

		/// <exception cref="Org.Apache.Hadoop.HA.ServiceFailedException"/>
		public override void ExitState(HAContext context)
		{
			try
			{
				context.StopActiveServices();
			}
			catch (IOException e)
			{
				throw new ServiceFailedException("Failed to stop active services", e);
			}
		}
	}
}
