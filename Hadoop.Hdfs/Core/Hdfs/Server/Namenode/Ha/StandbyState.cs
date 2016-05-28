using System.IO;
using Org.Apache.Hadoop.HA;
using Org.Apache.Hadoop.Hdfs.Server.Namenode;
using Org.Apache.Hadoop.Ipc;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode.HA
{
	/// <summary>Namenode standby state.</summary>
	/// <remarks>
	/// Namenode standby state. In this state the namenode acts as warm standby and
	/// keeps the following updated:
	/// <ul>
	/// <li>Namespace by getting the edits.</li>
	/// <li>Block location information by receiving block reports and blocks
	/// received from the datanodes.</li>
	/// </ul>
	/// It does not handle read/write/checkpoint operations.
	/// </remarks>
	public class StandbyState : HAState
	{
		public StandbyState()
			: base(HAServiceProtocol.HAServiceState.Standby)
		{
		}

		/// <exception cref="Org.Apache.Hadoop.HA.ServiceFailedException"/>
		public override void SetState(HAContext context, HAState s)
		{
			if (s == NameNode.ActiveState)
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
				context.StartStandbyServices();
			}
			catch (IOException e)
			{
				throw new ServiceFailedException("Failed to start standby services", e);
			}
		}

		/// <exception cref="Org.Apache.Hadoop.HA.ServiceFailedException"/>
		public override void PrepareToExitState(HAContext context)
		{
			context.PrepareToStopStandbyServices();
		}

		/// <exception cref="Org.Apache.Hadoop.HA.ServiceFailedException"/>
		public override void ExitState(HAContext context)
		{
			try
			{
				context.StopStandbyServices();
			}
			catch (IOException e)
			{
				throw new ServiceFailedException("Failed to stop standby services", e);
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Ipc.StandbyException"/>
		public override void CheckOperation(HAContext context, NameNode.OperationCategory
			 op)
		{
			if (op == NameNode.OperationCategory.Unchecked || (op == NameNode.OperationCategory
				.Read && context.AllowStaleReads()))
			{
				return;
			}
			string msg = "Operation category " + op + " is not supported in state " + context
				.GetState();
			throw new StandbyException(msg);
		}

		public override bool ShouldPopulateReplQueues()
		{
			return false;
		}
	}
}
