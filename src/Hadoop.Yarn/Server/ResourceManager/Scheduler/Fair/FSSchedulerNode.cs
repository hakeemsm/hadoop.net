using System;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmcontainer;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmnode;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fair
{
	public class FSSchedulerNode : SchedulerNode
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fair.FSSchedulerNode
			));

		private FSAppAttempt reservedAppSchedulable;

		public FSSchedulerNode(RMNode node, bool usePortForNodeName)
			: base(node, usePortForNodeName)
		{
		}

		public override void ReserveResource(SchedulerApplicationAttempt application, Priority
			 priority, RMContainer container)
		{
			lock (this)
			{
				// Check if it's already reserved
				RMContainer reservedContainer = GetReservedContainer();
				if (reservedContainer != null)
				{
					// Sanity check
					if (!container.GetContainer().GetNodeId().Equals(GetNodeID()))
					{
						throw new InvalidOperationException("Trying to reserve" + " container " + container
							 + " on node " + container.GetReservedNode() + " when currently" + " reserved resource "
							 + reservedContainer + " on node " + reservedContainer.GetReservedNode());
					}
					// Cannot reserve more than one application on a given node!
					if (!reservedContainer.GetContainer().GetId().GetApplicationAttemptId().Equals(container
						.GetContainer().GetId().GetApplicationAttemptId()))
					{
						throw new InvalidOperationException("Trying to reserve" + " container " + container
							 + " for application " + application.GetApplicationId() + " when currently" + " reserved container "
							 + reservedContainer + " on node " + this);
					}
					Log.Info("Updated reserved container " + container.GetContainer().GetId() + " on node "
						 + this + " for application " + application);
				}
				else
				{
					Log.Info("Reserved container " + container.GetContainer().GetId() + " on node " +
						 this + " for application " + application);
				}
				SetReservedContainer(container);
				this.reservedAppSchedulable = (FSAppAttempt)application;
			}
		}

		public override void UnreserveResource(SchedulerApplicationAttempt application)
		{
			lock (this)
			{
				// Cannot unreserve for wrong application...
				ApplicationAttemptId reservedApplication = GetReservedContainer().GetContainer().
					GetId().GetApplicationAttemptId();
				if (!reservedApplication.Equals(application.GetApplicationAttemptId()))
				{
					throw new InvalidOperationException("Trying to unreserve " + " for application " 
						+ application.GetApplicationId() + " when currently reserved " + " for application "
						 + reservedApplication.GetApplicationId() + " on node " + this);
				}
				SetReservedContainer(null);
				this.reservedAppSchedulable = null;
			}
		}

		public virtual FSAppAttempt GetReservedAppSchedulable()
		{
			lock (this)
			{
				return reservedAppSchedulable;
			}
		}
	}
}
