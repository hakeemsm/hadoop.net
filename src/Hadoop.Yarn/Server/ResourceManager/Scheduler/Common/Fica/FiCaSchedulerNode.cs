using System;
using System.Collections.Generic;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Nodelabels;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmcontainer;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmnode;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Common.Fica
{
	public class FiCaSchedulerNode : SchedulerNode
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Common.Fica.FiCaSchedulerNode
			));

		public FiCaSchedulerNode(RMNode node, bool usePortForNodeName, ICollection<string
			> nodeLabels)
			: base(node, usePortForNodeName, nodeLabels)
		{
		}

		public FiCaSchedulerNode(RMNode node, bool usePortForNodeName)
			: this(node, usePortForNodeName, CommonNodeLabelsManager.EmptyStringSet)
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
					// Cannot reserve more than one application attempt on a given node!
					// Reservation is still against attempt.
					if (!reservedContainer.GetContainer().GetId().GetApplicationAttemptId().Equals(container
						.GetContainer().GetId().GetApplicationAttemptId()))
					{
						throw new InvalidOperationException("Trying to reserve" + " container " + container
							 + " for application " + application.GetApplicationAttemptId() + " when currently"
							 + " reserved container " + reservedContainer + " on node " + this);
					}
					if (Log.IsDebugEnabled())
					{
						Log.Debug("Updated reserved container " + container.GetContainer().GetId() + " on node "
							 + this + " for application attempt " + application.GetApplicationAttemptId());
					}
				}
				else
				{
					if (Log.IsDebugEnabled())
					{
						Log.Debug("Reserved container " + container.GetContainer().GetId() + " on node " 
							+ this + " for application attempt " + application.GetApplicationAttemptId());
					}
				}
				SetReservedContainer(container);
			}
		}

		public override void UnreserveResource(SchedulerApplicationAttempt application)
		{
			lock (this)
			{
				// adding NP checks as this can now be called for preemption
				if (GetReservedContainer() != null && GetReservedContainer().GetContainer() != null
					 && GetReservedContainer().GetContainer().GetId() != null && GetReservedContainer
					().GetContainer().GetId().GetApplicationAttemptId() != null)
				{
					// Cannot unreserve for wrong application...
					ApplicationAttemptId reservedApplication = GetReservedContainer().GetContainer().
						GetId().GetApplicationAttemptId();
					if (!reservedApplication.Equals(application.GetApplicationAttemptId()))
					{
						throw new InvalidOperationException("Trying to unreserve " + " for application " 
							+ application.GetApplicationAttemptId() + " when currently reserved " + " for application "
							 + reservedApplication.GetApplicationId() + " on node " + this);
					}
				}
				SetReservedContainer(null);
			}
		}
	}
}
