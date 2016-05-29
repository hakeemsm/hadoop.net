using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Org.Apache.Commons.Lang;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Authorize;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Factories;
using Org.Apache.Hadoop.Yarn.Factory.Providers;
using Org.Apache.Hadoop.Yarn.Security;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Nodelabels;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmcontainer;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Common.Fica;
using Org.Apache.Hadoop.Yarn.Util.Resource;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Capacity
{
	public class ParentQueue : AbstractCSQueue
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Capacity.ParentQueue
			));

		protected internal readonly ICollection<CSQueue> childQueues;

		private readonly bool rootQueue;

		internal readonly IComparer<CSQueue> queueComparator;

		internal volatile int numApplications;

		private readonly CapacitySchedulerContext scheduler;

		private readonly RecordFactory recordFactory = RecordFactoryProvider.GetRecordFactory
			(null);

		/// <exception cref="System.IO.IOException"/>
		public ParentQueue(CapacitySchedulerContext cs, string queueName, CSQueue parent, 
			CSQueue old)
			: base(cs, queueName, parent, old)
		{
			this.scheduler = cs;
			this.queueComparator = cs.GetQueueComparator();
			this.rootQueue = (parent == null);
			float rawCapacity = cs.GetConfiguration().GetNonLabeledQueueCapacity(GetQueuePath
				());
			if (rootQueue && (rawCapacity != CapacitySchedulerConfiguration.MaximumCapacityValue
				))
			{
				throw new ArgumentException("Illegal " + "capacity of " + rawCapacity + " for queue "
					 + queueName + ". Must be " + CapacitySchedulerConfiguration.MaximumCapacityValue
					);
			}
			this.childQueues = new TreeSet<CSQueue>(queueComparator);
			SetupQueueConfigs(cs.GetClusterResource());
			Log.Info("Initialized parent-queue " + queueName + " name=" + queueName + ", fullname="
				 + GetQueuePath());
		}

		/// <exception cref="System.IO.IOException"/>
		internal override void SetupQueueConfigs(Resource clusterResource)
		{
			lock (this)
			{
				base.SetupQueueConfigs(clusterResource);
				StringBuilder aclsString = new StringBuilder();
				foreach (KeyValuePair<AccessType, AccessControlList> e in acls)
				{
					aclsString.Append(e.Key + ":" + e.Value.GetAclString());
				}
				StringBuilder labelStrBuilder = new StringBuilder();
				if (accessibleLabels != null)
				{
					foreach (string s in accessibleLabels)
					{
						labelStrBuilder.Append(s);
						labelStrBuilder.Append(",");
					}
				}
				Log.Info(queueName + ", capacity=" + this.queueCapacities.GetCapacity() + ", asboluteCapacity="
					 + this.queueCapacities.GetAbsoluteCapacity() + ", maxCapacity=" + this.queueCapacities
					.GetMaximumCapacity() + ", asboluteMaxCapacity=" + this.queueCapacities.GetAbsoluteMaximumCapacity
					() + ", state=" + state + ", acls=" + aclsString + ", labels=" + labelStrBuilder
					.ToString() + "\n" + ", reservationsContinueLooking=" + reservationsContinueLooking
					);
			}
		}

		private static float Precision = 0.0005f;

		// 0.05% precision
		internal virtual void SetChildQueues(ICollection<CSQueue> childQueues)
		{
			lock (this)
			{
				// Validate
				float childCapacities = 0;
				foreach (CSQueue queue in childQueues)
				{
					childCapacities += queue.GetCapacity();
				}
				float delta = Math.Abs(1.0f - childCapacities);
				// crude way to check
				// allow capacities being set to 0, and enforce child 0 if parent is 0
				if (((queueCapacities.GetCapacity() > 0) && (delta > Precision)) || ((queueCapacities
					.GetCapacity() == 0) && (childCapacities > 0)))
				{
					throw new ArgumentException("Illegal" + " capacity of " + childCapacities + " for children of queue "
						 + queueName);
				}
				// check label capacities
				foreach (string nodeLabel in labelManager.GetClusterNodeLabels())
				{
					float capacityByLabel = queueCapacities.GetCapacity(nodeLabel);
					// check children's labels
					float sum = 0;
					foreach (CSQueue queue_1 in childQueues)
					{
						sum += queue_1.GetQueueCapacities().GetCapacity(nodeLabel);
					}
					if ((capacityByLabel > 0 && Math.Abs(1.0f - sum) > Precision) || (capacityByLabel
						 == 0) && (sum > 0))
					{
						throw new ArgumentException("Illegal" + " capacity of " + sum + " for children of queue "
							 + queueName + " for label=" + nodeLabel);
					}
				}
				this.childQueues.Clear();
				Sharpen.Collections.AddAll(this.childQueues, childQueues);
				if (Log.IsDebugEnabled())
				{
					Log.Debug("setChildQueues: " + GetChildQueuesToPrint());
				}
			}
		}

		public override string GetQueuePath()
		{
			string parentPath = ((parent == null) ? string.Empty : (parent.GetQueuePath() + "."
				));
			return parentPath + GetQueueName();
		}

		public override QueueInfo GetQueueInfo(bool includeChildQueues, bool recursive)
		{
			lock (this)
			{
				QueueInfo queueInfo = GetQueueInfo();
				IList<QueueInfo> childQueuesInfo = new AList<QueueInfo>();
				if (includeChildQueues)
				{
					foreach (CSQueue child in childQueues)
					{
						// Get queue information recursively?
						childQueuesInfo.AddItem(child.GetQueueInfo(recursive, recursive));
					}
				}
				queueInfo.SetChildQueues(childQueuesInfo);
				return queueInfo;
			}
		}

		private QueueUserACLInfo GetUserAclInfo(UserGroupInformation user)
		{
			lock (this)
			{
				QueueUserACLInfo userAclInfo = recordFactory.NewRecordInstance<QueueUserACLInfo>(
					);
				IList<QueueACL> operations = new AList<QueueACL>();
				foreach (QueueACL operation in QueueACL.Values())
				{
					if (HasAccess(operation, user))
					{
						operations.AddItem(operation);
					}
				}
				userAclInfo.SetQueueName(GetQueueName());
				userAclInfo.SetUserAcls(operations);
				return userAclInfo;
			}
		}

		public override IList<QueueUserACLInfo> GetQueueUserAclInfo(UserGroupInformation 
			user)
		{
			lock (this)
			{
				IList<QueueUserACLInfo> userAcls = new AList<QueueUserACLInfo>();
				// Add parent queue acls
				userAcls.AddItem(GetUserAclInfo(user));
				// Add children queue acls
				foreach (CSQueue child in childQueues)
				{
					Sharpen.Collections.AddAll(userAcls, child.GetQueueUserAclInfo(user));
				}
				return userAcls;
			}
		}

		public override string ToString()
		{
			return queueName + ": " + "numChildQueue= " + childQueues.Count + ", " + "capacity="
				 + queueCapacities.GetCapacity() + ", " + "absoluteCapacity=" + queueCapacities.
				GetAbsoluteCapacity() + ", " + "usedResources=" + queueUsage.GetUsed() + "usedCapacity="
				 + GetUsedCapacity() + ", " + "numApps=" + GetNumApplications() + ", " + "numContainers="
				 + GetNumContainers();
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Reinitialize(CSQueue newlyParsedQueue, Resource clusterResource
			)
		{
			lock (this)
			{
				// Sanity check
				if (!(newlyParsedQueue is Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Capacity.ParentQueue
					) || !newlyParsedQueue.GetQueuePath().Equals(GetQueuePath()))
				{
					throw new IOException("Trying to reinitialize " + GetQueuePath() + " from " + newlyParsedQueue
						.GetQueuePath());
				}
				Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Capacity.ParentQueue newlyParsedParentQueue
					 = (Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Capacity.ParentQueue
					)newlyParsedQueue;
				// Set new configs
				SetupQueueConfigs(clusterResource);
				// Re-configure existing child queues and add new ones
				// The CS has already checked to ensure all existing child queues are present!
				IDictionary<string, CSQueue> currentChildQueues = GetQueues(childQueues);
				IDictionary<string, CSQueue> newChildQueues = GetQueues(newlyParsedParentQueue.childQueues
					);
				foreach (KeyValuePair<string, CSQueue> e in newChildQueues)
				{
					string newChildQueueName = e.Key;
					CSQueue newChildQueue = e.Value;
					CSQueue childQueue = currentChildQueues[newChildQueueName];
					// Check if the child-queue already exists
					if (childQueue != null)
					{
						// Re-init existing child queues
						childQueue.Reinitialize(newChildQueue, clusterResource);
						Log.Info(GetQueueName() + ": re-configured queue: " + childQueue);
					}
					else
					{
						// New child queue, do not re-init
						// Set parent to 'this'
						newChildQueue.SetParent(this);
						// Save in list of current child queues
						currentChildQueues[newChildQueueName] = newChildQueue;
						Log.Info(GetQueueName() + ": added new child queue: " + newChildQueue);
					}
				}
				// Re-sort all queues
				childQueues.Clear();
				Sharpen.Collections.AddAll(childQueues, currentChildQueues.Values);
			}
		}

		internal virtual IDictionary<string, CSQueue> GetQueues(ICollection<CSQueue> queues
			)
		{
			IDictionary<string, CSQueue> queuesMap = new Dictionary<string, CSQueue>();
			foreach (CSQueue queue in queues)
			{
				queuesMap[queue.GetQueueName()] = queue;
			}
			return queuesMap;
		}

		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		public override void SubmitApplication(ApplicationId applicationId, string user, 
			string queue)
		{
			lock (this)
			{
				// Sanity check
				if (queue.Equals(queueName))
				{
					throw new AccessControlException("Cannot submit application " + "to non-leaf queue: "
						 + queueName);
				}
				if (state != QueueState.Running)
				{
					throw new AccessControlException("Queue " + GetQueuePath() + " is STOPPED. Cannot accept submission of application: "
						 + applicationId);
				}
				AddApplication(applicationId, user);
			}
			// Inform the parent queue
			if (parent != null)
			{
				try
				{
					parent.SubmitApplication(applicationId, user, queue);
				}
				catch (AccessControlException ace)
				{
					Log.Info("Failed to submit application to parent-queue: " + parent.GetQueuePath()
						, ace);
					RemoveApplication(applicationId, user);
					throw;
				}
			}
		}

		public override void SubmitApplicationAttempt(FiCaSchedulerApp application, string
			 userName)
		{
		}

		// submit attempt logic.
		public override void FinishApplicationAttempt(FiCaSchedulerApp application, string
			 queue)
		{
		}

		// finish attempt logic.
		private void AddApplication(ApplicationId applicationId, string user)
		{
			lock (this)
			{
				++numApplications;
				Log.Info("Application added -" + " appId: " + applicationId + " user: " + user + 
					" leaf-queue of parent: " + GetQueueName() + " #applications: " + GetNumApplications
					());
			}
		}

		public override void FinishApplication(ApplicationId application, string user)
		{
			lock (this)
			{
				RemoveApplication(application, user);
			}
			// Inform the parent queue
			if (parent != null)
			{
				parent.FinishApplication(application, user);
			}
		}

		private void RemoveApplication(ApplicationId applicationId, string user)
		{
			lock (this)
			{
				--numApplications;
				Log.Info("Application removed -" + " appId: " + applicationId + " user: " + user 
					+ " leaf-queue of parent: " + GetQueueName() + " #applications: " + GetNumApplications
					());
			}
		}

		public override CSAssignment AssignContainers(Resource clusterResource, FiCaSchedulerNode
			 node, ResourceLimits resourceLimits)
		{
			lock (this)
			{
				CSAssignment assignment = new CSAssignment(Resources.CreateResource(0, 0), NodeType
					.NodeLocal);
				ICollection<string> nodeLabels = node.GetLabels();
				// if our queue cannot access this node, just return
				if (!SchedulerUtils.CheckQueueAccessToNode(accessibleLabels, nodeLabels))
				{
					return assignment;
				}
				while (CanAssign(clusterResource, node))
				{
					if (Log.IsDebugEnabled())
					{
						Log.Debug("Trying to assign containers to child-queue of " + GetQueueName());
					}
					// Are we over maximum-capacity for this queue?
					// This will also consider parent's limits and also continuous reservation
					// looking
					if (!base.CanAssignToThisQueue(clusterResource, nodeLabels, resourceLimits, minimumAllocation
						, Resources.CreateResource(GetMetrics().GetReservedMB(), GetMetrics().GetReservedVirtualCores
						())))
					{
						break;
					}
					// Schedule
					CSAssignment assignedToChild = AssignContainersToChildQueues(clusterResource, node
						, resourceLimits);
					assignment.SetType(assignedToChild.GetType());
					// Done if no child-queue assigned anything
					if (Resources.GreaterThan(resourceCalculator, clusterResource, assignedToChild.GetResource
						(), Resources.None()))
					{
						// Track resource utilization for the parent-queue
						base.AllocateResource(clusterResource, assignedToChild.GetResource(), nodeLabels);
						// Track resource utilization in this pass of the scheduler
						Resources.AddTo(assignment.GetResource(), assignedToChild.GetResource());
						Log.Info("assignedContainer" + " queue=" + GetQueueName() + " usedCapacity=" + GetUsedCapacity
							() + " absoluteUsedCapacity=" + GetAbsoluteUsedCapacity() + " used=" + queueUsage
							.GetUsed() + " cluster=" + clusterResource);
					}
					else
					{
						break;
					}
					if (Log.IsDebugEnabled())
					{
						Log.Debug("ParentQ=" + GetQueueName() + " assignedSoFarInThisIteration=" + assignment
							.GetResource() + " usedCapacity=" + GetUsedCapacity() + " absoluteUsedCapacity="
							 + GetAbsoluteUsedCapacity());
					}
					// Do not assign more than one container if this isn't the root queue
					// or if we've already assigned an off-switch container
					if (!rootQueue || assignment.GetType() == NodeType.OffSwitch)
					{
						if (Log.IsDebugEnabled())
						{
							if (rootQueue && assignment.GetType() == NodeType.OffSwitch)
							{
								Log.Debug("Not assigning more than one off-switch container," + " assignments so far: "
									 + assignment);
							}
						}
						break;
					}
				}
				return assignment;
			}
		}

		private bool CanAssign(Org.Apache.Hadoop.Yarn.Api.Records.Resource clusterResource
			, FiCaSchedulerNode node)
		{
			return (node.GetReservedContainer() == null) && Resources.GreaterThanOrEqual(resourceCalculator
				, clusterResource, node.GetAvailableResource(), minimumAllocation);
		}

		private ResourceLimits GetResourceLimitsOfChild(CSQueue child, Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 clusterResource, ResourceLimits parentLimits)
		{
			// Set resource-limit of a given child, child.limit =
			// min(my.limit - my.used + child.used, child.max)
			// Parent available resource = parent-limit - parent-used-resource
			Org.Apache.Hadoop.Yarn.Api.Records.Resource parentMaxAvailableResource = Resources
				.Subtract(parentLimits.GetLimit(), GetUsedResources());
			// Child's limit = parent-available-resource + child-used
			Org.Apache.Hadoop.Yarn.Api.Records.Resource childLimit = Resources.Add(parentMaxAvailableResource
				, child.GetUsedResources());
			// Get child's max resource
			Org.Apache.Hadoop.Yarn.Api.Records.Resource childConfiguredMaxResource = Resources
				.MultiplyAndNormalizeDown(resourceCalculator, labelManager.GetResourceByLabel(RMNodeLabelsManager
				.NoLabel, clusterResource), child.GetAbsoluteMaximumCapacity(), minimumAllocation
				);
			// Child's limit should be capped by child configured max resource
			childLimit = Resources.Min(resourceCalculator, clusterResource, childLimit, childConfiguredMaxResource
				);
			// Normalize before return
			childLimit = Resources.RoundDown(resourceCalculator, childLimit, minimumAllocation
				);
			return new ResourceLimits(childLimit);
		}

		private CSAssignment AssignContainersToChildQueues(Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 cluster, FiCaSchedulerNode node, ResourceLimits limits)
		{
			lock (this)
			{
				CSAssignment assignment = new CSAssignment(Resources.CreateResource(0, 0), NodeType
					.NodeLocal);
				PrintChildQueues();
				// Try to assign to most 'under-served' sub-queue
				for (IEnumerator<CSQueue> iter = childQueues.GetEnumerator(); iter.HasNext(); )
				{
					CSQueue childQueue = iter.Next();
					if (Log.IsDebugEnabled())
					{
						Log.Debug("Trying to assign to queue: " + childQueue.GetQueuePath() + " stats: " 
							+ childQueue);
					}
					// Get ResourceLimits of child queue before assign containers
					ResourceLimits childLimits = GetResourceLimitsOfChild(childQueue, cluster, limits
						);
					assignment = childQueue.AssignContainers(cluster, node, childLimits);
					if (Log.IsDebugEnabled())
					{
						Log.Debug("Assigned to queue: " + childQueue.GetQueuePath() + " stats: " + childQueue
							 + " --> " + assignment.GetResource() + ", " + assignment.GetType());
					}
					// If we do assign, remove the queue and re-insert in-order to re-sort
					if (Resources.GreaterThan(resourceCalculator, cluster, assignment.GetResource(), 
						Resources.None()))
					{
						// Remove and re-insert to sort
						iter.Remove();
						Log.Info("Re-sorting assigned queue: " + childQueue.GetQueuePath() + " stats: " +
							 childQueue);
						childQueues.AddItem(childQueue);
						if (Log.IsDebugEnabled())
						{
							PrintChildQueues();
						}
						break;
					}
				}
				return assignment;
			}
		}

		internal virtual string GetChildQueuesToPrint()
		{
			StringBuilder sb = new StringBuilder();
			foreach (CSQueue q in childQueues)
			{
				sb.Append(q.GetQueuePath() + "usedCapacity=(" + q.GetUsedCapacity() + "), " + " label=("
					 + StringUtils.Join(q.GetAccessibleNodeLabels().GetEnumerator(), ",") + ")");
			}
			return sb.ToString();
		}

		private void PrintChildQueues()
		{
			if (Log.IsDebugEnabled())
			{
				Log.Debug("printChildQueues - queue: " + GetQueuePath() + " child-queues: " + GetChildQueuesToPrint
					());
			}
		}

		public override void CompletedContainer(Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 clusterResource, FiCaSchedulerApp application, FiCaSchedulerNode node, RMContainer
			 rmContainer, ContainerStatus containerStatus, RMContainerEventType @event, CSQueue
			 completedChildQueue, bool sortQueues)
		{
			if (application != null)
			{
				// Careful! Locking order is important!
				// Book keeping
				lock (this)
				{
					base.ReleaseResource(clusterResource, rmContainer.GetContainer().GetResource(), node
						.GetLabels());
					Log.Info("completedContainer" + " queue=" + GetQueueName() + " usedCapacity=" + GetUsedCapacity
						() + " absoluteUsedCapacity=" + GetAbsoluteUsedCapacity() + " used=" + queueUsage
						.GetUsed() + " cluster=" + clusterResource);
					// Note that this is using an iterator on the childQueues so this can't
					// be called if already within an iterator for the childQueues. Like
					// from assignContainersToChildQueues.
					if (sortQueues)
					{
						// reinsert the updated queue
						for (IEnumerator<CSQueue> iter = childQueues.GetEnumerator(); iter.HasNext(); )
						{
							CSQueue csqueue = iter.Next();
							if (csqueue.Equals(completedChildQueue))
							{
								iter.Remove();
								Log.Info("Re-sorting completed queue: " + csqueue.GetQueuePath() + " stats: " + csqueue
									);
								childQueues.AddItem(csqueue);
								break;
							}
						}
					}
				}
				// Inform the parent
				if (parent != null)
				{
					// complete my parent
					parent.CompletedContainer(clusterResource, application, node, rmContainer, null, 
						@event, this, sortQueues);
				}
			}
		}

		public override void UpdateClusterResource(Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 clusterResource, ResourceLimits resourceLimits)
		{
			lock (this)
			{
				// Update all children
				foreach (CSQueue childQueue in childQueues)
				{
					// Get ResourceLimits of child queue before assign containers
					ResourceLimits childLimits = GetResourceLimitsOfChild(childQueue, clusterResource
						, resourceLimits);
					childQueue.UpdateClusterResource(clusterResource, childLimits);
				}
				// Update metrics
				CSQueueUtils.UpdateQueueStatistics(resourceCalculator, this, parent, clusterResource
					, minimumAllocation);
			}
		}

		public override IList<CSQueue> GetChildQueues()
		{
			lock (this)
			{
				return new AList<CSQueue>(childQueues);
			}
		}

		public override void RecoverContainer(Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 clusterResource, SchedulerApplicationAttempt attempt, RMContainer rmContainer)
		{
			if (rmContainer.GetState().Equals(RMContainerState.Completed))
			{
				return;
			}
			// Careful! Locking order is important! 
			lock (this)
			{
				FiCaSchedulerNode node = scheduler.GetNode(rmContainer.GetContainer().GetNodeId()
					);
				base.AllocateResource(clusterResource, rmContainer.GetContainer().GetResource(), 
					node.GetLabels());
			}
			if (parent != null)
			{
				parent.RecoverContainer(clusterResource, attempt, rmContainer);
			}
		}

		public override ActiveUsersManager GetActiveUsersManager()
		{
			// Should never be called since all applications are submitted to LeafQueues
			return null;
		}

		public override void CollectSchedulerApplications(ICollection<ApplicationAttemptId
			> apps)
		{
			lock (this)
			{
				foreach (CSQueue queue in childQueues)
				{
					queue.CollectSchedulerApplications(apps);
				}
			}
		}

		public override void AttachContainer(Org.Apache.Hadoop.Yarn.Api.Records.Resource 
			clusterResource, FiCaSchedulerApp application, RMContainer rmContainer)
		{
			if (application != null)
			{
				FiCaSchedulerNode node = scheduler.GetNode(rmContainer.GetContainer().GetNodeId()
					);
				base.AllocateResource(clusterResource, rmContainer.GetContainer().GetResource(), 
					node.GetLabels());
				Log.Info("movedContainer" + " queueMoveIn=" + GetQueueName() + " usedCapacity=" +
					 GetUsedCapacity() + " absoluteUsedCapacity=" + GetAbsoluteUsedCapacity() + " used="
					 + queueUsage.GetUsed() + " cluster=" + clusterResource);
				// Inform the parent
				if (parent != null)
				{
					parent.AttachContainer(clusterResource, application, rmContainer);
				}
			}
		}

		public override void DetachContainer(Org.Apache.Hadoop.Yarn.Api.Records.Resource 
			clusterResource, FiCaSchedulerApp application, RMContainer rmContainer)
		{
			if (application != null)
			{
				FiCaSchedulerNode node = scheduler.GetNode(rmContainer.GetContainer().GetNodeId()
					);
				base.ReleaseResource(clusterResource, rmContainer.GetContainer().GetResource(), node
					.GetLabels());
				Log.Info("movedContainer" + " queueMoveOut=" + GetQueueName() + " usedCapacity=" 
					+ GetUsedCapacity() + " absoluteUsedCapacity=" + GetAbsoluteUsedCapacity() + " used="
					 + queueUsage.GetUsed() + " cluster=" + clusterResource);
				// Inform the parent
				if (parent != null)
				{
					parent.DetachContainer(clusterResource, application, rmContainer);
				}
			}
		}

		public override int GetNumApplications()
		{
			lock (this)
			{
				return numApplications;
			}
		}
	}
}
