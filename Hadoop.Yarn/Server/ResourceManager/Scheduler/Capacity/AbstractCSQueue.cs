using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Collect;
using Org.Apache.Commons.Lang;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Authorize;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Factories;
using Org.Apache.Hadoop.Yarn.Factory.Providers;
using Org.Apache.Hadoop.Yarn.Nodelabels;
using Org.Apache.Hadoop.Yarn.Security;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Nodelabels;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmcontainer;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Common.Fica;
using Org.Apache.Hadoop.Yarn.Util.Resource;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Capacity
{
	public abstract class AbstractCSQueue : CSQueue
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Capacity.AbstractCSQueue
			));

		internal CSQueue parent;

		internal readonly string queueName;

		internal volatile int numContainers;

		internal readonly Resource minimumAllocation;

		internal Resource maximumAllocation;

		internal QueueState state;

		internal readonly QueueMetrics metrics;

		protected internal readonly PrivilegedEntity queueEntity;

		internal readonly ResourceCalculator resourceCalculator;

		internal ICollection<string> accessibleLabels;

		internal RMNodeLabelsManager labelManager;

		internal string defaultLabelExpression;

		internal IDictionary<AccessType, AccessControlList> acls = new Dictionary<AccessType
			, AccessControlList>();

		internal bool reservationsContinueLooking;

		private bool preemptionDisabled;

		internal ResourceUsage queueUsage;

		internal QueueCapacities queueCapacities;

		private readonly RecordFactory recordFactory = RecordFactoryProvider.GetRecordFactory
			(null);

		protected internal CapacitySchedulerContext csContext;

		protected internal YarnAuthorizationProvider authorizer = null;

		/// <exception cref="System.IO.IOException"/>
		public AbstractCSQueue(CapacitySchedulerContext cs, string queueName, CSQueue parent
			, CSQueue old)
		{
			// Track resource usage-by-label like used-resource/pending-resource, etc.
			// Track capacities like used-capcity/abs-used-capacity/capacity/abs-capacity,
			// etc.
			this.labelManager = cs.GetRMContext().GetNodeLabelManager();
			this.parent = parent;
			this.queueName = queueName;
			this.resourceCalculator = cs.GetResourceCalculator();
			// must be called after parent and queueName is set
			this.metrics = old != null ? old.GetMetrics() : QueueMetrics.ForQueue(GetQueuePath
				(), parent, cs.GetConfiguration().GetEnableUserMetrics(), cs.GetConf());
			this.csContext = cs;
			this.minimumAllocation = csContext.GetMinimumResourceCapability();
			// initialize ResourceUsage
			queueUsage = new ResourceUsage();
			queueEntity = new PrivilegedEntity(PrivilegedEntity.EntityType.Queue, GetQueuePath
				());
			// initialize QueueCapacities
			queueCapacities = new QueueCapacities(parent == null);
		}

		protected internal virtual void SetupConfigurableCapacities()
		{
			CSQueueUtils.LoadUpdateAndCheckCapacities(GetQueuePath(), csContext.GetConfiguration
				(), queueCapacities, parent == null ? null : parent.GetQueueCapacities());
		}

		public virtual float GetCapacity()
		{
			lock (this)
			{
				return queueCapacities.GetCapacity();
			}
		}

		public virtual float GetAbsoluteCapacity()
		{
			lock (this)
			{
				return queueCapacities.GetAbsoluteCapacity();
			}
		}

		public virtual float GetAbsoluteMaximumCapacity()
		{
			return queueCapacities.GetAbsoluteMaximumCapacity();
		}

		public virtual float GetAbsoluteUsedCapacity()
		{
			lock (this)
			{
				return queueCapacities.GetAbsoluteUsedCapacity();
			}
		}

		public virtual float GetMaximumCapacity()
		{
			return queueCapacities.GetMaximumCapacity();
		}

		public virtual float GetUsedCapacity()
		{
			lock (this)
			{
				return queueCapacities.GetUsedCapacity();
			}
		}

		public virtual Org.Apache.Hadoop.Yarn.Api.Records.Resource GetUsedResources()
		{
			return queueUsage.GetUsed();
		}

		public virtual int GetNumContainers()
		{
			lock (this)
			{
				return numContainers;
			}
		}

		public virtual QueueState GetState()
		{
			lock (this)
			{
				return state;
			}
		}

		public virtual QueueMetrics GetMetrics()
		{
			return metrics;
		}

		public virtual string GetQueueName()
		{
			return queueName;
		}

		public virtual PrivilegedEntity GetPrivilegedEntity()
		{
			return queueEntity;
		}

		public virtual CSQueue GetParent()
		{
			lock (this)
			{
				return parent;
			}
		}

		public virtual void SetParent(CSQueue newParentQueue)
		{
			lock (this)
			{
				this.parent = (ParentQueue)newParentQueue;
			}
		}

		public virtual ICollection<string> GetAccessibleNodeLabels()
		{
			return accessibleLabels;
		}

		public virtual bool HasAccess(QueueACL acl, UserGroupInformation user)
		{
			return authorizer.CheckPermission(SchedulerUtils.ToAccessType(acl), queueEntity, 
				user);
		}

		public virtual void SetUsedCapacity(float usedCapacity)
		{
			lock (this)
			{
				queueCapacities.SetUsedCapacity(usedCapacity);
			}
		}

		public virtual void SetAbsoluteUsedCapacity(float absUsedCapacity)
		{
			lock (this)
			{
				queueCapacities.SetAbsoluteUsedCapacity(absUsedCapacity);
			}
		}

		/// <summary>Set maximum capacity - used only for testing.</summary>
		/// <param name="maximumCapacity">new max capacity</param>
		internal virtual void SetMaxCapacity(float maximumCapacity)
		{
			lock (this)
			{
				// Sanity check
				CSQueueUtils.CheckMaxCapacity(GetQueueName(), queueCapacities.GetCapacity(), maximumCapacity
					);
				float absMaxCapacity = CSQueueUtils.ComputeAbsoluteMaximumCapacity(maximumCapacity
					, parent);
				CSQueueUtils.CheckAbsoluteCapacity(GetQueueName(), queueCapacities.GetAbsoluteCapacity
					(), absMaxCapacity);
				queueCapacities.SetMaximumCapacity(maximumCapacity);
				queueCapacities.SetAbsoluteMaximumCapacity(absMaxCapacity);
			}
		}

		public virtual string GetDefaultNodeLabelExpression()
		{
			return defaultLabelExpression;
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual void SetupQueueConfigs(Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 clusterResource)
		{
			lock (this)
			{
				// get labels
				this.accessibleLabels = csContext.GetConfiguration().GetAccessibleNodeLabels(GetQueuePath
					());
				this.defaultLabelExpression = csContext.GetConfiguration().GetDefaultNodeLabelExpression
					(GetQueuePath());
				// inherit from parent if labels not set
				if (this.accessibleLabels == null && parent != null)
				{
					this.accessibleLabels = parent.GetAccessibleNodeLabels();
				}
				// inherit from parent if labels not set
				if (this.defaultLabelExpression == null && parent != null && this.accessibleLabels
					.ContainsAll(parent.GetAccessibleNodeLabels()))
				{
					this.defaultLabelExpression = parent.GetDefaultNodeLabelExpression();
				}
				// After we setup labels, we can setup capacities
				SetupConfigurableCapacities();
				this.maximumAllocation = csContext.GetConfiguration().GetMaximumAllocationPerQueue
					(GetQueuePath());
				authorizer = YarnAuthorizationProvider.GetInstance(csContext.GetConf());
				this.state = csContext.GetConfiguration().GetState(GetQueuePath());
				this.acls = csContext.GetConfiguration().GetAcls(GetQueuePath());
				// Update metrics
				CSQueueUtils.UpdateQueueStatistics(resourceCalculator, this, parent, clusterResource
					, minimumAllocation);
				// Check if labels of this queue is a subset of parent queue, only do this
				// when we not root
				if (parent != null && parent.GetParent() != null)
				{
					if (parent.GetAccessibleNodeLabels() != null && !parent.GetAccessibleNodeLabels()
						.Contains(RMNodeLabelsManager.Any))
					{
						// if parent isn't "*", child shouldn't be "*" too
						if (this.GetAccessibleNodeLabels().Contains(RMNodeLabelsManager.Any))
						{
							throw new IOException("Parent's accessible queue is not ANY(*), " + "but child's accessible queue is *"
								);
						}
						else
						{
							ICollection<string> diff = Sets.Difference(this.GetAccessibleNodeLabels(), parent
								.GetAccessibleNodeLabels());
							if (!diff.IsEmpty())
							{
								throw new IOException("Some labels of child queue is not a subset " + "of parent queue, these labels=["
									 + StringUtils.Join(diff, ",") + "]");
							}
						}
					}
				}
				this.reservationsContinueLooking = csContext.GetConfiguration().GetReservationContinueLook
					();
				this.preemptionDisabled = IsQueueHierarchyPreemptionDisabled(this);
			}
		}

		protected internal virtual QueueInfo GetQueueInfo()
		{
			QueueInfo queueInfo = recordFactory.NewRecordInstance<QueueInfo>();
			queueInfo.SetQueueName(queueName);
			queueInfo.SetAccessibleNodeLabels(accessibleLabels);
			queueInfo.SetCapacity(queueCapacities.GetCapacity());
			queueInfo.SetMaximumCapacity(queueCapacities.GetMaximumCapacity());
			queueInfo.SetQueueState(state);
			queueInfo.SetDefaultNodeLabelExpression(defaultLabelExpression);
			queueInfo.SetCurrentCapacity(GetUsedCapacity());
			return queueInfo;
		}

		[InterfaceAudience.Private]
		public virtual Org.Apache.Hadoop.Yarn.Api.Records.Resource GetMaximumAllocation()
		{
			lock (this)
			{
				return maximumAllocation;
			}
		}

		[InterfaceAudience.Private]
		public virtual Org.Apache.Hadoop.Yarn.Api.Records.Resource GetMinimumAllocation()
		{
			return minimumAllocation;
		}

		internal virtual void AllocateResource(Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 clusterResource, Org.Apache.Hadoop.Yarn.Api.Records.Resource resource, ICollection
			<string> nodeLabels)
		{
			lock (this)
			{
				// Update usedResources by labels
				if (nodeLabels == null || nodeLabels.IsEmpty())
				{
					queueUsage.IncUsed(resource);
				}
				else
				{
					foreach (string label in Sets.Intersection(accessibleLabels, nodeLabels))
					{
						queueUsage.IncUsed(label, resource);
					}
				}
				++numContainers;
				CSQueueUtils.UpdateQueueStatistics(resourceCalculator, this, GetParent(), clusterResource
					, minimumAllocation);
			}
		}

		protected internal virtual void ReleaseResource(Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 clusterResource, Org.Apache.Hadoop.Yarn.Api.Records.Resource resource, ICollection
			<string> nodeLabels)
		{
			lock (this)
			{
				// Update usedResources by labels
				if (null == nodeLabels || nodeLabels.IsEmpty())
				{
					queueUsage.DecUsed(resource);
				}
				else
				{
					foreach (string label in Sets.Intersection(accessibleLabels, nodeLabels))
					{
						queueUsage.DecUsed(label, resource);
					}
				}
				CSQueueUtils.UpdateQueueStatistics(resourceCalculator, this, GetParent(), clusterResource
					, minimumAllocation);
				--numContainers;
			}
		}

		[InterfaceAudience.Private]
		public virtual bool GetReservationContinueLooking()
		{
			return reservationsContinueLooking;
		}

		[InterfaceAudience.Private]
		public virtual IDictionary<AccessType, AccessControlList> GetACLs()
		{
			return acls;
		}

		[InterfaceAudience.Private]
		public virtual bool GetPreemptionDisabled()
		{
			return preemptionDisabled;
		}

		[InterfaceAudience.Private]
		public virtual QueueCapacities GetQueueCapacities()
		{
			return queueCapacities;
		}

		[InterfaceAudience.Private]
		public virtual ResourceUsage GetQueueResourceUsage()
		{
			return queueUsage;
		}

		/// <summary>
		/// The specified queue is preemptable if system-wide preemption is turned on
		/// unless any queue in the <em>qPath</em> hierarchy has explicitly turned
		/// preemption off.
		/// </summary>
		/// <remarks>
		/// The specified queue is preemptable if system-wide preemption is turned on
		/// unless any queue in the <em>qPath</em> hierarchy has explicitly turned
		/// preemption off.
		/// NOTE: Preemptability is inherited from a queue's parent.
		/// </remarks>
		/// <returns>true if queue has preemption disabled, false otherwise</returns>
		private bool IsQueueHierarchyPreemptionDisabled(CSQueue q)
		{
			CapacitySchedulerConfiguration csConf = csContext.GetConfiguration();
			bool systemWidePreemption = csConf.GetBoolean(YarnConfiguration.RmSchedulerEnableMonitors
				, YarnConfiguration.DefaultRmSchedulerEnableMonitors);
			CSQueue parentQ = q.GetParent();
			// If the system-wide preemption switch is turned off, all of the queues in
			// the qPath hierarchy have preemption disabled, so return true.
			if (!systemWidePreemption)
			{
				return true;
			}
			// If q is the root queue and the system-wide preemption switch is turned
			// on, then q does not have preemption disabled (default=false, below)
			// unless the preemption_disabled property is explicitly set.
			if (parentQ == null)
			{
				return csConf.GetPreemptionDisabled(q.GetQueuePath(), false);
			}
			// If this is not the root queue, inherit the default value for the
			// preemption_disabled property from the parent. Preemptability will be
			// inherited from the parent's hierarchy unless explicitly overridden at
			// this level.
			return csConf.GetPreemptionDisabled(q.GetQueuePath(), parentQ.GetPreemptionDisabled
				());
		}

		private Org.Apache.Hadoop.Yarn.Api.Records.Resource GetCurrentLimitResource(string
			 nodeLabel, Org.Apache.Hadoop.Yarn.Api.Records.Resource clusterResource, ResourceLimits
			 currentResourceLimits)
		{
			/*
			* Current limit resource: For labeled resource: limit = queue-max-resource
			* (TODO, this part need update when we support labeled-limit) For
			* non-labeled resource: limit = min(queue-max-resource,
			* limit-set-by-parent)
			*/
			Org.Apache.Hadoop.Yarn.Api.Records.Resource queueMaxResource = Resources.MultiplyAndNormalizeDown
				(resourceCalculator, labelManager.GetResourceByLabel(nodeLabel, clusterResource)
				, queueCapacities.GetAbsoluteMaximumCapacity(nodeLabel), minimumAllocation);
			if (nodeLabel.Equals(RMNodeLabelsManager.NoLabel))
			{
				return Resources.Min(resourceCalculator, clusterResource, queueMaxResource, currentResourceLimits
					.GetLimit());
			}
			return queueMaxResource;
		}

		internal virtual bool CanAssignToThisQueue(Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 clusterResource, ICollection<string> nodeLabels, ResourceLimits currentResourceLimits
			, Org.Apache.Hadoop.Yarn.Api.Records.Resource nowRequired, Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 resourceCouldBeUnreserved)
		{
			lock (this)
			{
				// Get label of this queue can access, it's (nodeLabel AND queueLabel)
				ICollection<string> labelCanAccess;
				if (null == nodeLabels || nodeLabels.IsEmpty())
				{
					labelCanAccess = new HashSet<string>();
					// Any queue can always access any node without label
					labelCanAccess.AddItem(RMNodeLabelsManager.NoLabel);
				}
				else
				{
					labelCanAccess = new HashSet<string>(accessibleLabels.Contains(CommonNodeLabelsManager
						.Any) ? nodeLabels : Sets.Intersection(accessibleLabels, nodeLabels));
				}
				foreach (string label in labelCanAccess)
				{
					// New total resource = used + required
					Org.Apache.Hadoop.Yarn.Api.Records.Resource newTotalResource = Resources.Add(queueUsage
						.GetUsed(label), nowRequired);
					Org.Apache.Hadoop.Yarn.Api.Records.Resource currentLimitResource = GetCurrentLimitResource
						(label, clusterResource, currentResourceLimits);
					if (Resources.GreaterThan(resourceCalculator, clusterResource, newTotalResource, 
						currentLimitResource))
					{
						// if reservation continous looking enabled, check to see if could we
						// potentially use this node instead of a reserved node if the application
						// has reserved containers.
						// TODO, now only consider reservation cases when the node has no label
						if (this.reservationsContinueLooking && label.Equals(RMNodeLabelsManager.NoLabel)
							 && Resources.GreaterThan(resourceCalculator, clusterResource, resourceCouldBeUnreserved
							, Resources.None()))
						{
							// resource-without-reserved = used - reserved
							Org.Apache.Hadoop.Yarn.Api.Records.Resource newTotalWithoutReservedResource = Resources
								.Subtract(newTotalResource, resourceCouldBeUnreserved);
							// when total-used-without-reserved-resource < currentLimit, we still
							// have chance to allocate on this node by unreserving some containers
							if (Resources.LessThan(resourceCalculator, clusterResource, newTotalWithoutReservedResource
								, currentLimitResource))
							{
								if (Log.IsDebugEnabled())
								{
									Log.Debug("try to use reserved: " + GetQueueName() + " usedResources: " + queueUsage
										.GetUsed() + ", clusterResources: " + clusterResource + ", reservedResources: " 
										+ resourceCouldBeUnreserved + ", capacity-without-reserved: " + newTotalWithoutReservedResource
										 + ", maxLimitCapacity: " + currentLimitResource);
								}
								currentResourceLimits.SetAmountNeededUnreserve(Resources.Subtract(newTotalResource
									, currentLimitResource));
								return true;
							}
						}
						if (Log.IsDebugEnabled())
						{
							Log.Debug(GetQueueName() + "Check assign to queue, label=" + label + " usedResources: "
								 + queueUsage.GetUsed(label) + " clusterResources: " + clusterResource + " currentUsedCapacity "
								 + Resources.Divide(resourceCalculator, clusterResource, queueUsage.GetUsed(label
								), labelManager.GetResourceByLabel(label, clusterResource)) + " max-capacity: " 
								+ queueCapacities.GetAbsoluteMaximumCapacity(label) + ")");
						}
						return false;
					}
					return true;
				}
				// Actually, this will not happen, since labelCanAccess will be always
				// non-empty
				return false;
			}
		}

		public abstract ActiveUsersManager GetActiveUsersManager();

		public abstract QueueInfo GetQueueInfo(bool arg1, bool arg2);

		public abstract IList<QueueUserACLInfo> GetQueueUserAclInfo(UserGroupInformation 
			arg1);

		public abstract void RecoverContainer(Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 arg1, SchedulerApplicationAttempt arg2, RMContainer arg3);

		public abstract CSAssignment AssignContainers(Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 arg1, FiCaSchedulerNode arg2, ResourceLimits arg3);

		public abstract void AttachContainer(Org.Apache.Hadoop.Yarn.Api.Records.Resource 
			arg1, FiCaSchedulerApp arg2, RMContainer arg3);

		public abstract void CollectSchedulerApplications(ICollection<ApplicationAttemptId
			> arg1);

		public abstract void CompletedContainer(Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 arg1, FiCaSchedulerApp arg2, FiCaSchedulerNode arg3, RMContainer arg4, ContainerStatus
			 arg5, RMContainerEventType arg6, CSQueue arg7, bool arg8);

		public abstract void DetachContainer(Org.Apache.Hadoop.Yarn.Api.Records.Resource 
			arg1, FiCaSchedulerApp arg2, RMContainer arg3);

		public abstract void FinishApplication(ApplicationId arg1, string arg2);

		public abstract void FinishApplicationAttempt(FiCaSchedulerApp arg1, string arg2);

		public abstract IList<CSQueue> GetChildQueues();

		public abstract int GetNumApplications();

		public abstract string GetQueuePath();

		public abstract void Reinitialize(CSQueue arg1, Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 arg2);

		public abstract void SubmitApplication(ApplicationId arg1, string arg2, string arg3
			);

		public abstract void SubmitApplicationAttempt(FiCaSchedulerApp arg1, string arg2);

		public abstract void UpdateClusterResource(Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 arg1, ResourceLimits arg2);
	}
}
