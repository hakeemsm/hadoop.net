using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Com.Google.Common.Annotations;
using Org.Apache.Commons.Lang;
using Org.Apache.Commons.Lang.Mutable;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Authorize;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Factories;
using Org.Apache.Hadoop.Yarn.Factory.Providers;
using Org.Apache.Hadoop.Yarn.Nodelabels;
using Org.Apache.Hadoop.Yarn.Security;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Nodelabels;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmcontainer;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Common.Fica;
using Org.Apache.Hadoop.Yarn.Server.Utils;
using Org.Apache.Hadoop.Yarn.Util.Resource;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Capacity
{
	public class LeafQueue : AbstractCSQueue
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Capacity.LeafQueue
			));

		private float absoluteUsedCapacity = 0.0f;

		private int userLimit;

		private float userLimitFactor;

		protected internal int maxApplications;

		protected internal int maxApplicationsPerUser;

		private float maxAMResourcePerQueuePercent;

		private int nodeLocalityDelay;

		internal ICollection<FiCaSchedulerApp> activeApplications;

		internal IDictionary<ApplicationAttemptId, FiCaSchedulerApp> applicationAttemptMap
			 = new Dictionary<ApplicationAttemptId, FiCaSchedulerApp>();

		internal ICollection<FiCaSchedulerApp> pendingApplications;

		private float minimumAllocationFactor;

		private IDictionary<string, LeafQueue.User> users = new Dictionary<string, LeafQueue.User
			>();

		private readonly RecordFactory recordFactory = RecordFactoryProvider.GetRecordFactory
			(null);

		private CapacitySchedulerContext scheduler;

		private readonly ActiveUsersManager activeUsersManager;

		private Resource lastClusterResource = Resources.None();

		private Org.Apache.Hadoop.Yarn.Api.Records.Resource absoluteCapacityResource = Resources
			.None();

		private readonly LeafQueue.QueueResourceLimitsInfo queueResourceLimitsInfo = new 
			LeafQueue.QueueResourceLimitsInfo();

		private volatile ResourceLimits cachedResourceLimitsForHeadroom = null;

		/// <exception cref="System.IO.IOException"/>
		public LeafQueue(CapacitySchedulerContext cs, string queueName, CSQueue parent, CSQueue
			 old)
			: base(cs, queueName, parent, old)
		{
			// cache last cluster resource to compute actual capacity
			// absolute capacity as a resource (based on cluster resource)
			this.scheduler = cs;
			this.activeUsersManager = new ActiveUsersManager(metrics);
			if (Log.IsDebugEnabled())
			{
				Log.Debug("LeafQueue:" + " name=" + queueName + ", fullname=" + GetQueuePath());
			}
			IComparer<FiCaSchedulerApp> applicationComparator = cs.GetApplicationComparator();
			this.pendingApplications = new TreeSet<FiCaSchedulerApp>(applicationComparator);
			this.activeApplications = new TreeSet<FiCaSchedulerApp>(applicationComparator);
			SetupQueueConfigs(cs.GetClusterResource());
		}

		/// <exception cref="System.IO.IOException"/>
		internal override void SetupQueueConfigs(Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 clusterResource)
		{
			lock (this)
			{
				base.SetupQueueConfigs(clusterResource);
				this.lastClusterResource = clusterResource;
				UpdateAbsoluteCapacityResource(clusterResource);
				this.cachedResourceLimitsForHeadroom = new ResourceLimits(clusterResource);
				// Initialize headroom info, also used for calculating application 
				// master resource limits.  Since this happens during queue initialization
				// and all queues may not be realized yet, we'll use (optimistic) 
				// absoluteMaxCapacity (it will be replaced with the more accurate 
				// absoluteMaxAvailCapacity during headroom/userlimit/allocation events)
				SetQueueResourceLimitsInfo(clusterResource);
				CapacitySchedulerConfiguration conf = csContext.GetConfiguration();
				userLimit = conf.GetUserLimit(GetQueuePath());
				userLimitFactor = conf.GetUserLimitFactor(GetQueuePath());
				maxApplications = conf.GetMaximumApplicationsPerQueue(GetQueuePath());
				if (maxApplications < 0)
				{
					int maxSystemApps = conf.GetMaximumSystemApplications();
					maxApplications = (int)(maxSystemApps * queueCapacities.GetAbsoluteCapacity());
				}
				maxApplicationsPerUser = (int)(maxApplications * (userLimit / 100.0f) * userLimitFactor
					);
				maxAMResourcePerQueuePercent = conf.GetMaximumApplicationMasterResourcePerQueuePercent
					(GetQueuePath());
				if (!SchedulerUtils.CheckQueueLabelExpression(this.accessibleLabels, this.defaultLabelExpression
					, null))
				{
					throw new IOException("Invalid default label expression of " + " queue=" + GetQueueName
						() + " doesn't have permission to access all labels " + "in default label expression. labelExpression of resource request="
						 + (this.defaultLabelExpression == null ? string.Empty : this.defaultLabelExpression
						) + ". Queue labels=" + (GetAccessibleNodeLabels() == null ? string.Empty : StringUtils
						.Join(GetAccessibleNodeLabels().GetEnumerator(), ',')));
				}
				nodeLocalityDelay = conf.GetNodeLocalityDelay();
				// re-init this since max allocation could have changed
				this.minimumAllocationFactor = Resources.Ratio(resourceCalculator, Resources.Subtract
					(maximumAllocation, minimumAllocation), maximumAllocation);
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
				Log.Info("Initializing " + queueName + "\n" + "capacity = " + queueCapacities.GetCapacity
					() + " [= (float) configuredCapacity / 100 ]" + "\n" + "asboluteCapacity = " + queueCapacities
					.GetAbsoluteCapacity() + " [= parentAbsoluteCapacity * capacity ]" + "\n" + "maxCapacity = "
					 + queueCapacities.GetMaximumCapacity() + " [= configuredMaxCapacity ]" + "\n" +
					 "absoluteMaxCapacity = " + queueCapacities.GetAbsoluteMaximumCapacity() + " [= 1.0 maximumCapacity undefined, "
					 + "(parentAbsoluteMaxCapacity * maximumCapacity) / 100 otherwise ]" + "\n" + "userLimit = "
					 + userLimit + " [= configuredUserLimit ]" + "\n" + "userLimitFactor = " + userLimitFactor
					 + " [= configuredUserLimitFactor ]" + "\n" + "maxApplications = " + maxApplications
					 + " [= configuredMaximumSystemApplicationsPerQueue or" + " (int)(configuredMaximumSystemApplications * absoluteCapacity)]"
					 + "\n" + "maxApplicationsPerUser = " + maxApplicationsPerUser + " [= (int)(maxApplications * (userLimit / 100.0f) * "
					 + "userLimitFactor) ]" + "\n" + "usedCapacity = " + queueCapacities.GetUsedCapacity
					() + " [= usedResourcesMemory / " + "(clusterResourceMemory * absoluteCapacity)]"
					 + "\n" + "absoluteUsedCapacity = " + absoluteUsedCapacity + " [= usedResourcesMemory / clusterResourceMemory]"
					 + "\n" + "maxAMResourcePerQueuePercent = " + maxAMResourcePerQueuePercent + " [= configuredMaximumAMResourcePercent ]"
					 + "\n" + "minimumAllocationFactor = " + minimumAllocationFactor + " [= (float)(maximumAllocationMemory - minimumAllocationMemory) / "
					 + "maximumAllocationMemory ]" + "\n" + "maximumAllocation = " + maximumAllocation
					 + " [= configuredMaxAllocation ]" + "\n" + "numContainers = " + numContainers +
					 " [= currentNumContainers ]" + "\n" + "state = " + state + " [= configuredState ]"
					 + "\n" + "acls = " + aclsString + " [= configuredAcls ]" + "\n" + "nodeLocalityDelay = "
					 + nodeLocalityDelay + "\n" + "labels=" + labelStrBuilder.ToString() + "\n" + "nodeLocalityDelay = "
					 + nodeLocalityDelay + "\n" + "reservationsContinueLooking = " + reservationsContinueLooking
					 + "\n" + "preemptionDisabled = " + GetPreemptionDisabled() + "\n");
			}
		}

		public override string GetQueuePath()
		{
			return GetParent().GetQueuePath() + "." + GetQueueName();
		}

		/// <summary>Used only by tests.</summary>
		[InterfaceAudience.Private]
		public virtual float GetMinimumAllocationFactor()
		{
			return minimumAllocationFactor;
		}

		/// <summary>Used only by tests.</summary>
		[InterfaceAudience.Private]
		public virtual float GetMaxAMResourcePerQueuePercent()
		{
			return maxAMResourcePerQueuePercent;
		}

		public virtual int GetMaxApplications()
		{
			return maxApplications;
		}

		public virtual int GetMaxApplicationsPerUser()
		{
			lock (this)
			{
				return maxApplicationsPerUser;
			}
		}

		public override ActiveUsersManager GetActiveUsersManager()
		{
			return activeUsersManager;
		}

		public override IList<CSQueue> GetChildQueues()
		{
			return null;
		}

		/// <summary>Set user limit - used only for testing.</summary>
		/// <param name="userLimit">new user limit</param>
		internal virtual void SetUserLimit(int userLimit)
		{
			lock (this)
			{
				this.userLimit = userLimit;
			}
		}

		/// <summary>Set user limit factor - used only for testing.</summary>
		/// <param name="userLimitFactor">new user limit factor</param>
		internal virtual void SetUserLimitFactor(float userLimitFactor)
		{
			lock (this)
			{
				this.userLimitFactor = userLimitFactor;
			}
		}

		public override int GetNumApplications()
		{
			lock (this)
			{
				return GetNumPendingApplications() + GetNumActiveApplications();
			}
		}

		public virtual int GetNumPendingApplications()
		{
			lock (this)
			{
				return pendingApplications.Count;
			}
		}

		public virtual int GetNumActiveApplications()
		{
			lock (this)
			{
				return activeApplications.Count;
			}
		}

		[InterfaceAudience.Private]
		public virtual int GetNumApplications(string user)
		{
			lock (this)
			{
				return GetUser(user).GetTotalApplications();
			}
		}

		[InterfaceAudience.Private]
		public virtual int GetNumPendingApplications(string user)
		{
			lock (this)
			{
				return GetUser(user).GetPendingApplications();
			}
		}

		[InterfaceAudience.Private]
		public virtual int GetNumActiveApplications(string user)
		{
			lock (this)
			{
				return GetUser(user).GetActiveApplications();
			}
		}

		public override int GetNumContainers()
		{
			lock (this)
			{
				return numContainers;
			}
		}

		public override QueueState GetState()
		{
			lock (this)
			{
				return state;
			}
		}

		[InterfaceAudience.Private]
		public virtual int GetUserLimit()
		{
			lock (this)
			{
				return userLimit;
			}
		}

		[InterfaceAudience.Private]
		public virtual float GetUserLimitFactor()
		{
			lock (this)
			{
				return userLimitFactor;
			}
		}

		public override QueueInfo GetQueueInfo(bool includeChildQueues, bool recursive)
		{
			lock (this)
			{
				QueueInfo queueInfo = GetQueueInfo();
				return queueInfo;
			}
		}

		public override IList<QueueUserACLInfo> GetQueueUserAclInfo(UserGroupInformation 
			user)
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
				return Sharpen.Collections.SingletonList(userAclInfo);
			}
		}

		[InterfaceAudience.Private]
		public virtual int GetNodeLocalityDelay()
		{
			return nodeLocalityDelay;
		}

		public override string ToString()
		{
			return queueName + ": " + "capacity=" + queueCapacities.GetCapacity() + ", " + "absoluteCapacity="
				 + queueCapacities.GetAbsoluteCapacity() + ", " + "usedResources=" + queueUsage.
				GetUsed() + ", " + "usedCapacity=" + GetUsedCapacity() + ", " + "absoluteUsedCapacity="
				 + GetAbsoluteUsedCapacity() + ", " + "numApps=" + GetNumApplications() + ", " +
				 "numContainers=" + GetNumContainers();
		}

		[VisibleForTesting]
		public virtual void SetNodeLabelManager(RMNodeLabelsManager mgr)
		{
			lock (this)
			{
				this.labelManager = mgr;
			}
		}

		[VisibleForTesting]
		public virtual LeafQueue.User GetUser(string userName)
		{
			lock (this)
			{
				LeafQueue.User user = users[userName];
				if (user == null)
				{
					user = new LeafQueue.User();
					users[userName] = user;
				}
				return user;
			}
		}

		/// <returns>an ArrayList of UserInfo objects who are active in this queue</returns>
		public virtual AList<UserInfo> GetUsers()
		{
			lock (this)
			{
				AList<UserInfo> usersToReturn = new AList<UserInfo>();
				foreach (KeyValuePair<string, LeafQueue.User> entry in users)
				{
					LeafQueue.User user = entry.Value;
					usersToReturn.AddItem(new UserInfo(entry.Key, Resources.Clone(user.GetUsed()), user
						.GetActiveApplications(), user.GetPendingApplications(), Resources.Clone(user.GetConsumedAMResources
						()), Resources.Clone(user.GetUserResourceLimit())));
				}
				return usersToReturn;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Reinitialize(CSQueue newlyParsedQueue, Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 clusterResource)
		{
			lock (this)
			{
				// Sanity check
				if (!(newlyParsedQueue is Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Capacity.LeafQueue
					) || !newlyParsedQueue.GetQueuePath().Equals(GetQueuePath()))
				{
					throw new IOException("Trying to reinitialize " + GetQueuePath() + " from " + newlyParsedQueue
						.GetQueuePath());
				}
				Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Capacity.LeafQueue newlyParsedLeafQueue
					 = (Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Capacity.LeafQueue)newlyParsedQueue;
				// don't allow the maximum allocation to be decreased in size
				// since we have already told running AM's the size
				Org.Apache.Hadoop.Yarn.Api.Records.Resource oldMax = GetMaximumAllocation();
				Org.Apache.Hadoop.Yarn.Api.Records.Resource newMax = newlyParsedLeafQueue.GetMaximumAllocation
					();
				if (newMax.GetMemory() < oldMax.GetMemory() || newMax.GetVirtualCores() < oldMax.
					GetVirtualCores())
				{
					throw new IOException("Trying to reinitialize " + GetQueuePath() + " the maximum allocation size can not be decreased!"
						 + " Current setting: " + oldMax + ", trying to set it to: " + newMax);
				}
				SetupQueueConfigs(clusterResource);
				// queue metrics are updated, more resource may be available
				// activate the pending applications if possible
				ActivateApplications();
			}
		}

		public override void SubmitApplicationAttempt(FiCaSchedulerApp application, string
			 userName)
		{
			// Careful! Locking order is important!
			lock (this)
			{
				LeafQueue.User user = GetUser(userName);
				// Add the attempt to our data-structures
				AddApplicationAttempt(application, user);
			}
			// We don't want to update metrics for move app
			if (application.IsPending())
			{
				metrics.SubmitAppAttempt(userName);
			}
			GetParent().SubmitApplicationAttempt(application, userName);
		}

		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		public override void SubmitApplication(ApplicationId applicationId, string userName
			, string queue)
		{
			// Careful! Locking order is important!
			// Check queue ACLs
			UserGroupInformation userUgi = UserGroupInformation.CreateRemoteUser(userName);
			if (!HasAccess(QueueACL.SubmitApplications, userUgi) && !HasAccess(QueueACL.AdministerQueue
				, userUgi))
			{
				throw new AccessControlException("User " + userName + " cannot submit" + " applications to queue "
					 + GetQueuePath());
			}
			LeafQueue.User user = null;
			lock (this)
			{
				// Check if the queue is accepting jobs
				if (GetState() != QueueState.Running)
				{
					string msg = "Queue " + GetQueuePath() + " is STOPPED. Cannot accept submission of application: "
						 + applicationId;
					Log.Info(msg);
					throw new AccessControlException(msg);
				}
				// Check submission limits for queues
				if (GetNumApplications() >= GetMaxApplications())
				{
					string msg = "Queue " + GetQueuePath() + " already has " + GetNumApplications() +
						 " applications," + " cannot accept submission of application: " + applicationId;
					Log.Info(msg);
					throw new AccessControlException(msg);
				}
				// Check submission limits for the user on this queue
				user = GetUser(userName);
				if (user.GetTotalApplications() >= GetMaxApplicationsPerUser())
				{
					string msg = "Queue " + GetQueuePath() + " already has " + user.GetTotalApplications
						() + " applications from user " + userName + " cannot accept submission of application: "
						 + applicationId;
					Log.Info(msg);
					throw new AccessControlException(msg);
				}
			}
			// Inform the parent queue
			try
			{
				GetParent().SubmitApplication(applicationId, userName, queue);
			}
			catch (AccessControlException ace)
			{
				Log.Info("Failed to submit application to parent-queue: " + GetParent().GetQueuePath
					(), ace);
				throw;
			}
		}

		public virtual Org.Apache.Hadoop.Yarn.Api.Records.Resource GetAMResourceLimit()
		{
			lock (this)
			{
				/*
				* The limit to the amount of resources which can be consumed by
				* application masters for applications running in the queue
				* is calculated by taking the greater of the max resources currently
				* available to the queue (see absoluteMaxAvailCapacity) and the absolute
				* resources guaranteed for the queue and multiplying it by the am
				* resource percent.
				*
				* This is to allow a queue to grow its (proportional) application
				* master resource use up to its max capacity when other queues are
				* idle but to scale back down to it's guaranteed capacity as they
				* become busy.
				*
				*/
				Org.Apache.Hadoop.Yarn.Api.Records.Resource queueCurrentLimit;
				lock (queueResourceLimitsInfo)
				{
					queueCurrentLimit = queueResourceLimitsInfo.GetQueueCurrentLimit();
				}
				Org.Apache.Hadoop.Yarn.Api.Records.Resource queueCap = Resources.Max(resourceCalculator
					, lastClusterResource, absoluteCapacityResource, queueCurrentLimit);
				return Resources.MultiplyAndNormalizeUp(resourceCalculator, queueCap, maxAMResourcePerQueuePercent
					, minimumAllocation);
			}
		}

		public virtual Org.Apache.Hadoop.Yarn.Api.Records.Resource GetUserAMResourceLimit
			()
		{
			lock (this)
			{
				/*
				* The user amresource limit is based on the same approach as the
				* user limit (as it should represent a subset of that).  This means that
				* it uses the absolute queue capacity instead of the max and is modified
				* by the userlimit and the userlimit factor as is the userlimit
				*
				*/
				float effectiveUserLimit = Math.Max(userLimit / 100.0f, 1.0f / Math.Max(GetActiveUsersManager
					().GetNumActiveUsers(), 1));
				return Resources.MultiplyAndNormalizeUp(resourceCalculator, absoluteCapacityResource
					, maxAMResourcePerQueuePercent * effectiveUserLimit * userLimitFactor, minimumAllocation
					);
			}
		}

		private void ActivateApplications()
		{
			lock (this)
			{
				//limit of allowed resource usage for application masters
				Org.Apache.Hadoop.Yarn.Api.Records.Resource amLimit = GetAMResourceLimit();
				Org.Apache.Hadoop.Yarn.Api.Records.Resource userAMLimit = GetUserAMResourceLimit(
					);
				for (IEnumerator<FiCaSchedulerApp> i = pendingApplications.GetEnumerator(); i.HasNext
					(); )
				{
					FiCaSchedulerApp application = i.Next();
					// Check am resource limit
					Org.Apache.Hadoop.Yarn.Api.Records.Resource amIfStarted = Resources.Add(application
						.GetAMResource(), queueUsage.GetAMUsed());
					if (Log.IsDebugEnabled())
					{
						Log.Debug("application AMResource " + application.GetAMResource() + " maxAMResourcePerQueuePercent "
							 + maxAMResourcePerQueuePercent + " amLimit " + amLimit + " lastClusterResource "
							 + lastClusterResource + " amIfStarted " + amIfStarted);
					}
					if (!Resources.LessThanOrEqual(resourceCalculator, lastClusterResource, amIfStarted
						, amLimit))
					{
						if (GetNumActiveApplications() < 1)
						{
							Log.Warn("maximum-am-resource-percent is insufficient to start a" + " single application in queue, it is likely set too low."
								 + " skipping enforcement to allow at least one application to start");
						}
						else
						{
							Log.Info("not starting application as amIfStarted exceeds amLimit");
							continue;
						}
					}
					// Check user am resource limit
					LeafQueue.User user = GetUser(application.GetUser());
					Org.Apache.Hadoop.Yarn.Api.Records.Resource userAmIfStarted = Resources.Add(application
						.GetAMResource(), user.GetConsumedAMResources());
					if (!Resources.LessThanOrEqual(resourceCalculator, lastClusterResource, userAmIfStarted
						, userAMLimit))
					{
						if (GetNumActiveApplications() < 1)
						{
							Log.Warn("maximum-am-resource-percent is insufficient to start a" + " single application in queue for user, it is likely set too low."
								 + " skipping enforcement to allow at least one application to start");
						}
						else
						{
							Log.Info("not starting application as amIfStarted exceeds " + "userAmLimit");
							continue;
						}
					}
					user.ActivateApplication();
					activeApplications.AddItem(application);
					queueUsage.IncAMUsed(application.GetAMResource());
					user.GetResourceUsage().IncAMUsed(application.GetAMResource());
					i.Remove();
					Log.Info("Application " + application.GetApplicationId() + " from user: " + application
						.GetUser() + " activated in queue: " + GetQueueName());
				}
			}
		}

		private void AddApplicationAttempt(FiCaSchedulerApp application, LeafQueue.User user
			)
		{
			lock (this)
			{
				// Accept 
				user.SubmitApplication();
				pendingApplications.AddItem(application);
				applicationAttemptMap[application.GetApplicationAttemptId()] = application;
				// Activate applications
				ActivateApplications();
				Log.Info("Application added -" + " appId: " + application.GetApplicationId() + " user: "
					 + user + "," + " leaf-queue: " + GetQueueName() + " #user-pending-applications: "
					 + user.GetPendingApplications() + " #user-active-applications: " + user.GetActiveApplications
					() + " #queue-pending-applications: " + GetNumPendingApplications() + " #queue-active-applications: "
					 + GetNumActiveApplications());
			}
		}

		public override void FinishApplication(ApplicationId application, string user)
		{
			// Inform the activeUsersManager
			activeUsersManager.DeactivateApplication(user, application);
			// Inform the parent queue
			GetParent().FinishApplication(application, user);
		}

		public override void FinishApplicationAttempt(FiCaSchedulerApp application, string
			 queue)
		{
			// Careful! Locking order is important!
			lock (this)
			{
				RemoveApplicationAttempt(application, GetUser(application.GetUser()));
			}
			GetParent().FinishApplicationAttempt(application, queue);
		}

		public virtual void RemoveApplicationAttempt(FiCaSchedulerApp application, LeafQueue.User
			 user)
		{
			lock (this)
			{
				bool wasActive = activeApplications.Remove(application);
				if (!wasActive)
				{
					pendingApplications.Remove(application);
				}
				else
				{
					queueUsage.DecAMUsed(application.GetAMResource());
					user.GetResourceUsage().DecAMUsed(application.GetAMResource());
				}
				Sharpen.Collections.Remove(applicationAttemptMap, application.GetApplicationAttemptId
					());
				user.FinishApplication(wasActive);
				if (user.GetTotalApplications() == 0)
				{
					Sharpen.Collections.Remove(users, application.GetUser());
				}
				// Check if we can activate more applications
				ActivateApplications();
				Log.Info("Application removed -" + " appId: " + application.GetApplicationId() + 
					" user: " + application.GetUser() + " queue: " + GetQueueName() + " #user-pending-applications: "
					 + user.GetPendingApplications() + " #user-active-applications: " + user.GetActiveApplications
					() + " #queue-pending-applications: " + GetNumPendingApplications() + " #queue-active-applications: "
					 + GetNumActiveApplications());
			}
		}

		private FiCaSchedulerApp GetApplication(ApplicationAttemptId applicationAttemptId
			)
		{
			lock (this)
			{
				return applicationAttemptMap[applicationAttemptId];
			}
		}

		private static readonly CSAssignment NullAssignment = new CSAssignment(Resources.
			CreateResource(0, 0), NodeType.NodeLocal);

		private static readonly CSAssignment SkipAssignment = new CSAssignment(true);

		private static ICollection<string> GetRequestLabelSetByExpression(string labelExpression
			)
		{
			ICollection<string> labels = new HashSet<string>();
			if (null == labelExpression)
			{
				return labels;
			}
			foreach (string l in labelExpression.Split("&&"))
			{
				if (l.Trim().IsEmpty())
				{
					continue;
				}
				labels.AddItem(l.Trim());
			}
			return labels;
		}

		public override CSAssignment AssignContainers(Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 clusterResource, FiCaSchedulerNode node, ResourceLimits currentResourceLimits)
		{
			lock (this)
			{
				UpdateCurrentResourceLimits(currentResourceLimits, clusterResource);
				if (Log.IsDebugEnabled())
				{
					Log.Debug("assignContainers: node=" + node.GetNodeName() + " #applications=" + activeApplications
						.Count);
				}
				// if our queue cannot access this node, just return
				if (!SchedulerUtils.CheckQueueAccessToNode(accessibleLabels, node.GetLabels()))
				{
					return NullAssignment;
				}
				// Check for reserved resources
				RMContainer reservedContainer = node.GetReservedContainer();
				if (reservedContainer != null)
				{
					FiCaSchedulerApp application = GetApplication(reservedContainer.GetApplicationAttemptId
						());
					lock (application)
					{
						return AssignReservedContainer(application, node, reservedContainer, clusterResource
							);
					}
				}
				// Try to assign containers to applications in order
				foreach (FiCaSchedulerApp application_1 in activeApplications)
				{
					if (Log.IsDebugEnabled())
					{
						Log.Debug("pre-assignContainers for application " + application_1.GetApplicationId
							());
						application_1.ShowRequests();
					}
					lock (application_1)
					{
						// Check if this resource is on the blacklist
						if (SchedulerAppUtils.IsBlacklisted(application_1, node, Log))
						{
							continue;
						}
						// Schedule in priority order
						foreach (Priority priority in application_1.GetPriorities())
						{
							ResourceRequest anyRequest = application_1.GetResourceRequest(priority, ResourceRequest
								.Any);
							if (null == anyRequest)
							{
								continue;
							}
							// Required resource
							Org.Apache.Hadoop.Yarn.Api.Records.Resource required = anyRequest.GetCapability();
							// Do we need containers at this 'priority'?
							if (application_1.GetTotalRequiredResources(priority) <= 0)
							{
								continue;
							}
							if (!this.reservationsContinueLooking)
							{
								if (!ShouldAllocOrReserveNewContainer(application_1, priority, required))
								{
									if (Log.IsDebugEnabled())
									{
										Log.Debug("doesn't need containers based on reservation algo!");
									}
									continue;
								}
							}
							ICollection<string> requestedNodeLabels = GetRequestLabelSetByExpression(anyRequest
								.GetNodeLabelExpression());
							// Compute user-limit & set headroom
							// Note: We compute both user-limit & headroom with the highest 
							//       priority request as the target. 
							//       This works since we never assign lower priority requests
							//       before all higher priority ones are serviced.
							Org.Apache.Hadoop.Yarn.Api.Records.Resource userLimit = ComputeUserLimitAndSetHeadroom
								(application_1, clusterResource, required, requestedNodeLabels);
							// Check queue max-capacity limit
							if (!base.CanAssignToThisQueue(clusterResource, node.GetLabels(), currentResourceLimits
								, required, application_1.GetCurrentReservation()))
							{
								return NullAssignment;
							}
							// Check user limit
							if (!AssignToUser(clusterResource, application_1.GetUser(), userLimit, application_1
								, requestedNodeLabels, currentResourceLimits))
							{
								break;
							}
							// Inform the application it is about to get a scheduling opportunity
							application_1.AddSchedulingOpportunity(priority);
							// Try to schedule
							CSAssignment assignment = AssignContainersOnNode(clusterResource, node, application_1
								, priority, null, currentResourceLimits);
							// Did the application skip this node?
							if (assignment.GetSkipped())
							{
								// Don't count 'skipped nodes' as a scheduling opportunity!
								application_1.SubtractSchedulingOpportunity(priority);
								continue;
							}
							// Did we schedule or reserve a container?
							Org.Apache.Hadoop.Yarn.Api.Records.Resource assigned = assignment.GetResource();
							if (Resources.GreaterThan(resourceCalculator, clusterResource, assigned, Resources
								.None()))
							{
								// Book-keeping 
								// Note: Update headroom to account for current allocation too...
								AllocateResource(clusterResource, application_1, assigned, node.GetLabels());
								// Don't reset scheduling opportunities for non-local assignments
								// otherwise the app will be delayed for each non-local assignment.
								// This helps apps with many off-cluster requests schedule faster.
								if (assignment.GetType() != NodeType.OffSwitch)
								{
									if (Log.IsDebugEnabled())
									{
										Log.Debug("Resetting scheduling opportunities");
									}
									application_1.ResetSchedulingOpportunities(priority);
								}
								// Done
								return assignment;
							}
							else
							{
								// Do not assign out of order w.r.t priorities
								break;
							}
						}
					}
					if (Log.IsDebugEnabled())
					{
						Log.Debug("post-assignContainers for application " + application_1.GetApplicationId
							());
					}
					application_1.ShowRequests();
				}
				return NullAssignment;
			}
		}

		private CSAssignment AssignReservedContainer(FiCaSchedulerApp application, FiCaSchedulerNode
			 node, RMContainer rmContainer, Org.Apache.Hadoop.Yarn.Api.Records.Resource clusterResource
			)
		{
			lock (this)
			{
				// Do we still need this reservation?
				Priority priority = rmContainer.GetReservedPriority();
				if (application.GetTotalRequiredResources(priority) == 0)
				{
					// Release
					return new CSAssignment(application, rmContainer);
				}
				// Try to assign if we have sufficient resources
				AssignContainersOnNode(clusterResource, node, application, priority, rmContainer, 
					new ResourceLimits(Resources.None()));
				// Doesn't matter... since it's already charged for at time of reservation
				// "re-reservation" is *free*
				return new CSAssignment(Resources.None(), NodeType.NodeLocal);
			}
		}

		protected internal virtual Org.Apache.Hadoop.Yarn.Api.Records.Resource GetHeadroom
			(LeafQueue.User user, Org.Apache.Hadoop.Yarn.Api.Records.Resource queueCurrentLimit
			, Org.Apache.Hadoop.Yarn.Api.Records.Resource clusterResource, FiCaSchedulerApp 
			application, Org.Apache.Hadoop.Yarn.Api.Records.Resource required)
		{
			return GetHeadroom(user, queueCurrentLimit, clusterResource, ComputeUserLimit(application
				, clusterResource, required, user, null));
		}

		private Org.Apache.Hadoop.Yarn.Api.Records.Resource GetHeadroom(LeafQueue.User user
			, Org.Apache.Hadoop.Yarn.Api.Records.Resource currentResourceLimit, Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 clusterResource, Org.Apache.Hadoop.Yarn.Api.Records.Resource userLimit)
		{
			Org.Apache.Hadoop.Yarn.Api.Records.Resource headroom = Resources.ComponentwiseMin
				(Resources.Subtract(userLimit, user.GetUsed()), Resources.Subtract(currentResourceLimit
				, queueUsage.GetUsed()));
			// Normalize it before return
			headroom = Resources.RoundDown(resourceCalculator, headroom, minimumAllocation);
			return headroom;
		}

		private void SetQueueResourceLimitsInfo(Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 clusterResource)
		{
			lock (queueResourceLimitsInfo)
			{
				queueResourceLimitsInfo.SetQueueCurrentLimit(cachedResourceLimitsForHeadroom.GetLimit
					());
				queueResourceLimitsInfo.SetClusterResource(clusterResource);
			}
		}

		internal virtual Org.Apache.Hadoop.Yarn.Api.Records.Resource ComputeUserLimitAndSetHeadroom
			(FiCaSchedulerApp application, Org.Apache.Hadoop.Yarn.Api.Records.Resource clusterResource
			, Org.Apache.Hadoop.Yarn.Api.Records.Resource required, ICollection<string> requestedLabels
			)
		{
			string user = application.GetUser();
			LeafQueue.User queueUser = GetUser(user);
			// Compute user limit respect requested labels,
			// TODO, need consider headroom respect labels also
			Org.Apache.Hadoop.Yarn.Api.Records.Resource userLimit = ComputeUserLimit(application
				, clusterResource, required, queueUser, requestedLabels);
			SetQueueResourceLimitsInfo(clusterResource);
			Org.Apache.Hadoop.Yarn.Api.Records.Resource headroom = GetHeadroom(queueUser, cachedResourceLimitsForHeadroom
				.GetLimit(), clusterResource, userLimit);
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Headroom calculation for user " + user + ": " + " userLimit=" + userLimit
					 + " queueMaxAvailRes=" + cachedResourceLimitsForHeadroom.GetLimit() + " consumed="
					 + queueUser.GetUsed() + " headroom=" + headroom);
			}
			CapacityHeadroomProvider headroomProvider = new CapacityHeadroomProvider(queueUser
				, this, application, required, queueResourceLimitsInfo);
			application.SetHeadroomProvider(headroomProvider);
			metrics.SetAvailableResourcesToUser(user, headroom);
			return userLimit;
		}

		private Org.Apache.Hadoop.Yarn.Api.Records.Resource ComputeUserLimit(FiCaSchedulerApp
			 application, Org.Apache.Hadoop.Yarn.Api.Records.Resource clusterResource, Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 required, LeafQueue.User user, ICollection<string> requestedLabels)
		{
			// What is our current capacity? 
			// * It is equal to the max(required, queue-capacity) if
			//   we're running below capacity. The 'max' ensures that jobs in queues
			//   with miniscule capacity (< 1 slot) make progress
			// * If we're running over capacity, then its
			//   (usedResources + required) (which extra resources we are allocating)
			Org.Apache.Hadoop.Yarn.Api.Records.Resource queueCapacity = Org.Apache.Hadoop.Yarn.Api.Records.Resource
				.NewInstance(0, 0);
			if (requestedLabels != null && !requestedLabels.IsEmpty())
			{
				// if we have multiple labels to request, we will choose to use the first
				// label
				string firstLabel = requestedLabels.GetEnumerator().Next();
				queueCapacity = Resources.Max(resourceCalculator, clusterResource, queueCapacity, 
					Resources.MultiplyAndNormalizeUp(resourceCalculator, labelManager.GetResourceByLabel
					(firstLabel, clusterResource), queueCapacities.GetAbsoluteCapacity(firstLabel), 
					minimumAllocation));
			}
			else
			{
				// else there's no label on request, just to use absolute capacity as
				// capacity for nodes without label
				queueCapacity = Resources.MultiplyAndNormalizeUp(resourceCalculator, labelManager
					.GetResourceByLabel(CommonNodeLabelsManager.NoLabel, clusterResource), queueCapacities
					.GetAbsoluteCapacity(), minimumAllocation);
			}
			// Allow progress for queues with miniscule capacity
			queueCapacity = Resources.Max(resourceCalculator, clusterResource, queueCapacity, 
				required);
			Org.Apache.Hadoop.Yarn.Api.Records.Resource currentCapacity = Resources.LessThan(
				resourceCalculator, clusterResource, queueUsage.GetUsed(), queueCapacity) ? queueCapacity
				 : Resources.Add(queueUsage.GetUsed(), required);
			// Never allow a single user to take more than the 
			// queue's configured capacity * user-limit-factor.
			// Also, the queue's configured capacity should be higher than 
			// queue-hard-limit * ulMin
			int activeUsers = activeUsersManager.GetNumActiveUsers();
			Org.Apache.Hadoop.Yarn.Api.Records.Resource limit = Resources.RoundUp(resourceCalculator
				, Resources.Min(resourceCalculator, clusterResource, Resources.Max(resourceCalculator
				, clusterResource, Resources.DivideAndCeil(resourceCalculator, currentCapacity, 
				activeUsers), Resources.DivideAndCeil(resourceCalculator, Resources.MultiplyAndRoundDown
				(currentCapacity, userLimit), 100)), Resources.MultiplyAndRoundDown(queueCapacity
				, userLimitFactor)), minimumAllocation);
			if (Log.IsDebugEnabled())
			{
				string userName = application.GetUser();
				Log.Debug("User limit computation for " + userName + " in queue " + GetQueueName(
					) + " userLimit=" + userLimit + " userLimitFactor=" + userLimitFactor + " required: "
					 + required + " consumed: " + user.GetUsed() + " limit: " + limit + " queueCapacity: "
					 + queueCapacity + " qconsumed: " + queueUsage.GetUsed() + " currentCapacity: " 
					+ currentCapacity + " activeUsers: " + activeUsers + " clusterCapacity: " + clusterResource
					);
			}
			user.SetUserResourceLimit(limit);
			return limit;
		}

		[InterfaceAudience.Private]
		protected internal virtual bool AssignToUser(Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 clusterResource, string userName, Org.Apache.Hadoop.Yarn.Api.Records.Resource limit
			, FiCaSchedulerApp application, ICollection<string> requestLabels, ResourceLimits
			 currentResoureLimits)
		{
			lock (this)
			{
				LeafQueue.User user = GetUser(userName);
				string label = CommonNodeLabelsManager.NoLabel;
				if (requestLabels != null && !requestLabels.IsEmpty())
				{
					label = requestLabels.GetEnumerator().Next();
				}
				// Note: We aren't considering the current request since there is a fixed
				// overhead of the AM, but it's a > check, not a >= check, so...
				if (Resources.GreaterThan(resourceCalculator, clusterResource, user.GetUsed(label
					), limit))
				{
					// if enabled, check to see if could we potentially use this node instead
					// of a reserved node if the application has reserved containers
					if (this.reservationsContinueLooking)
					{
						if (Resources.LessThanOrEqual(resourceCalculator, clusterResource, Resources.Subtract
							(user.GetUsed(), application.GetCurrentReservation()), limit))
						{
							if (Log.IsDebugEnabled())
							{
								Log.Debug("User " + userName + " in queue " + GetQueueName() + " will exceed limit based on reservations - "
									 + " consumed: " + user.GetUsed() + " reserved: " + application.GetCurrentReservation
									() + " limit: " + limit);
							}
							Org.Apache.Hadoop.Yarn.Api.Records.Resource amountNeededToUnreserve = Resources.Subtract
								(user.GetUsed(label), limit);
							// we can only acquire a new container if we unreserve first since we ignored the
							// user limit. Choose the max of user limit or what was previously set by max
							// capacity.
							currentResoureLimits.SetAmountNeededUnreserve(Resources.Max(resourceCalculator, clusterResource
								, currentResoureLimits.GetAmountNeededUnreserve(), amountNeededToUnreserve));
							return true;
						}
					}
					if (Log.IsDebugEnabled())
					{
						Log.Debug("User " + userName + " in queue " + GetQueueName() + " will exceed limit - "
							 + " consumed: " + user.GetUsed() + " limit: " + limit);
					}
					return false;
				}
				return true;
			}
		}

		internal virtual bool ShouldAllocOrReserveNewContainer(FiCaSchedulerApp application
			, Priority priority, Org.Apache.Hadoop.Yarn.Api.Records.Resource required)
		{
			int requiredContainers = application.GetTotalRequiredResources(priority);
			int reservedContainers = application.GetNumReservedContainers(priority);
			int starvation = 0;
			if (reservedContainers > 0)
			{
				float nodeFactor = Resources.Ratio(resourceCalculator, required, GetMaximumAllocation
					());
				// Use percentage of node required to bias against large containers...
				// Protect against corner case where you need the whole node with
				// Math.min(nodeFactor, minimumAllocationFactor)
				starvation = (int)((application.GetReReservations(priority) / (float)reservedContainers
					) * (1.0f - (Math.Min(nodeFactor, GetMinimumAllocationFactor()))));
				if (Log.IsDebugEnabled())
				{
					Log.Debug("needsContainers:" + " app.#re-reserve=" + application.GetReReservations
						(priority) + " reserved=" + reservedContainers + " nodeFactor=" + nodeFactor + " minAllocFactor="
						 + GetMinimumAllocationFactor() + " starvation=" + starvation);
				}
			}
			return (((starvation + requiredContainers) - reservedContainers) > 0);
		}

		private CSAssignment AssignContainersOnNode(Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 clusterResource, FiCaSchedulerNode node, FiCaSchedulerApp application, Priority
			 priority, RMContainer reservedContainer, ResourceLimits currentResoureLimits)
		{
			Org.Apache.Hadoop.Yarn.Api.Records.Resource assigned = Resources.None();
			NodeType requestType = null;
			MutableObject allocatedContainer = new MutableObject();
			// Data-local
			ResourceRequest nodeLocalResourceRequest = application.GetResourceRequest(priority
				, node.GetNodeName());
			if (nodeLocalResourceRequest != null)
			{
				requestType = NodeType.NodeLocal;
				assigned = AssignNodeLocalContainers(clusterResource, nodeLocalResourceRequest, node
					, application, priority, reservedContainer, allocatedContainer, currentResoureLimits
					);
				if (Resources.GreaterThan(resourceCalculator, clusterResource, assigned, Resources
					.None()))
				{
					//update locality statistics
					if (allocatedContainer.GetValue() != null)
					{
						application.IncNumAllocatedContainers(NodeType.NodeLocal, requestType);
					}
					return new CSAssignment(assigned, NodeType.NodeLocal);
				}
			}
			// Rack-local
			ResourceRequest rackLocalResourceRequest = application.GetResourceRequest(priority
				, node.GetRackName());
			if (rackLocalResourceRequest != null)
			{
				if (!rackLocalResourceRequest.GetRelaxLocality())
				{
					return SkipAssignment;
				}
				if (requestType != NodeType.NodeLocal)
				{
					requestType = NodeType.RackLocal;
				}
				assigned = AssignRackLocalContainers(clusterResource, rackLocalResourceRequest, node
					, application, priority, reservedContainer, allocatedContainer, currentResoureLimits
					);
				if (Resources.GreaterThan(resourceCalculator, clusterResource, assigned, Resources
					.None()))
				{
					//update locality statistics
					if (allocatedContainer.GetValue() != null)
					{
						application.IncNumAllocatedContainers(NodeType.RackLocal, requestType);
					}
					return new CSAssignment(assigned, NodeType.RackLocal);
				}
			}
			// Off-switch
			ResourceRequest offSwitchResourceRequest = application.GetResourceRequest(priority
				, ResourceRequest.Any);
			if (offSwitchResourceRequest != null)
			{
				if (!offSwitchResourceRequest.GetRelaxLocality())
				{
					return SkipAssignment;
				}
				if (requestType != NodeType.NodeLocal && requestType != NodeType.RackLocal)
				{
					requestType = NodeType.OffSwitch;
				}
				assigned = AssignOffSwitchContainers(clusterResource, offSwitchResourceRequest, node
					, application, priority, reservedContainer, allocatedContainer, currentResoureLimits
					);
				// update locality statistics
				if (allocatedContainer.GetValue() != null)
				{
					application.IncNumAllocatedContainers(NodeType.OffSwitch, requestType);
				}
				return new CSAssignment(assigned, NodeType.OffSwitch);
			}
			return SkipAssignment;
		}

		[InterfaceAudience.Private]
		protected internal virtual bool FindNodeToUnreserve(Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 clusterResource, FiCaSchedulerNode node, FiCaSchedulerApp application, Priority
			 priority, Org.Apache.Hadoop.Yarn.Api.Records.Resource minimumUnreservedResource
			)
		{
			// need to unreserve some other container first
			NodeId idToUnreserve = application.GetNodeIdToUnreserve(priority, minimumUnreservedResource
				, resourceCalculator, clusterResource);
			if (idToUnreserve == null)
			{
				if (Log.IsDebugEnabled())
				{
					Log.Debug("checked to see if could unreserve for app but nothing " + "reserved that matches for this app"
						);
				}
				return false;
			}
			FiCaSchedulerNode nodeToUnreserve = scheduler.GetNode(idToUnreserve);
			if (nodeToUnreserve == null)
			{
				Log.Error("node to unreserve doesn't exist, nodeid: " + idToUnreserve);
				return false;
			}
			if (Log.IsDebugEnabled())
			{
				Log.Debug("unreserving for app: " + application.GetApplicationId() + " on nodeId: "
					 + idToUnreserve + " in order to replace reserved application and place it on node: "
					 + node.GetNodeID() + " needing: " + minimumUnreservedResource);
			}
			// headroom
			Resources.AddTo(application.GetHeadroom(), nodeToUnreserve.GetReservedContainer()
				.GetReservedResource());
			// Make sure to not have completedContainers sort the queues here since
			// we are already inside an iterator loop for the queues and this would
			// cause an concurrent modification exception.
			CompletedContainer(clusterResource, application, nodeToUnreserve, nodeToUnreserve
				.GetReservedContainer(), SchedulerUtils.CreateAbnormalContainerStatus(nodeToUnreserve
				.GetReservedContainer().GetContainerId(), SchedulerUtils.UnreservedContainer), RMContainerEventType
				.Released, null, false);
			return true;
		}

		private Org.Apache.Hadoop.Yarn.Api.Records.Resource AssignNodeLocalContainers(Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 clusterResource, ResourceRequest nodeLocalResourceRequest, FiCaSchedulerNode node
			, FiCaSchedulerApp application, Priority priority, RMContainer reservedContainer
			, MutableObject allocatedContainer, ResourceLimits currentResoureLimits)
		{
			if (CanAssign(application, priority, node, NodeType.NodeLocal, reservedContainer))
			{
				return AssignContainer(clusterResource, node, application, priority, nodeLocalResourceRequest
					, NodeType.NodeLocal, reservedContainer, allocatedContainer, currentResoureLimits
					);
			}
			return Resources.None();
		}

		private Org.Apache.Hadoop.Yarn.Api.Records.Resource AssignRackLocalContainers(Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 clusterResource, ResourceRequest rackLocalResourceRequest, FiCaSchedulerNode node
			, FiCaSchedulerApp application, Priority priority, RMContainer reservedContainer
			, MutableObject allocatedContainer, ResourceLimits currentResoureLimits)
		{
			if (CanAssign(application, priority, node, NodeType.RackLocal, reservedContainer))
			{
				return AssignContainer(clusterResource, node, application, priority, rackLocalResourceRequest
					, NodeType.RackLocal, reservedContainer, allocatedContainer, currentResoureLimits
					);
			}
			return Resources.None();
		}

		private Org.Apache.Hadoop.Yarn.Api.Records.Resource AssignOffSwitchContainers(Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 clusterResource, ResourceRequest offSwitchResourceRequest, FiCaSchedulerNode node
			, FiCaSchedulerApp application, Priority priority, RMContainer reservedContainer
			, MutableObject allocatedContainer, ResourceLimits currentResoureLimits)
		{
			if (CanAssign(application, priority, node, NodeType.OffSwitch, reservedContainer))
			{
				return AssignContainer(clusterResource, node, application, priority, offSwitchResourceRequest
					, NodeType.OffSwitch, reservedContainer, allocatedContainer, currentResoureLimits
					);
			}
			return Resources.None();
		}

		internal virtual bool CanAssign(FiCaSchedulerApp application, Priority priority, 
			FiCaSchedulerNode node, NodeType type, RMContainer reservedContainer)
		{
			// Clearly we need containers for this application...
			if (type == NodeType.OffSwitch)
			{
				if (reservedContainer != null)
				{
					return true;
				}
				// 'Delay' off-switch
				ResourceRequest offSwitchRequest = application.GetResourceRequest(priority, ResourceRequest
					.Any);
				long missedOpportunities = application.GetSchedulingOpportunities(priority);
				long requiredContainers = offSwitchRequest.GetNumContainers();
				float localityWaitFactor = application.GetLocalityWaitFactor(priority, scheduler.
					GetNumClusterNodes());
				return ((requiredContainers * localityWaitFactor) < missedOpportunities);
			}
			// Check if we need containers on this rack 
			ResourceRequest rackLocalRequest = application.GetResourceRequest(priority, node.
				GetRackName());
			if (rackLocalRequest == null || rackLocalRequest.GetNumContainers() <= 0)
			{
				return false;
			}
			// If we are here, we do need containers on this rack for RACK_LOCAL req
			if (type == NodeType.RackLocal)
			{
				// 'Delay' rack-local just a little bit...
				long missedOpportunities = application.GetSchedulingOpportunities(priority);
				return (Math.Min(scheduler.GetNumClusterNodes(), GetNodeLocalityDelay()) < missedOpportunities
					);
			}
			// Check if we need containers on this host
			if (type == NodeType.NodeLocal)
			{
				// Now check if we need containers on this host...
				ResourceRequest nodeLocalRequest = application.GetResourceRequest(priority, node.
					GetNodeName());
				if (nodeLocalRequest != null)
				{
					return nodeLocalRequest.GetNumContainers() > 0;
				}
			}
			return false;
		}

		private Container GetContainer(RMContainer rmContainer, FiCaSchedulerApp application
			, FiCaSchedulerNode node, Org.Apache.Hadoop.Yarn.Api.Records.Resource capability
			, Priority priority)
		{
			return (rmContainer != null) ? rmContainer.GetContainer() : CreateContainer(application
				, node, capability, priority);
		}

		internal virtual Container CreateContainer(FiCaSchedulerApp application, FiCaSchedulerNode
			 node, Org.Apache.Hadoop.Yarn.Api.Records.Resource capability, Priority priority
			)
		{
			NodeId nodeId = node.GetRMNode().GetNodeID();
			ContainerId containerId = BuilderUtils.NewContainerId(application.GetApplicationAttemptId
				(), application.GetNewContainerId());
			// Create the container
			Container container = BuilderUtils.NewContainer(containerId, nodeId, node.GetRMNode
				().GetHttpAddress(), capability, priority, null);
			return container;
		}

		private Org.Apache.Hadoop.Yarn.Api.Records.Resource AssignContainer(Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 clusterResource, FiCaSchedulerNode node, FiCaSchedulerApp application, Priority
			 priority, ResourceRequest request, NodeType type, RMContainer rmContainer, MutableObject
			 createdContainer, ResourceLimits currentResoureLimits)
		{
			if (Log.IsDebugEnabled())
			{
				Log.Debug("assignContainers: node=" + node.GetNodeName() + " application=" + application
					.GetApplicationId() + " priority=" + priority.GetPriority() + " request=" + request
					 + " type=" + type);
			}
			// check if the resource request can access the label
			if (!SchedulerUtils.CheckNodeLabelExpression(node.GetLabels(), request.GetNodeLabelExpression
				()))
			{
				// this is a reserved container, but we cannot allocate it now according
				// to label not match. This can be caused by node label changed
				// We should un-reserve this container.
				if (rmContainer != null)
				{
					Unreserve(application, priority, node, rmContainer);
				}
				return Resources.None();
			}
			Org.Apache.Hadoop.Yarn.Api.Records.Resource capability = request.GetCapability();
			Org.Apache.Hadoop.Yarn.Api.Records.Resource available = node.GetAvailableResource
				();
			Org.Apache.Hadoop.Yarn.Api.Records.Resource totalResource = node.GetTotalResource
				();
			if (!Resources.LessThanOrEqual(resourceCalculator, clusterResource, capability, totalResource
				))
			{
				Log.Warn("Node : " + node.GetNodeID() + " does not have sufficient resource for request : "
					 + request + " node total capability : " + node.GetTotalResource());
				return Resources.None();
			}
			System.Diagnostics.Debug.Assert(Resources.GreaterThan(resourceCalculator, clusterResource
				, available, Resources.None()));
			// Create the container if necessary
			Container container = GetContainer(rmContainer, application, node, capability, priority
				);
			// something went wrong getting/creating the container 
			if (container == null)
			{
				Log.Warn("Couldn't get container for allocation!");
				return Resources.None();
			}
			bool shouldAllocOrReserveNewContainer = ShouldAllocOrReserveNewContainer(application
				, priority, capability);
			// Can we allocate a container on this node?
			int availableContainers = resourceCalculator.ComputeAvailableContainers(available
				, capability);
			bool needToUnreserve = Resources.GreaterThan(resourceCalculator, clusterResource, 
				currentResoureLimits.GetAmountNeededUnreserve(), Resources.None());
			if (availableContainers > 0)
			{
				// Allocate...
				// Did we previously reserve containers at this 'priority'?
				if (rmContainer != null)
				{
					Unreserve(application, priority, node, rmContainer);
				}
				else
				{
					if (this.reservationsContinueLooking && node.GetLabels().IsEmpty())
					{
						// when reservationsContinueLooking is set, we may need to unreserve
						// some containers to meet this queue, its parents', or the users' resource limits.
						// TODO, need change here when we want to support continuous reservation
						// looking for labeled partitions.
						if (!shouldAllocOrReserveNewContainer || needToUnreserve)
						{
							// If we shouldn't allocate/reserve new container then we should unreserve one the same
							// size we are asking for since the currentResoureLimits.getAmountNeededUnreserve
							// could be zero. If the limit was hit then use the amount we need to unreserve to be
							// under the limit.
							Org.Apache.Hadoop.Yarn.Api.Records.Resource amountToUnreserve = capability;
							if (needToUnreserve)
							{
								amountToUnreserve = currentResoureLimits.GetAmountNeededUnreserve();
							}
							bool containerUnreserved = FindNodeToUnreserve(clusterResource, node, application
								, priority, amountToUnreserve);
							// When (minimum-unreserved-resource > 0 OR we cannot allocate new/reserved
							// container (That means we *have to* unreserve some resource to
							// continue)). If we failed to unreserve some resource, we can't continue.
							if (!containerUnreserved)
							{
								return Resources.None();
							}
						}
					}
				}
				// Inform the application
				RMContainer allocatedContainer = application.Allocate(type, node, priority, request
					, container);
				// Does the application need this resource?
				if (allocatedContainer == null)
				{
					return Resources.None();
				}
				// Inform the node
				node.AllocateContainer(allocatedContainer);
				Log.Info("assignedContainer" + " application attempt=" + application.GetApplicationAttemptId
					() + " container=" + container + " queue=" + this + " clusterResource=" + clusterResource
					);
				createdContainer.SetValue(allocatedContainer);
				return container.GetResource();
			}
			else
			{
				// if we are allowed to allocate but this node doesn't have space, reserve it or
				// if this was an already a reserved container, reserve it again
				if (shouldAllocOrReserveNewContainer || rmContainer != null)
				{
					if (reservationsContinueLooking && rmContainer == null)
					{
						// we could possibly ignoring queue capacity or user limits when
						// reservationsContinueLooking is set. Make sure we didn't need to unreserve
						// one.
						if (needToUnreserve)
						{
							if (Log.IsDebugEnabled())
							{
								Log.Debug("we needed to unreserve to be able to allocate");
							}
							return Resources.None();
						}
					}
					// Reserve by 'charging' in advance...
					Reserve(application, priority, node, rmContainer, container);
					Log.Info("Reserved container " + " application=" + application.GetApplicationId()
						 + " resource=" + request.GetCapability() + " queue=" + this.ToString() + " usedCapacity="
						 + GetUsedCapacity() + " absoluteUsedCapacity=" + GetAbsoluteUsedCapacity() + " used="
						 + queueUsage.GetUsed() + " cluster=" + clusterResource);
					return request.GetCapability();
				}
				return Resources.None();
			}
		}

		private void Reserve(FiCaSchedulerApp application, Priority priority, FiCaSchedulerNode
			 node, RMContainer rmContainer, Container container)
		{
			// Update reserved metrics if this is the first reservation
			if (rmContainer == null)
			{
				GetMetrics().ReserveResource(application.GetUser(), container.GetResource());
			}
			// Inform the application 
			rmContainer = application.Reserve(node, priority, rmContainer, container);
			// Update the node
			node.ReserveResource(application, priority, rmContainer);
		}

		private bool Unreserve(FiCaSchedulerApp application, Priority priority, FiCaSchedulerNode
			 node, RMContainer rmContainer)
		{
			// Done with the reservation?
			if (application.Unreserve(node, priority))
			{
				node.UnreserveResource(application);
				// Update reserved metrics
				GetMetrics().UnreserveResource(application.GetUser(), rmContainer.GetContainer().
					GetResource());
				return true;
			}
			return false;
		}

		public override void CompletedContainer(Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 clusterResource, FiCaSchedulerApp application, FiCaSchedulerNode node, RMContainer
			 rmContainer, ContainerStatus containerStatus, RMContainerEventType @event, CSQueue
			 childQueue, bool sortQueues)
		{
			if (application != null)
			{
				bool removed = false;
				// Careful! Locking order is important!
				lock (this)
				{
					Container container = rmContainer.GetContainer();
					// Inform the application & the node
					// Note: It's safe to assume that all state changes to RMContainer
					// happen under scheduler's lock... 
					// So, this is, in effect, a transaction across application & node
					if (rmContainer.GetState() == RMContainerState.Reserved)
					{
						removed = Unreserve(application, rmContainer.GetReservedPriority(), node, rmContainer
							);
					}
					else
					{
						removed = application.ContainerCompleted(rmContainer, containerStatus, @event);
						node.ReleaseContainer(container);
					}
					// Book-keeping
					if (removed)
					{
						ReleaseResource(clusterResource, application, container.GetResource(), node.GetLabels
							());
						Log.Info("completedContainer" + " container=" + container + " queue=" + this + " cluster="
							 + clusterResource);
					}
				}
				if (removed)
				{
					// Inform the parent queue _outside_ of the leaf-queue lock
					GetParent().CompletedContainer(clusterResource, application, node, rmContainer, null
						, @event, this, sortQueues);
				}
			}
		}

		internal virtual void AllocateResource(Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 clusterResource, SchedulerApplicationAttempt application, Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 resource, ICollection<string> nodeLabels)
		{
			lock (this)
			{
				base.AllocateResource(clusterResource, resource, nodeLabels);
				// Update user metrics
				string userName = application.GetUser();
				LeafQueue.User user = GetUser(userName);
				user.AssignContainer(resource, nodeLabels);
				// Note this is a bit unconventional since it gets the object and modifies
				// it here, rather then using set routine
				Resources.SubtractFrom(application.GetHeadroom(), resource);
				// headroom
				metrics.SetAvailableResourcesToUser(userName, application.GetHeadroom());
				if (Log.IsDebugEnabled())
				{
					Log.Info(GetQueueName() + " user=" + userName + " used=" + queueUsage.GetUsed() +
						 " numContainers=" + numContainers + " headroom = " + application.GetHeadroom() 
						+ " user-resources=" + user.GetUsed());
				}
			}
		}

		internal virtual void ReleaseResource(Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 clusterResource, FiCaSchedulerApp application, Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 resource, ICollection<string> nodeLabels)
		{
			lock (this)
			{
				base.ReleaseResource(clusterResource, resource, nodeLabels);
				// Update user metrics
				string userName = application.GetUser();
				LeafQueue.User user = GetUser(userName);
				user.ReleaseContainer(resource, nodeLabels);
				metrics.SetAvailableResourcesToUser(userName, application.GetHeadroom());
				Log.Info(GetQueueName() + " used=" + queueUsage.GetUsed() + " numContainers=" + numContainers
					 + " user=" + userName + " user-resources=" + user.GetUsed());
			}
		}

		private void UpdateAbsoluteCapacityResource(Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 clusterResource)
		{
			absoluteCapacityResource = Resources.MultiplyAndNormalizeUp(resourceCalculator, clusterResource
				, queueCapacities.GetAbsoluteCapacity(), minimumAllocation);
		}

		private void UpdateCurrentResourceLimits(ResourceLimits currentResourceLimits, Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 clusterResource)
		{
			// TODO: need consider non-empty node labels when resource limits supports
			// node labels
			// Even if ParentQueue will set limits respect child's max queue capacity,
			// but when allocating reserved container, CapacityScheduler doesn't do
			// this. So need cap limits by queue's max capacity here.
			this.cachedResourceLimitsForHeadroom = new ResourceLimits(currentResourceLimits.GetLimit
				());
			Org.Apache.Hadoop.Yarn.Api.Records.Resource queueMaxResource = Resources.MultiplyAndNormalizeDown
				(resourceCalculator, labelManager.GetResourceByLabel(RMNodeLabelsManager.NoLabel
				, clusterResource), queueCapacities.GetAbsoluteMaximumCapacity(RMNodeLabelsManager
				.NoLabel), minimumAllocation);
			this.cachedResourceLimitsForHeadroom.SetLimit(Resources.Min(resourceCalculator, clusterResource
				, queueMaxResource, currentResourceLimits.GetLimit()));
		}

		public override void UpdateClusterResource(Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 clusterResource, ResourceLimits currentResourceLimits)
		{
			lock (this)
			{
				UpdateCurrentResourceLimits(currentResourceLimits, clusterResource);
				lastClusterResource = clusterResource;
				UpdateAbsoluteCapacityResource(clusterResource);
				// Update headroom info based on new cluster resource value
				// absoluteMaxCapacity now,  will be replaced with absoluteMaxAvailCapacity
				// during allocation
				SetQueueResourceLimitsInfo(clusterResource);
				// Update metrics
				CSQueueUtils.UpdateQueueStatistics(resourceCalculator, this, GetParent(), clusterResource
					, minimumAllocation);
				// queue metrics are updated, more resource may be available
				// activate the pending applications if possible
				ActivateApplications();
				// Update application properties
				foreach (FiCaSchedulerApp application in activeApplications)
				{
					lock (application)
					{
						ComputeUserLimitAndSetHeadroom(application, clusterResource, Resources.None(), null
							);
					}
				}
			}
		}

		public class User
		{
			internal ResourceUsage userResourceUsage = new ResourceUsage();

			internal volatile Org.Apache.Hadoop.Yarn.Api.Records.Resource userResourceLimit = 
				Org.Apache.Hadoop.Yarn.Api.Records.Resource.NewInstance(0, 0);

			internal int pendingApplications = 0;

			internal int activeApplications = 0;

			public virtual ResourceUsage GetResourceUsage()
			{
				return userResourceUsage;
			}

			public virtual Org.Apache.Hadoop.Yarn.Api.Records.Resource GetUsed()
			{
				return userResourceUsage.GetUsed();
			}

			public virtual Org.Apache.Hadoop.Yarn.Api.Records.Resource GetUsed(string label)
			{
				return userResourceUsage.GetUsed(label);
			}

			public virtual int GetPendingApplications()
			{
				return pendingApplications;
			}

			public virtual int GetActiveApplications()
			{
				return activeApplications;
			}

			public virtual Org.Apache.Hadoop.Yarn.Api.Records.Resource GetConsumedAMResources
				()
			{
				return userResourceUsage.GetAMUsed();
			}

			public virtual int GetTotalApplications()
			{
				return GetPendingApplications() + GetActiveApplications();
			}

			public virtual void SubmitApplication()
			{
				lock (this)
				{
					++pendingApplications;
				}
			}

			public virtual void ActivateApplication()
			{
				lock (this)
				{
					--pendingApplications;
					++activeApplications;
				}
			}

			public virtual void FinishApplication(bool wasActive)
			{
				lock (this)
				{
					if (wasActive)
					{
						--activeApplications;
					}
					else
					{
						--pendingApplications;
					}
				}
			}

			public virtual void AssignContainer(Org.Apache.Hadoop.Yarn.Api.Records.Resource resource
				, ICollection<string> nodeLabels)
			{
				if (nodeLabels == null || nodeLabels.IsEmpty())
				{
					userResourceUsage.IncUsed(resource);
				}
				else
				{
					foreach (string label in nodeLabels)
					{
						userResourceUsage.IncUsed(label, resource);
					}
				}
			}

			public virtual void ReleaseContainer(Org.Apache.Hadoop.Yarn.Api.Records.Resource 
				resource, ICollection<string> nodeLabels)
			{
				if (nodeLabels == null || nodeLabels.IsEmpty())
				{
					userResourceUsage.DecUsed(resource);
				}
				else
				{
					foreach (string label in nodeLabels)
					{
						userResourceUsage.DecUsed(label, resource);
					}
				}
			}

			public virtual Org.Apache.Hadoop.Yarn.Api.Records.Resource GetUserResourceLimit()
			{
				return userResourceLimit;
			}

			public virtual void SetUserResourceLimit(Org.Apache.Hadoop.Yarn.Api.Records.Resource
				 userResourceLimit)
			{
				this.userResourceLimit = userResourceLimit;
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
				AllocateResource(clusterResource, attempt, rmContainer.GetContainer().GetResource
					(), node.GetLabels());
			}
			GetParent().RecoverContainer(clusterResource, attempt, rmContainer);
		}

		/// <summary>Obtain (read-only) collection of active applications.</summary>
		public virtual ICollection<FiCaSchedulerApp> GetApplications()
		{
			// need to access the list of apps from the preemption monitor
			return activeApplications;
		}

		// return a single Resource capturing the overal amount of pending resources
		public virtual Org.Apache.Hadoop.Yarn.Api.Records.Resource GetTotalResourcePending
			()
		{
			lock (this)
			{
				Org.Apache.Hadoop.Yarn.Api.Records.Resource ret = BuilderUtils.NewResource(0, 0);
				foreach (FiCaSchedulerApp f in activeApplications)
				{
					Resources.AddTo(ret, f.GetTotalPendingRequests());
				}
				return ret;
			}
		}

		public override void CollectSchedulerApplications(ICollection<ApplicationAttemptId
			> apps)
		{
			lock (this)
			{
				foreach (FiCaSchedulerApp pendingApp in pendingApplications)
				{
					apps.AddItem(pendingApp.GetApplicationAttemptId());
				}
				foreach (FiCaSchedulerApp app in activeApplications)
				{
					apps.AddItem(app.GetApplicationAttemptId());
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
				AllocateResource(clusterResource, application, rmContainer.GetContainer().GetResource
					(), node.GetLabels());
				Log.Info("movedContainer" + " container=" + rmContainer.GetContainer() + " resource="
					 + rmContainer.GetContainer().GetResource() + " queueMoveIn=" + this + " usedCapacity="
					 + GetUsedCapacity() + " absoluteUsedCapacity=" + GetAbsoluteUsedCapacity() + " used="
					 + queueUsage.GetUsed() + " cluster=" + clusterResource);
				// Inform the parent queue
				GetParent().AttachContainer(clusterResource, application, rmContainer);
			}
		}

		public override void DetachContainer(Org.Apache.Hadoop.Yarn.Api.Records.Resource 
			clusterResource, FiCaSchedulerApp application, RMContainer rmContainer)
		{
			if (application != null)
			{
				FiCaSchedulerNode node = scheduler.GetNode(rmContainer.GetContainer().GetNodeId()
					);
				ReleaseResource(clusterResource, application, rmContainer.GetContainer().GetResource
					(), node.GetLabels());
				Log.Info("movedContainer" + " container=" + rmContainer.GetContainer() + " resource="
					 + rmContainer.GetContainer().GetResource() + " queueMoveOut=" + this + " usedCapacity="
					 + GetUsedCapacity() + " absoluteUsedCapacity=" + GetAbsoluteUsedCapacity() + " used="
					 + queueUsage.GetUsed() + " cluster=" + clusterResource);
				// Inform the parent queue
				GetParent().DetachContainer(clusterResource, application, rmContainer);
			}
		}

		public virtual void SetCapacity(float capacity)
		{
			queueCapacities.SetCapacity(capacity);
		}

		public virtual void SetAbsoluteCapacity(float absoluteCapacity)
		{
			queueCapacities.SetAbsoluteCapacity(absoluteCapacity);
		}

		public virtual void SetMaxApplications(int maxApplications)
		{
			this.maxApplications = maxApplications;
		}

		internal class QueueResourceLimitsInfo
		{
			private Org.Apache.Hadoop.Yarn.Api.Records.Resource queueCurrentLimit;

			private Org.Apache.Hadoop.Yarn.Api.Records.Resource clusterResource;

			/*
			* Holds shared values used by all applications in
			* the queue to calculate headroom on demand
			*/
			public virtual void SetQueueCurrentLimit(Org.Apache.Hadoop.Yarn.Api.Records.Resource
				 currentLimit)
			{
				this.queueCurrentLimit = currentLimit;
			}

			public virtual Org.Apache.Hadoop.Yarn.Api.Records.Resource GetQueueCurrentLimit()
			{
				return queueCurrentLimit;
			}

			public virtual void SetClusterResource(Org.Apache.Hadoop.Yarn.Api.Records.Resource
				 clusterResource)
			{
				this.clusterResource = clusterResource;
			}

			public virtual Org.Apache.Hadoop.Yarn.Api.Records.Resource GetClusterResource()
			{
				return clusterResource;
			}
		}
	}
}
