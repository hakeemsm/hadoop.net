using System;
using System.Collections.Generic;
using Com.Google.Common.Collect;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Security.Authorize;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Nodelabels;
using Org.Apache.Hadoop.Yarn.Security;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Nodelabels;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Reservation;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler;
using Org.Apache.Hadoop.Yarn.Util.Resource;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Capacity
{
	public class CapacitySchedulerConfiguration : ReservationSchedulerConfiguration
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Capacity.CapacitySchedulerConfiguration
			));

		private const string CsConfigurationFile = "capacity-scheduler.xml";

		[InterfaceAudience.Private]
		public const string Prefix = "yarn.scheduler.capacity.";

		[InterfaceAudience.Private]
		public const string Dot = ".";

		[InterfaceAudience.Private]
		public const string MaximumApplicationsSuffix = "maximum-applications";

		[InterfaceAudience.Private]
		public const string MaximumSystemApplications = Prefix + MaximumApplicationsSuffix;

		[InterfaceAudience.Private]
		public const string MaximumAmResourceSuffix = "maximum-am-resource-percent";

		[InterfaceAudience.Private]
		public const string MaximumApplicationMastersResourcePercent = Prefix + MaximumAmResourceSuffix;

		[InterfaceAudience.Private]
		public const string Queues = "queues";

		[InterfaceAudience.Private]
		public const string Capacity = "capacity";

		[InterfaceAudience.Private]
		public const string MaximumCapacity = "maximum-capacity";

		[InterfaceAudience.Private]
		public const string UserLimit = "minimum-user-limit-percent";

		[InterfaceAudience.Private]
		public const string UserLimitFactor = "user-limit-factor";

		[InterfaceAudience.Private]
		public const string State = "state";

		[InterfaceAudience.Private]
		public const string AccessibleNodeLabels = "accessible-node-labels";

		[InterfaceAudience.Private]
		public const string DefaultNodeLabelExpression = "default-node-label-expression";

		public const string ReserveContLookAllNodes = Prefix + "reservations-continue-look-all-nodes";

		[InterfaceAudience.Private]
		public const bool DefaultReserveContLookAllNodes = true;

		[InterfaceAudience.Private]
		public const string MaximumAllocationMb = "maximum-allocation-mb";

		[InterfaceAudience.Private]
		public const string MaximumAllocationVcores = "maximum-allocation-vcores";

		[InterfaceAudience.Private]
		public const int DefaultMaximumSystemApplicatiions = 10000;

		[InterfaceAudience.Private]
		public const float DefaultMaximumApplicationmastersResourcePercent = 0.1f;

		[InterfaceAudience.Private]
		public const float Undefined = -1;

		[InterfaceAudience.Private]
		public const float MinimumCapacityValue = 0;

		[InterfaceAudience.Private]
		public const float MaximumCapacityValue = 100;

		[InterfaceAudience.Private]
		public const float DefaultMaximumCapacityValue = -1.0f;

		[InterfaceAudience.Private]
		public const int DefaultUserLimit = 100;

		[InterfaceAudience.Private]
		public const float DefaultUserLimitFactor = 1.0f;

		[InterfaceAudience.Private]
		public const string AllAcl = "*";

		[InterfaceAudience.Private]
		public const string NoneAcl = " ";

		[InterfaceAudience.Private]
		public const string EnableUserMetrics = Prefix + "user-metrics.enable";

		[InterfaceAudience.Private]
		public const bool DefaultEnableUserMetrics = false;

		/// <summary>ResourceComparator for scheduling.</summary>
		[InterfaceAudience.Private]
		public const string ResourceCalculatorClass = Prefix + "resource-calculator";

		[InterfaceAudience.Private]
		public static readonly Type DefaultResourceCalculatorClass = typeof(DefaultResourceCalculator
			);

		[InterfaceAudience.Private]
		public const string Root = "root";

		[InterfaceAudience.Private]
		public const string NodeLocalityDelay = Prefix + "node-locality-delay";

		[InterfaceAudience.Private]
		public const int DefaultNodeLocalityDelay = -1;

		[InterfaceAudience.Private]
		public const string ScheduleAsynchronouslyPrefix = Prefix + "schedule-asynchronously";

		[InterfaceAudience.Private]
		public const string ScheduleAsynchronouslyEnable = ScheduleAsynchronouslyPrefix +
			 ".enable";

		[InterfaceAudience.Private]
		public const bool DefaultScheduleAsynchronouslyEnable = false;

		[InterfaceAudience.Private]
		public const string QueueMapping = Prefix + "queue-mappings";

		[InterfaceAudience.Private]
		public const string EnableQueueMappingOverride = QueueMapping + "-override.enable";

		[InterfaceAudience.Private]
		public const bool DefaultEnableQueueMappingOverride = false;

		[InterfaceAudience.Private]
		public const string QueuePreemptionDisabled = "disable_preemption";

		public class QueueMapping
		{
			[System.Serializable]
			public sealed class MappingType
			{
				public static readonly CapacitySchedulerConfiguration.QueueMapping.MappingType User
					 = new CapacitySchedulerConfiguration.QueueMapping.MappingType("u");

				public static readonly CapacitySchedulerConfiguration.QueueMapping.MappingType Group
					 = new CapacitySchedulerConfiguration.QueueMapping.MappingType("g");

				private readonly string type;

				private MappingType(string type)
				{
					this.type = type;
				}

				public override string ToString()
				{
					return CapacitySchedulerConfiguration.QueueMapping.MappingType.type;
				}
			}

			internal CapacitySchedulerConfiguration.QueueMapping.MappingType type;

			internal string source;

			internal string queue;

			public QueueMapping(CapacitySchedulerConfiguration.QueueMapping.MappingType type, 
				string source, string queue)
			{
				this.type = type;
				this.source = source;
				this.queue = queue;
			}
		}

		[InterfaceAudience.Private]
		public const string AverageCapacity = "average-capacity";

		[InterfaceAudience.Private]
		public const string IsReservable = "reservable";

		[InterfaceAudience.Private]
		public const string ReservationWindow = "reservation-window";

		[InterfaceAudience.Private]
		public const string InstantaneousMaxCapacity = "instantaneous-max-capacity";

		[InterfaceAudience.Private]
		public const string ReservationAdmissionPolicy = "reservation-policy";

		[InterfaceAudience.Private]
		public const string ReservationAgentName = "reservation-agent";

		[InterfaceAudience.Private]
		public const string ReservationShowReservationAsQueue = "show-reservations-as-queues";

		[InterfaceAudience.Private]
		public const string ReservationPlannerName = "reservation-planner";

		[InterfaceAudience.Private]
		public const string ReservationMoveOnExpiry = "reservation-move-on-expiry";

		[InterfaceAudience.Private]
		public const string ReservationEnforcementWindow = "reservation-enforcement-window";

		public CapacitySchedulerConfiguration()
			: this(new Configuration())
		{
		}

		public CapacitySchedulerConfiguration(Configuration configuration)
			: this(configuration, true)
		{
		}

		public CapacitySchedulerConfiguration(Configuration configuration, bool useLocalConfigurationProvider
			)
			: base(configuration)
		{
			if (useLocalConfigurationProvider)
			{
				AddResource(CsConfigurationFile);
			}
		}

		internal static string GetQueuePrefix(string queue)
		{
			string queueName = Prefix + queue + Dot;
			return queueName;
		}

		private string GetNodeLabelPrefix(string queue, string label)
		{
			if (label.Equals(CommonNodeLabelsManager.NoLabel))
			{
				return GetQueuePrefix(queue);
			}
			return GetQueuePrefix(queue) + AccessibleNodeLabels + Dot + label + Dot;
		}

		public virtual int GetMaximumSystemApplications()
		{
			int maxApplications = GetInt(MaximumSystemApplications, DefaultMaximumSystemApplicatiions
				);
			return maxApplications;
		}

		public virtual float GetMaximumApplicationMasterResourcePercent()
		{
			return GetFloat(MaximumApplicationMastersResourcePercent, DefaultMaximumApplicationmastersResourcePercent
				);
		}

		/// <summary>Get the maximum applications per queue setting.</summary>
		/// <param name="queue">name of the queue</param>
		/// <returns>setting specified or -1 if not set</returns>
		public virtual int GetMaximumApplicationsPerQueue(string queue)
		{
			int maxApplicationsPerQueue = GetInt(GetQueuePrefix(queue) + MaximumApplicationsSuffix
				, (int)Undefined);
			return maxApplicationsPerQueue;
		}

		/// <summary>Get the maximum am resource percent per queue setting.</summary>
		/// <param name="queue">name of the queue</param>
		/// <returns>
		/// per queue setting or defaults to the global am-resource-percent
		/// setting if per queue setting not present
		/// </returns>
		public virtual float GetMaximumApplicationMasterResourcePerQueuePercent(string queue
			)
		{
			return GetFloat(GetQueuePrefix(queue) + MaximumAmResourceSuffix, GetMaximumApplicationMasterResourcePercent
				());
		}

		public virtual float GetNonLabeledQueueCapacity(string queue)
		{
			float capacity = queue.Equals("root") ? 100.0f : GetFloat(GetQueuePrefix(queue) +
				 Capacity, Undefined);
			if (capacity < MinimumCapacityValue || capacity > MaximumCapacityValue)
			{
				throw new ArgumentException("Illegal " + "capacity of " + capacity + " for queue "
					 + queue);
			}
			Log.Debug("CSConf - getCapacity: queuePrefix=" + GetQueuePrefix(queue) + ", capacity="
				 + capacity);
			return capacity;
		}

		public virtual void SetCapacity(string queue, float capacity)
		{
			if (queue.Equals("root"))
			{
				throw new ArgumentException("Cannot set capacity, root queue has a fixed capacity of 100.0f"
					);
			}
			SetFloat(GetQueuePrefix(queue) + Capacity, capacity);
			Log.Debug("CSConf - setCapacity: queuePrefix=" + GetQueuePrefix(queue) + ", capacity="
				 + capacity);
		}

		public virtual float GetNonLabeledQueueMaximumCapacity(string queue)
		{
			float maxCapacity = GetFloat(GetQueuePrefix(queue) + MaximumCapacity, MaximumCapacityValue
				);
			maxCapacity = (maxCapacity == DefaultMaximumCapacityValue) ? MaximumCapacityValue
				 : maxCapacity;
			return maxCapacity;
		}

		public virtual void SetMaximumCapacity(string queue, float maxCapacity)
		{
			if (maxCapacity > MaximumCapacityValue)
			{
				throw new ArgumentException("Illegal " + "maximum-capacity of " + maxCapacity + " for queue "
					 + queue);
			}
			SetFloat(GetQueuePrefix(queue) + MaximumCapacity, maxCapacity);
			Log.Debug("CSConf - setMaxCapacity: queuePrefix=" + GetQueuePrefix(queue) + ", maxCapacity="
				 + maxCapacity);
		}

		public virtual void SetCapacityByLabel(string queue, string label, float capacity
			)
		{
			SetFloat(GetNodeLabelPrefix(queue, label) + Capacity, capacity);
		}

		public virtual void SetMaximumCapacityByLabel(string queue, string label, float capacity
			)
		{
			SetFloat(GetNodeLabelPrefix(queue, label) + MaximumCapacity, capacity);
		}

		public virtual int GetUserLimit(string queue)
		{
			int userLimit = GetInt(GetQueuePrefix(queue) + UserLimit, DefaultUserLimit);
			return userLimit;
		}

		public virtual void SetUserLimit(string queue, int userLimit)
		{
			SetInt(GetQueuePrefix(queue) + UserLimit, userLimit);
			Log.Debug("here setUserLimit: queuePrefix=" + GetQueuePrefix(queue) + ", userLimit="
				 + GetUserLimit(queue));
		}

		public virtual float GetUserLimitFactor(string queue)
		{
			float userLimitFactor = GetFloat(GetQueuePrefix(queue) + UserLimitFactor, DefaultUserLimitFactor
				);
			return userLimitFactor;
		}

		public virtual void SetUserLimitFactor(string queue, float userLimitFactor)
		{
			SetFloat(GetQueuePrefix(queue) + UserLimitFactor, userLimitFactor);
		}

		public virtual QueueState GetState(string queue)
		{
			string state = Get(GetQueuePrefix(queue) + State);
			return (state != null) ? QueueState.ValueOf(StringUtils.ToUpperCase(state)) : QueueState
				.Running;
		}

		public virtual void SetAccessibleNodeLabels(string queue, ICollection<string> labels
			)
		{
			if (labels == null)
			{
				return;
			}
			string str = StringUtils.Join(",", labels);
			Set(GetQueuePrefix(queue) + AccessibleNodeLabels, str);
		}

		public virtual ICollection<string> GetAccessibleNodeLabels(string queue)
		{
			string accessibleLabelStr = Get(GetQueuePrefix(queue) + AccessibleNodeLabels);
			// When accessible-label is null, 
			if (accessibleLabelStr == null)
			{
				// Only return null when queue is not ROOT
				if (!queue.Equals(Root))
				{
					return null;
				}
			}
			else
			{
				// print a warning when accessibleNodeLabel specified in config and queue
				// is ROOT
				if (queue.Equals(Root))
				{
					Log.Warn("Accessible node labels for root queue will be ignored," + " it will be automatically set to \"*\"."
						);
				}
			}
			// always return ANY for queue root
			if (queue.Equals(Root))
			{
				return ImmutableSet.Of(RMNodeLabelsManager.Any);
			}
			// In other cases, split the accessibleLabelStr by ","
			ICollection<string> set = new HashSet<string>();
			foreach (string str in accessibleLabelStr.Split(","))
			{
				if (!str.Trim().IsEmpty())
				{
					set.AddItem(str.Trim());
				}
			}
			// if labels contains "*", only keep ANY behind
			if (set.Contains(RMNodeLabelsManager.Any))
			{
				set.Clear();
				set.AddItem(RMNodeLabelsManager.Any);
			}
			return Sharpen.Collections.UnmodifiableSet(set);
		}

		private float InternalGetLabeledQueueCapacity(string queue, string label, string 
			suffix, float defaultValue)
		{
			string capacityPropertyName = GetNodeLabelPrefix(queue, label) + suffix;
			float capacity = GetFloat(capacityPropertyName, defaultValue);
			if (capacity < MinimumCapacityValue || capacity > MaximumCapacityValue)
			{
				throw new ArgumentException("Illegal capacity of " + capacity + " for node-label="
					 + label + " in queue=" + queue + ", valid capacity should in range of [0, 100]."
					);
			}
			if (Log.IsDebugEnabled())
			{
				Log.Debug("CSConf - getCapacityOfLabel: prefix=" + GetNodeLabelPrefix(queue, label
					) + ", capacity=" + capacity);
			}
			return capacity;
		}

		public virtual float GetLabeledQueueCapacity(string queue, string label)
		{
			return InternalGetLabeledQueueCapacity(queue, label, Capacity, 0f);
		}

		public virtual float GetLabeledQueueMaximumCapacity(string queue, string label)
		{
			return InternalGetLabeledQueueCapacity(queue, label, MaximumCapacity, 100f);
		}

		public virtual string GetDefaultNodeLabelExpression(string queue)
		{
			string defaultLabelExpression = Get(GetQueuePrefix(queue) + DefaultNodeLabelExpression
				);
			if (defaultLabelExpression == null)
			{
				return null;
			}
			return defaultLabelExpression.Trim();
		}

		public virtual void SetDefaultNodeLabelExpression(string queue, string exp)
		{
			Set(GetQueuePrefix(queue) + DefaultNodeLabelExpression, exp);
		}

		/*
		* Returns whether we should continue to look at all heart beating nodes even
		* after the reservation limit was hit. The node heart beating in could
		* satisfy the request thus could be a better pick then waiting for the
		* reservation to be fullfilled.  This config is refreshable.
		*/
		public virtual bool GetReservationContinueLook()
		{
			return GetBoolean(ReserveContLookAllNodes, DefaultReserveContLookAllNodes);
		}

		private static string GetAclKey(QueueACL acl)
		{
			return "acl_" + StringUtils.ToLowerCase(acl.ToString());
		}

		public virtual AccessControlList GetAcl(string queue, QueueACL acl)
		{
			string queuePrefix = GetQueuePrefix(queue);
			// The root queue defaults to all access if not defined
			// Sub queues inherit access if not defined
			string defaultAcl = queue.Equals(Root) ? AllAcl : NoneAcl;
			string aclString = Get(queuePrefix + GetAclKey(acl), defaultAcl);
			return new AccessControlList(aclString);
		}

		public virtual void SetAcl(string queue, QueueACL acl, string aclString)
		{
			string queuePrefix = GetQueuePrefix(queue);
			Set(queuePrefix + GetAclKey(acl), aclString);
		}

		public virtual IDictionary<AccessType, AccessControlList> GetAcls(string queue)
		{
			IDictionary<AccessType, AccessControlList> acls = new Dictionary<AccessType, AccessControlList
				>();
			foreach (QueueACL acl in QueueACL.Values())
			{
				acls[SchedulerUtils.ToAccessType(acl)] = GetAcl(queue, acl);
			}
			return acls;
		}

		public virtual void SetAcls(string queue, IDictionary<QueueACL, AccessControlList
			> acls)
		{
			foreach (KeyValuePair<QueueACL, AccessControlList> e in acls)
			{
				SetAcl(queue, e.Key, e.Value.GetAclString());
			}
		}

		public virtual string[] GetQueues(string queue)
		{
			Log.Debug("CSConf - getQueues called for: queuePrefix=" + GetQueuePrefix(queue));
			string[] queues = GetStrings(GetQueuePrefix(queue) + Queues);
			IList<string> trimmedQueueNames = new AList<string>();
			if (null != queues)
			{
				foreach (string s in queues)
				{
					trimmedQueueNames.AddItem(s.Trim());
				}
				queues = Sharpen.Collections.ToArray(trimmedQueueNames, new string[0]);
			}
			Log.Debug("CSConf - getQueues: queuePrefix=" + GetQueuePrefix(queue) + ", queues="
				 + ((queues == null) ? string.Empty : StringUtils.ArrayToString(queues)));
			return queues;
		}

		public virtual void SetQueues(string queue, string[] subQueues)
		{
			Set(GetQueuePrefix(queue) + Queues, StringUtils.ArrayToString(subQueues));
			Log.Debug("CSConf - setQueues: qPrefix=" + GetQueuePrefix(queue) + ", queues=" + 
				StringUtils.ArrayToString(subQueues));
		}

		public virtual Org.Apache.Hadoop.Yarn.Api.Records.Resource GetMinimumAllocation()
		{
			int minimumMemory = GetInt(YarnConfiguration.RmSchedulerMinimumAllocationMb, YarnConfiguration
				.DefaultRmSchedulerMinimumAllocationMb);
			int minimumCores = GetInt(YarnConfiguration.RmSchedulerMinimumAllocationVcores, YarnConfiguration
				.DefaultRmSchedulerMinimumAllocationVcores);
			return Resources.CreateResource(minimumMemory, minimumCores);
		}

		public virtual Org.Apache.Hadoop.Yarn.Api.Records.Resource GetMaximumAllocation()
		{
			int maximumMemory = GetInt(YarnConfiguration.RmSchedulerMaximumAllocationMb, YarnConfiguration
				.DefaultRmSchedulerMaximumAllocationMb);
			int maximumCores = GetInt(YarnConfiguration.RmSchedulerMaximumAllocationVcores, YarnConfiguration
				.DefaultRmSchedulerMaximumAllocationVcores);
			return Resources.CreateResource(maximumMemory, maximumCores);
		}

		/// <summary>
		/// Get the per queue setting for the maximum limit to allocate to
		/// each container request.
		/// </summary>
		/// <param name="queue">name of the queue</param>
		/// <returns>setting specified per queue else falls back to the cluster setting</returns>
		public virtual Org.Apache.Hadoop.Yarn.Api.Records.Resource GetMaximumAllocationPerQueue
			(string queue)
		{
			string queuePrefix = GetQueuePrefix(queue);
			int maxAllocationMbPerQueue = GetInt(queuePrefix + MaximumAllocationMb, (int)Undefined
				);
			int maxAllocationVcoresPerQueue = GetInt(queuePrefix + MaximumAllocationVcores, (
				int)Undefined);
			if (Log.IsDebugEnabled())
			{
				Log.Debug("max alloc mb per queue for " + queue + " is " + maxAllocationMbPerQueue
					);
				Log.Debug("max alloc vcores per queue for " + queue + " is " + maxAllocationVcoresPerQueue
					);
			}
			Org.Apache.Hadoop.Yarn.Api.Records.Resource clusterMax = GetMaximumAllocation();
			if (maxAllocationMbPerQueue == (int)Undefined)
			{
				Log.Info("max alloc mb per queue for " + queue + " is undefined");
				maxAllocationMbPerQueue = clusterMax.GetMemory();
			}
			if (maxAllocationVcoresPerQueue == (int)Undefined)
			{
				Log.Info("max alloc vcore per queue for " + queue + " is undefined");
				maxAllocationVcoresPerQueue = clusterMax.GetVirtualCores();
			}
			Org.Apache.Hadoop.Yarn.Api.Records.Resource result = Resources.CreateResource(maxAllocationMbPerQueue
				, maxAllocationVcoresPerQueue);
			if (maxAllocationMbPerQueue > clusterMax.GetMemory() || maxAllocationVcoresPerQueue
				 > clusterMax.GetVirtualCores())
			{
				throw new ArgumentException("Queue maximum allocation cannot be larger than the cluster setting"
					 + " for queue " + queue + " max allocation per queue: " + result + " cluster setting: "
					 + clusterMax);
			}
			return result;
		}

		public virtual bool GetEnableUserMetrics()
		{
			return GetBoolean(EnableUserMetrics, DefaultEnableUserMetrics);
		}

		public virtual int GetNodeLocalityDelay()
		{
			int delay = GetInt(NodeLocalityDelay, DefaultNodeLocalityDelay);
			return (delay == DefaultNodeLocalityDelay) ? 0 : delay;
		}

		public virtual ResourceCalculator GetResourceCalculator()
		{
			return ReflectionUtils.NewInstance(GetClass<ResourceCalculator>(ResourceCalculatorClass
				, DefaultResourceCalculatorClass), this);
		}

		public virtual bool GetUsePortForNodeName()
		{
			return GetBoolean(YarnConfiguration.RmSchedulerIncludePortInNodeName, YarnConfiguration
				.DefaultRmSchedulerUsePortForNodeName);
		}

		public virtual void SetResourceComparator(Type resourceCalculatorClass)
		{
			SetClass(ResourceCalculatorClass, resourceCalculatorClass, typeof(ResourceCalculator
				));
		}

		public virtual bool GetScheduleAynschronously()
		{
			return GetBoolean(ScheduleAsynchronouslyEnable, DefaultScheduleAsynchronouslyEnable
				);
		}

		public virtual void SetScheduleAynschronously(bool async)
		{
			SetBoolean(ScheduleAsynchronouslyEnable, async);
		}

		public virtual bool GetOverrideWithQueueMappings()
		{
			return GetBoolean(EnableQueueMappingOverride, DefaultEnableQueueMappingOverride);
		}

		/// <summary>
		/// Returns a collection of strings, trimming leading and trailing whitespeace
		/// on each value
		/// </summary>
		/// <param name="str">String to parse</param>
		/// <param name="delim">delimiter to separate the values</param>
		/// <returns>Collection of parsed elements.</returns>
		private static ICollection<string> GetTrimmedStringCollection(string str, string 
			delim)
		{
			IList<string> values = new AList<string>();
			if (str == null)
			{
				return values;
			}
			StringTokenizer tokenizer = new StringTokenizer(str, delim);
			while (tokenizer.HasMoreTokens())
			{
				string next = tokenizer.NextToken();
				if (next == null || next.Trim().IsEmpty())
				{
					continue;
				}
				values.AddItem(next.Trim());
			}
			return values;
		}

		/// <summary>Get user/group mappings to queues.</summary>
		/// <returns>user/groups mappings or null on illegal configs</returns>
		public virtual IList<CapacitySchedulerConfiguration.QueueMapping> GetQueueMappings
			()
		{
			IList<CapacitySchedulerConfiguration.QueueMapping> mappings = new AList<CapacitySchedulerConfiguration.QueueMapping
				>();
			ICollection<string> mappingsString = GetTrimmedStringCollection(QueueMapping);
			foreach (string mappingValue in mappingsString)
			{
				string[] mapping = Sharpen.Collections.ToArray(GetTrimmedStringCollection(mappingValue
					, ":"), new string[] {  });
				if (mapping.Length != 3 || mapping[1].Length == 0 || mapping[2].Length == 0)
				{
					throw new ArgumentException("Illegal queue mapping " + mappingValue);
				}
				CapacitySchedulerConfiguration.QueueMapping m;
				try
				{
					CapacitySchedulerConfiguration.QueueMapping.MappingType mappingType;
					if (mapping[0].Equals("u"))
					{
						mappingType = CapacitySchedulerConfiguration.QueueMapping.MappingType.User;
					}
					else
					{
						if (mapping[0].Equals("g"))
						{
							mappingType = CapacitySchedulerConfiguration.QueueMapping.MappingType.Group;
						}
						else
						{
							throw new ArgumentException("unknown mapping prefix " + mapping[0]);
						}
					}
					m = new CapacitySchedulerConfiguration.QueueMapping(mappingType, mapping[1], mapping
						[2]);
				}
				catch
				{
					throw new ArgumentException("Illegal queue mapping " + mappingValue);
				}
				if (m != null)
				{
					mappings.AddItem(m);
				}
			}
			return mappings;
		}

		public override bool IsReservable(string queue)
		{
			bool isReservable = GetBoolean(GetQueuePrefix(queue) + IsReservable, false);
			return isReservable;
		}

		public virtual void SetReservable(string queue, bool isReservable)
		{
			SetBoolean(GetQueuePrefix(queue) + IsReservable, isReservable);
			Log.Debug("here setReservableQueue: queuePrefix=" + GetQueuePrefix(queue) + ", isReservableQueue="
				 + IsReservable(queue));
		}

		public override long GetReservationWindow(string queue)
		{
			long reservationWindow = GetLong(GetQueuePrefix(queue) + ReservationWindow, DefaultReservationWindow
				);
			return reservationWindow;
		}

		public override float GetAverageCapacity(string queue)
		{
			float avgCapacity = GetFloat(GetQueuePrefix(queue) + AverageCapacity, MaximumCapacityValue
				);
			return avgCapacity;
		}

		public override float GetInstantaneousMaxCapacity(string queue)
		{
			float instMaxCapacity = GetFloat(GetQueuePrefix(queue) + InstantaneousMaxCapacity
				, MaximumCapacityValue);
			return instMaxCapacity;
		}

		public virtual void SetInstantaneousMaxCapacity(string queue, float instMaxCapacity
			)
		{
			SetFloat(GetQueuePrefix(queue) + InstantaneousMaxCapacity, instMaxCapacity);
		}

		public virtual void SetReservationWindow(string queue, long reservationWindow)
		{
			SetLong(GetQueuePrefix(queue) + ReservationWindow, reservationWindow);
		}

		public virtual void SetAverageCapacity(string queue, float avgCapacity)
		{
			SetFloat(GetQueuePrefix(queue) + AverageCapacity, avgCapacity);
		}

		public override string GetReservationAdmissionPolicy(string queue)
		{
			string reservationPolicy = Get(GetQueuePrefix(queue) + ReservationAdmissionPolicy
				, DefaultReservationAdmissionPolicy);
			return reservationPolicy;
		}

		public virtual void SetReservationAdmissionPolicy(string queue, string reservationPolicy
			)
		{
			Set(GetQueuePrefix(queue) + ReservationAdmissionPolicy, reservationPolicy);
		}

		public override string GetReservationAgent(string queue)
		{
			string reservationAgent = Get(GetQueuePrefix(queue) + ReservationAgentName, DefaultReservationAgentName
				);
			return reservationAgent;
		}

		public virtual void SetReservationAgent(string queue, string reservationPolicy)
		{
			Set(GetQueuePrefix(queue) + ReservationAgentName, reservationPolicy);
		}

		public override bool GetShowReservationAsQueues(string queuePath)
		{
			bool showReservationAsQueues = GetBoolean(GetQueuePrefix(queuePath) + ReservationShowReservationAsQueue
				, DefaultShowReservationsAsQueues);
			return showReservationAsQueues;
		}

		public override string GetReplanner(string queue)
		{
			string replanner = Get(GetQueuePrefix(queue) + ReservationPlannerName, DefaultReservationPlannerName
				);
			return replanner;
		}

		public override bool GetMoveOnExpiry(string queue)
		{
			bool killOnExpiry = GetBoolean(GetQueuePrefix(queue) + ReservationMoveOnExpiry, DefaultReservationMoveOnExpiry
				);
			return killOnExpiry;
		}

		public override long GetEnforcementWindow(string queue)
		{
			long enforcementWindow = GetLong(GetQueuePrefix(queue) + ReservationEnforcementWindow
				, DefaultReservationEnforcementWindow);
			return enforcementWindow;
		}

		/// <summary>
		/// Sets the <em>disable_preemption</em> property in order to indicate
		/// whether or not container preemption will be disabled for the specified
		/// queue.
		/// </summary>
		/// <param name="queue">queue path</param>
		/// <param name="preemptionDisabled">true if preemption is disabled on queue</param>
		public virtual void SetPreemptionDisabled(string queue, bool preemptionDisabled)
		{
			SetBoolean(GetQueuePrefix(queue) + QueuePreemptionDisabled, preemptionDisabled);
		}

		/// <summary>Indicates whether preemption is disabled on the specified queue.</summary>
		/// <param name="queue">queue path to query</param>
		/// <param name="defaultVal">
		/// used as default if the <em>disable_preemption</em>
		/// is not set in the configuration
		/// </param>
		/// <returns>true if preemption is disabled on <em>queue</em>, false otherwise</returns>
		public virtual bool GetPreemptionDisabled(string queue, bool defaultVal)
		{
			bool preemptionDisabled = GetBoolean(GetQueuePrefix(queue) + QueuePreemptionDisabled
				, defaultVal);
			return preemptionDisabled;
		}

		/// <summary>Get configured node labels in a given queuePath</summary>
		public virtual ICollection<string> GetConfiguredNodeLabels(string queuePath)
		{
			ICollection<string> configuredNodeLabels = new HashSet<string>();
			KeyValuePair<string, string> e = null;
			IEnumerator<KeyValuePair<string, string>> iter = GetEnumerator();
			while (iter.HasNext())
			{
				e = iter.Next();
				string key = e.Key;
				if (key.StartsWith(GetQueuePrefix(queuePath) + AccessibleNodeLabels + Dot))
				{
					// Find <label-name> in
					// <queue-path>.accessible-node-labels.<label-name>.property
					int labelStartIdx = key.IndexOf(AccessibleNodeLabels) + AccessibleNodeLabels.Length
						 + 1;
					int labelEndIndx = key.IndexOf('.', labelStartIdx);
					string labelName = Sharpen.Runtime.Substring(key, labelStartIdx, labelEndIndx);
					configuredNodeLabels.AddItem(labelName);
				}
			}
			// always add NO_LABEL
			configuredNodeLabels.AddItem(RMNodeLabelsManager.NoLabel);
			return configuredNodeLabels;
		}
	}
}
