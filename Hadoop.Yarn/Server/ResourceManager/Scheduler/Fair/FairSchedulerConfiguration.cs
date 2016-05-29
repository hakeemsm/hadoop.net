using System;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Server.Utils;
using Org.Apache.Hadoop.Yarn.Util.Resource;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fair
{
	public class FairSchedulerConfiguration : Configuration
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fair.FairSchedulerConfiguration
			).FullName);

		/// <summary>Increment request grant-able by the RM scheduler.</summary>
		/// <remarks>
		/// Increment request grant-able by the RM scheduler.
		/// These properties are looked up in the yarn-site.xml
		/// </remarks>
		public const string RmSchedulerIncrementAllocationMb = YarnConfiguration.YarnPrefix
			 + "scheduler.increment-allocation-mb";

		public const int DefaultRmSchedulerIncrementAllocationMb = 1024;

		public const string RmSchedulerIncrementAllocationVcores = YarnConfiguration.YarnPrefix
			 + "scheduler.increment-allocation-vcores";

		public const int DefaultRmSchedulerIncrementAllocationVcores = 1;

		private const string ConfPrefix = "yarn.scheduler.fair.";

		public const string AllocationFile = ConfPrefix + "allocation.file";

		protected internal const string DefaultAllocationFile = "fair-scheduler.xml";

		/// <summary>Whether to enable the Fair Scheduler event log</summary>
		public const string EventLogEnabled = ConfPrefix + "event-log-enabled";

		public const bool DefaultEventLogEnabled = false;

		protected internal const string EventLogDir = "eventlog.dir";

		/// <summary>Whether pools can be created that were not specified in the FS configuration file
		/// 	</summary>
		protected internal const string AllowUndeclaredPools = ConfPrefix + "allow-undeclared-pools";

		protected internal const bool DefaultAllowUndeclaredPools = true;

		/// <summary>
		/// Whether to use the user name as the queue name (instead of "default") if
		/// the request does not specify a queue.
		/// </summary>
		protected internal const string UserAsDefaultQueue = ConfPrefix + "user-as-default-queue";

		protected internal const bool DefaultUserAsDefaultQueue = true;

		protected internal const float DefaultLocalityThreshold = -1.0f;

		/// <summary>Cluster threshold for node locality.</summary>
		protected internal const string LocalityThresholdNode = ConfPrefix + "locality.threshold.node";

		protected internal const float DefaultLocalityThresholdNode = DefaultLocalityThreshold;

		/// <summary>Cluster threshold for rack locality.</summary>
		protected internal const string LocalityThresholdRack = ConfPrefix + "locality.threshold.rack";

		protected internal const float DefaultLocalityThresholdRack = DefaultLocalityThreshold;

		/// <summary>Delay for node locality.</summary>
		protected internal const string LocalityDelayNodeMs = ConfPrefix + "locality-delay-node-ms";

		protected internal const long DefaultLocalityDelayNodeMs = -1L;

		/// <summary>Delay for rack locality.</summary>
		protected internal const string LocalityDelayRackMs = ConfPrefix + "locality-delay-rack-ms";

		protected internal const long DefaultLocalityDelayRackMs = -1L;

		/// <summary>Enable continuous scheduling or not.</summary>
		protected internal const string ContinuousSchedulingEnabled = ConfPrefix + "continuous-scheduling-enabled";

		protected internal const bool DefaultContinuousSchedulingEnabled = false;

		/// <summary>Sleep time of each pass in continuous scheduling (5ms in default)</summary>
		protected internal const string ContinuousSchedulingSleepMs = ConfPrefix + "continuous-scheduling-sleep-ms";

		protected internal const int DefaultContinuousSchedulingSleepMs = 5;

		/// <summary>Whether preemption is enabled.</summary>
		protected internal const string Preemption = ConfPrefix + "preemption";

		protected internal const bool DefaultPreemption = false;

		protected internal const string PreemptionThreshold = ConfPrefix + "preemption.cluster-utilization-threshold";

		protected internal const float DefaultPreemptionThreshold = 0.8f;

		protected internal const string PreemptionInterval = ConfPrefix + "preemptionInterval";

		protected internal const int DefaultPreemptionInterval = 5000;

		protected internal const string WaitTimeBeforeKill = ConfPrefix + "waitTimeBeforeKill";

		protected internal const int DefaultWaitTimeBeforeKill = 15000;

		/// <summary>Whether to assign multiple containers in one check-in.</summary>
		public const string AssignMultiple = ConfPrefix + "assignmultiple";

		protected internal const bool DefaultAssignMultiple = false;

		/// <summary>Whether to give more weight to apps requiring many resources.</summary>
		protected internal const string SizeBasedWeight = ConfPrefix + "sizebasedweight";

		protected internal const bool DefaultSizeBasedWeight = false;

		/// <summary>Maximum number of containers to assign on each check-in.</summary>
		protected internal const string MaxAssign = ConfPrefix + "max.assign";

		protected internal const int DefaultMaxAssign = -1;

		/// <summary>The update interval for calculating resources in FairScheduler .</summary>
		public const string UpdateIntervalMs = ConfPrefix + "update-interval-ms";

		public const int DefaultUpdateIntervalMs = 500;

		public FairSchedulerConfiguration()
			: base()
		{
		}

		public FairSchedulerConfiguration(Configuration conf)
			: base(conf)
		{
		}

		public virtual Resource GetMinimumAllocation()
		{
			int mem = GetInt(YarnConfiguration.RmSchedulerMinimumAllocationMb, YarnConfiguration
				.DefaultRmSchedulerMinimumAllocationMb);
			int cpu = GetInt(YarnConfiguration.RmSchedulerMinimumAllocationVcores, YarnConfiguration
				.DefaultRmSchedulerMinimumAllocationVcores);
			return Resources.CreateResource(mem, cpu);
		}

		public virtual Org.Apache.Hadoop.Yarn.Api.Records.Resource GetMaximumAllocation()
		{
			int mem = GetInt(YarnConfiguration.RmSchedulerMaximumAllocationMb, YarnConfiguration
				.DefaultRmSchedulerMaximumAllocationMb);
			int cpu = GetInt(YarnConfiguration.RmSchedulerMaximumAllocationVcores, YarnConfiguration
				.DefaultRmSchedulerMaximumAllocationVcores);
			return Resources.CreateResource(mem, cpu);
		}

		public virtual Org.Apache.Hadoop.Yarn.Api.Records.Resource GetIncrementAllocation
			()
		{
			int incrementMemory = GetInt(RmSchedulerIncrementAllocationMb, DefaultRmSchedulerIncrementAllocationMb
				);
			int incrementCores = GetInt(RmSchedulerIncrementAllocationVcores, DefaultRmSchedulerIncrementAllocationVcores
				);
			return Resources.CreateResource(incrementMemory, incrementCores);
		}

		public virtual float GetLocalityThresholdNode()
		{
			return GetFloat(LocalityThresholdNode, DefaultLocalityThresholdNode);
		}

		public virtual float GetLocalityThresholdRack()
		{
			return GetFloat(LocalityThresholdRack, DefaultLocalityThresholdRack);
		}

		public virtual bool IsContinuousSchedulingEnabled()
		{
			return GetBoolean(ContinuousSchedulingEnabled, DefaultContinuousSchedulingEnabled
				);
		}

		public virtual int GetContinuousSchedulingSleepMs()
		{
			return GetInt(ContinuousSchedulingSleepMs, DefaultContinuousSchedulingSleepMs);
		}

		public virtual long GetLocalityDelayNodeMs()
		{
			return GetLong(LocalityDelayNodeMs, DefaultLocalityDelayNodeMs);
		}

		public virtual long GetLocalityDelayRackMs()
		{
			return GetLong(LocalityDelayRackMs, DefaultLocalityDelayRackMs);
		}

		public virtual bool GetPreemptionEnabled()
		{
			return GetBoolean(Preemption, DefaultPreemption);
		}

		public virtual float GetPreemptionUtilizationThreshold()
		{
			return GetFloat(PreemptionThreshold, DefaultPreemptionThreshold);
		}

		public virtual bool GetAssignMultiple()
		{
			return GetBoolean(AssignMultiple, DefaultAssignMultiple);
		}

		public virtual int GetMaxAssign()
		{
			return GetInt(MaxAssign, DefaultMaxAssign);
		}

		public virtual bool GetSizeBasedWeight()
		{
			return GetBoolean(SizeBasedWeight, DefaultSizeBasedWeight);
		}

		public virtual bool IsEventLogEnabled()
		{
			return GetBoolean(EventLogEnabled, DefaultEventLogEnabled);
		}

		public virtual string GetEventlogDir()
		{
			return Get(EventLogDir, new FilePath(Runtime.GetProperty("hadoop.log.dir", "/tmp/"
				)).GetAbsolutePath() + FilePath.separator + "fairscheduler");
		}

		public virtual int GetPreemptionInterval()
		{
			return GetInt(PreemptionInterval, DefaultPreemptionInterval);
		}

		public virtual int GetWaitTimeBeforeKill()
		{
			return GetInt(WaitTimeBeforeKill, DefaultWaitTimeBeforeKill);
		}

		public virtual bool GetUsePortForNodeName()
		{
			return GetBoolean(YarnConfiguration.RmSchedulerIncludePortInNodeName, YarnConfiguration
				.DefaultRmSchedulerUsePortForNodeName);
		}

		/// <summary>
		/// Parses a resource config value of a form like "1024", "1024 mb",
		/// or "1024 mb, 3 vcores".
		/// </summary>
		/// <remarks>
		/// Parses a resource config value of a form like "1024", "1024 mb",
		/// or "1024 mb, 3 vcores". If no units are given, megabytes are assumed.
		/// </remarks>
		/// <exception cref="AllocationConfigurationException"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fair.AllocationConfigurationException
		/// 	"/>
		public static Org.Apache.Hadoop.Yarn.Api.Records.Resource ParseResourceConfigValue
			(string val)
		{
			try
			{
				val = StringUtils.ToLowerCase(val);
				int memory = FindResource(val, "mb");
				int vcores = FindResource(val, "vcores");
				return BuilderUtils.NewResource(memory, vcores);
			}
			catch (AllocationConfigurationException ex)
			{
				throw;
			}
			catch (Exception ex)
			{
				throw new AllocationConfigurationException("Error reading resource config", ex);
			}
		}

		public virtual long GetUpdateInterval()
		{
			return GetLong(UpdateIntervalMs, DefaultUpdateIntervalMs);
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fair.AllocationConfigurationException
		/// 	"/>
		private static int FindResource(string val, string units)
		{
			Sharpen.Pattern pattern = Sharpen.Pattern.Compile("(\\d+)\\s*" + units);
			Matcher matcher = pattern.Matcher(val);
			if (!matcher.Find())
			{
				throw new AllocationConfigurationException("Missing resource: " + units);
			}
			return System.Convert.ToInt32(matcher.Group(1));
		}
	}
}
