using System;
using Org.Apache.Hadoop.Mapreduce;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Server.Jobtracker
{
	/// <summary>Place holder for JobTracker server-level configuration.</summary>
	/// <remarks>
	/// Place holder for JobTracker server-level configuration.
	/// The keys should have "mapreduce.jobtracker." as the prefix
	/// </remarks>
	public abstract class JTConfig : MRConfig
	{
		public const string JtIpcAddress = "mapreduce.jobtracker.address";

		public const string JtHttpAddress = "mapreduce.jobtracker.http.address";

		public const string JtIpcHandlerCount = "mapreduce.jobtracker.handler.count";

		public const string JtRestartEnabled = "mapreduce.jobtracker.restart.recover";

		public const string JtTaskScheduler = "mapreduce.jobtracker.taskscheduler";

		public const string JtInstrumentation = "mapreduce.jobtracker.instrumentation";

		public const string JtTasksPerJob = "mapreduce.jobtracker.maxtasks.perjob";

		public const string JtHeartbeatsInSecond = "mapreduce.jobtracker.heartbeats.in.second";

		public const string JtHeartbeatsScalingFactor = "mapreduce.jobtracker.heartbeats.scaling.factor";

		public const string JtHeartbeatIntervalMin = "mapreduce.jobtracker.heartbeat.interval.min";

		public const int JtHeartbeatIntervalMinDefault = 300;

		public const string JtPersistJobstatus = "mapreduce.jobtracker.persist.jobstatus.active";

		public const string JtPersistJobstatusHours = "mapreduce.jobtracker.persist.jobstatus.hours";

		public const string JtPersistJobstatusDir = "mapreduce.jobtracker.persist.jobstatus.dir";

		[System.ObsoleteAttribute(@"Use MR_SUPERGROUP instead")]
		public const string JtSupergroup = "mapreduce.jobtracker.permissions.supergroup";

		public const string JtRetirejobs = "mapreduce.jobtracker.retirejobs";

		public const string JtRetirejobCacheSize = "mapreduce.jobtracker.retiredjobs.cache.size";

		public const string JtTaskcacheLevels = "mapreduce.jobtracker.taskcache.levels";

		public const string JtTaskAllocPadFraction = "mapreduce.jobtracker.taskscheduler.taskalloc.capacitypad";

		public const string JtJobinitThreads = "mapreduce.jobtracker.jobinit.threads";

		public const string JtTrackerExpiryInterval = "mapreduce.jobtracker.expire.trackers.interval";

		public const string JtRunningtasksPerJob = "mapreduce.jobtracker.taskscheduler.maxrunningtasks.perjob";

		public const string JtHostsFilename = "mapreduce.jobtracker.hosts.filename";

		public const string JtHostsExcludeFilename = "mapreduce.jobtracker.hosts.exclude.filename";

		public const string JtJobhistoryCacheSize = "mapreduce.jobtracker.jobhistory.lru.cache.size";

		public const string JtJobhistoryBlockSize = "mapreduce.jobtracker.jobhistory.block.size";

		public const string JtJobhistoryCompletedLocation = "mapreduce.jobtracker.jobhistory.completed.location";

		public const string JtJobhistoryLocation = "mapreduce.jobtracker.jobhistory.location";

		public const string JtJobhistoryTaskprogressNumberSplits = "mapreduce.jobtracker.jobhistory.task.numberprogresssplits";

		public const string JtAvgBlacklistThreshold = "mapreduce.jobtracker.blacklist.average.threshold";

		public const string JtSystemDir = "mapreduce.jobtracker.system.dir";

		public const string JtStagingAreaRoot = "mapreduce.jobtracker.staging.root.dir";

		public const string JtMaxTrackerBlacklists = "mapreduce.jobtracker.tasktracker.maxblacklists";

		public const string JtJobhistoryMaxage = "mapreduce.jobtracker.jobhistory.maxage";

		public const string JtMaxMapmemoryMb = "mapreduce.jobtracker.maxmapmemory.mb";

		public const string JtMaxReducememoryMb = "mapreduce.jobtracker.maxreducememory.mb";

		public const string JtMaxJobSplitMetainfoSize = "mapreduce.jobtracker.split.metainfo.maxsize";

		public const string JtUserName = "mapreduce.jobtracker.kerberos.principal";

		public const string JtKeytabFile = "mapreduce.jobtracker.keytab.file";

		public const string PrivateActionsKey = "mapreduce.jobtracker.webinterface.trusted";

		public const string JtPlugins = "mapreduce.jobtracker.plugins";

		public const string ShuffleExceptionStackRegex = "mapreduce.reduce.shuffle.catch.exception.stack.regex";

		public const string ShuffleExceptionMsgRegex = "mapreduce.reduce.shuffle.catch.exception.message.regex";
		// JobTracker configuration parameters
		// number of partial task progress reports we retain in job history
	}

	public static class JTConfigConstants
	{
	}
}
