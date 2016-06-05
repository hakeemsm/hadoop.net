using System;
using Org.Apache.Hadoop.Mapreduce;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Server.Tasktracker
{
	/// <summary>Place holder for TaskTracker server-level configuration.</summary>
	/// <remarks>
	/// Place holder for TaskTracker server-level configuration.
	/// The keys should have "mapreduce.tasktracker." as the prefix
	/// </remarks>
	public abstract class TTConfig : MRConfig
	{
		public const string TtHealthCheckerInterval = "mapreduce.tasktracker.healthchecker.interval";

		public const string TtHealthCheckerScriptArgs = "mapreduce.tasktracker.healthchecker.script.args";

		public const string TtHealthCheckerScriptPath = "mapreduce.tasktracker.healthchecker.script.path";

		public const string TtHealthCheckerScriptTimeout = "mapreduce.tasktracker.healthchecker.script.timeout";

		public const string TtLocalDirMinspaceKill = "mapreduce.tasktracker.local.dir.minspacekill";

		public const string TtLocalDirMinspaceStart = "mapreduce.tasktracker.local.dir.minspacestart";

		public const string TtHttpAddress = "mapreduce.tasktracker.http.address";

		public const string TtReportAddress = "mapreduce.tasktracker.report.address";

		public const string TtTaskController = "mapreduce.tasktracker.taskcontroller";

		public const string TtContentionTracking = "mapreduce.tasktracker.contention.tracking";

		public const string TtStaticResolutions = "mapreduce.tasktracker.net.static.resolutions";

		public const string TtHttpThreads = "mapreduce.tasktracker.http.threads";

		public const string TtHostName = "mapreduce.tasktracker.host.name";

		public const string TtSleepTimeBeforeSigKill = "mapreduce.tasktracker.tasks.sleeptimebeforesigkill";

		public const string TtDnsInterface = "mapreduce.tasktracker.dns.interface";

		public const string TtDnsNameserver = "mapreduce.tasktracker.dns.nameserver";

		public const string TtMaxTaskCompletionEventsToPoll = "mapreduce.tasktracker.events.batchsize";

		public const string TtIndexCache = "mapreduce.tasktracker.indexcache.mb";

		public const string TtInstrumentation = "mapreduce.tasktracker.instrumentation";

		public const string TtMapSlots = "mapreduce.tasktracker.map.tasks.maximum";

		[System.ObsoleteAttribute(@"Use TtResourceCalculatorPlugin instead")]
		public const string TtMemoryCalculatorPlugin = "mapreduce.tasktracker.memorycalculatorplugin";

		public const string TtResourceCalculatorPlugin = "mapreduce.tasktracker.resourcecalculatorplugin";

		public const string TtReduceSlots = "mapreduce.tasktracker.reduce.tasks.maximum";

		public const string TtMemoryManagerMonitoringInterval = "mapreduce.tasktracker.taskmemorymanager.monitoringinterval";

		public const string TtLocalCacheSize = "mapreduce.tasktracker.cache.local.size";

		public const string TtLocalCacheSubdirsLimit = "mapreduce.tasktracker.cache.local.numberdirectories";

		public const string TtOutofbandHearbeat = "mapreduce.tasktracker.outofband.heartbeat";

		public const string TtReservedPhyscialmemoryMb = "mapreduce.tasktracker.reserved.physicalmemory.mb";

		public const string TtUserName = "mapreduce.tasktracker.kerberos.principal";

		public const string TtKeytabFile = "mapreduce.tasktracker.keytab.file";

		public const string TtGroup = "mapreduce.tasktracker.group";

		public const string TtUserlogcleanupSleeptime = "mapreduce.tasktracker.userlogcleanup.sleeptime";

		public const string TtDistributedCacheCheckPeriod = "mapreduce.tasktracker.distributedcache.checkperiod";

		/// <summary>
		/// Percentage of the local distributed cache that should be kept in between
		/// garbage collection.
		/// </summary>
		public const string TtLocalCacheKeepAroundPct = "mapreduce.tasktracker.cache.local.keep.pct";
		// Task-tracker configuration properties
	}

	public static class TTConfigConstants
	{
	}
}
