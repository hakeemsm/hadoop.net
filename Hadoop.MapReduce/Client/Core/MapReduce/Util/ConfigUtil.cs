using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Mapred.Pipes;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Lib.Fieldsel;
using Org.Apache.Hadoop.Mapreduce.Lib.Input;
using Org.Apache.Hadoop.Mapreduce.Lib.Jobcontrol;
using Org.Apache.Hadoop.Mapreduce.Lib.Join;
using Org.Apache.Hadoop.Mapreduce.Lib.Map;
using Org.Apache.Hadoop.Mapreduce.Lib.Output;
using Org.Apache.Hadoop.Mapreduce.Lib.Partition;
using Org.Apache.Hadoop.Mapreduce.Server.Jobtracker;
using Org.Apache.Hadoop.Mapreduce.Server.Tasktracker;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Util
{
	/// <summary>Place holder for deprecated keys in the framework</summary>
	public class ConfigUtil
	{
		/// <summary>Adds all the deprecated keys.</summary>
		/// <remarks>Adds all the deprecated keys. Loads mapred-default.xml and mapred-site.xml
		/// 	</remarks>
		public static void LoadResources()
		{
			AddDeprecatedKeys();
			Configuration.AddDefaultResource("mapred-default.xml");
			Configuration.AddDefaultResource("mapred-site.xml");
			Configuration.AddDefaultResource("yarn-default.xml");
			Configuration.AddDefaultResource("yarn-site.xml");
		}

		/// <summary>Adds deprecated keys and the corresponding new keys to the Configuration
		/// 	</summary>
		private static void AddDeprecatedKeys()
		{
			Configuration.AddDeprecations(new Configuration.DeprecationDelta[] { new Configuration.DeprecationDelta
				("mapred.temp.dir", MRConfig.TempDir), new Configuration.DeprecationDelta("mapred.local.dir"
				, MRConfig.LocalDir), new Configuration.DeprecationDelta("mapred.cluster.map.memory.mb"
				, MRConfig.MapmemoryMb), new Configuration.DeprecationDelta("mapred.cluster.reduce.memory.mb"
				, MRConfig.ReducememoryMb), new Configuration.DeprecationDelta("mapred.acls.enabled"
				, MRConfig.MrAclsEnabled), new Configuration.DeprecationDelta("mapred.cluster.max.map.memory.mb"
				, JTConfig.JtMaxMapmemoryMb), new Configuration.DeprecationDelta("mapred.cluster.max.reduce.memory.mb"
				, JTConfig.JtMaxReducememoryMb), new Configuration.DeprecationDelta("mapred.cluster.average.blacklist.threshold"
				, JTConfig.JtAvgBlacklistThreshold), new Configuration.DeprecationDelta("hadoop.job.history.location"
				, JTConfig.JtJobhistoryLocation), new Configuration.DeprecationDelta("mapred.job.tracker.history.completed.location"
				, JTConfig.JtJobhistoryCompletedLocation), new Configuration.DeprecationDelta("mapred.jobtracker.job.history.block.size"
				, JTConfig.JtJobhistoryBlockSize), new Configuration.DeprecationDelta("mapred.job.tracker.jobhistory.lru.cache.size"
				, JTConfig.JtJobhistoryCacheSize), new Configuration.DeprecationDelta("mapred.hosts"
				, JTConfig.JtHostsFilename), new Configuration.DeprecationDelta("mapred.hosts.exclude"
				, JTConfig.JtHostsExcludeFilename), new Configuration.DeprecationDelta("mapred.system.dir"
				, JTConfig.JtSystemDir), new Configuration.DeprecationDelta("mapred.max.tracker.blacklists"
				, JTConfig.JtMaxTrackerBlacklists), new Configuration.DeprecationDelta("mapred.job.tracker"
				, JTConfig.JtIpcAddress), new Configuration.DeprecationDelta("mapred.job.tracker.http.address"
				, JTConfig.JtHttpAddress), new Configuration.DeprecationDelta("mapred.job.tracker.handler.count"
				, JTConfig.JtIpcHandlerCount), new Configuration.DeprecationDelta("mapred.jobtracker.restart.recover"
				, JTConfig.JtRestartEnabled), new Configuration.DeprecationDelta("mapred.jobtracker.taskScheduler"
				, JTConfig.JtTaskScheduler), new Configuration.DeprecationDelta("mapred.jobtracker.taskScheduler.maxRunningTasksPerJob"
				, JTConfig.JtRunningtasksPerJob), new Configuration.DeprecationDelta("mapred.jobtracker.instrumentation"
				, JTConfig.JtInstrumentation), new Configuration.DeprecationDelta("mapred.jobtracker.maxtasks.per.job"
				, JTConfig.JtTasksPerJob), new Configuration.DeprecationDelta("mapred.heartbeats.in.second"
				, JTConfig.JtHeartbeatsInSecond), new Configuration.DeprecationDelta("mapred.job.tracker.persist.jobstatus.active"
				, JTConfig.JtPersistJobstatus), new Configuration.DeprecationDelta("mapred.job.tracker.persist.jobstatus.hours"
				, JTConfig.JtPersistJobstatusHours), new Configuration.DeprecationDelta("mapred.job.tracker.persist.jobstatus.dir"
				, JTConfig.JtPersistJobstatusDir), new Configuration.DeprecationDelta("mapred.permissions.supergroup"
				, MRConfig.MrSupergroup), new Configuration.DeprecationDelta("mapreduce.jobtracker.permissions.supergroup"
				, MRConfig.MrSupergroup), new Configuration.DeprecationDelta("mapred.task.cache.levels"
				, JTConfig.JtTaskcacheLevels), new Configuration.DeprecationDelta("mapred.jobtracker.taskalloc.capacitypad"
				, JTConfig.JtTaskAllocPadFraction), new Configuration.DeprecationDelta("mapred.jobinit.threads"
				, JTConfig.JtJobinitThreads), new Configuration.DeprecationDelta("mapred.tasktracker.expiry.interval"
				, JTConfig.JtTrackerExpiryInterval), new Configuration.DeprecationDelta("mapred.job.tracker.retiredjobs.cache.size"
				, JTConfig.JtRetirejobCacheSize), new Configuration.DeprecationDelta("mapred.job.tracker.retire.jobs"
				, JTConfig.JtRetirejobs), new Configuration.DeprecationDelta("mapred.healthChecker.interval"
				, TTConfig.TtHealthCheckerInterval), new Configuration.DeprecationDelta("mapred.healthChecker.script.args"
				, TTConfig.TtHealthCheckerScriptArgs), new Configuration.DeprecationDelta("mapred.healthChecker.script.path"
				, TTConfig.TtHealthCheckerScriptPath), new Configuration.DeprecationDelta("mapred.healthChecker.script.timeout"
				, TTConfig.TtHealthCheckerScriptTimeout), new Configuration.DeprecationDelta("mapred.local.dir.minspacekill"
				, TTConfig.TtLocalDirMinspaceKill), new Configuration.DeprecationDelta("mapred.local.dir.minspacestart"
				, TTConfig.TtLocalDirMinspaceStart), new Configuration.DeprecationDelta("mapred.task.tracker.http.address"
				, TTConfig.TtHttpAddress), new Configuration.DeprecationDelta("mapred.task.tracker.report.address"
				, TTConfig.TtReportAddress), new Configuration.DeprecationDelta("mapred.task.tracker.task-controller"
				, TTConfig.TtTaskController), new Configuration.DeprecationDelta("mapred.tasktracker.dns.interface"
				, TTConfig.TtDnsInterface), new Configuration.DeprecationDelta("mapred.tasktracker.dns.nameserver"
				, TTConfig.TtDnsNameserver), new Configuration.DeprecationDelta("mapred.tasktracker.events.batchsize"
				, TTConfig.TtMaxTaskCompletionEventsToPoll), new Configuration.DeprecationDelta(
				"mapred.tasktracker.indexcache.mb", TTConfig.TtIndexCache), new Configuration.DeprecationDelta
				("mapred.tasktracker.instrumentation", TTConfig.TtInstrumentation), new Configuration.DeprecationDelta
				("mapred.tasktracker.map.tasks.maximum", TTConfig.TtMapSlots), new Configuration.DeprecationDelta
				("mapred.tasktracker.memory_calculator_plugin", TTConfig.TtResourceCalculatorPlugin
				), new Configuration.DeprecationDelta("mapred.tasktracker.memorycalculatorplugin"
				, TTConfig.TtResourceCalculatorPlugin), new Configuration.DeprecationDelta("mapred.tasktracker.reduce.tasks.maximum"
				, TTConfig.TtReduceSlots), new Configuration.DeprecationDelta("mapred.tasktracker.taskmemorymanager.monitoring-interval"
				, TTConfig.TtMemoryManagerMonitoringInterval), new Configuration.DeprecationDelta
				("mapred.tasktracker.tasks.sleeptime-before-sigkill", TTConfig.TtSleepTimeBeforeSigKill
				), new Configuration.DeprecationDelta("slave.host.name", TTConfig.TtHostName), new 
				Configuration.DeprecationDelta("tasktracker.http.threads", TTConfig.TtHttpThreads
				), new Configuration.DeprecationDelta("hadoop.net.static.resolutions", TTConfig.
				TtStaticResolutions), new Configuration.DeprecationDelta("local.cache.size", TTConfig
				.TtLocalCacheSize), new Configuration.DeprecationDelta("tasktracker.contention.tracking"
				, TTConfig.TtContentionTracking), new Configuration.DeprecationDelta("yarn.app.mapreduce.yarn.app.mapreduce.client-am.ipc.max-retries-on-timeouts"
				, MRJobConfig.MrClientToAmIpcMaxRetriesOnTimeouts), new Configuration.DeprecationDelta
				("job.end.notification.url", MRJobConfig.MrJobEndNotificationUrl), new Configuration.DeprecationDelta
				("job.end.retry.attempts", MRJobConfig.MrJobEndRetryAttempts), new Configuration.DeprecationDelta
				("job.end.retry.interval", MRJobConfig.MrJobEndRetryInterval), new Configuration.DeprecationDelta
				("mapred.committer.job.setup.cleanup.needed", MRJobConfig.SetupCleanupNeeded), new 
				Configuration.DeprecationDelta("mapred.jar", MRJobConfig.Jar), new Configuration.DeprecationDelta
				("mapred.job.id", MRJobConfig.Id), new Configuration.DeprecationDelta("mapred.job.name"
				, MRJobConfig.JobName), new Configuration.DeprecationDelta("mapred.job.priority"
				, MRJobConfig.Priority), new Configuration.DeprecationDelta("mapred.job.queue.name"
				, MRJobConfig.QueueName), new Configuration.DeprecationDelta("mapred.job.reuse.jvm.num.tasks"
				, MRJobConfig.JvmNumtasksTorun), new Configuration.DeprecationDelta("mapred.map.tasks"
				, MRJobConfig.NumMaps), new Configuration.DeprecationDelta("mapred.max.tracker.failures"
				, MRJobConfig.MaxTaskFailuresPerTracker), new Configuration.DeprecationDelta("mapred.reduce.slowstart.completed.maps"
				, MRJobConfig.CompletedMapsForReduceSlowstart), new Configuration.DeprecationDelta
				("mapred.reduce.tasks", MRJobConfig.NumReduces), new Configuration.DeprecationDelta
				("mapred.skip.on", MRJobConfig.SkipRecords), new Configuration.DeprecationDelta(
				"mapred.skip.out.dir", MRJobConfig.SkipOutdir), new Configuration.DeprecationDelta
				("mapred.speculative.execution.slowTaskThreshold", MRJobConfig.SpeculativeSlowtaskThreshold
				), new Configuration.DeprecationDelta("mapred.speculative.execution.speculativeCap"
				, MRJobConfig.SpeculativecapRunningTasks), new Configuration.DeprecationDelta("job.local.dir"
				, MRJobConfig.JobLocalDir), new Configuration.DeprecationDelta("mapreduce.inputformat.class"
				, MRJobConfig.InputFormatClassAttr), new Configuration.DeprecationDelta("mapreduce.map.class"
				, MRJobConfig.MapClassAttr), new Configuration.DeprecationDelta("mapreduce.combine.class"
				, MRJobConfig.CombineClassAttr), new Configuration.DeprecationDelta("mapreduce.reduce.class"
				, MRJobConfig.ReduceClassAttr), new Configuration.DeprecationDelta("mapreduce.outputformat.class"
				, MRJobConfig.OutputFormatClassAttr), new Configuration.DeprecationDelta("mapreduce.partitioner.class"
				, MRJobConfig.PartitionerClassAttr), new Configuration.DeprecationDelta("mapred.job.classpath.archives"
				, MRJobConfig.ClasspathArchives), new Configuration.DeprecationDelta("mapred.job.classpath.files"
				, MRJobConfig.ClasspathFiles), new Configuration.DeprecationDelta("mapred.cache.files"
				, MRJobConfig.CacheFiles), new Configuration.DeprecationDelta("mapred.cache.archives"
				, MRJobConfig.CacheArchives), new Configuration.DeprecationDelta("mapred.cache.localFiles"
				, MRJobConfig.CacheLocalfiles), new Configuration.DeprecationDelta("mapred.cache.localArchives"
				, MRJobConfig.CacheLocalarchives), new Configuration.DeprecationDelta("mapred.cache.files.filesizes"
				, MRJobConfig.CacheFilesSizes), new Configuration.DeprecationDelta("mapred.cache.archives.filesizes"
				, MRJobConfig.CacheArchivesSizes), new Configuration.DeprecationDelta("mapred.cache.files.timestamps"
				, MRJobConfig.CacheFileTimestamps), new Configuration.DeprecationDelta("mapred.cache.archives.timestamps"
				, MRJobConfig.CacheArchivesTimestamps), new Configuration.DeprecationDelta("mapred.working.dir"
				, MRJobConfig.WorkingDir), new Configuration.DeprecationDelta("user.name", MRJobConfig
				.UserName), new Configuration.DeprecationDelta("mapred.output.key.class", MRJobConfig
				.OutputKeyClass), new Configuration.DeprecationDelta("mapred.output.value.class"
				, MRJobConfig.OutputValueClass), new Configuration.DeprecationDelta("mapred.output.value.groupfn.class"
				, MRJobConfig.GroupComparatorClass), new Configuration.DeprecationDelta("mapred.output.key.comparator.class"
				, MRJobConfig.KeyComparator), new Configuration.DeprecationDelta("io.sort.factor"
				, MRJobConfig.IoSortFactor), new Configuration.DeprecationDelta("io.sort.mb", MRJobConfig
				.IoSortMb), new Configuration.DeprecationDelta("keep.failed.task.files", MRJobConfig
				.PreserveFailedTaskFiles), new Configuration.DeprecationDelta("keep.task.files.pattern"
				, MRJobConfig.PreserveFilesPattern), new Configuration.DeprecationDelta("mapred.debug.out.lines"
				, MRJobConfig.TaskDebugoutLines), new Configuration.DeprecationDelta("mapred.merge.recordsBeforeProgress"
				, MRJobConfig.RecordsBeforeProgress), new Configuration.DeprecationDelta("mapred.merge.recordsBeforeProgress"
				, MRJobConfig.CombineRecordsBeforeProgress), new Configuration.DeprecationDelta(
				"mapred.skip.attempts.to.start.skipping", MRJobConfig.SkipStartAttempts), new Configuration.DeprecationDelta
				("mapred.task.id", MRJobConfig.TaskAttemptId), new Configuration.DeprecationDelta
				("mapred.task.is.map", MRJobConfig.TaskIsmap), new Configuration.DeprecationDelta
				("mapred.task.partition", MRJobConfig.TaskPartition), new Configuration.DeprecationDelta
				("mapred.task.profile", MRJobConfig.TaskProfile), new Configuration.DeprecationDelta
				("mapred.task.profile.maps", MRJobConfig.NumMapProfiles), new Configuration.DeprecationDelta
				("mapred.task.profile.reduces", MRJobConfig.NumReduceProfiles), new Configuration.DeprecationDelta
				("mapred.task.timeout", MRJobConfig.TaskTimeout), new Configuration.DeprecationDelta
				("mapred.tip.id", MRJobConfig.TaskId), new Configuration.DeprecationDelta("mapred.work.output.dir"
				, MRJobConfig.TaskOutputDir), new Configuration.DeprecationDelta("mapred.userlog.limit.kb"
				, MRJobConfig.TaskUserlogLimit), new Configuration.DeprecationDelta("mapred.userlog.retain.hours"
				, MRJobConfig.UserLogRetainHours), new Configuration.DeprecationDelta("mapred.task.profile.params"
				, MRJobConfig.TaskProfileParams), new Configuration.DeprecationDelta("io.sort.spill.percent"
				, MRJobConfig.MapSortSpillPercent), new Configuration.DeprecationDelta("map.input.file"
				, MRJobConfig.MapInputFile), new Configuration.DeprecationDelta("map.input.length"
				, MRJobConfig.MapInputPath), new Configuration.DeprecationDelta("map.input.start"
				, MRJobConfig.MapInputStart), new Configuration.DeprecationDelta("mapred.job.map.memory.mb"
				, MRJobConfig.MapMemoryMb), new Configuration.DeprecationDelta("mapred.map.child.env"
				, MRJobConfig.MapEnv), new Configuration.DeprecationDelta("mapred.map.child.java.opts"
				, MRJobConfig.MapJavaOpts), new Configuration.DeprecationDelta("mapred.map.max.attempts"
				, MRJobConfig.MapMaxAttempts), new Configuration.DeprecationDelta("mapred.map.task.debug.script"
				, MRJobConfig.MapDebugScript), new Configuration.DeprecationDelta("mapred.map.tasks.speculative.execution"
				, MRJobConfig.MapSpeculative), new Configuration.DeprecationDelta("mapred.max.map.failures.percent"
				, MRJobConfig.MapFailuresMaxPercent), new Configuration.DeprecationDelta("mapred.skip.map.auto.incr.proc.count"
				, MRJobConfig.MapSkipIncrProcCount), new Configuration.DeprecationDelta("mapred.skip.map.max.skip.records"
				, MRJobConfig.MapSkipMaxRecords), new Configuration.DeprecationDelta("min.num.spills.for.combine"
				, MRJobConfig.MapCombineMinSpills), new Configuration.DeprecationDelta("mapred.compress.map.output"
				, MRJobConfig.MapOutputCompress), new Configuration.DeprecationDelta("mapred.map.output.compression.codec"
				, MRJobConfig.MapOutputCompressCodec), new Configuration.DeprecationDelta("mapred.mapoutput.key.class"
				, MRJobConfig.MapOutputKeyClass), new Configuration.DeprecationDelta("mapred.mapoutput.value.class"
				, MRJobConfig.MapOutputValueClass), new Configuration.DeprecationDelta("map.output.key.field.separator"
				, MRJobConfig.MapOutputKeyFieldSeperator), new Configuration.DeprecationDelta("mapred.map.child.log.level"
				, MRJobConfig.MapLogLevel), new Configuration.DeprecationDelta("mapred.inmem.merge.threshold"
				, MRJobConfig.ReduceMergeInmemThreshold), new Configuration.DeprecationDelta("mapred.job.reduce.input.buffer.percent"
				, MRJobConfig.ReduceInputBufferPercent), new Configuration.DeprecationDelta("mapred.job.reduce.markreset.buffer.percent"
				, MRJobConfig.ReduceMarkresetBufferPercent), new Configuration.DeprecationDelta(
				"mapred.job.reduce.memory.mb", MRJobConfig.ReduceMemoryMb), new Configuration.DeprecationDelta
				("mapred.job.reduce.total.mem.bytes", MRJobConfig.ReduceMemoryTotalBytes), new Configuration.DeprecationDelta
				("mapred.job.shuffle.input.buffer.percent", MRJobConfig.ShuffleInputBufferPercent
				), new Configuration.DeprecationDelta("mapred.job.shuffle.merge.percent", MRJobConfig
				.ShuffleMergePercent), new Configuration.DeprecationDelta("mapred.max.reduce.failures.percent"
				, MRJobConfig.ReduceFailuresMaxpercent), new Configuration.DeprecationDelta("mapred.reduce.child.env"
				, MRJobConfig.ReduceEnv), new Configuration.DeprecationDelta("mapred.reduce.child.java.opts"
				, MRJobConfig.ReduceJavaOpts), new Configuration.DeprecationDelta("mapred.reduce.max.attempts"
				, MRJobConfig.ReduceMaxAttempts), new Configuration.DeprecationDelta("mapred.reduce.parallel.copies"
				, MRJobConfig.ShuffleParallelCopies), new Configuration.DeprecationDelta("mapred.reduce.task.debug.script"
				, MRJobConfig.ReduceDebugScript), new Configuration.DeprecationDelta("mapred.reduce.tasks.speculative.execution"
				, MRJobConfig.ReduceSpeculative), new Configuration.DeprecationDelta("mapred.shuffle.connect.timeout"
				, MRJobConfig.ShuffleConnectTimeout), new Configuration.DeprecationDelta("mapred.shuffle.read.timeout"
				, MRJobConfig.ShuffleReadTimeout), new Configuration.DeprecationDelta("mapred.skip.reduce.auto.incr.proc.count"
				, MRJobConfig.ReduceSkipIncrProcCount), new Configuration.DeprecationDelta("mapred.skip.reduce.max.skip.groups"
				, MRJobConfig.ReduceSkipMaxgroups), new Configuration.DeprecationDelta("mapred.reduce.child.log.level"
				, MRJobConfig.ReduceLogLevel), new Configuration.DeprecationDelta("mapreduce.job.counters.limit"
				, MRJobConfig.CountersMaxKey), new Configuration.DeprecationDelta("jobclient.completion.poll.interval"
				, Job.CompletionPollIntervalKey), new Configuration.DeprecationDelta("jobclient.progress.monitor.poll.interval"
				, Job.ProgressMonitorPollIntervalKey), new Configuration.DeprecationDelta("jobclient.output.filter"
				, Job.OutputFilter), new Configuration.DeprecationDelta("mapred.submit.replication"
				, Job.SubmitReplication), new Configuration.DeprecationDelta("mapred.used.genericoptionsparser"
				, Job.UsedGenericParser), new Configuration.DeprecationDelta("mapred.input.dir", 
				FileInputFormat.InputDir), new Configuration.DeprecationDelta("mapred.input.pathFilter.class"
				, FileInputFormat.PathfilterClass), new Configuration.DeprecationDelta("mapred.max.split.size"
				, FileInputFormat.SplitMaxsize), new Configuration.DeprecationDelta("mapred.min.split.size"
				, FileInputFormat.SplitMinsize), new Configuration.DeprecationDelta("mapred.output.compress"
				, FileOutputFormat.Compress), new Configuration.DeprecationDelta("mapred.output.compression.codec"
				, FileOutputFormat.CompressCodec), new Configuration.DeprecationDelta("mapred.output.compression.type"
				, FileOutputFormat.CompressType), new Configuration.DeprecationDelta("mapred.output.dir"
				, FileOutputFormat.Outdir), new Configuration.DeprecationDelta("mapred.seqbinary.output.key.class"
				, SequenceFileAsBinaryOutputFormat.KeyClass), new Configuration.DeprecationDelta
				("mapred.seqbinary.output.value.class", SequenceFileAsBinaryOutputFormat.ValueClass
				), new Configuration.DeprecationDelta("sequencefile.filter.class", SequenceFileInputFilter
				.FilterClass), new Configuration.DeprecationDelta("sequencefile.filter.regex", SequenceFileInputFilter
				.FilterRegex), new Configuration.DeprecationDelta("sequencefile.filter.frequency"
				, SequenceFileInputFilter.FilterFrequency), new Configuration.DeprecationDelta("mapred.input.dir.mappers"
				, MultipleInputs.DirMappers), new Configuration.DeprecationDelta("mapred.input.dir.formats"
				, MultipleInputs.DirFormats), new Configuration.DeprecationDelta("mapred.line.input.format.linespermap"
				, NLineInputFormat.LinesPerMap), new Configuration.DeprecationDelta("mapred.binary.partitioner.left.offset"
				, BinaryPartitioner.LeftOffsetPropertyName), new Configuration.DeprecationDelta(
				"mapred.binary.partitioner.right.offset", BinaryPartitioner.RightOffsetPropertyName
				), new Configuration.DeprecationDelta("mapred.text.key.comparator.options", KeyFieldBasedComparator
				.ComparatorOptions), new Configuration.DeprecationDelta("mapred.text.key.partitioner.options"
				, KeyFieldBasedPartitioner.PartitionerOptions), new Configuration.DeprecationDelta
				("mapred.mapper.regex.group", RegexMapper.Group), new Configuration.DeprecationDelta
				("mapred.mapper.regex", RegexMapper.Pattern), new Configuration.DeprecationDelta
				("create.empty.dir.if.nonexist", ControlledJob.CreateDir), new Configuration.DeprecationDelta
				("mapred.data.field.separator", FieldSelectionHelper.DataFieldSeperator), new Configuration.DeprecationDelta
				("map.output.key.value.fields.spec", FieldSelectionHelper.MapOutputKeyValueSpec)
				, new Configuration.DeprecationDelta("reduce.output.key.value.fields.spec", FieldSelectionHelper
				.ReduceOutputKeyValueSpec), new Configuration.DeprecationDelta("mapred.min.split.size.per.node"
				, CombineFileInputFormat.SplitMinsizePernode), new Configuration.DeprecationDelta
				("mapred.min.split.size.per.rack", CombineFileInputFormat.SplitMinsizePerrack), 
				new Configuration.DeprecationDelta("key.value.separator.in.input.line", KeyValueLineRecordReader
				.KeyValueSeperator), new Configuration.DeprecationDelta("mapred.linerecordreader.maxlength"
				, LineRecordReader.MaxLineLength), new Configuration.DeprecationDelta("mapred.lazy.output.format"
				, LazyOutputFormat.OutputFormat), new Configuration.DeprecationDelta("mapred.textoutputformat.separator"
				, TextOutputFormat.Seperator), new Configuration.DeprecationDelta("mapred.join.expr"
				, CompositeInputFormat.JoinExpr), new Configuration.DeprecationDelta("mapred.join.keycomparator"
				, CompositeInputFormat.JoinComparator), new Configuration.DeprecationDelta("hadoop.pipes.command-file.keep"
				, Submitter.PreserveCommandfile), new Configuration.DeprecationDelta("hadoop.pipes.executable"
				, Submitter.Executable), new Configuration.DeprecationDelta("hadoop.pipes.executable.interpretor"
				, Submitter.Interpretor), new Configuration.DeprecationDelta("hadoop.pipes.java.mapper"
				, Submitter.IsJavaMap), new Configuration.DeprecationDelta("hadoop.pipes.java.recordreader"
				, Submitter.IsJavaRr), new Configuration.DeprecationDelta("hadoop.pipes.java.recordwriter"
				, Submitter.IsJavaRw), new Configuration.DeprecationDelta("hadoop.pipes.java.reducer"
				, Submitter.IsJavaReduce), new Configuration.DeprecationDelta("hadoop.pipes.partitioner"
				, Submitter.Partitioner), new Configuration.DeprecationDelta("mapred.pipes.user.inputformat"
				, Submitter.InputFormat), new Configuration.DeprecationDelta("webinterface.private.actions"
				, JTConfig.PrivateActionsKey), new Configuration.DeprecationDelta("security.task.umbilical.protocol.acl"
				, MRJobConfig.MrAmSecurityServiceAuthorizationTaskUmbilical), new Configuration.DeprecationDelta
				("security.job.submission.protocol.acl", MRJobConfig.MrAmSecurityServiceAuthorizationClient
				), new Configuration.DeprecationDelta("mapreduce.user.classpath.first", MRJobConfig
				.MapreduceJobUserClasspathFirst), new Configuration.DeprecationDelta(JTConfig.JtMaxJobSplitMetainfoSize
				, MRJobConfig.SplitMetainfoMaxsize), new Configuration.DeprecationDelta("mapred.input.dir.recursive"
				, FileInputFormat.InputDirRecursive) });
		}

		public static void Main(string[] args)
		{
			LoadResources();
			Configuration.DumpDeprecatedKeys();
		}
	}
}
