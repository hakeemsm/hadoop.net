using System;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce
{
	public abstract class MRJobConfig
	{
		public const string InputFormatClassAttr = "mapreduce.job.inputformat.class";

		public const string MapClassAttr = "mapreduce.job.map.class";

		public const string MapOutputCollectorClassAttr = "mapreduce.job.map.output.collector.class";

		public const string CombineClassAttr = "mapreduce.job.combine.class";

		public const string ReduceClassAttr = "mapreduce.job.reduce.class";

		public const string OutputFormatClassAttr = "mapreduce.job.outputformat.class";

		public const string PartitionerClassAttr = "mapreduce.job.partitioner.class";

		public const string SetupCleanupNeeded = "mapreduce.job.committer.setup.cleanup.needed";

		public const string TaskCleanupNeeded = "mapreduce.job.committer.task.cleanup.needed";

		public const string Jar = "mapreduce.job.jar";

		public const string Id = "mapreduce.job.id";

		public const string JobName = "mapreduce.job.name";

		public const string JarUnpackPattern = "mapreduce.job.jar.unpack.pattern";

		public const string UserName = "mapreduce.job.user.name";

		public const string Priority = "mapreduce.job.priority";

		public const string QueueName = "mapreduce.job.queuename";

		public const string ReservationId = "mapreduce.job.reservation.id";

		public const string JobTags = "mapreduce.job.tags";

		public const string JvmNumtasksTorun = "mapreduce.job.jvm.numtasks";

		public const string SplitFile = "mapreduce.job.splitfile";

		public const string SplitMetainfoMaxsize = "mapreduce.job.split.metainfo.maxsize";

		public const long DefaultSplitMetainfoMaxsize = 10000000L;

		public const string NumMaps = "mapreduce.job.maps";

		public const string MaxTaskFailuresPerTracker = "mapreduce.job.maxtaskfailures.per.tracker";

		public const string CompletedMapsForReduceSlowstart = "mapreduce.job.reduce.slowstart.completedmaps";

		public const string NumReduces = "mapreduce.job.reduces";

		public const string SkipRecords = "mapreduce.job.skiprecords";

		public const string SkipOutdir = "mapreduce.job.skip.outdir";

		[Obsolete]
		public const string SpeculativeSlownodeThreshold = "mapreduce.job.speculative.slownodethreshold";

		public const string SpeculativeSlowtaskThreshold = "mapreduce.job.speculative.slowtaskthreshold";

		[Obsolete]
		public const string Speculativecap = "mapreduce.job.speculative.speculativecap";

		public const string SpeculativecapRunningTasks = "mapreduce.job.speculative.speculative-cap-running-tasks";

		public const double DefaultSpeculativecapRunningTasks = 0.1;

		public const string SpeculativecapTotalTasks = "mapreduce.job.speculative.speculative-cap-total-tasks";

		public const double DefaultSpeculativecapTotalTasks = 0.01;

		public const string SpeculativeMinimumAllowedTasks = "mapreduce.job.speculative.minimum-allowed-tasks";

		public const int DefaultSpeculativeMinimumAllowedTasks = 10;

		public const string SpeculativeRetryAfterNoSpeculate = "mapreduce.job.speculative.retry-after-no-speculate";

		public const long DefaultSpeculativeRetryAfterNoSpeculate = 1000L;

		public const string SpeculativeRetryAfterSpeculate = "mapreduce.job.speculative.retry-after-speculate";

		public const long DefaultSpeculativeRetryAfterSpeculate = 15000L;

		public const string JobLocalDir = "mapreduce.job.local.dir";

		public const string OutputKeyClass = "mapreduce.job.output.key.class";

		public const string OutputValueClass = "mapreduce.job.output.value.class";

		public const string KeyComparator = "mapreduce.job.output.key.comparator.class";

		public const string CombinerGroupComparatorClass = "mapreduce.job.combiner.group.comparator.class";

		public const string GroupComparatorClass = "mapreduce.job.output.group.comparator.class";

		public const string WorkingDir = "mapreduce.job.working.dir";

		public const string ClasspathArchives = "mapreduce.job.classpath.archives";

		public const string ClasspathFiles = "mapreduce.job.classpath.files";

		public const string CacheFiles = "mapreduce.job.cache.files";

		public const string CacheArchives = "mapreduce.job.cache.archives";

		public const string CacheFilesSizes = "mapreduce.job.cache.files.filesizes";

		public const string CacheArchivesSizes = "mapreduce.job.cache.archives.filesizes";

		public const string CacheLocalfiles = "mapreduce.job.cache.local.files";

		public const string CacheLocalarchives = "mapreduce.job.cache.local.archives";

		public const string CacheFileTimestamps = "mapreduce.job.cache.files.timestamps";

		public const string CacheArchivesTimestamps = "mapreduce.job.cache.archives.timestamps";

		public const string CacheFileVisibilities = "mapreduce.job.cache.files.visibilities";

		public const string CacheArchivesVisibilities = "mapreduce.job.cache.archives.visibilities";

		[System.ObsoleteAttribute(@"Symlinks are always on and cannot be disabled.")]
		public const string CacheSymlink = "mapreduce.job.cache.symlink.create";

		public const string UserLogRetainHours = "mapreduce.job.userlog.retain.hours";

		public const string MapreduceJobUserClasspathFirst = "mapreduce.job.user.classpath.first";

		public const string MapreduceJobClassloader = "mapreduce.job.classloader";

		/// <summary>
		/// A comma-separated list of services that function as ShuffleProvider aux-services
		/// (in addition to the built-in ShuffleHandler).
		/// </summary>
		/// <remarks>
		/// A comma-separated list of services that function as ShuffleProvider aux-services
		/// (in addition to the built-in ShuffleHandler).
		/// These services can serve shuffle requests from reducetasks.
		/// </remarks>
		public const string MapreduceJobShuffleProviderServices = "mapreduce.job.shuffle.provider.services";

		public const string MapreduceJobClassloaderSystemClasses = "mapreduce.job.classloader.system.classes";

		public const string IoSortFactor = "mapreduce.task.io.sort.factor";

		public const string IoSortMb = "mapreduce.task.io.sort.mb";

		public const string IndexCacheMemoryLimit = "mapreduce.task.index.cache.limit.bytes";

		public const string PreserveFailedTaskFiles = "mapreduce.task.files.preserve.failedtasks";

		public const string PreserveFilesPattern = "mapreduce.task.files.preserve.filepattern";

		public const string TaskDebugoutLines = "mapreduce.task.debugout.lines";

		public const string RecordsBeforeProgress = "mapreduce.task.merge.progress.records";

		public const string SkipStartAttempts = "mapreduce.task.skip.start.attempts";

		public const string TaskAttemptId = "mapreduce.task.attempt.id";

		public const string TaskIsmap = "mapreduce.task.ismap";

		public const bool DefaultTaskIsmap = true;

		public const string TaskPartition = "mapreduce.task.partition";

		public const string TaskProfile = "mapreduce.task.profile";

		public const string TaskProfileParams = "mapreduce.task.profile.params";

		public const string DefaultTaskProfileParams = "-agentlib:hprof=cpu=samples,heap=sites,force=n,thread=y,"
			 + "verbose=n,file=%s";

		public const string NumMapProfiles = "mapreduce.task.profile.maps";

		public const string NumReduceProfiles = "mapreduce.task.profile.reduces";

		public const string TaskMapProfileParams = "mapreduce.task.profile.map.params";

		public const string TaskReduceProfileParams = "mapreduce.task.profile.reduce.params";

		public const string TaskTimeout = "mapreduce.task.timeout";

		public const string TaskTimeoutCheckIntervalMs = "mapreduce.task.timeout.check-interval-ms";

		public const string TaskId = "mapreduce.task.id";

		public const string TaskOutputDir = "mapreduce.task.output.dir";

		public const string TaskUserlogLimit = "mapreduce.task.userlog.limit.kb";

		public const string MapSortSpillPercent = "mapreduce.map.sort.spill.percent";

		public const string MapInputFile = "mapreduce.map.input.file";

		public const string MapInputPath = "mapreduce.map.input.length";

		public const string MapInputStart = "mapreduce.map.input.start";

		public const string MapMemoryMb = "mapreduce.map.memory.mb";

		public const int DefaultMapMemoryMb = 1024;

		public const string MapCpuVcores = "mapreduce.map.cpu.vcores";

		public const int DefaultMapCpuVcores = 1;

		public const string MapEnv = "mapreduce.map.env";

		public const string MapJavaOpts = "mapreduce.map.java.opts";

		public const string MapMaxAttempts = "mapreduce.map.maxattempts";

		public const string MapDebugScript = "mapreduce.map.debug.script";

		public const string MapSpeculative = "mapreduce.map.speculative";

		public const string MapFailuresMaxPercent = "mapreduce.map.failures.maxpercent";

		public const string MapSkipIncrProcCount = "mapreduce.map.skip.proc-count.auto-incr";

		public const string MapSkipMaxRecords = "mapreduce.map.skip.maxrecords";

		public const string MapCombineMinSpills = "mapreduce.map.combine.minspills";

		public const string MapOutputCompress = "mapreduce.map.output.compress";

		public const string MapOutputCompressCodec = "mapreduce.map.output.compress.codec";

		public const string MapOutputKeyClass = "mapreduce.map.output.key.class";

		public const string MapOutputValueClass = "mapreduce.map.output.value.class";

		public const string MapOutputKeyFieldSeperator = "mapreduce.map.output.key.field.separator";

		public const string MapLogLevel = "mapreduce.map.log.level";

		public const string ReduceLogLevel = "mapreduce.reduce.log.level";

		public const string DefaultLogLevel = "INFO";

		public const string ReduceMergeInmemThreshold = "mapreduce.reduce.merge.inmem.threshold";

		public const string ReduceInputBufferPercent = "mapreduce.reduce.input.buffer.percent";

		public const string ReduceMarkresetBufferPercent = "mapreduce.reduce.markreset.buffer.percent";

		public const string ReduceMarkresetBufferSize = "mapreduce.reduce.markreset.buffer.size";

		public const string ReduceMemoryMb = "mapreduce.reduce.memory.mb";

		public const int DefaultReduceMemoryMb = 1024;

		public const string ReduceCpuVcores = "mapreduce.reduce.cpu.vcores";

		public const int DefaultReduceCpuVcores = 1;

		public const string ReduceMemoryTotalBytes = "mapreduce.reduce.memory.totalbytes";

		public const string ShuffleInputBufferPercent = "mapreduce.reduce.shuffle.input.buffer.percent";

		public const float DefaultShuffleInputBufferPercent = 0.70f;

		public const string ShuffleMemoryLimitPercent = "mapreduce.reduce.shuffle.memory.limit.percent";

		public const string ShuffleMergePercent = "mapreduce.reduce.shuffle.merge.percent";

		public const string ReduceFailuresMaxpercent = "mapreduce.reduce.failures.maxpercent";

		public const string ReduceEnv = "mapreduce.reduce.env";

		public const string ReduceJavaOpts = "mapreduce.reduce.java.opts";

		public const string MapreduceJobDir = "mapreduce.job.dir";

		public const string ReduceMaxAttempts = "mapreduce.reduce.maxattempts";

		public const string ShuffleParallelCopies = "mapreduce.reduce.shuffle.parallelcopies";

		public const string ReduceDebugScript = "mapreduce.reduce.debug.script";

		public const string ReduceSpeculative = "mapreduce.reduce.speculative";

		public const string ShuffleConnectTimeout = "mapreduce.reduce.shuffle.connect.timeout";

		public const string ShuffleReadTimeout = "mapreduce.reduce.shuffle.read.timeout";

		public const string ShuffleFetchFailures = "mapreduce.reduce.shuffle.maxfetchfailures";

		public const string MaxAllowedFetchFailuresFraction = "mapreduce.reduce.shuffle.max-fetch-failures-fraction";

		public const float DefaultMaxAllowedFetchFailuresFraction = 0.5f;

		public const string MaxFetchFailuresNotifications = "mapreduce.reduce.shuffle.max-fetch-failures-notifications";

		public const int DefaultMaxFetchFailuresNotifications = 3;

		public const string ShuffleFetchRetryIntervalMs = "mapreduce.reduce.shuffle.fetch.retry.interval-ms";

		/// <summary>Default interval that fetcher retry to fetch during NM restart.</summary>
		public const int DefaultShuffleFetchRetryIntervalMs = 1000;

		public const string ShuffleFetchRetryTimeoutMs = "mapreduce.reduce.shuffle.fetch.retry.timeout-ms";

		public const string ShuffleFetchRetryEnabled = "mapreduce.reduce.shuffle.fetch.retry.enabled";

		public const string ShuffleNotifyReaderror = "mapreduce.reduce.shuffle.notify.readerror";

		public const string MaxShuffleFetchRetryDelay = "mapreduce.reduce.shuffle.retry-delay.max.ms";

		public const long DefaultMaxShuffleFetchRetryDelay = 60000;

		public const string MaxShuffleFetchHostFailures = "mapreduce.reduce.shuffle.max-host-failures";

		public const int DefaultMaxShuffleFetchHostFailures = 5;

		public const string ReduceSkipIncrProcCount = "mapreduce.reduce.skip.proc-count.auto-incr";

		public const string ReduceSkipMaxgroups = "mapreduce.reduce.skip.maxgroups";

		public const string ReduceMemtomemThreshold = "mapreduce.reduce.merge.memtomem.threshold";

		public const string ReduceMemtomemEnabled = "mapreduce.reduce.merge.memtomem.enabled";

		public const string CombineRecordsBeforeProgress = "mapreduce.task.combine.progress.records";

		public const string JobNamenodes = "mapreduce.job.hdfs-servers";

		public const string JobJobtrackerId = "mapreduce.job.kerberos.jtprinicipal";

		public const string JobCancelDelegationToken = "mapreduce.job.complete.cancel.delegation.tokens";

		public const string JobAclViewJob = "mapreduce.job.acl-view-job";

		public const string DefaultJobAclViewJob = " ";

		public const string JobAclModifyJob = "mapreduce.job.acl-modify-job";

		public const string DefaultJobAclModifyJob = " ";

		public const string JobRunningMapLimit = "mapreduce.job.running.map.limit";

		public const int DefaultJobRunningMapLimit = 0;

		public const string JobRunningReduceLimit = "mapreduce.job.running.reduce.limit";

		public const int DefaultJobRunningReduceLimit = 0;

		public const string MapreduceJobCredentialsBinary = "mapreduce.job.credentials.binary";

		public const string JobTokenTrackingIdsEnabled = "mapreduce.job.token.tracking.ids.enabled";

		public const bool DefaultJobTokenTrackingIdsEnabled = false;

		public const string JobTokenTrackingIds = "mapreduce.job.token.tracking.ids";

		public const string JobSubmithost = "mapreduce.job.submithostname";

		public const string JobSubmithostaddr = "mapreduce.job.submithostaddress";

		public const string CountersMaxKey = "mapreduce.job.counters.max";

		public const int CountersMaxDefault = 120;

		public const string CounterGroupNameMaxKey = "mapreduce.job.counters.group.name.max";

		public const int CounterGroupNameMaxDefault = 128;

		public const string CounterNameMaxKey = "mapreduce.job.counters.counter.name.max";

		public const int CounterNameMaxDefault = 64;

		public const string CounterGroupsMaxKey = "mapreduce.job.counters.groups.max";

		public const int CounterGroupsMaxDefault = 50;

		public const string JobUbertaskEnable = "mapreduce.job.ubertask.enable";

		public const string JobUbertaskMaxmaps = "mapreduce.job.ubertask.maxmaps";

		public const string JobUbertaskMaxreduces = "mapreduce.job.ubertask.maxreduces";

		public const string JobUbertaskMaxbytes = "mapreduce.job.ubertask.maxbytes";

		public const string MapreduceJobEmitTimelineData = "mapreduce.job.emit-timeline-data";

		public const bool DefaultMapreduceJobEmitTimelineData = false;

		public const string MrPrefix = "yarn.app.mapreduce.";

		public const string MrAmPrefix = MrPrefix + "am.";

		/// <summary>
		/// The number of client retries to the AM - before reconnecting to the RM
		/// to fetch Application State.
		/// </summary>
		public const string MrClientToAmIpcMaxRetries = MrPrefix + "client-am.ipc.max-retries";

		public const int DefaultMrClientToAmIpcMaxRetries = 3;

		/// <summary>
		/// The number of client retries on socket timeouts to the AM - before
		/// reconnecting to the RM to fetch Application Status.
		/// </summary>
		public const string MrClientToAmIpcMaxRetriesOnTimeouts = MrPrefix + "client-am.ipc.max-retries-on-timeouts";

		public const int DefaultMrClientToAmIpcMaxRetriesOnTimeouts = 3;

		/// <summary>The number of client retries to the RM/HS before throwing exception.</summary>
		public const string MrClientMaxRetries = MrPrefix + "client.max-retries";

		public const int DefaultMrClientMaxRetries = 3;

		/// <summary>How many times to retry jobclient calls (via getjob)</summary>
		public const string MrClientJobMaxRetries = MrPrefix + "client.job.max-retries";

		public const int DefaultMrClientJobMaxRetries = 0;

		/// <summary>How long to wait between jobclient retries on failure</summary>
		public const string MrClientJobRetryInterval = MrPrefix + "client.job.retry-interval";

		public const long DefaultMrClientJobRetryInterval = 2000;

		/// <summary>The staging directory for map reduce.</summary>
		public const string MrAmStagingDir = MrAmPrefix + "staging-dir";

		public const string DefaultMrAmStagingDir = "/tmp/hadoop-yarn/staging";

		/// <summary>The amount of memory the MR app master needs.</summary>
		public const string MrAmVmemMb = MrAmPrefix + "resource.mb";

		public const int DefaultMrAmVmemMb = 1536;

		/// <summary>The number of virtual cores the MR app master needs.</summary>
		public const string MrAmCpuVcores = MrAmPrefix + "resource.cpu-vcores";

		public const int DefaultMrAmCpuVcores = 1;

		/// <summary>Command line arguments passed to the MR app master.</summary>
		public const string MrAmCommandOpts = MrAmPrefix + "command-opts";

		public const string DefaultMrAmCommandOpts = "-Xmx1024m";

		/// <summary>Admin command opts passed to the MR app master.</summary>
		public const string MrAmAdminCommandOpts = MrAmPrefix + "admin-command-opts";

		public const string DefaultMrAmAdminCommandOpts = string.Empty;

		/// <summary>Root Logging level passed to the MR app master.</summary>
		public const string MrAmLogLevel = MrAmPrefix + "log.level";

		public const string DefaultMrAmLogLevel = "INFO";

		public const string MrAmLogKb = MrAmPrefix + "container.log.limit.kb";

		public const int DefaultMrAmLogKb = 0;

		public const string MrAmLogBackups = MrAmPrefix + "container.log.backups";

		public const int DefaultMrAmLogBackups = 0;

		/// <summary>The number of splits when reporting progress in MR</summary>
		public const string MrAmNumProgressSplits = MrAmPrefix + "num-progress-splits";

		public const int DefaultMrAmNumProgressSplits = 12;

		/// <summary>
		/// Upper limit on the number of threads user to launch containers in the app
		/// master.
		/// </summary>
		/// <remarks>
		/// Upper limit on the number of threads user to launch containers in the app
		/// master. Expect level config, you shouldn't be needing it in most cases.
		/// </remarks>
		public const string MrAmContainerlauncherThreadCountLimit = MrAmPrefix + "containerlauncher.thread-count-limit";

		public const int DefaultMrAmContainerlauncherThreadCountLimit = 500;

		/// <summary>The initial size of thread pool to launch containers in the app master</summary>
		public const string MrAmContainerlauncherThreadpoolInitialSize = MrAmPrefix + "containerlauncher.threadpool-initial-size";

		public const int DefaultMrAmContainerlauncherThreadpoolInitialSize = 10;

		/// <summary>Number of threads to handle job client RPC requests.</summary>
		public const string MrAmJobClientThreadCount = MrAmPrefix + "job.client.thread-count";

		public const int DefaultMrAmJobClientThreadCount = 1;

		/// <summary>Range of ports that the MapReduce AM can use when binding.</summary>
		/// <remarks>
		/// Range of ports that the MapReduce AM can use when binding. Leave blank
		/// if you want all possible ports.
		/// </remarks>
		public const string MrAmJobClientPortRange = MrAmPrefix + "job.client.port-range";

		/// <summary>Enable blacklisting of nodes in the job.</summary>
		public const string MrAmJobNodeBlacklistingEnable = MrAmPrefix + "job.node-blacklisting.enable";

		/// <summary>Ignore blacklisting if a certain percentage of nodes have been blacklisted
		/// 	</summary>
		public const string MrAmIgnoreBlacklistingBlacklistedNodePerecent = MrAmPrefix + 
			"job.node-blacklisting.ignore-threshold-node-percent";

		public const int DefaultMrAmIgnoreBlacklistingBlacklistedNodePercent = 33;

		/// <summary>Enable job recovery.</summary>
		public const string MrAmJobRecoveryEnable = MrAmPrefix + "job.recovery.enable";

		public const bool MrAmJobRecoveryEnableDefault = true;

		/// <summary>
		/// Limit on the number of reducers that can be preempted to ensure that at
		/// least one map task can run if it needs to.
		/// </summary>
		/// <remarks>
		/// Limit on the number of reducers that can be preempted to ensure that at
		/// least one map task can run if it needs to. Percentage between 0.0 and 1.0
		/// </remarks>
		public const string MrAmJobReducePreemptionLimit = MrAmPrefix + "job.reduce.preemption.limit";

		public const float DefaultMrAmJobReducePreemptionLimit = 0.5f;

		/// <summary>AM ACL disabled.</summary>
		public const string JobAmAccessDisabled = "mapreduce.job.am-access-disabled";

		public const bool DefaultJobAmAccessDisabled = false;

		/// <summary>Limit reduces starting until a certain percentage of maps have finished.
		/// 	</summary>
		/// <remarks>
		/// Limit reduces starting until a certain percentage of maps have finished.
		/// Percentage between 0.0 and 1.0
		/// </remarks>
		public const string MrAmJobReduceRampupUpLimit = MrAmPrefix + "job.reduce.rampup.limit";

		public const float DefaultMrAmJobReduceRampUpLimit = 0.5f;

		/// <summary>The class that should be used for speculative execution calculations.</summary>
		public const string MrAmJobSpeculator = MrAmPrefix + "job.speculator.class";

		/// <summary>Class used to estimate task resource needs.</summary>
		public const string MrAmTaskEstimator = MrAmPrefix + "job.task.estimator.class";

		/// <summary>The lambda value in the smoothing function of the task estimator.</summary>
		public const string MrAmTaskEstimatorSmoothLambdaMs = MrAmPrefix + "job.task.estimator.exponential.smooth.lambda-ms";

		public const long DefaultMrAmTaskEstimatorSmoothLambdaMs = 1000L * 60;

		/// <summary>true if the smoothing rate should be exponential.</summary>
		public const string MrAmTaskEstimatorExponentialRateEnable = MrAmPrefix + "job.task.estimator.exponential.smooth.rate";

		/// <summary>The number of threads used to handle task RPC calls.</summary>
		public const string MrAmTaskListenerThreadCount = MrAmPrefix + "job.task.listener.thread-count";

		public const int DefaultMrAmTaskListenerThreadCount = 30;

		/// <summary>How often the AM should send heartbeats to the RM.</summary>
		public const string MrAmToRmHeartbeatIntervalMs = MrAmPrefix + "scheduler.heartbeat.interval-ms";

		public const int DefaultMrAmToRmHeartbeatIntervalMs = 1000;

		/// <summary>
		/// If contact with RM is lost, the AM will wait MR_AM_TO_RM_WAIT_INTERVAL_MS
		/// milliseconds before aborting.
		/// </summary>
		/// <remarks>
		/// If contact with RM is lost, the AM will wait MR_AM_TO_RM_WAIT_INTERVAL_MS
		/// milliseconds before aborting. During this interval, AM will still try
		/// to contact the RM.
		/// </remarks>
		public const string MrAmToRmWaitIntervalMs = MrAmPrefix + "scheduler.connection.wait.interval-ms";

		public const int DefaultMrAmToRmWaitIntervalMs = 360000;

		/// <summary>
		/// How long to wait in milliseconds for the output committer to cancel
		/// an operation when the job is being killed
		/// </summary>
		public const string MrAmCommitterCancelTimeoutMs = MrAmPrefix + "job.committer.cancel-timeout";

		public const int DefaultMrAmCommitterCancelTimeoutMs = 60 * 1000;

		/// <summary>Defines a time window in milliseconds for output committer operations.</summary>
		/// <remarks>
		/// Defines a time window in milliseconds for output committer operations.
		/// If contact with the RM has occurred within this window then commit
		/// operations are allowed, otherwise the AM will not allow output committer
		/// operations until contact with the RM has been re-established.
		/// </remarks>
		public const string MrAmCommitWindowMs = MrAmPrefix + "job.committer.commit-window";

		public const int DefaultMrAmCommitWindowMs = 10 * 1000;

		/// <summary>Boolean.</summary>
		/// <remarks>
		/// Boolean. Create the base dirs in the JobHistoryEventHandler
		/// Set to false for multi-user clusters.  This is an internal config that
		/// is set by the MR framework and read by it too.
		/// </remarks>
		public const string MrAmCreateJhIntermediateBaseDir = MrAmPrefix + "create-intermediate-jh-base-dir";

		public const string MrAmHistoryMaxUnflushedCompleteEvents = MrAmPrefix + "history.max-unflushed-events";

		public const int DefaultMrAmHistoryMaxUnflushedCompleteEvents = 200;

		public const string MrAmHistoryJobCompleteUnflushedMultiplier = MrAmPrefix + "history.job-complete-unflushed-multiplier";

		public const int DefaultMrAmHistoryJobCompleteUnflushedMultiplier = 30;

		public const string MrAmHistoryCompleteEventFlushTimeoutMs = MrAmPrefix + "history.complete-event-flush-timeout";

		public const long DefaultMrAmHistoryCompleteEventFlushTimeoutMs = 30 * 1000l;

		public const string MrAmHistoryUseBatchedFlushQueueSizeThreshold = MrAmPrefix + "history.use-batched-flush.queue-size.threshold";

		public const int DefaultMrAmHistoryUseBatchedFlushQueueSizeThreshold = 50;

		public const string MrAmHardKillTimeoutMs = MrAmPrefix + "hard-kill-timeout-ms";

		public const long DefaultMrAmHardKillTimeoutMs = 10 * 1000l;

		/// <summary>
		/// The threshold in terms of seconds after which an unsatisfied mapper request
		/// triggers reducer preemption to free space.
		/// </summary>
		/// <remarks>
		/// The threshold in terms of seconds after which an unsatisfied mapper request
		/// triggers reducer preemption to free space. Default 0 implies that the reduces
		/// should be preempted immediately after allocation if there is currently no
		/// room for newly allocated mappers.
		/// </remarks>
		public const string MrJobReducerPreemptDelaySec = "mapreduce.job.reducer.preempt.delay.sec";

		public const int DefaultMrJobReducerPreemptDelaySec = 0;

		public const string MrAmEnv = MrAmPrefix + "env";

		public const string MrAmAdminUserEnv = MrAmPrefix + "admin.user.env";

		public const string MrAmProfile = MrAmPrefix + "profile";

		public const bool DefaultMrAmProfile = false;

		public const string MrAmProfileParams = MrAmPrefix + "profile.params";

		public const string MapredMapAdminJavaOpts = "mapreduce.admin.map.child.java.opts";

		public const string MapredReduceAdminJavaOpts = "mapreduce.admin.reduce.child.java.opts";

		public const string DefaultMapredAdminJavaOpts = "-Djava.net.preferIPv4Stack=true "
			 + "-Dhadoop.metrics.log.level=WARN ";

		public const string MapredAdminUserShell = "mapreduce.admin.user.shell";

		public const string DefaultShell = "/bin/bash";

		public const string MapredAdminUserEnv = "mapreduce.admin.user.env";

		public const string DefaultMapredAdminUserEnv = Shell.Windows ? "PATH=%PATH%;%HADOOP_COMMON_HOME%\\bin"
			 : "LD_LIBRARY_PATH=$HADOOP_COMMON_HOME/lib/native";

		public const string Workdir = "work";

		public const string Output = "output";

		public const string HadoopWorkDir = "HADOOP_WORK_DIR";

		public const string StdoutLogfileEnv = "STDOUT_LOGFILE_ENV";

		public const string StderrLogfileEnv = "STDERR_LOGFILE_ENV";

		public const string JobSubmitDir = "jobSubmitDir";

		public const string JobConfFile = "job.xml";

		public const string JobJar = "job.jar";

		public const string JobSplit = "job.split";

		public const string JobSplitMetainfo = "job.splitmetainfo";

		public const string ApplicationMasterClass = "org.apache.hadoop.mapreduce.v2.app.MRAppMaster";

		public const string MapreduceV2ChildClass = "org.apache.hadoop.mapred.YarnChild";

		public const string ApplicationAttemptId = "mapreduce.job.application.attempt.id";

		/// <summary>Job end notification.</summary>
		public const string MrJobEndNotificationUrl = "mapreduce.job.end-notification.url";

		public const string MrJobEndNotificationProxy = "mapreduce.job.end-notification.proxy";

		public const string MrJobEndNotificationTimeout = "mapreduce.job.end-notification.timeout";

		public const string MrJobEndRetryAttempts = "mapreduce.job.end-notification.retry.attempts";

		public const string MrJobEndRetryInterval = "mapreduce.job.end-notification.retry.interval";

		public const string MrJobEndNotificationMaxAttempts = "mapreduce.job.end-notification.max.attempts";

		public const string MrJobEndNotificationMaxRetryInterval = "mapreduce.job.end-notification.max.retry.interval";

		public const int DefaultMrJobEndNotificationTimeout = 5000;

		public const string MrAmSecurityServiceAuthorizationTaskUmbilical = "security.job.task.protocol.acl";

		public const string MrAmSecurityServiceAuthorizationClient = "security.job.client.protocol.acl";

		/// <summary>CLASSPATH for all YARN MapReduce applications.</summary>
		public const string MapreduceApplicationClasspath = "mapreduce.application.classpath";

		public const string MapreduceJobLog4jPropertiesFile = "mapreduce.job.log4j-properties-file";

		/// <summary>Path to MapReduce framework archive</summary>
		public const string MapreduceApplicationFrameworkPath = "mapreduce.application.framework.path";

		/// <summary>
		/// Default CLASSPATH for all YARN MapReduce applications constructed with
		/// platform-agnostic syntax.
		/// </summary>
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public const string DefaultMapreduceCrossPlatformApplicationClasspath = Apps.CrossPlatformify
			("HADOOP_MAPRED_HOME") + "/share/hadoop/mapreduce/*," + Apps.CrossPlatformify("HADOOP_MAPRED_HOME"
			) + "/share/hadoop/mapreduce/lib/*";

		/// <summary>
		/// Default platform-specific CLASSPATH for all YARN MapReduce applications
		/// constructed based on client OS syntax.
		/// </summary>
		/// <remarks>
		/// Default platform-specific CLASSPATH for all YARN MapReduce applications
		/// constructed based on client OS syntax.
		/// <p>
		/// Note: Use
		/// <see cref="DEFAULT_MAPREDUCE_CROSS_PLATFORM_APPLICATION_CLASSPATH"/>
		/// for cross-platform practice i.e. submit an application from a Windows
		/// client to a Linux/Unix server or vice versa.
		/// </p>
		/// </remarks>
		public const string DefaultMapreduceApplicationClasspath = Shell.Windows ? "%HADOOP_MAPRED_HOME%\\share\\hadoop\\mapreduce\\*,"
			 + "%HADOOP_MAPRED_HOME%\\share\\hadoop\\mapreduce\\lib\\*" : "$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/*,"
			 + "$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/lib/*";

		public const string WorkflowId = "mapreduce.workflow.id";

		public const string TaskLogBackups = MrPrefix + "task.container.log.backups";

		public const int DefaultTaskLogBackups = 0;

		public const string ReduceSeparateShuffleLog = MrPrefix + "shuffle.log.separate";

		public const bool DefaultReduceSeparateShuffleLog = true;

		public const string ShuffleLogBackups = MrPrefix + "shuffle.log.backups";

		public const int DefaultShuffleLogBackups = 0;

		public const string ShuffleLogKb = MrPrefix + "shuffle.log.limit.kb";

		public const long DefaultShuffleLogKb = 0L;

		public const string WorkflowName = "mapreduce.workflow.name";

		public const string WorkflowNodeName = "mapreduce.workflow.node.name";

		public const string WorkflowAdjacencyPrefixString = "mapreduce.workflow.adjacency.";

		public const string WorkflowAdjacencyPrefixPattern = "^mapreduce\\.workflow\\.adjacency\\..+";

		public const string WorkflowTags = "mapreduce.workflow.tags";

		/// <summary>The maximum number of application attempts.</summary>
		/// <remarks>
		/// The maximum number of application attempts.
		/// It is a application-specific setting.
		/// </remarks>
		public const string MrAmMaxAttempts = "mapreduce.am.max-attempts";

		public const int DefaultMrAmMaxAttempts = 2;

		public const string MrApplicationType = "MAPREDUCE";

		public const string MrEncryptedIntermediateData = "mapreduce.job.encrypted-intermediate-data";

		public const bool DefaultMrEncryptedIntermediateData = false;

		public const string MrEncryptedIntermediateDataKeySizeBits = "mapreduce.job.encrypted-intermediate-data-key-size-bits";

		public const int DefaultMrEncryptedIntermediateDataKeySizeBits = 128;

		public const string MrEncryptedIntermediateDataBufferKb = "mapreduce.job.encrypted-intermediate-data.buffer.kb";

		public const int DefaultMrEncryptedIntermediateDataBufferKb = 128;
		// Put all of the attribute names in here so that Job and JobContext are
		// consistent.
		// SPECULATIVE_SLOWNODE_THRESHOLD is obsolete and will be deleted in the future
		// SPECULATIVECAP is obsolete and will be deleted in the future
		// internal use only
		// ditto
		/* config for tracking the local file where all the credentials for the job
		* credentials.
		*/
		/* Configs for tracking ids of tokens used by a job */
		// don't roll
		// Environment variables used by Pipes. (TODO: these
		// do not appear to be used by current pipes source code!)
		// This should be the directory where splits file gets localized on the node
		// running ApplicationMaster.
		// This should be the name of the localized job-configuration file on the node
		// running ApplicationMaster and Task
		// This should be the name of the localized job-jar file on the node running
		// individual containers/tasks.
		/*
		* MR AM Service Authorization
		*/
		// don't roll
		// don't roll
	}

	public static class MRJobConfigConstants
	{
	}
}
