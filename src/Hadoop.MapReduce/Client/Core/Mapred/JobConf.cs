using System;
using System.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.IO.Compress;
using Org.Apache.Hadoop.Mapred.Lib;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Util;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Util;
using Org.Apache.Log4j;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	/// <summary>A map/reduce job configuration.</summary>
	/// <remarks>
	/// A map/reduce job configuration.
	/// <p><code>JobConf</code> is the primary interface for a user to describe a
	/// map-reduce job to the Hadoop framework for execution. The framework tries to
	/// faithfully execute the job as-is described by <code>JobConf</code>, however:
	/// <ol>
	/// <li>
	/// Some configuration parameters might have been marked as
	/// &lt;a href="
	/// <docRoot/>
	/// /org/apache/hadoop/conf/Configuration.html#FinalParams"&gt;
	/// final</a> by administrators and hence cannot be altered.
	/// </li>
	/// <li>
	/// While some job parameters are straight-forward to set
	/// (e.g.
	/// <see cref="SetNumReduceTasks(int)"/>
	/// ), some parameters interact subtly
	/// with the rest of the framework and/or job-configuration and is relatively
	/// more complex for the user to control finely
	/// (e.g.
	/// <see cref="SetNumMapTasks(int)"/>
	/// ).
	/// </li>
	/// </ol>
	/// <p><code>JobConf</code> typically specifies the
	/// <see cref="Mapper{K1, V1, K2, V2}"/>
	/// , combiner
	/// (if any),
	/// <see cref="Partitioner{K2, V2}"/>
	/// ,
	/// <see cref="Reducer{K2, V2, K3, V3}"/>
	/// ,
	/// <see cref="InputFormat{K, V}"/>
	/// and
	/// <see cref="OutputFormat{K, V}"/>
	/// implementations to be used etc.
	/// <p>Optionally <code>JobConf</code> is used to specify other advanced facets
	/// of the job such as <code>Comparator</code>s to be used, files to be put in
	/// the
	/// <see cref="Org.Apache.Hadoop.Mapreduce.Filecache.DistributedCache"/>
	/// , whether or not intermediate and/or job outputs
	/// are to be compressed (and how), debugability via user-provided scripts
	/// (
	/// <see cref="SetMapDebugScript(string)"/>
	/// /
	/// <see cref="SetReduceDebugScript(string)"/>
	/// ),
	/// for doing post-processing on task logs, task's stdout, stderr, syslog.
	/// and etc.</p>
	/// <p>Here is an example on how to configure a job via <code>JobConf</code>:</p>
	/// <p><blockquote><pre>
	/// // Create a new JobConf
	/// JobConf job = new JobConf(new Configuration(), MyJob.class);
	/// // Specify various job-specific parameters
	/// job.setJobName("myjob");
	/// FileInputFormat.setInputPaths(job, new Path("in"));
	/// FileOutputFormat.setOutputPath(job, new Path("out"));
	/// job.setMapperClass(MyJob.MyMapper.class);
	/// job.setCombinerClass(MyJob.MyReducer.class);
	/// job.setReducerClass(MyJob.MyReducer.class);
	/// job.setInputFormat(SequenceFileInputFormat.class);
	/// job.setOutputFormat(SequenceFileOutputFormat.class);
	/// </pre></blockquote>
	/// </remarks>
	/// <seealso cref="JobClient"/>
	/// <seealso cref="ClusterStatus"/>
	/// <seealso cref="Org.Apache.Hadoop.Util.Tool"/>
	/// <seealso cref="Org.Apache.Hadoop.Mapreduce.Filecache.DistributedCache"/>
	public class JobConf : Configuration
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Mapred.JobConf
			));

		static JobConf()
		{
			ConfigUtil.LoadResources();
		}

		[System.ObsoleteAttribute(@"Use MapreduceJobMapMemoryMbProperty andMapreduceJobReduceMemoryMbProperty"
			)]
		public const string MapredTaskMaxvmemProperty = "mapred.task.maxvmem";

		[System.ObsoleteAttribute(@"")]
		public const string UpperLimitOnTaskVmemProperty = "mapred.task.limit.maxvmem";

		[System.ObsoleteAttribute]
		public const string MapredTaskDefaultMaxvmemProperty = "mapred.task.default.maxvmem";

		[System.ObsoleteAttribute]
		public const string MapredTaskMaxpmemProperty = "mapred.task.maxpmem";

		/// <summary>
		/// A value which if set for memory related configuration options,
		/// indicates that the options are turned off.
		/// </summary>
		/// <remarks>
		/// A value which if set for memory related configuration options,
		/// indicates that the options are turned off.
		/// Deprecated because it makes no sense in the context of MR2.
		/// </remarks>
		[Obsolete]
		public const long DisabledMemoryLimit = -1L;

		/// <summary>Property name for the configuration property mapreduce.cluster.local.dir
		/// 	</summary>
		public const string MapredLocalDirProperty = MRConfig.LocalDir;

		/// <summary>
		/// Name of the queue to which jobs will be submitted, if no queue
		/// name is mentioned.
		/// </summary>
		public const string DefaultQueueName = "default";

		internal const string MapreduceJobMapMemoryMbProperty = JobContext.MapMemoryMb;

		internal const string MapreduceJobReduceMemoryMbProperty = JobContext.ReduceMemoryMb;

		/// <summary>
		/// The variable is kept for M/R 1.x applications, while M/R 2.x applications
		/// should use
		/// <see cref="MapreduceJobMapMemoryMbProperty"/>
		/// </summary>
		[Obsolete]
		public const string MapredJobMapMemoryMbProperty = "mapred.job.map.memory.mb";

		/// <summary>
		/// The variable is kept for M/R 1.x applications, while M/R 2.x applications
		/// should use
		/// <see cref="MapreduceJobReduceMemoryMbProperty"/>
		/// </summary>
		[Obsolete]
		public const string MapredJobReduceMemoryMbProperty = "mapred.job.reduce.memory.mb";

		/// <summary>Pattern for the default unpacking behavior for job jars</summary>
		public static readonly Sharpen.Pattern UnpackJarPatternDefault = Sharpen.Pattern.
			Compile("(?:classes/|lib/).*");

		/// <summary>
		/// Configuration key to set the java command line options for the child
		/// map and reduce tasks.
		/// </summary>
		/// <remarks>
		/// Configuration key to set the java command line options for the child
		/// map and reduce tasks.
		/// Java opts for the task tracker child processes.
		/// The following symbol, if present, will be interpolated: @taskid@.
		/// It is replaced by current TaskID. Any other occurrences of '@' will go
		/// unchanged.
		/// For example, to enable verbose gc logging to a file named for the taskid in
		/// /tmp and to set the heap maximum to be a gigabyte, pass a 'value' of:
		/// -Xmx1024m -verbose:gc -Xloggc:/tmp/@taskid@.gc
		/// The configuration variable
		/// <see cref="MapredTaskEnv"/>
		/// can be used to pass
		/// other environment variables to the child processes.
		/// </remarks>
		[System.ObsoleteAttribute(@"Use MapredMapTaskJavaOpts or MapredReduceTaskJavaOpts"
			)]
		public const string MapredTaskJavaOpts = "mapred.child.java.opts";

		/// <summary>Configuration key to set the java command line options for the map tasks.
		/// 	</summary>
		/// <remarks>
		/// Configuration key to set the java command line options for the map tasks.
		/// Java opts for the task tracker child map processes.
		/// The following symbol, if present, will be interpolated: @taskid@.
		/// It is replaced by current TaskID. Any other occurrences of '@' will go
		/// unchanged.
		/// For example, to enable verbose gc logging to a file named for the taskid in
		/// /tmp and to set the heap maximum to be a gigabyte, pass a 'value' of:
		/// -Xmx1024m -verbose:gc -Xloggc:/tmp/@taskid@.gc
		/// The configuration variable
		/// <see cref="MapredMapTaskEnv"/>
		/// can be used to pass
		/// other environment variables to the map processes.
		/// </remarks>
		public const string MapredMapTaskJavaOpts = JobContext.MapJavaOpts;

		/// <summary>Configuration key to set the java command line options for the reduce tasks.
		/// 	</summary>
		/// <remarks>
		/// Configuration key to set the java command line options for the reduce tasks.
		/// Java opts for the task tracker child reduce processes.
		/// The following symbol, if present, will be interpolated: @taskid@.
		/// It is replaced by current TaskID. Any other occurrences of '@' will go
		/// unchanged.
		/// For example, to enable verbose gc logging to a file named for the taskid in
		/// /tmp and to set the heap maximum to be a gigabyte, pass a 'value' of:
		/// -Xmx1024m -verbose:gc -Xloggc:/tmp/@taskid@.gc
		/// The configuration variable
		/// <see cref="MapredReduceTaskEnv"/>
		/// can be used to
		/// pass process environment variables to the reduce processes.
		/// </remarks>
		public const string MapredReduceTaskJavaOpts = JobContext.ReduceJavaOpts;

		public const string DefaultMapredTaskJavaOpts = "-Xmx200m";

		[System.ObsoleteAttribute(@"Configuration key to set the maximum virtual memory available to the child map and reduce tasks (in kilo-bytes). This has been deprecated and will no longer have any effect."
			)]
		public const string MapredTaskUlimit = "mapred.child.ulimit";

		[System.ObsoleteAttribute(@"Configuration key to set the maximum virtual memory available to the map tasks (in kilo-bytes). This has been deprecated and will no longer have any effect."
			)]
		public const string MapredMapTaskUlimit = "mapreduce.map.ulimit";

		[System.ObsoleteAttribute(@"Configuration key to set the maximum virtual memory available to the reduce tasks (in kilo-bytes). This has been deprecated and will no longer have any effect."
			)]
		public const string MapredReduceTaskUlimit = "mapreduce.reduce.ulimit";

		/// <summary>Configuration key to set the environment of the child map/reduce tasks.</summary>
		/// <remarks>
		/// Configuration key to set the environment of the child map/reduce tasks.
		/// The format of the value is <code>k1=v1,k2=v2</code>. Further it can
		/// reference existing environment variables via <code>$key</code> on
		/// Linux or <code>%key%</code> on Windows.
		/// Example:
		/// <ul>
		/// <li> A=foo - This will set the env variable A to foo. </li>
		/// <li> B=$X:c This is inherit tasktracker's X env variable on Linux. </li>
		/// <li> B=%X%;c This is inherit tasktracker's X env variable on Windows. </li>
		/// </ul>
		/// </remarks>
		[System.ObsoleteAttribute(@"Use MapredMapTaskEnv or MapredReduceTaskEnv")]
		public const string MapredTaskEnv = "mapred.child.env";

		/// <summary>Configuration key to set the environment of the child map tasks.</summary>
		/// <remarks>
		/// Configuration key to set the environment of the child map tasks.
		/// The format of the value is <code>k1=v1,k2=v2</code>. Further it can
		/// reference existing environment variables via <code>$key</code> on
		/// Linux or <code>%key%</code> on Windows.
		/// Example:
		/// <ul>
		/// <li> A=foo - This will set the env variable A to foo. </li>
		/// <li> B=$X:c This is inherit tasktracker's X env variable on Linux. </li>
		/// <li> B=%X%;c This is inherit tasktracker's X env variable on Windows. </li>
		/// </ul>
		/// </remarks>
		public const string MapredMapTaskEnv = JobContext.MapEnv;

		/// <summary>Configuration key to set the environment of the child reduce tasks.</summary>
		/// <remarks>
		/// Configuration key to set the environment of the child reduce tasks.
		/// The format of the value is <code>k1=v1,k2=v2</code>. Further it can
		/// reference existing environment variables via <code>$key</code> on
		/// Linux or <code>%key%</code> on Windows.
		/// Example:
		/// <ul>
		/// <li> A=foo - This will set the env variable A to foo. </li>
		/// <li> B=$X:c This is inherit tasktracker's X env variable on Linux. </li>
		/// <li> B=%X%;c This is inherit tasktracker's X env variable on Windows. </li>
		/// </ul>
		/// </remarks>
		public const string MapredReduceTaskEnv = JobContext.ReduceEnv;

		private Credentials credentials = new Credentials();

		/// <summary>
		/// Configuration key to set the logging
		/// <see cref="Org.Apache.Log4j.Level"/>
		/// for the map task.
		/// The allowed logging levels are:
		/// OFF, FATAL, ERROR, WARN, INFO, DEBUG, TRACE and ALL.
		/// </summary>
		public const string MapredMapTaskLogLevel = JobContext.MapLogLevel;

		/// <summary>
		/// Configuration key to set the logging
		/// <see cref="Org.Apache.Log4j.Level"/>
		/// for the reduce task.
		/// The allowed logging levels are:
		/// OFF, FATAL, ERROR, WARN, INFO, DEBUG, TRACE and ALL.
		/// </summary>
		public const string MapredReduceTaskLogLevel = JobContext.ReduceLogLevel;

		/// <summary>Default logging level for map/reduce tasks.</summary>
		public static readonly Level DefaultLogLevel = Level.Info;

		/// <summary>
		/// The variable is kept for M/R 1.x applications, M/R 2.x applications should
		/// use
		/// <see cref="Org.Apache.Hadoop.Mapreduce.MRJobConfig.WorkflowId"/>
		/// instead
		/// </summary>
		[Obsolete]
		public const string WorkflowId = MRJobConfig.WorkflowId;

		/// <summary>
		/// The variable is kept for M/R 1.x applications, M/R 2.x applications should
		/// use
		/// <see cref="Org.Apache.Hadoop.Mapreduce.MRJobConfig.WorkflowName"/>
		/// instead
		/// </summary>
		[Obsolete]
		public const string WorkflowName = MRJobConfig.WorkflowName;

		/// <summary>
		/// The variable is kept for M/R 1.x applications, M/R 2.x applications should
		/// use
		/// <see cref="Org.Apache.Hadoop.Mapreduce.MRJobConfig.WorkflowNodeName"/>
		/// instead
		/// </summary>
		[Obsolete]
		public const string WorkflowNodeName = MRJobConfig.WorkflowNodeName;

		/// <summary>
		/// The variable is kept for M/R 1.x applications, M/R 2.x applications should
		/// use
		/// <see cref="Org.Apache.Hadoop.Mapreduce.MRJobConfig.WorkflowAdjacencyPrefixString"
		/// 	/>
		/// instead
		/// </summary>
		[Obsolete]
		public const string WorkflowAdjacencyPrefixString = MRJobConfig.WorkflowAdjacencyPrefixString;

		/// <summary>
		/// The variable is kept for M/R 1.x applications, M/R 2.x applications should
		/// use
		/// <see cref="Org.Apache.Hadoop.Mapreduce.MRJobConfig.WorkflowAdjacencyPrefixPattern
		/// 	"/>
		/// instead
		/// </summary>
		[Obsolete]
		public const string WorkflowAdjacencyPrefixPattern = MRJobConfig.WorkflowAdjacencyPrefixPattern;

		/// <summary>
		/// The variable is kept for M/R 1.x applications, M/R 2.x applications should
		/// use
		/// <see cref="Org.Apache.Hadoop.Mapreduce.MRJobConfig.WorkflowTags"/>
		/// instead
		/// </summary>
		[Obsolete]
		public const string WorkflowTags = MRJobConfig.WorkflowTags;

		/// <summary>
		/// The variable is kept for M/R 1.x applications, M/R 2.x applications should
		/// not use it
		/// </summary>
		[Obsolete]
		public const string MapreduceRecoverJob = "mapreduce.job.restart.recover";

		/// <summary>
		/// The variable is kept for M/R 1.x applications, M/R 2.x applications should
		/// not use it
		/// </summary>
		[Obsolete]
		public const bool DefaultMapreduceRecoverJob = true;

		/// <summary>Construct a map/reduce job configuration.</summary>
		public JobConf()
		{
			CheckAndWarnDeprecation();
		}

		/// <summary>Construct a map/reduce job configuration.</summary>
		/// <param name="exampleClass">a class whose containing jar is used as the job's jar.
		/// 	</param>
		public JobConf(Type exampleClass)
		{
			SetJarByClass(exampleClass);
			CheckAndWarnDeprecation();
		}

		/// <summary>Construct a map/reduce job configuration.</summary>
		/// <param name="conf">a Configuration whose settings will be inherited.</param>
		public JobConf(Configuration conf)
			: base(conf)
		{
			if (conf is Org.Apache.Hadoop.Mapred.JobConf)
			{
				Org.Apache.Hadoop.Mapred.JobConf that = (Org.Apache.Hadoop.Mapred.JobConf)conf;
				credentials = that.credentials;
			}
			CheckAndWarnDeprecation();
		}

		/// <summary>Construct a map/reduce job configuration.</summary>
		/// <param name="conf">a Configuration whose settings will be inherited.</param>
		/// <param name="exampleClass">a class whose containing jar is used as the job's jar.
		/// 	</param>
		public JobConf(Configuration conf, Type exampleClass)
			: this(conf)
		{
			SetJarByClass(exampleClass);
		}

		/// <summary>Construct a map/reduce configuration.</summary>
		/// <param name="config">a Configuration-format XML job description file.</param>
		public JobConf(string config)
			: this(new Path(config))
		{
		}

		/// <summary>Construct a map/reduce configuration.</summary>
		/// <param name="config">a Configuration-format XML job description file.</param>
		public JobConf(Path config)
			: base()
		{
			AddResource(config);
			CheckAndWarnDeprecation();
		}

		/// <summary>
		/// A new map/reduce configuration where the behavior of reading from the
		/// default resources can be turned off.
		/// </summary>
		/// <remarks>
		/// A new map/reduce configuration where the behavior of reading from the
		/// default resources can be turned off.
		/// <p>
		/// If the parameter
		/// <paramref name="loadDefaults"/>
		/// is false, the new instance
		/// will not load resources from the default files.
		/// </remarks>
		/// <param name="loadDefaults">specifies whether to load from the default files</param>
		public JobConf(bool loadDefaults)
			: base(loadDefaults)
		{
			CheckAndWarnDeprecation();
		}

		/// <summary>Get credentials for the job.</summary>
		/// <returns>credentials for the job</returns>
		public virtual Credentials GetCredentials()
		{
			return credentials;
		}

		[InterfaceAudience.Private]
		public virtual void SetCredentials(Credentials credentials)
		{
			this.credentials = credentials;
		}

		/// <summary>Get the user jar for the map-reduce job.</summary>
		/// <returns>the user jar for the map-reduce job.</returns>
		public virtual string GetJar()
		{
			return Get(JobContext.Jar);
		}

		/// <summary>Set the user jar for the map-reduce job.</summary>
		/// <param name="jar">the user jar for the map-reduce job.</param>
		public virtual void SetJar(string jar)
		{
			Set(JobContext.Jar, jar);
		}

		/// <summary>Get the pattern for jar contents to unpack on the tasktracker</summary>
		public virtual Sharpen.Pattern GetJarUnpackPattern()
		{
			return GetPattern(JobContext.JarUnpackPattern, UnpackJarPatternDefault);
		}

		/// <summary>Set the job's jar file by finding an example class location.</summary>
		/// <param name="cls">the example class.</param>
		public virtual void SetJarByClass(Type cls)
		{
			string jar = ClassUtil.FindContainingJar(cls);
			if (jar != null)
			{
				SetJar(jar);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual string[] GetLocalDirs()
		{
			return GetTrimmedStrings(MRConfig.LocalDir);
		}

		/// <summary>Use MRAsyncDiskService.moveAndDeleteAllVolumes instead.</summary>
		/// <exception cref="System.IO.IOException"/>
		[Obsolete]
		public virtual void DeleteLocalFiles()
		{
			string[] localDirs = GetLocalDirs();
			for (int i = 0; i < localDirs.Length; i++)
			{
				FileSystem.GetLocal(this).Delete(new Path(localDirs[i]), true);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void DeleteLocalFiles(string subdir)
		{
			string[] localDirs = GetLocalDirs();
			for (int i = 0; i < localDirs.Length; i++)
			{
				FileSystem.GetLocal(this).Delete(new Path(localDirs[i], subdir), true);
			}
		}

		/// <summary>Constructs a local file name.</summary>
		/// <remarks>
		/// Constructs a local file name. Files are distributed among configured
		/// local directories.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public virtual Path GetLocalPath(string pathString)
		{
			return GetLocalPath(MRConfig.LocalDir, pathString);
		}

		/// <summary>Get the reported username for this job.</summary>
		/// <returns>the username</returns>
		public virtual string GetUser()
		{
			return Get(JobContext.UserName);
		}

		/// <summary>Set the reported username for this job.</summary>
		/// <param name="user">the username for this job.</param>
		public virtual void SetUser(string user)
		{
			Set(JobContext.UserName, user);
		}

		/// <summary>
		/// Set whether the framework should keep the intermediate files for
		/// failed tasks.
		/// </summary>
		/// <param name="keep">
		/// <code>true</code> if framework should keep the intermediate files
		/// for failed tasks, <code>false</code> otherwise.
		/// </param>
		public virtual void SetKeepFailedTaskFiles(bool keep)
		{
			SetBoolean(JobContext.PreserveFailedTaskFiles, keep);
		}

		/// <summary>Should the temporary files for failed tasks be kept?</summary>
		/// <returns>should the files be kept?</returns>
		public virtual bool GetKeepFailedTaskFiles()
		{
			return GetBoolean(JobContext.PreserveFailedTaskFiles, false);
		}

		/// <summary>Set a regular expression for task names that should be kept.</summary>
		/// <remarks>
		/// Set a regular expression for task names that should be kept.
		/// The regular expression ".*_m_000123_0" would keep the files
		/// for the first instance of map 123 that ran.
		/// </remarks>
		/// <param name="pattern">
		/// the java.util.regex.Pattern to match against the
		/// task names.
		/// </param>
		public virtual void SetKeepTaskFilesPattern(string pattern)
		{
			Set(JobContext.PreserveFilesPattern, pattern);
		}

		/// <summary>
		/// Get the regular expression that is matched against the task names
		/// to see if we need to keep the files.
		/// </summary>
		/// <returns>the pattern as a string, if it was set, othewise null.</returns>
		public virtual string GetKeepTaskFilesPattern()
		{
			return Get(JobContext.PreserveFilesPattern);
		}

		/// <summary>Set the current working directory for the default file system.</summary>
		/// <param name="dir">the new current working directory.</param>
		public virtual void SetWorkingDirectory(Path dir)
		{
			dir = new Path(GetWorkingDirectory(), dir);
			Set(JobContext.WorkingDir, dir.ToString());
		}

		/// <summary>Get the current working directory for the default file system.</summary>
		/// <returns>the directory name.</returns>
		public virtual Path GetWorkingDirectory()
		{
			string name = Get(JobContext.WorkingDir);
			if (name != null)
			{
				return new Path(name);
			}
			else
			{
				try
				{
					Path dir = FileSystem.Get(this).GetWorkingDirectory();
					Set(JobContext.WorkingDir, dir.ToString());
					return dir;
				}
				catch (IOException e)
				{
					throw new RuntimeException(e);
				}
			}
		}

		/// <summary>
		/// Sets the number of tasks that a spawned task JVM should run
		/// before it exits
		/// </summary>
		/// <param name="numTasks">
		/// the number of tasks to execute; defaults to 1;
		/// -1 signifies no limit
		/// </param>
		public virtual void SetNumTasksToExecutePerJvm(int numTasks)
		{
			SetInt(JobContext.JvmNumtasksTorun, numTasks);
		}

		/// <summary>Get the number of tasks that a spawned JVM should execute</summary>
		public virtual int GetNumTasksToExecutePerJvm()
		{
			return GetInt(JobContext.JvmNumtasksTorun, 1);
		}

		/// <summary>
		/// Get the
		/// <see cref="InputFormat{K, V}"/>
		/// implementation for the map-reduce job,
		/// defaults to
		/// <see cref="TextInputFormat"/>
		/// if not specified explicity.
		/// </summary>
		/// <returns>
		/// the
		/// <see cref="InputFormat{K, V}"/>
		/// implementation for the map-reduce job.
		/// </returns>
		public virtual InputFormat GetInputFormat()
		{
			return ReflectionUtils.NewInstance(GetClass<InputFormat>("mapred.input.format.class"
				, typeof(TextInputFormat)), this);
		}

		/// <summary>
		/// Set the
		/// <see cref="InputFormat{K, V}"/>
		/// implementation for the map-reduce job.
		/// </summary>
		/// <param name="theClass">
		/// the
		/// <see cref="InputFormat{K, V}"/>
		/// implementation for the map-reduce
		/// job.
		/// </param>
		public virtual void SetInputFormat(Type theClass)
		{
			SetClass("mapred.input.format.class", theClass, typeof(InputFormat));
		}

		/// <summary>
		/// Get the
		/// <see cref="OutputFormat{K, V}"/>
		/// implementation for the map-reduce job,
		/// defaults to
		/// <see cref="TextOutputFormat{K, V}"/>
		/// if not specified explicity.
		/// </summary>
		/// <returns>
		/// the
		/// <see cref="OutputFormat{K, V}"/>
		/// implementation for the map-reduce job.
		/// </returns>
		public virtual OutputFormat GetOutputFormat()
		{
			return ReflectionUtils.NewInstance(GetClass<OutputFormat>("mapred.output.format.class"
				, typeof(TextOutputFormat)), this);
		}

		/// <summary>
		/// Get the
		/// <see cref="OutputCommitter"/>
		/// implementation for the map-reduce job,
		/// defaults to
		/// <see cref="FileOutputCommitter"/>
		/// if not specified explicitly.
		/// </summary>
		/// <returns>
		/// the
		/// <see cref="OutputCommitter"/>
		/// implementation for the map-reduce job.
		/// </returns>
		public virtual OutputCommitter GetOutputCommitter()
		{
			return (OutputCommitter)ReflectionUtils.NewInstance(GetClass<OutputCommitter>("mapred.output.committer.class"
				, typeof(FileOutputCommitter)), this);
		}

		/// <summary>
		/// Set the
		/// <see cref="OutputCommitter"/>
		/// implementation for the map-reduce job.
		/// </summary>
		/// <param name="theClass">
		/// the
		/// <see cref="OutputCommitter"/>
		/// implementation for the map-reduce
		/// job.
		/// </param>
		public virtual void SetOutputCommitter(Type theClass)
		{
			SetClass("mapred.output.committer.class", theClass, typeof(OutputCommitter));
		}

		/// <summary>
		/// Set the
		/// <see cref="OutputFormat{K, V}"/>
		/// implementation for the map-reduce job.
		/// </summary>
		/// <param name="theClass">
		/// the
		/// <see cref="OutputFormat{K, V}"/>
		/// implementation for the map-reduce
		/// job.
		/// </param>
		public virtual void SetOutputFormat(Type theClass)
		{
			SetClass("mapred.output.format.class", theClass, typeof(OutputFormat));
		}

		/// <summary>Should the map outputs be compressed before transfer?</summary>
		/// <param name="compress">should the map outputs be compressed?</param>
		public virtual void SetCompressMapOutput(bool compress)
		{
			SetBoolean(JobContext.MapOutputCompress, compress);
		}

		/// <summary>Are the outputs of the maps be compressed?</summary>
		/// <returns>
		/// <code>true</code> if the outputs of the maps are to be compressed,
		/// <code>false</code> otherwise.
		/// </returns>
		public virtual bool GetCompressMapOutput()
		{
			return GetBoolean(JobContext.MapOutputCompress, false);
		}

		/// <summary>
		/// Set the given class as the
		/// <see cref="Org.Apache.Hadoop.IO.Compress.CompressionCodec"/>
		/// for the map outputs.
		/// </summary>
		/// <param name="codecClass">
		/// the
		/// <see cref="Org.Apache.Hadoop.IO.Compress.CompressionCodec"/>
		/// class that will compress
		/// the map outputs.
		/// </param>
		public virtual void SetMapOutputCompressorClass(Type codecClass)
		{
			SetCompressMapOutput(true);
			SetClass(JobContext.MapOutputCompressCodec, codecClass, typeof(CompressionCodec));
		}

		/// <summary>
		/// Get the
		/// <see cref="Org.Apache.Hadoop.IO.Compress.CompressionCodec"/>
		/// for compressing the map outputs.
		/// </summary>
		/// <param name="defaultValue">
		/// the
		/// <see cref="Org.Apache.Hadoop.IO.Compress.CompressionCodec"/>
		/// to return if not set
		/// </param>
		/// <returns>
		/// the
		/// <see cref="Org.Apache.Hadoop.IO.Compress.CompressionCodec"/>
		/// class that should be used to compress the
		/// map outputs.
		/// </returns>
		/// <exception cref="System.ArgumentException">if the class was specified, but not found
		/// 	</exception>
		public virtual Type GetMapOutputCompressorClass(Type defaultValue)
		{
			Type codecClass = defaultValue;
			string name = Get(JobContext.MapOutputCompressCodec);
			if (name != null)
			{
				try
				{
					codecClass = GetClassByName(name).AsSubclass<CompressionCodec>();
				}
				catch (TypeLoadException e)
				{
					throw new ArgumentException("Compression codec " + name + " was not found.", e);
				}
			}
			return codecClass;
		}

		/// <summary>Get the key class for the map output data.</summary>
		/// <remarks>
		/// Get the key class for the map output data. If it is not set, use the
		/// (final) output key class. This allows the map output key class to be
		/// different than the final output key class.
		/// </remarks>
		/// <returns>the map output key class.</returns>
		public virtual Type GetMapOutputKeyClass()
		{
			Type retv = GetClass<object>(JobContext.MapOutputKeyClass, null);
			if (retv == null)
			{
				retv = GetOutputKeyClass();
			}
			return retv;
		}

		/// <summary>Set the key class for the map output data.</summary>
		/// <remarks>
		/// Set the key class for the map output data. This allows the user to
		/// specify the map output key class to be different than the final output
		/// value class.
		/// </remarks>
		/// <param name="theClass">the map output key class.</param>
		public virtual void SetMapOutputKeyClass(Type theClass)
		{
			SetClass(JobContext.MapOutputKeyClass, theClass, typeof(object));
		}

		/// <summary>Get the value class for the map output data.</summary>
		/// <remarks>
		/// Get the value class for the map output data. If it is not set, use the
		/// (final) output value class This allows the map output value class to be
		/// different than the final output value class.
		/// </remarks>
		/// <returns>the map output value class.</returns>
		public virtual Type GetMapOutputValueClass()
		{
			Type retv = GetClass<object>(JobContext.MapOutputValueClass, null);
			if (retv == null)
			{
				retv = GetOutputValueClass();
			}
			return retv;
		}

		/// <summary>Set the value class for the map output data.</summary>
		/// <remarks>
		/// Set the value class for the map output data. This allows the user to
		/// specify the map output value class to be different than the final output
		/// value class.
		/// </remarks>
		/// <param name="theClass">the map output value class.</param>
		public virtual void SetMapOutputValueClass(Type theClass)
		{
			SetClass(JobContext.MapOutputValueClass, theClass, typeof(object));
		}

		/// <summary>Get the key class for the job output data.</summary>
		/// <returns>the key class for the job output data.</returns>
		public virtual Type GetOutputKeyClass()
		{
			return GetClass<object>(JobContext.OutputKeyClass, typeof(LongWritable));
		}

		/// <summary>Set the key class for the job output data.</summary>
		/// <param name="theClass">the key class for the job output data.</param>
		public virtual void SetOutputKeyClass(Type theClass)
		{
			SetClass(JobContext.OutputKeyClass, theClass, typeof(object));
		}

		/// <summary>
		/// Get the
		/// <see cref="Org.Apache.Hadoop.IO.RawComparator{T}"/>
		/// comparator used to compare keys.
		/// </summary>
		/// <returns>
		/// the
		/// <see cref="Org.Apache.Hadoop.IO.RawComparator{T}"/>
		/// comparator used to compare keys.
		/// </returns>
		public virtual RawComparator GetOutputKeyComparator()
		{
			Type theClass = GetClass<RawComparator>(JobContext.KeyComparator, null);
			if (theClass != null)
			{
				return ReflectionUtils.NewInstance(theClass, this);
			}
			return WritableComparator.Get(GetMapOutputKeyClass().AsSubclass<WritableComparable
				>(), this);
		}

		/// <summary>
		/// Set the
		/// <see cref="Org.Apache.Hadoop.IO.RawComparator{T}"/>
		/// comparator used to compare keys.
		/// </summary>
		/// <param name="theClass">
		/// the
		/// <see cref="Org.Apache.Hadoop.IO.RawComparator{T}"/>
		/// comparator used to
		/// compare keys.
		/// </param>
		/// <seealso cref="SetOutputValueGroupingComparator(System.Type{T})"></seealso>
		public virtual void SetOutputKeyComparatorClass(Type theClass)
		{
			SetClass(JobContext.KeyComparator, theClass, typeof(RawComparator));
		}

		/// <summary>
		/// Set the
		/// <see cref="Org.Apache.Hadoop.Mapred.Lib.KeyFieldBasedComparator{K, V}"/>
		/// options used to compare keys.
		/// </summary>
		/// <param name="keySpec">
		/// the key specification of the form -k pos1[,pos2], where,
		/// pos is of the form f[.c][opts], where f is the number
		/// of the key field to use, and c is the number of the first character from
		/// the beginning of the field. Fields and character posns are numbered
		/// starting with 1; a character position of zero in pos2 indicates the
		/// field's last character. If '.c' is omitted from pos1, it defaults to 1
		/// (the beginning of the field); if omitted from pos2, it defaults to 0
		/// (the end of the field). opts are ordering options. The supported options
		/// are:
		/// -n, (Sort numerically)
		/// -r, (Reverse the result of comparison)
		/// </param>
		public virtual void SetKeyFieldComparatorOptions(string keySpec)
		{
			SetOutputKeyComparatorClass(typeof(KeyFieldBasedComparator));
			Set(KeyFieldBasedComparator.ComparatorOptions, keySpec);
		}

		/// <summary>
		/// Get the
		/// <see cref="Org.Apache.Hadoop.Mapred.Lib.KeyFieldBasedComparator{K, V}"/>
		/// options
		/// </summary>
		public virtual string GetKeyFieldComparatorOption()
		{
			return Get(KeyFieldBasedComparator.ComparatorOptions);
		}

		/// <summary>
		/// Set the
		/// <see cref="Org.Apache.Hadoop.Mapred.Lib.KeyFieldBasedPartitioner{K2, V2}"/>
		/// options used for
		/// <see cref="Partitioner{K2, V2}"/>
		/// </summary>
		/// <param name="keySpec">
		/// the key specification of the form -k pos1[,pos2], where,
		/// pos is of the form f[.c][opts], where f is the number
		/// of the key field to use, and c is the number of the first character from
		/// the beginning of the field. Fields and character posns are numbered
		/// starting with 1; a character position of zero in pos2 indicates the
		/// field's last character. If '.c' is omitted from pos1, it defaults to 1
		/// (the beginning of the field); if omitted from pos2, it defaults to 0
		/// (the end of the field).
		/// </param>
		public virtual void SetKeyFieldPartitionerOptions(string keySpec)
		{
			SetPartitionerClass(typeof(KeyFieldBasedPartitioner));
			Set(KeyFieldBasedPartitioner.PartitionerOptions, keySpec);
		}

		/// <summary>
		/// Get the
		/// <see cref="Org.Apache.Hadoop.Mapred.Lib.KeyFieldBasedPartitioner{K2, V2}"/>
		/// options
		/// </summary>
		public virtual string GetKeyFieldPartitionerOption()
		{
			return Get(KeyFieldBasedPartitioner.PartitionerOptions);
		}

		/// <summary>
		/// Get the user defined
		/// <see cref="Org.Apache.Hadoop.IO.WritableComparable{T}"/>
		/// comparator for
		/// grouping keys of inputs to the combiner.
		/// </summary>
		/// <returns>comparator set by the user for grouping values.</returns>
		/// <seealso cref="SetCombinerKeyGroupingComparator(System.Type{T})">for details.</seealso>
		public virtual RawComparator GetCombinerKeyGroupingComparator()
		{
			Type theClass = GetClass<RawComparator>(JobContext.CombinerGroupComparatorClass, 
				null);
			if (theClass == null)
			{
				return GetOutputKeyComparator();
			}
			return ReflectionUtils.NewInstance(theClass, this);
		}

		/// <summary>
		/// Get the user defined
		/// <see cref="Org.Apache.Hadoop.IO.WritableComparable{T}"/>
		/// comparator for
		/// grouping keys of inputs to the reduce.
		/// </summary>
		/// <returns>comparator set by the user for grouping values.</returns>
		/// <seealso cref="SetOutputValueGroupingComparator(System.Type{T})">for details.</seealso>
		public virtual RawComparator GetOutputValueGroupingComparator()
		{
			Type theClass = GetClass<RawComparator>(JobContext.GroupComparatorClass, null);
			if (theClass == null)
			{
				return GetOutputKeyComparator();
			}
			return ReflectionUtils.NewInstance(theClass, this);
		}

		/// <summary>
		/// Set the user defined
		/// <see cref="Org.Apache.Hadoop.IO.RawComparator{T}"/>
		/// comparator for
		/// grouping keys in the input to the combiner.
		/// <p>This comparator should be provided if the equivalence rules for keys
		/// for sorting the intermediates are different from those for grouping keys
		/// before each call to
		/// <see cref="Reducer{K2, V2, K3, V3}.Reduce(object, System.Collections.IEnumerator{E}, OutputCollector{K, V}, Reporter)
		/// 	"/>
		/// .</p>
		/// <p>For key-value pairs (K1,V1) and (K2,V2), the values (V1, V2) are passed
		/// in a single call to the reduce function if K1 and K2 compare as equal.</p>
		/// <p>Since
		/// <see cref="SetOutputKeyComparatorClass(System.Type{T})"/>
		/// can be used to control
		/// how keys are sorted, this can be used in conjunction to simulate
		/// <i>secondary sort on values</i>.</p>
		/// <p><i>Note</i>: This is not a guarantee of the combiner sort being
		/// <i>stable</i> in any sense. (In any case, with the order of available
		/// map-outputs to the combiner being non-deterministic, it wouldn't make
		/// that much sense.)</p>
		/// </summary>
		/// <param name="theClass">
		/// the comparator class to be used for grouping keys for the
		/// combiner. It should implement <code>RawComparator</code>.
		/// </param>
		/// <seealso cref="SetOutputKeyComparatorClass(System.Type{T})"/>
		public virtual void SetCombinerKeyGroupingComparator(Type theClass)
		{
			SetClass(JobContext.CombinerGroupComparatorClass, theClass, typeof(RawComparator)
				);
		}

		/// <summary>
		/// Set the user defined
		/// <see cref="Org.Apache.Hadoop.IO.RawComparator{T}"/>
		/// comparator for
		/// grouping keys in the input to the reduce.
		/// <p>This comparator should be provided if the equivalence rules for keys
		/// for sorting the intermediates are different from those for grouping keys
		/// before each call to
		/// <see cref="Reducer{K2, V2, K3, V3}.Reduce(object, System.Collections.IEnumerator{E}, OutputCollector{K, V}, Reporter)
		/// 	"/>
		/// .</p>
		/// <p>For key-value pairs (K1,V1) and (K2,V2), the values (V1, V2) are passed
		/// in a single call to the reduce function if K1 and K2 compare as equal.</p>
		/// <p>Since
		/// <see cref="SetOutputKeyComparatorClass(System.Type{T})"/>
		/// can be used to control
		/// how keys are sorted, this can be used in conjunction to simulate
		/// <i>secondary sort on values</i>.</p>
		/// <p><i>Note</i>: This is not a guarantee of the reduce sort being
		/// <i>stable</i> in any sense. (In any case, with the order of available
		/// map-outputs to the reduce being non-deterministic, it wouldn't make
		/// that much sense.)</p>
		/// </summary>
		/// <param name="theClass">
		/// the comparator class to be used for grouping keys.
		/// It should implement <code>RawComparator</code>.
		/// </param>
		/// <seealso cref="SetOutputKeyComparatorClass(System.Type{T})"/>
		/// <seealso cref="SetCombinerKeyGroupingComparator(System.Type{T})"/>
		public virtual void SetOutputValueGroupingComparator(Type theClass)
		{
			SetClass(JobContext.GroupComparatorClass, theClass, typeof(RawComparator));
		}

		/// <summary>
		/// Should the framework use the new context-object code for running
		/// the mapper?
		/// </summary>
		/// <returns>true, if the new api should be used</returns>
		public virtual bool GetUseNewMapper()
		{
			return GetBoolean("mapred.mapper.new-api", false);
		}

		/// <summary>Set whether the framework should use the new api for the mapper.</summary>
		/// <remarks>
		/// Set whether the framework should use the new api for the mapper.
		/// This is the default for jobs submitted with the new Job api.
		/// </remarks>
		/// <param name="flag">true, if the new api should be used</param>
		public virtual void SetUseNewMapper(bool flag)
		{
			SetBoolean("mapred.mapper.new-api", flag);
		}

		/// <summary>
		/// Should the framework use the new context-object code for running
		/// the reducer?
		/// </summary>
		/// <returns>true, if the new api should be used</returns>
		public virtual bool GetUseNewReducer()
		{
			return GetBoolean("mapred.reducer.new-api", false);
		}

		/// <summary>Set whether the framework should use the new api for the reducer.</summary>
		/// <remarks>
		/// Set whether the framework should use the new api for the reducer.
		/// This is the default for jobs submitted with the new Job api.
		/// </remarks>
		/// <param name="flag">true, if the new api should be used</param>
		public virtual void SetUseNewReducer(bool flag)
		{
			SetBoolean("mapred.reducer.new-api", flag);
		}

		/// <summary>Get the value class for job outputs.</summary>
		/// <returns>the value class for job outputs.</returns>
		public virtual Type GetOutputValueClass()
		{
			return GetClass<object>(JobContext.OutputValueClass, typeof(Text));
		}

		/// <summary>Set the value class for job outputs.</summary>
		/// <param name="theClass">the value class for job outputs.</param>
		public virtual void SetOutputValueClass(Type theClass)
		{
			SetClass(JobContext.OutputValueClass, theClass, typeof(object));
		}

		/// <summary>
		/// Get the
		/// <see cref="Mapper{K1, V1, K2, V2}"/>
		/// class for the job.
		/// </summary>
		/// <returns>
		/// the
		/// <see cref="Mapper{K1, V1, K2, V2}"/>
		/// class for the job.
		/// </returns>
		public virtual Type GetMapperClass()
		{
			return GetClass<Mapper>("mapred.mapper.class", typeof(IdentityMapper));
		}

		/// <summary>
		/// Set the
		/// <see cref="Mapper{K1, V1, K2, V2}"/>
		/// class for the job.
		/// </summary>
		/// <param name="theClass">
		/// the
		/// <see cref="Mapper{K1, V1, K2, V2}"/>
		/// class for the job.
		/// </param>
		public virtual void SetMapperClass(Type theClass)
		{
			SetClass("mapred.mapper.class", theClass, typeof(Mapper));
		}

		/// <summary>
		/// Get the
		/// <see cref="MapRunnable{K1, V1, K2, V2}"/>
		/// class for the job.
		/// </summary>
		/// <returns>
		/// the
		/// <see cref="MapRunnable{K1, V1, K2, V2}"/>
		/// class for the job.
		/// </returns>
		public virtual Type GetMapRunnerClass()
		{
			return GetClass<MapRunnable>("mapred.map.runner.class", typeof(MapRunner));
		}

		/// <summary>
		/// Expert: Set the
		/// <see cref="MapRunnable{K1, V1, K2, V2}"/>
		/// class for the job.
		/// Typically used to exert greater control on
		/// <see cref="Mapper{K1, V1, K2, V2}"/>
		/// s.
		/// </summary>
		/// <param name="theClass">
		/// the
		/// <see cref="MapRunnable{K1, V1, K2, V2}"/>
		/// class for the job.
		/// </param>
		public virtual void SetMapRunnerClass(Type theClass)
		{
			SetClass("mapred.map.runner.class", theClass, typeof(MapRunnable));
		}

		/// <summary>
		/// Get the
		/// <see cref="Partitioner{K2, V2}"/>
		/// used to partition
		/// <see cref="Mapper{K1, V1, K2, V2}"/>
		/// -outputs
		/// to be sent to the
		/// <see cref="Reducer{K2, V2, K3, V3}"/>
		/// s.
		/// </summary>
		/// <returns>
		/// the
		/// <see cref="Partitioner{K2, V2}"/>
		/// used to partition map-outputs.
		/// </returns>
		public virtual Type GetPartitionerClass()
		{
			return GetClass<Partitioner>("mapred.partitioner.class", typeof(HashPartitioner));
		}

		/// <summary>
		/// Set the
		/// <see cref="Partitioner{K2, V2}"/>
		/// class used to partition
		/// <see cref="Mapper{K1, V1, K2, V2}"/>
		/// -outputs to be sent to the
		/// <see cref="Reducer{K2, V2, K3, V3}"/>
		/// s.
		/// </summary>
		/// <param name="theClass">
		/// the
		/// <see cref="Partitioner{K2, V2}"/>
		/// used to partition map-outputs.
		/// </param>
		public virtual void SetPartitionerClass(Type theClass)
		{
			SetClass("mapred.partitioner.class", theClass, typeof(Partitioner));
		}

		/// <summary>
		/// Get the
		/// <see cref="Reducer{K2, V2, K3, V3}"/>
		/// class for the job.
		/// </summary>
		/// <returns>
		/// the
		/// <see cref="Reducer{K2, V2, K3, V3}"/>
		/// class for the job.
		/// </returns>
		public virtual Type GetReducerClass()
		{
			return GetClass<Reducer>("mapred.reducer.class", typeof(IdentityReducer));
		}

		/// <summary>
		/// Set the
		/// <see cref="Reducer{K2, V2, K3, V3}"/>
		/// class for the job.
		/// </summary>
		/// <param name="theClass">
		/// the
		/// <see cref="Reducer{K2, V2, K3, V3}"/>
		/// class for the job.
		/// </param>
		public virtual void SetReducerClass(Type theClass)
		{
			SetClass("mapred.reducer.class", theClass, typeof(Reducer));
		}

		/// <summary>
		/// Get the user-defined <i>combiner</i> class used to combine map-outputs
		/// before being sent to the reducers.
		/// </summary>
		/// <remarks>
		/// Get the user-defined <i>combiner</i> class used to combine map-outputs
		/// before being sent to the reducers. Typically the combiner is same as the
		/// the
		/// <see cref="Reducer{K2, V2, K3, V3}"/>
		/// for the job i.e.
		/// <see cref="GetReducerClass()"/>
		/// .
		/// </remarks>
		/// <returns>the user-defined combiner class used to combine map-outputs.</returns>
		public virtual Type GetCombinerClass()
		{
			return GetClass<Reducer>("mapred.combiner.class", null);
		}

		/// <summary>
		/// Set the user-defined <i>combiner</i> class used to combine map-outputs
		/// before being sent to the reducers.
		/// </summary>
		/// <remarks>
		/// Set the user-defined <i>combiner</i> class used to combine map-outputs
		/// before being sent to the reducers.
		/// <p>The combiner is an application-specified aggregation operation, which
		/// can help cut down the amount of data transferred between the
		/// <see cref="Mapper{K1, V1, K2, V2}"/>
		/// and the
		/// <see cref="Reducer{K2, V2, K3, V3}"/>
		/// , leading to better performance.</p>
		/// <p>The framework may invoke the combiner 0, 1, or multiple times, in both
		/// the mapper and reducer tasks. In general, the combiner is called as the
		/// sort/merge result is written to disk. The combiner must:
		/// <ul>
		/// <li> be side-effect free</li>
		/// <li> have the same input and output key types and the same input and
		/// output value types</li>
		/// </ul>
		/// <p>Typically the combiner is same as the <code>Reducer</code> for the
		/// job i.e.
		/// <see cref="SetReducerClass(System.Type{T})"/>
		/// .</p>
		/// </remarks>
		/// <param name="theClass">
		/// the user-defined combiner class used to combine
		/// map-outputs.
		/// </param>
		public virtual void SetCombinerClass(Type theClass)
		{
			SetClass("mapred.combiner.class", theClass, typeof(Reducer));
		}

		/// <summary>
		/// Should speculative execution be used for this job?
		/// Defaults to <code>true</code>.
		/// </summary>
		/// <returns>
		/// <code>true</code> if speculative execution be used for this job,
		/// <code>false</code> otherwise.
		/// </returns>
		public virtual bool GetSpeculativeExecution()
		{
			return (GetMapSpeculativeExecution() || GetReduceSpeculativeExecution());
		}

		/// <summary>Turn speculative execution on or off for this job.</summary>
		/// <param name="speculativeExecution">
		/// <code>true</code> if speculative execution
		/// should be turned on, else <code>false</code>.
		/// </param>
		public virtual void SetSpeculativeExecution(bool speculativeExecution)
		{
			SetMapSpeculativeExecution(speculativeExecution);
			SetReduceSpeculativeExecution(speculativeExecution);
		}

		/// <summary>
		/// Should speculative execution be used for this job for map tasks?
		/// Defaults to <code>true</code>.
		/// </summary>
		/// <returns>
		/// <code>true</code> if speculative execution be
		/// used for this job for map tasks,
		/// <code>false</code> otherwise.
		/// </returns>
		public virtual bool GetMapSpeculativeExecution()
		{
			return GetBoolean(JobContext.MapSpeculative, true);
		}

		/// <summary>Turn speculative execution on or off for this job for map tasks.</summary>
		/// <param name="speculativeExecution">
		/// <code>true</code> if speculative execution
		/// should be turned on for map tasks,
		/// else <code>false</code>.
		/// </param>
		public virtual void SetMapSpeculativeExecution(bool speculativeExecution)
		{
			SetBoolean(JobContext.MapSpeculative, speculativeExecution);
		}

		/// <summary>
		/// Should speculative execution be used for this job for reduce tasks?
		/// Defaults to <code>true</code>.
		/// </summary>
		/// <returns>
		/// <code>true</code> if speculative execution be used
		/// for reduce tasks for this job,
		/// <code>false</code> otherwise.
		/// </returns>
		public virtual bool GetReduceSpeculativeExecution()
		{
			return GetBoolean(JobContext.ReduceSpeculative, true);
		}

		/// <summary>Turn speculative execution on or off for this job for reduce tasks.</summary>
		/// <param name="speculativeExecution">
		/// <code>true</code> if speculative execution
		/// should be turned on for reduce tasks,
		/// else <code>false</code>.
		/// </param>
		public virtual void SetReduceSpeculativeExecution(bool speculativeExecution)
		{
			SetBoolean(JobContext.ReduceSpeculative, speculativeExecution);
		}

		/// <summary>Get configured the number of reduce tasks for this job.</summary>
		/// <remarks>
		/// Get configured the number of reduce tasks for this job.
		/// Defaults to <code>1</code>.
		/// </remarks>
		/// <returns>the number of reduce tasks for this job.</returns>
		public virtual int GetNumMapTasks()
		{
			return GetInt(JobContext.NumMaps, 1);
		}

		/// <summary>Set the number of map tasks for this job.</summary>
		/// <remarks>
		/// Set the number of map tasks for this job.
		/// <p><i>Note</i>: This is only a <i>hint</i> to the framework. The actual
		/// number of spawned map tasks depends on the number of
		/// <see cref="InputSplit"/>
		/// s
		/// generated by the job's
		/// <see cref="InputFormat{K, V}.GetSplits(JobConf, int)"/>
		/// .
		/// A custom
		/// <see cref="InputFormat{K, V}"/>
		/// is typically used to accurately control
		/// the number of map tasks for the job.</p>
		/// <b id="NoOfMaps">How many maps?</b>
		/// <p>The number of maps is usually driven by the total size of the inputs
		/// i.e. total number of blocks of the input files.</p>
		/// <p>The right level of parallelism for maps seems to be around 10-100 maps
		/// per-node, although it has been set up to 300 or so for very cpu-light map
		/// tasks. Task setup takes awhile, so it is best if the maps take at least a
		/// minute to execute.</p>
		/// <p>The default behavior of file-based
		/// <see cref="InputFormat{K, V}"/>
		/// s is to split the
		/// input into <i>logical</i>
		/// <see cref="InputSplit"/>
		/// s based on the total size, in
		/// bytes, of input files. However, the
		/// <see cref="Org.Apache.Hadoop.FS.FileSystem"/>
		/// blocksize of the
		/// input files is treated as an upper bound for input splits. A lower bound
		/// on the split size can be set via
		/// &lt;a href="
		/// <docRoot/>
		/// /../mapred-default.html#mapreduce.input.fileinputformat.split.minsize"&gt;
		/// mapreduce.input.fileinputformat.split.minsize</a>.</p>
		/// <p>Thus, if you expect 10TB of input data and have a blocksize of 128MB,
		/// you'll end up with 82,000 maps, unless
		/// <see cref="SetNumMapTasks(int)"/>
		/// is
		/// used to set it even higher.</p>
		/// </remarks>
		/// <param name="n">the number of map tasks for this job.</param>
		/// <seealso cref="InputFormat{K, V}.GetSplits(JobConf, int)"/>
		/// <seealso cref="FileInputFormat{K, V}"/>
		/// <seealso cref="Org.Apache.Hadoop.FS.FileSystem.GetDefaultBlockSize()"/>
		/// <seealso cref="Org.Apache.Hadoop.FS.FileStatus.GetBlockSize()"/>
		public virtual void SetNumMapTasks(int n)
		{
			SetInt(JobContext.NumMaps, n);
		}

		/// <summary>Get configured the number of reduce tasks for this job.</summary>
		/// <remarks>
		/// Get configured the number of reduce tasks for this job. Defaults to
		/// <code>1</code>.
		/// </remarks>
		/// <returns>the number of reduce tasks for this job.</returns>
		public virtual int GetNumReduceTasks()
		{
			return GetInt(JobContext.NumReduces, 1);
		}

		/// <summary>Set the requisite number of reduce tasks for this job.</summary>
		/// <remarks>
		/// Set the requisite number of reduce tasks for this job.
		/// <b id="NoOfReduces">How many reduces?</b>
		/// <p>The right number of reduces seems to be <code>0.95</code> or
		/// <code>1.75</code> multiplied by (&lt;<i>no. of nodes</i>&gt; *
		/// &lt;a href="
		/// <docRoot/>
		/// /../mapred-default.html#mapreduce.tasktracker.reduce.tasks.maximum"&gt;
		/// mapreduce.tasktracker.reduce.tasks.maximum</a>).
		/// </p>
		/// <p>With <code>0.95</code> all of the reduces can launch immediately and
		/// start transfering map outputs as the maps finish. With <code>1.75</code>
		/// the faster nodes will finish their first round of reduces and launch a
		/// second wave of reduces doing a much better job of load balancing.</p>
		/// <p>Increasing the number of reduces increases the framework overhead, but
		/// increases load balancing and lowers the cost of failures.</p>
		/// <p>The scaling factors above are slightly less than whole numbers to
		/// reserve a few reduce slots in the framework for speculative-tasks, failures
		/// etc.</p>
		/// <b id="ReducerNone">Reducer NONE</b>
		/// <p>It is legal to set the number of reduce-tasks to <code>zero</code>.</p>
		/// <p>In this case the output of the map-tasks directly go to distributed
		/// file-system, to the path set by
		/// <see cref="FileOutputFormat{K, V}.SetOutputPath(JobConf, Org.Apache.Hadoop.FS.Path)
		/// 	"/>
		/// . Also, the
		/// framework doesn't sort the map-outputs before writing it out to HDFS.</p>
		/// </remarks>
		/// <param name="n">the number of reduce tasks for this job.</param>
		public virtual void SetNumReduceTasks(int n)
		{
			SetInt(JobContext.NumReduces, n);
		}

		/// <summary>
		/// Get the configured number of maximum attempts that will be made to run a
		/// map task, as specified by the <code>mapreduce.map.maxattempts</code>
		/// property.
		/// </summary>
		/// <remarks>
		/// Get the configured number of maximum attempts that will be made to run a
		/// map task, as specified by the <code>mapreduce.map.maxattempts</code>
		/// property. If this property is not already set, the default is 4 attempts.
		/// </remarks>
		/// <returns>the max number of attempts per map task.</returns>
		public virtual int GetMaxMapAttempts()
		{
			return GetInt(JobContext.MapMaxAttempts, 4);
		}

		/// <summary>
		/// Expert: Set the number of maximum attempts that will be made to run a
		/// map task.
		/// </summary>
		/// <param name="n">the number of attempts per map task.</param>
		public virtual void SetMaxMapAttempts(int n)
		{
			SetInt(JobContext.MapMaxAttempts, n);
		}

		/// <summary>
		/// Get the configured number of maximum attempts  that will be made to run a
		/// reduce task, as specified by the <code>mapreduce.reduce.maxattempts</code>
		/// property.
		/// </summary>
		/// <remarks>
		/// Get the configured number of maximum attempts  that will be made to run a
		/// reduce task, as specified by the <code>mapreduce.reduce.maxattempts</code>
		/// property. If this property is not already set, the default is 4 attempts.
		/// </remarks>
		/// <returns>the max number of attempts per reduce task.</returns>
		public virtual int GetMaxReduceAttempts()
		{
			return GetInt(JobContext.ReduceMaxAttempts, 4);
		}

		/// <summary>
		/// Expert: Set the number of maximum attempts that will be made to run a
		/// reduce task.
		/// </summary>
		/// <param name="n">the number of attempts per reduce task.</param>
		public virtual void SetMaxReduceAttempts(int n)
		{
			SetInt(JobContext.ReduceMaxAttempts, n);
		}

		/// <summary>Get the user-specified job name.</summary>
		/// <remarks>
		/// Get the user-specified job name. This is only used to identify the
		/// job to the user.
		/// </remarks>
		/// <returns>the job's name, defaulting to "".</returns>
		public virtual string GetJobName()
		{
			return Get(JobContext.JobName, string.Empty);
		}

		/// <summary>Set the user-specified job name.</summary>
		/// <param name="name">the job's new name.</param>
		public virtual void SetJobName(string name)
		{
			Set(JobContext.JobName, name);
		}

		/// <summary>Get the user-specified session identifier.</summary>
		/// <remarks>
		/// Get the user-specified session identifier. The default is the empty string.
		/// The session identifier is used to tag metric data that is reported to some
		/// performance metrics system via the org.apache.hadoop.metrics API.  The
		/// session identifier is intended, in particular, for use by Hadoop-On-Demand
		/// (HOD) which allocates a virtual Hadoop cluster dynamically and transiently.
		/// HOD will set the session identifier by modifying the mapred-site.xml file
		/// before starting the cluster.
		/// When not running under HOD, this identifer is expected to remain set to
		/// the empty string.
		/// </remarks>
		/// <returns>the session identifier, defaulting to "".</returns>
		[Obsolete]
		public virtual string GetSessionId()
		{
			return Get("session.id", string.Empty);
		}

		/// <summary>Set the user-specified session identifier.</summary>
		/// <param name="sessionId">the new session id.</param>
		[Obsolete]
		public virtual void SetSessionId(string sessionId)
		{
			Set("session.id", sessionId);
		}

		/// <summary>Set the maximum no.</summary>
		/// <remarks>
		/// Set the maximum no. of failures of a given job per tasktracker.
		/// If the no. of task failures exceeds <code>noFailures</code>, the
		/// tasktracker is <i>blacklisted</i> for this job.
		/// </remarks>
		/// <param name="noFailures">maximum no. of failures of a given job per tasktracker.</param>
		public virtual void SetMaxTaskFailuresPerTracker(int noFailures)
		{
			SetInt(JobContext.MaxTaskFailuresPerTracker, noFailures);
		}

		/// <summary>Expert: Get the maximum no.</summary>
		/// <remarks>
		/// Expert: Get the maximum no. of failures of a given job per tasktracker.
		/// If the no. of task failures exceeds this, the tasktracker is
		/// <i>blacklisted</i> for this job.
		/// </remarks>
		/// <returns>the maximum no. of failures of a given job per tasktracker.</returns>
		public virtual int GetMaxTaskFailuresPerTracker()
		{
			return GetInt(JobContext.MaxTaskFailuresPerTracker, 3);
		}

		/// <summary>
		/// Get the maximum percentage of map tasks that can fail without
		/// the job being aborted.
		/// </summary>
		/// <remarks>
		/// Get the maximum percentage of map tasks that can fail without
		/// the job being aborted.
		/// Each map task is executed a minimum of
		/// <see cref="GetMaxMapAttempts()"/>
		/// 
		/// attempts before being declared as <i>failed</i>.
		/// Defaults to <code>zero</code>, i.e. <i>any</i> failed map-task results in
		/// the job being declared as
		/// <see cref="JobStatus.Failed"/>
		/// .
		/// </remarks>
		/// <returns>
		/// the maximum percentage of map tasks that can fail without
		/// the job being aborted.
		/// </returns>
		public virtual int GetMaxMapTaskFailuresPercent()
		{
			return GetInt(JobContext.MapFailuresMaxPercent, 0);
		}

		/// <summary>
		/// Expert: Set the maximum percentage of map tasks that can fail without the
		/// job being aborted.
		/// </summary>
		/// <remarks>
		/// Expert: Set the maximum percentage of map tasks that can fail without the
		/// job being aborted.
		/// Each map task is executed a minimum of
		/// <see cref="GetMaxMapAttempts()"/>
		/// attempts
		/// before being declared as <i>failed</i>.
		/// </remarks>
		/// <param name="percent">
		/// the maximum percentage of map tasks that can fail without
		/// the job being aborted.
		/// </param>
		public virtual void SetMaxMapTaskFailuresPercent(int percent)
		{
			SetInt(JobContext.MapFailuresMaxPercent, percent);
		}

		/// <summary>
		/// Get the maximum percentage of reduce tasks that can fail without
		/// the job being aborted.
		/// </summary>
		/// <remarks>
		/// Get the maximum percentage of reduce tasks that can fail without
		/// the job being aborted.
		/// Each reduce task is executed a minimum of
		/// <see cref="GetMaxReduceAttempts()"/>
		/// 
		/// attempts before being declared as <i>failed</i>.
		/// Defaults to <code>zero</code>, i.e. <i>any</i> failed reduce-task results
		/// in the job being declared as
		/// <see cref="JobStatus.Failed"/>
		/// .
		/// </remarks>
		/// <returns>
		/// the maximum percentage of reduce tasks that can fail without
		/// the job being aborted.
		/// </returns>
		public virtual int GetMaxReduceTaskFailuresPercent()
		{
			return GetInt(JobContext.ReduceFailuresMaxpercent, 0);
		}

		/// <summary>
		/// Set the maximum percentage of reduce tasks that can fail without the job
		/// being aborted.
		/// </summary>
		/// <remarks>
		/// Set the maximum percentage of reduce tasks that can fail without the job
		/// being aborted.
		/// Each reduce task is executed a minimum of
		/// <see cref="GetMaxReduceAttempts()"/>
		/// 
		/// attempts before being declared as <i>failed</i>.
		/// </remarks>
		/// <param name="percent">
		/// the maximum percentage of reduce tasks that can fail without
		/// the job being aborted.
		/// </param>
		public virtual void SetMaxReduceTaskFailuresPercent(int percent)
		{
			SetInt(JobContext.ReduceFailuresMaxpercent, percent);
		}

		/// <summary>
		/// Set
		/// <see cref="JobPriority"/>
		/// for this job.
		/// </summary>
		/// <param name="prio">
		/// the
		/// <see cref="JobPriority"/>
		/// for this job.
		/// </param>
		public virtual void SetJobPriority(JobPriority prio)
		{
			Set(JobContext.Priority, prio.ToString());
		}

		/// <summary>
		/// Get the
		/// <see cref="JobPriority"/>
		/// for this job.
		/// </summary>
		/// <returns>
		/// the
		/// <see cref="JobPriority"/>
		/// for this job.
		/// </returns>
		public virtual JobPriority GetJobPriority()
		{
			string prio = Get(JobContext.Priority);
			if (prio == null)
			{
				return JobPriority.Normal;
			}
			return JobPriority.ValueOf(prio);
		}

		/// <summary>Set JobSubmitHostName for this job.</summary>
		/// <param name="hostname">the JobSubmitHostName for this job.</param>
		internal virtual void SetJobSubmitHostName(string hostname)
		{
			Set(MRJobConfig.JobSubmithost, hostname);
		}

		/// <summary>Get the  JobSubmitHostName for this job.</summary>
		/// <returns>the JobSubmitHostName for this job.</returns>
		internal virtual string GetJobSubmitHostName()
		{
			string hostname = Get(MRJobConfig.JobSubmithost);
			return hostname;
		}

		/// <summary>Set JobSubmitHostAddress for this job.</summary>
		/// <param name="hostadd">the JobSubmitHostAddress for this job.</param>
		internal virtual void SetJobSubmitHostAddress(string hostadd)
		{
			Set(MRJobConfig.JobSubmithostaddr, hostadd);
		}

		/// <summary>Get JobSubmitHostAddress for this job.</summary>
		/// <returns>JobSubmitHostAddress for this job.</returns>
		internal virtual string GetJobSubmitHostAddress()
		{
			string hostadd = Get(MRJobConfig.JobSubmithostaddr);
			return hostadd;
		}

		/// <summary>Get whether the task profiling is enabled.</summary>
		/// <returns>true if some tasks will be profiled</returns>
		public virtual bool GetProfileEnabled()
		{
			return GetBoolean(JobContext.TaskProfile, false);
		}

		/// <summary>
		/// Set whether the system should collect profiler information for some of
		/// the tasks in this job? The information is stored in the user log
		/// directory.
		/// </summary>
		/// <param name="newValue">true means it should be gathered</param>
		public virtual void SetProfileEnabled(bool newValue)
		{
			SetBoolean(JobContext.TaskProfile, newValue);
		}

		/// <summary>Get the profiler configuration arguments.</summary>
		/// <remarks>
		/// Get the profiler configuration arguments.
		/// The default value for this property is
		/// "-agentlib:hprof=cpu=samples,heap=sites,force=n,thread=y,verbose=n,file=%s"
		/// </remarks>
		/// <returns>the parameters to pass to the task child to configure profiling</returns>
		public virtual string GetProfileParams()
		{
			return Get(JobContext.TaskProfileParams, MRJobConfig.DefaultTaskProfileParams);
		}

		/// <summary>Set the profiler configuration arguments.</summary>
		/// <remarks>
		/// Set the profiler configuration arguments. If the string contains a '%s' it
		/// will be replaced with the name of the profiling output file when the task
		/// runs.
		/// This value is passed to the task child JVM on the command line.
		/// </remarks>
		/// <param name="value">the configuration string</param>
		public virtual void SetProfileParams(string value)
		{
			Set(JobContext.TaskProfileParams, value);
		}

		/// <summary>Get the range of maps or reduces to profile.</summary>
		/// <param name="isMap">is the task a map?</param>
		/// <returns>the task ranges</returns>
		public virtual Configuration.IntegerRanges GetProfileTaskRange(bool isMap)
		{
			return GetRange((isMap ? JobContext.NumMapProfiles : JobContext.NumReduceProfiles
				), "0-2");
		}

		/// <summary>Set the ranges of maps or reduces to profile.</summary>
		/// <remarks>
		/// Set the ranges of maps or reduces to profile. setProfileEnabled(true)
		/// must also be called.
		/// </remarks>
		/// <param name="newValue">a set of integer ranges of the map ids</param>
		public virtual void SetProfileTaskRange(bool isMap, string newValue)
		{
			// parse the value to make sure it is legal
			new Configuration.IntegerRanges(newValue);
			Set((isMap ? JobContext.NumMapProfiles : JobContext.NumReduceProfiles), newValue);
		}

		/// <summary>Set the debug script to run when the map tasks fail.</summary>
		/// <remarks>
		/// Set the debug script to run when the map tasks fail.
		/// <p>The debug script can aid debugging of failed map tasks. The script is
		/// given task's stdout, stderr, syslog, jobconf files as arguments.</p>
		/// <p>The debug command, run on the node where the map failed, is:</p>
		/// <p><blockquote><pre>
		/// $script $stdout $stderr $syslog $jobconf.
		/// </pre></blockquote>
		/// <p> The script file is distributed through
		/// <see cref="Org.Apache.Hadoop.Mapreduce.Filecache.DistributedCache"/>
		/// 
		/// APIs. The script needs to be symlinked. </p>
		/// <p>Here is an example on how to submit a script
		/// <p><blockquote><pre>
		/// job.setMapDebugScript("./myscript");
		/// DistributedCache.createSymlink(job);
		/// DistributedCache.addCacheFile("/debug/scripts/myscript#myscript");
		/// </pre></blockquote>
		/// </remarks>
		/// <param name="mDbgScript">the script name</param>
		public virtual void SetMapDebugScript(string mDbgScript)
		{
			Set(JobContext.MapDebugScript, mDbgScript);
		}

		/// <summary>Get the map task's debug script.</summary>
		/// <returns>the debug Script for the mapred job for failed map tasks.</returns>
		/// <seealso cref="SetMapDebugScript(string)"/>
		public virtual string GetMapDebugScript()
		{
			return Get(JobContext.MapDebugScript);
		}

		/// <summary>Set the debug script to run when the reduce tasks fail.</summary>
		/// <remarks>
		/// Set the debug script to run when the reduce tasks fail.
		/// <p>The debug script can aid debugging of failed reduce tasks. The script
		/// is given task's stdout, stderr, syslog, jobconf files as arguments.</p>
		/// <p>The debug command, run on the node where the map failed, is:</p>
		/// <p><blockquote><pre>
		/// $script $stdout $stderr $syslog $jobconf.
		/// </pre></blockquote>
		/// <p> The script file is distributed through
		/// <see cref="Org.Apache.Hadoop.Mapreduce.Filecache.DistributedCache"/>
		/// 
		/// APIs. The script file needs to be symlinked </p>
		/// <p>Here is an example on how to submit a script
		/// <p><blockquote><pre>
		/// job.setReduceDebugScript("./myscript");
		/// DistributedCache.createSymlink(job);
		/// DistributedCache.addCacheFile("/debug/scripts/myscript#myscript");
		/// </pre></blockquote>
		/// </remarks>
		/// <param name="rDbgScript">the script name</param>
		public virtual void SetReduceDebugScript(string rDbgScript)
		{
			Set(JobContext.ReduceDebugScript, rDbgScript);
		}

		/// <summary>Get the reduce task's debug Script</summary>
		/// <returns>the debug script for the mapred job for failed reduce tasks.</returns>
		/// <seealso cref="SetReduceDebugScript(string)"/>
		public virtual string GetReduceDebugScript()
		{
			return Get(JobContext.ReduceDebugScript);
		}

		/// <summary>
		/// Get the uri to be invoked in-order to send a notification after the job
		/// has completed (success/failure).
		/// </summary>
		/// <returns>
		/// the job end notification uri, <code>null</code> if it hasn't
		/// been set.
		/// </returns>
		/// <seealso cref="SetJobEndNotificationURI(string)"/>
		public virtual string GetJobEndNotificationURI()
		{
			return Get(JobContext.MrJobEndNotificationUrl);
		}

		/// <summary>
		/// Set the uri to be invoked in-order to send a notification after the job
		/// has completed (success/failure).
		/// </summary>
		/// <remarks>
		/// Set the uri to be invoked in-order to send a notification after the job
		/// has completed (success/failure).
		/// <p>The uri can contain 2 special parameters: <tt>$jobId</tt> and
		/// <tt>$jobStatus</tt>. Those, if present, are replaced by the job's
		/// identifier and completion-status respectively.</p>
		/// <p>This is typically used by application-writers to implement chaining of
		/// Map-Reduce jobs in an <i>asynchronous manner</i>.</p>
		/// </remarks>
		/// <param name="uri">the job end notification uri</param>
		/// <seealso cref="JobStatus"/>
		public virtual void SetJobEndNotificationURI(string uri)
		{
			Set(JobContext.MrJobEndNotificationUrl, uri);
		}

		/// <summary>
		/// Get job-specific shared directory for use as scratch space
		/// <p>
		/// When a job starts, a shared directory is created at location
		/// <code>
		/// ${mapreduce.cluster.local.dir}/taskTracker/$user/jobcache/$jobid/work/ </code>.
		/// </summary>
		/// <remarks>
		/// Get job-specific shared directory for use as scratch space
		/// <p>
		/// When a job starts, a shared directory is created at location
		/// <code>
		/// ${mapreduce.cluster.local.dir}/taskTracker/$user/jobcache/$jobid/work/ </code>.
		/// This directory is exposed to the users through
		/// <code>mapreduce.job.local.dir </code>.
		/// So, the tasks can use this space
		/// as scratch space and share files among them. </p>
		/// This value is available as System property also.
		/// </remarks>
		/// <returns>The localized job specific shared directory</returns>
		public virtual string GetJobLocalDir()
		{
			return Get(JobContext.JobLocalDir);
		}

		/// <summary>Get memory required to run a map task of the job, in MB.</summary>
		/// <remarks>
		/// Get memory required to run a map task of the job, in MB.
		/// If a value is specified in the configuration, it is returned.
		/// Else, it returns
		/// <see cref="Org.Apache.Hadoop.Mapreduce.MRJobConfig.DefaultMapMemoryMb"/>
		/// .
		/// <p>
		/// For backward compatibility, if the job configuration sets the
		/// key
		/// <see cref="MapredTaskMaxvmemProperty"/>
		/// to a value different
		/// from
		/// <see cref="DisabledMemoryLimit"/>
		/// , that value will be used
		/// after converting it from bytes to MB.
		/// </remarks>
		/// <returns>memory required to run a map task of the job, in MB,</returns>
		public virtual long GetMemoryForMapTask()
		{
			long value = GetDeprecatedMemoryValue();
			if (value < 0)
			{
				return GetLong(Org.Apache.Hadoop.Mapred.JobConf.MapredJobMapMemoryMbProperty, JobContext
					.DefaultMapMemoryMb);
			}
			return value;
		}

		public virtual void SetMemoryForMapTask(long mem)
		{
			SetLong(Org.Apache.Hadoop.Mapred.JobConf.MapreduceJobMapMemoryMbProperty, mem);
			// In case that M/R 1.x applications use the old property name
			SetLong(Org.Apache.Hadoop.Mapred.JobConf.MapredJobMapMemoryMbProperty, mem);
		}

		/// <summary>Get memory required to run a reduce task of the job, in MB.</summary>
		/// <remarks>
		/// Get memory required to run a reduce task of the job, in MB.
		/// If a value is specified in the configuration, it is returned.
		/// Else, it returns
		/// <see cref="Org.Apache.Hadoop.Mapreduce.MRJobConfig.DefaultReduceMemoryMb"/>
		/// .
		/// <p>
		/// For backward compatibility, if the job configuration sets the
		/// key
		/// <see cref="MapredTaskMaxvmemProperty"/>
		/// to a value different
		/// from
		/// <see cref="DisabledMemoryLimit"/>
		/// , that value will be used
		/// after converting it from bytes to MB.
		/// </remarks>
		/// <returns>memory required to run a reduce task of the job, in MB.</returns>
		public virtual long GetMemoryForReduceTask()
		{
			long value = GetDeprecatedMemoryValue();
			if (value < 0)
			{
				return GetLong(Org.Apache.Hadoop.Mapred.JobConf.MapredJobReduceMemoryMbProperty, 
					JobContext.DefaultReduceMemoryMb);
			}
			return value;
		}

		// Return the value set to the key MAPRED_TASK_MAXVMEM_PROPERTY,
		// converted into MBs.
		// Returns DISABLED_MEMORY_LIMIT if unset, or set to a negative
		// value.
		private long GetDeprecatedMemoryValue()
		{
			long oldValue = GetLong(MapredTaskMaxvmemProperty, DisabledMemoryLimit);
			if (oldValue > 0)
			{
				oldValue /= (1024 * 1024);
			}
			return oldValue;
		}

		public virtual void SetMemoryForReduceTask(long mem)
		{
			SetLong(Org.Apache.Hadoop.Mapred.JobConf.MapreduceJobReduceMemoryMbProperty, mem);
			// In case that M/R 1.x applications use the old property name
			SetLong(Org.Apache.Hadoop.Mapred.JobConf.MapredJobReduceMemoryMbProperty, mem);
		}

		/// <summary>Return the name of the queue to which this job is submitted.</summary>
		/// <remarks>
		/// Return the name of the queue to which this job is submitted.
		/// Defaults to 'default'.
		/// </remarks>
		/// <returns>name of the queue</returns>
		public virtual string GetQueueName()
		{
			return Get(JobContext.QueueName, DefaultQueueName);
		}

		/// <summary>Set the name of the queue to which this job should be submitted.</summary>
		/// <param name="queueName">Name of the queue</param>
		public virtual void SetQueueName(string queueName)
		{
			Set(JobContext.QueueName, queueName);
		}

		/// <summary>Normalize the negative values in configuration</summary>
		/// <param name="val"/>
		/// <returns>normalized value</returns>
		public static long NormalizeMemoryConfigValue(long val)
		{
			if (val < 0)
			{
				val = DisabledMemoryLimit;
			}
			return val;
		}

		/// <summary>Find a jar that contains a class of the same name, if any.</summary>
		/// <remarks>
		/// Find a jar that contains a class of the same name, if any.
		/// It will return a jar file, even if that is not the first thing
		/// on the class path that has a class with the same name.
		/// </remarks>
		/// <param name="my_class">the class to find.</param>
		/// <returns>a jar file that contains the class, or null.</returns>
		public static string FindContainingJar(Type my_class)
		{
			return ClassUtil.FindContainingJar(my_class);
		}

		/// <summary>Get the memory required to run a task of this job, in bytes.</summary>
		/// <remarks>
		/// Get the memory required to run a task of this job, in bytes. See
		/// <see cref="MapredTaskMaxvmemProperty"/>
		/// <p>
		/// This method is deprecated. Now, different memory limits can be
		/// set for map and reduce tasks of a job, in MB.
		/// <p>
		/// For backward compatibility, if the job configuration sets the
		/// key
		/// <see cref="MapredTaskMaxvmemProperty"/>
		/// , that value is returned.
		/// Otherwise, this method will return the larger of the values returned by
		/// <see cref="GetMemoryForMapTask()"/>
		/// and
		/// <see cref="GetMemoryForReduceTask()"/>
		/// after converting them into bytes.
		/// </remarks>
		/// <returns>Memory required to run a task of this job, in bytes.</returns>
		/// <seealso cref="SetMaxVirtualMemoryForTask(long)"/>
		[System.ObsoleteAttribute(@"Use GetMemoryForMapTask() andGetMemoryForReduceTask()"
			)]
		public virtual long GetMaxVirtualMemoryForTask()
		{
			Log.Warn("getMaxVirtualMemoryForTask() is deprecated. " + "Instead use getMemoryForMapTask() and getMemoryForReduceTask()"
				);
			long value = GetLong(MapredTaskMaxvmemProperty, Math.Max(GetMemoryForMapTask(), GetMemoryForReduceTask
				()) * 1024 * 1024);
			return value;
		}

		/// <summary>Set the maximum amount of memory any task of this job can use.</summary>
		/// <remarks>
		/// Set the maximum amount of memory any task of this job can use. See
		/// <see cref="MapredTaskMaxvmemProperty"/>
		/// <p>
		/// mapred.task.maxvmem is split into
		/// mapreduce.map.memory.mb
		/// and mapreduce.map.memory.mb,mapred
		/// each of the new key are set
		/// as mapred.task.maxvmem / 1024
		/// as new values are in MB
		/// </remarks>
		/// <param name="vmem">
		/// Maximum amount of virtual memory in bytes any task of this job
		/// can use.
		/// </param>
		/// <seealso cref="GetMaxVirtualMemoryForTask()"/>
		[System.ObsoleteAttribute(@"Use SetMemoryForMapTask(long)  and Use SetMemoryForReduceTask(long)"
			)]
		public virtual void SetMaxVirtualMemoryForTask(long vmem)
		{
			Log.Warn("setMaxVirtualMemoryForTask() is deprecated." + "Instead use setMemoryForMapTask() and setMemoryForReduceTask()"
				);
			if (vmem < 0)
			{
				throw new ArgumentException("Task memory allocation may not be < 0");
			}
			if (Get(Org.Apache.Hadoop.Mapred.JobConf.MapredTaskMaxvmemProperty) == null)
			{
				SetMemoryForMapTask(vmem / (1024 * 1024));
				//Changing bytes to mb
				SetMemoryForReduceTask(vmem / (1024 * 1024));
			}
			else
			{
				//Changing bytes to mb
				this.SetLong(Org.Apache.Hadoop.Mapred.JobConf.MapredTaskMaxvmemProperty, vmem);
			}
		}

		[System.ObsoleteAttribute(@"this variable is deprecated and nolonger in use.")]
		public virtual long GetMaxPhysicalMemoryForTask()
		{
			Log.Warn("The API getMaxPhysicalMemoryForTask() is deprecated." + " Refer to the APIs getMemoryForMapTask() and"
				 + " getMemoryForReduceTask() for details.");
			return -1;
		}

		/*
		* @deprecated this
		*/
		[Obsolete]
		public virtual void SetMaxPhysicalMemoryForTask(long mem)
		{
			Log.Warn("The API setMaxPhysicalMemoryForTask() is deprecated." + " The value set is ignored. Refer to "
				 + " setMemoryForMapTask() and setMemoryForReduceTask() for details.");
		}

		internal static string DeprecatedString(string key)
		{
			return "The variable " + key + " is no longer used.";
		}

		private void CheckAndWarnDeprecation()
		{
			if (Get(Org.Apache.Hadoop.Mapred.JobConf.MapredTaskMaxvmemProperty) != null)
			{
				Log.Warn(Org.Apache.Hadoop.Mapred.JobConf.DeprecatedString(Org.Apache.Hadoop.Mapred.JobConf
					.MapredTaskMaxvmemProperty) + " Instead use " + Org.Apache.Hadoop.Mapred.JobConf
					.MapreduceJobMapMemoryMbProperty + " and " + Org.Apache.Hadoop.Mapred.JobConf.MapreduceJobReduceMemoryMbProperty
					);
			}
			if (Get(Org.Apache.Hadoop.Mapred.JobConf.MapredTaskUlimit) != null)
			{
				Log.Warn(Org.Apache.Hadoop.Mapred.JobConf.DeprecatedString(Org.Apache.Hadoop.Mapred.JobConf
					.MapredTaskUlimit));
			}
			if (Get(Org.Apache.Hadoop.Mapred.JobConf.MapredMapTaskUlimit) != null)
			{
				Log.Warn(Org.Apache.Hadoop.Mapred.JobConf.DeprecatedString(Org.Apache.Hadoop.Mapred.JobConf
					.MapredMapTaskUlimit));
			}
			if (Get(Org.Apache.Hadoop.Mapred.JobConf.MapredReduceTaskUlimit) != null)
			{
				Log.Warn(Org.Apache.Hadoop.Mapred.JobConf.DeprecatedString(Org.Apache.Hadoop.Mapred.JobConf
					.MapredReduceTaskUlimit));
			}
		}
	}
}
