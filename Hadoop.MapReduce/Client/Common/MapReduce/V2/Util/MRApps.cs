using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Com.Google.Common.Annotations;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Filecache;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn.Api;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.Util
{
	/// <summary>Helper class for MR applications</summary>
	public class MRApps : Apps
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(MRApps));

		public static string ToString(JobId jid)
		{
			return jid.ToString();
		}

		public static JobId ToJobID(string jid)
		{
			return TypeConverter.ToYarn(JobID.ForName(jid));
		}

		public static string ToString(TaskId tid)
		{
			return tid.ToString();
		}

		public static TaskId ToTaskID(string tid)
		{
			return TypeConverter.ToYarn(TaskID.ForName(tid));
		}

		public static string ToString(TaskAttemptId taid)
		{
			return taid.ToString();
		}

		public static TaskAttemptId ToTaskAttemptID(string taid)
		{
			return TypeConverter.ToYarn(TaskAttemptID.ForName(taid));
		}

		public static string TaskSymbol(Org.Apache.Hadoop.Mapreduce.V2.Api.Records.TaskType
			 type)
		{
			switch (type)
			{
				case Org.Apache.Hadoop.Mapreduce.V2.Api.Records.TaskType.Map:
				{
					return "m";
				}

				case Org.Apache.Hadoop.Mapreduce.V2.Api.Records.TaskType.Reduce:
				{
					return "r";
				}
			}
			throw new YarnRuntimeException("Unknown task type: " + type.ToString());
		}

		[System.Serializable]
		public sealed class TaskAttemptStateUI
		{
			public static readonly MRApps.TaskAttemptStateUI New = new MRApps.TaskAttemptStateUI
				(new TaskAttemptState[] { TaskAttemptState.New, TaskAttemptState.Starting });

			public static readonly MRApps.TaskAttemptStateUI Running = new MRApps.TaskAttemptStateUI
				(new TaskAttemptState[] { TaskAttemptState.Running, TaskAttemptState.CommitPending
				 });

			public static readonly MRApps.TaskAttemptStateUI Successful = new MRApps.TaskAttemptStateUI
				(new TaskAttemptState[] { TaskAttemptState.Succeeded });

			public static readonly MRApps.TaskAttemptStateUI Failed = new MRApps.TaskAttemptStateUI
				(new TaskAttemptState[] { TaskAttemptState.Failed });

			public static readonly MRApps.TaskAttemptStateUI Killed = new MRApps.TaskAttemptStateUI
				(new TaskAttemptState[] { TaskAttemptState.Killed });

			private readonly IList<TaskAttemptState> correspondingStates;

			private TaskAttemptStateUI(TaskAttemptState[] correspondingStates)
			{
				this.correspondingStates = Arrays.AsList(correspondingStates);
			}

			public bool CorrespondsTo(TaskAttemptState state)
			{
				return this.correspondingStates.Contains(state);
			}
		}

		[System.Serializable]
		public sealed class TaskStateUI
		{
			public static readonly MRApps.TaskStateUI Running = new MRApps.TaskStateUI(new TaskState
				[] { TaskState.Running });

			public static readonly MRApps.TaskStateUI Pending = new MRApps.TaskStateUI(new TaskState
				[] { TaskState.Scheduled });

			public static readonly MRApps.TaskStateUI Completed = new MRApps.TaskStateUI(new 
				TaskState[] { TaskState.Succeeded, TaskState.Failed, TaskState.Killed });

			private readonly IList<TaskState> correspondingStates;

			private TaskStateUI(TaskState[] correspondingStates)
			{
				this.correspondingStates = Arrays.AsList(correspondingStates);
			}

			public bool CorrespondsTo(TaskState state)
			{
				return this.correspondingStates.Contains(state);
			}
		}

		public static TaskType TaskType(string symbol)
		{
			// JDK 7 supports switch on strings
			if (symbol.Equals("m"))
			{
				return TaskType.Map;
			}
			if (symbol.Equals("r"))
			{
				return TaskType.Reduce;
			}
			throw new YarnRuntimeException("Unknown task symbol: " + symbol);
		}

		public static MRApps.TaskAttemptStateUI TaskAttemptState(string attemptStateStr)
		{
			return MRApps.TaskAttemptStateUI.ValueOf(attemptStateStr);
		}

		public static MRApps.TaskStateUI TaskState(string taskStateStr)
		{
			return MRApps.TaskStateUI.ValueOf(taskStateStr);
		}

		// gets the base name of the MapReduce framework or null if no
		// framework was configured
		private static string GetMRFrameworkName(Configuration conf)
		{
			string frameworkName = null;
			string framework = conf.Get(MRJobConfig.MapreduceApplicationFrameworkPath, string.Empty
				);
			if (!framework.IsEmpty())
			{
				URI uri;
				try
				{
					uri = new URI(framework);
				}
				catch (URISyntaxException e)
				{
					throw new ArgumentException("Unable to parse '" + framework + "' as a URI, check the setting for "
						 + MRJobConfig.MapreduceApplicationFrameworkPath, e);
				}
				frameworkName = uri.GetFragment();
				if (frameworkName == null)
				{
					frameworkName = new Path(uri).GetName();
				}
			}
			return frameworkName;
		}

		/// <exception cref="System.IO.IOException"/>
		private static void SetMRFrameworkClasspath(IDictionary<string, string> environment
			, Configuration conf)
		{
			// Propagate the system classpath when using the mini cluster
			if (conf.GetBoolean(YarnConfiguration.IsMiniYarnCluster, false))
			{
				MRApps.AddToEnvironment(environment, ApplicationConstants.Environment.Classpath.ToString
					(), Runtime.GetProperty("java.class.path"), conf);
			}
			bool crossPlatform = conf.GetBoolean(MRConfig.MapreduceAppSubmissionCrossPlatform
				, MRConfig.DefaultMapreduceAppSubmissionCrossPlatform);
			// if the framework is specified then only use the MR classpath
			string frameworkName = GetMRFrameworkName(conf);
			if (frameworkName == null)
			{
				// Add standard Hadoop classes
				foreach (string c in conf.GetStrings(YarnConfiguration.YarnApplicationClasspath, 
					crossPlatform ? YarnConfiguration.DefaultYarnCrossPlatformApplicationClasspath : 
					YarnConfiguration.DefaultYarnApplicationClasspath))
				{
					MRApps.AddToEnvironment(environment, ApplicationConstants.Environment.Classpath.ToString
						(), c.Trim(), conf);
				}
			}
			bool foundFrameworkInClasspath = (frameworkName == null);
			foreach (string c_1 in conf.GetStrings(MRJobConfig.MapreduceApplicationClasspath, 
				crossPlatform ? StringUtils.GetStrings(MRJobConfig.DefaultMapreduceCrossPlatformApplicationClasspath
				) : StringUtils.GetStrings(MRJobConfig.DefaultMapreduceApplicationClasspath)))
			{
				MRApps.AddToEnvironment(environment, ApplicationConstants.Environment.Classpath.ToString
					(), c_1.Trim(), conf);
				if (!foundFrameworkInClasspath)
				{
					foundFrameworkInClasspath = c_1.Contains(frameworkName);
				}
			}
			if (!foundFrameworkInClasspath)
			{
				throw new ArgumentException("Could not locate MapReduce framework name '" + frameworkName
					 + "' in " + MRJobConfig.MapreduceApplicationClasspath);
			}
		}

		// TODO: Remove duplicates.
		/// <exception cref="System.IO.IOException"/>
		public static void SetClasspath(IDictionary<string, string> environment, Configuration
			 conf)
		{
			bool userClassesTakesPrecedence = conf.GetBoolean(MRJobConfig.MapreduceJobUserClasspathFirst
				, false);
			string classpathEnvVar = conf.GetBoolean(MRJobConfig.MapreduceJobClassloader, false
				) ? ApplicationConstants.Environment.AppClasspath.ToString() : ApplicationConstants.Environment
				.Classpath.ToString();
			string hadoopClasspathEnvVar = ApplicationConstants.Environment.HadoopClasspath.ToString
				();
			MRApps.AddToEnvironment(environment, classpathEnvVar, CrossPlatformifyMREnv(conf, 
				ApplicationConstants.Environment.Pwd), conf);
			MRApps.AddToEnvironment(environment, hadoopClasspathEnvVar, CrossPlatformifyMREnv
				(conf, ApplicationConstants.Environment.Pwd), conf);
			if (!userClassesTakesPrecedence)
			{
				MRApps.SetMRFrameworkClasspath(environment, conf);
			}
			AddClasspathToEnv(environment, classpathEnvVar, conf);
			AddClasspathToEnv(environment, hadoopClasspathEnvVar, conf);
			if (userClassesTakesPrecedence)
			{
				MRApps.SetMRFrameworkClasspath(environment, conf);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public static void AddClasspathToEnv(IDictionary<string, string> environment, string
			 classpathEnvVar, Configuration conf)
		{
			MRApps.AddToEnvironment(environment, classpathEnvVar, MRJobConfig.JobJar + Path.Separator
				 + MRJobConfig.JobJar, conf);
			MRApps.AddToEnvironment(environment, classpathEnvVar, MRJobConfig.JobJar + Path.Separator
				 + "classes" + Path.Separator, conf);
			MRApps.AddToEnvironment(environment, classpathEnvVar, MRJobConfig.JobJar + Path.Separator
				 + "lib" + Path.Separator + "*", conf);
			MRApps.AddToEnvironment(environment, classpathEnvVar, CrossPlatformifyMREnv(conf, 
				ApplicationConstants.Environment.Pwd) + Path.Separator + "*", conf);
			// a * in the classpath will only find a .jar, so we need to filter out
			// all .jars and add everything else
			AddToClasspathIfNotJar(DistributedCache.GetFileClassPaths(conf), DistributedCache
				.GetCacheFiles(conf), conf, environment, classpathEnvVar);
			AddToClasspathIfNotJar(DistributedCache.GetArchiveClassPaths(conf), DistributedCache
				.GetCacheArchives(conf), conf, environment, classpathEnvVar);
		}

		/// <summary>Add the paths to the classpath if they are not jars</summary>
		/// <param name="paths">the paths to add to the classpath</param>
		/// <param name="withLinks">the corresponding paths that may have a link name in them
		/// 	</param>
		/// <param name="conf">used to resolve the paths</param>
		/// <param name="environment">the environment to update CLASSPATH in</param>
		/// <exception cref="System.IO.IOException">if there is an error resolving any of the paths.
		/// 	</exception>
		private static void AddToClasspathIfNotJar(Path[] paths, URI[] withLinks, Configuration
			 conf, IDictionary<string, string> environment, string classpathEnvVar)
		{
			if (paths != null)
			{
				Dictionary<Path, string> linkLookup = new Dictionary<Path, string>();
				if (withLinks != null)
				{
					foreach (URI u in withLinks)
					{
						Path p = new Path(u);
						FileSystem remoteFS = p.GetFileSystem(conf);
						p = remoteFS.ResolvePath(p.MakeQualified(remoteFS.GetUri(), remoteFS.GetWorkingDirectory
							()));
						string name = (null == u.GetFragment()) ? p.GetName() : u.GetFragment();
						if (!StringUtils.ToLowerCase(name).EndsWith(".jar"))
						{
							linkLookup[p] = name;
						}
					}
				}
				foreach (Path p_1 in paths)
				{
					FileSystem remoteFS = p_1.GetFileSystem(conf);
					p_1 = remoteFS.ResolvePath(p_1.MakeQualified(remoteFS.GetUri(), remoteFS.GetWorkingDirectory
						()));
					string name = linkLookup[p_1];
					if (name == null)
					{
						name = p_1.GetName();
					}
					if (!StringUtils.ToLowerCase(name).EndsWith(".jar"))
					{
						MRApps.AddToEnvironment(environment, classpathEnvVar, CrossPlatformifyMREnv(conf, 
							ApplicationConstants.Environment.Pwd) + Path.Separator + name, conf);
					}
				}
			}
		}

		/// <summary>
		/// Creates and sets a
		/// <see cref="Org.Apache.Hadoop.Util.ApplicationClassLoader"/>
		/// on the given
		/// configuration and as the thread context classloader, if
		/// <see cref="Org.Apache.Hadoop.Mapreduce.MRJobConfig.MapreduceJobClassloader"/>
		/// is set to true, and
		/// the APP_CLASSPATH environment variable is set.
		/// </summary>
		/// <param name="conf"/>
		/// <exception cref="System.IO.IOException"/>
		public static void SetJobClassLoader(Configuration conf)
		{
			SetClassLoader(CreateJobClassLoader(conf), conf);
		}

		/// <summary>
		/// Creates a
		/// <see cref="Org.Apache.Hadoop.Util.ApplicationClassLoader"/>
		/// if
		/// <see cref="Org.Apache.Hadoop.Mapreduce.MRJobConfig.MapreduceJobClassloader"/>
		/// is set to true, and
		/// the APP_CLASSPATH environment variable is set.
		/// </summary>
		/// <param name="conf"/>
		/// <returns>
		/// the created job classloader, or null if the job classloader is not
		/// enabled or the APP_CLASSPATH environment variable is not set
		/// </returns>
		/// <exception cref="System.IO.IOException"/>
		public static ClassLoader CreateJobClassLoader(Configuration conf)
		{
			ClassLoader jobClassLoader = null;
			if (conf.GetBoolean(MRJobConfig.MapreduceJobClassloader, false))
			{
				string appClasspath = Runtime.Getenv(ApplicationConstants.Environment.AppClasspath
					.Key());
				if (appClasspath == null)
				{
					Log.Warn("Not creating job classloader since APP_CLASSPATH is not set.");
				}
				else
				{
					Log.Info("Creating job classloader");
					if (Log.IsDebugEnabled())
					{
						Log.Debug("APP_CLASSPATH=" + appClasspath);
					}
					string[] systemClasses = GetSystemClasses(conf);
					jobClassLoader = CreateJobClassLoader(appClasspath, systemClasses);
				}
			}
			return jobClassLoader;
		}

		/// <summary>
		/// Sets the provided classloader on the given configuration and as the thread
		/// context classloader if the classloader is not null.
		/// </summary>
		/// <param name="classLoader"/>
		/// <param name="conf"/>
		public static void SetClassLoader(ClassLoader classLoader, Configuration conf)
		{
			if (classLoader != null)
			{
				Log.Info("Setting classloader " + classLoader.GetType().FullName + " on the configuration and as the thread context classloader"
					);
				conf.SetClassLoader(classLoader);
				Sharpen.Thread.CurrentThread().SetContextClassLoader(classLoader);
			}
		}

		[VisibleForTesting]
		internal static string[] GetSystemClasses(Configuration conf)
		{
			return conf.GetTrimmedStrings(MRJobConfig.MapreduceJobClassloaderSystemClasses);
		}

		/// <exception cref="System.IO.IOException"/>
		private static ClassLoader CreateJobClassLoader(string appClasspath, string[] systemClasses
			)
		{
			try
			{
				return AccessController.DoPrivileged(new _PrivilegedExceptionAction_420(appClasspath
					, systemClasses));
			}
			catch (PrivilegedActionException e)
			{
				Exception t = e.InnerException;
				if (t is UriFormatException)
				{
					throw (UriFormatException)t;
				}
				throw new IOException(e);
			}
		}

		private sealed class _PrivilegedExceptionAction_420 : PrivilegedExceptionAction<ClassLoader
			>
		{
			public _PrivilegedExceptionAction_420(string appClasspath, string[] systemClasses
				)
			{
				this.appClasspath = appClasspath;
				this.systemClasses = systemClasses;
			}

			/// <exception cref="System.UriFormatException"/>
			public ClassLoader Run()
			{
				return new ApplicationClassLoader(appClasspath, typeof(MRApps).GetClassLoader(), 
					Arrays.AsList(systemClasses));
			}

			private readonly string appClasspath;

			private readonly string[] systemClasses;
		}

		private const string StagingConstant = ".staging";

		public static Path GetStagingAreaDir(Configuration conf, string user)
		{
			return new Path(conf.Get(MRJobConfig.MrAmStagingDir, MRJobConfig.DefaultMrAmStagingDir
				) + Path.Separator + user + Path.Separator + StagingConstant);
		}

		public static string GetJobFile(Configuration conf, string user, JobID jobId)
		{
			Path jobFile = new Path(MRApps.GetStagingAreaDir(conf, user), jobId.ToString() + 
				Path.Separator + MRJobConfig.JobConfFile);
			return jobFile.ToString();
		}

		public static Path GetEndJobCommitSuccessFile(Configuration conf, string user, JobId
			 jobId)
		{
			Path endCommitFile = new Path(MRApps.GetStagingAreaDir(conf, user), jobId.ToString
				() + Path.Separator + "COMMIT_SUCCESS");
			return endCommitFile;
		}

		public static Path GetEndJobCommitFailureFile(Configuration conf, string user, JobId
			 jobId)
		{
			Path endCommitFile = new Path(MRApps.GetStagingAreaDir(conf, user), jobId.ToString
				() + Path.Separator + "COMMIT_FAIL");
			return endCommitFile;
		}

		public static Path GetStartJobCommitFile(Configuration conf, string user, JobId jobId
			)
		{
			Path startCommitFile = new Path(MRApps.GetStagingAreaDir(conf, user), jobId.ToString
				() + Path.Separator + "COMMIT_STARTED");
			return startCommitFile;
		}

		/// <exception cref="System.IO.IOException"/>
		public static void SetupDistributedCache(Configuration conf, IDictionary<string, 
			LocalResource> localResources)
		{
			// Cache archives
			ParseDistributedCacheArtifacts(conf, localResources, LocalResourceType.Archive, DistributedCache
				.GetCacheArchives(conf), DistributedCache.GetArchiveTimestamps(conf), GetFileSizes
				(conf, MRJobConfig.CacheArchivesSizes), DistributedCache.GetArchiveVisibilities(
				conf));
			// Cache files
			ParseDistributedCacheArtifacts(conf, localResources, LocalResourceType.File, DistributedCache
				.GetCacheFiles(conf), DistributedCache.GetFileTimestamps(conf), GetFileSizes(conf
				, MRJobConfig.CacheFilesSizes), DistributedCache.GetFileVisibilities(conf));
		}

		/// <summary>
		/// Set up the DistributedCache related configs to make
		/// <see cref="Org.Apache.Hadoop.Mapreduce.Filecache.DistributedCache.GetLocalCacheFiles(Org.Apache.Hadoop.Conf.Configuration)
		/// 	"/>
		/// and
		/// <see cref="Org.Apache.Hadoop.Mapreduce.Filecache.DistributedCache.GetLocalCacheArchives(Org.Apache.Hadoop.Conf.Configuration)
		/// 	"/>
		/// working.
		/// </summary>
		/// <param name="conf"/>
		/// <exception cref="System.IO.IOException"/>
		public static void SetupDistributedCacheLocal(Configuration conf)
		{
			string localWorkDir = Runtime.Getenv("PWD");
			//        ^ ^ all symlinks are created in the current work-dir
			// Update the configuration object with localized archives.
			URI[] cacheArchives = DistributedCache.GetCacheArchives(conf);
			if (cacheArchives != null)
			{
				IList<string> localArchives = new AList<string>();
				for (int i = 0; i < cacheArchives.Length; ++i)
				{
					URI u = cacheArchives[i];
					Path p = new Path(u);
					Path name = new Path((null == u.GetFragment()) ? p.GetName() : u.GetFragment());
					string linkName = name.ToUri().GetPath();
					localArchives.AddItem(new Path(localWorkDir, linkName).ToUri().GetPath());
				}
				if (!localArchives.IsEmpty())
				{
					conf.Set(MRJobConfig.CacheLocalarchives, StringUtils.ArrayToString(Sharpen.Collections.ToArray
						(localArchives, new string[localArchives.Count])));
				}
			}
			// Update the configuration object with localized files.
			URI[] cacheFiles = DistributedCache.GetCacheFiles(conf);
			if (cacheFiles != null)
			{
				IList<string> localFiles = new AList<string>();
				for (int i = 0; i < cacheFiles.Length; ++i)
				{
					URI u = cacheFiles[i];
					Path p = new Path(u);
					Path name = new Path((null == u.GetFragment()) ? p.GetName() : u.GetFragment());
					string linkName = name.ToUri().GetPath();
					localFiles.AddItem(new Path(localWorkDir, linkName).ToUri().GetPath());
				}
				if (!localFiles.IsEmpty())
				{
					conf.Set(MRJobConfig.CacheLocalfiles, StringUtils.ArrayToString(Sharpen.Collections.ToArray
						(localFiles, new string[localFiles.Count])));
				}
			}
		}

		private static string GetResourceDescription(LocalResourceType type)
		{
			if (type == LocalResourceType.Archive || type == LocalResourceType.Pattern)
			{
				return "cache archive (" + MRJobConfig.CacheArchives + ") ";
			}
			return "cache file (" + MRJobConfig.CacheFiles + ") ";
		}

		private static string ToString(URL url)
		{
			StringBuilder b = new StringBuilder();
			b.Append(url.GetScheme()).Append("://").Append(url.GetHost());
			if (url.GetPort() >= 0)
			{
				b.Append(":").Append(url.GetPort());
			}
			b.Append(url.GetFile());
			return b.ToString();
		}

		// TODO - Move this to MR!
		// Use TaskDistributedCacheManager.CacheFiles.makeCacheFiles(URI[], 
		// long[], boolean[], Path[], FileType)
		/// <exception cref="System.IO.IOException"/>
		private static void ParseDistributedCacheArtifacts(Configuration conf, IDictionary
			<string, LocalResource> localResources, LocalResourceType type, URI[] uris, long
			[] timestamps, long[] sizes, bool[] visibilities)
		{
			if (uris != null)
			{
				// Sanity check
				if ((uris.Length != timestamps.Length) || (uris.Length != sizes.Length) || (uris.
					Length != visibilities.Length))
				{
					throw new ArgumentException("Invalid specification for " + "distributed-cache artifacts of type "
						 + type + " :" + " #uris=" + uris.Length + " #timestamps=" + timestamps.Length +
						 " #visibilities=" + visibilities.Length);
				}
				for (int i = 0; i < uris.Length; ++i)
				{
					URI u = uris[i];
					Path p = new Path(u);
					FileSystem remoteFS = p.GetFileSystem(conf);
					p = remoteFS.ResolvePath(p.MakeQualified(remoteFS.GetUri(), remoteFS.GetWorkingDirectory
						()));
					// Add URI fragment or just the filename
					Path name = new Path((null == u.GetFragment()) ? p.GetName() : u.GetFragment());
					if (name.IsAbsolute())
					{
						throw new ArgumentException("Resource name must be relative");
					}
					string linkName = name.ToUri().GetPath();
					LocalResource orig = localResources[linkName];
					URL url = ConverterUtils.GetYarnUrlFromURI(p.ToUri());
					if (orig != null && !orig.GetResource().Equals(url))
					{
						Log.Warn(GetResourceDescription(orig.GetType()) + ToString(orig.GetResource()) + 
							" conflicts with " + GetResourceDescription(type) + ToString(url) + " This will be an error in Hadoop 2.0"
							);
						continue;
					}
					localResources[linkName] = LocalResource.NewInstance(ConverterUtils.GetYarnUrlFromURI
						(p.ToUri()), type, visibilities[i] ? LocalResourceVisibility.Public : LocalResourceVisibility
						.Private, sizes[i], timestamps[i]);
				}
			}
		}

		// TODO - Move this to MR!
		private static long[] GetFileSizes(Configuration conf, string key)
		{
			string[] strs = conf.GetStrings(key);
			if (strs == null)
			{
				return null;
			}
			long[] result = new long[strs.Length];
			for (int i = 0; i < strs.Length; ++i)
			{
				result[i] = long.Parse(strs[i]);
			}
			return result;
		}

		public static string GetChildLogLevel(Configuration conf, bool isMap)
		{
			if (isMap)
			{
				return conf.Get(MRJobConfig.MapLogLevel, JobConf.DefaultLogLevel.ToString());
			}
			else
			{
				return conf.Get(MRJobConfig.ReduceLogLevel, JobConf.DefaultLogLevel.ToString());
			}
		}

		/// <summary>
		/// Add the JVM system properties necessary to configure
		/// <see cref="Org.Apache.Hadoop.Yarn.ContainerLogAppender"/>
		/// or
		/// <see cref="Org.Apache.Hadoop.Yarn.ContainerRollingLogAppender"/>
		/// .
		/// </summary>
		/// <param name="task">for map/reduce, or null for app master</param>
		/// <param name="vargs">the argument list to append to</param>
		/// <param name="conf">configuration of MR job</param>
		public static void AddLog4jSystemProperties(Task task, IList<string> vargs, Configuration
			 conf)
		{
			string log4jPropertyFile = conf.Get(MRJobConfig.MapreduceJobLog4jPropertiesFile, 
				string.Empty);
			if (log4jPropertyFile.IsEmpty())
			{
				vargs.AddItem("-Dlog4j.configuration=container-log4j.properties");
			}
			else
			{
				URI log4jURI = null;
				try
				{
					log4jURI = new URI(log4jPropertyFile);
				}
				catch (URISyntaxException e)
				{
					throw new ArgumentException(e);
				}
				Path log4jPath = new Path(log4jURI);
				vargs.AddItem("-Dlog4j.configuration=" + log4jPath.GetName());
			}
			long logSize;
			string logLevel;
			int numBackups;
			if (task == null)
			{
				logSize = conf.GetLong(MRJobConfig.MrAmLogKb, MRJobConfig.DefaultMrAmLogKb) << 10;
				logLevel = conf.Get(MRJobConfig.MrAmLogLevel, MRJobConfig.DefaultMrAmLogLevel);
				numBackups = conf.GetInt(MRJobConfig.MrAmLogBackups, MRJobConfig.DefaultMrAmLogBackups
					);
			}
			else
			{
				logSize = TaskLog.GetTaskLogLimitBytes(conf);
				logLevel = GetChildLogLevel(conf, task.IsMapTask());
				numBackups = conf.GetInt(MRJobConfig.TaskLogBackups, MRJobConfig.DefaultTaskLogBackups
					);
			}
			vargs.AddItem("-D" + YarnConfiguration.YarnAppContainerLogDir + "=" + ApplicationConstants
				.LogDirExpansionVar);
			vargs.AddItem("-D" + YarnConfiguration.YarnAppContainerLogSize + "=" + logSize);
			if (logSize > 0L && numBackups > 0)
			{
				// log should be rolled
				vargs.AddItem("-D" + YarnConfiguration.YarnAppContainerLogBackups + "=" + numBackups
					);
				vargs.AddItem("-Dhadoop.root.logger=" + logLevel + ",CRLA");
			}
			else
			{
				vargs.AddItem("-Dhadoop.root.logger=" + logLevel + ",CLA");
			}
			vargs.AddItem("-Dhadoop.root.logfile=" + TaskLog.LogName.Syslog);
			if (task != null && !task.IsMapTask() && conf.GetBoolean(MRJobConfig.ReduceSeparateShuffleLog
				, MRJobConfig.DefaultReduceSeparateShuffleLog))
			{
				int numShuffleBackups = conf.GetInt(MRJobConfig.ShuffleLogBackups, MRJobConfig.DefaultShuffleLogBackups
					);
				long shuffleLogSize = conf.GetLong(MRJobConfig.ShuffleLogKb, MRJobConfig.DefaultShuffleLogKb
					) << 10;
				string shuffleLogger = logLevel + (shuffleLogSize > 0L && numShuffleBackups > 0 ? 
					",shuffleCRLA" : ",shuffleCLA");
				vargs.AddItem("-D" + MRJobConfig.MrPrefix + "shuffle.logger=" + shuffleLogger);
				vargs.AddItem("-D" + MRJobConfig.MrPrefix + "shuffle.logfile=" + TaskLog.LogName.
					Syslog + ".shuffle");
				vargs.AddItem("-D" + MRJobConfig.MrPrefix + "shuffle.log.filesize=" + shuffleLogSize
					);
				vargs.AddItem("-D" + MRJobConfig.MrPrefix + "shuffle.log.backups=" + numShuffleBackups
					);
			}
		}

		public static void SetEnvFromInputString(IDictionary<string, string> env, string 
			envString, Configuration conf)
		{
			string classPathSeparator = conf.GetBoolean(MRConfig.MapreduceAppSubmissionCrossPlatform
				, MRConfig.DefaultMapreduceAppSubmissionCrossPlatform) ? ApplicationConstants.ClassPathSeparator
				 : FilePath.pathSeparator;
			Apps.SetEnvFromInputString(env, envString, classPathSeparator);
		}

		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public static void AddToEnvironment(IDictionary<string, string> environment, string
			 variable, string value, Configuration conf)
		{
			string classPathSeparator = conf.GetBoolean(MRConfig.MapreduceAppSubmissionCrossPlatform
				, MRConfig.DefaultMapreduceAppSubmissionCrossPlatform) ? ApplicationConstants.ClassPathSeparator
				 : FilePath.pathSeparator;
			Apps.AddToEnvironment(environment, variable, value, classPathSeparator);
		}

		public static string CrossPlatformifyMREnv(Configuration conf, ApplicationConstants.Environment
			 env)
		{
			bool crossPlatform = conf.GetBoolean(MRConfig.MapreduceAppSubmissionCrossPlatform
				, MRConfig.DefaultMapreduceAppSubmissionCrossPlatform);
			return crossPlatform ? env.$$() : env.$();
		}
	}
}
