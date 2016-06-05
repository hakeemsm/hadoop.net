using System;
using System.IO;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Filecache;
using Org.Apache.Hadoop.Mapreduce.Lib.Input;
using Org.Apache.Hadoop.Mapreduce.Lib.Output;
using Org.Apache.Hadoop.Mapreduce.Lib.Partition;
using Org.Apache.Hadoop.Security;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Task
{
	/// <summary>
	/// A read-only view of the job that is provided to the tasks while they
	/// are running.
	/// </summary>
	public class JobContextImpl : JobContext
	{
		protected internal readonly JobConf conf;

		private JobID jobId;

		/// <summary>The UserGroupInformation object that has a reference to the current user
		/// 	</summary>
		protected internal UserGroupInformation ugi;

		protected internal readonly Credentials credentials;

		public JobContextImpl(Configuration conf, JobID jobId)
		{
			if (conf is JobConf)
			{
				this.conf = (JobConf)conf;
			}
			else
			{
				this.conf = new JobConf(conf);
			}
			this.jobId = jobId;
			this.credentials = this.conf.GetCredentials();
			try
			{
				this.ugi = UserGroupInformation.GetCurrentUser();
			}
			catch (IOException e)
			{
				throw new RuntimeException(e);
			}
		}

		/// <summary>Return the configuration for the job.</summary>
		/// <returns>the shared configuration object</returns>
		public virtual Configuration GetConfiguration()
		{
			return conf;
		}

		/// <summary>Get the unique ID for the job.</summary>
		/// <returns>the object with the job id</returns>
		public virtual JobID GetJobID()
		{
			return jobId;
		}

		/// <summary>Set the JobID.</summary>
		public virtual void SetJobID(JobID jobId)
		{
			this.jobId = jobId;
		}

		/// <summary>Get configured the number of reduce tasks for this job.</summary>
		/// <remarks>
		/// Get configured the number of reduce tasks for this job. Defaults to
		/// <code>1</code>.
		/// </remarks>
		/// <returns>the number of reduce tasks for this job.</returns>
		public virtual int GetNumReduceTasks()
		{
			return conf.GetNumReduceTasks();
		}

		/// <summary>Get the current working directory for the default file system.</summary>
		/// <returns>the directory name.</returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual Path GetWorkingDirectory()
		{
			return conf.GetWorkingDirectory();
		}

		/// <summary>Get the key class for the job output data.</summary>
		/// <returns>the key class for the job output data.</returns>
		public virtual Type GetOutputKeyClass()
		{
			return conf.GetOutputKeyClass();
		}

		/// <summary>Get the value class for job outputs.</summary>
		/// <returns>the value class for job outputs.</returns>
		public virtual Type GetOutputValueClass()
		{
			return conf.GetOutputValueClass();
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
			return conf.GetMapOutputKeyClass();
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
			return conf.GetMapOutputValueClass();
		}

		/// <summary>Get the user-specified job name.</summary>
		/// <remarks>
		/// Get the user-specified job name. This is only used to identify the
		/// job to the user.
		/// </remarks>
		/// <returns>the job's name, defaulting to "".</returns>
		public virtual string GetJobName()
		{
			return conf.GetJobName();
		}

		/// <summary>
		/// Get the
		/// <see cref="Org.Apache.Hadoop.Mapreduce.InputFormat{K, V}"/>
		/// class for the job.
		/// </summary>
		/// <returns>
		/// the
		/// <see cref="Org.Apache.Hadoop.Mapreduce.InputFormat{K, V}"/>
		/// class for the job.
		/// </returns>
		/// <exception cref="System.TypeLoadException"/>
		public virtual Type GetInputFormatClass()
		{
			return (Type)conf.GetClass(InputFormatClassAttr, typeof(TextInputFormat));
		}

		/// <summary>
		/// Get the
		/// <see cref="Org.Apache.Hadoop.Mapreduce.Mapper{KEYIN, VALUEIN, KEYOUT, VALUEOUT}"/
		/// 	>
		/// class for the job.
		/// </summary>
		/// <returns>
		/// the
		/// <see cref="Org.Apache.Hadoop.Mapreduce.Mapper{KEYIN, VALUEIN, KEYOUT, VALUEOUT}"/
		/// 	>
		/// class for the job.
		/// </returns>
		/// <exception cref="System.TypeLoadException"/>
		public virtual Type GetMapperClass()
		{
			return (Type)conf.GetClass(MapClassAttr, typeof(Mapper));
		}

		/// <summary>Get the combiner class for the job.</summary>
		/// <returns>the combiner class for the job.</returns>
		/// <exception cref="System.TypeLoadException"/>
		public virtual Type GetCombinerClass()
		{
			return (Type)conf.GetClass(CombineClassAttr, null);
		}

		/// <summary>
		/// Get the
		/// <see cref="Org.Apache.Hadoop.Mapreduce.Reducer{KEYIN, VALUEIN, KEYOUT, VALUEOUT}"
		/// 	/>
		/// class for the job.
		/// </summary>
		/// <returns>
		/// the
		/// <see cref="Org.Apache.Hadoop.Mapreduce.Reducer{KEYIN, VALUEIN, KEYOUT, VALUEOUT}"
		/// 	/>
		/// class for the job.
		/// </returns>
		/// <exception cref="System.TypeLoadException"/>
		public virtual Type GetReducerClass()
		{
			return (Type)conf.GetClass(ReduceClassAttr, typeof(Reducer));
		}

		/// <summary>
		/// Get the
		/// <see cref="Org.Apache.Hadoop.Mapreduce.OutputFormat{K, V}"/>
		/// class for the job.
		/// </summary>
		/// <returns>
		/// the
		/// <see cref="Org.Apache.Hadoop.Mapreduce.OutputFormat{K, V}"/>
		/// class for the job.
		/// </returns>
		/// <exception cref="System.TypeLoadException"/>
		public virtual Type GetOutputFormatClass()
		{
			return (Type)conf.GetClass(OutputFormatClassAttr, typeof(TextOutputFormat));
		}

		/// <summary>
		/// Get the
		/// <see cref="Org.Apache.Hadoop.Mapreduce.Partitioner{KEY, VALUE}"/>
		/// class for the job.
		/// </summary>
		/// <returns>
		/// the
		/// <see cref="Org.Apache.Hadoop.Mapreduce.Partitioner{KEY, VALUE}"/>
		/// class for the job.
		/// </returns>
		/// <exception cref="System.TypeLoadException"/>
		public virtual Type GetPartitionerClass()
		{
			return (Type)conf.GetClass(PartitionerClassAttr, typeof(HashPartitioner));
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
		public virtual RawComparator<object> GetSortComparator()
		{
			return conf.GetOutputKeyComparator();
		}

		/// <summary>Get the pathname of the job's jar.</summary>
		/// <returns>the pathname</returns>
		public virtual string GetJar()
		{
			return conf.GetJar();
		}

		/// <summary>
		/// Get the user defined
		/// <see cref="Org.Apache.Hadoop.IO.RawComparator{T}"/>
		/// comparator for
		/// grouping keys of inputs to the combiner.
		/// </summary>
		/// <returns>comparator set by the user for grouping values.</returns>
		/// <seealso cref="Org.Apache.Hadoop.Mapreduce.Job.SetCombinerKeyGroupingComparatorClass(System.Type{T})
		/// 	">for details.</seealso>
		public virtual RawComparator<object> GetCombinerKeyGroupingComparator()
		{
			return conf.GetCombinerKeyGroupingComparator();
		}

		/// <summary>
		/// Get the user defined
		/// <see cref="Org.Apache.Hadoop.IO.RawComparator{T}"/>
		/// comparator for
		/// grouping keys of inputs to the reduce.
		/// </summary>
		/// <returns>comparator set by the user for grouping values.</returns>
		/// <seealso cref="Org.Apache.Hadoop.Mapreduce.Job.SetGroupingComparatorClass(System.Type{T})
		/// 	">for details.</seealso>
		public virtual RawComparator<object> GetGroupingComparator()
		{
			return conf.GetOutputValueGroupingComparator();
		}

		/// <summary>Get whether job-setup and job-cleanup is needed for the job</summary>
		/// <returns>boolean</returns>
		public virtual bool GetJobSetupCleanupNeeded()
		{
			return conf.GetBoolean(MRJobConfig.SetupCleanupNeeded, true);
		}

		/// <summary>Get whether task-cleanup is needed for the job</summary>
		/// <returns>boolean</returns>
		public virtual bool GetTaskCleanupNeeded()
		{
			return conf.GetBoolean(MRJobConfig.TaskCleanupNeeded, true);
		}

		/// <summary>
		/// This method checks to see if symlinks are to be create for the
		/// localized cache files in the current working directory
		/// </summary>
		/// <returns>true if symlinks are to be created- else return false</returns>
		public virtual bool GetSymlink()
		{
			return DistributedCache.GetSymlink(conf);
		}

		/// <summary>Get the archive entries in classpath as an array of Path</summary>
		public virtual Path[] GetArchiveClassPaths()
		{
			return DistributedCache.GetArchiveClassPaths(conf);
		}

		/// <summary>Get cache archives set in the Configuration</summary>
		/// <returns>A URI array of the caches set in the Configuration</returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual URI[] GetCacheArchives()
		{
			return DistributedCache.GetCacheArchives(conf);
		}

		/// <summary>Get cache files set in the Configuration</summary>
		/// <returns>A URI array of the files set in the Configuration</returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual URI[] GetCacheFiles()
		{
			return DistributedCache.GetCacheFiles(conf);
		}

		/// <summary>Return the path array of the localized caches</summary>
		/// <returns>A path array of localized caches</returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual Path[] GetLocalCacheArchives()
		{
			return DistributedCache.GetLocalCacheArchives(conf);
		}

		/// <summary>Return the path array of the localized files</summary>
		/// <returns>A path array of localized files</returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual Path[] GetLocalCacheFiles()
		{
			return DistributedCache.GetLocalCacheFiles(conf);
		}

		/// <summary>Get the file entries in classpath as an array of Path</summary>
		public virtual Path[] GetFileClassPaths()
		{
			return DistributedCache.GetFileClassPaths(conf);
		}

		/// <summary>Parse a list of longs into strings.</summary>
		/// <param name="timestamps">the list of longs to parse</param>
		/// <returns>a list of string that were parsed. same length as timestamps.</returns>
		private static string[] ToTimestampStrs(long[] timestamps)
		{
			if (timestamps == null)
			{
				return null;
			}
			string[] result = new string[timestamps.Length];
			for (int i = 0; i < timestamps.Length; ++i)
			{
				result[i] = System.Convert.ToString(timestamps[i]);
			}
			return result;
		}

		/// <summary>Get the timestamps of the archives.</summary>
		/// <remarks>
		/// Get the timestamps of the archives.  Used by internal
		/// DistributedCache and MapReduce code.
		/// </remarks>
		/// <returns>a string array of timestamps</returns>
		public virtual string[] GetArchiveTimestamps()
		{
			return ToTimestampStrs(DistributedCache.GetArchiveTimestamps(conf));
		}

		/// <summary>Get the timestamps of the files.</summary>
		/// <remarks>
		/// Get the timestamps of the files.  Used by internal
		/// DistributedCache and MapReduce code.
		/// </remarks>
		/// <returns>a string array of timestamps</returns>
		public virtual string[] GetFileTimestamps()
		{
			return ToTimestampStrs(DistributedCache.GetFileTimestamps(conf));
		}

		/// <summary>
		/// Get the configured number of maximum attempts that will be made to run a
		/// map task, as specified by the <code>mapred.map.max.attempts</code>
		/// property.
		/// </summary>
		/// <remarks>
		/// Get the configured number of maximum attempts that will be made to run a
		/// map task, as specified by the <code>mapred.map.max.attempts</code>
		/// property. If this property is not already set, the default is 4 attempts.
		/// </remarks>
		/// <returns>the max number of attempts per map task.</returns>
		public virtual int GetMaxMapAttempts()
		{
			return conf.GetMaxMapAttempts();
		}

		/// <summary>
		/// Get the configured number of maximum attempts  that will be made to run a
		/// reduce task, as specified by the <code>mapred.reduce.max.attempts</code>
		/// property.
		/// </summary>
		/// <remarks>
		/// Get the configured number of maximum attempts  that will be made to run a
		/// reduce task, as specified by the <code>mapred.reduce.max.attempts</code>
		/// property. If this property is not already set, the default is 4 attempts.
		/// </remarks>
		/// <returns>the max number of attempts per reduce task.</returns>
		public virtual int GetMaxReduceAttempts()
		{
			return conf.GetMaxReduceAttempts();
		}

		/// <summary>Get whether the task profiling is enabled.</summary>
		/// <returns>true if some tasks will be profiled</returns>
		public virtual bool GetProfileEnabled()
		{
			return conf.GetProfileEnabled();
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
			return conf.GetProfileParams();
		}

		/// <summary>Get the range of maps or reduces to profile.</summary>
		/// <param name="isMap">is the task a map?</param>
		/// <returns>the task ranges</returns>
		public virtual Configuration.IntegerRanges GetProfileTaskRange(bool isMap)
		{
			return conf.GetProfileTaskRange(isMap);
		}

		/// <summary>Get the reported username for this job.</summary>
		/// <returns>the username</returns>
		public virtual string GetUser()
		{
			return conf.GetUser();
		}

		public virtual Credentials GetCredentials()
		{
			return credentials;
		}
	}
}
