using System;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Security;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce
{
	/// <summary>
	/// A read-only view of the job that is provided to the tasks while they
	/// are running.
	/// </summary>
	public interface JobContext : MRJobConfig
	{
		/// <summary>Return the configuration for the job.</summary>
		/// <returns>the shared configuration object</returns>
		Configuration GetConfiguration();

		/// <summary>Get credentials for the job.</summary>
		/// <returns>credentials for the job</returns>
		Credentials GetCredentials();

		/// <summary>Get the unique ID for the job.</summary>
		/// <returns>the object with the job id</returns>
		JobID GetJobID();

		/// <summary>Get configured the number of reduce tasks for this job.</summary>
		/// <remarks>
		/// Get configured the number of reduce tasks for this job. Defaults to
		/// <code>1</code>.
		/// </remarks>
		/// <returns>the number of reduce tasks for this job.</returns>
		int GetNumReduceTasks();

		/// <summary>Get the current working directory for the default file system.</summary>
		/// <returns>the directory name.</returns>
		/// <exception cref="System.IO.IOException"/>
		Path GetWorkingDirectory();

		/// <summary>Get the key class for the job output data.</summary>
		/// <returns>the key class for the job output data.</returns>
		Type GetOutputKeyClass();

		/// <summary>Get the value class for job outputs.</summary>
		/// <returns>the value class for job outputs.</returns>
		Type GetOutputValueClass();

		/// <summary>Get the key class for the map output data.</summary>
		/// <remarks>
		/// Get the key class for the map output data. If it is not set, use the
		/// (final) output key class. This allows the map output key class to be
		/// different than the final output key class.
		/// </remarks>
		/// <returns>the map output key class.</returns>
		Type GetMapOutputKeyClass();

		/// <summary>Get the value class for the map output data.</summary>
		/// <remarks>
		/// Get the value class for the map output data. If it is not set, use the
		/// (final) output value class This allows the map output value class to be
		/// different than the final output value class.
		/// </remarks>
		/// <returns>the map output value class.</returns>
		Type GetMapOutputValueClass();

		/// <summary>Get the user-specified job name.</summary>
		/// <remarks>
		/// Get the user-specified job name. This is only used to identify the
		/// job to the user.
		/// </remarks>
		/// <returns>the job's name, defaulting to "".</returns>
		string GetJobName();

		/// <summary>
		/// Get the
		/// <see cref="InputFormat{K, V}"/>
		/// class for the job.
		/// </summary>
		/// <returns>
		/// the
		/// <see cref="InputFormat{K, V}"/>
		/// class for the job.
		/// </returns>
		/// <exception cref="System.TypeLoadException"/>
		Type GetInputFormatClass();

		/// <summary>
		/// Get the
		/// <see cref="Mapper{KEYIN, VALUEIN, KEYOUT, VALUEOUT}"/>
		/// class for the job.
		/// </summary>
		/// <returns>
		/// the
		/// <see cref="Mapper{KEYIN, VALUEIN, KEYOUT, VALUEOUT}"/>
		/// class for the job.
		/// </returns>
		/// <exception cref="System.TypeLoadException"/>
		Type GetMapperClass();

		/// <summary>Get the combiner class for the job.</summary>
		/// <returns>the combiner class for the job.</returns>
		/// <exception cref="System.TypeLoadException"/>
		Type GetCombinerClass();

		/// <summary>
		/// Get the
		/// <see cref="Reducer{KEYIN, VALUEIN, KEYOUT, VALUEOUT}"/>
		/// class for the job.
		/// </summary>
		/// <returns>
		/// the
		/// <see cref="Reducer{KEYIN, VALUEIN, KEYOUT, VALUEOUT}"/>
		/// class for the job.
		/// </returns>
		/// <exception cref="System.TypeLoadException"/>
		Type GetReducerClass();

		/// <summary>
		/// Get the
		/// <see cref="OutputFormat{K, V}"/>
		/// class for the job.
		/// </summary>
		/// <returns>
		/// the
		/// <see cref="OutputFormat{K, V}"/>
		/// class for the job.
		/// </returns>
		/// <exception cref="System.TypeLoadException"/>
		Type GetOutputFormatClass();

		/// <summary>
		/// Get the
		/// <see cref="Partitioner{KEY, VALUE}"/>
		/// class for the job.
		/// </summary>
		/// <returns>
		/// the
		/// <see cref="Partitioner{KEY, VALUE}"/>
		/// class for the job.
		/// </returns>
		/// <exception cref="System.TypeLoadException"/>
		Type GetPartitionerClass();

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
		RawComparator<object> GetSortComparator();

		/// <summary>Get the pathname of the job's jar.</summary>
		/// <returns>the pathname</returns>
		string GetJar();

		/// <summary>
		/// Get the user defined
		/// <see cref="Org.Apache.Hadoop.IO.RawComparator{T}"/>
		/// comparator for
		/// grouping keys of inputs to the combiner.
		/// </summary>
		/// <returns>comparator set by the user for grouping values.</returns>
		/// <seealso cref="Job.SetCombinerKeyGroupingComparatorClass(System.Type{T})"/>
		RawComparator<object> GetCombinerKeyGroupingComparator();

		/// <summary>
		/// Get the user defined
		/// <see cref="Org.Apache.Hadoop.IO.RawComparator{T}"/>
		/// comparator for
		/// grouping keys of inputs to the reduce.
		/// </summary>
		/// <returns>comparator set by the user for grouping values.</returns>
		/// <seealso cref="Job.SetGroupingComparatorClass(System.Type{T})"/>
		/// <seealso cref="GetCombinerKeyGroupingComparator()"/>
		RawComparator<object> GetGroupingComparator();

		/// <summary>Get whether job-setup and job-cleanup is needed for the job</summary>
		/// <returns>boolean</returns>
		bool GetJobSetupCleanupNeeded();

		/// <summary>Get whether task-cleanup is needed for the job</summary>
		/// <returns>boolean</returns>
		bool GetTaskCleanupNeeded();

		/// <summary>Get whether the task profiling is enabled.</summary>
		/// <returns>true if some tasks will be profiled</returns>
		bool GetProfileEnabled();

		/// <summary>Get the profiler configuration arguments.</summary>
		/// <remarks>
		/// Get the profiler configuration arguments.
		/// The default value for this property is
		/// "-agentlib:hprof=cpu=samples,heap=sites,force=n,thread=y,verbose=n,file=%s"
		/// </remarks>
		/// <returns>the parameters to pass to the task child to configure profiling</returns>
		string GetProfileParams();

		/// <summary>Get the range of maps or reduces to profile.</summary>
		/// <param name="isMap">is the task a map?</param>
		/// <returns>the task ranges</returns>
		Configuration.IntegerRanges GetProfileTaskRange(bool isMap);

		/// <summary>Get the reported username for this job.</summary>
		/// <returns>the username</returns>
		string GetUser();

		/// <summary>
		/// Originally intended to check if symlinks should be used, but currently
		/// symlinks cannot be disabled.
		/// </summary>
		/// <returns>true</returns>
		[Obsolete]
		bool GetSymlink();

		/// <summary>Get the archive entries in classpath as an array of Path</summary>
		Path[] GetArchiveClassPaths();

		/// <summary>Get cache archives set in the Configuration</summary>
		/// <returns>A URI array of the caches set in the Configuration</returns>
		/// <exception cref="System.IO.IOException"/>
		URI[] GetCacheArchives();

		/// <summary>Get cache files set in the Configuration</summary>
		/// <returns>A URI array of the files set in the Configuration</returns>
		/// <exception cref="System.IO.IOException"/>
		URI[] GetCacheFiles();

		/// <summary>Return the path array of the localized caches</summary>
		/// <returns>A path array of localized caches</returns>
		/// <exception cref="System.IO.IOException"/>
		[System.ObsoleteAttribute(@"the array returned only includes the items the were downloaded. There is no way to map this to what is returned byGetCacheArchives() ."
			)]
		Path[] GetLocalCacheArchives();

		/// <summary>Return the path array of the localized files</summary>
		/// <returns>A path array of localized files</returns>
		/// <exception cref="System.IO.IOException"/>
		[System.ObsoleteAttribute(@"the array returned only includes the items the were downloaded. There is no way to map this to what is returned byGetCacheFiles() ."
			)]
		Path[] GetLocalCacheFiles();

		/// <summary>Get the file entries in classpath as an array of Path</summary>
		Path[] GetFileClassPaths();

		/// <summary>Get the timestamps of the archives.</summary>
		/// <remarks>
		/// Get the timestamps of the archives.  Used by internal
		/// DistributedCache and MapReduce code.
		/// </remarks>
		/// <returns>a string array of timestamps</returns>
		string[] GetArchiveTimestamps();

		/// <summary>Get the timestamps of the files.</summary>
		/// <remarks>
		/// Get the timestamps of the files.  Used by internal
		/// DistributedCache and MapReduce code.
		/// </remarks>
		/// <returns>a string array of timestamps</returns>
		string[] GetFileTimestamps();

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
		int GetMaxMapAttempts();

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
		int GetMaxReduceAttempts();
	}
}
