using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	/// <summary>Utility class for skip bad records functionality.</summary>
	/// <remarks>
	/// Utility class for skip bad records functionality. It contains various
	/// settings related to skipping of bad records.
	/// <p>Hadoop provides an optional mode of execution in which the bad records
	/// are detected and skipped in further attempts.
	/// <p>This feature can be used when map/reduce tasks crashes deterministically on
	/// certain input. This happens due to bugs in the map/reduce function. The usual
	/// course would be to fix these bugs. But sometimes this is not possible;
	/// perhaps the bug is in third party libraries for which the source code is
	/// not available. Due to this, the task never reaches to completion even with
	/// multiple attempts and complete data for that task is lost.</p>
	/// <p>With this feature, only a small portion of data is lost surrounding
	/// the bad record, which may be acceptable for some user applications.
	/// see
	/// <see cref="SetMapperMaxSkipRecords(Org.Apache.Hadoop.Conf.Configuration, long)"/>
	/// </p>
	/// <p>The skipping mode gets kicked off after certain no of failures
	/// see
	/// <see cref="SetAttemptsToStartSkipping(Org.Apache.Hadoop.Conf.Configuration, int)"
	/// 	/>
	/// </p>
	/// <p>In the skipping mode, the map/reduce task maintains the record range which
	/// is getting processed at all times. Before giving the input to the
	/// map/reduce function, it sends this record range to the Task tracker.
	/// If task crashes, the Task tracker knows which one was the last reported
	/// range. On further attempts that range get skipped.</p>
	/// </remarks>
	public class SkipBadRecords
	{
		/// <summary>
		/// Special counters which are written by the application and are
		/// used by the framework for detecting bad records.
		/// </summary>
		/// <remarks>
		/// Special counters which are written by the application and are
		/// used by the framework for detecting bad records. For detecting bad records
		/// these counters must be incremented by the application.
		/// </remarks>
		public const string CounterGroup = "SkippingTaskCounters";

		/// <summary>Number of processed map records.</summary>
		/// <seealso cref="GetAutoIncrMapperProcCount(Org.Apache.Hadoop.Conf.Configuration)"/
		/// 	>
		public const string CounterMapProcessedRecords = "MapProcessedRecords";

		/// <summary>Number of processed reduce groups.</summary>
		/// <seealso cref="GetAutoIncrReducerProcCount(Org.Apache.Hadoop.Conf.Configuration)"
		/// 	/>
		public const string CounterReduceProcessedGroups = "ReduceProcessedGroups";

		private const string AttemptsToStartSkipping = JobContext.SkipStartAttempts;

		private const string AutoIncrMapProcCount = JobContext.MapSkipIncrProcCount;

		private const string AutoIncrReduceProcCount = JobContext.ReduceSkipIncrProcCount;

		private const string OutPath = JobContext.SkipOutdir;

		private const string MapperMaxSkipRecords = JobContext.MapSkipMaxRecords;

		private const string ReducerMaxSkipGroups = JobContext.ReduceSkipMaxgroups;

		/// <summary>
		/// Get the number of Task attempts AFTER which skip mode
		/// will be kicked off.
		/// </summary>
		/// <remarks>
		/// Get the number of Task attempts AFTER which skip mode
		/// will be kicked off. When skip mode is kicked off, the
		/// tasks reports the range of records which it will process
		/// next to the TaskTracker. So that on failures, TT knows which
		/// ones are possibly the bad records. On further executions,
		/// those are skipped.
		/// Default value is 2.
		/// </remarks>
		/// <param name="conf">the configuration</param>
		/// <returns>attemptsToStartSkipping no of task attempts</returns>
		public static int GetAttemptsToStartSkipping(Configuration conf)
		{
			return conf.GetInt(AttemptsToStartSkipping, 2);
		}

		/// <summary>
		/// Set the number of Task attempts AFTER which skip mode
		/// will be kicked off.
		/// </summary>
		/// <remarks>
		/// Set the number of Task attempts AFTER which skip mode
		/// will be kicked off. When skip mode is kicked off, the
		/// tasks reports the range of records which it will process
		/// next to the TaskTracker. So that on failures, TT knows which
		/// ones are possibly the bad records. On further executions,
		/// those are skipped.
		/// Default value is 2.
		/// </remarks>
		/// <param name="conf">the configuration</param>
		/// <param name="attemptsToStartSkipping">no of task attempts</param>
		public static void SetAttemptsToStartSkipping(Configuration conf, int attemptsToStartSkipping
			)
		{
			conf.SetInt(AttemptsToStartSkipping, attemptsToStartSkipping);
		}

		/// <summary>
		/// Get the flag which if set to true,
		/// <see cref="CounterMapProcessedRecords"/>
		/// is incremented
		/// by MapRunner after invoking the map function. This value must be set to
		/// false for applications which process the records asynchronously
		/// or buffer the input records. For example streaming.
		/// In such cases applications should increment this counter on their own.
		/// Default value is true.
		/// </summary>
		/// <param name="conf">the configuration</param>
		/// <returns>
		/// <code>true</code> if auto increment
		/// <see cref="CounterMapProcessedRecords"/>
		/// .
		/// <code>false</code> otherwise.
		/// </returns>
		public static bool GetAutoIncrMapperProcCount(Configuration conf)
		{
			return conf.GetBoolean(AutoIncrMapProcCount, true);
		}

		/// <summary>
		/// Set the flag which if set to true,
		/// <see cref="CounterMapProcessedRecords"/>
		/// is incremented
		/// by MapRunner after invoking the map function. This value must be set to
		/// false for applications which process the records asynchronously
		/// or buffer the input records. For example streaming.
		/// In such cases applications should increment this counter on their own.
		/// Default value is true.
		/// </summary>
		/// <param name="conf">the configuration</param>
		/// <param name="autoIncr">
		/// whether to auto increment
		/// <see cref="CounterMapProcessedRecords"/>
		/// .
		/// </param>
		public static void SetAutoIncrMapperProcCount(Configuration conf, bool autoIncr)
		{
			conf.SetBoolean(AutoIncrMapProcCount, autoIncr);
		}

		/// <summary>
		/// Get the flag which if set to true,
		/// <see cref="CounterReduceProcessedGroups"/>
		/// is incremented
		/// by framework after invoking the reduce function. This value must be set to
		/// false for applications which process the records asynchronously
		/// or buffer the input records. For example streaming.
		/// In such cases applications should increment this counter on their own.
		/// Default value is true.
		/// </summary>
		/// <param name="conf">the configuration</param>
		/// <returns>
		/// <code>true</code> if auto increment
		/// <see cref="CounterReduceProcessedGroups"/>
		/// .
		/// <code>false</code> otherwise.
		/// </returns>
		public static bool GetAutoIncrReducerProcCount(Configuration conf)
		{
			return conf.GetBoolean(AutoIncrReduceProcCount, true);
		}

		/// <summary>
		/// Set the flag which if set to true,
		/// <see cref="CounterReduceProcessedGroups"/>
		/// is incremented
		/// by framework after invoking the reduce function. This value must be set to
		/// false for applications which process the records asynchronously
		/// or buffer the input records. For example streaming.
		/// In such cases applications should increment this counter on their own.
		/// Default value is true.
		/// </summary>
		/// <param name="conf">the configuration</param>
		/// <param name="autoIncr">
		/// whether to auto increment
		/// <see cref="CounterReduceProcessedGroups"/>
		/// .
		/// </param>
		public static void SetAutoIncrReducerProcCount(Configuration conf, bool autoIncr)
		{
			conf.SetBoolean(AutoIncrReduceProcCount, autoIncr);
		}

		/// <summary>Get the directory to which skipped records are written.</summary>
		/// <remarks>
		/// Get the directory to which skipped records are written. By default it is
		/// the sub directory of the output _logs directory.
		/// User can stop writing skipped records by setting the value null.
		/// </remarks>
		/// <param name="conf">the configuration.</param>
		/// <returns>
		/// path skip output directory. Null is returned if this is not set
		/// and output directory is also not set.
		/// </returns>
		public static Path GetSkipOutputPath(Configuration conf)
		{
			string name = conf.Get(OutPath);
			if (name != null)
			{
				if ("none".Equals(name))
				{
					return null;
				}
				return new Path(name);
			}
			Path outPath = FileOutputFormat.GetOutputPath(new JobConf(conf));
			return outPath == null ? null : new Path(outPath, "_logs" + Path.Separator + "skip"
				);
		}

		/// <summary>Set the directory to which skipped records are written.</summary>
		/// <remarks>
		/// Set the directory to which skipped records are written. By default it is
		/// the sub directory of the output _logs directory.
		/// User can stop writing skipped records by setting the value null.
		/// </remarks>
		/// <param name="conf">the configuration.</param>
		/// <param name="path">skip output directory path</param>
		public static void SetSkipOutputPath(JobConf conf, Path path)
		{
			string pathStr = null;
			if (path == null)
			{
				pathStr = "none";
			}
			else
			{
				pathStr = path.ToString();
			}
			conf.Set(OutPath, pathStr);
		}

		/// <summary>
		/// Get the number of acceptable skip records surrounding the bad record PER
		/// bad record in mapper.
		/// </summary>
		/// <remarks>
		/// Get the number of acceptable skip records surrounding the bad record PER
		/// bad record in mapper. The number includes the bad record as well.
		/// To turn the feature of detection/skipping of bad records off, set the
		/// value to 0.
		/// The framework tries to narrow down the skipped range by retrying
		/// until this threshold is met OR all attempts get exhausted for this task.
		/// Set the value to Long.MAX_VALUE to indicate that framework need not try to
		/// narrow down. Whatever records(depends on application) get skipped are
		/// acceptable.
		/// Default value is 0.
		/// </remarks>
		/// <param name="conf">the configuration</param>
		/// <returns>maxSkipRecs acceptable skip records.</returns>
		public static long GetMapperMaxSkipRecords(Configuration conf)
		{
			return conf.GetLong(MapperMaxSkipRecords, 0);
		}

		/// <summary>
		/// Set the number of acceptable skip records surrounding the bad record PER
		/// bad record in mapper.
		/// </summary>
		/// <remarks>
		/// Set the number of acceptable skip records surrounding the bad record PER
		/// bad record in mapper. The number includes the bad record as well.
		/// To turn the feature of detection/skipping of bad records off, set the
		/// value to 0.
		/// The framework tries to narrow down the skipped range by retrying
		/// until this threshold is met OR all attempts get exhausted for this task.
		/// Set the value to Long.MAX_VALUE to indicate that framework need not try to
		/// narrow down. Whatever records(depends on application) get skipped are
		/// acceptable.
		/// Default value is 0.
		/// </remarks>
		/// <param name="conf">the configuration</param>
		/// <param name="maxSkipRecs">acceptable skip records.</param>
		public static void SetMapperMaxSkipRecords(Configuration conf, long maxSkipRecs)
		{
			conf.SetLong(MapperMaxSkipRecords, maxSkipRecs);
		}

		/// <summary>
		/// Get the number of acceptable skip groups surrounding the bad group PER
		/// bad group in reducer.
		/// </summary>
		/// <remarks>
		/// Get the number of acceptable skip groups surrounding the bad group PER
		/// bad group in reducer. The number includes the bad group as well.
		/// To turn the feature of detection/skipping of bad groups off, set the
		/// value to 0.
		/// The framework tries to narrow down the skipped range by retrying
		/// until this threshold is met OR all attempts get exhausted for this task.
		/// Set the value to Long.MAX_VALUE to indicate that framework need not try to
		/// narrow down. Whatever groups(depends on application) get skipped are
		/// acceptable.
		/// Default value is 0.
		/// </remarks>
		/// <param name="conf">the configuration</param>
		/// <returns>maxSkipGrps acceptable skip groups.</returns>
		public static long GetReducerMaxSkipGroups(Configuration conf)
		{
			return conf.GetLong(ReducerMaxSkipGroups, 0);
		}

		/// <summary>
		/// Set the number of acceptable skip groups surrounding the bad group PER
		/// bad group in reducer.
		/// </summary>
		/// <remarks>
		/// Set the number of acceptable skip groups surrounding the bad group PER
		/// bad group in reducer. The number includes the bad group as well.
		/// To turn the feature of detection/skipping of bad groups off, set the
		/// value to 0.
		/// The framework tries to narrow down the skipped range by retrying
		/// until this threshold is met OR all attempts get exhausted for this task.
		/// Set the value to Long.MAX_VALUE to indicate that framework need not try to
		/// narrow down. Whatever groups(depends on application) get skipped are
		/// acceptable.
		/// Default value is 0.
		/// </remarks>
		/// <param name="conf">the configuration</param>
		/// <param name="maxSkipGrps">acceptable skip groups.</param>
		public static void SetReducerMaxSkipGroups(Configuration conf, long maxSkipGrps)
		{
			conf.SetLong(ReducerMaxSkipGroups, maxSkipGrps);
		}
	}
}
