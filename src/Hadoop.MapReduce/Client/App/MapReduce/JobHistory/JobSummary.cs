using System.Text;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Jobhistory
{
	public class JobSummary
	{
		private JobId jobId;

		private long jobSubmitTime;

		private long jobLaunchTime;

		private long firstMapTaskLaunchTime;

		private long firstReduceTaskLaunchTime;

		private long jobFinishTime;

		private int numFinishedMaps;

		private int numFailedMaps;

		private int numFinishedReduces;

		private int numFailedReduces;

		private int resourcesPerMap;

		private int resourcesPerReduce;

		private string user;

		private string queue;

		private string jobStatus;

		private long mapSlotSeconds;

		private long reduceSlotSeconds;

		private string jobName;

		internal JobSummary()
		{
		}

		// MapAttempteStarted |
		// TaskAttemptStartEvent
		// ReduceAttemptStarted |
		// TaskAttemptStartEvent
		// resources used per map/min resource
		// resources used per reduce/min resource
		// resource models
		// private int numSlotsPerReduce; | Doesn't make sense with potentially
		// different resource models
		// TODO Not generated yet in MRV2
		// TODO Not generated yet MRV2
		// private int clusterSlotCapacity;
		public virtual JobId GetJobId()
		{
			return jobId;
		}

		public virtual void SetJobId(JobId jobId)
		{
			this.jobId = jobId;
		}

		public virtual long GetJobSubmitTime()
		{
			return jobSubmitTime;
		}

		public virtual void SetJobSubmitTime(long jobSubmitTime)
		{
			this.jobSubmitTime = jobSubmitTime;
		}

		public virtual long GetJobLaunchTime()
		{
			return jobLaunchTime;
		}

		public virtual void SetJobLaunchTime(long jobLaunchTime)
		{
			this.jobLaunchTime = jobLaunchTime;
		}

		public virtual long GetFirstMapTaskLaunchTime()
		{
			return firstMapTaskLaunchTime;
		}

		public virtual void SetFirstMapTaskLaunchTime(long firstMapTaskLaunchTime)
		{
			this.firstMapTaskLaunchTime = firstMapTaskLaunchTime;
		}

		public virtual long GetFirstReduceTaskLaunchTime()
		{
			return firstReduceTaskLaunchTime;
		}

		public virtual void SetFirstReduceTaskLaunchTime(long firstReduceTaskLaunchTime)
		{
			this.firstReduceTaskLaunchTime = firstReduceTaskLaunchTime;
		}

		public virtual long GetJobFinishTime()
		{
			return jobFinishTime;
		}

		public virtual void SetJobFinishTime(long jobFinishTime)
		{
			this.jobFinishTime = jobFinishTime;
		}

		public virtual int GetNumFinishedMaps()
		{
			return numFinishedMaps;
		}

		public virtual void SetNumFinishedMaps(int numFinishedMaps)
		{
			this.numFinishedMaps = numFinishedMaps;
		}

		public virtual int GetNumFailedMaps()
		{
			return numFailedMaps;
		}

		public virtual void SetNumFailedMaps(int numFailedMaps)
		{
			this.numFailedMaps = numFailedMaps;
		}

		public virtual int GetResourcesPerMap()
		{
			return resourcesPerMap;
		}

		public virtual void SetResourcesPerMap(int resourcesPerMap)
		{
			this.resourcesPerMap = resourcesPerMap;
		}

		public virtual int GetNumFinishedReduces()
		{
			return numFinishedReduces;
		}

		public virtual void SetNumFinishedReduces(int numFinishedReduces)
		{
			this.numFinishedReduces = numFinishedReduces;
		}

		public virtual int GetNumFailedReduces()
		{
			return numFailedReduces;
		}

		public virtual void SetNumFailedReduces(int numFailedReduces)
		{
			this.numFailedReduces = numFailedReduces;
		}

		public virtual int GetResourcesPerReduce()
		{
			return this.resourcesPerReduce;
		}

		public virtual void SetResourcesPerReduce(int resourcesPerReduce)
		{
			this.resourcesPerReduce = resourcesPerReduce;
		}

		public virtual string GetUser()
		{
			return user;
		}

		public virtual void SetUser(string user)
		{
			this.user = user;
		}

		public virtual string GetQueue()
		{
			return queue;
		}

		public virtual void SetQueue(string queue)
		{
			this.queue = queue;
		}

		public virtual string GetJobStatus()
		{
			return jobStatus;
		}

		public virtual void SetJobStatus(string jobStatus)
		{
			this.jobStatus = jobStatus;
		}

		public virtual long GetMapSlotSeconds()
		{
			return mapSlotSeconds;
		}

		public virtual void SetMapSlotSeconds(long mapSlotSeconds)
		{
			this.mapSlotSeconds = mapSlotSeconds;
		}

		public virtual long GetReduceSlotSeconds()
		{
			return reduceSlotSeconds;
		}

		public virtual void SetReduceSlotSeconds(long reduceSlotSeconds)
		{
			this.reduceSlotSeconds = reduceSlotSeconds;
		}

		public virtual string GetJobName()
		{
			return jobName;
		}

		public virtual void SetJobName(string jobName)
		{
			this.jobName = jobName;
		}

		public virtual string GetJobSummaryString()
		{
			JobSummary.SummaryBuilder summary = new JobSummary.SummaryBuilder().Add("jobId", 
				jobId).Add("submitTime", jobSubmitTime).Add("launchTime", jobLaunchTime).Add("firstMapTaskLaunchTime"
				, firstMapTaskLaunchTime).Add("firstReduceTaskLaunchTime", firstReduceTaskLaunchTime
				).Add("finishTime", jobFinishTime).Add("resourcesPerMap", resourcesPerMap).Add("resourcesPerReduce"
				, resourcesPerReduce).Add("numMaps", numFinishedMaps + numFailedMaps).Add("numReduces"
				, numFinishedReduces + numFailedReduces).Add("user", user).Add("queue", queue).Add
				("status", jobStatus).Add("mapSlotSeconds", mapSlotSeconds).Add("reduceSlotSeconds"
				, reduceSlotSeconds).Add("jobName", jobName);
			return summary.ToString();
		}

		internal const char Equals = '=';

		internal static readonly char[] charsToEscape = new char[] { StringUtils.Comma, Equals
			, StringUtils.EscapeChar };

		internal class SummaryBuilder
		{
			internal readonly StringBuilder buffer = new StringBuilder();

			// A little optimization for a very common case
			internal virtual JobSummary.SummaryBuilder Add(string key, long value)
			{
				return _add(key, System.Convert.ToString(value));
			}

			internal virtual JobSummary.SummaryBuilder Add<T>(string key, T value)
			{
				string escapedString = StringUtils.EscapeString(value.ToString(), StringUtils.EscapeChar
					, charsToEscape).ReplaceAll("\n", "\\\\n").ReplaceAll("\r", "\\\\r");
				return _add(key, escapedString);
			}

			internal virtual JobSummary.SummaryBuilder Add(JobSummary.SummaryBuilder summary)
			{
				if (buffer.Length > 0)
				{
					buffer.Append(StringUtils.Comma);
				}
				buffer.Append(summary.buffer);
				return this;
			}

			internal virtual JobSummary.SummaryBuilder _add(string key, string value)
			{
				if (buffer.Length > 0)
				{
					buffer.Append(StringUtils.Comma);
				}
				buffer.Append(key).Append(Equals).Append(value);
				return this;
			}

			public override string ToString()
			{
				return buffer.ToString();
			}
		}
	}
}
