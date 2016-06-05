using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.Jobhistory
{
	/// <summary>
	/// Maintains information which may be used by the jobHistory indexing
	/// system.
	/// </summary>
	public class JobIndexInfo
	{
		private long submitTime;

		private long finishTime;

		private string user;

		private string queueName;

		private string jobName;

		private JobId jobId;

		private int numMaps;

		private int numReduces;

		private string jobStatus;

		private long jobStartTime;

		public JobIndexInfo()
		{
		}

		public JobIndexInfo(long submitTime, long finishTime, string user, string jobName
			, JobId jobId, int numMaps, int numReduces, string jobStatus)
			: this(submitTime, finishTime, user, jobName, jobId, numMaps, numReduces, jobStatus
				, JobConf.DefaultQueueName)
		{
		}

		public JobIndexInfo(long submitTime, long finishTime, string user, string jobName
			, JobId jobId, int numMaps, int numReduces, string jobStatus, string queueName)
		{
			this.submitTime = submitTime;
			this.finishTime = finishTime;
			this.user = user;
			this.jobName = jobName;
			this.jobId = jobId;
			this.numMaps = numMaps;
			this.numReduces = numReduces;
			this.jobStatus = jobStatus;
			this.jobStartTime = -1;
			this.queueName = queueName;
		}

		public virtual long GetSubmitTime()
		{
			return submitTime;
		}

		public virtual void SetSubmitTime(long submitTime)
		{
			this.submitTime = submitTime;
		}

		public virtual long GetFinishTime()
		{
			return finishTime;
		}

		public virtual void SetFinishTime(long finishTime)
		{
			this.finishTime = finishTime;
		}

		public virtual string GetUser()
		{
			return user;
		}

		public virtual void SetUser(string user)
		{
			this.user = user;
		}

		public virtual string GetQueueName()
		{
			return queueName;
		}

		public virtual void SetQueueName(string queueName)
		{
			this.queueName = queueName;
		}

		public virtual string GetJobName()
		{
			return jobName;
		}

		public virtual void SetJobName(string jobName)
		{
			this.jobName = jobName;
		}

		public virtual JobId GetJobId()
		{
			return jobId;
		}

		public virtual void SetJobId(JobId jobId)
		{
			this.jobId = jobId;
		}

		public virtual int GetNumMaps()
		{
			return numMaps;
		}

		public virtual void SetNumMaps(int numMaps)
		{
			this.numMaps = numMaps;
		}

		public virtual int GetNumReduces()
		{
			return numReduces;
		}

		public virtual void SetNumReduces(int numReduces)
		{
			this.numReduces = numReduces;
		}

		public virtual string GetJobStatus()
		{
			return jobStatus;
		}

		public virtual void SetJobStatus(string jobStatus)
		{
			this.jobStatus = jobStatus;
		}

		public virtual long GetJobStartTime()
		{
			return jobStartTime;
		}

		public virtual void SetJobStartTime(long lTime)
		{
			this.jobStartTime = lTime;
		}

		public override string ToString()
		{
			return "JobIndexInfo [submitTime=" + submitTime + ", finishTime=" + finishTime + 
				", user=" + user + ", jobName=" + jobName + ", jobId=" + jobId + ", numMaps=" + 
				numMaps + ", numReduces=" + numReduces + ", jobStatus=" + jobStatus + "]";
		}
	}
}
