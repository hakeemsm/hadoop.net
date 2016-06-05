using Org.Apache.Hadoop.Mapreduce;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	/// <summary>
	/// Generic operation that maps to the dependent set of ACLs that drive the
	/// authorization of the operation.
	/// </summary>
	[System.Serializable]
	public sealed class Operation
	{
		public static readonly Org.Apache.Hadoop.Mapred.Operation ViewJobCounters = new Org.Apache.Hadoop.Mapred.Operation
			(QueueACL.AdministerJobs, JobACL.ViewJob);

		public static readonly Org.Apache.Hadoop.Mapred.Operation ViewJobDetails = new Org.Apache.Hadoop.Mapred.Operation
			(QueueACL.AdministerJobs, JobACL.ViewJob);

		public static readonly Org.Apache.Hadoop.Mapred.Operation ViewTaskLogs = new Org.Apache.Hadoop.Mapred.Operation
			(QueueACL.AdministerJobs, JobACL.ViewJob);

		public static readonly Org.Apache.Hadoop.Mapred.Operation KillJob = new Org.Apache.Hadoop.Mapred.Operation
			(QueueACL.AdministerJobs, JobACL.ModifyJob);

		public static readonly Org.Apache.Hadoop.Mapred.Operation FailTask = new Org.Apache.Hadoop.Mapred.Operation
			(QueueACL.AdministerJobs, JobACL.ModifyJob);

		public static readonly Org.Apache.Hadoop.Mapred.Operation KillTask = new Org.Apache.Hadoop.Mapred.Operation
			(QueueACL.AdministerJobs, JobACL.ModifyJob);

		public static readonly Org.Apache.Hadoop.Mapred.Operation SetJobPriority = new Org.Apache.Hadoop.Mapred.Operation
			(QueueACL.AdministerJobs, JobACL.ModifyJob);

		public static readonly Org.Apache.Hadoop.Mapred.Operation SubmitJob = new Org.Apache.Hadoop.Mapred.Operation
			(QueueACL.SubmitJob, null);

		public QueueACL qACLNeeded;

		public JobACL jobACLNeeded;

		internal Operation(QueueACL qACL, JobACL jobACL)
		{
			this.qACLNeeded = qACL;
			this.jobACLNeeded = jobACL;
		}
	}
}
