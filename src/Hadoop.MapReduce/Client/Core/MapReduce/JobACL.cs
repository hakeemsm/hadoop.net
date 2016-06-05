using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce
{
	/// <summary>Job related ACLs</summary>
	[System.Serializable]
	public sealed class JobACL
	{
		/// <summary>ACL for 'viewing' job.</summary>
		/// <remarks>
		/// ACL for 'viewing' job. Dictates who can 'view' some or all of the job
		/// related details.
		/// </remarks>
		public static readonly Org.Apache.Hadoop.Mapreduce.JobACL ViewJob = new Org.Apache.Hadoop.Mapreduce.JobACL
			(MRJobConfig.JobAclViewJob);

		/// <summary>ACL for 'modifying' job.</summary>
		/// <remarks>
		/// ACL for 'modifying' job. Dictates who can 'modify' the job for e.g., by
		/// killing the job, killing/failing a task of the job or setting priority of
		/// the job.
		/// </remarks>
		public static readonly Org.Apache.Hadoop.Mapreduce.JobACL ModifyJob = new Org.Apache.Hadoop.Mapreduce.JobACL
			(MRJobConfig.JobAclModifyJob);

		internal string aclName;

		internal JobACL(string name)
		{
			this.aclName = name;
		}

		/// <summary>Get the name of the ACL.</summary>
		/// <remarks>
		/// Get the name of the ACL. Here it is same as the name of the configuration
		/// property for specifying the ACL for the job.
		/// </remarks>
		/// <returns>aclName</returns>
		public string GetAclName()
		{
			return Org.Apache.Hadoop.Mapreduce.JobACL.aclName;
		}
	}
}
