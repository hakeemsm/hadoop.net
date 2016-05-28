using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	/// <summary>
	/// Enum representing an AccessControlList that drives set of operations that
	/// can be performed on a queue.
	/// </summary>
	[System.Serializable]
	public sealed class QueueACL
	{
		public static readonly Org.Apache.Hadoop.Mapred.QueueACL SubmitJob = new Org.Apache.Hadoop.Mapred.QueueACL
			("acl-submit-job");

		public static readonly Org.Apache.Hadoop.Mapred.QueueACL AdministerJobs = new Org.Apache.Hadoop.Mapred.QueueACL
			("acl-administer-jobs");

		private readonly string aclName;

		internal QueueACL(string aclName)
		{
			// Currently this ACL acl-administer-jobs is checked for the operations
			// FAIL_TASK, KILL_TASK, KILL_JOB, SET_JOB_PRIORITY and VIEW_JOB.
			// TODO: Add ACL for LIST_JOBS when we have ability to authenticate
			//       users in UI
			// TODO: Add ACL for CHANGE_ACL when we have an admin tool for
			//       configuring queues.
			this.aclName = aclName;
		}

		public string GetAclName()
		{
			return Org.Apache.Hadoop.Mapred.QueueACL.aclName;
		}
	}
}
