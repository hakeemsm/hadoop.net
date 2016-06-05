using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	/// <summary>
	/// Class to encapsulate Queue ACLs for a particular
	/// user.
	/// </summary>
	internal class QueueAclsInfo : Org.Apache.Hadoop.Mapreduce.QueueAclsInfo
	{
		/// <summary>Default constructor for QueueAclsInfo.</summary>
		internal QueueAclsInfo()
			: base()
		{
		}

		/// <summary>
		/// Construct a new QueueAclsInfo object using the queue name and the
		/// queue operations array
		/// </summary>
		/// <param name="queueName">Name of the job queue</param>
		/// <param name="queue">operations</param>
		internal QueueAclsInfo(string queueName, string[] operations)
			: base(queueName, operations)
		{
		}

		public static Org.Apache.Hadoop.Mapred.QueueAclsInfo Downgrade(Org.Apache.Hadoop.Mapreduce.QueueAclsInfo
			 acl)
		{
			return new Org.Apache.Hadoop.Mapred.QueueAclsInfo(acl.GetQueueName(), acl.GetOperations
				());
		}
	}
}
