using System.Collections.Generic;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmcontainer;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler
{
	public interface Queue
	{
		/// <summary>Get the queue name</summary>
		/// <returns>queue name</returns>
		string GetQueueName();

		/// <summary>Get the queue metrics</summary>
		/// <returns>the queue metrics</returns>
		QueueMetrics GetMetrics();

		/// <summary>Get queue information</summary>
		/// <param name="includeChildQueues">include child queues?</param>
		/// <param name="recursive">recursively get child queue information?</param>
		/// <returns>queue information</returns>
		QueueInfo GetQueueInfo(bool includeChildQueues, bool recursive);

		/// <summary>Get queue ACLs for given <code>user</code>.</summary>
		/// <param name="user">username</param>
		/// <returns>queue ACLs for user</returns>
		IList<QueueUserACLInfo> GetQueueUserAclInfo(UserGroupInformation user);

		bool HasAccess(QueueACL acl, UserGroupInformation user);

		ActiveUsersManager GetActiveUsersManager();

		/// <summary>Recover the state of the queue for a given container.</summary>
		/// <param name="clusterResource">the resource of the cluster</param>
		/// <param name="schedulerAttempt">the application for which the container was allocated
		/// 	</param>
		/// <param name="rmContainer">the container that was recovered.</param>
		void RecoverContainer(Resource clusterResource, SchedulerApplicationAttempt schedulerAttempt
			, RMContainer rmContainer);

		/// <summary>
		/// Get labels can be accessed of this queue
		/// labels={*}, means this queue can access any label
		/// labels={ }, means this queue cannot access any label except node without label
		/// labels={a, b, c} means this queue can access a or b or c
		/// </summary>
		/// <returns>labels</returns>
		ICollection<string> GetAccessibleNodeLabels();

		/// <summary>Get default label expression of this queue.</summary>
		/// <remarks>
		/// Get default label expression of this queue. If label expression of
		/// ApplicationSubmissionContext and label expression of Resource Request not
		/// set, this will be used.
		/// </remarks>
		/// <returns>default label expression</returns>
		string GetDefaultNodeLabelExpression();
	}
}
