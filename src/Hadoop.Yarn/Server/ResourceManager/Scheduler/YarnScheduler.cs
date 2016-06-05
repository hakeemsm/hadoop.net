using System.Collections.Generic;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Event;
using Org.Apache.Hadoop.Yarn.Proto;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmcontainer;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Common;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Event;
using Org.Apache.Hadoop.Yarn.Util.Resource;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler
{
	/// <summary>
	/// This interface is used by the components to talk to the
	/// scheduler for allocating of resources, cleaning up resources.
	/// </summary>
	public interface YarnScheduler : EventHandler<SchedulerEvent>
	{
		/// <summary>Get queue information</summary>
		/// <param name="queueName">queue name</param>
		/// <param name="includeChildQueues">include child queues?</param>
		/// <param name="recursive">get children queues?</param>
		/// <returns>queue information</returns>
		/// <exception cref="System.IO.IOException"/>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		QueueInfo GetQueueInfo(string queueName, bool includeChildQueues, bool recursive);

		/// <summary>Get acls for queues for current user.</summary>
		/// <returns>acls for queues for current user</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		IList<QueueUserACLInfo> GetQueueUserAclInfo();

		/// <summary>Get the whole resource capacity of the cluster.</summary>
		/// <returns>the whole resource capacity of the cluster.</returns>
		[InterfaceStability.Unstable]
		Resource GetClusterResource();

		/// <summary>
		/// Get minimum allocatable
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.Resource"/>
		/// .
		/// </summary>
		/// <returns>minimum allocatable resource</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		Resource GetMinimumResourceCapability();

		/// <summary>
		/// Get maximum allocatable
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.Resource"/>
		/// at the cluster level.
		/// </summary>
		/// <returns>maximum allocatable resource</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		Resource GetMaximumResourceCapability();

		/// <summary>
		/// Get maximum allocatable
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.Resource"/>
		/// for the queue specified.
		/// </summary>
		/// <param name="queueName">queue name</param>
		/// <returns>maximum allocatable resource</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		Resource GetMaximumResourceCapability(string queueName);

		[InterfaceStability.Evolving]
		ResourceCalculator GetResourceCalculator();

		/// <summary>Get the number of nodes available in the cluster.</summary>
		/// <returns>the number of available nodes.</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		int GetNumClusterNodes();

		/// <summary>The main api between the ApplicationMaster and the Scheduler.</summary>
		/// <remarks>
		/// The main api between the ApplicationMaster and the Scheduler.
		/// The ApplicationMaster is updating his future resource requirements
		/// and may release containers he doens't need.
		/// </remarks>
		/// <param name="appAttemptId"/>
		/// <param name="ask"/>
		/// <param name="release"/>
		/// <param name="blacklistAdditions"></param>
		/// <param name="blacklistRemovals"></param>
		/// <returns>
		/// the
		/// <see cref="Allocation"/>
		/// for the application
		/// </returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		Allocation Allocate(ApplicationAttemptId appAttemptId, IList<ResourceRequest> ask
			, IList<ContainerId> release, IList<string> blacklistAdditions, IList<string> blacklistRemovals
			);

		/// <summary>Get node resource usage report.</summary>
		/// <param name="nodeId"/>
		/// <returns>
		/// the
		/// <see cref="SchedulerNodeReport"/>
		/// for the node or null
		/// if nodeId does not point to a defined node.
		/// </returns>
		[InterfaceStability.Stable]
		SchedulerNodeReport GetNodeReport(NodeId nodeId);

		/// <summary>Get the Scheduler app for a given app attempt Id.</summary>
		/// <param name="appAttemptId">the id of the application attempt</param>
		/// <returns>SchedulerApp for this given attempt.</returns>
		[InterfaceStability.Stable]
		SchedulerAppReport GetSchedulerAppInfo(ApplicationAttemptId appAttemptId);

		/// <summary>Get a resource usage report from a given app attempt ID.</summary>
		/// <param name="appAttemptId">the id of the application attempt</param>
		/// <returns>resource usage report for this given attempt</returns>
		[InterfaceStability.Evolving]
		ApplicationResourceUsageReport GetAppResourceUsageReport(ApplicationAttemptId appAttemptId
			);

		/// <summary>Get the root queue for the scheduler.</summary>
		/// <returns>the root queue for the scheduler.</returns>
		[InterfaceStability.Evolving]
		QueueMetrics GetRootQueueMetrics();

		/// <summary>Check if the user has permission to perform the operation.</summary>
		/// <remarks>
		/// Check if the user has permission to perform the operation.
		/// If the user has
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.QueueACL.AdministerQueue"/>
		/// permission,
		/// this user can view/modify the applications in this queue
		/// </remarks>
		/// <param name="callerUGI"/>
		/// <param name="acl"/>
		/// <param name="queueName"/>
		/// <returns>
		/// <code>true</code> if the user has the permission,
		/// <code>false</code> otherwise
		/// </returns>
		bool CheckAccess(UserGroupInformation callerUGI, QueueACL acl, string queueName);

		/// <summary>Gets the apps under a given queue</summary>
		/// <param name="queueName">the name of the queue.</param>
		/// <returns>a collection of app attempt ids in the given queue.</returns>
		[InterfaceStability.Stable]
		IList<ApplicationAttemptId> GetAppsInQueue(string queueName);

		/// <summary>Get the container for the given containerId.</summary>
		/// <param name="containerId"/>
		/// <returns>the container for the given containerId.</returns>
		[InterfaceStability.Unstable]
		RMContainer GetRMContainer(ContainerId containerId);

		/// <summary>Moves the given application to the given queue</summary>
		/// <param name="appId"/>
		/// <param name="newQueue"/>
		/// <returns>the name of the queue the application was placed into</returns>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException">if the move cannot be carried out
		/// 	</exception>
		[InterfaceStability.Evolving]
		string MoveApplication(ApplicationId appId, string newQueue);

		/// <summary>
		/// Completely drain sourceQueue of applications, by moving all of them to
		/// destQueue.
		/// </summary>
		/// <param name="sourceQueue"/>
		/// <param name="destQueue"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		void MoveAllApps(string sourceQueue, string destQueue);

		/// <summary>Terminate all applications in the specified queue.</summary>
		/// <param name="queueName">the name of queue to be drained</param>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		void KillAllAppsInQueue(string queueName);

		/// <summary>Remove an existing queue.</summary>
		/// <remarks>
		/// Remove an existing queue. Implementations might limit when a queue could be
		/// removed (e.g., must have zero entitlement, and no applications running, or
		/// must be a leaf, etc..).
		/// </remarks>
		/// <param name="queueName">name of the queue to remove</param>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		void RemoveQueue(string queueName);

		/// <summary>Add to the scheduler a new Queue.</summary>
		/// <remarks>
		/// Add to the scheduler a new Queue. Implementations might limit what type of
		/// queues can be dynamically added (e.g., Queue must be a leaf, must be
		/// attached to existing parent, must have zero entitlement).
		/// </remarks>
		/// <param name="newQueue">the queue being added.</param>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		void AddQueue(Queue newQueue);

		/// <summary>
		/// This method increase the entitlement for current queue (must respect
		/// invariants, e.g., no overcommit of parents, non negative, etc.).
		/// </summary>
		/// <remarks>
		/// This method increase the entitlement for current queue (must respect
		/// invariants, e.g., no overcommit of parents, non negative, etc.).
		/// Entitlement is a general term for weights in FairScheduler, capacity for
		/// the CapacityScheduler, etc.
		/// </remarks>
		/// <param name="queue">the queue for which we change entitlement</param>
		/// <param name="entitlement">
		/// the new entitlement for the queue (capacity,
		/// maxCapacity, etc..)
		/// </param>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		void SetEntitlement(string queue, QueueEntitlement entitlement);

		/// <summary>Gets the list of names for queues managed by the Reservation System</summary>
		/// <returns>the list of queues which support reservations</returns>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		ICollection<string> GetPlanQueues();

		/// <summary>
		/// Return a collection of the resource types that are considered when
		/// scheduling
		/// </summary>
		/// <returns>an EnumSet containing the resource types</returns>
		EnumSet<YarnServiceProtos.SchedulerResourceTypes> GetSchedulingResourceTypes();
	}
}
