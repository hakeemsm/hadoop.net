using System.Collections.Generic;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmcontainer;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Common.Fica;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Capacity
{
	/// <summary>
	/// <code>CSQueue</code> represents a node in the tree of
	/// hierarchical queues in the
	/// <see cref="CapacityScheduler"/>
	/// .
	/// </summary>
	public interface CSQueue : Queue
	{
		/// <summary>Get the parent <code>Queue</code>.</summary>
		/// <returns>the parent queue</returns>
		CSQueue GetParent();

		/// <summary>Set the parent <code>Queue</code>.</summary>
		/// <param name="newParentQueue">new parent queue</param>
		void SetParent(CSQueue newParentQueue);

		/// <summary>Get the queue name.</summary>
		/// <returns>the queue name</returns>
		string GetQueueName();

		/// <summary>Get the full name of the queue, including the heirarchy.</summary>
		/// <returns>the full name of the queue</returns>
		string GetQueuePath();

		/// <summary>Get the configured <em>capacity</em> of the queue.</summary>
		/// <returns>configured queue capacity</returns>
		float GetCapacity();

		/// <summary>
		/// Get capacity of the parent of the queue as a function of the
		/// cumulative capacity in the cluster.
		/// </summary>
		/// <returns>
		/// capacity of the parent of the queue as a function of the
		/// cumulative capacity in the cluster
		/// </returns>
		float GetAbsoluteCapacity();

		/// <summary>Get the configured maximum-capacity of the queue.</summary>
		/// <returns>the configured maximum-capacity of the queue</returns>
		float GetMaximumCapacity();

		/// <summary>
		/// Get maximum-capacity of the queue as a funciton of the cumulative capacity
		/// of the cluster.
		/// </summary>
		/// <returns>
		/// maximum-capacity of the queue as a funciton of the cumulative capacity
		/// of the cluster
		/// </returns>
		float GetAbsoluteMaximumCapacity();

		/// <summary>
		/// Get the current absolute used capacity of the queue
		/// relative to the entire cluster.
		/// </summary>
		/// <returns>queue absolute used capacity</returns>
		float GetAbsoluteUsedCapacity();

		/// <summary>Set used capacity of the queue.</summary>
		/// <param name="usedCapacity">used capacity of the queue</param>
		void SetUsedCapacity(float usedCapacity);

		/// <summary>Set absolute used capacity of the queue.</summary>
		/// <param name="absUsedCapacity">absolute used capacity of the queue</param>
		void SetAbsoluteUsedCapacity(float absUsedCapacity);

		/// <summary>
		/// Get the current used capacity of nodes without label(s) of the queue
		/// and it's children (if any).
		/// </summary>
		/// <returns>queue used capacity</returns>
		float GetUsedCapacity();

		/// <summary>
		/// Get the currently utilized resources which allocated at nodes without any
		/// labels in the cluster by the queue and children (if any).
		/// </summary>
		/// <returns>used resources by the queue and it's children</returns>
		Resource GetUsedResources();

		/// <summary>Get the current run-state of the queue</summary>
		/// <returns>current run-state</returns>
		QueueState GetState();

		/// <summary>Get child queues</summary>
		/// <returns>child queues</returns>
		IList<CSQueue> GetChildQueues();

		/// <summary>Check if the <code>user</code> has permission to perform the operation</summary>
		/// <param name="acl">ACL</param>
		/// <param name="user">user</param>
		/// <returns>
		/// <code>true</code> if the user has the permission,
		/// <code>false</code> otherwise
		/// </returns>
		bool HasAccess(QueueACL acl, UserGroupInformation user);

		/// <summary>Submit a new application to the queue.</summary>
		/// <param name="applicationId">the applicationId of the application being submitted</param>
		/// <param name="user">user who submitted the application</param>
		/// <param name="queue">queue to which the application is submitted</param>
		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		void SubmitApplication(ApplicationId applicationId, string user, string queue);

		/// <summary>Submit an application attempt to the queue.</summary>
		void SubmitApplicationAttempt(FiCaSchedulerApp application, string userName);

		/// <summary>An application submitted to this queue has finished.</summary>
		/// <param name="applicationId"/>
		/// <param name="user">user who submitted the application</param>
		void FinishApplication(ApplicationId applicationId, string user);

		/// <summary>An application attempt submitted to this queue has finished.</summary>
		void FinishApplicationAttempt(FiCaSchedulerApp application, string queue);

		/// <summary>Assign containers to applications in the queue or it's children (if any).
		/// 	</summary>
		/// <param name="clusterResource">the resource of the cluster.</param>
		/// <param name="node">node on which resources are available</param>
		/// <param name="resourceLimits">how much overall resource of this queue can use.</param>
		/// <returns>the assignment</returns>
		CSAssignment AssignContainers(Resource clusterResource, FiCaSchedulerNode node, ResourceLimits
			 resourceLimits);

		/// <summary>A container assigned to the queue has completed.</summary>
		/// <param name="clusterResource">the resource of the cluster</param>
		/// <param name="application">application to which the container was assigned</param>
		/// <param name="node">node on which the container completed</param>
		/// <param name="container">
		/// completed container,
		/// <code>null</code> if it was just a reservation
		/// </param>
		/// <param name="containerStatus">
		/// <code>ContainerStatus</code> for the completed
		/// container
		/// </param>
		/// <param name="childQueue"><code>CSQueue</code> to reinsert in childQueues</param>
		/// <param name="event">event to be sent to the container</param>
		/// <param name="sortQueues">indicates whether it should re-sort the queues</param>
		void CompletedContainer(Resource clusterResource, FiCaSchedulerApp application, FiCaSchedulerNode
			 node, RMContainer container, ContainerStatus containerStatus, RMContainerEventType
			 @event, CSQueue childQueue, bool sortQueues);

		/// <summary>Get the number of applications in the queue.</summary>
		/// <returns>number of applications</returns>
		int GetNumApplications();

		/// <summary>Reinitialize the queue.</summary>
		/// <param name="newlyParsedQueue">new queue to re-initalize from</param>
		/// <param name="clusterResource">resources in the cluster</param>
		/// <exception cref="System.IO.IOException"/>
		void Reinitialize(CSQueue newlyParsedQueue, Resource clusterResource);

		/// <summary>Update the cluster resource for queues as we add/remove nodes</summary>
		/// <param name="clusterResource">the current cluster resource</param>
		/// <param name="resourceLimits">the current ResourceLimits</param>
		void UpdateClusterResource(Resource clusterResource, ResourceLimits resourceLimits
			);

		/// <summary>
		/// Get the
		/// <see cref="Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.ActiveUsersManager
		/// 	"/>
		/// for the queue.
		/// </summary>
		/// <returns>the <code>ActiveUsersManager</code> for the queue</returns>
		ActiveUsersManager GetActiveUsersManager();

		/// <summary>Adds all applications in the queue and its subqueues to the given collection.
		/// 	</summary>
		/// <param name="apps">the collection to add the applications to</param>
		void CollectSchedulerApplications(ICollection<ApplicationAttemptId> apps);

		/// <summary>Detach a container from this queue</summary>
		/// <param name="clusterResource">the current cluster resource</param>
		/// <param name="application">application to which the container was assigned</param>
		/// <param name="container">the container to detach</param>
		void DetachContainer(Resource clusterResource, FiCaSchedulerApp application, RMContainer
			 container);

		/// <summary>Attach a container to this queue</summary>
		/// <param name="clusterResource">the current cluster resource</param>
		/// <param name="application">application to which the container was assigned</param>
		/// <param name="container">the container to attach</param>
		void AttachContainer(Resource clusterResource, FiCaSchedulerApp application, RMContainer
			 container);

		/// <summary>Check whether <em>disable_preemption</em> property is set for this queue
		/// 	</summary>
		/// <returns>true if <em>disable_preemption</em> is set, false if not</returns>
		bool GetPreemptionDisabled();

		/// <summary>Get QueueCapacities of this queue</summary>
		/// <returns>queueCapacities</returns>
		QueueCapacities GetQueueCapacities();

		/// <summary>Get ResourceUsage of this queue</summary>
		/// <returns>resourceUsage</returns>
		ResourceUsage GetQueueResourceUsage();
	}
}
