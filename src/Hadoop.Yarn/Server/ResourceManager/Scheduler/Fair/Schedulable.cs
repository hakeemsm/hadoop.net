using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Resource;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmcontainer;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fair
{
	/// <summary>
	/// A Schedulable represents an entity that can be scheduled such as an
	/// application or a queue.
	/// </summary>
	/// <remarks>
	/// A Schedulable represents an entity that can be scheduled such as an
	/// application or a queue. It provides a common interface so that algorithms
	/// such as fair sharing can be applied both within a queue and across queues.
	/// A Schedulable is responsible for three roles:
	/// 1) Assign resources through
	/// <see cref="AssignContainer(FSSchedulerNode)"/>
	/// .
	/// 2) It provides information about the app/queue to the scheduler, including:
	/// - Demand (maximum number of tasks required)
	/// - Minimum share (for queues)
	/// - Job/queue weight (for fair sharing)
	/// - Start time and priority (for FIFO)
	/// 3) It can be assigned a fair share, for use with fair scheduling.
	/// Schedulable also contains two methods for performing scheduling computations:
	/// - updateDemand() is called periodically to compute the demand of the various
	/// jobs and queues, which may be expensive (e.g. jobs must iterate through all
	/// their tasks to count failed tasks, tasks that can be speculated, etc).
	/// - redistributeShare() is called after demands are updated and a Schedulable's
	/// fair share has been set by its parent to let it distribute its share among
	/// the other Schedulables within it (e.g. for queues that want to perform fair
	/// sharing among their jobs).
	/// </remarks>
	public interface Schedulable
	{
		/// <summary>
		/// Name of job/queue, used for debugging as well as for breaking ties in
		/// scheduling order deterministically.
		/// </summary>
		string GetName();

		/// <summary>Maximum number of resources required by this Schedulable.</summary>
		/// <remarks>
		/// Maximum number of resources required by this Schedulable. This is defined as
		/// number of currently utilized resources + number of unlaunched resources (that
		/// are either not yet launched or need to be speculated).
		/// </remarks>
		Resource GetDemand();

		/// <summary>Get the aggregate amount of resources consumed by the schedulable.</summary>
		Resource GetResourceUsage();

		/// <summary>Minimum Resource share assigned to the schedulable.</summary>
		Resource GetMinShare();

		/// <summary>Maximum Resource share assigned to the schedulable.</summary>
		Resource GetMaxShare();

		/// <summary>Job/queue weight in fair sharing.</summary>
		ResourceWeights GetWeights();

		/// <summary>Start time for jobs in FIFO queues; meaningless for QueueSchedulables.</summary>
		long GetStartTime();

		/// <summary>Job priority for jobs in FIFO queues; meaningless for QueueSchedulables.
		/// 	</summary>
		Priority GetPriority();

		/// <summary>Refresh the Schedulable's demand and those of its children if any.</summary>
		void UpdateDemand();

		/// <summary>
		/// Assign a container on this node if possible, and return the amount of
		/// resources assigned.
		/// </summary>
		Org.Apache.Hadoop.Yarn.Api.Records.Resource AssignContainer(FSSchedulerNode node);

		/// <summary>Preempt a container from this Schedulable if possible.</summary>
		RMContainer PreemptContainer();

		/// <summary>Get the fair share assigned to this Schedulable.</summary>
		Org.Apache.Hadoop.Yarn.Api.Records.Resource GetFairShare();

		/// <summary>Assign a fair share to this Schedulable.</summary>
		void SetFairShare(Org.Apache.Hadoop.Yarn.Api.Records.Resource fairShare);
	}
}
