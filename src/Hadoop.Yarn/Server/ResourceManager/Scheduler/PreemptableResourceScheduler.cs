using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmcontainer;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler
{
	/// <summary>Interface for a scheduler that supports preemption/killing</summary>
	public interface PreemptableResourceScheduler : ResourceScheduler
	{
		/// <summary>
		/// If the scheduler support container reservations, this method is used to
		/// ask the scheduler to drop the reservation for the given container.
		/// </summary>
		/// <param name="container">Reference to reserved container allocation.</param>
		void DropContainerReservation(RMContainer container);

		/// <summary>
		/// Ask the scheduler to obtain back the container from a specific application
		/// by issuing a preemption request
		/// </summary>
		/// <param name="aid">the application from which we want to get a container back</param>
		/// <param name="container">the container we want back</param>
		void PreemptContainer(ApplicationAttemptId aid, RMContainer container);

		/// <summary>Ask the scheduler to forcibly interrupt the container given as input</summary>
		/// <param name="container"/>
		void KillContainer(RMContainer container);
	}
}
