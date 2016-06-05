using System.Collections.Generic;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Reservation
{
	public interface Planner
	{
		/// <summary>
		/// Update the existing
		/// <see cref="Plan"/>
		/// , by adding/removing/updating existing
		/// reservations, and adding a subset of the reservation requests in the
		/// contracts parameter.
		/// </summary>
		/// <param name="plan">
		/// the
		/// <see cref="Plan"/>
		/// to replan
		/// </param>
		/// <param name="contracts">the list of reservation requests</param>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Reservation.Exceptions.PlanningException
		/// 	"/>
		void Plan(Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Reservation.Plan plan, IList
			<ReservationDefinition> contracts);

		/// <summary>Initialize the replanner</summary>
		/// <param name="planQueueName">the name of the queue for this plan</param>
		/// <param name="conf">the scheduler configuration</param>
		void Init(string planQueueName, ReservationSchedulerConfiguration conf);
	}
}
