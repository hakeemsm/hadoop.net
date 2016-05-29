using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Reservation
{
	/// <summary>
	/// This is the interface for policy that validate new
	/// <see cref="ReservationAllocation"/>
	/// s for allocations being added to a
	/// <see cref="Plan"/>
	/// .
	/// Individual policies will be enforcing different invariants.
	/// </summary>
	public interface SharingPolicy
	{
		/// <summary>Initialize this policy</summary>
		/// <param name="planQueuePath">the name of the queue for this plan</param>
		/// <param name="conf">the system configuration</param>
		void Init(string planQueuePath, ReservationSchedulerConfiguration conf);

		/// <summary>
		/// This method runs the policy validation logic, and return true/false on
		/// whether the
		/// <see cref="ReservationAllocation"/>
		/// is acceptable according to this
		/// sharing policy.
		/// </summary>
		/// <param name="plan">
		/// the
		/// <see cref="Plan"/>
		/// we validate against
		/// </param>
		/// <param name="newAllocation">
		/// the allocation proposed to be added to the
		/// <see cref="Plan"/>
		/// </param>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Reservation.Exceptions.PlanningException
		/// 	">
		/// if the policy is respected if we add this
		/// <see cref="ReservationAllocation"/>
		/// to the
		/// <see cref="Plan"/>
		/// </exception>
		void Validate(Plan plan, ReservationAllocation newAllocation);

		/// <summary>
		/// Returns the time range before and after the current reservation considered
		/// by this policy.
		/// </summary>
		/// <remarks>
		/// Returns the time range before and after the current reservation considered
		/// by this policy. In particular, this informs the archival process for the
		/// <see cref="Plan"/>
		/// , i.e., reservations regarding times before (now - validWindow)
		/// can be deleted.
		/// </remarks>
		/// <returns>validWindow the window of validity considered by the policy.</returns>
		long GetValidWindow();
	}
}
