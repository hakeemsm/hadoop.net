using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Reservation
{
	/// <summary>
	/// A Plan represents the central data structure of a reservation system that
	/// maintains the "agenda" for the cluster.
	/// </summary>
	/// <remarks>
	/// A Plan represents the central data structure of a reservation system that
	/// maintains the "agenda" for the cluster. In particular, it maintains
	/// information on how a set of
	/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ReservationDefinition"/>
	/// that have been
	/// previously accepted will be honored.
	/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ReservationDefinition"/>
	/// submitted by the users through the RM public
	/// APIs are passed to appropriate
	/// <see cref="ReservationAgent"/>
	/// s, which in turn will
	/// consult the Plan (via the
	/// <see cref="PlanView"/>
	/// interface) and try to determine
	/// whether there are sufficient resources available in this Plan to satisfy the
	/// temporal and resource constraints of a
	/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ReservationDefinition"/>
	/// . If a
	/// valid allocation is found the agent will try to store it in the plan (via the
	/// <see cref="PlanEdit"/>
	/// interface). Upon success the system return to the user a
	/// positive acknowledgment, and a reservation identifier to be later used to
	/// access the reserved resources.
	/// A
	/// <see cref="PlanFollower"/>
	/// will continuously read from the Plan and will
	/// affect the instantaneous allocation of resources among jobs running by
	/// publishing the "current" slice of the Plan to the underlying scheduler. I.e.,
	/// the configuration of queues/weights of the scheduler are modified to reflect
	/// the allocations in the Plan.
	/// As this interface have several methods we decompose them into three groups:
	/// <see cref="PlanContext"/>
	/// : containing configuration type information,
	/// <see cref="PlanView"/>
	/// read-only access to the plan state, and
	/// <see cref="PlanEdit"/>
	/// write access to the plan state.
	/// </remarks>
	public interface Plan : PlanContext, PlanView, PlanEdit
	{
	}
}
