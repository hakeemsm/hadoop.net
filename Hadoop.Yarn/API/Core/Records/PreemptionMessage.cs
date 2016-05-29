using Org.Apache.Hadoop.Classification;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Records
{
	/// <summary>
	/// A
	/// <see cref="PreemptionMessage"/>
	/// is part of the RM-AM protocol, and it is used by
	/// the RM to specify resources that the RM wants to reclaim from this
	/// <c>ApplicationMaster</c>
	/// (AM). The AM receives a
	/// <see cref="StrictPreemptionContract"/>
	/// message encoding which containers the platform may
	/// forcibly kill, granting it an opportunity to checkpoint state or adjust its
	/// execution plan. The message may also include a
	/// <see cref="PreemptionContract"/>
	/// granting the AM more latitude in selecting which resources to return to the
	/// cluster.
	/// <p>
	/// The AM should decode both parts of the message. The
	/// <see cref="StrictPreemptionContract"/>
	/// specifies particular allocations that the RM
	/// requires back. The AM can checkpoint containers' state, adjust its execution
	/// plan to move the computation, or take no action and hope that conditions that
	/// caused the RM to ask for the container will change.
	/// <p>
	/// In contrast, the
	/// <see cref="PreemptionContract"/>
	/// also includes a description of
	/// resources with a set of containers. If the AM releases containers matching
	/// that profile, then the containers enumerated in
	/// <see cref="PreemptionContract.GetContainers()"/>
	/// may not be killed.
	/// <p>
	/// Each preemption message reflects the RM's current understanding of the
	/// cluster state, so a request to return <em>N</em> containers may not
	/// reflect containers the AM is releasing, recently exited containers the RM has
	/// yet to learn about, or new containers allocated before the message was
	/// generated. Conversely, an RM may request a different profile of containers in
	/// subsequent requests.
	/// <p>
	/// The policy enforced by the RM is part of the scheduler. Generally, only
	/// containers that have been requested consistently should be killed, but the
	/// details are not specified.
	/// </summary>
	public abstract class PreemptionMessage
	{
		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public static PreemptionMessage NewInstance(StrictPreemptionContract set, PreemptionContract
			 contract)
		{
			PreemptionMessage message = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<PreemptionMessage
				>();
			message.SetStrictContract(set);
			message.SetContract(contract);
			return message;
		}

		/// <returns>
		/// Specific resources that may be killed by the
		/// <code>ResourceManager</code>
		/// </returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Evolving]
		public abstract StrictPreemptionContract GetStrictContract();

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract void SetStrictContract(StrictPreemptionContract set);

		/// <returns>Contract describing resources to return to the cluster.</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Evolving]
		public abstract PreemptionContract GetContract();

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract void SetContract(PreemptionContract contract);
	}
}
