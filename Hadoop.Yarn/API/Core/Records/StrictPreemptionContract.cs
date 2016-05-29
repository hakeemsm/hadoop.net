using System.Collections.Generic;
using Org.Apache.Hadoop.Classification;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Records
{
	/// <summary>Enumeration of particular allocations to be reclaimed.</summary>
	/// <remarks>
	/// Enumeration of particular allocations to be reclaimed. The platform will
	/// reclaim exactly these resources, so the <code>ApplicationMaster</code> (AM)
	/// may attempt to checkpoint work or adjust its execution plan to accommodate
	/// it. In contrast to
	/// <see cref="PreemptionContract"/>
	/// , the AM has no flexibility in
	/// selecting which resources to return to the cluster.
	/// </remarks>
	/// <seealso cref="PreemptionMessage"/>
	public abstract class StrictPreemptionContract
	{
		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public static StrictPreemptionContract NewInstance(ICollection<PreemptionContainer
			> containers)
		{
			StrictPreemptionContract contract = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord
				<StrictPreemptionContract>();
			contract.SetContainers(containers);
			return contract;
		}

		/// <summary>
		/// Get the set of
		/// <see cref="PreemptionContainer"/>
		/// specifying containers owned by
		/// the <code>ApplicationMaster</code> that may be reclaimed by the
		/// <code>ResourceManager</code>.
		/// </summary>
		/// <returns>
		/// the set of
		/// <see cref="ContainerId"/>
		/// to be preempted.
		/// </returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Evolving]
		public abstract ICollection<PreemptionContainer> GetContainers();

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract void SetContainers(ICollection<PreemptionContainer> containers);
	}
}
