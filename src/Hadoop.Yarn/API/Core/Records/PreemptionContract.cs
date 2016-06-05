using System.Collections.Generic;
using Org.Apache.Hadoop.Classification;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Records
{
	/// <summary>Description of resources requested back by the <code>ResourceManager</code>.
	/// 	</summary>
	/// <remarks>
	/// Description of resources requested back by the <code>ResourceManager</code>.
	/// The <code>ApplicationMaster</code> (AM) can satisfy this request according
	/// to its own priorities to prevent containers from being forcibly killed by
	/// the platform.
	/// </remarks>
	/// <seealso cref="PreemptionMessage"/>
	public abstract class PreemptionContract
	{
		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public static PreemptionContract NewInstance(IList<PreemptionResourceRequest> req
			, ICollection<PreemptionContainer> containers)
		{
			PreemptionContract contract = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<PreemptionContract
				>();
			contract.SetResourceRequest(req);
			contract.SetContainers(containers);
			return contract;
		}

		/// <summary>
		/// If the AM releases resources matching these requests, then the
		/// <see cref="PreemptionContainer"/>
		/// s enumerated in
		/// <see cref="GetContainers()"/>
		/// should not be
		/// evicted from the cluster. Due to delays in propagating cluster state and
		/// sending these messages, there are conditions where satisfied contracts may
		/// not prevent the platform from killing containers.
		/// </summary>
		/// <returns>
		/// List of
		/// <see cref="PreemptionResourceRequest"/>
		/// to update the
		/// <code>ApplicationMaster</code> about resources requested back by the
		/// <code>ResourceManager</code>.
		/// </returns>
		/// <seealso cref="Org.Apache.Hadoop.Yarn.Api.Protocolrecords.AllocateRequest.SetAskList(System.Collections.Generic.IList{E})
		/// 	"/>
		[InterfaceAudience.Public]
		[InterfaceStability.Evolving]
		public abstract IList<PreemptionResourceRequest> GetResourceRequest();

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract void SetResourceRequest(IList<PreemptionResourceRequest> req);

		/// <summary>
		/// Assign the set of
		/// <see cref="PreemptionContainer"/>
		/// specifying which containers
		/// owned by the <code>ApplicationMaster</code> that may be reclaimed by the
		/// <code>ResourceManager</code>. If the AM prefers a different set of
		/// containers, then it may checkpoint or kill containers matching the
		/// description in
		/// <see cref="GetResourceRequest()"/>
		/// .
		/// </summary>
		/// <returns>Set of containers at risk if the contract is not met.</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Evolving]
		public abstract ICollection<PreemptionContainer> GetContainers();

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract void SetContainers(ICollection<PreemptionContainer> containers);
	}
}
