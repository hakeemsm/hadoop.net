using System.Collections.Generic;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Protocolrecords
{
	/// <summary>
	/// The response sent by the <code>ResourceManager</code> the
	/// <code>ApplicationMaster</code> during resource negotiation.
	/// </summary>
	/// <remarks>
	/// The response sent by the <code>ResourceManager</code> the
	/// <code>ApplicationMaster</code> during resource negotiation.
	/// <p>
	/// The response, includes:
	/// <ul>
	/// <li>Response ID to track duplicate responses.</li>
	/// <li>
	/// An AMCommand sent by ResourceManager to let the
	/// <c>ApplicationMaster</c>
	/// take some actions (resync, shutdown etc.).
	/// </li>
	/// <li>A list of newly allocated
	/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.Container"/>
	/// .</li>
	/// <li>A list of completed
	/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.Container"/>
	/// s' statuses.</li>
	/// <li>
	/// The available headroom for resources in the cluster for the
	/// application.
	/// </li>
	/// <li>A list of nodes whose status has been updated.</li>
	/// <li>The number of available nodes in a cluster.</li>
	/// <li>A description of resources requested back by the cluster</li>
	/// <li>AMRMToken, if AMRMToken has been rolled over</li>
	/// </ul>
	/// </remarks>
	/// <seealso cref="Org.Apache.Hadoop.Yarn.Api.ApplicationMasterProtocol.Allocate(AllocateRequest)
	/// 	"/>
	public abstract class AllocateResponse
	{
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public static AllocateResponse NewInstance(int responseId, IList<ContainerStatus>
			 completedContainers, IList<Container> allocatedContainers, IList<NodeReport> updatedNodes
			, Resource availResources, AMCommand command, int numClusterNodes, PreemptionMessage
			 preempt, IList<NMToken> nmTokens)
		{
			AllocateResponse response = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<AllocateResponse
				>();
			response.SetNumClusterNodes(numClusterNodes);
			response.SetResponseId(responseId);
			response.SetCompletedContainersStatuses(completedContainers);
			response.SetAllocatedContainers(allocatedContainers);
			response.SetUpdatedNodes(updatedNodes);
			response.SetAvailableResources(availResources);
			response.SetAMCommand(command);
			response.SetPreemptionMessage(preempt);
			response.SetNMTokens(nmTokens);
			return response;
		}

		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public static AllocateResponse NewInstance(int responseId, IList<ContainerStatus>
			 completedContainers, IList<Container> allocatedContainers, IList<NodeReport> updatedNodes
			, Resource availResources, AMCommand command, int numClusterNodes, PreemptionMessage
			 preempt, IList<NMToken> nmTokens, IList<ContainerResourceIncrease> increasedContainers
			, IList<ContainerResourceDecrease> decreasedContainers)
		{
			AllocateResponse response = NewInstance(responseId, completedContainers, allocatedContainers
				, updatedNodes, availResources, command, numClusterNodes, preempt, nmTokens);
			response.SetIncreasedContainers(increasedContainers);
			response.SetDecreasedContainers(decreasedContainers);
			return response;
		}

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public static AllocateResponse NewInstance(int responseId, IList<ContainerStatus>
			 completedContainers, IList<Container> allocatedContainers, IList<NodeReport> updatedNodes
			, Resource availResources, AMCommand command, int numClusterNodes, PreemptionMessage
			 preempt, IList<NMToken> nmTokens, Token amRMToken, IList<ContainerResourceIncrease
			> increasedContainers, IList<ContainerResourceDecrease> decreasedContainers)
		{
			AllocateResponse response = NewInstance(responseId, completedContainers, allocatedContainers
				, updatedNodes, availResources, command, numClusterNodes, preempt, nmTokens, increasedContainers
				, decreasedContainers);
			response.SetAMRMToken(amRMToken);
			return response;
		}

		/// <summary>
		/// If the <code>ResourceManager</code> needs the
		/// <code>ApplicationMaster</code> to take some action then it will send an
		/// AMCommand to the <code>ApplicationMaster</code>.
		/// </summary>
		/// <remarks>
		/// If the <code>ResourceManager</code> needs the
		/// <code>ApplicationMaster</code> to take some action then it will send an
		/// AMCommand to the <code>ApplicationMaster</code>. See <code>AMCommand</code>
		/// for details on commands and actions for them.
		/// </remarks>
		/// <returns>
		/// <code>AMCommand</code> if the <code>ApplicationMaster</code> should
		/// take action, <code>null</code> otherwise
		/// </returns>
		/// <seealso cref="Org.Apache.Hadoop.Yarn.Api.Records.AMCommand"/>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract AMCommand GetAMCommand();

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract void SetAMCommand(AMCommand command);

		/// <summary>Get the <em>last response id</em>.</summary>
		/// <returns><em>last response id</em></returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract int GetResponseId();

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract void SetResponseId(int responseId);

		/// <summary>
		/// Get the list of <em>newly allocated</em> <code>Container</code> by the
		/// <code>ResourceManager</code>.
		/// </summary>
		/// <returns>list of <em>newly allocated</em> <code>Container</code></returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract IList<Container> GetAllocatedContainers();

		/// <summary>
		/// Set the list of <em>newly allocated</em> <code>Container</code> by the
		/// <code>ResourceManager</code>.
		/// </summary>
		/// <param name="containers">list of <em>newly allocated</em> <code>Container</code></param>
		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract void SetAllocatedContainers(IList<Container> containers);

		/// <summary>
		/// Get the <em>available headroom</em> for resources in the cluster for the
		/// application.
		/// </summary>
		/// <returns>
		/// limit of available headroom for resources in the cluster for the
		/// application
		/// </returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract Resource GetAvailableResources();

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract void SetAvailableResources(Resource limit);

		/// <summary>Get the list of <em>completed containers' statuses</em>.</summary>
		/// <returns>the list of <em>completed containers' statuses</em></returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract IList<ContainerStatus> GetCompletedContainersStatuses();

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract void SetCompletedContainersStatuses(IList<ContainerStatus> containers
			);

		/// <summary>Get the list of <em>updated <code>NodeReport</code>s</em>.</summary>
		/// <remarks>
		/// Get the list of <em>updated <code>NodeReport</code>s</em>. Updates could
		/// be changes in health, availability etc of the nodes.
		/// </remarks>
		/// <returns>The delta of updated nodes since the last response</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract IList<NodeReport> GetUpdatedNodes();

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract void SetUpdatedNodes(IList<NodeReport> updatedNodes);

		/// <summary>Get the number of hosts available on the cluster.</summary>
		/// <returns>the available host count.</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract int GetNumClusterNodes();

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract void SetNumClusterNodes(int numNodes);

		/// <summary>
		/// Get the description of containers owned by the AM, but requested back by
		/// the cluster.
		/// </summary>
		/// <remarks>
		/// Get the description of containers owned by the AM, but requested back by
		/// the cluster. Note that the RM may have an inconsistent view of the
		/// resources owned by the AM. These messages are advisory, and the AM may
		/// elect to ignore them.
		/// <p>
		/// The message is a snapshot of the resources the RM wants back from the AM.
		/// While demand persists, the RM will repeat its request; applications should
		/// not interpret each message as a request for <em>additional</em>
		/// resources on top of previous messages. Resources requested consistently
		/// over some duration may be forcibly killed by the RM.
		/// </remarks>
		/// <returns>A specification of the resources to reclaim from this AM.</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Evolving]
		public abstract PreemptionMessage GetPreemptionMessage();

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract void SetPreemptionMessage(PreemptionMessage request);

		/// <summary>Get the list of NMTokens required for communicating with NM.</summary>
		/// <remarks>
		/// Get the list of NMTokens required for communicating with NM. New NMTokens
		/// issued only if
		/// <p>
		/// 1) AM is receiving first container on underlying NodeManager.<br />
		/// OR<br />
		/// 2) NMToken master key rolled over in ResourceManager and AM is getting new
		/// container on the same underlying NodeManager.
		/// <p>
		/// AM will receive one NMToken per NM irrespective of the number of containers
		/// issued on same NM. AM is expected to store these tokens until issued a
		/// new token for the same NM.
		/// </remarks>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract IList<NMToken> GetNMTokens();

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract void SetNMTokens(IList<NMToken> nmTokens);

		/// <summary>Get the list of newly increased containers by <code>ResourceManager</code>
		/// 	</summary>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract IList<ContainerResourceIncrease> GetIncreasedContainers();

		/// <summary>Set the list of newly increased containers by <code>ResourceManager</code>
		/// 	</summary>
		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract void SetIncreasedContainers(IList<ContainerResourceIncrease> increasedContainers
			);

		/// <summary>Get the list of newly decreased containers by <code>NodeManager</code></summary>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract IList<ContainerResourceDecrease> GetDecreasedContainers();

		/// <summary>Set the list of newly decreased containers by <code>NodeManager</code></summary>
		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract void SetDecreasedContainers(IList<ContainerResourceDecrease> decreasedContainers
			);

		/// <summary>The AMRMToken that belong to this attempt</summary>
		/// <returns>The AMRMToken that belong to this attempt</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract Token GetAMRMToken();

		[InterfaceAudience.Private]
		[InterfaceStability.Unstable]
		public abstract void SetAMRMToken(Token amRMToken);
	}
}
