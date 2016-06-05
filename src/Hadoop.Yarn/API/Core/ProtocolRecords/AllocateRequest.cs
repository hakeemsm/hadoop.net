using System.Collections.Generic;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Protocolrecords
{
	/// <summary>
	/// <p>The core request sent by the <code>ApplicationMaster</code> to the
	/// <code>ResourceManager</code> to obtain resources in the cluster.</p>
	/// <p>The request includes:
	/// <ul>
	/// <li>A response id to track duplicate responses.</li>
	/// <li>Progress information.</li>
	/// <li>
	/// A list of
	/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.ResourceRequest"/>
	/// to inform the
	/// <code>ResourceManager</code> about the application's
	/// resource requirements.
	/// </li>
	/// <li>
	/// A list of unused
	/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.Container"/>
	/// which are being returned.
	/// </li>
	/// </ul>
	/// </summary>
	/// <seealso cref="Org.Apache.Hadoop.Yarn.Api.ApplicationMasterProtocol.Allocate(AllocateRequest)
	/// 	"/>
	public abstract class AllocateRequest
	{
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public static AllocateRequest NewInstance(int responseID, float appProgress, IList
			<ResourceRequest> resourceAsk, IList<ContainerId> containersToBeReleased, ResourceBlacklistRequest
			 resourceBlacklistRequest)
		{
			return NewInstance(responseID, appProgress, resourceAsk, containersToBeReleased, 
				resourceBlacklistRequest, null);
		}

		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public static AllocateRequest NewInstance(int responseID, float appProgress, IList
			<ResourceRequest> resourceAsk, IList<ContainerId> containersToBeReleased, ResourceBlacklistRequest
			 resourceBlacklistRequest, IList<ContainerResourceIncreaseRequest> increaseRequests
			)
		{
			AllocateRequest allocateRequest = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<AllocateRequest
				>();
			allocateRequest.SetResponseId(responseID);
			allocateRequest.SetProgress(appProgress);
			allocateRequest.SetAskList(resourceAsk);
			allocateRequest.SetReleaseList(containersToBeReleased);
			allocateRequest.SetResourceBlacklistRequest(resourceBlacklistRequest);
			allocateRequest.SetIncreaseRequests(increaseRequests);
			return allocateRequest;
		}

		/// <summary>Get the <em>response id</em> used to track duplicate responses.</summary>
		/// <returns><em>response id</em></returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract int GetResponseId();

		/// <summary>Set the <em>response id</em> used to track duplicate responses.</summary>
		/// <param name="id"><em>response id</em></param>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract void SetResponseId(int id);

		/// <summary>Get the <em>current progress</em> of application.</summary>
		/// <returns><em>current progress</em> of application</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract float GetProgress();

		/// <summary>Set the <em>current progress</em> of application</summary>
		/// <param name="progress"><em>current progress</em> of application</param>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract void SetProgress(float progress);

		/// <summary>
		/// Get the list of <code>ResourceRequest</code> to update the
		/// <code>ResourceManager</code> about the application's resource requirements.
		/// </summary>
		/// <returns>the list of <code>ResourceRequest</code></returns>
		/// <seealso cref="Org.Apache.Hadoop.Yarn.Api.Records.ResourceRequest"/>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract IList<ResourceRequest> GetAskList();

		/// <summary>
		/// Set list of <code>ResourceRequest</code> to update the
		/// <code>ResourceManager</code> about the application's resource requirements.
		/// </summary>
		/// <param name="resourceRequests">
		/// list of <code>ResourceRequest</code> to update the
		/// <code>ResourceManager</code> about the application's
		/// resource requirements
		/// </param>
		/// <seealso cref="Org.Apache.Hadoop.Yarn.Api.Records.ResourceRequest"/>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract void SetAskList(IList<ResourceRequest> resourceRequests);

		/// <summary>
		/// Get the list of <code>ContainerId</code> of containers being
		/// released by the <code>ApplicationMaster</code>.
		/// </summary>
		/// <returns>
		/// list of <code>ContainerId</code> of containers being
		/// released by the <code>ApplicationMaster</code>
		/// </returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract IList<ContainerId> GetReleaseList();

		/// <summary>
		/// Set the list of <code>ContainerId</code> of containers being
		/// released by the <code>ApplicationMaster</code>
		/// </summary>
		/// <param name="releaseContainers">
		/// list of <code>ContainerId</code> of
		/// containers being released by the
		/// <code>ApplicationMaster</code>
		/// </param>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract void SetReleaseList(IList<ContainerId> releaseContainers);

		/// <summary>
		/// Get the <code>ResourceBlacklistRequest</code> being sent by the
		/// <code>ApplicationMaster</code>.
		/// </summary>
		/// <returns>
		/// the <code>ResourceBlacklistRequest</code> being sent by the
		/// <code>ApplicationMaster</code>
		/// </returns>
		/// <seealso cref="Org.Apache.Hadoop.Yarn.Api.Records.ResourceBlacklistRequest"/>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract ResourceBlacklistRequest GetResourceBlacklistRequest();

		/// <summary>
		/// Set the <code>ResourceBlacklistRequest</code> to inform the
		/// <code>ResourceManager</code> about the blacklist additions and removals
		/// per the <code>ApplicationMaster</code>.
		/// </summary>
		/// <param name="resourceBlacklistRequest">
		/// the <code>ResourceBlacklistRequest</code>
		/// to inform the <code>ResourceManager</code> about
		/// the blacklist additions and removals
		/// per the <code>ApplicationMaster</code>
		/// </param>
		/// <seealso cref="Org.Apache.Hadoop.Yarn.Api.Records.ResourceBlacklistRequest"/>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract void SetResourceBlacklistRequest(ResourceBlacklistRequest resourceBlacklistRequest
			);

		/// <summary>
		/// Get the <code>ContainerResourceIncreaseRequest</code> being sent by the
		/// <code>ApplicationMaster</code>
		/// </summary>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract IList<ContainerResourceIncreaseRequest> GetIncreaseRequests();

		/// <summary>
		/// Set the <code>ContainerResourceIncreaseRequest</code> to inform the
		/// <code>ResourceManager</code> about some container's resources need to be
		/// increased
		/// </summary>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract void SetIncreaseRequests(IList<ContainerResourceIncreaseRequest> 
			increaseRequests);
	}
}
