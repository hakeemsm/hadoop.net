using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager
{
	public interface NodeStatusUpdater : Org.Apache.Hadoop.Service.Service
	{
		/// <summary>
		/// Schedule a heartbeat to the ResourceManager outside of the normal,
		/// periodic heartbeating process.
		/// </summary>
		/// <remarks>
		/// Schedule a heartbeat to the ResourceManager outside of the normal,
		/// periodic heartbeating process. This is typically called when the state
		/// of containers on the node has changed to notify the RM sooner.
		/// </remarks>
		void SendOutofBandHeartBeat();

		/// <summary>Get the ResourceManager identifier received during registration</summary>
		/// <returns>the ResourceManager ID</returns>
		long GetRMIdentifier();

		/// <summary>Query if a container has recently completed</summary>
		/// <param name="containerId">the container ID</param>
		/// <returns>true if the container has recently completed</returns>
		bool IsContainerRecentlyStopped(ContainerId containerId);

		/// <summary>Add a container to the list of containers that have recently completed</summary>
		/// <param name="containerId">the ID of the completed container</param>
		void AddCompletedContainer(ContainerId containerId);

		/// <summary>Clear the list of recently completed containers</summary>
		void ClearFinishedContainersFromCache();
	}
}
