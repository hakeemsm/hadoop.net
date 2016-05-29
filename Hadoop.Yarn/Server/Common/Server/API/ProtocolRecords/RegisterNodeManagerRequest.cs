using System.Collections.Generic;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords
{
	public abstract class RegisterNodeManagerRequest
	{
		public static RegisterNodeManagerRequest NewInstance(NodeId nodeId, int httpPort, 
			Resource resource, string nodeManagerVersionId, IList<NMContainerStatus> containerStatuses
			, IList<ApplicationId> runningApplications)
		{
			RegisterNodeManagerRequest request = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord
				<RegisterNodeManagerRequest>();
			request.SetHttpPort(httpPort);
			request.SetResource(resource);
			request.SetNodeId(nodeId);
			request.SetNMVersion(nodeManagerVersionId);
			request.SetContainerStatuses(containerStatuses);
			request.SetRunningApplications(runningApplications);
			return request;
		}

		public abstract NodeId GetNodeId();

		public abstract int GetHttpPort();

		public abstract Resource GetResource();

		public abstract string GetNMVersion();

		public abstract IList<NMContainerStatus> GetNMContainerStatuses();

		/// <summary>
		/// We introduce this here because currently YARN RM doesn't persist nodes info
		/// for application running.
		/// </summary>
		/// <remarks>
		/// We introduce this here because currently YARN RM doesn't persist nodes info
		/// for application running. When RM restart happened, we cannot determinate if
		/// a node should do application cleanup (like log-aggregation, status update,
		/// etc.) or not.
		/// <p>
		/// When we have this running application list in node manager register
		/// request, we can recover nodes info for running applications. And then we
		/// can take actions accordingly
		/// </remarks>
		/// <returns>running application list in this node</returns>
		public abstract IList<ApplicationId> GetRunningApplications();

		public abstract void SetNodeId(NodeId nodeId);

		public abstract void SetHttpPort(int port);

		public abstract void SetResource(Resource resource);

		public abstract void SetNMVersion(string version);

		public abstract void SetContainerStatuses(IList<NMContainerStatus> containerStatuses
			);

		/// <summary>
		/// Setter for
		/// <see cref="GetRunningApplications()"/>
		/// </summary>
		/// <param name="runningApplications">running application in this node</param>
		public abstract void SetRunningApplications(IList<ApplicationId> runningApplications
			);
	}
}
