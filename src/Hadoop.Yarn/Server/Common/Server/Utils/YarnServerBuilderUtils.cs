using System.Collections.Generic;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Factories;
using Org.Apache.Hadoop.Yarn.Factory.Providers;
using Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Server.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Utils
{
	/// <summary>Server Builder utilities to construct various objects.</summary>
	public class YarnServerBuilderUtils
	{
		private static readonly RecordFactory recordFactory = RecordFactoryProvider.GetRecordFactory
			(null);

		public static NodeHeartbeatResponse NewNodeHeartbeatResponse(int responseId, NodeAction
			 action, IList<ContainerId> containersToCleanUp, IList<ApplicationId> applicationsToCleanUp
			, MasterKey containerTokenMasterKey, MasterKey nmTokenMasterKey, long nextHeartbeatInterval
			)
		{
			NodeHeartbeatResponse response = recordFactory.NewRecordInstance<NodeHeartbeatResponse
				>();
			response.SetResponseId(responseId);
			response.SetNodeAction(action);
			response.SetContainerTokenMasterKey(containerTokenMasterKey);
			response.SetNMTokenMasterKey(nmTokenMasterKey);
			response.SetNextHeartBeatInterval(nextHeartbeatInterval);
			if (containersToCleanUp != null)
			{
				response.AddAllContainersToCleanup(containersToCleanUp);
			}
			if (applicationsToCleanUp != null)
			{
				response.AddAllApplicationsToCleanup(applicationsToCleanUp);
			}
			return response;
		}
	}
}
