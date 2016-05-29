using System.Collections.Generic;
using System.Net;
using Com.Google.Common.Annotations;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Factories;
using Org.Apache.Hadoop.Yarn.Factory.Providers;
using Org.Apache.Hadoop.Yarn.Security;
using Org.Apache.Hadoop.Yarn.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Utils
{
	/// <summary>Builder utilities to construct various objects.</summary>
	public class BuilderUtils
	{
		private static readonly RecordFactory recordFactory = RecordFactoryProvider.GetRecordFactory
			(null);

		[System.Serializable]
		public class ApplicationIdComparator : IComparer<ApplicationId>
		{
			public virtual int Compare(ApplicationId a1, ApplicationId a2)
			{
				return a1.CompareTo(a2);
			}
		}

		[System.Serializable]
		public class ContainerIdComparator : IComparer<ContainerId>
		{
			public virtual int Compare(ContainerId c1, ContainerId c2)
			{
				return c1.CompareTo(c2);
			}
		}

		public static LocalResource NewLocalResource(URL url, LocalResourceType type, LocalResourceVisibility
			 visibility, long size, long timestamp, bool shouldBeUploadedToSharedCache)
		{
			LocalResource resource = recordFactory.NewRecordInstance<LocalResource>();
			resource.SetResource(url);
			resource.SetType(type);
			resource.SetVisibility(visibility);
			resource.SetSize(size);
			resource.SetTimestamp(timestamp);
			resource.SetShouldBeUploadedToSharedCache(shouldBeUploadedToSharedCache);
			return resource;
		}

		public static LocalResource NewLocalResource(URI uri, LocalResourceType type, LocalResourceVisibility
			 visibility, long size, long timestamp, bool shouldBeUploadedToSharedCache)
		{
			return NewLocalResource(ConverterUtils.GetYarnUrlFromURI(uri), type, visibility, 
				size, timestamp, shouldBeUploadedToSharedCache);
		}

		public static ApplicationId NewApplicationId(RecordFactory recordFactory, long clustertimestamp
			, CharSequence id)
		{
			return ApplicationId.NewInstance(clustertimestamp, System.Convert.ToInt32(id.ToString
				()));
		}

		public static ApplicationId NewApplicationId(RecordFactory recordFactory, long clusterTimeStamp
			, int id)
		{
			return ApplicationId.NewInstance(clusterTimeStamp, id);
		}

		public static ApplicationId NewApplicationId(long clusterTimeStamp, int id)
		{
			return ApplicationId.NewInstance(clusterTimeStamp, id);
		}

		public static ApplicationAttemptId NewApplicationAttemptId(ApplicationId appId, int
			 attemptId)
		{
			return ApplicationAttemptId.NewInstance(appId, attemptId);
		}

		public static ApplicationId Convert(long clustertimestamp, CharSequence id)
		{
			return ApplicationId.NewInstance(clustertimestamp, System.Convert.ToInt32(id.ToString
				()));
		}

		public static ContainerId NewContainerId(ApplicationAttemptId appAttemptId, long 
			containerId)
		{
			return ContainerId.NewContainerId(appAttemptId, containerId);
		}

		public static ContainerId NewContainerId(int appId, int appAttemptId, long timestamp
			, long id)
		{
			ApplicationId applicationId = NewApplicationId(timestamp, appId);
			ApplicationAttemptId applicationAttemptId = NewApplicationAttemptId(applicationId
				, appAttemptId);
			ContainerId cId = NewContainerId(applicationAttemptId, id);
			return cId;
		}

		/// <exception cref="System.IO.IOException"/>
		public static Token NewContainerToken(ContainerId cId, string host, int port, string
			 user, Resource r, long expiryTime, int masterKeyId, byte[] password, long rmIdentifier
			)
		{
			ContainerTokenIdentifier identifier = new ContainerTokenIdentifier(cId, host + ":"
				 + port, user, r, expiryTime, masterKeyId, rmIdentifier, Priority.NewInstance(0)
				, 0);
			return NewContainerToken(BuilderUtils.NewNodeId(host, port), password, identifier
				);
		}

		public static ContainerId NewContainerId(RecordFactory recordFactory, ApplicationId
			 appId, ApplicationAttemptId appAttemptId, int containerId)
		{
			return ContainerId.NewContainerId(appAttemptId, containerId);
		}

		public static NodeId NewNodeId(string host, int port)
		{
			return NodeId.NewInstance(host, port);
		}

		public static NodeReport NewNodeReport(NodeId nodeId, NodeState nodeState, string
			 httpAddress, string rackName, Resource used, Resource capability, int numContainers
			, string healthReport, long lastHealthReportTime)
		{
			return NewNodeReport(nodeId, nodeState, httpAddress, rackName, used, capability, 
				numContainers, healthReport, lastHealthReportTime, null);
		}

		public static NodeReport NewNodeReport(NodeId nodeId, NodeState nodeState, string
			 httpAddress, string rackName, Resource used, Resource capability, int numContainers
			, string healthReport, long lastHealthReportTime, ICollection<string> nodeLabels
			)
		{
			NodeReport nodeReport = recordFactory.NewRecordInstance<NodeReport>();
			nodeReport.SetNodeId(nodeId);
			nodeReport.SetNodeState(nodeState);
			nodeReport.SetHttpAddress(httpAddress);
			nodeReport.SetRackName(rackName);
			nodeReport.SetUsed(used);
			nodeReport.SetCapability(capability);
			nodeReport.SetNumContainers(numContainers);
			nodeReport.SetHealthReport(healthReport);
			nodeReport.SetLastHealthReportTime(lastHealthReportTime);
			nodeReport.SetNodeLabels(nodeLabels);
			return nodeReport;
		}

		public static ContainerStatus NewContainerStatus(ContainerId containerId, ContainerState
			 containerState, string diagnostics, int exitStatus)
		{
			ContainerStatus containerStatus = recordFactory.NewRecordInstance<ContainerStatus
				>();
			containerStatus.SetState(containerState);
			containerStatus.SetContainerId(containerId);
			containerStatus.SetDiagnostics(diagnostics);
			containerStatus.SetExitStatus(exitStatus);
			return containerStatus;
		}

		public static Container NewContainer(ContainerId containerId, NodeId nodeId, string
			 nodeHttpAddress, Resource resource, Priority priority, Token containerToken)
		{
			Container container = recordFactory.NewRecordInstance<Container>();
			container.SetId(containerId);
			container.SetNodeId(nodeId);
			container.SetNodeHttpAddress(nodeHttpAddress);
			container.SetResource(resource);
			container.SetPriority(priority);
			container.SetContainerToken(containerToken);
			return container;
		}

		public static T NewToken<T>(byte[] identifier, string kind, byte[] password, string
			 service)
			where T : Token
		{
			System.Type tokenClass = typeof(T);
			T token = recordFactory.NewRecordInstance(tokenClass);
			token.SetIdentifier(ByteBuffer.Wrap(identifier));
			token.SetKind(kind);
			token.SetPassword(ByteBuffer.Wrap(password));
			token.SetService(service);
			return token;
		}

		public static Token NewDelegationToken(byte[] identifier, string kind, byte[] password
			, string service)
		{
			return NewToken<Token>(identifier, kind, password, service);
		}

		public static Token NewClientToAMToken(byte[] identifier, string kind, byte[] password
			, string service)
		{
			return NewToken<Token>(identifier, kind, password, service);
		}

		public static Token NewAMRMToken(byte[] identifier, string kind, byte[] password, 
			string service)
		{
			return NewToken<Token>(identifier, kind, password, service);
		}

		[VisibleForTesting]
		public static Token NewContainerToken(NodeId nodeId, byte[] password, ContainerTokenIdentifier
			 tokenIdentifier)
		{
			// RPC layer client expects ip:port as service for tokens
			IPEndPoint addr = NetUtils.CreateSocketAddrForHost(nodeId.GetHost(), nodeId.GetPort
				());
			// NOTE: use SecurityUtil.setTokenService if this becomes a "real" token
			Token containerToken = NewToken<Token>(tokenIdentifier.GetBytes(), ContainerTokenIdentifier
				.Kind.ToString(), password, SecurityUtil.BuildTokenService(addr).ToString());
			return containerToken;
		}

		/// <exception cref="System.IO.IOException"/>
		public static ContainerTokenIdentifier NewContainerTokenIdentifier(Token containerToken
			)
		{
			Org.Apache.Hadoop.Security.Token.Token<ContainerTokenIdentifier> token = new Org.Apache.Hadoop.Security.Token.Token
				<ContainerTokenIdentifier>(((byte[])containerToken.GetIdentifier().Array()), ((byte
				[])containerToken.GetPassword().Array()), new Text(containerToken.GetKind()), new 
				Text(containerToken.GetService()));
			return token.DecodeIdentifier();
		}

		public static ContainerLaunchContext NewContainerLaunchContext(IDictionary<string
			, LocalResource> localResources, IDictionary<string, string> environment, IList<
			string> commands, IDictionary<string, ByteBuffer> serviceData, ByteBuffer tokens
			, IDictionary<ApplicationAccessType, string> acls)
		{
			ContainerLaunchContext container = recordFactory.NewRecordInstance<ContainerLaunchContext
				>();
			container.SetLocalResources(localResources);
			container.SetEnvironment(environment);
			container.SetCommands(commands);
			container.SetServiceData(serviceData);
			container.SetTokens(tokens);
			container.SetApplicationACLs(acls);
			return container;
		}

		public static Priority NewPriority(int p)
		{
			Priority priority = recordFactory.NewRecordInstance<Priority>();
			priority.SetPriority(p);
			return priority;
		}

		public static ResourceRequest NewResourceRequest(Priority priority, string hostName
			, Resource capability, int numContainers)
		{
			ResourceRequest request = recordFactory.NewRecordInstance<ResourceRequest>();
			request.SetPriority(priority);
			request.SetResourceName(hostName);
			request.SetCapability(capability);
			request.SetNumContainers(numContainers);
			return request;
		}

		public static ResourceRequest NewResourceRequest(ResourceRequest r)
		{
			ResourceRequest request = recordFactory.NewRecordInstance<ResourceRequest>();
			request.SetPriority(r.GetPriority());
			request.SetResourceName(r.GetResourceName());
			request.SetCapability(r.GetCapability());
			request.SetNumContainers(r.GetNumContainers());
			return request;
		}

		public static ApplicationReport NewApplicationReport(ApplicationId applicationId, 
			ApplicationAttemptId applicationAttemptId, string user, string queue, string name
			, string host, int rpcPort, Org.Apache.Hadoop.Yarn.Api.Records.Token clientToAMToken
			, YarnApplicationState state, string diagnostics, string url, long startTime, long
			 finishTime, FinalApplicationStatus finalStatus, ApplicationResourceUsageReport 
			appResources, string origTrackingUrl, float progress, string appType, Org.Apache.Hadoop.Yarn.Api.Records.Token
			 amRmToken, ICollection<string> tags)
		{
			ApplicationReport report = recordFactory.NewRecordInstance<ApplicationReport>();
			report.SetApplicationId(applicationId);
			report.SetCurrentApplicationAttemptId(applicationAttemptId);
			report.SetUser(user);
			report.SetQueue(queue);
			report.SetName(name);
			report.SetHost(host);
			report.SetRpcPort(rpcPort);
			report.SetClientToAMToken(clientToAMToken);
			report.SetYarnApplicationState(state);
			report.SetDiagnostics(diagnostics);
			report.SetTrackingUrl(url);
			report.SetStartTime(startTime);
			report.SetFinishTime(finishTime);
			report.SetFinalApplicationStatus(finalStatus);
			report.SetApplicationResourceUsageReport(appResources);
			report.SetOriginalTrackingUrl(origTrackingUrl);
			report.SetProgress(progress);
			report.SetApplicationType(appType);
			report.SetAMRMToken(amRmToken);
			report.SetApplicationTags(tags);
			return report;
		}

		public static ApplicationSubmissionContext NewApplicationSubmissionContext(ApplicationId
			 applicationId, string applicationName, string queue, Priority priority, ContainerLaunchContext
			 amContainer, bool isUnmanagedAM, bool cancelTokensWhenComplete, int maxAppAttempts
			, Resource resource, string applicationType)
		{
			ApplicationSubmissionContext context = recordFactory.NewRecordInstance<ApplicationSubmissionContext
				>();
			context.SetApplicationId(applicationId);
			context.SetApplicationName(applicationName);
			context.SetQueue(queue);
			context.SetPriority(priority);
			context.SetAMContainerSpec(amContainer);
			context.SetUnmanagedAM(isUnmanagedAM);
			context.SetCancelTokensWhenComplete(cancelTokensWhenComplete);
			context.SetMaxAppAttempts(maxAppAttempts);
			context.SetResource(resource);
			context.SetApplicationType(applicationType);
			return context;
		}

		public static ApplicationSubmissionContext NewApplicationSubmissionContext(ApplicationId
			 applicationId, string applicationName, string queue, Priority priority, ContainerLaunchContext
			 amContainer, bool isUnmanagedAM, bool cancelTokensWhenComplete, int maxAppAttempts
			, Resource resource)
		{
			return NewApplicationSubmissionContext(applicationId, applicationName, queue, priority
				, amContainer, isUnmanagedAM, cancelTokensWhenComplete, maxAppAttempts, resource
				, null);
		}

		public static ApplicationResourceUsageReport NewApplicationResourceUsageReport(int
			 numUsedContainers, int numReservedContainers, Resource usedResources, Resource 
			reservedResources, Resource neededResources, long memorySeconds, long vcoreSeconds
			)
		{
			ApplicationResourceUsageReport report = recordFactory.NewRecordInstance<ApplicationResourceUsageReport
				>();
			report.SetNumUsedContainers(numUsedContainers);
			report.SetNumReservedContainers(numReservedContainers);
			report.SetUsedResources(usedResources);
			report.SetReservedResources(reservedResources);
			report.SetNeededResources(neededResources);
			report.SetMemorySeconds(memorySeconds);
			report.SetVcoreSeconds(vcoreSeconds);
			return report;
		}

		public static Resource NewResource(int memory, int vCores)
		{
			Resource resource = recordFactory.NewRecordInstance<Resource>();
			resource.SetMemory(memory);
			resource.SetVirtualCores(vCores);
			return resource;
		}

		public static URL NewURL(string scheme, string host, int port, string file)
		{
			URL url = recordFactory.NewRecordInstance<URL>();
			url.SetScheme(scheme);
			url.SetHost(host);
			url.SetPort(port);
			url.SetFile(file);
			return url;
		}

		public static AllocateResponse NewAllocateResponse(int responseId, IList<ContainerStatus
			> completedContainers, IList<Container> allocatedContainers, IList<NodeReport> updatedNodes
			, Resource availResources, AMCommand command, int numClusterNodes, PreemptionMessage
			 preempt)
		{
			AllocateResponse response = recordFactory.NewRecordInstance<AllocateResponse>();
			response.SetNumClusterNodes(numClusterNodes);
			response.SetResponseId(responseId);
			response.SetCompletedContainersStatuses(completedContainers);
			response.SetAllocatedContainers(allocatedContainers);
			response.SetUpdatedNodes(updatedNodes);
			response.SetAvailableResources(availResources);
			response.SetAMCommand(command);
			response.SetPreemptionMessage(preempt);
			return response;
		}
	}
}
