using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Text;
using Com.Google.Common.Annotations;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Token;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn.Api;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Client;
using Org.Apache.Hadoop.Yarn.Event;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Ipc;
using Org.Apache.Hadoop.Yarn.Security;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp.Attempt;
using Org.Apache.Hadoop.Yarn.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Amlauncher
{
	/// <summary>The launch of the AM itself.</summary>
	public class AMLauncher : Runnable
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Amlauncher.AMLauncher
			));

		private ContainerManagementProtocol containerMgrProxy;

		private readonly RMAppAttempt application;

		private readonly Configuration conf;

		private readonly AMLauncherEventType eventType;

		private readonly RMContext rmContext;

		private readonly Container masterContainer;

		private readonly EventHandler handler;

		public AMLauncher(RMContext rmContext, RMAppAttempt application, AMLauncherEventType
			 eventType, Configuration conf)
		{
			this.application = application;
			this.conf = conf;
			this.eventType = eventType;
			this.rmContext = rmContext;
			this.handler = rmContext.GetDispatcher().GetEventHandler();
			this.masterContainer = application.GetMasterContainer();
		}

		/// <exception cref="System.IO.IOException"/>
		private void Connect()
		{
			ContainerId masterContainerID = masterContainer.GetId();
			containerMgrProxy = GetContainerMgrProxy(masterContainerID);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		private void Launch()
		{
			Connect();
			ContainerId masterContainerID = masterContainer.GetId();
			ApplicationSubmissionContext applicationContext = application.GetSubmissionContext
				();
			Log.Info("Setting up container " + masterContainer + " for AM " + application.GetAppAttemptId
				());
			ContainerLaunchContext launchContext = CreateAMContainerLaunchContext(applicationContext
				, masterContainerID);
			StartContainerRequest scRequest = StartContainerRequest.NewInstance(launchContext
				, masterContainer.GetContainerToken());
			IList<StartContainerRequest> list = new AList<StartContainerRequest>();
			list.AddItem(scRequest);
			StartContainersRequest allRequests = StartContainersRequest.NewInstance(list);
			StartContainersResponse response = containerMgrProxy.StartContainers(allRequests);
			if (response.GetFailedRequests() != null && response.GetFailedRequests().Contains
				(masterContainerID))
			{
				Exception t = response.GetFailedRequests()[masterContainerID].DeSerialize();
				ParseAndThrowException(t);
			}
			else
			{
				Log.Info("Done launching container " + masterContainer + " for AM " + application
					.GetAppAttemptId());
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		private void Cleanup()
		{
			Connect();
			ContainerId containerId = masterContainer.GetId();
			IList<ContainerId> containerIds = new AList<ContainerId>();
			containerIds.AddItem(containerId);
			StopContainersRequest stopRequest = StopContainersRequest.NewInstance(containerIds
				);
			StopContainersResponse response = containerMgrProxy.StopContainers(stopRequest);
			if (response.GetFailedRequests() != null && response.GetFailedRequests().Contains
				(containerId))
			{
				Exception t = response.GetFailedRequests()[containerId].DeSerialize();
				ParseAndThrowException(t);
			}
		}

		// Protected. For tests.
		protected internal virtual ContainerManagementProtocol GetContainerMgrProxy(ContainerId
			 containerId)
		{
			NodeId node = masterContainer.GetNodeId();
			IPEndPoint containerManagerConnectAddress = NetUtils.CreateSocketAddrForHost(node
				.GetHost(), node.GetPort());
			YarnRPC rpc = GetYarnRPC();
			UserGroupInformation currentUser = UserGroupInformation.CreateRemoteUser(containerId
				.GetApplicationAttemptId().ToString());
			string user = rmContext.GetRMApps()[containerId.GetApplicationAttemptId().GetApplicationId
				()].GetUser();
			Token token = rmContext.GetNMTokenSecretManager().CreateNMToken(containerId.GetApplicationAttemptId
				(), node, user);
			currentUser.AddToken(ConverterUtils.ConvertFromYarn(token, containerManagerConnectAddress
				));
			return NMProxy.CreateNMProxy<ContainerManagementProtocol>(conf, currentUser, rpc, 
				containerManagerConnectAddress);
		}

		[VisibleForTesting]
		protected internal virtual YarnRPC GetYarnRPC()
		{
			return YarnRPC.Create(conf);
		}

		// TODO: Don't create again and again.
		/// <exception cref="System.IO.IOException"/>
		private ContainerLaunchContext CreateAMContainerLaunchContext(ApplicationSubmissionContext
			 applicationMasterContext, ContainerId containerID)
		{
			// Construct the actual Container
			ContainerLaunchContext container = applicationMasterContext.GetAMContainerSpec();
			Log.Info("Command to launch container " + containerID + " : " + StringUtils.ArrayToString
				(Sharpen.Collections.ToArray(container.GetCommands(), new string[0])));
			// Finalize the container
			SetupTokens(container, containerID);
			return container;
		}

		/// <exception cref="System.IO.IOException"/>
		private void SetupTokens(ContainerLaunchContext container, ContainerId containerID
			)
		{
			IDictionary<string, string> environment = container.GetEnvironment();
			environment[ApplicationConstants.ApplicationWebProxyBaseEnv] = application.GetWebProxyBase
				();
			// Set AppSubmitTime and MaxAppAttempts to be consumable by the AM.
			ApplicationId applicationId = application.GetAppAttemptId().GetApplicationId();
			environment[ApplicationConstants.AppSubmitTimeEnv] = rmContext.GetRMApps()[applicationId
				].GetSubmitTime().ToString();
			environment[ApplicationConstants.MaxAppAttemptsEnv] = rmContext.GetRMApps()[applicationId
				].GetMaxAppAttempts().ToString();
			Credentials credentials = new Credentials();
			DataInputByteBuffer dibb = new DataInputByteBuffer();
			if (container.GetTokens() != null)
			{
				// TODO: Don't do this kind of checks everywhere.
				dibb.Reset(container.GetTokens());
				credentials.ReadTokenStorageStream(dibb);
			}
			// Add AMRMToken
			Org.Apache.Hadoop.Security.Token.Token<AMRMTokenIdentifier> amrmToken = CreateAndSetAMRMToken
				();
			if (amrmToken != null)
			{
				credentials.AddToken(amrmToken.GetService(), amrmToken);
			}
			DataOutputBuffer dob = new DataOutputBuffer();
			credentials.WriteTokenStorageToStream(dob);
			container.SetTokens(ByteBuffer.Wrap(dob.GetData(), 0, dob.GetLength()));
		}

		[VisibleForTesting]
		protected internal virtual Org.Apache.Hadoop.Security.Token.Token<AMRMTokenIdentifier
			> CreateAndSetAMRMToken()
		{
			Org.Apache.Hadoop.Security.Token.Token<AMRMTokenIdentifier> amrmToken = this.rmContext
				.GetAMRMTokenSecretManager().CreateAndGetAMRMToken(application.GetAppAttemptId()
				);
			((RMAppAttemptImpl)application).SetAMRMToken(amrmToken);
			return amrmToken;
		}

		public virtual void Run()
		{
			switch (eventType)
			{
				case AMLauncherEventType.Launch:
				{
					try
					{
						Log.Info("Launching master" + application.GetAppAttemptId());
						Launch();
						handler.Handle(new RMAppAttemptEvent(application.GetAppAttemptId(), RMAppAttemptEventType
							.Launched));
					}
					catch (Exception ie)
					{
						string message = "Error launching " + application.GetAppAttemptId() + ". Got exception: "
							 + StringUtils.StringifyException(ie);
						Log.Info(message);
						handler.Handle(new RMAppAttemptEvent(application.GetAppAttemptId(), RMAppAttemptEventType
							.LaunchFailed, message));
					}
					break;
				}

				case AMLauncherEventType.Cleanup:
				{
					try
					{
						Log.Info("Cleaning master " + application.GetAppAttemptId());
						Cleanup();
					}
					catch (IOException ie)
					{
						Log.Info("Error cleaning master ", ie);
					}
					catch (YarnException e)
					{
						StringBuilder sb = new StringBuilder("Container ");
						sb.Append(masterContainer.GetId().ToString());
						sb.Append(" is not handled by this NodeManager");
						if (!e.Message.Contains(sb.ToString()))
						{
							// Ignoring if container is already killed by Node Manager.
							Log.Info("Error cleaning master ", e);
						}
					}
					break;
				}

				default:
				{
					Log.Warn("Received unknown event-type " + eventType + ". Ignoring.");
					break;
				}
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		private void ParseAndThrowException(Exception t)
		{
			if (t is YarnException)
			{
				throw (YarnException)t;
			}
			else
			{
				if (t is SecretManager.InvalidToken)
				{
					throw (SecretManager.InvalidToken)t;
				}
				else
				{
					throw (IOException)t;
				}
			}
		}
	}
}
