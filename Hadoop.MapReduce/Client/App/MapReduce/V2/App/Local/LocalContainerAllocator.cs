using System;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Mapreduce.V2.App;
using Org.Apache.Hadoop.Mapreduce.V2.App.Client;
using Org.Apache.Hadoop.Mapreduce.V2.App.Job.Event;
using Org.Apache.Hadoop.Mapreduce.V2.App.RM;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Client;
using Org.Apache.Hadoop.Yarn.Event;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Factories;
using Org.Apache.Hadoop.Yarn.Factory.Providers;
using Org.Apache.Hadoop.Yarn.Security;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App.Local
{
	/// <summary>Allocates containers locally.</summary>
	/// <remarks>
	/// Allocates containers locally. Doesn't allocate a real container;
	/// instead sends an allocated event for all requests.
	/// </remarks>
	public class LocalContainerAllocator : RMCommunicator, ContainerAllocator
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Mapreduce.V2.App.Local.LocalContainerAllocator
			));

		private readonly EventHandler eventHandler;

		private long retryInterval;

		private long retrystartTime;

		private string nmHost;

		private int nmPort;

		private int nmHttpPort;

		private ContainerId containerId;

		protected internal int lastResponseID;

		private readonly RecordFactory recordFactory = RecordFactoryProvider.GetRecordFactory
			(null);

		public LocalContainerAllocator(ClientService clientService, AppContext context, string
			 nmHost, int nmPort, int nmHttpPort, ContainerId cId)
			: base(clientService, context)
		{
			this.eventHandler = context.GetEventHandler();
			this.nmHost = nmHost;
			this.nmPort = nmPort;
			this.nmHttpPort = nmHttpPort;
			this.containerId = cId;
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceInit(Configuration conf)
		{
			base.ServiceInit(conf);
			retryInterval = GetConfig().GetLong(MRJobConfig.MrAmToRmWaitIntervalMs, MRJobConfig
				.DefaultMrAmToRmWaitIntervalMs);
			// Init startTime to current time. If all goes well, it will be reset after
			// first attempt to contact RM.
			retrystartTime = Runtime.CurrentTimeMillis();
		}

		/// <exception cref="System.Exception"/>
		protected internal override void Heartbeat()
		{
			lock (this)
			{
				AllocateRequest allocateRequest = AllocateRequest.NewInstance(this.lastResponseID
					, base.GetApplicationProgress(), new AList<ResourceRequest>(), new AList<ContainerId
					>(), null);
				AllocateResponse allocateResponse = null;
				try
				{
					allocateResponse = scheduler.Allocate(allocateRequest);
					// Reset retry count if no exception occurred.
					retrystartTime = Runtime.CurrentTimeMillis();
				}
				catch (ApplicationAttemptNotFoundException e)
				{
					Log.Info("Event from RM: shutting down Application Master");
					// This can happen if the RM has been restarted. If it is in that state,
					// this application must clean itself up.
					eventHandler.Handle(new JobEvent(this.GetJob().GetID(), JobEventType.JobAmReboot)
						);
					throw new YarnRuntimeException("Resource Manager doesn't recognize AttemptId: " +
						 this.GetContext().GetApplicationID(), e);
				}
				catch (ApplicationMasterNotRegisteredException)
				{
					Log.Info("ApplicationMaster is out of sync with ResourceManager," + " hence resync and send outstanding requests."
						);
					this.lastResponseID = 0;
					Register();
				}
				catch (Exception e)
				{
					// This can happen when the connection to the RM has gone down. Keep
					// re-trying until the retryInterval has expired.
					if (Runtime.CurrentTimeMillis() - retrystartTime >= retryInterval)
					{
						Log.Error("Could not contact RM after " + retryInterval + " milliseconds.");
						eventHandler.Handle(new JobEvent(this.GetJob().GetID(), JobEventType.InternalError
							));
						throw new YarnRuntimeException("Could not contact RM after " + retryInterval + " milliseconds."
							);
					}
					// Throw this up to the caller, which may decide to ignore it and
					// continue to attempt to contact the RM.
					throw;
				}
				if (allocateResponse != null)
				{
					this.lastResponseID = allocateResponse.GetResponseId();
					Token token = allocateResponse.GetAMRMToken();
					if (token != null)
					{
						UpdateAMRMToken(token);
					}
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void UpdateAMRMToken(Token token)
		{
			Org.Apache.Hadoop.Security.Token.Token<AMRMTokenIdentifier> amrmToken = new Org.Apache.Hadoop.Security.Token.Token
				<AMRMTokenIdentifier>(((byte[])token.GetIdentifier().Array()), ((byte[])token.GetPassword
				().Array()), new Text(token.GetKind()), new Text(token.GetService()));
			UserGroupInformation currentUGI = UserGroupInformation.GetCurrentUser();
			currentUGI.AddToken(amrmToken);
			amrmToken.SetService(ClientRMProxy.GetAMRMTokenService(GetConfig()));
		}

		public virtual void Handle(ContainerAllocatorEvent @event)
		{
			if (@event.GetType() == ContainerAllocator.EventType.ContainerReq)
			{
				Log.Info("Processing the event " + @event.ToString());
				// Assign the same container ID as the AM
				ContainerId cID = ContainerId.NewContainerId(GetContext().GetApplicationAttemptId
					(), this.containerId.GetContainerId());
				Container container = recordFactory.NewRecordInstance<Container>();
				container.SetId(cID);
				NodeId nodeId = NodeId.NewInstance(this.nmHost, this.nmPort);
				container.SetNodeId(nodeId);
				container.SetContainerToken(null);
				container.SetNodeHttpAddress(this.nmHost + ":" + this.nmHttpPort);
				// send the container-assigned event to task attempt
				if (@event.GetAttemptID().GetTaskId().GetTaskType() == TaskType.Map)
				{
					JobCounterUpdateEvent jce = new JobCounterUpdateEvent(@event.GetAttemptID().GetTaskId
						().GetJobId());
					// TODO Setting OTHER_LOCAL_MAP for now.
					jce.AddCounterUpdate(JobCounter.OtherLocalMaps, 1);
					eventHandler.Handle(jce);
				}
				eventHandler.Handle(new TaskAttemptContainerAssignedEvent(@event.GetAttemptID(), 
					container, applicationACLs));
			}
		}
	}
}
