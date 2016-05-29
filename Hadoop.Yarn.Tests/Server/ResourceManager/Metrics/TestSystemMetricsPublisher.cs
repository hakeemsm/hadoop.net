using System.Text;
using NUnit.Framework;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Api.Records.Timeline;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Server.Applicationhistoryservice;
using Org.Apache.Hadoop.Yarn.Server.Metrics;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp.Attempt;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmcontainer;
using Org.Apache.Hadoop.Yarn.Server.Timeline;
using Org.Apache.Hadoop.Yarn.Server.Timeline.Recovery;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Metrics
{
	public class TestSystemMetricsPublisher
	{
		private static ApplicationHistoryServer timelineServer;

		private static SystemMetricsPublisher metricsPublisher;

		private static TimelineStore store;

		/// <exception cref="System.Exception"/>
		[BeforeClass]
		public static void Setup()
		{
			YarnConfiguration conf = new YarnConfiguration();
			conf.SetBoolean(YarnConfiguration.TimelineServiceEnabled, true);
			conf.SetBoolean(YarnConfiguration.RmSystemMetricsPublisherEnabled, true);
			conf.SetClass(YarnConfiguration.TimelineServiceStore, typeof(MemoryTimelineStore)
				, typeof(TimelineStore));
			conf.SetClass(YarnConfiguration.TimelineServiceStateStoreClass, typeof(MemoryTimelineStateStore
				), typeof(TimelineStateStore));
			conf.SetInt(YarnConfiguration.RmSystemMetricsPublisherDispatcherPoolSize, 2);
			timelineServer = new ApplicationHistoryServer();
			timelineServer.Init(conf);
			timelineServer.Start();
			store = timelineServer.GetTimelineStore();
			metricsPublisher = new SystemMetricsPublisher();
			metricsPublisher.Init(conf);
			metricsPublisher.Start();
		}

		/// <exception cref="System.Exception"/>
		[AfterClass]
		public static void TearDown()
		{
			if (metricsPublisher != null)
			{
				metricsPublisher.Stop();
			}
			if (timelineServer != null)
			{
				timelineServer.Stop();
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestPublishApplicationMetrics()
		{
			for (int i = 1; i <= 2; ++i)
			{
				ApplicationId appId = ApplicationId.NewInstance(0, i);
				RMApp app = CreateRMApp(appId);
				metricsPublisher.AppCreated(app, app.GetStartTime());
				metricsPublisher.AppFinished(app, RMAppState.Finished, app.GetFinishTime());
				if (i == 1)
				{
					metricsPublisher.AppACLsUpdated(app, "uers1,user2", 4L);
				}
				else
				{
					// in case user doesn't specify the ACLs
					metricsPublisher.AppACLsUpdated(app, null, 4L);
				}
				TimelineEntity entity = null;
				do
				{
					entity = store.GetEntity(appId.ToString(), ApplicationMetricsConstants.EntityType
						, EnumSet.AllOf<TimelineReader.Field>());
				}
				while (entity == null || entity.GetEvents().Count < 3);
				// ensure three events are both published before leaving the loop
				// verify all the fields
				NUnit.Framework.Assert.AreEqual(ApplicationMetricsConstants.EntityType, entity.GetEntityType
					());
				NUnit.Framework.Assert.AreEqual(app.GetApplicationId().ToString(), entity.GetEntityId
					());
				NUnit.Framework.Assert.AreEqual(app.GetName(), entity.GetOtherInfo()[ApplicationMetricsConstants
					.NameEntityInfo]);
				NUnit.Framework.Assert.AreEqual(app.GetQueue(), entity.GetOtherInfo()[ApplicationMetricsConstants
					.QueueEntityInfo]);
				NUnit.Framework.Assert.AreEqual(app.GetUser(), entity.GetOtherInfo()[ApplicationMetricsConstants
					.UserEntityInfo]);
				NUnit.Framework.Assert.AreEqual(app.GetApplicationType(), entity.GetOtherInfo()[ApplicationMetricsConstants
					.TypeEntityInfo]);
				NUnit.Framework.Assert.AreEqual(app.GetSubmitTime(), entity.GetOtherInfo()[ApplicationMetricsConstants
					.SubmittedTimeEntityInfo]);
				if (i == 1)
				{
					NUnit.Framework.Assert.AreEqual("uers1,user2", entity.GetOtherInfo()[ApplicationMetricsConstants
						.AppViewAclsEntityInfo]);
				}
				else
				{
					NUnit.Framework.Assert.AreEqual(string.Empty, entity.GetOtherInfo()[ApplicationMetricsConstants
						.AppViewAclsEntityInfo]);
					NUnit.Framework.Assert.AreEqual(app.GetRMAppMetrics().GetMemorySeconds(), long.Parse
						(entity.GetOtherInfo()[ApplicationMetricsConstants.AppMemMetrics].ToString()));
					NUnit.Framework.Assert.AreEqual(app.GetRMAppMetrics().GetVcoreSeconds(), long.Parse
						(entity.GetOtherInfo()[ApplicationMetricsConstants.AppCpuMetrics].ToString()));
				}
				bool hasCreatedEvent = false;
				bool hasFinishedEvent = false;
				bool hasACLsUpdatedEvent = false;
				foreach (TimelineEvent @event in entity.GetEvents())
				{
					if (@event.GetEventType().Equals(ApplicationMetricsConstants.CreatedEventType))
					{
						hasCreatedEvent = true;
						NUnit.Framework.Assert.AreEqual(app.GetStartTime(), @event.GetTimestamp());
					}
					else
					{
						if (@event.GetEventType().Equals(ApplicationMetricsConstants.FinishedEventType))
						{
							hasFinishedEvent = true;
							NUnit.Framework.Assert.AreEqual(app.GetFinishTime(), @event.GetTimestamp());
							NUnit.Framework.Assert.AreEqual(app.GetDiagnostics().ToString(), @event.GetEventInfo
								()[ApplicationMetricsConstants.DiagnosticsInfoEventInfo]);
							NUnit.Framework.Assert.AreEqual(app.GetFinalApplicationStatus().ToString(), @event
								.GetEventInfo()[ApplicationMetricsConstants.FinalStatusEventInfo]);
							NUnit.Framework.Assert.AreEqual(YarnApplicationState.Finished.ToString(), @event.
								GetEventInfo()[ApplicationMetricsConstants.StateEventInfo]);
						}
						else
						{
							if (@event.GetEventType().Equals(ApplicationMetricsConstants.AclsUpdatedEventType
								))
							{
								hasACLsUpdatedEvent = true;
								NUnit.Framework.Assert.AreEqual(4L, @event.GetTimestamp());
							}
						}
					}
				}
				NUnit.Framework.Assert.IsTrue(hasCreatedEvent && hasFinishedEvent && hasACLsUpdatedEvent
					);
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestPublishAppAttemptMetrics()
		{
			ApplicationAttemptId appAttemptId = ApplicationAttemptId.NewInstance(ApplicationId
				.NewInstance(0, 1), 1);
			RMAppAttempt appAttempt = CreateRMAppAttempt(appAttemptId);
			metricsPublisher.AppAttemptRegistered(appAttempt, int.MaxValue + 1L);
			RMApp app = Org.Mockito.Mockito.Mock<RMApp>();
			Org.Mockito.Mockito.When(app.GetFinalApplicationStatus()).ThenReturn(FinalApplicationStatus
				.Undefined);
			metricsPublisher.AppAttemptFinished(appAttempt, RMAppAttemptState.Finished, app, 
				int.MaxValue + 2L);
			TimelineEntity entity = null;
			do
			{
				entity = store.GetEntity(appAttemptId.ToString(), AppAttemptMetricsConstants.EntityType
					, EnumSet.AllOf<TimelineReader.Field>());
			}
			while (entity == null || entity.GetEvents().Count < 2);
			// ensure two events are both published before leaving the loop
			// verify all the fields
			NUnit.Framework.Assert.AreEqual(AppAttemptMetricsConstants.EntityType, entity.GetEntityType
				());
			NUnit.Framework.Assert.AreEqual(appAttemptId.ToString(), entity.GetEntityId());
			NUnit.Framework.Assert.AreEqual(appAttemptId.GetApplicationId().ToString(), entity
				.GetPrimaryFilters()[AppAttemptMetricsConstants.ParentPrimaryFilter].GetEnumerator
				().Next());
			bool hasRegisteredEvent = false;
			bool hasFinishedEvent = false;
			foreach (TimelineEvent @event in entity.GetEvents())
			{
				if (@event.GetEventType().Equals(AppAttemptMetricsConstants.RegisteredEventType))
				{
					hasRegisteredEvent = true;
					NUnit.Framework.Assert.AreEqual(appAttempt.GetHost(), @event.GetEventInfo()[AppAttemptMetricsConstants
						.HostEventInfo]);
					NUnit.Framework.Assert.AreEqual(appAttempt.GetRpcPort(), @event.GetEventInfo()[AppAttemptMetricsConstants
						.RpcPortEventInfo]);
					NUnit.Framework.Assert.AreEqual(appAttempt.GetMasterContainer().GetId().ToString(
						), @event.GetEventInfo()[AppAttemptMetricsConstants.MasterContainerEventInfo]);
				}
				else
				{
					if (@event.GetEventType().Equals(AppAttemptMetricsConstants.FinishedEventType))
					{
						hasFinishedEvent = true;
						NUnit.Framework.Assert.AreEqual(appAttempt.GetDiagnostics(), @event.GetEventInfo(
							)[AppAttemptMetricsConstants.DiagnosticsInfoEventInfo]);
						NUnit.Framework.Assert.AreEqual(appAttempt.GetTrackingUrl(), @event.GetEventInfo(
							)[AppAttemptMetricsConstants.TrackingUrlEventInfo]);
						NUnit.Framework.Assert.AreEqual(appAttempt.GetOriginalTrackingUrl(), @event.GetEventInfo
							()[AppAttemptMetricsConstants.OriginalTrackingUrlEventInfo]);
						NUnit.Framework.Assert.AreEqual(FinalApplicationStatus.Undefined.ToString(), @event
							.GetEventInfo()[AppAttemptMetricsConstants.FinalStatusEventInfo]);
						NUnit.Framework.Assert.AreEqual(YarnApplicationAttemptState.Finished.ToString(), 
							@event.GetEventInfo()[AppAttemptMetricsConstants.StateEventInfo]);
					}
				}
			}
			NUnit.Framework.Assert.IsTrue(hasRegisteredEvent && hasFinishedEvent);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestPublishContainerMetrics()
		{
			ContainerId containerId = ContainerId.NewContainerId(ApplicationAttemptId.NewInstance
				(ApplicationId.NewInstance(0, 1), 1), 1);
			RMContainer container = CreateRMContainer(containerId);
			metricsPublisher.ContainerCreated(container, container.GetCreationTime());
			metricsPublisher.ContainerFinished(container, container.GetFinishTime());
			TimelineEntity entity = null;
			do
			{
				entity = store.GetEntity(containerId.ToString(), ContainerMetricsConstants.EntityType
					, EnumSet.AllOf<TimelineReader.Field>());
			}
			while (entity == null || entity.GetEvents().Count < 2);
			// ensure two events are both published before leaving the loop
			// verify all the fields
			NUnit.Framework.Assert.AreEqual(ContainerMetricsConstants.EntityType, entity.GetEntityType
				());
			NUnit.Framework.Assert.AreEqual(containerId.ToString(), entity.GetEntityId());
			NUnit.Framework.Assert.AreEqual(containerId.GetApplicationAttemptId().ToString(), 
				entity.GetPrimaryFilters()[ContainerMetricsConstants.ParentPrimariyFilter].GetEnumerator
				().Next());
			NUnit.Framework.Assert.AreEqual(container.GetAllocatedNode().GetHost(), entity.GetOtherInfo
				()[ContainerMetricsConstants.AllocatedHostEntityInfo]);
			NUnit.Framework.Assert.AreEqual(container.GetAllocatedNode().GetPort(), entity.GetOtherInfo
				()[ContainerMetricsConstants.AllocatedPortEntityInfo]);
			NUnit.Framework.Assert.AreEqual(container.GetAllocatedResource().GetMemory(), entity
				.GetOtherInfo()[ContainerMetricsConstants.AllocatedMemoryEntityInfo]);
			NUnit.Framework.Assert.AreEqual(container.GetAllocatedResource().GetVirtualCores(
				), entity.GetOtherInfo()[ContainerMetricsConstants.AllocatedVcoreEntityInfo]);
			NUnit.Framework.Assert.AreEqual(container.GetAllocatedPriority().GetPriority(), entity
				.GetOtherInfo()[ContainerMetricsConstants.AllocatedPriorityEntityInfo]);
			bool hasCreatedEvent = false;
			bool hasFinishedEvent = false;
			foreach (TimelineEvent @event in entity.GetEvents())
			{
				if (@event.GetEventType().Equals(ContainerMetricsConstants.CreatedEventType))
				{
					hasCreatedEvent = true;
					NUnit.Framework.Assert.AreEqual(container.GetCreationTime(), @event.GetTimestamp(
						));
				}
				else
				{
					if (@event.GetEventType().Equals(ContainerMetricsConstants.FinishedEventType))
					{
						hasFinishedEvent = true;
						NUnit.Framework.Assert.AreEqual(container.GetFinishTime(), @event.GetTimestamp());
						NUnit.Framework.Assert.AreEqual(container.GetDiagnosticsInfo(), @event.GetEventInfo
							()[ContainerMetricsConstants.DiagnosticsInfoEventInfo]);
						NUnit.Framework.Assert.AreEqual(container.GetContainerExitStatus(), @event.GetEventInfo
							()[ContainerMetricsConstants.ExitStatusEventInfo]);
						NUnit.Framework.Assert.AreEqual(container.GetContainerState().ToString(), @event.
							GetEventInfo()[ContainerMetricsConstants.StateEventInfo]);
					}
				}
			}
			NUnit.Framework.Assert.IsTrue(hasCreatedEvent && hasFinishedEvent);
		}

		private static RMApp CreateRMApp(ApplicationId appId)
		{
			RMApp app = Org.Mockito.Mockito.Mock<RMApp>();
			Org.Mockito.Mockito.When(app.GetApplicationId()).ThenReturn(appId);
			Org.Mockito.Mockito.When(app.GetName()).ThenReturn("test app");
			Org.Mockito.Mockito.When(app.GetApplicationType()).ThenReturn("test app type");
			Org.Mockito.Mockito.When(app.GetUser()).ThenReturn("test user");
			Org.Mockito.Mockito.When(app.GetQueue()).ThenReturn("test queue");
			Org.Mockito.Mockito.When(app.GetSubmitTime()).ThenReturn(int.MaxValue + 1L);
			Org.Mockito.Mockito.When(app.GetStartTime()).ThenReturn(int.MaxValue + 2L);
			Org.Mockito.Mockito.When(app.GetFinishTime()).ThenReturn(int.MaxValue + 3L);
			Org.Mockito.Mockito.When(app.GetDiagnostics()).ThenReturn(new StringBuilder("test diagnostics info"
				));
			RMAppAttempt appAttempt = Org.Mockito.Mockito.Mock<RMAppAttempt>();
			Org.Mockito.Mockito.When(appAttempt.GetAppAttemptId()).ThenReturn(ApplicationAttemptId
				.NewInstance(appId, 1));
			Org.Mockito.Mockito.When(app.GetCurrentAppAttempt()).ThenReturn(appAttempt);
			Org.Mockito.Mockito.When(app.GetFinalApplicationStatus()).ThenReturn(FinalApplicationStatus
				.Undefined);
			Org.Mockito.Mockito.When(app.GetRMAppMetrics()).ThenReturn(new RMAppMetrics(null, 
				0, 0, int.MaxValue, long.MaxValue));
			return app;
		}

		private static RMAppAttempt CreateRMAppAttempt(ApplicationAttemptId appAttemptId)
		{
			RMAppAttempt appAttempt = Org.Mockito.Mockito.Mock<RMAppAttempt>();
			Org.Mockito.Mockito.When(appAttempt.GetAppAttemptId()).ThenReturn(appAttemptId);
			Org.Mockito.Mockito.When(appAttempt.GetHost()).ThenReturn("test host");
			Org.Mockito.Mockito.When(appAttempt.GetRpcPort()).ThenReturn(-100);
			Container container = Org.Mockito.Mockito.Mock<Container>();
			Org.Mockito.Mockito.When(container.GetId()).ThenReturn(ContainerId.NewContainerId
				(appAttemptId, 1));
			Org.Mockito.Mockito.When(appAttempt.GetMasterContainer()).ThenReturn(container);
			Org.Mockito.Mockito.When(appAttempt.GetDiagnostics()).ThenReturn("test diagnostics info"
				);
			Org.Mockito.Mockito.When(appAttempt.GetTrackingUrl()).ThenReturn("test tracking url"
				);
			Org.Mockito.Mockito.When(appAttempt.GetOriginalTrackingUrl()).ThenReturn("test original tracking url"
				);
			return appAttempt;
		}

		private static RMContainer CreateRMContainer(ContainerId containerId)
		{
			RMContainer container = Org.Mockito.Mockito.Mock<RMContainer>();
			Org.Mockito.Mockito.When(container.GetContainerId()).ThenReturn(containerId);
			Org.Mockito.Mockito.When(container.GetAllocatedNode()).ThenReturn(NodeId.NewInstance
				("test host", -100));
			Org.Mockito.Mockito.When(container.GetAllocatedResource()).ThenReturn(Resource.NewInstance
				(-1, -1));
			Org.Mockito.Mockito.When(container.GetAllocatedPriority()).ThenReturn(Priority.Undefined
				);
			Org.Mockito.Mockito.When(container.GetCreationTime()).ThenReturn(int.MaxValue + 1L
				);
			Org.Mockito.Mockito.When(container.GetFinishTime()).ThenReturn(int.MaxValue + 2L);
			Org.Mockito.Mockito.When(container.GetDiagnosticsInfo()).ThenReturn("test diagnostics info"
				);
			Org.Mockito.Mockito.When(container.GetContainerExitStatus()).ThenReturn(-1);
			Org.Mockito.Mockito.When(container.GetContainerState()).ThenReturn(ContainerState
				.Complete);
			Container mockContainer = Org.Mockito.Mockito.Mock<Container>();
			Org.Mockito.Mockito.When(container.GetContainer()).ThenReturn(mockContainer);
			Org.Mockito.Mockito.When(mockContainer.GetNodeHttpAddress()).ThenReturn("http://localhost:1234"
				);
			return container;
		}
	}
}
