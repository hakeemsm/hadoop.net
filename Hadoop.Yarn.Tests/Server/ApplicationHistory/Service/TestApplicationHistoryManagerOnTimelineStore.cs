using System;
using System.Collections.Generic;
using NUnit.Framework;
using NUnit.Framework.Runners;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Authorize;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Api.Records.Timeline;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Server.Metrics;
using Org.Apache.Hadoop.Yarn.Server.Security;
using Org.Apache.Hadoop.Yarn.Server.Timeline;
using Org.Apache.Hadoop.Yarn.Server.Timeline.Security;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Applicationhistoryservice
{
	public class TestApplicationHistoryManagerOnTimelineStore
	{
		private const int Scale = 5;

		private static TimelineStore store;

		private ApplicationHistoryManagerOnTimelineStore historyManager;

		private UserGroupInformation callerUGI;

		private Configuration conf;

		/// <exception cref="System.Exception"/>
		[BeforeClass]
		public static void PrepareStore()
		{
			store = CreateStore(Scale);
			TimelineEntities entities = new TimelineEntities();
			entities.AddEntity(CreateApplicationTimelineEntity(ApplicationId.NewInstance(0, Scale
				 + 1), true, true, false));
			entities.AddEntity(CreateApplicationTimelineEntity(ApplicationId.NewInstance(0, Scale
				 + 2), true, false, true));
			store.Put(entities);
		}

		/// <exception cref="System.Exception"/>
		public static TimelineStore CreateStore(int scale)
		{
			TimelineStore store = new MemoryTimelineStore();
			PrepareTimelineStore(store, scale);
			return store;
		}

		/// <exception cref="System.Exception"/>
		[SetUp]
		public virtual void Setup()
		{
			// Only test the ACLs of the generic history
			TimelineACLsManager aclsManager = new TimelineACLsManager(new YarnConfiguration()
				);
			TimelineDataManager dataManager = new TimelineDataManager(store, aclsManager);
			ApplicationACLsManager appAclsManager = new ApplicationACLsManager(conf);
			historyManager = new ApplicationHistoryManagerOnTimelineStore(dataManager, appAclsManager
				);
			historyManager.Init(conf);
			historyManager.Start();
		}

		[NUnit.Framework.TearDown]
		public virtual void TearDown()
		{
			if (historyManager != null)
			{
				historyManager.Stop();
			}
		}

		[Parameterized.Parameters]
		public static ICollection<object[]> Callers()
		{
			// user1 is the owner
			// user2 is the authorized user
			// user3 is the unauthorized user
			// admin is the admin acl
			return Arrays.AsList(new object[][] { new object[] { string.Empty }, new object[]
				 { "user1" }, new object[] { "user2" }, new object[] { "user3" }, new object[] { 
				"admin" } });
		}

		public TestApplicationHistoryManagerOnTimelineStore(string caller)
		{
			conf = new YarnConfiguration();
			if (!caller.Equals(string.Empty))
			{
				callerUGI = UserGroupInformation.CreateRemoteUser(caller, SaslRpcServer.AuthMethod
					.Simple);
				conf.SetBoolean(YarnConfiguration.YarnAclEnable, true);
				conf.Set(YarnConfiguration.YarnAdminAcl, "admin");
			}
		}

		/// <exception cref="System.Exception"/>
		private static void PrepareTimelineStore(TimelineStore store, int scale)
		{
			for (int i = 1; i <= scale; ++i)
			{
				TimelineEntities entities = new TimelineEntities();
				ApplicationId appId = ApplicationId.NewInstance(0, i);
				if (i == 2)
				{
					entities.AddEntity(CreateApplicationTimelineEntity(appId, true, false, false));
				}
				else
				{
					entities.AddEntity(CreateApplicationTimelineEntity(appId, false, false, false));
				}
				store.Put(entities);
				for (int j = 1; j <= scale; ++j)
				{
					entities = new TimelineEntities();
					ApplicationAttemptId appAttemptId = ApplicationAttemptId.NewInstance(appId, j);
					entities.AddEntity(CreateAppAttemptTimelineEntity(appAttemptId));
					store.Put(entities);
					for (int k = 1; k <= scale; ++k)
					{
						entities = new TimelineEntities();
						ContainerId containerId = ContainerId.NewContainerId(appAttemptId, k);
						entities.AddEntity(CreateContainerEntity(containerId));
						store.Put(entities);
					}
				}
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGetApplicationReport()
		{
			for (int i = 1; i <= 2; ++i)
			{
				ApplicationId appId = ApplicationId.NewInstance(0, i);
				ApplicationReport app;
				if (callerUGI == null)
				{
					app = historyManager.GetApplication(appId);
				}
				else
				{
					app = callerUGI.DoAs(new _PrivilegedExceptionAction_170(this, appId));
				}
				NUnit.Framework.Assert.IsNotNull(app);
				NUnit.Framework.Assert.AreEqual(appId, app.GetApplicationId());
				NUnit.Framework.Assert.AreEqual("test app", app.GetName());
				NUnit.Framework.Assert.AreEqual("test app type", app.GetApplicationType());
				NUnit.Framework.Assert.AreEqual("user1", app.GetUser());
				NUnit.Framework.Assert.AreEqual("test queue", app.GetQueue());
				NUnit.Framework.Assert.AreEqual(int.MaxValue + 2L + app.GetApplicationId().GetId(
					), app.GetStartTime());
				NUnit.Framework.Assert.AreEqual(int.MaxValue + 3L + +app.GetApplicationId().GetId
					(), app.GetFinishTime());
				NUnit.Framework.Assert.IsTrue(Math.Abs(app.GetProgress() - 1.0F) < 0.0001);
				// App 2 doesn't have the ACLs, such that the default ACLs " " will be used.
				// Nobody except admin and owner has access to the details of the app.
				if ((i == 1 && callerUGI != null && callerUGI.GetShortUserName().Equals("user3"))
					 || (i == 2 && callerUGI != null && (callerUGI.GetShortUserName().Equals("user2"
					) || callerUGI.GetShortUserName().Equals("user3"))))
				{
					NUnit.Framework.Assert.AreEqual(ApplicationAttemptId.NewInstance(appId, -1), app.
						GetCurrentApplicationAttemptId());
					NUnit.Framework.Assert.AreEqual(ApplicationHistoryManagerOnTimelineStore.Unavailable
						, app.GetHost());
					NUnit.Framework.Assert.AreEqual(-1, app.GetRpcPort());
					NUnit.Framework.Assert.AreEqual(ApplicationHistoryManagerOnTimelineStore.Unavailable
						, app.GetTrackingUrl());
					NUnit.Framework.Assert.AreEqual(ApplicationHistoryManagerOnTimelineStore.Unavailable
						, app.GetOriginalTrackingUrl());
					NUnit.Framework.Assert.AreEqual(string.Empty, app.GetDiagnostics());
				}
				else
				{
					NUnit.Framework.Assert.AreEqual(ApplicationAttemptId.NewInstance(appId, 1), app.GetCurrentApplicationAttemptId
						());
					NUnit.Framework.Assert.AreEqual("test host", app.GetHost());
					NUnit.Framework.Assert.AreEqual(100, app.GetRpcPort());
					NUnit.Framework.Assert.AreEqual("test tracking url", app.GetTrackingUrl());
					NUnit.Framework.Assert.AreEqual("test original tracking url", app.GetOriginalTrackingUrl
						());
					NUnit.Framework.Assert.AreEqual("test diagnostics info", app.GetDiagnostics());
				}
				ApplicationResourceUsageReport applicationResourceUsageReport = app.GetApplicationResourceUsageReport
					();
				NUnit.Framework.Assert.AreEqual(123, applicationResourceUsageReport.GetMemorySeconds
					());
				NUnit.Framework.Assert.AreEqual(345, applicationResourceUsageReport.GetVcoreSeconds
					());
				NUnit.Framework.Assert.AreEqual(FinalApplicationStatus.Undefined, app.GetFinalApplicationStatus
					());
				NUnit.Framework.Assert.AreEqual(YarnApplicationState.Finished, app.GetYarnApplicationState
					());
			}
		}

		private sealed class _PrivilegedExceptionAction_170 : PrivilegedExceptionAction<ApplicationReport
			>
		{
			public _PrivilegedExceptionAction_170(TestApplicationHistoryManagerOnTimelineStore
				 _enclosing, ApplicationId appId)
			{
				this._enclosing = _enclosing;
				this.appId = appId;
			}

			/// <exception cref="System.Exception"/>
			public ApplicationReport Run()
			{
				return this._enclosing.historyManager.GetApplication(appId);
			}

			private readonly TestApplicationHistoryManagerOnTimelineStore _enclosing;

			private readonly ApplicationId appId;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGetApplicationReportWithNotAttempt()
		{
			ApplicationId appId = ApplicationId.NewInstance(0, Scale + 1);
			ApplicationReport app;
			if (callerUGI == null)
			{
				app = historyManager.GetApplication(appId);
			}
			else
			{
				app = callerUGI.DoAs(new _PrivilegedExceptionAction_236(this, appId));
			}
			NUnit.Framework.Assert.IsNotNull(app);
			NUnit.Framework.Assert.AreEqual(appId, app.GetApplicationId());
			NUnit.Framework.Assert.AreEqual(ApplicationAttemptId.NewInstance(appId, -1), app.
				GetCurrentApplicationAttemptId());
		}

		private sealed class _PrivilegedExceptionAction_236 : PrivilegedExceptionAction<ApplicationReport
			>
		{
			public _PrivilegedExceptionAction_236(TestApplicationHistoryManagerOnTimelineStore
				 _enclosing, ApplicationId appId)
			{
				this._enclosing = _enclosing;
				this.appId = appId;
			}

			/// <exception cref="System.Exception"/>
			public ApplicationReport Run()
			{
				return this._enclosing.historyManager.GetApplication(appId);
			}

			private readonly TestApplicationHistoryManagerOnTimelineStore _enclosing;

			private readonly ApplicationId appId;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGetApplicationAttemptReport()
		{
			ApplicationAttemptId appAttemptId = ApplicationAttemptId.NewInstance(ApplicationId
				.NewInstance(0, 1), 1);
			ApplicationAttemptReport appAttempt;
			if (callerUGI == null)
			{
				appAttempt = historyManager.GetApplicationAttempt(appAttemptId);
			}
			else
			{
				try
				{
					appAttempt = callerUGI.DoAs(new _PrivilegedExceptionAction_259(this, appAttemptId
						));
					if (callerUGI != null && callerUGI.GetShortUserName().Equals("user3"))
					{
						// The exception is expected
						NUnit.Framework.Assert.Fail();
					}
				}
				catch (AuthorizationException e)
				{
					if (callerUGI != null && callerUGI.GetShortUserName().Equals("user3"))
					{
						// The exception is expected
						return;
					}
					throw;
				}
			}
			NUnit.Framework.Assert.IsNotNull(appAttempt);
			NUnit.Framework.Assert.AreEqual(appAttemptId, appAttempt.GetApplicationAttemptId(
				));
			NUnit.Framework.Assert.AreEqual(ContainerId.NewContainerId(appAttemptId, 1), appAttempt
				.GetAMContainerId());
			NUnit.Framework.Assert.AreEqual("test host", appAttempt.GetHost());
			NUnit.Framework.Assert.AreEqual(100, appAttempt.GetRpcPort());
			NUnit.Framework.Assert.AreEqual("test tracking url", appAttempt.GetTrackingUrl());
			NUnit.Framework.Assert.AreEqual("test original tracking url", appAttempt.GetOriginalTrackingUrl
				());
			NUnit.Framework.Assert.AreEqual("test diagnostics info", appAttempt.GetDiagnostics
				());
			NUnit.Framework.Assert.AreEqual(YarnApplicationAttemptState.Finished, appAttempt.
				GetYarnApplicationAttemptState());
		}

		private sealed class _PrivilegedExceptionAction_259 : PrivilegedExceptionAction<ApplicationAttemptReport
			>
		{
			public _PrivilegedExceptionAction_259(TestApplicationHistoryManagerOnTimelineStore
				 _enclosing, ApplicationAttemptId appAttemptId)
			{
				this._enclosing = _enclosing;
				this.appAttemptId = appAttemptId;
			}

			/// <exception cref="System.Exception"/>
			public ApplicationAttemptReport Run()
			{
				return this._enclosing.historyManager.GetApplicationAttempt(appAttemptId);
			}

			private readonly TestApplicationHistoryManagerOnTimelineStore _enclosing;

			private readonly ApplicationAttemptId appAttemptId;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGetContainerReport()
		{
			ContainerId containerId = ContainerId.NewContainerId(ApplicationAttemptId.NewInstance
				(ApplicationId.NewInstance(0, 1), 1), 1);
			ContainerReport container;
			if (callerUGI == null)
			{
				container = historyManager.GetContainer(containerId);
			}
			else
			{
				try
				{
					container = callerUGI.DoAs(new _PrivilegedExceptionAction_302(this, containerId));
					if (callerUGI != null && callerUGI.GetShortUserName().Equals("user3"))
					{
						// The exception is expected
						NUnit.Framework.Assert.Fail();
					}
				}
				catch (AuthorizationException e)
				{
					if (callerUGI != null && callerUGI.GetShortUserName().Equals("user3"))
					{
						// The exception is expected
						return;
					}
					throw;
				}
			}
			NUnit.Framework.Assert.IsNotNull(container);
			NUnit.Framework.Assert.AreEqual(int.MaxValue + 1L, container.GetCreationTime());
			NUnit.Framework.Assert.AreEqual(int.MaxValue + 2L, container.GetFinishTime());
			NUnit.Framework.Assert.AreEqual(Resource.NewInstance(-1, -1), container.GetAllocatedResource
				());
			NUnit.Framework.Assert.AreEqual(NodeId.NewInstance("test host", 100), container.GetAssignedNode
				());
			NUnit.Framework.Assert.AreEqual(Priority.Undefined, container.GetPriority());
			NUnit.Framework.Assert.AreEqual("test diagnostics info", container.GetDiagnosticsInfo
				());
			NUnit.Framework.Assert.AreEqual(ContainerState.Complete, container.GetContainerState
				());
			NUnit.Framework.Assert.AreEqual(-1, container.GetContainerExitStatus());
			NUnit.Framework.Assert.AreEqual("http://0.0.0.0:8188/applicationhistory/logs/" + 
				"test host:100/container_0_0001_01_000001/" + "container_0_0001_01_000001/user1"
				, container.GetLogUrl());
		}

		private sealed class _PrivilegedExceptionAction_302 : PrivilegedExceptionAction<ContainerReport
			>
		{
			public _PrivilegedExceptionAction_302(TestApplicationHistoryManagerOnTimelineStore
				 _enclosing, ContainerId containerId)
			{
				this._enclosing = _enclosing;
				this.containerId = containerId;
			}

			/// <exception cref="System.Exception"/>
			public ContainerReport Run()
			{
				return this._enclosing.historyManager.GetContainer(containerId);
			}

			private readonly TestApplicationHistoryManagerOnTimelineStore _enclosing;

			private readonly ContainerId containerId;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGetApplications()
		{
			ICollection<ApplicationReport> apps = historyManager.GetApplications(long.MaxValue
				).Values;
			NUnit.Framework.Assert.IsNotNull(apps);
			NUnit.Framework.Assert.AreEqual(Scale + 1, apps.Count);
			ApplicationId ignoredAppId = ApplicationId.NewInstance(0, Scale + 2);
			foreach (ApplicationReport app in apps)
			{
				Assert.AssertNotEquals(ignoredAppId, app.GetApplicationId());
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGetApplicationAttempts()
		{
			ApplicationId appId = ApplicationId.NewInstance(0, 1);
			ICollection<ApplicationAttemptReport> appAttempts;
			if (callerUGI == null)
			{
				appAttempts = historyManager.GetApplicationAttempts(appId).Values;
			}
			else
			{
				try
				{
					appAttempts = callerUGI.DoAs(new _PrivilegedExceptionAction_358(this, appId));
					if (callerUGI != null && callerUGI.GetShortUserName().Equals("user3"))
					{
						// The exception is expected
						NUnit.Framework.Assert.Fail();
					}
				}
				catch (AuthorizationException e)
				{
					if (callerUGI != null && callerUGI.GetShortUserName().Equals("user3"))
					{
						// The exception is expected
						return;
					}
					throw;
				}
			}
			NUnit.Framework.Assert.IsNotNull(appAttempts);
			NUnit.Framework.Assert.AreEqual(Scale, appAttempts.Count);
		}

		private sealed class _PrivilegedExceptionAction_358 : PrivilegedExceptionAction<ICollection
			<ApplicationAttemptReport>>
		{
			public _PrivilegedExceptionAction_358(TestApplicationHistoryManagerOnTimelineStore
				 _enclosing, ApplicationId appId)
			{
				this._enclosing = _enclosing;
				this.appId = appId;
			}

			/// <exception cref="System.Exception"/>
			public ICollection<ApplicationAttemptReport> Run()
			{
				return this._enclosing.historyManager.GetApplicationAttempts(appId).Values;
			}

			private readonly TestApplicationHistoryManagerOnTimelineStore _enclosing;

			private readonly ApplicationId appId;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGetContainers()
		{
			ApplicationAttemptId appAttemptId = ApplicationAttemptId.NewInstance(ApplicationId
				.NewInstance(0, 1), 1);
			ICollection<ContainerReport> containers;
			if (callerUGI == null)
			{
				containers = historyManager.GetContainers(appAttemptId).Values;
			}
			else
			{
				try
				{
					containers = callerUGI.DoAs(new _PrivilegedExceptionAction_390(this, appAttemptId
						));
					if (callerUGI != null && callerUGI.GetShortUserName().Equals("user3"))
					{
						// The exception is expected
						NUnit.Framework.Assert.Fail();
					}
				}
				catch (AuthorizationException e)
				{
					if (callerUGI != null && callerUGI.GetShortUserName().Equals("user3"))
					{
						// The exception is expected
						return;
					}
					throw;
				}
			}
			NUnit.Framework.Assert.IsNotNull(containers);
			NUnit.Framework.Assert.AreEqual(Scale, containers.Count);
		}

		private sealed class _PrivilegedExceptionAction_390 : PrivilegedExceptionAction<ICollection
			<ContainerReport>>
		{
			public _PrivilegedExceptionAction_390(TestApplicationHistoryManagerOnTimelineStore
				 _enclosing, ApplicationAttemptId appAttemptId)
			{
				this._enclosing = _enclosing;
				this.appAttemptId = appAttemptId;
			}

			/// <exception cref="System.Exception"/>
			public ICollection<ContainerReport> Run()
			{
				return this._enclosing.historyManager.GetContainers(appAttemptId).Values;
			}

			private readonly TestApplicationHistoryManagerOnTimelineStore _enclosing;

			private readonly ApplicationAttemptId appAttemptId;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGetAMContainer()
		{
			ApplicationAttemptId appAttemptId = ApplicationAttemptId.NewInstance(ApplicationId
				.NewInstance(0, 1), 1);
			ContainerReport container;
			if (callerUGI == null)
			{
				container = historyManager.GetAMContainer(appAttemptId);
			}
			else
			{
				try
				{
					container = callerUGI.DoAs(new _PrivilegedExceptionAction_422(this, appAttemptId)
						);
					if (callerUGI != null && callerUGI.GetShortUserName().Equals("user3"))
					{
						// The exception is expected
						NUnit.Framework.Assert.Fail();
					}
				}
				catch (AuthorizationException e)
				{
					if (callerUGI != null && callerUGI.GetShortUserName().Equals("user3"))
					{
						// The exception is expected
						return;
					}
					throw;
				}
			}
			NUnit.Framework.Assert.IsNotNull(container);
			NUnit.Framework.Assert.AreEqual(appAttemptId, container.GetContainerId().GetApplicationAttemptId
				());
		}

		private sealed class _PrivilegedExceptionAction_422 : PrivilegedExceptionAction<ContainerReport
			>
		{
			public _PrivilegedExceptionAction_422(TestApplicationHistoryManagerOnTimelineStore
				 _enclosing, ApplicationAttemptId appAttemptId)
			{
				this._enclosing = _enclosing;
				this.appAttemptId = appAttemptId;
			}

			/// <exception cref="System.Exception"/>
			public ContainerReport Run()
			{
				return this._enclosing.historyManager.GetAMContainer(appAttemptId);
			}

			private readonly TestApplicationHistoryManagerOnTimelineStore _enclosing;

			private readonly ApplicationAttemptId appAttemptId;
		}

		private static TimelineEntity CreateApplicationTimelineEntity(ApplicationId appId
			, bool emptyACLs, bool noAttemptId, bool wrongAppId)
		{
			TimelineEntity entity = new TimelineEntity();
			entity.SetEntityType(ApplicationMetricsConstants.EntityType);
			if (wrongAppId)
			{
				entity.SetEntityId("wrong_app_id");
			}
			else
			{
				entity.SetEntityId(appId.ToString());
			}
			entity.SetDomainId(TimelineDataManager.DefaultDomainId);
			entity.AddPrimaryFilter(TimelineStore.SystemFilter.EntityOwner.ToString(), "yarn"
				);
			IDictionary<string, object> entityInfo = new Dictionary<string, object>();
			entityInfo[ApplicationMetricsConstants.NameEntityInfo] = "test app";
			entityInfo[ApplicationMetricsConstants.TypeEntityInfo] = "test app type";
			entityInfo[ApplicationMetricsConstants.UserEntityInfo] = "user1";
			entityInfo[ApplicationMetricsConstants.QueueEntityInfo] = "test queue";
			entityInfo[ApplicationMetricsConstants.SubmittedTimeEntityInfo] = int.MaxValue + 
				1L;
			entityInfo[ApplicationMetricsConstants.AppMemMetrics] = 123;
			entityInfo[ApplicationMetricsConstants.AppCpuMetrics] = 345;
			if (emptyACLs)
			{
				entityInfo[ApplicationMetricsConstants.AppViewAclsEntityInfo] = string.Empty;
			}
			else
			{
				entityInfo[ApplicationMetricsConstants.AppViewAclsEntityInfo] = "user2";
			}
			entity.SetOtherInfo(entityInfo);
			TimelineEvent tEvent = new TimelineEvent();
			tEvent.SetEventType(ApplicationMetricsConstants.CreatedEventType);
			tEvent.SetTimestamp(int.MaxValue + 2L + appId.GetId());
			entity.AddEvent(tEvent);
			tEvent = new TimelineEvent();
			tEvent.SetEventType(ApplicationMetricsConstants.FinishedEventType);
			tEvent.SetTimestamp(int.MaxValue + 3L + appId.GetId());
			IDictionary<string, object> eventInfo = new Dictionary<string, object>();
			eventInfo[ApplicationMetricsConstants.DiagnosticsInfoEventInfo] = "test diagnostics info";
			eventInfo[ApplicationMetricsConstants.FinalStatusEventInfo] = FinalApplicationStatus
				.Undefined.ToString();
			eventInfo[ApplicationMetricsConstants.StateEventInfo] = YarnApplicationState.Finished
				.ToString();
			if (!noAttemptId)
			{
				eventInfo[ApplicationMetricsConstants.LatestAppAttemptEventInfo] = ApplicationAttemptId
					.NewInstance(appId, 1);
			}
			tEvent.SetEventInfo(eventInfo);
			entity.AddEvent(tEvent);
			return entity;
		}

		private static TimelineEntity CreateAppAttemptTimelineEntity(ApplicationAttemptId
			 appAttemptId)
		{
			TimelineEntity entity = new TimelineEntity();
			entity.SetEntityType(AppAttemptMetricsConstants.EntityType);
			entity.SetEntityId(appAttemptId.ToString());
			entity.SetDomainId(TimelineDataManager.DefaultDomainId);
			entity.AddPrimaryFilter(AppAttemptMetricsConstants.ParentPrimaryFilter, appAttemptId
				.GetApplicationId().ToString());
			entity.AddPrimaryFilter(TimelineStore.SystemFilter.EntityOwner.ToString(), "yarn"
				);
			TimelineEvent tEvent = new TimelineEvent();
			tEvent.SetEventType(AppAttemptMetricsConstants.RegisteredEventType);
			tEvent.SetTimestamp(int.MaxValue + 1L);
			IDictionary<string, object> eventInfo = new Dictionary<string, object>();
			eventInfo[AppAttemptMetricsConstants.TrackingUrlEventInfo] = "test tracking url";
			eventInfo[AppAttemptMetricsConstants.OriginalTrackingUrlEventInfo] = "test original tracking url";
			eventInfo[AppAttemptMetricsConstants.HostEventInfo] = "test host";
			eventInfo[AppAttemptMetricsConstants.RpcPortEventInfo] = 100;
			eventInfo[AppAttemptMetricsConstants.MasterContainerEventInfo] = ContainerId.NewContainerId
				(appAttemptId, 1);
			tEvent.SetEventInfo(eventInfo);
			entity.AddEvent(tEvent);
			tEvent = new TimelineEvent();
			tEvent.SetEventType(AppAttemptMetricsConstants.FinishedEventType);
			tEvent.SetTimestamp(int.MaxValue + 2L);
			eventInfo = new Dictionary<string, object>();
			eventInfo[AppAttemptMetricsConstants.TrackingUrlEventInfo] = "test tracking url";
			eventInfo[AppAttemptMetricsConstants.OriginalTrackingUrlEventInfo] = "test original tracking url";
			eventInfo[AppAttemptMetricsConstants.DiagnosticsInfoEventInfo] = "test diagnostics info";
			eventInfo[AppAttemptMetricsConstants.FinalStatusEventInfo] = FinalApplicationStatus
				.Undefined.ToString();
			eventInfo[AppAttemptMetricsConstants.StateEventInfo] = YarnApplicationAttemptState
				.Finished.ToString();
			tEvent.SetEventInfo(eventInfo);
			entity.AddEvent(tEvent);
			return entity;
		}

		private static TimelineEntity CreateContainerEntity(ContainerId containerId)
		{
			TimelineEntity entity = new TimelineEntity();
			entity.SetEntityType(ContainerMetricsConstants.EntityType);
			entity.SetEntityId(containerId.ToString());
			entity.SetDomainId(TimelineDataManager.DefaultDomainId);
			entity.AddPrimaryFilter(ContainerMetricsConstants.ParentPrimariyFilter, containerId
				.GetApplicationAttemptId().ToString());
			entity.AddPrimaryFilter(TimelineStore.SystemFilter.EntityOwner.ToString(), "yarn"
				);
			IDictionary<string, object> entityInfo = new Dictionary<string, object>();
			entityInfo[ContainerMetricsConstants.AllocatedMemoryEntityInfo] = -1;
			entityInfo[ContainerMetricsConstants.AllocatedVcoreEntityInfo] = -1;
			entityInfo[ContainerMetricsConstants.AllocatedHostEntityInfo] = "test host";
			entityInfo[ContainerMetricsConstants.AllocatedPortEntityInfo] = 100;
			entityInfo[ContainerMetricsConstants.AllocatedPriorityEntityInfo] = -1;
			entity.SetOtherInfo(entityInfo);
			TimelineEvent tEvent = new TimelineEvent();
			tEvent.SetEventType(ContainerMetricsConstants.CreatedEventType);
			tEvent.SetTimestamp(int.MaxValue + 1L);
			entity.AddEvent(tEvent);
			tEvent = new TimelineEvent();
			tEvent.SetEventType(ContainerMetricsConstants.FinishedEventType);
			tEvent.SetTimestamp(int.MaxValue + 2L);
			IDictionary<string, object> eventInfo = new Dictionary<string, object>();
			eventInfo[ContainerMetricsConstants.DiagnosticsInfoEventInfo] = "test diagnostics info";
			eventInfo[ContainerMetricsConstants.ExitStatusEventInfo] = -1;
			eventInfo[ContainerMetricsConstants.StateEventInfo] = ContainerState.Complete.ToString
				();
			tEvent.SetEventInfo(eventInfo);
			entity.AddEvent(tEvent);
			return entity;
		}
	}
}
