using System.Collections.Generic;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Factories;
using Org.Apache.Hadoop.Yarn.Factory.Providers;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp.Attempt;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Event;
using Org.Apache.Hadoop.Yarn.Server.Utils;
using Org.Apache.Hadoop.Yarn.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fair
{
	public class FairSchedulerTestBase
	{
		protected internal class MockClock : Clock
		{
			private long time = 0;

			public virtual long GetTime()
			{
				return time;
			}

			public virtual void Tick(int seconds)
			{
				time = time + seconds * 1000;
			}
		}

		public static readonly string TestDir = new FilePath(Runtime.GetProperty("test.build.data"
			, "/tmp")).GetAbsolutePath();

		private static RecordFactory recordFactory = RecordFactoryProvider.GetRecordFactory
			(null);

		protected internal int AppId = 1;

		protected internal int AttemptId = 1;

		protected internal Configuration conf;

		protected internal FairScheduler scheduler;

		protected internal ResourceManager resourceManager;

		// Incrementing counter for scheduling apps
		// Incrementing counter for scheduling attempts
		// Helper methods
		public virtual Configuration CreateConfiguration()
		{
			Configuration conf = new YarnConfiguration();
			conf.SetClass(YarnConfiguration.RmScheduler, typeof(FairScheduler), typeof(ResourceScheduler
				));
			conf.SetInt(YarnConfiguration.RmSchedulerMinimumAllocationMb, 0);
			conf.SetInt(FairSchedulerConfiguration.RmSchedulerIncrementAllocationMb, 1024);
			conf.SetInt(YarnConfiguration.RmSchedulerMaximumAllocationMb, 10240);
			conf.SetBoolean(FairSchedulerConfiguration.AssignMultiple, false);
			conf.SetFloat(FairSchedulerConfiguration.PreemptionThreshold, 0f);
			return conf;
		}

		protected internal virtual ApplicationAttemptId CreateAppAttemptId(int appId, int
			 attemptId)
		{
			ApplicationId appIdImpl = ApplicationId.NewInstance(0, appId);
			return ApplicationAttemptId.NewInstance(appIdImpl, attemptId);
		}

		protected internal virtual ResourceRequest CreateResourceRequest(int memory, string
			 host, int priority, int numContainers, bool relaxLocality)
		{
			return CreateResourceRequest(memory, 1, host, priority, numContainers, relaxLocality
				);
		}

		protected internal virtual ResourceRequest CreateResourceRequest(int memory, int 
			vcores, string host, int priority, int numContainers, bool relaxLocality)
		{
			ResourceRequest request = recordFactory.NewRecordInstance<ResourceRequest>();
			request.SetCapability(BuilderUtils.NewResource(memory, vcores));
			request.SetResourceName(host);
			request.SetNumContainers(numContainers);
			Priority prio = recordFactory.NewRecordInstance<Priority>();
			prio.SetPriority(priority);
			request.SetPriority(prio);
			request.SetRelaxLocality(relaxLocality);
			return request;
		}

		/// <summary>
		/// Creates a single container priority-1 request and submits to
		/// scheduler.
		/// </summary>
		protected internal virtual ApplicationAttemptId CreateSchedulingRequest(int memory
			, string queueId, string userId)
		{
			return CreateSchedulingRequest(memory, queueId, userId, 1);
		}

		protected internal virtual ApplicationAttemptId CreateSchedulingRequest(int memory
			, int vcores, string queueId, string userId)
		{
			return CreateSchedulingRequest(memory, vcores, queueId, userId, 1);
		}

		protected internal virtual ApplicationAttemptId CreateSchedulingRequest(int memory
			, string queueId, string userId, int numContainers)
		{
			return CreateSchedulingRequest(memory, queueId, userId, numContainers, 1);
		}

		protected internal virtual ApplicationAttemptId CreateSchedulingRequest(int memory
			, int vcores, string queueId, string userId, int numContainers)
		{
			return CreateSchedulingRequest(memory, vcores, queueId, userId, numContainers, 1);
		}

		protected internal virtual ApplicationAttemptId CreateSchedulingRequest(int memory
			, string queueId, string userId, int numContainers, int priority)
		{
			return CreateSchedulingRequest(memory, 1, queueId, userId, numContainers, priority
				);
		}

		protected internal virtual ApplicationAttemptId CreateSchedulingRequest(int memory
			, int vcores, string queueId, string userId, int numContainers, int priority)
		{
			ApplicationAttemptId id = CreateAppAttemptId(this.AppId++, this.AttemptId++);
			scheduler.AddApplication(id.GetApplicationId(), queueId, userId, false);
			// This conditional is for testAclSubmitApplication where app is rejected
			// and no app is added.
			if (scheduler.GetSchedulerApplications().Contains(id.GetApplicationId()))
			{
				scheduler.AddApplicationAttempt(id, false, false);
			}
			IList<ResourceRequest> ask = new AList<ResourceRequest>();
			ResourceRequest request = CreateResourceRequest(memory, vcores, ResourceRequest.Any
				, priority, numContainers, true);
			ask.AddItem(request);
			RMApp rmApp = Org.Mockito.Mockito.Mock<RMApp>();
			RMAppAttempt rmAppAttempt = Org.Mockito.Mockito.Mock<RMAppAttempt>();
			Org.Mockito.Mockito.When(rmApp.GetCurrentAppAttempt()).ThenReturn(rmAppAttempt);
			Org.Mockito.Mockito.When(rmAppAttempt.GetRMAppAttemptMetrics()).ThenReturn(new RMAppAttemptMetrics
				(id, resourceManager.GetRMContext()));
			resourceManager.GetRMContext().GetRMApps()[id.GetApplicationId()] = rmApp;
			scheduler.Allocate(id, ask, new AList<ContainerId>(), null, null);
			return id;
		}

		protected internal virtual ApplicationAttemptId CreateSchedulingRequest(string queueId
			, string userId, IList<ResourceRequest> ask)
		{
			ApplicationAttemptId id = CreateAppAttemptId(this.AppId++, this.AttemptId++);
			scheduler.AddApplication(id.GetApplicationId(), queueId, userId, false);
			// This conditional is for testAclSubmitApplication where app is rejected
			// and no app is added.
			if (scheduler.GetSchedulerApplications().Contains(id.GetApplicationId()))
			{
				scheduler.AddApplicationAttempt(id, false, false);
			}
			RMApp rmApp = Org.Mockito.Mockito.Mock<RMApp>();
			RMAppAttempt rmAppAttempt = Org.Mockito.Mockito.Mock<RMAppAttempt>();
			Org.Mockito.Mockito.When(rmApp.GetCurrentAppAttempt()).ThenReturn(rmAppAttempt);
			Org.Mockito.Mockito.When(rmAppAttempt.GetRMAppAttemptMetrics()).ThenReturn(new RMAppAttemptMetrics
				(id, resourceManager.GetRMContext()));
			resourceManager.GetRMContext().GetRMApps()[id.GetApplicationId()] = rmApp;
			scheduler.Allocate(id, ask, new AList<ContainerId>(), null, null);
			return id;
		}

		protected internal virtual void CreateSchedulingRequestExistingApplication(int memory
			, int priority, ApplicationAttemptId attId)
		{
			ResourceRequest request = CreateResourceRequest(memory, ResourceRequest.Any, priority
				, 1, true);
			CreateSchedulingRequestExistingApplication(request, attId);
		}

		protected internal virtual void CreateSchedulingRequestExistingApplication(int memory
			, int vcores, int priority, ApplicationAttemptId attId)
		{
			ResourceRequest request = CreateResourceRequest(memory, vcores, ResourceRequest.Any
				, priority, 1, true);
			CreateSchedulingRequestExistingApplication(request, attId);
		}

		protected internal virtual void CreateSchedulingRequestExistingApplication(ResourceRequest
			 request, ApplicationAttemptId attId)
		{
			IList<ResourceRequest> ask = new AList<ResourceRequest>();
			ask.AddItem(request);
			scheduler.Allocate(attId, ask, new AList<ContainerId>(), null, null);
		}

		protected internal virtual void CreateApplicationWithAMResource(ApplicationAttemptId
			 attId, string queue, string user, Resource amResource)
		{
			RMContext rmContext = resourceManager.GetRMContext();
			RMApp rmApp = new RMAppImpl(attId.GetApplicationId(), rmContext, conf, null, null
				, null, ApplicationSubmissionContext.NewInstance(null, null, null, null, null, false
				, false, 0, amResource, null), null, null, 0, null, null, null);
			rmContext.GetRMApps()[attId.GetApplicationId()] = rmApp;
			AppAddedSchedulerEvent appAddedEvent = new AppAddedSchedulerEvent(attId.GetApplicationId
				(), queue, user);
			scheduler.Handle(appAddedEvent);
			AppAttemptAddedSchedulerEvent attempAddedEvent = new AppAttemptAddedSchedulerEvent
				(attId, false);
			scheduler.Handle(attempAddedEvent);
		}

		protected internal virtual RMApp CreateMockRMApp(ApplicationAttemptId attemptId)
		{
			RMApp app = Org.Mockito.Mockito.Mock<RMAppImpl>();
			Org.Mockito.Mockito.When(app.GetApplicationId()).ThenReturn(attemptId.GetApplicationId
				());
			RMAppAttemptImpl attempt = Org.Mockito.Mockito.Mock<RMAppAttemptImpl>();
			Org.Mockito.Mockito.When(attempt.GetAppAttemptId()).ThenReturn(attemptId);
			RMAppAttemptMetrics attemptMetric = Org.Mockito.Mockito.Mock<RMAppAttemptMetrics>
				();
			Org.Mockito.Mockito.When(attempt.GetRMAppAttemptMetrics()).ThenReturn(attemptMetric
				);
			Org.Mockito.Mockito.When(app.GetCurrentAppAttempt()).ThenReturn(attempt);
			resourceManager.GetRMContext().GetRMApps()[attemptId.GetApplicationId()] = app;
			return app;
		}
	}
}
