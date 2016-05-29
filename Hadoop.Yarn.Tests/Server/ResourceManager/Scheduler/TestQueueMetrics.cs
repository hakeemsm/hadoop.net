using System;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Metrics2;
using Org.Apache.Hadoop.Metrics2.Impl;
using Org.Apache.Hadoop.Test;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fifo;
using Org.Apache.Hadoop.Yarn.Server.Utils;
using Org.Apache.Hadoop.Yarn.Util.Resource;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler
{
	public class TestQueueMetrics
	{
		internal const int Gb = 1024;

		private static readonly Configuration conf = new Configuration();

		private MetricsSystem ms;

		// MB
		[NUnit.Framework.SetUp]
		public virtual void SetUp()
		{
			ms = new MetricsSystemImpl();
			QueueMetrics.ClearQueueMetrics();
		}

		[NUnit.Framework.Test]
		public virtual void TestDefaultSingleQueueMetrics()
		{
			string queueName = "single";
			string user = "alice";
			QueueMetrics metrics = QueueMetrics.ForQueue(ms, queueName, null, false, conf);
			MetricsSource queueSource = QueueSource(ms, queueName);
			AppSchedulingInfo app = MockApp(user);
			metrics.SubmitApp(user);
			MetricsSource userSource = UserSource(ms, queueName, user);
			CheckApps(queueSource, 1, 0, 0, 0, 0, 0, true);
			metrics.SubmitAppAttempt(user);
			CheckApps(queueSource, 1, 1, 0, 0, 0, 0, true);
			metrics.SetAvailableResourcesToQueue(Resources.CreateResource(100 * Gb, 100));
			metrics.IncrPendingResources(user, 5, Resources.CreateResource(3 * Gb, 3));
			// Available resources is set externally, as it depends on dynamic
			// configurable cluster/queue resources
			CheckResources(queueSource, 0, 0, 0, 0, 0, 100 * Gb, 100, 15 * Gb, 15, 5, 0, 0, 0
				);
			metrics.RunAppAttempt(app.GetApplicationId(), user);
			CheckApps(queueSource, 1, 0, 1, 0, 0, 0, true);
			metrics.AllocateResources(user, 3, Resources.CreateResource(2 * Gb, 2), true);
			CheckResources(queueSource, 6 * Gb, 6, 3, 3, 0, 100 * Gb, 100, 9 * Gb, 9, 2, 0, 0
				, 0);
			metrics.ReleaseResources(user, 1, Resources.CreateResource(2 * Gb, 2));
			CheckResources(queueSource, 4 * Gb, 4, 2, 3, 1, 100 * Gb, 100, 9 * Gb, 9, 2, 0, 0
				, 0);
			metrics.FinishAppAttempt(app.GetApplicationId(), app.IsPending(), app.GetUser());
			CheckApps(queueSource, 1, 0, 0, 0, 0, 0, true);
			metrics.FinishApp(user, RMAppState.Finished);
			CheckApps(queueSource, 1, 0, 0, 1, 0, 0, true);
			NUnit.Framework.Assert.IsNull(userSource);
		}

		[NUnit.Framework.Test]
		public virtual void TestQueueAppMetricsForMultipleFailures()
		{
			string queueName = "single";
			string user = "alice";
			QueueMetrics metrics = QueueMetrics.ForQueue(ms, queueName, null, false, new Configuration
				());
			MetricsSource queueSource = QueueSource(ms, queueName);
			AppSchedulingInfo app = MockApp(user);
			metrics.SubmitApp(user);
			MetricsSource userSource = UserSource(ms, queueName, user);
			CheckApps(queueSource, 1, 0, 0, 0, 0, 0, true);
			metrics.SubmitAppAttempt(user);
			CheckApps(queueSource, 1, 1, 0, 0, 0, 0, true);
			metrics.RunAppAttempt(app.GetApplicationId(), user);
			CheckApps(queueSource, 1, 0, 1, 0, 0, 0, true);
			metrics.FinishAppAttempt(app.GetApplicationId(), app.IsPending(), app.GetUser());
			CheckApps(queueSource, 1, 0, 0, 0, 0, 0, true);
			// As the application has failed, framework retries the same application
			// based on configuration
			metrics.SubmitAppAttempt(user);
			CheckApps(queueSource, 1, 1, 0, 0, 0, 0, true);
			metrics.RunAppAttempt(app.GetApplicationId(), user);
			CheckApps(queueSource, 1, 0, 1, 0, 0, 0, true);
			// Suppose say application has failed this time as well.
			metrics.FinishAppAttempt(app.GetApplicationId(), app.IsPending(), app.GetUser());
			CheckApps(queueSource, 1, 0, 0, 0, 0, 0, true);
			// As the application has failed, framework retries the same application
			// based on configuration
			metrics.SubmitAppAttempt(user);
			CheckApps(queueSource, 1, 1, 0, 0, 0, 0, true);
			metrics.RunAppAttempt(app.GetApplicationId(), user);
			CheckApps(queueSource, 1, 0, 1, 0, 0, 0, true);
			// Suppose say application has failed, and there's no more retries.
			metrics.FinishAppAttempt(app.GetApplicationId(), app.IsPending(), app.GetUser());
			CheckApps(queueSource, 1, 0, 0, 0, 0, 0, true);
			metrics.FinishApp(user, RMAppState.Failed);
			CheckApps(queueSource, 1, 0, 0, 0, 1, 0, true);
			NUnit.Framework.Assert.IsNull(userSource);
		}

		[NUnit.Framework.Test]
		public virtual void TestSingleQueueWithUserMetrics()
		{
			string queueName = "single2";
			string user = "dodo";
			QueueMetrics metrics = QueueMetrics.ForQueue(ms, queueName, null, true, conf);
			MetricsSource queueSource = QueueSource(ms, queueName);
			AppSchedulingInfo app = MockApp(user);
			metrics.SubmitApp(user);
			MetricsSource userSource = UserSource(ms, queueName, user);
			CheckApps(queueSource, 1, 0, 0, 0, 0, 0, true);
			CheckApps(userSource, 1, 0, 0, 0, 0, 0, true);
			metrics.SubmitAppAttempt(user);
			CheckApps(queueSource, 1, 1, 0, 0, 0, 0, true);
			CheckApps(userSource, 1, 1, 0, 0, 0, 0, true);
			metrics.SetAvailableResourcesToQueue(Resources.CreateResource(100 * Gb, 100));
			metrics.SetAvailableResourcesToUser(user, Resources.CreateResource(10 * Gb, 10));
			metrics.IncrPendingResources(user, 5, Resources.CreateResource(3 * Gb, 3));
			// Available resources is set externally, as it depends on dynamic
			// configurable cluster/queue resources
			CheckResources(queueSource, 0, 0, 0, 0, 0, 100 * Gb, 100, 15 * Gb, 15, 5, 0, 0, 0
				);
			CheckResources(userSource, 0, 0, 0, 0, 0, 10 * Gb, 10, 15 * Gb, 15, 5, 0, 0, 0);
			metrics.RunAppAttempt(app.GetApplicationId(), user);
			CheckApps(queueSource, 1, 0, 1, 0, 0, 0, true);
			CheckApps(userSource, 1, 0, 1, 0, 0, 0, true);
			metrics.AllocateResources(user, 3, Resources.CreateResource(2 * Gb, 2), true);
			CheckResources(queueSource, 6 * Gb, 6, 3, 3, 0, 100 * Gb, 100, 9 * Gb, 9, 2, 0, 0
				, 0);
			CheckResources(userSource, 6 * Gb, 6, 3, 3, 0, 10 * Gb, 10, 9 * Gb, 9, 2, 0, 0, 0
				);
			metrics.ReleaseResources(user, 1, Resources.CreateResource(2 * Gb, 2));
			CheckResources(queueSource, 4 * Gb, 4, 2, 3, 1, 100 * Gb, 100, 9 * Gb, 9, 2, 0, 0
				, 0);
			CheckResources(userSource, 4 * Gb, 4, 2, 3, 1, 10 * Gb, 10, 9 * Gb, 9, 2, 0, 0, 0
				);
			metrics.FinishAppAttempt(app.GetApplicationId(), app.IsPending(), app.GetUser());
			CheckApps(queueSource, 1, 0, 0, 0, 0, 0, true);
			CheckApps(userSource, 1, 0, 0, 0, 0, 0, true);
			metrics.FinishApp(user, RMAppState.Finished);
			CheckApps(queueSource, 1, 0, 0, 1, 0, 0, true);
			CheckApps(userSource, 1, 0, 0, 1, 0, 0, true);
		}

		[NUnit.Framework.Test]
		public virtual void TestTwoLevelWithUserMetrics()
		{
			string parentQueueName = "root";
			string leafQueueName = "root.leaf";
			string user = "alice";
			QueueMetrics parentMetrics = QueueMetrics.ForQueue(ms, parentQueueName, null, true
				, conf);
			Queue parentQueue = MockitoMaker.Make(MockitoMaker.Stub<Queue>().Returning(parentMetrics
				).from.GetMetrics());
			QueueMetrics metrics = QueueMetrics.ForQueue(ms, leafQueueName, parentQueue, true
				, conf);
			MetricsSource parentQueueSource = QueueSource(ms, parentQueueName);
			MetricsSource queueSource = QueueSource(ms, leafQueueName);
			AppSchedulingInfo app = MockApp(user);
			metrics.SubmitApp(user);
			MetricsSource userSource = UserSource(ms, leafQueueName, user);
			MetricsSource parentUserSource = UserSource(ms, parentQueueName, user);
			CheckApps(queueSource, 1, 0, 0, 0, 0, 0, true);
			CheckApps(parentQueueSource, 1, 0, 0, 0, 0, 0, true);
			CheckApps(userSource, 1, 0, 0, 0, 0, 0, true);
			CheckApps(parentUserSource, 1, 0, 0, 0, 0, 0, true);
			metrics.SubmitAppAttempt(user);
			CheckApps(queueSource, 1, 1, 0, 0, 0, 0, true);
			CheckApps(parentQueueSource, 1, 1, 0, 0, 0, 0, true);
			CheckApps(userSource, 1, 1, 0, 0, 0, 0, true);
			CheckApps(parentUserSource, 1, 1, 0, 0, 0, 0, true);
			parentMetrics.SetAvailableResourcesToQueue(Resources.CreateResource(100 * Gb, 100
				));
			metrics.SetAvailableResourcesToQueue(Resources.CreateResource(100 * Gb, 100));
			parentMetrics.SetAvailableResourcesToUser(user, Resources.CreateResource(10 * Gb, 
				10));
			metrics.SetAvailableResourcesToUser(user, Resources.CreateResource(10 * Gb, 10));
			metrics.IncrPendingResources(user, 5, Resources.CreateResource(3 * Gb, 3));
			CheckResources(queueSource, 0, 0, 0, 0, 0, 100 * Gb, 100, 15 * Gb, 15, 5, 0, 0, 0
				);
			CheckResources(parentQueueSource, 0, 0, 0, 0, 0, 100 * Gb, 100, 15 * Gb, 15, 5, 0
				, 0, 0);
			CheckResources(userSource, 0, 0, 0, 0, 0, 10 * Gb, 10, 15 * Gb, 15, 5, 0, 0, 0);
			CheckResources(parentUserSource, 0, 0, 0, 0, 0, 10 * Gb, 10, 15 * Gb, 15, 5, 0, 0
				, 0);
			metrics.RunAppAttempt(app.GetApplicationId(), user);
			CheckApps(queueSource, 1, 0, 1, 0, 0, 0, true);
			CheckApps(userSource, 1, 0, 1, 0, 0, 0, true);
			metrics.AllocateResources(user, 3, Resources.CreateResource(2 * Gb, 2), true);
			metrics.ReserveResource(user, Resources.CreateResource(3 * Gb, 3));
			// Available resources is set externally, as it depends on dynamic
			// configurable cluster/queue resources
			CheckResources(queueSource, 6 * Gb, 6, 3, 3, 0, 100 * Gb, 100, 9 * Gb, 9, 2, 3 * 
				Gb, 3, 1);
			CheckResources(parentQueueSource, 6 * Gb, 6, 3, 3, 0, 100 * Gb, 100, 9 * Gb, 9, 2
				, 3 * Gb, 3, 1);
			CheckResources(userSource, 6 * Gb, 6, 3, 3, 0, 10 * Gb, 10, 9 * Gb, 9, 2, 3 * Gb, 
				3, 1);
			CheckResources(parentUserSource, 6 * Gb, 6, 3, 3, 0, 10 * Gb, 10, 9 * Gb, 9, 2, 3
				 * Gb, 3, 1);
			metrics.ReleaseResources(user, 1, Resources.CreateResource(2 * Gb, 2));
			metrics.UnreserveResource(user, Resources.CreateResource(3 * Gb, 3));
			CheckResources(queueSource, 4 * Gb, 4, 2, 3, 1, 100 * Gb, 100, 9 * Gb, 9, 2, 0, 0
				, 0);
			CheckResources(parentQueueSource, 4 * Gb, 4, 2, 3, 1, 100 * Gb, 100, 9 * Gb, 9, 2
				, 0, 0, 0);
			CheckResources(userSource, 4 * Gb, 4, 2, 3, 1, 10 * Gb, 10, 9 * Gb, 9, 2, 0, 0, 0
				);
			CheckResources(parentUserSource, 4 * Gb, 4, 2, 3, 1, 10 * Gb, 10, 9 * Gb, 9, 2, 0
				, 0, 0);
			metrics.FinishAppAttempt(app.GetApplicationId(), app.IsPending(), app.GetUser());
			CheckApps(queueSource, 1, 0, 0, 0, 0, 0, true);
			CheckApps(parentQueueSource, 1, 0, 0, 0, 0, 0, true);
			CheckApps(userSource, 1, 0, 0, 0, 0, 0, true);
			CheckApps(parentUserSource, 1, 0, 0, 0, 0, 0, true);
			metrics.FinishApp(user, RMAppState.Finished);
			CheckApps(queueSource, 1, 0, 0, 1, 0, 0, true);
			CheckApps(parentQueueSource, 1, 0, 0, 1, 0, 0, true);
			CheckApps(userSource, 1, 0, 0, 1, 0, 0, true);
			CheckApps(parentUserSource, 1, 0, 0, 1, 0, 0, true);
		}

		[NUnit.Framework.Test]
		public virtual void TestMetricsCache()
		{
			MetricsSystem ms = new MetricsSystemImpl("cache");
			ms.Start();
			try
			{
				string p1 = "root1";
				string leafQueueName = "root1.leaf";
				QueueMetrics p1Metrics = QueueMetrics.ForQueue(ms, p1, null, true, conf);
				Queue parentQueue1 = MockitoMaker.Make(MockitoMaker.Stub<Queue>().Returning(p1Metrics
					).from.GetMetrics());
				QueueMetrics metrics = QueueMetrics.ForQueue(ms, leafQueueName, parentQueue1, true
					, conf);
				NUnit.Framework.Assert.IsNotNull("QueueMetrics for A shoudn't be null", metrics);
				// Re-register to check for cache hit, shouldn't blow up metrics-system...
				// also, verify parent-metrics
				QueueMetrics alterMetrics = QueueMetrics.ForQueue(ms, leafQueueName, parentQueue1
					, true, conf);
				NUnit.Framework.Assert.IsNotNull("QueueMetrics for alterMetrics shoudn't be null"
					, alterMetrics);
			}
			finally
			{
				ms.Shutdown();
			}
		}

		[NUnit.Framework.Test]
		public virtual void TestMetricsInitializedOnRMInit()
		{
			YarnConfiguration conf = new YarnConfiguration();
			conf.SetClass(YarnConfiguration.RmScheduler, typeof(FifoScheduler), typeof(ResourceScheduler
				));
			MockRM rm = new MockRM(conf);
			QueueMetrics metrics = rm.GetResourceScheduler().GetRootQueueMetrics();
			CheckApps(metrics, 0, 0, 0, 0, 0, 0, true);
			MetricsAsserts.AssertGauge("ReservedContainers", 0, metrics);
		}

		// This is to test all metrics can consistently show up if specified true to
		// collect all metrics, even though they are not modified from last time they
		// are collected. If not collecting all metrics, only modified metrics will show up.
		[NUnit.Framework.Test]
		public virtual void TestCollectAllMetrics()
		{
			string queueName = "single";
			QueueMetrics.ForQueue(ms, queueName, null, false, conf);
			MetricsSource queueSource = QueueSource(ms, queueName);
			CheckApps(queueSource, 0, 0, 0, 0, 0, 0, true);
			try
			{
				// do not collect all metrics
				CheckApps(queueSource, 0, 0, 0, 0, 0, 0, false);
				NUnit.Framework.Assert.Fail();
			}
			catch (Exception e)
			{
				NUnit.Framework.Assert.IsTrue(e.Message.Contains("Expected exactly one metric for name "
					));
			}
			// collect all metrics
			CheckApps(queueSource, 0, 0, 0, 0, 0, 0, true);
		}

		public static void CheckApps(MetricsSource source, int submitted, int pending, int
			 running, int completed, int failed, int killed, bool all)
		{
			MetricsRecordBuilder rb = MetricsAsserts.GetMetrics(source, all);
			MetricsAsserts.AssertCounter("AppsSubmitted", submitted, rb);
			MetricsAsserts.AssertGauge("AppsPending", pending, rb);
			MetricsAsserts.AssertGauge("AppsRunning", running, rb);
			MetricsAsserts.AssertCounter("AppsCompleted", completed, rb);
			MetricsAsserts.AssertCounter("AppsFailed", failed, rb);
			MetricsAsserts.AssertCounter("AppsKilled", killed, rb);
		}

		public static void CheckResources(MetricsSource source, int allocatedMB, int allocatedCores
			, int allocCtnrs, long aggreAllocCtnrs, long aggreReleasedCtnrs, int availableMB
			, int availableCores, int pendingMB, int pendingCores, int pendingCtnrs, int reservedMB
			, int reservedCores, int reservedCtnrs)
		{
			MetricsRecordBuilder rb = MetricsAsserts.GetMetrics(source);
			MetricsAsserts.AssertGauge("AllocatedMB", allocatedMB, rb);
			MetricsAsserts.AssertGauge("AllocatedVCores", allocatedCores, rb);
			MetricsAsserts.AssertGauge("AllocatedContainers", allocCtnrs, rb);
			MetricsAsserts.AssertCounter("AggregateContainersAllocated", aggreAllocCtnrs, rb);
			MetricsAsserts.AssertCounter("AggregateContainersReleased", aggreReleasedCtnrs, rb
				);
			MetricsAsserts.AssertGauge("AvailableMB", availableMB, rb);
			MetricsAsserts.AssertGauge("AvailableVCores", availableCores, rb);
			MetricsAsserts.AssertGauge("PendingMB", pendingMB, rb);
			MetricsAsserts.AssertGauge("PendingVCores", pendingCores, rb);
			MetricsAsserts.AssertGauge("PendingContainers", pendingCtnrs, rb);
			MetricsAsserts.AssertGauge("ReservedMB", reservedMB, rb);
			MetricsAsserts.AssertGauge("ReservedVCores", reservedCores, rb);
			MetricsAsserts.AssertGauge("ReservedContainers", reservedCtnrs, rb);
		}

		private static AppSchedulingInfo MockApp(string user)
		{
			AppSchedulingInfo app = Org.Mockito.Mockito.Mock<AppSchedulingInfo>();
			Org.Mockito.Mockito.When(app.GetUser()).ThenReturn(user);
			ApplicationId appId = BuilderUtils.NewApplicationId(1, 1);
			ApplicationAttemptId id = BuilderUtils.NewApplicationAttemptId(appId, 1);
			Org.Mockito.Mockito.When(app.GetApplicationAttemptId()).ThenReturn(id);
			return app;
		}

		public static MetricsSource QueueSource(MetricsSystem ms, string queue)
		{
			MetricsSource s = ms.GetSource(QueueMetrics.SourceName(queue).ToString());
			return s;
		}

		public static MetricsSource UserSource(MetricsSystem ms, string queue, string user
			)
		{
			MetricsSource s = ms.GetSource(QueueMetrics.SourceName(queue).Append(",user=").Append
				(user).ToString());
			return s;
		}
	}
}
