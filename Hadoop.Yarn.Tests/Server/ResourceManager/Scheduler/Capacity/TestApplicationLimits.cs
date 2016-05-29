using System.Collections.Generic;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Factories;
using Org.Apache.Hadoop.Yarn.Factory.Providers;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Common.Fica;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Security;
using Org.Apache.Hadoop.Yarn.Util.Resource;
using Org.Mockito;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Capacity
{
	public class TestApplicationLimits
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(TestApplicationLimits)
			);

		internal const int Gb = 1024;

		internal LeafQueue queue;

		private readonly ResourceCalculator resourceCalculator = new DefaultResourceCalculator
			();

		internal RMContext rmContext = null;

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.SetUp]
		public virtual void SetUp()
		{
			CapacitySchedulerConfiguration csConf = new CapacitySchedulerConfiguration();
			YarnConfiguration conf = new YarnConfiguration();
			SetupQueueConfiguration(csConf);
			rmContext = TestUtils.GetMockRMContext();
			CapacitySchedulerContext csContext = Org.Mockito.Mockito.Mock<CapacitySchedulerContext
				>();
			Org.Mockito.Mockito.When(csContext.GetConfiguration()).ThenReturn(csConf);
			Org.Mockito.Mockito.When(csContext.GetConf()).ThenReturn(conf);
			Org.Mockito.Mockito.When(csContext.GetMinimumResourceCapability()).ThenReturn(Resources
				.CreateResource(Gb, 1));
			Org.Mockito.Mockito.When(csContext.GetMaximumResourceCapability()).ThenReturn(Resources
				.CreateResource(16 * Gb, 32));
			Org.Mockito.Mockito.When(csContext.GetClusterResource()).ThenReturn(Resources.CreateResource
				(10 * 16 * Gb, 10 * 32));
			Org.Mockito.Mockito.When(csContext.GetApplicationComparator()).ThenReturn(CapacityScheduler
				.applicationComparator);
			Org.Mockito.Mockito.When(csContext.GetQueueComparator()).ThenReturn(CapacityScheduler
				.queueComparator);
			Org.Mockito.Mockito.When(csContext.GetResourceCalculator()).ThenReturn(resourceCalculator
				);
			Org.Mockito.Mockito.When(csContext.GetRMContext()).ThenReturn(rmContext);
			RMContainerTokenSecretManager containerTokenSecretManager = new RMContainerTokenSecretManager
				(conf);
			containerTokenSecretManager.RollMasterKey();
			Org.Mockito.Mockito.When(csContext.GetContainerTokenSecretManager()).ThenReturn(containerTokenSecretManager
				);
			IDictionary<string, CSQueue> queues = new Dictionary<string, CSQueue>();
			CSQueue root = CapacityScheduler.ParseQueue(csContext, csConf, null, "root", queues
				, queues, TestUtils.spyHook);
			queue = Org.Mockito.Mockito.Spy(new LeafQueue(csContext, A, root, null));
			// Stub out ACL checks
			Org.Mockito.Mockito.DoReturn(true).When(queue).HasAccess(Matchers.Any<QueueACL>()
				, Matchers.Any<UserGroupInformation>());
			// Some default values
			Org.Mockito.Mockito.DoReturn(100).When(queue).GetMaxApplications();
			Org.Mockito.Mockito.DoReturn(25).When(queue).GetMaxApplicationsPerUser();
		}

		private const string A = "a";

		private const string B = "b";

		private void SetupQueueConfiguration(CapacitySchedulerConfiguration conf)
		{
			// Define top-level queues
			conf.SetQueues(CapacitySchedulerConfiguration.Root, new string[] { A, B });
			string QA = CapacitySchedulerConfiguration.Root + "." + A;
			conf.SetCapacity(QA, 10);
			string QB = CapacitySchedulerConfiguration.Root + "." + B;
			conf.SetCapacity(QB, 90);
			conf.SetUserLimit(CapacitySchedulerConfiguration.Root + "." + A, 50);
			conf.SetUserLimitFactor(CapacitySchedulerConfiguration.Root + "." + A, 5.0f);
			Log.Info("Setup top-level queues a and b");
		}

		private FiCaSchedulerApp GetMockApplication(int appId, string user, Org.Apache.Hadoop.Yarn.Api.Records.Resource
			 amResource)
		{
			FiCaSchedulerApp application = Org.Mockito.Mockito.Mock<FiCaSchedulerApp>();
			ApplicationAttemptId applicationAttemptId = TestUtils.GetMockApplicationAttemptId
				(appId, 0);
			Org.Mockito.Mockito.DoReturn(applicationAttemptId.GetApplicationId()).When(application
				).GetApplicationId();
			Org.Mockito.Mockito.DoReturn(applicationAttemptId).When(application).GetApplicationAttemptId
				();
			Org.Mockito.Mockito.DoReturn(user).When(application).GetUser();
			Org.Mockito.Mockito.DoReturn(amResource).When(application).GetAMResource();
			return application;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAMResourceLimit()
		{
			string user_0 = "user_0";
			string user_1 = "user_1";
			// This uses the default 10% of cluster value for the max am resources
			// which are allowed, at 80GB = 8GB for AM's at the queue level.  The user
			// am limit is 4G initially (based on the queue absolute capacity)
			// when there is only 1 user, and drops to 2G (the userlimit) when there
			// is a second user
			Org.Apache.Hadoop.Yarn.Api.Records.Resource clusterResource = Org.Apache.Hadoop.Yarn.Api.Records.Resource
				.NewInstance(80 * Gb, 40);
			queue.UpdateClusterResource(clusterResource, new ResourceLimits(clusterResource));
			ActiveUsersManager activeUsersManager = Org.Mockito.Mockito.Mock<ActiveUsersManager
				>();
			Org.Mockito.Mockito.When(queue.GetActiveUsersManager()).ThenReturn(activeUsersManager
				);
			NUnit.Framework.Assert.AreEqual(Org.Apache.Hadoop.Yarn.Api.Records.Resource.NewInstance
				(8 * Gb, 1), queue.GetAMResourceLimit());
			NUnit.Framework.Assert.AreEqual(Org.Apache.Hadoop.Yarn.Api.Records.Resource.NewInstance
				(4 * Gb, 1), queue.GetUserAMResourceLimit());
			// Two apps for user_0, both start
			int ApplicationId = 0;
			FiCaSchedulerApp app_0 = GetMockApplication(ApplicationId++, user_0, Org.Apache.Hadoop.Yarn.Api.Records.Resource
				.NewInstance(2 * Gb, 1));
			queue.SubmitApplicationAttempt(app_0, user_0);
			NUnit.Framework.Assert.AreEqual(1, queue.GetNumActiveApplications());
			NUnit.Framework.Assert.AreEqual(0, queue.GetNumPendingApplications());
			NUnit.Framework.Assert.AreEqual(1, queue.GetNumActiveApplications(user_0));
			NUnit.Framework.Assert.AreEqual(0, queue.GetNumPendingApplications(user_0));
			Org.Mockito.Mockito.When(activeUsersManager.GetNumActiveUsers()).ThenReturn(1);
			FiCaSchedulerApp app_1 = GetMockApplication(ApplicationId++, user_0, Org.Apache.Hadoop.Yarn.Api.Records.Resource
				.NewInstance(2 * Gb, 1));
			queue.SubmitApplicationAttempt(app_1, user_0);
			NUnit.Framework.Assert.AreEqual(2, queue.GetNumActiveApplications());
			NUnit.Framework.Assert.AreEqual(0, queue.GetNumPendingApplications());
			NUnit.Framework.Assert.AreEqual(2, queue.GetNumActiveApplications(user_0));
			NUnit.Framework.Assert.AreEqual(0, queue.GetNumPendingApplications(user_0));
			// AMLimits unchanged
			NUnit.Framework.Assert.AreEqual(Org.Apache.Hadoop.Yarn.Api.Records.Resource.NewInstance
				(8 * Gb, 1), queue.GetAMResourceLimit());
			NUnit.Framework.Assert.AreEqual(Org.Apache.Hadoop.Yarn.Api.Records.Resource.NewInstance
				(4 * Gb, 1), queue.GetUserAMResourceLimit());
			// One app for user_1, starts
			FiCaSchedulerApp app_2 = GetMockApplication(ApplicationId++, user_1, Org.Apache.Hadoop.Yarn.Api.Records.Resource
				.NewInstance(2 * Gb, 1));
			queue.SubmitApplicationAttempt(app_2, user_1);
			NUnit.Framework.Assert.AreEqual(3, queue.GetNumActiveApplications());
			NUnit.Framework.Assert.AreEqual(0, queue.GetNumPendingApplications());
			NUnit.Framework.Assert.AreEqual(1, queue.GetNumActiveApplications(user_1));
			NUnit.Framework.Assert.AreEqual(0, queue.GetNumPendingApplications(user_1));
			Org.Mockito.Mockito.When(activeUsersManager.GetNumActiveUsers()).ThenReturn(2);
			// Now userAMResourceLimit drops to the queue configured 50% as there is
			// another user active
			NUnit.Framework.Assert.AreEqual(Org.Apache.Hadoop.Yarn.Api.Records.Resource.NewInstance
				(8 * Gb, 1), queue.GetAMResourceLimit());
			NUnit.Framework.Assert.AreEqual(Org.Apache.Hadoop.Yarn.Api.Records.Resource.NewInstance
				(2 * Gb, 1), queue.GetUserAMResourceLimit());
			// Second user_1 app cannot start
			FiCaSchedulerApp app_3 = GetMockApplication(ApplicationId++, user_1, Org.Apache.Hadoop.Yarn.Api.Records.Resource
				.NewInstance(2 * Gb, 1));
			queue.SubmitApplicationAttempt(app_3, user_1);
			NUnit.Framework.Assert.AreEqual(3, queue.GetNumActiveApplications());
			NUnit.Framework.Assert.AreEqual(1, queue.GetNumPendingApplications());
			NUnit.Framework.Assert.AreEqual(1, queue.GetNumActiveApplications(user_1));
			NUnit.Framework.Assert.AreEqual(1, queue.GetNumPendingApplications(user_1));
			// Now finish app so another should be activated
			queue.FinishApplicationAttempt(app_2, A);
			NUnit.Framework.Assert.AreEqual(3, queue.GetNumActiveApplications());
			NUnit.Framework.Assert.AreEqual(0, queue.GetNumPendingApplications());
			NUnit.Framework.Assert.AreEqual(1, queue.GetNumActiveApplications(user_1));
			NUnit.Framework.Assert.AreEqual(0, queue.GetNumPendingApplications(user_1));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestLimitsComputation()
		{
			CapacitySchedulerConfiguration csConf = new CapacitySchedulerConfiguration();
			SetupQueueConfiguration(csConf);
			YarnConfiguration conf = new YarnConfiguration();
			CapacitySchedulerContext csContext = Org.Mockito.Mockito.Mock<CapacitySchedulerContext
				>();
			Org.Mockito.Mockito.When(csContext.GetConfiguration()).ThenReturn(csConf);
			Org.Mockito.Mockito.When(csContext.GetConf()).ThenReturn(conf);
			Org.Mockito.Mockito.When(csContext.GetMinimumResourceCapability()).ThenReturn(Resources
				.CreateResource(Gb, 1));
			Org.Mockito.Mockito.When(csContext.GetMaximumResourceCapability()).ThenReturn(Resources
				.CreateResource(16 * Gb, 16));
			Org.Mockito.Mockito.When(csContext.GetApplicationComparator()).ThenReturn(CapacityScheduler
				.applicationComparator);
			Org.Mockito.Mockito.When(csContext.GetQueueComparator()).ThenReturn(CapacityScheduler
				.queueComparator);
			Org.Mockito.Mockito.When(csContext.GetResourceCalculator()).ThenReturn(resourceCalculator
				);
			Org.Mockito.Mockito.When(csContext.GetRMContext()).ThenReturn(rmContext);
			// Say cluster has 100 nodes of 16G each
			Org.Apache.Hadoop.Yarn.Api.Records.Resource clusterResource = Resources.CreateResource
				(100 * 16 * Gb, 100 * 16);
			Org.Mockito.Mockito.When(csContext.GetClusterResource()).ThenReturn(clusterResource
				);
			IDictionary<string, CSQueue> queues = new Dictionary<string, CSQueue>();
			CSQueue root = CapacityScheduler.ParseQueue(csContext, csConf, null, "root", queues
				, queues, TestUtils.spyHook);
			LeafQueue queue = (LeafQueue)queues[A];
			Log.Info("Queue 'A' -" + " AMResourceLimit=" + queue.GetAMResourceLimit() + " UserAMResourceLimit="
				 + queue.GetUserAMResourceLimit());
			NUnit.Framework.Assert.AreEqual(queue.GetAMResourceLimit(), Org.Apache.Hadoop.Yarn.Api.Records.Resource
				.NewInstance(160 * Gb, 1));
			NUnit.Framework.Assert.AreEqual(queue.GetUserAMResourceLimit(), Org.Apache.Hadoop.Yarn.Api.Records.Resource
				.NewInstance(80 * Gb, 1));
			NUnit.Framework.Assert.AreEqual((int)(clusterResource.GetMemory() * queue.GetAbsoluteCapacity
				()), queue.GetMetrics().GetAvailableMB());
			// Add some nodes to the cluster & test new limits
			clusterResource = Resources.CreateResource(120 * 16 * Gb);
			root.UpdateClusterResource(clusterResource, new ResourceLimits(clusterResource));
			NUnit.Framework.Assert.AreEqual(queue.GetAMResourceLimit(), Org.Apache.Hadoop.Yarn.Api.Records.Resource
				.NewInstance(192 * Gb, 1));
			NUnit.Framework.Assert.AreEqual(queue.GetUserAMResourceLimit(), Org.Apache.Hadoop.Yarn.Api.Records.Resource
				.NewInstance(96 * Gb, 1));
			NUnit.Framework.Assert.AreEqual((int)(clusterResource.GetMemory() * queue.GetAbsoluteCapacity
				()), queue.GetMetrics().GetAvailableMB());
			// should return -1 if per queue setting not set
			NUnit.Framework.Assert.AreEqual((int)CapacitySchedulerConfiguration.Undefined, csConf
				.GetMaximumApplicationsPerQueue(queue.GetQueuePath()));
			int expectedMaxApps = (int)(CapacitySchedulerConfiguration.DefaultMaximumSystemApplicatiions
				 * queue.GetAbsoluteCapacity());
			NUnit.Framework.Assert.AreEqual(expectedMaxApps, queue.GetMaxApplications());
			int expectedMaxAppsPerUser = (int)(expectedMaxApps * (queue.GetUserLimit() / 100.0f
				) * queue.GetUserLimitFactor());
			NUnit.Framework.Assert.AreEqual(expectedMaxAppsPerUser, queue.GetMaxApplicationsPerUser
				());
			// should default to global setting if per queue setting not set
			NUnit.Framework.Assert.AreEqual((long)CapacitySchedulerConfiguration.DefaultMaximumApplicationmastersResourcePercent
				, (long)csConf.GetMaximumApplicationMasterResourcePerQueuePercent(queue.GetQueuePath
				()));
			// Change the per-queue max AM resources percentage.
			csConf.SetFloat("yarn.scheduler.capacity." + queue.GetQueuePath() + ".maximum-am-resource-percent"
				, 0.5f);
			// Re-create queues to get new configs.
			queues = new Dictionary<string, CSQueue>();
			root = CapacityScheduler.ParseQueue(csContext, csConf, null, "root", queues, queues
				, TestUtils.spyHook);
			clusterResource = Resources.CreateResource(100 * 16 * Gb);
			queue = (LeafQueue)queues[A];
			NUnit.Framework.Assert.AreEqual((long)0.5, (long)csConf.GetMaximumApplicationMasterResourcePerQueuePercent
				(queue.GetQueuePath()));
			NUnit.Framework.Assert.AreEqual(queue.GetAMResourceLimit(), Org.Apache.Hadoop.Yarn.Api.Records.Resource
				.NewInstance(800 * Gb, 1));
			NUnit.Framework.Assert.AreEqual(queue.GetUserAMResourceLimit(), Org.Apache.Hadoop.Yarn.Api.Records.Resource
				.NewInstance(400 * Gb, 1));
			// Change the per-queue max applications.
			csConf.SetInt("yarn.scheduler.capacity." + queue.GetQueuePath() + ".maximum-applications"
				, 9999);
			// Re-create queues to get new configs.
			queues = new Dictionary<string, CSQueue>();
			root = CapacityScheduler.ParseQueue(csContext, csConf, null, "root", queues, queues
				, TestUtils.spyHook);
			queue = (LeafQueue)queues[A];
			NUnit.Framework.Assert.AreEqual(9999, (int)csConf.GetMaximumApplicationsPerQueue(
				queue.GetQueuePath()));
			NUnit.Framework.Assert.AreEqual(9999, queue.GetMaxApplications());
			expectedMaxAppsPerUser = (int)(9999 * (queue.GetUserLimit() / 100.0f) * queue.GetUserLimitFactor
				());
			NUnit.Framework.Assert.AreEqual(expectedMaxAppsPerUser, queue.GetMaxApplicationsPerUser
				());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestActiveApplicationLimits()
		{
			string user_0 = "user_0";
			string user_1 = "user_1";
			string user_2 = "user_2";
			NUnit.Framework.Assert.AreEqual(Org.Apache.Hadoop.Yarn.Api.Records.Resource.NewInstance
				(16 * Gb, 1), queue.GetAMResourceLimit());
			NUnit.Framework.Assert.AreEqual(Org.Apache.Hadoop.Yarn.Api.Records.Resource.NewInstance
				(8 * Gb, 1), queue.GetUserAMResourceLimit());
			int ApplicationId = 0;
			// Submit first application
			FiCaSchedulerApp app_0 = GetMockApplication(ApplicationId++, user_0, Resources.CreateResource
				(4 * Gb, 0));
			queue.SubmitApplicationAttempt(app_0, user_0);
			NUnit.Framework.Assert.AreEqual(1, queue.GetNumActiveApplications());
			NUnit.Framework.Assert.AreEqual(0, queue.GetNumPendingApplications());
			NUnit.Framework.Assert.AreEqual(1, queue.GetNumActiveApplications(user_0));
			NUnit.Framework.Assert.AreEqual(0, queue.GetNumPendingApplications(user_0));
			// Submit second application
			FiCaSchedulerApp app_1 = GetMockApplication(ApplicationId++, user_0, Resources.CreateResource
				(4 * Gb, 0));
			queue.SubmitApplicationAttempt(app_1, user_0);
			NUnit.Framework.Assert.AreEqual(2, queue.GetNumActiveApplications());
			NUnit.Framework.Assert.AreEqual(0, queue.GetNumPendingApplications());
			NUnit.Framework.Assert.AreEqual(2, queue.GetNumActiveApplications(user_0));
			NUnit.Framework.Assert.AreEqual(0, queue.GetNumPendingApplications(user_0));
			// Submit third application, should remain pending due to user amlimit
			FiCaSchedulerApp app_2 = GetMockApplication(ApplicationId++, user_0, Resources.CreateResource
				(4 * Gb, 0));
			queue.SubmitApplicationAttempt(app_2, user_0);
			NUnit.Framework.Assert.AreEqual(2, queue.GetNumActiveApplications());
			NUnit.Framework.Assert.AreEqual(1, queue.GetNumPendingApplications());
			NUnit.Framework.Assert.AreEqual(2, queue.GetNumActiveApplications(user_0));
			NUnit.Framework.Assert.AreEqual(1, queue.GetNumPendingApplications(user_0));
			// Finish one application, app_2 should be activated
			queue.FinishApplicationAttempt(app_0, A);
			NUnit.Framework.Assert.AreEqual(2, queue.GetNumActiveApplications());
			NUnit.Framework.Assert.AreEqual(0, queue.GetNumPendingApplications());
			NUnit.Framework.Assert.AreEqual(2, queue.GetNumActiveApplications(user_0));
			NUnit.Framework.Assert.AreEqual(0, queue.GetNumPendingApplications(user_0));
			// Submit another one for user_0
			FiCaSchedulerApp app_3 = GetMockApplication(ApplicationId++, user_0, Resources.CreateResource
				(4 * Gb, 0));
			queue.SubmitApplicationAttempt(app_3, user_0);
			NUnit.Framework.Assert.AreEqual(2, queue.GetNumActiveApplications());
			NUnit.Framework.Assert.AreEqual(1, queue.GetNumPendingApplications());
			NUnit.Framework.Assert.AreEqual(2, queue.GetNumActiveApplications(user_0));
			NUnit.Framework.Assert.AreEqual(1, queue.GetNumPendingApplications(user_0));
			// Submit first app for user_1
			FiCaSchedulerApp app_4 = GetMockApplication(ApplicationId++, user_1, Resources.CreateResource
				(8 * Gb, 0));
			queue.SubmitApplicationAttempt(app_4, user_1);
			NUnit.Framework.Assert.AreEqual(3, queue.GetNumActiveApplications());
			NUnit.Framework.Assert.AreEqual(1, queue.GetNumPendingApplications());
			NUnit.Framework.Assert.AreEqual(2, queue.GetNumActiveApplications(user_0));
			NUnit.Framework.Assert.AreEqual(1, queue.GetNumPendingApplications(user_0));
			NUnit.Framework.Assert.AreEqual(1, queue.GetNumActiveApplications(user_1));
			NUnit.Framework.Assert.AreEqual(0, queue.GetNumPendingApplications(user_1));
			// Submit first app for user_2, should block due to queue amlimit
			FiCaSchedulerApp app_5 = GetMockApplication(ApplicationId++, user_2, Resources.CreateResource
				(8 * Gb, 0));
			queue.SubmitApplicationAttempt(app_5, user_2);
			NUnit.Framework.Assert.AreEqual(3, queue.GetNumActiveApplications());
			NUnit.Framework.Assert.AreEqual(2, queue.GetNumPendingApplications());
			NUnit.Framework.Assert.AreEqual(2, queue.GetNumActiveApplications(user_0));
			NUnit.Framework.Assert.AreEqual(1, queue.GetNumPendingApplications(user_0));
			NUnit.Framework.Assert.AreEqual(1, queue.GetNumActiveApplications(user_1));
			NUnit.Framework.Assert.AreEqual(0, queue.GetNumPendingApplications(user_1));
			NUnit.Framework.Assert.AreEqual(1, queue.GetNumPendingApplications(user_2));
			// Now finish one app of user_1 so app_5 should be activated
			queue.FinishApplicationAttempt(app_4, A);
			NUnit.Framework.Assert.AreEqual(3, queue.GetNumActiveApplications());
			NUnit.Framework.Assert.AreEqual(1, queue.GetNumPendingApplications());
			NUnit.Framework.Assert.AreEqual(2, queue.GetNumActiveApplications(user_0));
			NUnit.Framework.Assert.AreEqual(1, queue.GetNumPendingApplications(user_0));
			NUnit.Framework.Assert.AreEqual(0, queue.GetNumActiveApplications(user_1));
			NUnit.Framework.Assert.AreEqual(0, queue.GetNumPendingApplications(user_1));
			NUnit.Framework.Assert.AreEqual(1, queue.GetNumActiveApplications(user_2));
			NUnit.Framework.Assert.AreEqual(0, queue.GetNumPendingApplications(user_2));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestActiveLimitsWithKilledApps()
		{
			string user_0 = "user_0";
			int ApplicationId = 0;
			// Submit first application
			FiCaSchedulerApp app_0 = GetMockApplication(ApplicationId++, user_0, Resources.CreateResource
				(4 * Gb, 0));
			queue.SubmitApplicationAttempt(app_0, user_0);
			NUnit.Framework.Assert.AreEqual(1, queue.GetNumActiveApplications());
			NUnit.Framework.Assert.AreEqual(0, queue.GetNumPendingApplications());
			NUnit.Framework.Assert.AreEqual(1, queue.GetNumActiveApplications(user_0));
			NUnit.Framework.Assert.AreEqual(0, queue.GetNumPendingApplications(user_0));
			NUnit.Framework.Assert.IsTrue(queue.activeApplications.Contains(app_0));
			// Submit second application
			FiCaSchedulerApp app_1 = GetMockApplication(ApplicationId++, user_0, Resources.CreateResource
				(4 * Gb, 0));
			queue.SubmitApplicationAttempt(app_1, user_0);
			NUnit.Framework.Assert.AreEqual(2, queue.GetNumActiveApplications());
			NUnit.Framework.Assert.AreEqual(0, queue.GetNumPendingApplications());
			NUnit.Framework.Assert.AreEqual(2, queue.GetNumActiveApplications(user_0));
			NUnit.Framework.Assert.AreEqual(0, queue.GetNumPendingApplications(user_0));
			NUnit.Framework.Assert.IsTrue(queue.activeApplications.Contains(app_1));
			// Submit third application, should remain pending
			FiCaSchedulerApp app_2 = GetMockApplication(ApplicationId++, user_0, Resources.CreateResource
				(4 * Gb, 0));
			queue.SubmitApplicationAttempt(app_2, user_0);
			NUnit.Framework.Assert.AreEqual(2, queue.GetNumActiveApplications());
			NUnit.Framework.Assert.AreEqual(1, queue.GetNumPendingApplications());
			NUnit.Framework.Assert.AreEqual(2, queue.GetNumActiveApplications(user_0));
			NUnit.Framework.Assert.AreEqual(1, queue.GetNumPendingApplications(user_0));
			NUnit.Framework.Assert.IsTrue(queue.pendingApplications.Contains(app_2));
			// Submit fourth application, should remain pending
			FiCaSchedulerApp app_3 = GetMockApplication(ApplicationId++, user_0, Resources.CreateResource
				(4 * Gb, 0));
			queue.SubmitApplicationAttempt(app_3, user_0);
			NUnit.Framework.Assert.AreEqual(2, queue.GetNumActiveApplications());
			NUnit.Framework.Assert.AreEqual(2, queue.GetNumPendingApplications());
			NUnit.Framework.Assert.AreEqual(2, queue.GetNumActiveApplications(user_0));
			NUnit.Framework.Assert.AreEqual(2, queue.GetNumPendingApplications(user_0));
			NUnit.Framework.Assert.IsTrue(queue.pendingApplications.Contains(app_3));
			// Kill 3rd pending application
			queue.FinishApplicationAttempt(app_2, A);
			NUnit.Framework.Assert.AreEqual(2, queue.GetNumActiveApplications());
			NUnit.Framework.Assert.AreEqual(1, queue.GetNumPendingApplications());
			NUnit.Framework.Assert.AreEqual(2, queue.GetNumActiveApplications(user_0));
			NUnit.Framework.Assert.AreEqual(1, queue.GetNumPendingApplications(user_0));
			NUnit.Framework.Assert.IsFalse(queue.pendingApplications.Contains(app_2));
			NUnit.Framework.Assert.IsFalse(queue.activeApplications.Contains(app_2));
			// Finish 1st application, app_3 should become active
			queue.FinishApplicationAttempt(app_0, A);
			NUnit.Framework.Assert.AreEqual(2, queue.GetNumActiveApplications());
			NUnit.Framework.Assert.AreEqual(0, queue.GetNumPendingApplications());
			NUnit.Framework.Assert.AreEqual(2, queue.GetNumActiveApplications(user_0));
			NUnit.Framework.Assert.AreEqual(0, queue.GetNumPendingApplications(user_0));
			NUnit.Framework.Assert.IsTrue(queue.activeApplications.Contains(app_3));
			NUnit.Framework.Assert.IsFalse(queue.pendingApplications.Contains(app_3));
			NUnit.Framework.Assert.IsFalse(queue.activeApplications.Contains(app_0));
			// Finish 2nd application
			queue.FinishApplicationAttempt(app_1, A);
			NUnit.Framework.Assert.AreEqual(1, queue.GetNumActiveApplications());
			NUnit.Framework.Assert.AreEqual(0, queue.GetNumPendingApplications());
			NUnit.Framework.Assert.AreEqual(1, queue.GetNumActiveApplications(user_0));
			NUnit.Framework.Assert.AreEqual(0, queue.GetNumPendingApplications(user_0));
			NUnit.Framework.Assert.IsFalse(queue.activeApplications.Contains(app_1));
			// Finish 4th application
			queue.FinishApplicationAttempt(app_3, A);
			NUnit.Framework.Assert.AreEqual(0, queue.GetNumActiveApplications());
			NUnit.Framework.Assert.AreEqual(0, queue.GetNumPendingApplications());
			NUnit.Framework.Assert.AreEqual(0, queue.GetNumActiveApplications(user_0));
			NUnit.Framework.Assert.AreEqual(0, queue.GetNumPendingApplications(user_0));
			NUnit.Framework.Assert.IsFalse(queue.activeApplications.Contains(app_3));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestHeadroom()
		{
			CapacitySchedulerConfiguration csConf = new CapacitySchedulerConfiguration();
			csConf.SetUserLimit(CapacitySchedulerConfiguration.Root + "." + A, 25);
			SetupQueueConfiguration(csConf);
			YarnConfiguration conf = new YarnConfiguration();
			CapacitySchedulerContext csContext = Org.Mockito.Mockito.Mock<CapacitySchedulerContext
				>();
			Org.Mockito.Mockito.When(csContext.GetConfiguration()).ThenReturn(csConf);
			Org.Mockito.Mockito.When(csContext.GetConf()).ThenReturn(conf);
			Org.Mockito.Mockito.When(csContext.GetMinimumResourceCapability()).ThenReturn(Resources
				.CreateResource(Gb));
			Org.Mockito.Mockito.When(csContext.GetMaximumResourceCapability()).ThenReturn(Resources
				.CreateResource(16 * Gb));
			Org.Mockito.Mockito.When(csContext.GetApplicationComparator()).ThenReturn(CapacityScheduler
				.applicationComparator);
			Org.Mockito.Mockito.When(csContext.GetQueueComparator()).ThenReturn(CapacityScheduler
				.queueComparator);
			Org.Mockito.Mockito.When(csContext.GetResourceCalculator()).ThenReturn(resourceCalculator
				);
			Org.Mockito.Mockito.When(csContext.GetRMContext()).ThenReturn(rmContext);
			// Say cluster has 100 nodes of 16G each
			Org.Apache.Hadoop.Yarn.Api.Records.Resource clusterResource = Resources.CreateResource
				(100 * 16 * Gb);
			Org.Mockito.Mockito.When(csContext.GetClusterResource()).ThenReturn(clusterResource
				);
			IDictionary<string, CSQueue> queues = new Dictionary<string, CSQueue>();
			CapacityScheduler.ParseQueue(csContext, csConf, null, "root", queues, queues, TestUtils
				.spyHook);
			// Manipulate queue 'a'
			LeafQueue queue = TestLeafQueue.StubLeafQueue((LeafQueue)queues[A]);
			string host_0 = "host_0";
			string rack_0 = "rack_0";
			FiCaSchedulerNode node_0 = TestUtils.GetMockNode(host_0, rack_0, 0, 16 * Gb);
			string user_0 = "user_0";
			string user_1 = "user_1";
			RecordFactory recordFactory = RecordFactoryProvider.GetRecordFactory(null);
			RMContext rmContext = TestUtils.GetMockRMContext();
			RMContext spyRMContext = Org.Mockito.Mockito.Spy(rmContext);
			ConcurrentMap<ApplicationId, RMApp> spyApps = Org.Mockito.Mockito.Spy(new ConcurrentHashMap
				<ApplicationId, RMApp>());
			RMApp rmApp = Org.Mockito.Mockito.Mock<RMApp>();
			ResourceRequest amResourceRequest = Org.Mockito.Mockito.Mock<ResourceRequest>();
			Org.Apache.Hadoop.Yarn.Api.Records.Resource amResource = Resources.CreateResource
				(0, 0);
			Org.Mockito.Mockito.When(amResourceRequest.GetCapability()).ThenReturn(amResource
				);
			Org.Mockito.Mockito.When(rmApp.GetAMResourceRequest()).ThenReturn(amResourceRequest
				);
			Org.Mockito.Mockito.DoReturn(rmApp).When(spyApps)[(ApplicationId)Matchers.Any()];
			Org.Mockito.Mockito.When(spyRMContext.GetRMApps()).ThenReturn(spyApps);
			Priority priority_1 = TestUtils.CreateMockPriority(1);
			// Submit first application with some resource-requests from user_0, 
			// and check headroom
			ApplicationAttemptId appAttemptId_0_0 = TestUtils.GetMockApplicationAttemptId(0, 
				0);
			FiCaSchedulerApp app_0_0 = new FiCaSchedulerApp(appAttemptId_0_0, user_0, queue, 
				queue.GetActiveUsersManager(), spyRMContext);
			queue.SubmitApplicationAttempt(app_0_0, user_0);
			IList<ResourceRequest> app_0_0_requests = new AList<ResourceRequest>();
			app_0_0_requests.AddItem(TestUtils.CreateResourceRequest(ResourceRequest.Any, 1 *
				 Gb, 2, true, priority_1, recordFactory));
			app_0_0.UpdateResourceRequests(app_0_0_requests);
			// Schedule to compute 
			queue.AssignContainers(clusterResource, node_0, new ResourceLimits(clusterResource
				));
			Org.Apache.Hadoop.Yarn.Api.Records.Resource expectedHeadroom = Resources.CreateResource
				(10 * 16 * Gb, 1);
			NUnit.Framework.Assert.AreEqual(expectedHeadroom, app_0_0.GetHeadroom());
			// Submit second application from user_0, check headroom
			ApplicationAttemptId appAttemptId_0_1 = TestUtils.GetMockApplicationAttemptId(1, 
				0);
			FiCaSchedulerApp app_0_1 = new FiCaSchedulerApp(appAttemptId_0_1, user_0, queue, 
				queue.GetActiveUsersManager(), spyRMContext);
			queue.SubmitApplicationAttempt(app_0_1, user_0);
			IList<ResourceRequest> app_0_1_requests = new AList<ResourceRequest>();
			app_0_1_requests.AddItem(TestUtils.CreateResourceRequest(ResourceRequest.Any, 1 *
				 Gb, 2, true, priority_1, recordFactory));
			app_0_1.UpdateResourceRequests(app_0_1_requests);
			// Schedule to compute 
			queue.AssignContainers(clusterResource, node_0, new ResourceLimits(clusterResource
				));
			// Schedule to compute
			NUnit.Framework.Assert.AreEqual(expectedHeadroom, app_0_0.GetHeadroom());
			NUnit.Framework.Assert.AreEqual(expectedHeadroom, app_0_1.GetHeadroom());
			// no change
			// Submit first application from user_1, check  for new headroom
			ApplicationAttemptId appAttemptId_1_0 = TestUtils.GetMockApplicationAttemptId(2, 
				0);
			FiCaSchedulerApp app_1_0 = new FiCaSchedulerApp(appAttemptId_1_0, user_1, queue, 
				queue.GetActiveUsersManager(), spyRMContext);
			queue.SubmitApplicationAttempt(app_1_0, user_1);
			IList<ResourceRequest> app_1_0_requests = new AList<ResourceRequest>();
			app_1_0_requests.AddItem(TestUtils.CreateResourceRequest(ResourceRequest.Any, 1 *
				 Gb, 2, true, priority_1, recordFactory));
			app_1_0.UpdateResourceRequests(app_1_0_requests);
			// Schedule to compute 
			queue.AssignContainers(clusterResource, node_0, new ResourceLimits(clusterResource
				));
			// Schedule to compute
			expectedHeadroom = Resources.CreateResource(10 * 16 * Gb / 2, 1);
			// changes
			NUnit.Framework.Assert.AreEqual(expectedHeadroom, app_0_0.GetHeadroom());
			NUnit.Framework.Assert.AreEqual(expectedHeadroom, app_0_1.GetHeadroom());
			NUnit.Framework.Assert.AreEqual(expectedHeadroom, app_1_0.GetHeadroom());
			// Now reduce cluster size and check for the smaller headroom
			clusterResource = Resources.CreateResource(90 * 16 * Gb);
			queue.AssignContainers(clusterResource, node_0, new ResourceLimits(clusterResource
				));
			// Schedule to compute
			expectedHeadroom = Resources.CreateResource(9 * 16 * Gb / 2, 1);
			// changes
			NUnit.Framework.Assert.AreEqual(expectedHeadroom, app_0_0.GetHeadroom());
			NUnit.Framework.Assert.AreEqual(expectedHeadroom, app_0_1.GetHeadroom());
			NUnit.Framework.Assert.AreEqual(expectedHeadroom, app_1_0.GetHeadroom());
		}

		[NUnit.Framework.TearDown]
		public virtual void TearDown()
		{
		}
	}
}
