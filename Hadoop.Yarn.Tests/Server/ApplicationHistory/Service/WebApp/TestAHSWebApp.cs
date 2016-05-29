using Com.Google.Inject;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Yarn.Api;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Server.Applicationhistoryservice;
using Org.Apache.Hadoop.Yarn.Util;
using Org.Apache.Hadoop.Yarn.Webapp;
using Org.Apache.Hadoop.Yarn.Webapp.Test;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Applicationhistoryservice.Webapp
{
	public class TestAHSWebApp : ApplicationHistoryStoreTestUtils
	{
		public virtual void SetApplicationHistoryStore(ApplicationHistoryStore store)
		{
			this.store = store;
		}

		[SetUp]
		public virtual void Setup()
		{
			store = new MemoryApplicationHistoryStore();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAppControllerIndex()
		{
			ApplicationHistoryManager ahManager = Org.Mockito.Mockito.Mock<ApplicationHistoryManager
				>();
			Injector injector = WebAppTests.CreateMockInjector<ApplicationHistoryManager>(ahManager
				);
			AHSController controller = injector.GetInstance<AHSController>();
			controller.Index();
			NUnit.Framework.Assert.AreEqual("Application History", controller.Get(Params.Title
				, "unknown"));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestView()
		{
			Injector injector = WebAppTests.CreateMockInjector<ApplicationBaseProtocol>(MockApplicationHistoryClientService
				(5, 1, 1));
			AHSView ahsViewInstance = injector.GetInstance<AHSView>();
			ahsViewInstance.Render();
			WebAppTests.FlushOutput(injector);
			ahsViewInstance.Set(YarnWebParams.AppState, YarnApplicationState.Failed.ToString(
				));
			ahsViewInstance.Render();
			WebAppTests.FlushOutput(injector);
			ahsViewInstance.Set(YarnWebParams.AppState, StringHelper.Cjoin(YarnApplicationState
				.Failed.ToString(), YarnApplicationState.Killed));
			ahsViewInstance.Render();
			WebAppTests.FlushOutput(injector);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAppPage()
		{
			Injector injector = WebAppTests.CreateMockInjector<ApplicationBaseProtocol>(MockApplicationHistoryClientService
				(1, 5, 1));
			AppPage appPageInstance = injector.GetInstance<AppPage>();
			appPageInstance.Render();
			WebAppTests.FlushOutput(injector);
			appPageInstance.Set(YarnWebParams.ApplicationId, ApplicationId.NewInstance(0, 1).
				ToString());
			appPageInstance.Render();
			WebAppTests.FlushOutput(injector);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAppAttemptPage()
		{
			Injector injector = WebAppTests.CreateMockInjector<ApplicationBaseProtocol>(MockApplicationHistoryClientService
				(1, 1, 5));
			AppAttemptPage appAttemptPageInstance = injector.GetInstance<AppAttemptPage>();
			appAttemptPageInstance.Render();
			WebAppTests.FlushOutput(injector);
			appAttemptPageInstance.Set(YarnWebParams.ApplicationAttemptId, ApplicationAttemptId
				.NewInstance(ApplicationId.NewInstance(0, 1), 1).ToString());
			appAttemptPageInstance.Render();
			WebAppTests.FlushOutput(injector);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestContainerPage()
		{
			Injector injector = WebAppTests.CreateMockInjector<ApplicationBaseProtocol>(MockApplicationHistoryClientService
				(1, 1, 1));
			ContainerPage containerPageInstance = injector.GetInstance<ContainerPage>();
			containerPageInstance.Render();
			WebAppTests.FlushOutput(injector);
			containerPageInstance.Set(YarnWebParams.ContainerId, ContainerId.NewContainerId(ApplicationAttemptId
				.NewInstance(ApplicationId.NewInstance(0, 1), 1), 1).ToString());
			containerPageInstance.Render();
			WebAppTests.FlushOutput(injector);
		}

		/// <exception cref="System.Exception"/>
		internal virtual ApplicationHistoryClientService MockApplicationHistoryClientService
			(int numApps, int numAppAttempts, int numContainers)
		{
			ApplicationHistoryManager ahManager = new TestAHSWebApp.MockApplicationHistoryManagerImpl
				(this, store);
			ApplicationHistoryClientService historyClientService = new ApplicationHistoryClientService
				(ahManager);
			for (int i = 1; i <= numApps; ++i)
			{
				ApplicationId appId = ApplicationId.NewInstance(0, i);
				WriteApplicationStartData(appId);
				for (int j = 1; j <= numAppAttempts; ++j)
				{
					ApplicationAttemptId appAttemptId = ApplicationAttemptId.NewInstance(appId, j);
					WriteApplicationAttemptStartData(appAttemptId);
					for (int k = 1; k <= numContainers; ++k)
					{
						ContainerId containerId = ContainerId.NewContainerId(appAttemptId, k);
						WriteContainerStartData(containerId);
						WriteContainerFinishData(containerId);
					}
					WriteApplicationAttemptFinishData(appAttemptId);
				}
				WriteApplicationFinishData(appId);
			}
			return historyClientService;
		}

		internal class MockApplicationHistoryManagerImpl : ApplicationHistoryManagerImpl
		{
			public MockApplicationHistoryManagerImpl(TestAHSWebApp _enclosing, ApplicationHistoryStore
				 store)
				: base()
			{
				this._enclosing = _enclosing;
				this.Init(new YarnConfiguration());
				this.Start();
			}

			protected internal override ApplicationHistoryStore CreateApplicationHistoryStore
				(Configuration conf)
			{
				return this._enclosing.store;
			}

			private readonly TestAHSWebApp _enclosing;
		}
	}
}
