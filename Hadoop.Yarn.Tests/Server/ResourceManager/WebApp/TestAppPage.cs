using System;
using System.IO;
using System.Text;
using Com.Google.Inject;
using Org.Apache.Hadoop.Yarn.Api;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp;
using Org.Apache.Hadoop.Yarn.Server.Webapp;
using Org.Apache.Hadoop.Yarn.Webapp;
using Org.Apache.Hadoop.Yarn.Webapp.Test;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Webapp
{
	public class TestAppPage
	{
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAppBlockRenderWithNullCurrentAppAttempt()
		{
			ApplicationId AppId = ApplicationId.NewInstance(1234L, 0);
			Injector injector;
			// init app
			RMApp app = Org.Mockito.Mockito.Mock<RMApp>();
			Org.Mockito.Mockito.When(app.GetTrackingUrl()).ThenReturn("http://host:123");
			Org.Mockito.Mockito.When(app.GetState()).ThenReturn(RMAppState.Failed);
			Org.Mockito.Mockito.When(app.GetApplicationId()).ThenReturn(AppId);
			Org.Mockito.Mockito.When(app.GetApplicationType()).ThenReturn("Type");
			Org.Mockito.Mockito.When(app.GetUser()).ThenReturn("user");
			Org.Mockito.Mockito.When(app.GetName()).ThenReturn("Name");
			Org.Mockito.Mockito.When(app.GetQueue()).ThenReturn("queue");
			Org.Mockito.Mockito.When(app.GetDiagnostics()).ThenReturn(new StringBuilder());
			Org.Mockito.Mockito.When(app.GetFinalApplicationStatus()).ThenReturn(FinalApplicationStatus
				.Failed);
			Org.Mockito.Mockito.When(app.GetFinalApplicationStatus()).ThenReturn(FinalApplicationStatus
				.Failed);
			Org.Mockito.Mockito.When(app.GetStartTime()).ThenReturn(0L);
			Org.Mockito.Mockito.When(app.GetFinishTime()).ThenReturn(0L);
			Org.Mockito.Mockito.When(app.CreateApplicationState()).ThenReturn(YarnApplicationState
				.Failed);
			RMAppMetrics appMetrics = new RMAppMetrics(Resource.NewInstance(0, 0), 0, 0, 0, 0
				);
			Org.Mockito.Mockito.When(app.GetRMAppMetrics()).ThenReturn(appMetrics);
			// initialize RM Context, and create RMApp, without creating RMAppAttempt
			RMContext rmContext = TestRMWebApp.MockRMContext(15, 1, 2, 8);
			rmContext.GetRMApps()[AppId] = app;
			injector = WebAppTests.CreateMockInjector<RMContext>(rmContext, new _Module_76(rmContext
				));
			AppBlock instance = injector.GetInstance<AppBlock>();
			instance.Set(YarnWebParams.ApplicationId, AppId.ToString());
			instance.Render();
		}

		private sealed class _Module_76 : Module
		{
			public _Module_76(RMContext rmContext)
			{
				this.rmContext = rmContext;
			}

			public void Configure(Binder binder)
			{
				try
				{
					ResourceManager rm = TestRMWebApp.MockRm(rmContext);
					binder.Bind<ResourceManager>().ToInstance(rm);
					binder.Bind<ApplicationBaseProtocol>().ToInstance(rm.GetClientRMService());
				}
				catch (IOException e)
				{
					throw new InvalidOperationException(e);
				}
			}

			private readonly RMContext rmContext;
		}
	}
}
