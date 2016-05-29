using System;
using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Collect;
using Com.Google.Inject;
using NUnit.Framework;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn.Api;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Event;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp.Attempt;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmcontainer;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fair;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Security;
using Org.Apache.Hadoop.Yarn.Webapp.Test;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Webapp
{
	public class TestRMWebAppFairScheduler
	{
		[NUnit.Framework.Test]
		public virtual void TestFairSchedulerWebAppPage()
		{
			IList<RMAppState> appStates = Arrays.AsList(RMAppState.New, RMAppState.NewSaving, 
				RMAppState.Submitted);
			RMContext rmContext = MockRMContext(appStates);
			Injector injector = WebAppTests.CreateMockInjector<RMContext>(rmContext, new _Module_70
				(rmContext));
			FairSchedulerPage fsViewInstance = injector.GetInstance<FairSchedulerPage>();
			fsViewInstance.Render();
			WebAppTests.FlushOutput(injector);
		}

		private sealed class _Module_70 : Module
		{
			public _Module_70(RMContext rmContext)
			{
				this.rmContext = rmContext;
			}

			public void Configure(Binder binder)
			{
				try
				{
					ResourceManager mockRmWithFairScheduler = TestRMWebAppFairScheduler.MockRm(rmContext
						);
					binder.Bind<ResourceManager>().ToInstance(mockRmWithFairScheduler);
					binder.Bind<ApplicationBaseProtocol>().ToInstance(mockRmWithFairScheduler.GetClientRMService
						());
				}
				catch (IOException e)
				{
					throw new InvalidOperationException(e);
				}
			}

			private readonly RMContext rmContext;
		}

		/// <summary>
		/// Testing inconsistent state between AbstractYarnScheduler#applications and
		/// RMContext#applications
		/// </summary>
		[NUnit.Framework.Test]
		public virtual void TestFairSchedulerWebAppPageInInconsistentState()
		{
			IList<RMAppState> appStates = Arrays.AsList(RMAppState.New, RMAppState.NewSaving, 
				RMAppState.Submitted, RMAppState.Running, RMAppState.FinalSaving, RMAppState.Accepted
				, RMAppState.Finished);
			RMContext rmContext = MockRMContext(appStates);
			Injector injector = WebAppTests.CreateMockInjector<RMContext>(rmContext, new _Module_110
				(rmContext));
			FairSchedulerPage fsViewInstance = injector.GetInstance<FairSchedulerPage>();
			try
			{
				fsViewInstance.Render();
			}
			catch (Exception e)
			{
				NUnit.Framework.Assert.Fail("Failed to render FairSchedulerPage: " + StringUtils.
					StringifyException(e));
			}
			WebAppTests.FlushOutput(injector);
		}

		private sealed class _Module_110 : Module
		{
			public _Module_110(RMContext rmContext)
			{
				this.rmContext = rmContext;
			}

			public void Configure(Binder binder)
			{
				try
				{
					ResourceManager mockRmWithFairScheduler = TestRMWebAppFairScheduler.MockRmWithApps
						(rmContext);
					binder.Bind<ResourceManager>().ToInstance(mockRmWithFairScheduler);
					binder.Bind<ApplicationBaseProtocol>().ToInstance(mockRmWithFairScheduler.GetClientRMService
						());
				}
				catch (IOException e)
				{
					throw new InvalidOperationException(e);
				}
			}

			private readonly RMContext rmContext;
		}

		private static RMContext MockRMContext(IList<RMAppState> states)
		{
			ConcurrentMap<ApplicationId, RMApp> applicationsMaps = Maps.NewConcurrentMap();
			int i = 0;
			foreach (RMAppState state in states)
			{
				MockRMApp app = new _MockRMApp_142(i, i, state);
				RMAppAttempt attempt = Org.Mockito.Mockito.Mock<RMAppAttempt>();
				app.SetCurrentAppAttempt(attempt);
				applicationsMaps[app.GetApplicationId()] = app;
				i++;
			}
			RMContextImpl rmContext = new _RMContextImpl_159(applicationsMaps, null, null, null
				, null, null, null, null, null, null, null);
			return rmContext;
		}

		private sealed class _MockRMApp_142 : MockRMApp
		{
			public _MockRMApp_142(int baseArg1, long baseArg2, RMAppState baseArg3)
				: base(baseArg1, baseArg2, baseArg3)
			{
			}

			public override RMAppMetrics GetRMAppMetrics()
			{
				return new RMAppMetrics(Resource.NewInstance(0, 0), 0, 0, 0, 0);
			}

			public override YarnApplicationState CreateApplicationState()
			{
				return YarnApplicationState.Accepted;
			}
		}

		private sealed class _RMContextImpl_159 : RMContextImpl
		{
			public _RMContextImpl_159(ConcurrentMap<ApplicationId, RMApp> applicationsMaps, Dispatcher
				 baseArg1, ContainerAllocationExpirer baseArg2, AMLivelinessMonitor baseArg3, AMLivelinessMonitor
				 baseArg4, DelegationTokenRenewer baseArg5, AMRMTokenSecretManager baseArg6, RMContainerTokenSecretManager
				 baseArg7, NMTokenSecretManagerInRM baseArg8, ClientToAMTokenSecretManagerInRM baseArg9
				, ResourceScheduler baseArg10)
				: base(baseArg1, baseArg2, baseArg3, baseArg4, baseArg5, baseArg6, baseArg7, baseArg8
					, baseArg9, baseArg10)
			{
				this.applicationsMaps = applicationsMaps;
			}

			public override ConcurrentMap<ApplicationId, RMApp> GetRMApps()
			{
				return applicationsMaps;
			}

			public override ResourceScheduler GetScheduler()
			{
				return Org.Mockito.Mockito.Mock<AbstractYarnScheduler>();
			}

			private readonly ConcurrentMap<ApplicationId, RMApp> applicationsMaps;
		}

		/// <exception cref="System.IO.IOException"/>
		private static ResourceManager MockRm(RMContext rmContext)
		{
			ResourceManager rm = Org.Mockito.Mockito.Mock<ResourceManager>();
			ResourceScheduler rs = MockFairScheduler();
			ClientRMService clientRMService = MockClientRMService(rmContext);
			Org.Mockito.Mockito.When(rm.GetResourceScheduler()).ThenReturn(rs);
			Org.Mockito.Mockito.When(rm.GetRMContext()).ThenReturn(rmContext);
			Org.Mockito.Mockito.When(rm.GetClientRMService()).ThenReturn(clientRMService);
			return rm;
		}

		/// <exception cref="System.IO.IOException"/>
		private static FairScheduler MockFairScheduler()
		{
			FairScheduler fs = new FairScheduler();
			FairSchedulerConfiguration conf = new FairSchedulerConfiguration();
			fs.SetRMContext(new RMContextImpl(null, null, null, null, null, null, new RMContainerTokenSecretManager
				(conf), new NMTokenSecretManagerInRM(conf), new ClientToAMTokenSecretManagerInRM
				(), null));
			fs.Init(conf);
			return fs;
		}

		/// <exception cref="System.IO.IOException"/>
		private static ResourceManager MockRmWithApps(RMContext rmContext)
		{
			ResourceManager rm = Org.Mockito.Mockito.Mock<ResourceManager>();
			ResourceScheduler rs = MockFairSchedulerWithoutApps(rmContext);
			ClientRMService clientRMService = MockClientRMService(rmContext);
			Org.Mockito.Mockito.When(rm.GetResourceScheduler()).ThenReturn(rs);
			Org.Mockito.Mockito.When(rm.GetRMContext()).ThenReturn(rmContext);
			Org.Mockito.Mockito.When(rm.GetClientRMService()).ThenReturn(clientRMService);
			return rm;
		}

		/// <exception cref="System.IO.IOException"/>
		private static FairScheduler MockFairSchedulerWithoutApps(RMContext rmContext)
		{
			FairScheduler fs = new _FairScheduler_207();
			FairSchedulerConfiguration conf = new FairSchedulerConfiguration();
			fs.SetRMContext(rmContext);
			fs.Init(conf);
			return fs;
		}

		private sealed class _FairScheduler_207 : FairScheduler
		{
			public _FairScheduler_207()
			{
			}

			public override FSAppAttempt GetSchedulerApp(ApplicationAttemptId applicationAttemptId
				)
			{
				return null;
			}

			public override FSAppAttempt GetApplicationAttempt(ApplicationAttemptId applicationAttemptId
				)
			{
				return null;
			}
		}

		public static ClientRMService MockClientRMService(RMContext rmContext)
		{
			return Org.Mockito.Mockito.Mock<ClientRMService>();
		}
	}
}
