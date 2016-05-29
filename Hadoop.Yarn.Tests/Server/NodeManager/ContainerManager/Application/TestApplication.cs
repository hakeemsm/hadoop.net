using System;
using System.Collections.Generic;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Event;
using Org.Apache.Hadoop.Yarn.Security;
using Org.Apache.Hadoop.Yarn.Server.Api.Records;
using Org.Apache.Hadoop.Yarn.Server.Api.Records.Impl.PB;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Launcher;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Localizer.Event;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Loghandler.Event;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Monitor;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Security;
using Org.Apache.Hadoop.Yarn.Server.Security;
using Org.Apache.Hadoop.Yarn.Server.Utils;
using Org.Mockito;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application
{
	public class TestApplication
	{
		/// <summary>All container start events before application running.</summary>
		[NUnit.Framework.Test]
		public virtual void TestApplicationInit1()
		{
			TestApplication.WrappedApplication wa = null;
			try
			{
				wa = new TestApplication.WrappedApplication(this, 1, 314159265358979L, "yak", 3);
				wa.InitApplication();
				wa.InitContainer(1);
				NUnit.Framework.Assert.AreEqual(ApplicationState.Initing, wa.app.GetApplicationState
					());
				NUnit.Framework.Assert.AreEqual(1, wa.app.GetContainers().Count);
				wa.InitContainer(0);
				wa.InitContainer(2);
				NUnit.Framework.Assert.AreEqual(ApplicationState.Initing, wa.app.GetApplicationState
					());
				NUnit.Framework.Assert.AreEqual(3, wa.app.GetContainers().Count);
				wa.ApplicationInited();
				NUnit.Framework.Assert.AreEqual(ApplicationState.Running, wa.app.GetApplicationState
					());
				for (int i = 0; i < wa.containers.Count; i++)
				{
					Org.Mockito.Mockito.Verify(wa.containerBus).Handle(Matchers.ArgThat(new TestApplication.ContainerInitMatcher
						(this, wa.containers[i].GetContainerId())));
				}
			}
			finally
			{
				if (wa != null)
				{
					wa.Finished();
				}
			}
		}

		/// <summary>Container start events after Application Running</summary>
		[NUnit.Framework.Test]
		public virtual void TestApplicationInit2()
		{
			TestApplication.WrappedApplication wa = null;
			try
			{
				wa = new TestApplication.WrappedApplication(this, 2, 314159265358979L, "yak", 3);
				wa.InitApplication();
				wa.InitContainer(0);
				NUnit.Framework.Assert.AreEqual(ApplicationState.Initing, wa.app.GetApplicationState
					());
				NUnit.Framework.Assert.AreEqual(1, wa.app.GetContainers().Count);
				wa.ApplicationInited();
				NUnit.Framework.Assert.AreEqual(ApplicationState.Running, wa.app.GetApplicationState
					());
				Org.Mockito.Mockito.Verify(wa.containerBus).Handle(Matchers.ArgThat(new TestApplication.ContainerInitMatcher
					(this, wa.containers[0].GetContainerId())));
				wa.InitContainer(1);
				wa.InitContainer(2);
				NUnit.Framework.Assert.AreEqual(ApplicationState.Running, wa.app.GetApplicationState
					());
				NUnit.Framework.Assert.AreEqual(3, wa.app.GetContainers().Count);
				for (int i = 1; i < wa.containers.Count; i++)
				{
					Org.Mockito.Mockito.Verify(wa.containerBus).Handle(Matchers.ArgThat(new TestApplication.ContainerInitMatcher
						(this, wa.containers[i].GetContainerId())));
				}
			}
			finally
			{
				if (wa != null)
				{
					wa.Finished();
				}
			}
		}

		/// <summary>
		/// App state RUNNING after all containers complete, before RM sends
		/// APP_FINISHED
		/// </summary>
		[NUnit.Framework.Test]
		public virtual void TestAppRunningAfterContainersComplete()
		{
			TestApplication.WrappedApplication wa = null;
			try
			{
				wa = new TestApplication.WrappedApplication(this, 3, 314159265358979L, "yak", 3);
				wa.InitApplication();
				wa.InitContainer(-1);
				NUnit.Framework.Assert.AreEqual(ApplicationState.Initing, wa.app.GetApplicationState
					());
				wa.ApplicationInited();
				NUnit.Framework.Assert.AreEqual(ApplicationState.Running, wa.app.GetApplicationState
					());
				wa.ContainerFinished(0);
				NUnit.Framework.Assert.AreEqual(ApplicationState.Running, wa.app.GetApplicationState
					());
				NUnit.Framework.Assert.AreEqual(2, wa.app.GetContainers().Count);
				wa.ContainerFinished(1);
				wa.ContainerFinished(2);
				NUnit.Framework.Assert.AreEqual(ApplicationState.Running, wa.app.GetApplicationState
					());
				NUnit.Framework.Assert.AreEqual(0, wa.app.GetContainers().Count);
			}
			finally
			{
				if (wa != null)
				{
					wa.Finished();
				}
			}
		}

		/// <summary>Finished containers properly tracked when only container finishes in APP_INITING
		/// 	</summary>
		[NUnit.Framework.Test]
		public virtual void TestContainersCompleteDuringAppInit1()
		{
			TestApplication.WrappedApplication wa = null;
			try
			{
				wa = new TestApplication.WrappedApplication(this, 3, 314159265358979L, "yak", 1);
				wa.InitApplication();
				wa.InitContainer(-1);
				NUnit.Framework.Assert.AreEqual(ApplicationState.Initing, wa.app.GetApplicationState
					());
				wa.ContainerFinished(0);
				NUnit.Framework.Assert.AreEqual(ApplicationState.Initing, wa.app.GetApplicationState
					());
				wa.ApplicationInited();
				NUnit.Framework.Assert.AreEqual(ApplicationState.Running, wa.app.GetApplicationState
					());
				NUnit.Framework.Assert.AreEqual(0, wa.app.GetContainers().Count);
			}
			finally
			{
				if (wa != null)
				{
					wa.Finished();
				}
			}
		}

		/// <summary>Finished containers properly tracked when 1 of several containers finishes in APP_INITING
		/// 	</summary>
		[NUnit.Framework.Test]
		public virtual void TestContainersCompleteDuringAppInit2()
		{
			TestApplication.WrappedApplication wa = null;
			try
			{
				wa = new TestApplication.WrappedApplication(this, 3, 314159265358979L, "yak", 3);
				wa.InitApplication();
				wa.InitContainer(-1);
				NUnit.Framework.Assert.AreEqual(ApplicationState.Initing, wa.app.GetApplicationState
					());
				wa.ContainerFinished(0);
				NUnit.Framework.Assert.AreEqual(ApplicationState.Initing, wa.app.GetApplicationState
					());
				wa.ApplicationInited();
				NUnit.Framework.Assert.AreEqual(ApplicationState.Running, wa.app.GetApplicationState
					());
				NUnit.Framework.Assert.AreEqual(2, wa.app.GetContainers().Count);
				wa.ContainerFinished(1);
				wa.ContainerFinished(2);
				NUnit.Framework.Assert.AreEqual(ApplicationState.Running, wa.app.GetApplicationState
					());
				NUnit.Framework.Assert.AreEqual(0, wa.app.GetContainers().Count);
			}
			finally
			{
				if (wa != null)
				{
					wa.Finished();
				}
			}
		}

		[NUnit.Framework.Test]
		public virtual void TestAppFinishedOnRunningContainers()
		{
			TestApplication.WrappedApplication wa = null;
			try
			{
				wa = new TestApplication.WrappedApplication(this, 4, 314159265358979L, "yak", 3);
				wa.InitApplication();
				wa.InitContainer(-1);
				NUnit.Framework.Assert.AreEqual(ApplicationState.Initing, wa.app.GetApplicationState
					());
				wa.ApplicationInited();
				NUnit.Framework.Assert.AreEqual(ApplicationState.Running, wa.app.GetApplicationState
					());
				wa.ContainerFinished(0);
				NUnit.Framework.Assert.AreEqual(ApplicationState.Running, wa.app.GetApplicationState
					());
				NUnit.Framework.Assert.AreEqual(2, wa.app.GetContainers().Count);
				wa.AppFinished();
				NUnit.Framework.Assert.AreEqual(ApplicationState.FinishingContainersWait, wa.app.
					GetApplicationState());
				NUnit.Framework.Assert.AreEqual(2, wa.app.GetContainers().Count);
				for (int i = 1; i < wa.containers.Count; i++)
				{
					Org.Mockito.Mockito.Verify(wa.containerBus).Handle(Matchers.ArgThat(new TestApplication.ContainerKillMatcher
						(this, wa.containers[i].GetContainerId())));
				}
				wa.ContainerFinished(1);
				NUnit.Framework.Assert.AreEqual(ApplicationState.FinishingContainersWait, wa.app.
					GetApplicationState());
				NUnit.Framework.Assert.AreEqual(1, wa.app.GetContainers().Count);
				Org.Mockito.Mockito.Reset(wa.localizerBus);
				wa.ContainerFinished(2);
				// All containers finished. Cleanup should be called.
				NUnit.Framework.Assert.AreEqual(ApplicationState.ApplicationResourcesCleaningup, 
					wa.app.GetApplicationState());
				NUnit.Framework.Assert.AreEqual(0, wa.app.GetContainers().Count);
				Org.Mockito.Mockito.Verify(wa.localizerBus).Handle(Matchers.RefEq(new ApplicationLocalizationEvent
					(LocalizationEventType.DestroyApplicationResources, wa.app)));
				Org.Mockito.Mockito.Verify(wa.auxBus).Handle(Matchers.RefEq(new AuxServicesEvent(
					AuxServicesEventType.ApplicationStop, wa.appId)));
				wa.AppResourcesCleanedup();
				foreach (Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container
					 container in wa.containers)
				{
					ContainerTokenIdentifier identifier = wa.GetContainerTokenIdentifier(container.GetContainerId
						());
					WaitForContainerTokenToExpire(identifier);
					NUnit.Framework.Assert.IsTrue(wa.context.GetContainerTokenSecretManager().IsValidStartContainerRequest
						(identifier));
				}
				NUnit.Framework.Assert.AreEqual(ApplicationState.Finished, wa.app.GetApplicationState
					());
			}
			finally
			{
				if (wa != null)
				{
					wa.Finished();
				}
			}
		}

		protected internal virtual ContainerTokenIdentifier WaitForContainerTokenToExpire
			(ContainerTokenIdentifier identifier)
		{
			int attempts = 5;
			while (Runtime.CurrentTimeMillis() < identifier.GetExpiryTimeStamp() && attempts--
				 > 0)
			{
				try
				{
					Sharpen.Thread.Sleep(1000);
				}
				catch (Exception)
				{
				}
			}
			return identifier;
		}

		[NUnit.Framework.Test]
		public virtual void TestAppFinishedOnCompletedContainers()
		{
			TestApplication.WrappedApplication wa = null;
			try
			{
				wa = new TestApplication.WrappedApplication(this, 5, 314159265358979L, "yak", 3);
				wa.InitApplication();
				wa.InitContainer(-1);
				NUnit.Framework.Assert.AreEqual(ApplicationState.Initing, wa.app.GetApplicationState
					());
				wa.ApplicationInited();
				NUnit.Framework.Assert.AreEqual(ApplicationState.Running, wa.app.GetApplicationState
					());
				Org.Mockito.Mockito.Reset(wa.localizerBus);
				wa.ContainerFinished(0);
				wa.ContainerFinished(1);
				wa.ContainerFinished(2);
				NUnit.Framework.Assert.AreEqual(ApplicationState.Running, wa.app.GetApplicationState
					());
				NUnit.Framework.Assert.AreEqual(0, wa.app.GetContainers().Count);
				wa.AppFinished();
				NUnit.Framework.Assert.AreEqual(ApplicationState.ApplicationResourcesCleaningup, 
					wa.app.GetApplicationState());
				Org.Mockito.Mockito.Verify(wa.localizerBus).Handle(Matchers.RefEq(new ApplicationLocalizationEvent
					(LocalizationEventType.DestroyApplicationResources, wa.app)));
				wa.AppResourcesCleanedup();
				foreach (Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container
					 container in wa.containers)
				{
					ContainerTokenIdentifier identifier = wa.GetContainerTokenIdentifier(container.GetContainerId
						());
					WaitForContainerTokenToExpire(identifier);
					NUnit.Framework.Assert.IsTrue(wa.context.GetContainerTokenSecretManager().IsValidStartContainerRequest
						(identifier));
				}
				NUnit.Framework.Assert.AreEqual(ApplicationState.Finished, wa.app.GetApplicationState
					());
			}
			finally
			{
				if (wa != null)
				{
					wa.Finished();
				}
			}
		}

		//TODO Re-work after Application transitions are changed.
		//  @Test
		public virtual void TestStartContainerAfterAppFinished()
		{
			TestApplication.WrappedApplication wa = null;
			try
			{
				wa = new TestApplication.WrappedApplication(this, 5, 314159265358979L, "yak", 3);
				wa.InitApplication();
				wa.InitContainer(-1);
				NUnit.Framework.Assert.AreEqual(ApplicationState.Initing, wa.app.GetApplicationState
					());
				wa.ApplicationInited();
				NUnit.Framework.Assert.AreEqual(ApplicationState.Running, wa.app.GetApplicationState
					());
				Org.Mockito.Mockito.Reset(wa.localizerBus);
				wa.ContainerFinished(0);
				wa.ContainerFinished(1);
				wa.ContainerFinished(2);
				NUnit.Framework.Assert.AreEqual(ApplicationState.Running, wa.app.GetApplicationState
					());
				NUnit.Framework.Assert.AreEqual(0, wa.app.GetContainers().Count);
				wa.AppFinished();
				NUnit.Framework.Assert.AreEqual(ApplicationState.ApplicationResourcesCleaningup, 
					wa.app.GetApplicationState());
				Org.Mockito.Mockito.Verify(wa.localizerBus).Handle(Matchers.RefEq(new ApplicationLocalizationEvent
					(LocalizationEventType.DestroyApplicationResources, wa.app)));
				wa.AppResourcesCleanedup();
				NUnit.Framework.Assert.AreEqual(ApplicationState.Finished, wa.app.GetApplicationState
					());
			}
			finally
			{
				if (wa != null)
				{
					wa.Finished();
				}
			}
		}

		//TODO Re-work after Application transitions are changed.
		//  @Test
		public virtual void TestAppFinishedOnIniting()
		{
			// AM may send a startContainer() - AM APP_FINIHSED processed after
			// APP_FINISHED on another NM
			TestApplication.WrappedApplication wa = null;
			try
			{
				wa = new TestApplication.WrappedApplication(this, 1, 314159265358979L, "yak", 3);
				wa.InitApplication();
				wa.InitContainer(0);
				NUnit.Framework.Assert.AreEqual(ApplicationState.Initing, wa.app.GetApplicationState
					());
				NUnit.Framework.Assert.AreEqual(1, wa.app.GetContainers().Count);
				Org.Mockito.Mockito.Reset(wa.localizerBus);
				wa.AppFinished();
				Org.Mockito.Mockito.Verify(wa.containerBus).Handle(Matchers.ArgThat(new TestApplication.ContainerKillMatcher
					(this, wa.containers[0].GetContainerId())));
				NUnit.Framework.Assert.AreEqual(ApplicationState.FinishingContainersWait, wa.app.
					GetApplicationState());
				wa.ContainerFinished(0);
				NUnit.Framework.Assert.AreEqual(ApplicationState.ApplicationResourcesCleaningup, 
					wa.app.GetApplicationState());
				Org.Mockito.Mockito.Verify(wa.localizerBus).Handle(Matchers.RefEq(new ApplicationLocalizationEvent
					(LocalizationEventType.DestroyApplicationResources, wa.app)));
				wa.InitContainer(1);
				NUnit.Framework.Assert.AreEqual(ApplicationState.ApplicationResourcesCleaningup, 
					wa.app.GetApplicationState());
				NUnit.Framework.Assert.AreEqual(0, wa.app.GetContainers().Count);
				wa.AppResourcesCleanedup();
				NUnit.Framework.Assert.AreEqual(ApplicationState.Finished, wa.app.GetApplicationState
					());
			}
			finally
			{
				if (wa != null)
				{
					wa.Finished();
				}
			}
		}

		[NUnit.Framework.Test]
		public virtual void TestNMTokenSecretManagerCleanup()
		{
			TestApplication.WrappedApplication wa = null;
			try
			{
				wa = new TestApplication.WrappedApplication(this, 1, 314159265358979L, "yak", 1);
				wa.InitApplication();
				wa.InitContainer(0);
				NUnit.Framework.Assert.AreEqual(ApplicationState.Initing, wa.app.GetApplicationState
					());
				NUnit.Framework.Assert.AreEqual(1, wa.app.GetContainers().Count);
				wa.AppFinished();
				wa.ContainerFinished(0);
				wa.AppResourcesCleanedup();
				NUnit.Framework.Assert.AreEqual(ApplicationState.Finished, wa.app.GetApplicationState
					());
				Org.Mockito.Mockito.Verify(wa.nmTokenSecretMgr).AppFinished(Matchers.Eq(wa.appId)
					);
			}
			finally
			{
				if (wa != null)
				{
					wa.Finished();
				}
			}
		}

		private class ContainerKillMatcher : ArgumentMatcher<ContainerEvent>
		{
			private ContainerId cId;

			public ContainerKillMatcher(TestApplication _enclosing, ContainerId cId)
			{
				this._enclosing = _enclosing;
				this.cId = cId;
			}

			public override bool Matches(object argument)
			{
				if (argument is ContainerKillEvent)
				{
					ContainerKillEvent @event = (ContainerKillEvent)argument;
					return @event.GetContainerID().Equals(this.cId);
				}
				return false;
			}

			private readonly TestApplication _enclosing;
		}

		private class ContainerInitMatcher : ArgumentMatcher<ContainerEvent>
		{
			private ContainerId cId;

			public ContainerInitMatcher(TestApplication _enclosing, ContainerId cId)
			{
				this._enclosing = _enclosing;
				this.cId = cId;
			}

			public override bool Matches(object argument)
			{
				if (argument is ContainerInitEvent)
				{
					ContainerInitEvent @event = (ContainerInitEvent)argument;
					return @event.GetContainerID().Equals(this.cId);
				}
				return false;
			}

			private readonly TestApplication _enclosing;
		}

		private class WrappedApplication
		{
			internal readonly DrainDispatcher dispatcher;

			internal readonly EventHandler<LocalizationEvent> localizerBus;

			internal readonly EventHandler<ContainersLauncherEvent> launcherBus;

			internal readonly EventHandler<ContainersMonitorEvent> monitorBus;

			internal readonly EventHandler<AuxServicesEvent> auxBus;

			internal readonly EventHandler<ContainerEvent> containerBus;

			internal readonly EventHandler<LogHandlerEvent> logAggregationBus;

			internal readonly string user;

			internal readonly IList<Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container
				> containers;

			internal readonly Context context;

			internal readonly IDictionary<ContainerId, ContainerTokenIdentifier> containerTokenIdentifierMap;

			internal readonly NMTokenSecretManagerInNM nmTokenSecretMgr;

			internal readonly ApplicationId appId;

			internal readonly Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application.Application
				 app;

			internal WrappedApplication(TestApplication _enclosing, int id, long timestamp, string
				 user, int numContainers)
			{
				this._enclosing = _enclosing;
				Configuration conf = new Configuration();
				this.dispatcher = new DrainDispatcher();
				this.containerTokenIdentifierMap = new Dictionary<ContainerId, ContainerTokenIdentifier
					>();
				this.dispatcher.Init(conf);
				this.localizerBus = Org.Mockito.Mockito.Mock<EventHandler>();
				this.launcherBus = Org.Mockito.Mockito.Mock<EventHandler>();
				this.monitorBus = Org.Mockito.Mockito.Mock<EventHandler>();
				this.auxBus = Org.Mockito.Mockito.Mock<EventHandler>();
				this.containerBus = Org.Mockito.Mockito.Mock<EventHandler>();
				this.logAggregationBus = Org.Mockito.Mockito.Mock<EventHandler>();
				this.dispatcher.Register(typeof(LocalizationEventType), this.localizerBus);
				this.dispatcher.Register(typeof(ContainersLauncherEventType), this.launcherBus);
				this.dispatcher.Register(typeof(ContainersMonitorEventType), this.monitorBus);
				this.dispatcher.Register(typeof(AuxServicesEventType), this.auxBus);
				this.dispatcher.Register(typeof(ContainerEventType), this.containerBus);
				this.dispatcher.Register(typeof(LogHandlerEventType), this.logAggregationBus);
				this.nmTokenSecretMgr = Org.Mockito.Mockito.Mock<NMTokenSecretManagerInNM>();
				this.context = Org.Mockito.Mockito.Mock<Context>();
				Org.Mockito.Mockito.When(this.context.GetContainerTokenSecretManager()).ThenReturn
					(new NMContainerTokenSecretManager(conf));
				Org.Mockito.Mockito.When(this.context.GetApplicationACLsManager()).ThenReturn(new 
					ApplicationACLsManager(conf));
				Org.Mockito.Mockito.When(this.context.GetNMTokenSecretManager()).ThenReturn(this.
					nmTokenSecretMgr);
				// Setting master key
				MasterKey masterKey = new MasterKeyPBImpl();
				masterKey.SetKeyId(123);
				masterKey.SetBytes(ByteBuffer.Wrap(new byte[] { (123) }));
				this.context.GetContainerTokenSecretManager().SetMasterKey(masterKey);
				this.user = user;
				this.appId = BuilderUtils.NewApplicationId(timestamp, id);
				this.app = new ApplicationImpl(this.dispatcher, this.user, this.appId, null, this
					.context);
				this.containers = new AList<Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container
					>();
				for (int i = 0; i < numContainers; i++)
				{
					Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container container
						 = this._enclosing.CreateMockedContainer(this.appId, i);
					this.containers.AddItem(container);
					long currentTime = Runtime.CurrentTimeMillis();
					ContainerTokenIdentifier identifier = new ContainerTokenIdentifier(container.GetContainerId
						(), string.Empty, string.Empty, null, currentTime + 2000, masterKey.GetKeyId(), 
						currentTime, Priority.NewInstance(0), 0);
					this.containerTokenIdentifierMap[identifier.GetContainerID()] = identifier;
					this.context.GetContainerTokenSecretManager().StartContainerSuccessful(identifier
						);
					NUnit.Framework.Assert.IsFalse(this.context.GetContainerTokenSecretManager().IsValidStartContainerRequest
						(identifier));
				}
				this.dispatcher.Start();
			}

			private void DrainDispatcherEvents()
			{
				this.dispatcher.Await();
			}

			public virtual void Finished()
			{
				this.dispatcher.Stop();
			}

			public virtual void InitApplication()
			{
				this.app.Handle(new ApplicationInitEvent(this.appId, new Dictionary<ApplicationAccessType
					, string>()));
			}

			public virtual void InitContainer(int containerNum)
			{
				if (containerNum == -1)
				{
					for (int i = 0; i < this.containers.Count; i++)
					{
						this.app.Handle(new ApplicationContainerInitEvent(this.containers[i]));
					}
				}
				else
				{
					this.app.Handle(new ApplicationContainerInitEvent(this.containers[containerNum]));
				}
				this.DrainDispatcherEvents();
			}

			public virtual void ContainerFinished(int containerNum)
			{
				this.app.Handle(new ApplicationContainerFinishedEvent(this.containers[containerNum
					].GetContainerId()));
				this.DrainDispatcherEvents();
			}

			public virtual void ApplicationInited()
			{
				this.app.Handle(new ApplicationInitedEvent(this.appId));
				this.DrainDispatcherEvents();
			}

			public virtual void AppFinished()
			{
				this.app.Handle(new ApplicationFinishEvent(this.appId, "Finish Application"));
				this.DrainDispatcherEvents();
			}

			public virtual void AppResourcesCleanedup()
			{
				this.app.Handle(new ApplicationEvent(this.appId, ApplicationEventType.ApplicationResourcesCleanedup
					));
				this.DrainDispatcherEvents();
			}

			public virtual ContainerTokenIdentifier GetContainerTokenIdentifier(ContainerId containerId
				)
			{
				return this.containerTokenIdentifierMap[containerId];
			}

			private readonly TestApplication _enclosing;
		}

		private Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container
			 CreateMockedContainer(ApplicationId appId, int containerId)
		{
			ApplicationAttemptId appAttemptId = BuilderUtils.NewApplicationAttemptId(appId, 1
				);
			ContainerId cId = BuilderUtils.NewContainerId(appAttemptId, containerId);
			Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container c = 
				Org.Mockito.Mockito.Mock<Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container
				>();
			Org.Mockito.Mockito.When(c.GetContainerId()).ThenReturn(cId);
			ContainerLaunchContext launchContext = Org.Mockito.Mockito.Mock<ContainerLaunchContext
				>();
			Org.Mockito.Mockito.When(c.GetLaunchContext()).ThenReturn(launchContext);
			Org.Mockito.Mockito.When(launchContext.GetApplicationACLs()).ThenReturn(new Dictionary
				<ApplicationAccessType, string>());
			return c;
		}
	}
}
