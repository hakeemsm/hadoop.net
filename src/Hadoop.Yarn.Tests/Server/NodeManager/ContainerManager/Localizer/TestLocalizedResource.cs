using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Event;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Localizer.Event;
using Org.Mockito;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Localizer
{
	public class TestLocalizedResource
	{
		internal static ContainerId GetMockContainer(long id)
		{
			ApplicationId appId = Org.Mockito.Mockito.Mock<ApplicationId>();
			Org.Mockito.Mockito.When(appId.GetClusterTimestamp()).ThenReturn(314159265L);
			Org.Mockito.Mockito.When(appId.GetId()).ThenReturn(3);
			ApplicationAttemptId appAttemptId = Org.Mockito.Mockito.Mock<ApplicationAttemptId
				>();
			Org.Mockito.Mockito.When(appAttemptId.GetApplicationId()).ThenReturn(appId);
			Org.Mockito.Mockito.When(appAttemptId.GetAttemptId()).ThenReturn(0);
			ContainerId container = Org.Mockito.Mockito.Mock<ContainerId>();
			Org.Mockito.Mockito.When(container.GetContainerId()).ThenReturn(id);
			Org.Mockito.Mockito.When(container.GetApplicationAttemptId()).ThenReturn(appAttemptId
				);
			return container;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNotification()
		{
			// mocked generic
			DrainDispatcher dispatcher = new DrainDispatcher();
			dispatcher.Init(new Configuration());
			try
			{
				dispatcher.Start();
				EventHandler<ContainerEvent> containerBus = Org.Mockito.Mockito.Mock<EventHandler
					>();
				EventHandler<LocalizerEvent> localizerBus = Org.Mockito.Mockito.Mock<EventHandler
					>();
				dispatcher.Register(typeof(ContainerEventType), containerBus);
				dispatcher.Register(typeof(LocalizerEventType), localizerBus);
				// mock resource
				LocalResource apiRsrc = CreateMockResource();
				ContainerId container0 = GetMockContainer(0L);
				Credentials creds0 = new Credentials();
				LocalResourceVisibility vis0 = LocalResourceVisibility.Private;
				LocalizerContext ctxt0 = new LocalizerContext("yak", container0, creds0);
				LocalResourceRequest rsrcA = new LocalResourceRequest(apiRsrc);
				LocalizedResource local = new LocalizedResource(rsrcA, dispatcher);
				local.Handle(new ResourceRequestEvent(rsrcA, vis0, ctxt0));
				dispatcher.Await();
				// Register C0, verify request event
				TestLocalizedResource.LocalizerEventMatcher matchesL0Req = new TestLocalizedResource.LocalizerEventMatcher
					(container0, creds0, vis0, LocalizerEventType.RequestResourceLocalization);
				Org.Mockito.Mockito.Verify(localizerBus).Handle(Matchers.ArgThat(matchesL0Req));
				NUnit.Framework.Assert.AreEqual(ResourceState.Downloading, local.GetState());
				// Register C1, verify request event
				Credentials creds1 = new Credentials();
				ContainerId container1 = GetMockContainer(1L);
				LocalizerContext ctxt1 = new LocalizerContext("yak", container1, creds1);
				LocalResourceVisibility vis1 = LocalResourceVisibility.Public;
				local.Handle(new ResourceRequestEvent(rsrcA, vis1, ctxt1));
				dispatcher.Await();
				TestLocalizedResource.LocalizerEventMatcher matchesL1Req = new TestLocalizedResource.LocalizerEventMatcher
					(container1, creds1, vis1, LocalizerEventType.RequestResourceLocalization);
				Org.Mockito.Mockito.Verify(localizerBus).Handle(Matchers.ArgThat(matchesL1Req));
				// Release C0 container localization, verify no notification
				local.Handle(new ResourceReleaseEvent(rsrcA, container0));
				dispatcher.Await();
				Org.Mockito.Mockito.Verify(containerBus, Org.Mockito.Mockito.Never()).Handle(Matchers.IsA
					<ContainerEvent>());
				NUnit.Framework.Assert.AreEqual(ResourceState.Downloading, local.GetState());
				// Release C1 container localization, verify no notification
				local.Handle(new ResourceReleaseEvent(rsrcA, container1));
				dispatcher.Await();
				Org.Mockito.Mockito.Verify(containerBus, Org.Mockito.Mockito.Never()).Handle(Matchers.IsA
					<ContainerEvent>());
				NUnit.Framework.Assert.AreEqual(ResourceState.Downloading, local.GetState());
				// Register C2, C3
				ContainerId container2 = GetMockContainer(2L);
				LocalResourceVisibility vis2 = LocalResourceVisibility.Private;
				Credentials creds2 = new Credentials();
				LocalizerContext ctxt2 = new LocalizerContext("yak", container2, creds2);
				ContainerId container3 = GetMockContainer(3L);
				LocalResourceVisibility vis3 = LocalResourceVisibility.Private;
				Credentials creds3 = new Credentials();
				LocalizerContext ctxt3 = new LocalizerContext("yak", container3, creds3);
				local.Handle(new ResourceRequestEvent(rsrcA, vis2, ctxt2));
				local.Handle(new ResourceRequestEvent(rsrcA, vis3, ctxt3));
				dispatcher.Await();
				TestLocalizedResource.LocalizerEventMatcher matchesL2Req = new TestLocalizedResource.LocalizerEventMatcher
					(container2, creds2, vis2, LocalizerEventType.RequestResourceLocalization);
				Org.Mockito.Mockito.Verify(localizerBus).Handle(Matchers.ArgThat(matchesL2Req));
				TestLocalizedResource.LocalizerEventMatcher matchesL3Req = new TestLocalizedResource.LocalizerEventMatcher
					(container3, creds3, vis3, LocalizerEventType.RequestResourceLocalization);
				Org.Mockito.Mockito.Verify(localizerBus).Handle(Matchers.ArgThat(matchesL3Req));
				// Successful localization. verify notification C2, C3
				Path locA = new Path("file:///cache/rsrcA");
				local.Handle(new ResourceLocalizedEvent(rsrcA, locA, 10));
				dispatcher.Await();
				TestLocalizedResource.ContainerEventMatcher matchesC2Localized = new TestLocalizedResource.ContainerEventMatcher
					(container2, ContainerEventType.ResourceLocalized);
				TestLocalizedResource.ContainerEventMatcher matchesC3Localized = new TestLocalizedResource.ContainerEventMatcher
					(container3, ContainerEventType.ResourceLocalized);
				Org.Mockito.Mockito.Verify(containerBus).Handle(Matchers.ArgThat(matchesC2Localized
					));
				Org.Mockito.Mockito.Verify(containerBus).Handle(Matchers.ArgThat(matchesC3Localized
					));
				NUnit.Framework.Assert.AreEqual(ResourceState.Localized, local.GetState());
				// Register C4, verify notification
				ContainerId container4 = GetMockContainer(4L);
				Credentials creds4 = new Credentials();
				LocalizerContext ctxt4 = new LocalizerContext("yak", container4, creds4);
				LocalResourceVisibility vis4 = LocalResourceVisibility.Private;
				local.Handle(new ResourceRequestEvent(rsrcA, vis4, ctxt4));
				dispatcher.Await();
				TestLocalizedResource.ContainerEventMatcher matchesC4Localized = new TestLocalizedResource.ContainerEventMatcher
					(container4, ContainerEventType.ResourceLocalized);
				Org.Mockito.Mockito.Verify(containerBus).Handle(Matchers.ArgThat(matchesC4Localized
					));
				NUnit.Framework.Assert.AreEqual(ResourceState.Localized, local.GetState());
			}
			finally
			{
				dispatcher.Stop();
			}
		}

		internal static LocalResource CreateMockResource()
		{
			// mock rsrc location
			URL uriA = Org.Mockito.Mockito.Mock<URL>();
			Org.Mockito.Mockito.When(uriA.GetScheme()).ThenReturn("file");
			Org.Mockito.Mockito.When(uriA.GetHost()).ThenReturn(null);
			Org.Mockito.Mockito.When(uriA.GetFile()).ThenReturn("/localA/rsrc");
			LocalResource apiRsrc = Org.Mockito.Mockito.Mock<LocalResource>();
			Org.Mockito.Mockito.When(apiRsrc.GetResource()).ThenReturn(uriA);
			Org.Mockito.Mockito.When(apiRsrc.GetTimestamp()).ThenReturn(4344L);
			Org.Mockito.Mockito.When(apiRsrc.GetType()).ThenReturn(LocalResourceType.File);
			return apiRsrc;
		}

		internal class LocalizerEventMatcher : ArgumentMatcher<LocalizerEvent>
		{
			internal Credentials creds;

			internal LocalResourceVisibility vis;

			private readonly ContainerId idRef;

			private readonly LocalizerEventType type;

			public LocalizerEventMatcher(ContainerId idRef, Credentials creds, LocalResourceVisibility
				 vis, LocalizerEventType type)
			{
				this.vis = vis;
				this.type = type;
				this.creds = creds;
				this.idRef = idRef;
			}

			public override bool Matches(object o)
			{
				if (!(o is LocalizerResourceRequestEvent))
				{
					return false;
				}
				LocalizerResourceRequestEvent evt = (LocalizerResourceRequestEvent)o;
				return idRef == evt.GetContext().GetContainerId() && type == evt.GetType() && vis
					 == evt.GetVisibility() && creds == evt.GetContext().GetCredentials();
			}
		}

		internal class ContainerEventMatcher : ArgumentMatcher<ContainerEvent>
		{
			private readonly ContainerId idRef;

			private readonly ContainerEventType type;

			public ContainerEventMatcher(ContainerId idRef, ContainerEventType type)
			{
				this.idRef = idRef;
				this.type = type;
			}

			public override bool Matches(object o)
			{
				if (!(o is ContainerEvent))
				{
					return false;
				}
				ContainerEvent evt = (ContainerEvent)o;
				return idRef == evt.GetContainerID() && type == evt.GetType();
			}
		}
	}
}
