using System;
using System.Collections.Generic;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Event;
using Org.Apache.Hadoop.Yarn.Security;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Launcher;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Localizer;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Localizer.Event;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Loghandler.Event;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Monitor;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Metrics;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Recovery;
using Org.Apache.Hadoop.Yarn.Server.Utils;
using Org.Mockito;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container
{
	public class TestContainer
	{
		internal readonly NodeManagerMetrics metrics = NodeManagerMetrics.Create();

		internal readonly Configuration conf = new YarnConfiguration();

		internal readonly string FakeLocalizationError = "Fake localization error";

		/// <summary>Verify correct container request events sent to localizer.</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestLocalizationRequest()
		{
			TestContainer.WrappedContainer wc = null;
			try
			{
				wc = new TestContainer.WrappedContainer(this, 7, 314159265358979L, 4344, "yak");
				NUnit.Framework.Assert.AreEqual(ContainerState.New, wc.c.GetContainerState());
				wc.InitContainer();
				// Verify request for public/private resources to localizer
				TestContainer.ResourcesRequestedMatcher matchesReq = new TestContainer.ResourcesRequestedMatcher
					(wc.localResources, EnumSet.Of(LocalResourceVisibility.Public, LocalResourceVisibility
					.Private, LocalResourceVisibility.Application));
				Org.Mockito.Mockito.Verify(wc.localizerBus).Handle(Matchers.ArgThat(matchesReq));
				NUnit.Framework.Assert.AreEqual(ContainerState.Localizing, wc.c.GetContainerState
					());
			}
			finally
			{
				if (wc != null)
				{
					wc.Finished();
				}
			}
		}

		/// <summary>Verify container launch when all resources already cached.</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestLocalizationLaunch()
		{
			TestContainer.WrappedContainer wc = null;
			try
			{
				wc = new TestContainer.WrappedContainer(this, 8, 314159265358979L, 4344, "yak");
				NUnit.Framework.Assert.AreEqual(ContainerState.New, wc.c.GetContainerState());
				wc.InitContainer();
				IDictionary<Path, IList<string>> localPaths = wc.LocalizeResources();
				// all resources should be localized
				NUnit.Framework.Assert.AreEqual(ContainerState.Localized, wc.c.GetContainerState(
					));
				NUnit.Framework.Assert.IsNotNull(wc.c.GetLocalizedResources());
				foreach (KeyValuePair<Path, IList<string>> loc in wc.c.GetLocalizedResources())
				{
					NUnit.Framework.Assert.AreEqual(Sharpen.Collections.Remove(localPaths, loc.Key), 
						loc.Value);
				}
				NUnit.Framework.Assert.IsTrue(localPaths.IsEmpty());
				TestContainer.WrappedContainer wcf = wc;
				// verify container launch
				ArgumentMatcher<ContainersLauncherEvent> matchesContainerLaunch = new _ArgumentMatcher_153
					(wcf);
				Org.Mockito.Mockito.Verify(wc.launcherBus).Handle(Matchers.ArgThat(matchesContainerLaunch
					));
			}
			finally
			{
				if (wc != null)
				{
					wc.Finished();
				}
			}
		}

		private sealed class _ArgumentMatcher_153 : ArgumentMatcher<ContainersLauncherEvent
			>
		{
			public _ArgumentMatcher_153(TestContainer.WrappedContainer wcf)
			{
				this.wcf = wcf;
			}

			public override bool Matches(object o)
			{
				ContainersLauncherEvent launchEvent = (ContainersLauncherEvent)o;
				return wcf.c == launchEvent.GetContainer();
			}

			private readonly TestContainer.WrappedContainer wcf;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestExternalKill()
		{
			// mocked generic
			TestContainer.WrappedContainer wc = null;
			try
			{
				wc = new TestContainer.WrappedContainer(this, 13, 314159265358979L, 4344, "yak");
				wc.InitContainer();
				wc.LocalizeResources();
				int running = metrics.GetRunningContainers();
				wc.LaunchContainer();
				NUnit.Framework.Assert.AreEqual(running + 1, metrics.GetRunningContainers());
				Org.Mockito.Mockito.Reset(wc.localizerBus);
				wc.ContainerKilledOnRequest();
				NUnit.Framework.Assert.AreEqual(ContainerState.ExitedWithFailure, wc.c.GetContainerState
					());
				NUnit.Framework.Assert.IsNull(wc.c.GetLocalizedResources());
				VerifyCleanupCall(wc);
				int failed = metrics.GetFailedContainers();
				wc.ContainerResourcesCleanup();
				NUnit.Framework.Assert.AreEqual(ContainerState.Done, wc.c.GetContainerState());
				NUnit.Framework.Assert.AreEqual(failed + 1, metrics.GetFailedContainers());
				NUnit.Framework.Assert.AreEqual(running, metrics.GetRunningContainers());
			}
			finally
			{
				if (wc != null)
				{
					wc.Finished();
				}
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestCleanupOnFailure()
		{
			// mocked generic
			TestContainer.WrappedContainer wc = null;
			try
			{
				wc = new TestContainer.WrappedContainer(this, 10, 314159265358979L, 4344, "yak");
				wc.InitContainer();
				wc.LocalizeResources();
				wc.LaunchContainer();
				Org.Mockito.Mockito.Reset(wc.localizerBus);
				wc.ContainerFailed(ContainerExecutor.ExitCode.ForceKilled.GetExitCode());
				NUnit.Framework.Assert.AreEqual(ContainerState.ExitedWithFailure, wc.c.GetContainerState
					());
				NUnit.Framework.Assert.IsNull(wc.c.GetLocalizedResources());
				VerifyCleanupCall(wc);
			}
			finally
			{
				if (wc != null)
				{
					wc.Finished();
				}
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestCleanupOnSuccess()
		{
			// mocked generic
			TestContainer.WrappedContainer wc = null;
			try
			{
				wc = new TestContainer.WrappedContainer(this, 11, 314159265358979L, 4344, "yak");
				wc.InitContainer();
				wc.LocalizeResources();
				int running = metrics.GetRunningContainers();
				wc.LaunchContainer();
				NUnit.Framework.Assert.AreEqual(running + 1, metrics.GetRunningContainers());
				Org.Mockito.Mockito.Reset(wc.localizerBus);
				wc.ContainerSuccessful();
				NUnit.Framework.Assert.AreEqual(ContainerState.ExitedWithSuccess, wc.c.GetContainerState
					());
				NUnit.Framework.Assert.IsNull(wc.c.GetLocalizedResources());
				VerifyCleanupCall(wc);
				int completed = metrics.GetCompletedContainers();
				wc.ContainerResourcesCleanup();
				NUnit.Framework.Assert.AreEqual(ContainerState.Done, wc.c.GetContainerState());
				NUnit.Framework.Assert.AreEqual(completed + 1, metrics.GetCompletedContainers());
				NUnit.Framework.Assert.AreEqual(running, metrics.GetRunningContainers());
			}
			finally
			{
				if (wc != null)
				{
					wc.Finished();
				}
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestInitWhileDone()
		{
			// mocked generic
			TestContainer.WrappedContainer wc = null;
			try
			{
				wc = new TestContainer.WrappedContainer(this, 6, 314159265358979L, 4344, "yak");
				wc.InitContainer();
				wc.LocalizeResources();
				wc.LaunchContainer();
				Org.Mockito.Mockito.Reset(wc.localizerBus);
				wc.ContainerSuccessful();
				wc.ContainerResourcesCleanup();
				NUnit.Framework.Assert.AreEqual(ContainerState.Done, wc.c.GetContainerState());
				NUnit.Framework.Assert.IsNull(wc.c.GetLocalizedResources());
				// Now in DONE, issue INIT
				wc.InitContainer();
				// Verify still in DONE
				NUnit.Framework.Assert.AreEqual(ContainerState.Done, wc.c.GetContainerState());
				NUnit.Framework.Assert.IsNull(wc.c.GetLocalizedResources());
				VerifyCleanupCall(wc);
			}
			finally
			{
				if (wc != null)
				{
					wc.Finished();
				}
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestLocalizationFailureAtDone()
		{
			// mocked generic
			TestContainer.WrappedContainer wc = null;
			try
			{
				wc = new TestContainer.WrappedContainer(this, 6, 314159265358979L, 4344, "yak");
				wc.InitContainer();
				wc.LocalizeResources();
				wc.LaunchContainer();
				Org.Mockito.Mockito.Reset(wc.localizerBus);
				wc.ContainerSuccessful();
				wc.ContainerResourcesCleanup();
				NUnit.Framework.Assert.AreEqual(ContainerState.Done, wc.c.GetContainerState());
				NUnit.Framework.Assert.IsNull(wc.c.GetLocalizedResources());
				// Now in DONE, issue RESOURCE_FAILED as done by LocalizeRunner
				wc.ResourceFailedContainer();
				// Verify still in DONE
				NUnit.Framework.Assert.AreEqual(ContainerState.Done, wc.c.GetContainerState());
				NUnit.Framework.Assert.IsNull(wc.c.GetLocalizedResources());
				VerifyCleanupCall(wc);
			}
			finally
			{
				if (wc != null)
				{
					wc.Finished();
				}
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestCleanupOnKillRequest()
		{
			// mocked generic
			TestContainer.WrappedContainer wc = null;
			try
			{
				wc = new TestContainer.WrappedContainer(this, 12, 314159265358979L, 4344, "yak");
				wc.InitContainer();
				wc.LocalizeResources();
				wc.LaunchContainer();
				Org.Mockito.Mockito.Reset(wc.localizerBus);
				wc.KillContainer();
				NUnit.Framework.Assert.AreEqual(ContainerState.Killing, wc.c.GetContainerState());
				NUnit.Framework.Assert.IsNull(wc.c.GetLocalizedResources());
				wc.ContainerKilledOnRequest();
				VerifyCleanupCall(wc);
			}
			finally
			{
				if (wc != null)
				{
					wc.Finished();
				}
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestKillOnNew()
		{
			TestContainer.WrappedContainer wc = null;
			try
			{
				wc = new TestContainer.WrappedContainer(this, 13, 314159265358979L, 4344, "yak");
				NUnit.Framework.Assert.AreEqual(ContainerState.New, wc.c.GetContainerState());
				int killed = metrics.GetKilledContainers();
				wc.KillContainer();
				NUnit.Framework.Assert.AreEqual(ContainerState.Done, wc.c.GetContainerState());
				NUnit.Framework.Assert.AreEqual(ContainerExitStatus.KilledByResourcemanager, wc.c
					.CloneAndGetContainerStatus().GetExitStatus());
				NUnit.Framework.Assert.IsTrue(wc.c.CloneAndGetContainerStatus().GetDiagnostics().
					Contains("KillRequest"));
				NUnit.Framework.Assert.AreEqual(killed + 1, metrics.GetKilledContainers());
			}
			finally
			{
				if (wc != null)
				{
					wc.Finished();
				}
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestKillOnLocalizing()
		{
			TestContainer.WrappedContainer wc = null;
			try
			{
				wc = new TestContainer.WrappedContainer(this, 14, 314159265358979L, 4344, "yak");
				wc.InitContainer();
				NUnit.Framework.Assert.AreEqual(ContainerState.Localizing, wc.c.GetContainerState
					());
				wc.KillContainer();
				NUnit.Framework.Assert.AreEqual(ContainerState.Killing, wc.c.GetContainerState());
				NUnit.Framework.Assert.AreEqual(ContainerExitStatus.KilledByResourcemanager, wc.c
					.CloneAndGetContainerStatus().GetExitStatus());
				NUnit.Framework.Assert.IsTrue(wc.c.CloneAndGetContainerStatus().GetDiagnostics().
					Contains("KillRequest"));
				int killed = metrics.GetKilledContainers();
				wc.ContainerResourcesCleanup();
				NUnit.Framework.Assert.AreEqual(ContainerState.Done, wc.c.GetContainerState());
				NUnit.Framework.Assert.AreEqual(killed + 1, metrics.GetKilledContainers());
			}
			finally
			{
				if (wc != null)
				{
					wc.Finished();
				}
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestKillOnLocalizationFailed()
		{
			TestContainer.WrappedContainer wc = null;
			try
			{
				wc = new TestContainer.WrappedContainer(this, 15, 314159265358979L, 4344, "yak");
				wc.InitContainer();
				wc.FailLocalizeResources(wc.GetLocalResourceCount());
				NUnit.Framework.Assert.AreEqual(ContainerState.LocalizationFailed, wc.c.GetContainerState
					());
				NUnit.Framework.Assert.IsNull(wc.c.GetLocalizedResources());
				wc.KillContainer();
				NUnit.Framework.Assert.AreEqual(ContainerState.LocalizationFailed, wc.c.GetContainerState
					());
				NUnit.Framework.Assert.IsNull(wc.c.GetLocalizedResources());
				VerifyCleanupCall(wc);
				int failed = metrics.GetFailedContainers();
				wc.ContainerResourcesCleanup();
				NUnit.Framework.Assert.AreEqual(ContainerState.Done, wc.c.GetContainerState());
				NUnit.Framework.Assert.AreEqual(failed + 1, metrics.GetFailedContainers());
			}
			finally
			{
				if (wc != null)
				{
					wc.Finished();
				}
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestKillOnLocalizedWhenContainerNotLaunched()
		{
			TestContainer.WrappedContainer wc = null;
			try
			{
				wc = new TestContainer.WrappedContainer(this, 17, 314159265358979L, 4344, "yak");
				wc.InitContainer();
				wc.LocalizeResources();
				NUnit.Framework.Assert.AreEqual(ContainerState.Localized, wc.c.GetContainerState(
					));
				ContainerLaunch launcher = wc.launcher.running[wc.c.GetContainerId()];
				wc.KillContainer();
				NUnit.Framework.Assert.AreEqual(ContainerState.Killing, wc.c.GetContainerState());
				launcher.Call();
				wc.DrainDispatcherEvents();
				NUnit.Framework.Assert.AreEqual(ContainerState.ContainerCleanedupAfterKill, wc.c.
					GetContainerState());
				NUnit.Framework.Assert.IsNull(wc.c.GetLocalizedResources());
				VerifyCleanupCall(wc);
				int killed = metrics.GetKilledContainers();
				wc.c.Handle(new ContainerEvent(wc.c.GetContainerId(), ContainerEventType.ContainerResourcesCleanedup
					));
				NUnit.Framework.Assert.AreEqual(ContainerState.Done, wc.c.GetContainerState());
				NUnit.Framework.Assert.AreEqual(killed + 1, metrics.GetKilledContainers());
				NUnit.Framework.Assert.AreEqual(0, metrics.GetRunningContainers());
			}
			finally
			{
				if (wc != null)
				{
					wc.Finished();
				}
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestKillOnLocalizedWhenContainerLaunched()
		{
			TestContainer.WrappedContainer wc = null;
			try
			{
				wc = new TestContainer.WrappedContainer(this, 17, 314159265358979L, 4344, "yak");
				wc.InitContainer();
				wc.LocalizeResources();
				NUnit.Framework.Assert.AreEqual(ContainerState.Localized, wc.c.GetContainerState(
					));
				ContainerLaunch launcher = wc.launcher.running[wc.c.GetContainerId()];
				launcher.Call();
				wc.DrainDispatcherEvents();
				NUnit.Framework.Assert.AreEqual(ContainerState.ExitedWithFailure, wc.c.GetContainerState
					());
				wc.KillContainer();
				NUnit.Framework.Assert.AreEqual(ContainerState.ExitedWithFailure, wc.c.GetContainerState
					());
				NUnit.Framework.Assert.IsNull(wc.c.GetLocalizedResources());
				VerifyCleanupCall(wc);
			}
			finally
			{
				if (wc != null)
				{
					wc.Finished();
				}
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestResourceLocalizedOnLocalizationFailed()
		{
			TestContainer.WrappedContainer wc = null;
			try
			{
				wc = new TestContainer.WrappedContainer(this, 16, 314159265358979L, 4344, "yak");
				wc.InitContainer();
				int failCount = wc.GetLocalResourceCount() / 2;
				if (failCount == 0)
				{
					failCount = 1;
				}
				wc.FailLocalizeResources(failCount);
				NUnit.Framework.Assert.AreEqual(ContainerState.LocalizationFailed, wc.c.GetContainerState
					());
				NUnit.Framework.Assert.IsNull(wc.c.GetLocalizedResources());
				wc.LocalizeResourcesFromInvalidState(failCount);
				NUnit.Framework.Assert.AreEqual(ContainerState.LocalizationFailed, wc.c.GetContainerState
					());
				NUnit.Framework.Assert.IsNull(wc.c.GetLocalizedResources());
				VerifyCleanupCall(wc);
				NUnit.Framework.Assert.IsTrue(wc.GetDiagnostics().Contains(FakeLocalizationError)
					);
			}
			finally
			{
				if (wc != null)
				{
					wc.Finished();
				}
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestResourceFailedOnLocalizationFailed()
		{
			TestContainer.WrappedContainer wc = null;
			try
			{
				wc = new TestContainer.WrappedContainer(this, 16, 314159265358979L, 4344, "yak");
				wc.InitContainer();
				IEnumerator<string> lRsrcKeys = wc.localResources.Keys.GetEnumerator();
				string key1 = lRsrcKeys.Next();
				string key2 = lRsrcKeys.Next();
				wc.FailLocalizeSpecificResource(key1);
				NUnit.Framework.Assert.AreEqual(ContainerState.LocalizationFailed, wc.c.GetContainerState
					());
				NUnit.Framework.Assert.IsNull(wc.c.GetLocalizedResources());
				wc.FailLocalizeSpecificResource(key2);
				NUnit.Framework.Assert.AreEqual(ContainerState.LocalizationFailed, wc.c.GetContainerState
					());
				NUnit.Framework.Assert.IsNull(wc.c.GetLocalizedResources());
				VerifyCleanupCall(wc);
			}
			finally
			{
				if (wc != null)
				{
					wc.Finished();
				}
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestResourceFailedOnKilling()
		{
			TestContainer.WrappedContainer wc = null;
			try
			{
				wc = new TestContainer.WrappedContainer(this, 16, 314159265358979L, 4344, "yak");
				wc.InitContainer();
				IEnumerator<string> lRsrcKeys = wc.localResources.Keys.GetEnumerator();
				string key1 = lRsrcKeys.Next();
				wc.KillContainer();
				NUnit.Framework.Assert.AreEqual(ContainerState.Killing, wc.c.GetContainerState());
				NUnit.Framework.Assert.IsNull(wc.c.GetLocalizedResources());
				wc.FailLocalizeSpecificResource(key1);
				NUnit.Framework.Assert.AreEqual(ContainerState.Killing, wc.c.GetContainerState());
				NUnit.Framework.Assert.IsNull(wc.c.GetLocalizedResources());
				VerifyCleanupCall(wc);
			}
			finally
			{
				if (wc != null)
				{
					wc.Finished();
				}
			}
		}

		/// <summary>Verify serviceData correctly sent.</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestServiceData()
		{
			TestContainer.WrappedContainer wc = null;
			try
			{
				wc = new TestContainer.WrappedContainer(this, 9, 314159265358979L, 4344, "yak", false
					, true);
				NUnit.Framework.Assert.AreEqual(ContainerState.New, wc.c.GetContainerState());
				wc.InitContainer();
				foreach (KeyValuePair<string, ByteBuffer> e in wc.serviceData)
				{
					ArgumentMatcher<AuxServicesEvent> matchesServiceReq = new _ArgumentMatcher_539(e);
					Org.Mockito.Mockito.Verify(wc.auxBus).Handle(Matchers.ArgThat(matchesServiceReq));
				}
				TestContainer.WrappedContainer wcf = wc;
				// verify launch on empty resource request
				ArgumentMatcher<ContainersLauncherEvent> matchesLaunchReq = new _ArgumentMatcher_553
					(wcf);
				Org.Mockito.Mockito.Verify(wc.launcherBus).Handle(Matchers.ArgThat(matchesLaunchReq
					));
			}
			finally
			{
				if (wc != null)
				{
					wc.Finished();
				}
			}
		}

		private sealed class _ArgumentMatcher_539 : ArgumentMatcher<AuxServicesEvent>
		{
			public _ArgumentMatcher_539(KeyValuePair<string, ByteBuffer> e)
			{
				this.e = e;
			}

			public override bool Matches(object o)
			{
				AuxServicesEvent evt = (AuxServicesEvent)o;
				return e.Key.Equals(evt.GetServiceID()) && 0 == e.Value.CompareTo(evt.GetServiceData
					());
			}

			private readonly KeyValuePair<string, ByteBuffer> e;
		}

		private sealed class _ArgumentMatcher_553 : ArgumentMatcher<ContainersLauncherEvent
			>
		{
			public _ArgumentMatcher_553(TestContainer.WrappedContainer wcf)
			{
				this.wcf = wcf;
			}

			public override bool Matches(object o)
			{
				ContainersLauncherEvent evt = (ContainersLauncherEvent)o;
				return evt.GetType() == ContainersLauncherEventType.LaunchContainer && wcf.cId.Equals
					(evt.GetContainer().GetContainerId());
			}

			private readonly TestContainer.WrappedContainer wcf;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestLaunchAfterKillRequest()
		{
			TestContainer.WrappedContainer wc = null;
			try
			{
				wc = new TestContainer.WrappedContainer(this, 14, 314159265358979L, 4344, "yak");
				wc.InitContainer();
				wc.LocalizeResources();
				wc.KillContainer();
				NUnit.Framework.Assert.AreEqual(ContainerState.Killing, wc.c.GetContainerState());
				NUnit.Framework.Assert.IsNull(wc.c.GetLocalizedResources());
				wc.LaunchContainer();
				NUnit.Framework.Assert.AreEqual(ContainerState.Killing, wc.c.GetContainerState());
				NUnit.Framework.Assert.IsNull(wc.c.GetLocalizedResources());
				wc.ContainerKilledOnRequest();
				VerifyCleanupCall(wc);
			}
			finally
			{
				if (wc != null)
				{
					wc.Finished();
				}
			}
		}

		/// <exception cref="System.Exception"/>
		private void VerifyCleanupCall(TestContainer.WrappedContainer wc)
		{
			TestContainer.ResourcesReleasedMatcher matchesReq = new TestContainer.ResourcesReleasedMatcher
				(wc.localResources, EnumSet.Of(LocalResourceVisibility.Public, LocalResourceVisibility
				.Private, LocalResourceVisibility.Application));
			Org.Mockito.Mockito.Verify(wc.localizerBus).Handle(Matchers.ArgThat(matchesReq));
		}

		private class ResourcesReleasedMatcher : ArgumentMatcher<LocalizationEvent>
		{
			internal readonly HashSet<LocalResourceRequest> resources = new HashSet<LocalResourceRequest
				>();

			/// <exception cref="Sharpen.URISyntaxException"/>
			internal ResourcesReleasedMatcher(IDictionary<string, LocalResource> allResources
				, EnumSet<LocalResourceVisibility> vis)
			{
				foreach (KeyValuePair<string, LocalResource> e in allResources)
				{
					if (vis.Contains(e.Value.GetVisibility()))
					{
						resources.AddItem(new LocalResourceRequest(e.Value));
					}
				}
			}

			public override bool Matches(object o)
			{
				if (!(o is ContainerLocalizationCleanupEvent))
				{
					return false;
				}
				ContainerLocalizationCleanupEvent evt = (ContainerLocalizationCleanupEvent)o;
				HashSet<LocalResourceRequest> expected = new HashSet<LocalResourceRequest>(resources
					);
				foreach (ICollection<LocalResourceRequest> rc in evt.GetResources().Values)
				{
					foreach (LocalResourceRequest rsrc in rc)
					{
						if (!expected.Remove(rsrc))
						{
							return false;
						}
					}
				}
				return expected.IsEmpty();
			}
		}

		private class ResourcesRequestedMatcher : ArgumentMatcher<LocalizationEvent>
		{
			internal readonly HashSet<LocalResourceRequest> resources = new HashSet<LocalResourceRequest
				>();

			/// <exception cref="Sharpen.URISyntaxException"/>
			internal ResourcesRequestedMatcher(IDictionary<string, LocalResource> allResources
				, EnumSet<LocalResourceVisibility> vis)
			{
				// Accept iff the resource payload matches.
				foreach (KeyValuePair<string, LocalResource> e in allResources)
				{
					if (vis.Contains(e.Value.GetVisibility()))
					{
						resources.AddItem(new LocalResourceRequest(e.Value));
					}
				}
			}

			public override bool Matches(object o)
			{
				ContainerLocalizationRequestEvent evt = (ContainerLocalizationRequestEvent)o;
				HashSet<LocalResourceRequest> expected = new HashSet<LocalResourceRequest>(resources
					);
				foreach (ICollection<LocalResourceRequest> rc in evt.GetRequestedResources().Values)
				{
					foreach (LocalResourceRequest rsrc in rc)
					{
						if (!expected.Remove(rsrc))
						{
							return false;
						}
					}
				}
				return expected.IsEmpty();
			}
		}

		private static KeyValuePair<string, LocalResource> GetMockRsrc(Random r, LocalResourceVisibility
			 vis)
		{
			string name = long.ToHexString(r.NextLong());
			URL url = BuilderUtils.NewURL("file", null, 0, "/local" + vis + "/" + name);
			LocalResource rsrc = BuilderUtils.NewLocalResource(url, LocalResourceType.File, vis
				, r.Next(1024) + 1024L, r.Next(1024) + 2048L, false);
			return new AbstractMap.SimpleEntry<string, LocalResource>(name, rsrc);
		}

		private static IDictionary<string, LocalResource> CreateLocalResources(Random r)
		{
			IDictionary<string, LocalResource> localResources = new Dictionary<string, LocalResource
				>();
			for (int i = r.Next(5) + 5; i >= 0; --i)
			{
				KeyValuePair<string, LocalResource> rsrc = GetMockRsrc(r, LocalResourceVisibility
					.Public);
				localResources[rsrc.Key] = rsrc.Value;
			}
			for (int i_1 = r.Next(5) + 5; i_1 >= 0; --i_1)
			{
				KeyValuePair<string, LocalResource> rsrc = GetMockRsrc(r, LocalResourceVisibility
					.Private);
				localResources[rsrc.Key] = rsrc.Value;
			}
			for (int i_2 = r.Next(2) + 2; i_2 >= 0; --i_2)
			{
				KeyValuePair<string, LocalResource> rsrc = GetMockRsrc(r, LocalResourceVisibility
					.Application);
				localResources[rsrc.Key] = rsrc.Value;
			}
			return localResources;
		}

		private static IDictionary<string, ByteBuffer> CreateServiceData(Random r)
		{
			IDictionary<string, ByteBuffer> serviceData = new Dictionary<string, ByteBuffer>(
				);
			for (int i = r.Next(5) + 5; i >= 0; --i)
			{
				string service = long.ToHexString(r.NextLong());
				byte[] b = new byte[r.Next(1024) + 1024];
				r.NextBytes(b);
				serviceData[service] = ByteBuffer.Wrap(b);
			}
			return serviceData;
		}

		private class WrappedContainer
		{
			internal readonly DrainDispatcher dispatcher;

			internal readonly EventHandler<LocalizationEvent> localizerBus;

			internal readonly EventHandler<ContainersLauncherEvent> launcherBus;

			internal readonly EventHandler<ContainersMonitorEvent> monitorBus;

			internal readonly EventHandler<AuxServicesEvent> auxBus;

			internal readonly EventHandler<ApplicationEvent> appBus;

			internal readonly EventHandler<LogHandlerEvent> LogBus;

			internal readonly ContainersLauncher launcher;

			internal readonly ContainerLaunchContext ctxt;

			internal readonly ContainerId cId;

			internal readonly Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container
				 c;

			internal readonly IDictionary<string, LocalResource> localResources;

			internal readonly IDictionary<string, ByteBuffer> serviceData;

			/// <exception cref="System.IO.IOException"/>
			internal WrappedContainer(TestContainer _enclosing, int appId, long timestamp, int
				 id, string user)
				: this(appId, timestamp, id, user, true, false)
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="System.IO.IOException"/>
			internal WrappedContainer(TestContainer _enclosing, int appId, long timestamp, int
				 id, string user, bool withLocalRes, bool withServiceData)
			{
				this._enclosing = _enclosing;
				this.dispatcher = new DrainDispatcher();
				this.dispatcher.Init(new Configuration());
				this.localizerBus = Org.Mockito.Mockito.Mock<EventHandler>();
				this.launcherBus = Org.Mockito.Mockito.Mock<EventHandler>();
				this.monitorBus = Org.Mockito.Mockito.Mock<EventHandler>();
				this.auxBus = Org.Mockito.Mockito.Mock<EventHandler>();
				this.appBus = Org.Mockito.Mockito.Mock<EventHandler>();
				this.LogBus = Org.Mockito.Mockito.Mock<EventHandler>();
				this.dispatcher.Register(typeof(LocalizationEventType), this.localizerBus);
				this.dispatcher.Register(typeof(ContainersLauncherEventType), this.launcherBus);
				this.dispatcher.Register(typeof(ContainersMonitorEventType), this.monitorBus);
				this.dispatcher.Register(typeof(AuxServicesEventType), this.auxBus);
				this.dispatcher.Register(typeof(ApplicationEventType), this.appBus);
				this.dispatcher.Register(typeof(LogHandlerEventType), this.LogBus);
				Context context = Org.Mockito.Mockito.Mock<Context>();
				Org.Mockito.Mockito.When(context.GetApplications()).ThenReturn(new ConcurrentHashMap
					<ApplicationId, Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application.Application
					>());
				NMNullStateStoreService stateStore = new NMNullStateStoreService();
				Org.Mockito.Mockito.When(context.GetNMStateStore()).ThenReturn(stateStore);
				ContainerExecutor executor = Org.Mockito.Mockito.Mock<ContainerExecutor>();
				this.launcher = new ContainersLauncher(context, this.dispatcher, executor, null, 
					null);
				// create a mock ExecutorService, which will not really launch
				// ContainerLaunch at all.
				this.launcher.containerLauncher = Org.Mockito.Mockito.Mock<ExecutorService>();
				Future future = Org.Mockito.Mockito.Mock<Future>();
				Org.Mockito.Mockito.When(this.launcher.containerLauncher.Submit(Matchers.Any<Callable
					>())).ThenReturn(future);
				Org.Mockito.Mockito.When(future.IsDone()).ThenReturn(false);
				Org.Mockito.Mockito.When(future.Cancel(false)).ThenReturn(true);
				this.launcher.Init(new Configuration());
				this.launcher.Start();
				this.dispatcher.Register(typeof(ContainersLauncherEventType), this.launcher);
				this.ctxt = Org.Mockito.Mockito.Mock<ContainerLaunchContext>();
				Org.Apache.Hadoop.Yarn.Api.Records.Container mockContainer = Org.Mockito.Mockito.
					Mock<Org.Apache.Hadoop.Yarn.Api.Records.Container>();
				this.cId = BuilderUtils.NewContainerId(appId, 1, timestamp, id);
				Org.Mockito.Mockito.When(mockContainer.GetId()).ThenReturn(this.cId);
				Resource resource = BuilderUtils.NewResource(1024, 1);
				Org.Mockito.Mockito.When(mockContainer.GetResource()).ThenReturn(resource);
				string host = "127.0.0.1";
				int port = 1234;
				long currentTime = Runtime.CurrentTimeMillis();
				ContainerTokenIdentifier identifier = new ContainerTokenIdentifier(this.cId, "127.0.0.1"
					, user, resource, currentTime + 10000L, 123, currentTime, Priority.NewInstance(0
					), 0);
				Token token = BuilderUtils.NewContainerToken(BuilderUtils.NewNodeId(host, port), 
					Sharpen.Runtime.GetBytesForString("password"), identifier);
				Org.Mockito.Mockito.When(mockContainer.GetContainerToken()).ThenReturn(token);
				if (withLocalRes)
				{
					Random r = new Random();
					long seed = r.NextLong();
					r.SetSeed(seed);
					System.Console.Out.WriteLine("WrappedContainerLocalResource seed: " + seed);
					this.localResources = TestContainer.CreateLocalResources(r);
				}
				else
				{
					this.localResources = Sharpen.Collections.EmptyMap<string, LocalResource>();
				}
				Org.Mockito.Mockito.When(this.ctxt.GetLocalResources()).ThenReturn(this.localResources
					);
				if (withServiceData)
				{
					Random r = new Random();
					long seed = r.NextLong();
					r.SetSeed(seed);
					System.Console.Out.WriteLine("ServiceData seed: " + seed);
					this.serviceData = TestContainer.CreateServiceData(r);
				}
				else
				{
					this.serviceData = Sharpen.Collections.EmptyMap<string, ByteBuffer>();
				}
				Org.Mockito.Mockito.When(this.ctxt.GetServiceData()).ThenReturn(this.serviceData);
				this.c = new ContainerImpl(this._enclosing.conf, this.dispatcher, new NMNullStateStoreService
					(), this.ctxt, null, this._enclosing.metrics, identifier);
				this.dispatcher.Register(typeof(ContainerEventType), new _EventHandler_813(this));
				this.dispatcher.Start();
			}

			private sealed class _EventHandler_813 : EventHandler<ContainerEvent>
			{
				public _EventHandler_813(WrappedContainer _enclosing)
				{
					this._enclosing = _enclosing;
				}

				public void Handle(ContainerEvent @event)
				{
					this._enclosing.c.Handle(@event);
				}

				private readonly WrappedContainer _enclosing;
			}

			private void DrainDispatcherEvents()
			{
				this.dispatcher.Await();
			}

			public virtual void Finished()
			{
				this.dispatcher.Stop();
			}

			public virtual void InitContainer()
			{
				this.c.Handle(new ContainerEvent(this.cId, ContainerEventType.InitContainer));
				this.DrainDispatcherEvents();
			}

			public virtual void ResourceFailedContainer()
			{
				this.c.Handle(new ContainerEvent(this.cId, ContainerEventType.ResourceFailed));
				this.DrainDispatcherEvents();
			}

			// Localize resources 
			// Skip some resources so as to consider them failed
			/// <exception cref="Sharpen.URISyntaxException"/>
			public virtual IDictionary<Path, IList<string>> DoLocalizeResources(bool checkLocalizingState
				, int skipRsrcCount)
			{
				Path cache = new Path("file:///cache");
				IDictionary<Path, IList<string>> localPaths = new Dictionary<Path, IList<string>>
					();
				int counter = 0;
				foreach (KeyValuePair<string, LocalResource> rsrc in this.localResources)
				{
					if (counter++ < skipRsrcCount)
					{
						continue;
					}
					if (checkLocalizingState)
					{
						NUnit.Framework.Assert.AreEqual(ContainerState.Localizing, this.c.GetContainerState
							());
					}
					LocalResourceRequest req = new LocalResourceRequest(rsrc.Value);
					Path p = new Path(cache, rsrc.Key);
					localPaths[p] = Arrays.AsList(rsrc.Key);
					// rsrc copied to p
					this.c.Handle(new ContainerResourceLocalizedEvent(this.c.GetContainerId(), req, p
						));
				}
				this.DrainDispatcherEvents();
				return localPaths;
			}

			/// <exception cref="Sharpen.URISyntaxException"/>
			public virtual IDictionary<Path, IList<string>> LocalizeResources()
			{
				return this.DoLocalizeResources(true, 0);
			}

			/// <exception cref="Sharpen.URISyntaxException"/>
			public virtual void LocalizeResourcesFromInvalidState(int skipRsrcCount)
			{
				this.DoLocalizeResources(false, skipRsrcCount);
			}

			/// <exception cref="Sharpen.URISyntaxException"/>
			public virtual void FailLocalizeSpecificResource(string rsrcKey)
			{
				LocalResource rsrc = this.localResources[rsrcKey];
				LocalResourceRequest req = new LocalResourceRequest(rsrc);
				Exception e = new Exception(this._enclosing.FakeLocalizationError);
				this.c.Handle(new ContainerResourceFailedEvent(this.c.GetContainerId(), req, e.Message
					));
				this.DrainDispatcherEvents();
			}

			// fail to localize some resources
			/// <exception cref="Sharpen.URISyntaxException"/>
			public virtual void FailLocalizeResources(int failRsrcCount)
			{
				int counter = 0;
				foreach (KeyValuePair<string, LocalResource> rsrc in this.localResources)
				{
					if (counter >= failRsrcCount)
					{
						break;
					}
					++counter;
					LocalResourceRequest req = new LocalResourceRequest(rsrc.Value);
					Exception e = new Exception(this._enclosing.FakeLocalizationError);
					this.c.Handle(new ContainerResourceFailedEvent(this.c.GetContainerId(), req, e.Message
						));
				}
				this.DrainDispatcherEvents();
			}

			public virtual void LaunchContainer()
			{
				this.c.Handle(new ContainerEvent(this.cId, ContainerEventType.ContainerLaunched));
				this.DrainDispatcherEvents();
			}

			public virtual void ContainerSuccessful()
			{
				this.c.Handle(new ContainerEvent(this.cId, ContainerEventType.ContainerExitedWithSuccess
					));
				this.DrainDispatcherEvents();
			}

			public virtual void ContainerResourcesCleanup()
			{
				this.c.Handle(new ContainerEvent(this.cId, ContainerEventType.ContainerResourcesCleanedup
					));
				this.DrainDispatcherEvents();
			}

			public virtual void ContainerFailed(int exitCode)
			{
				string diagnosticMsg = "Container completed with exit code " + exitCode;
				this.c.Handle(new ContainerExitEvent(this.cId, ContainerEventType.ContainerExitedWithFailure
					, exitCode, diagnosticMsg));
				ContainerStatus containerStatus = this.c.CloneAndGetContainerStatus();
				System.Diagnostics.Debug.Assert(containerStatus.GetDiagnostics().Contains(diagnosticMsg
					));
				System.Diagnostics.Debug.Assert(containerStatus.GetExitStatus() == exitCode);
				this.DrainDispatcherEvents();
			}

			public virtual void KillContainer()
			{
				this.c.Handle(new ContainerKillEvent(this.cId, ContainerExitStatus.KilledByResourcemanager
					, "KillRequest"));
				this.DrainDispatcherEvents();
			}

			public virtual void ContainerKilledOnRequest()
			{
				int exitCode = ContainerExitStatus.KilledByResourcemanager;
				string diagnosticMsg = "Container completed with exit code " + exitCode;
				this.c.Handle(new ContainerExitEvent(this.cId, ContainerEventType.ContainerKilledOnRequest
					, exitCode, diagnosticMsg));
				ContainerStatus containerStatus = this.c.CloneAndGetContainerStatus();
				System.Diagnostics.Debug.Assert(containerStatus.GetDiagnostics().Contains(diagnosticMsg
					));
				System.Diagnostics.Debug.Assert(containerStatus.GetExitStatus() == exitCode);
				this.DrainDispatcherEvents();
			}

			public virtual int GetLocalResourceCount()
			{
				return this.localResources.Count;
			}

			public virtual string GetDiagnostics()
			{
				return this.c.CloneAndGetContainerStatus().GetDiagnostics();
			}

			private readonly TestContainer _enclosing;
		}
	}
}
