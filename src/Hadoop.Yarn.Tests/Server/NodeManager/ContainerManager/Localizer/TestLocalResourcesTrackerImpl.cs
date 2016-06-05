using System;
using System.Collections.Generic;
using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Api.Records.Impl.PB;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Event;
using Org.Apache.Hadoop.Yarn.Proto;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Localizer.Event;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Recovery;
using Org.Apache.Hadoop.Yarn.Server.Utils;
using Org.Mockito;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Localizer
{
	public class TestLocalResourcesTrackerImpl
	{
		public virtual void Test()
		{
			string user = "testuser";
			DrainDispatcher dispatcher = null;
			try
			{
				Configuration conf = new Configuration();
				dispatcher = CreateDispatcher(conf);
				EventHandler<LocalizerEvent> localizerEventHandler = Org.Mockito.Mockito.Mock<EventHandler
					>();
				EventHandler<LocalizerEvent> containerEventHandler = Org.Mockito.Mockito.Mock<EventHandler
					>();
				dispatcher.Register(typeof(LocalizerEventType), localizerEventHandler);
				dispatcher.Register(typeof(ContainerEventType), containerEventHandler);
				DeletionService mockDelService = Org.Mockito.Mockito.Mock<DeletionService>();
				ContainerId cId1 = BuilderUtils.NewContainerId(1, 1, 1, 1);
				LocalizerContext lc1 = new LocalizerContext(user, cId1, null);
				ContainerId cId2 = BuilderUtils.NewContainerId(1, 1, 1, 2);
				LocalizerContext lc2 = new LocalizerContext(user, cId2, null);
				LocalResourceRequest req1 = CreateLocalResourceRequest(user, 1, 1, LocalResourceVisibility
					.Public);
				LocalResourceRequest req2 = CreateLocalResourceRequest(user, 2, 1, LocalResourceVisibility
					.Public);
				LocalizedResource lr1 = CreateLocalizedResource(req1, dispatcher);
				LocalizedResource lr2 = CreateLocalizedResource(req2, dispatcher);
				ConcurrentMap<LocalResourceRequest, LocalizedResource> localrsrc = new ConcurrentHashMap
					<LocalResourceRequest, LocalizedResource>();
				localrsrc[req1] = lr1;
				localrsrc[req2] = lr2;
				LocalResourcesTracker tracker = new LocalResourcesTrackerImpl(user, null, dispatcher
					, localrsrc, false, conf, new NMNullStateStoreService());
				ResourceEvent req11Event = new ResourceRequestEvent(req1, LocalResourceVisibility
					.Public, lc1);
				ResourceEvent req12Event = new ResourceRequestEvent(req1, LocalResourceVisibility
					.Public, lc2);
				ResourceEvent req21Event = new ResourceRequestEvent(req2, LocalResourceVisibility
					.Public, lc1);
				ResourceEvent rel11Event = new ResourceReleaseEvent(req1, cId1);
				ResourceEvent rel12Event = new ResourceReleaseEvent(req1, cId2);
				ResourceEvent rel21Event = new ResourceReleaseEvent(req2, cId1);
				// Localize R1 for C1
				tracker.Handle(req11Event);
				// Localize R1 for C2
				tracker.Handle(req12Event);
				// Localize R2 for C1
				tracker.Handle(req21Event);
				dispatcher.Await();
				Org.Mockito.Mockito.Verify(localizerEventHandler, Org.Mockito.Mockito.Times(3)).Handle
					(Matchers.Any<LocalizerResourceRequestEvent>());
				// Verify refCount for R1 is 2
				NUnit.Framework.Assert.AreEqual(2, lr1.GetRefCount());
				// Verify refCount for R2 is 1
				NUnit.Framework.Assert.AreEqual(1, lr2.GetRefCount());
				// Release R2 for C1
				tracker.Handle(rel21Event);
				dispatcher.Await();
				VerifyTrackedResourceCount(tracker, 2);
				// Verify resource with non zero ref count is not removed.
				NUnit.Framework.Assert.AreEqual(2, lr1.GetRefCount());
				NUnit.Framework.Assert.IsFalse(tracker.Remove(lr1, mockDelService));
				VerifyTrackedResourceCount(tracker, 2);
				// Localize resource1
				ResourceLocalizedEvent rle = new ResourceLocalizedEvent(req1, new Path("file:///tmp/r1"
					), 1);
				lr1.Handle(rle);
				NUnit.Framework.Assert.IsTrue(lr1.GetState().Equals(ResourceState.Localized));
				// Release resource1
				tracker.Handle(rel11Event);
				tracker.Handle(rel12Event);
				NUnit.Framework.Assert.AreEqual(0, lr1.GetRefCount());
				// Verify resources in state LOCALIZED with ref-count=0 is removed.
				NUnit.Framework.Assert.IsTrue(tracker.Remove(lr1, mockDelService));
				VerifyTrackedResourceCount(tracker, 1);
			}
			finally
			{
				if (dispatcher != null)
				{
					dispatcher.Stop();
				}
			}
		}

		public virtual void TestConsistency()
		{
			string user = "testuser";
			DrainDispatcher dispatcher = null;
			try
			{
				Configuration conf = new Configuration();
				dispatcher = CreateDispatcher(conf);
				EventHandler<LocalizerEvent> localizerEventHandler = Org.Mockito.Mockito.Mock<EventHandler
					>();
				EventHandler<LocalizerEvent> containerEventHandler = Org.Mockito.Mockito.Mock<EventHandler
					>();
				dispatcher.Register(typeof(LocalizerEventType), localizerEventHandler);
				dispatcher.Register(typeof(ContainerEventType), containerEventHandler);
				ContainerId cId1 = BuilderUtils.NewContainerId(1, 1, 1, 1);
				LocalizerContext lc1 = new LocalizerContext(user, cId1, null);
				LocalResourceRequest req1 = CreateLocalResourceRequest(user, 1, 1, LocalResourceVisibility
					.Public);
				LocalizedResource lr1 = CreateLocalizedResource(req1, dispatcher);
				ConcurrentMap<LocalResourceRequest, LocalizedResource> localrsrc = new ConcurrentHashMap
					<LocalResourceRequest, LocalizedResource>();
				localrsrc[req1] = lr1;
				LocalResourcesTracker tracker = new LocalResourcesTrackerImpl(user, null, dispatcher
					, localrsrc, false, conf, new NMNullStateStoreService());
				ResourceEvent req11Event = new ResourceRequestEvent(req1, LocalResourceVisibility
					.Public, lc1);
				ResourceEvent rel11Event = new ResourceReleaseEvent(req1, cId1);
				// Localize R1 for C1
				tracker.Handle(req11Event);
				dispatcher.Await();
				// Verify refCount for R1 is 1
				NUnit.Framework.Assert.AreEqual(1, lr1.GetRefCount());
				dispatcher.Await();
				VerifyTrackedResourceCount(tracker, 1);
				// Localize resource1
				ResourceLocalizedEvent rle = new ResourceLocalizedEvent(req1, new Path("file:///tmp/r1"
					), 1);
				lr1.Handle(rle);
				NUnit.Framework.Assert.IsTrue(lr1.GetState().Equals(ResourceState.Localized));
				NUnit.Framework.Assert.IsTrue(Createdummylocalizefile(new Path("file:///tmp/r1"))
					);
				LocalizedResource rsrcbefore = tracker.GetEnumerator().Next();
				FilePath resFile = new FilePath(lr1.GetLocalPath().ToUri().GetRawPath().ToString(
					));
				NUnit.Framework.Assert.IsTrue(resFile.Exists());
				NUnit.Framework.Assert.IsTrue(resFile.Delete());
				// Localize R1 for C1
				tracker.Handle(req11Event);
				dispatcher.Await();
				lr1.Handle(rle);
				NUnit.Framework.Assert.IsTrue(lr1.GetState().Equals(ResourceState.Localized));
				LocalizedResource rsrcafter = tracker.GetEnumerator().Next();
				if (rsrcbefore == rsrcafter)
				{
					NUnit.Framework.Assert.Fail("Localized resource should not be equal");
				}
				// Release resource1
				tracker.Handle(rel11Event);
			}
			finally
			{
				if (dispatcher != null)
				{
					dispatcher.Stop();
				}
			}
		}

		public virtual void TestLocalResourceCache()
		{
			string user = "testuser";
			DrainDispatcher dispatcher = null;
			try
			{
				Configuration conf = new Configuration();
				dispatcher = CreateDispatcher(conf);
				EventHandler<LocalizerEvent> localizerEventHandler = Org.Mockito.Mockito.Mock<EventHandler
					>();
				EventHandler<ContainerEvent> containerEventHandler = Org.Mockito.Mockito.Mock<EventHandler
					>();
				// Registering event handlers.
				dispatcher.Register(typeof(LocalizerEventType), localizerEventHandler);
				dispatcher.Register(typeof(ContainerEventType), containerEventHandler);
				ConcurrentMap<LocalResourceRequest, LocalizedResource> localrsrc = new ConcurrentHashMap
					<LocalResourceRequest, LocalizedResource>();
				LocalResourcesTracker tracker = new LocalResourcesTrackerImpl(user, null, dispatcher
					, localrsrc, true, conf, new NMNullStateStoreService());
				LocalResourceRequest lr = CreateLocalResourceRequest(user, 1, 1, LocalResourceVisibility
					.Public);
				// Creating 2 containers for same application which will be requesting
				// same local resource.
				// Container 1 requesting local resource.
				ContainerId cId1 = BuilderUtils.NewContainerId(1, 1, 1, 1);
				LocalizerContext lc1 = new LocalizerContext(user, cId1, null);
				ResourceEvent reqEvent1 = new ResourceRequestEvent(lr, LocalResourceVisibility.Private
					, lc1);
				// No resource request is initially present in local cache
				NUnit.Framework.Assert.AreEqual(0, localrsrc.Count);
				// Container-1 requesting local resource.
				tracker.Handle(reqEvent1);
				dispatcher.Await();
				// New localized Resource should have been added to local resource map
				// and the requesting container will be added to its waiting queue.
				NUnit.Framework.Assert.AreEqual(1, localrsrc.Count);
				NUnit.Framework.Assert.IsTrue(localrsrc.Contains(lr));
				NUnit.Framework.Assert.AreEqual(1, localrsrc[lr].GetRefCount());
				NUnit.Framework.Assert.IsTrue(localrsrc[lr].@ref.Contains(cId1));
				NUnit.Framework.Assert.AreEqual(ResourceState.Downloading, localrsrc[lr].GetState
					());
				// Container 2 requesting the resource
				ContainerId cId2 = BuilderUtils.NewContainerId(1, 1, 1, 2);
				LocalizerContext lc2 = new LocalizerContext(user, cId2, null);
				ResourceEvent reqEvent2 = new ResourceRequestEvent(lr, LocalResourceVisibility.Private
					, lc2);
				tracker.Handle(reqEvent2);
				dispatcher.Await();
				// Container 2 should have been added to the waiting queue of the local
				// resource
				NUnit.Framework.Assert.AreEqual(2, localrsrc[lr].GetRefCount());
				NUnit.Framework.Assert.IsTrue(localrsrc[lr].@ref.Contains(cId2));
				// Failing resource localization
				ResourceEvent resourceFailedEvent = new ResourceFailedLocalizationEvent(lr, (new 
					Exception("test").Message));
				// Backing up the resource to track its state change as it will be
				// removed after the failed event.
				LocalizedResource localizedResource = localrsrc[lr];
				tracker.Handle(resourceFailedEvent);
				dispatcher.Await();
				// After receiving failed resource event; all waiting containers will be
				// notified with Container Resource Failed Event.
				NUnit.Framework.Assert.AreEqual(0, localrsrc.Count);
				Org.Mockito.Mockito.Verify(containerEventHandler, Org.Mockito.Mockito.Timeout(1000
					).Times(2)).Handle(Matchers.IsA<ContainerResourceFailedEvent>());
				NUnit.Framework.Assert.AreEqual(ResourceState.Failed, localizedResource.GetState(
					));
				// Container 1 trying to release the resource (This resource is already
				// deleted from the cache. This call should return silently without
				// exception.
				ResourceReleaseEvent relEvent1 = new ResourceReleaseEvent(lr, cId1);
				tracker.Handle(relEvent1);
				dispatcher.Await();
				// Container-3 now requests for the same resource. This request call
				// is coming prior to Container-2's release call.
				ContainerId cId3 = BuilderUtils.NewContainerId(1, 1, 1, 3);
				LocalizerContext lc3 = new LocalizerContext(user, cId3, null);
				ResourceEvent reqEvent3 = new ResourceRequestEvent(lr, LocalResourceVisibility.Private
					, lc3);
				tracker.Handle(reqEvent3);
				dispatcher.Await();
				// Local resource cache now should have the requested resource and the
				// number of waiting containers should be 1.
				NUnit.Framework.Assert.AreEqual(1, localrsrc.Count);
				NUnit.Framework.Assert.IsTrue(localrsrc.Contains(lr));
				NUnit.Framework.Assert.AreEqual(1, localrsrc[lr].GetRefCount());
				NUnit.Framework.Assert.IsTrue(localrsrc[lr].@ref.Contains(cId3));
				// Container-2 Releases the resource
				ResourceReleaseEvent relEvent2 = new ResourceReleaseEvent(lr, cId2);
				tracker.Handle(relEvent2);
				dispatcher.Await();
				// Making sure that there is no change in the cache after the release.
				NUnit.Framework.Assert.AreEqual(1, localrsrc.Count);
				NUnit.Framework.Assert.IsTrue(localrsrc.Contains(lr));
				NUnit.Framework.Assert.AreEqual(1, localrsrc[lr].GetRefCount());
				NUnit.Framework.Assert.IsTrue(localrsrc[lr].@ref.Contains(cId3));
				// Sending ResourceLocalizedEvent to tracker. In turn resource should
				// send Container Resource Localized Event to waiting containers.
				Path localizedPath = new Path("/tmp/file1");
				ResourceLocalizedEvent localizedEvent = new ResourceLocalizedEvent(lr, localizedPath
					, 123L);
				tracker.Handle(localizedEvent);
				dispatcher.Await();
				// Verifying ContainerResourceLocalizedEvent .
				Org.Mockito.Mockito.Verify(containerEventHandler, Org.Mockito.Mockito.Timeout(1000
					).Times(1)).Handle(Matchers.IsA<ContainerResourceLocalizedEvent>());
				NUnit.Framework.Assert.AreEqual(ResourceState.Localized, localrsrc[lr].GetState()
					);
				NUnit.Framework.Assert.AreEqual(1, localrsrc[lr].GetRefCount());
				// Container-3 releasing the resource.
				ResourceReleaseEvent relEvent3 = new ResourceReleaseEvent(lr, cId3);
				tracker.Handle(relEvent3);
				dispatcher.Await();
				NUnit.Framework.Assert.AreEqual(0, localrsrc[lr].GetRefCount());
			}
			finally
			{
				if (dispatcher != null)
				{
					dispatcher.Stop();
				}
			}
		}

		public virtual void TestHierarchicalLocalCacheDirectories()
		{
			string user = "testuser";
			DrainDispatcher dispatcher = null;
			try
			{
				Configuration conf = new Configuration();
				// setting per directory file limit to 1.
				conf.Set(YarnConfiguration.NmLocalCacheMaxFilesPerDirectory, "37");
				dispatcher = CreateDispatcher(conf);
				EventHandler<LocalizerEvent> localizerEventHandler = Org.Mockito.Mockito.Mock<EventHandler
					>();
				EventHandler<LocalizerEvent> containerEventHandler = Org.Mockito.Mockito.Mock<EventHandler
					>();
				dispatcher.Register(typeof(LocalizerEventType), localizerEventHandler);
				dispatcher.Register(typeof(ContainerEventType), containerEventHandler);
				DeletionService mockDelService = Org.Mockito.Mockito.Mock<DeletionService>();
				ConcurrentMap<LocalResourceRequest, LocalizedResource> localrsrc = new ConcurrentHashMap
					<LocalResourceRequest, LocalizedResource>();
				LocalResourcesTracker tracker = new LocalResourcesTrackerImpl(user, null, dispatcher
					, localrsrc, true, conf, new NMNullStateStoreService());
				// This is a random path. NO File creation will take place at this place.
				Path localDir = new Path("/tmp");
				// Container 1 needs lr1 resource
				ContainerId cId1 = BuilderUtils.NewContainerId(1, 1, 1, 1);
				LocalResourceRequest lr1 = CreateLocalResourceRequest(user, 1, 1, LocalResourceVisibility
					.Public);
				LocalizerContext lc1 = new LocalizerContext(user, cId1, null);
				// Container 1 requests lr1 to be localized
				ResourceEvent reqEvent1 = new ResourceRequestEvent(lr1, LocalResourceVisibility.Public
					, lc1);
				tracker.Handle(reqEvent1);
				// Simulate the process of localization of lr1
				// NOTE: Localization path from tracker has resource ID at end
				Path hierarchicalPath1 = tracker.GetPathForLocalization(lr1, localDir, null).GetParent
					();
				// Simulate lr1 getting localized
				ResourceLocalizedEvent rle1 = new ResourceLocalizedEvent(lr1, new Path(hierarchicalPath1
					.ToUri().ToString() + Path.Separator + "file1"), 120);
				tracker.Handle(rle1);
				// Localization successful.
				LocalResourceRequest lr2 = CreateLocalResourceRequest(user, 3, 3, LocalResourceVisibility
					.Public);
				// Container 1 requests lr2 to be localized.
				ResourceEvent reqEvent2 = new ResourceRequestEvent(lr2, LocalResourceVisibility.Public
					, lc1);
				tracker.Handle(reqEvent2);
				Path hierarchicalPath2 = tracker.GetPathForLocalization(lr2, localDir, null).GetParent
					();
				// localization failed.
				ResourceFailedLocalizationEvent rfe2 = new ResourceFailedLocalizationEvent(lr2, new 
					Exception("Test").ToString());
				tracker.Handle(rfe2);
				/*
				* The path returned for two localization should be different because we
				* are limiting one file per sub-directory.
				*/
				NUnit.Framework.Assert.AreNotSame(hierarchicalPath1, hierarchicalPath2);
				LocalResourceRequest lr3 = CreateLocalResourceRequest(user, 2, 2, LocalResourceVisibility
					.Public);
				ResourceEvent reqEvent3 = new ResourceRequestEvent(lr3, LocalResourceVisibility.Public
					, lc1);
				tracker.Handle(reqEvent3);
				Path hierarchicalPath3 = tracker.GetPathForLocalization(lr3, localDir, null).GetParent
					();
				// localization successful
				ResourceLocalizedEvent rle3 = new ResourceLocalizedEvent(lr3, new Path(hierarchicalPath3
					.ToUri().ToString() + Path.Separator + "file3"), 120);
				tracker.Handle(rle3);
				// Verifying that path created is inside the subdirectory
				NUnit.Framework.Assert.AreEqual(hierarchicalPath3.ToUri().ToString(), hierarchicalPath1
					.ToUri().ToString() + Path.Separator + "0");
				// Container 1 releases resource lr1
				ResourceEvent relEvent1 = new ResourceReleaseEvent(lr1, cId1);
				tracker.Handle(relEvent1);
				// Validate the file counts now
				int resources = 0;
				IEnumerator<LocalizedResource> iter = tracker.GetEnumerator();
				while (iter.HasNext())
				{
					iter.Next();
					resources++;
				}
				// There should be only two resources lr1 and lr3 now.
				NUnit.Framework.Assert.AreEqual(2, resources);
				// Now simulate cache cleanup - removes unused resources.
				iter = tracker.GetEnumerator();
				while (iter.HasNext())
				{
					LocalizedResource rsrc = iter.Next();
					if (rsrc.GetRefCount() == 0)
					{
						NUnit.Framework.Assert.IsTrue(tracker.Remove(rsrc, mockDelService));
						resources--;
					}
				}
				// lr1 is not used by anyone and will be removed, only lr3 will hang
				// around
				NUnit.Framework.Assert.AreEqual(1, resources);
			}
			finally
			{
				if (dispatcher != null)
				{
					dispatcher.Stop();
				}
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestStateStoreSuccessfulLocalization()
		{
			string user = "someuser";
			ApplicationId appId = ApplicationId.NewInstance(1, 1);
			// This is a random path. NO File creation will take place at this place.
			Path localDir = new Path("/tmp");
			Configuration conf = new YarnConfiguration();
			DrainDispatcher dispatcher = null;
			dispatcher = CreateDispatcher(conf);
			EventHandler<LocalizerEvent> localizerEventHandler = Org.Mockito.Mockito.Mock<EventHandler
				>();
			EventHandler<LocalizerEvent> containerEventHandler = Org.Mockito.Mockito.Mock<EventHandler
				>();
			dispatcher.Register(typeof(LocalizerEventType), localizerEventHandler);
			dispatcher.Register(typeof(ContainerEventType), containerEventHandler);
			DeletionService mockDelService = Org.Mockito.Mockito.Mock<DeletionService>();
			NMStateStoreService stateStore = Org.Mockito.Mockito.Mock<NMStateStoreService>();
			try
			{
				LocalResourcesTracker tracker = new LocalResourcesTrackerImpl(user, appId, dispatcher
					, false, conf, stateStore);
				// Container 1 needs lr1 resource
				ContainerId cId1 = BuilderUtils.NewContainerId(1, 1, 1, 1);
				LocalResourceRequest lr1 = CreateLocalResourceRequest(user, 1, 1, LocalResourceVisibility
					.Application);
				LocalizerContext lc1 = new LocalizerContext(user, cId1, null);
				// Container 1 requests lr1 to be localized
				ResourceEvent reqEvent1 = new ResourceRequestEvent(lr1, LocalResourceVisibility.Application
					, lc1);
				tracker.Handle(reqEvent1);
				dispatcher.Await();
				// Simulate the process of localization of lr1
				Path hierarchicalPath1 = tracker.GetPathForLocalization(lr1, localDir, null);
				ArgumentCaptor<YarnProtos.LocalResourceProto> localResourceCaptor = ArgumentCaptor
					.ForClass<YarnProtos.LocalResourceProto>();
				ArgumentCaptor<Path> pathCaptor = ArgumentCaptor.ForClass<Path>();
				Org.Mockito.Mockito.Verify(stateStore).StartResourceLocalization(Matchers.Eq(user
					), Matchers.Eq(appId), localResourceCaptor.Capture(), pathCaptor.Capture());
				YarnProtos.LocalResourceProto lrProto = localResourceCaptor.GetValue();
				Path localizedPath1 = pathCaptor.GetValue();
				NUnit.Framework.Assert.AreEqual(lr1, new LocalResourceRequest(new LocalResourcePBImpl
					(lrProto)));
				NUnit.Framework.Assert.AreEqual(hierarchicalPath1, localizedPath1.GetParent());
				// Simulate lr1 getting localized
				ResourceLocalizedEvent rle1 = new ResourceLocalizedEvent(lr1, pathCaptor.GetValue
					(), 120);
				tracker.Handle(rle1);
				dispatcher.Await();
				ArgumentCaptor<YarnServerNodemanagerRecoveryProtos.LocalizedResourceProto> localizedProtoCaptor
					 = ArgumentCaptor.ForClass<YarnServerNodemanagerRecoveryProtos.LocalizedResourceProto
					>();
				Org.Mockito.Mockito.Verify(stateStore).FinishResourceLocalization(Matchers.Eq(user
					), Matchers.Eq(appId), localizedProtoCaptor.Capture());
				YarnServerNodemanagerRecoveryProtos.LocalizedResourceProto localizedProto = localizedProtoCaptor
					.GetValue();
				NUnit.Framework.Assert.AreEqual(lr1, new LocalResourceRequest(new LocalResourcePBImpl
					(localizedProto.GetResource())));
				NUnit.Framework.Assert.AreEqual(localizedPath1.ToString(), localizedProto.GetLocalPath
					());
				LocalizedResource localizedRsrc1 = tracker.GetLocalizedResource(lr1);
				NUnit.Framework.Assert.IsNotNull(localizedRsrc1);
				// simulate release and retention processing
				tracker.Handle(new ResourceReleaseEvent(lr1, cId1));
				dispatcher.Await();
				bool removeResult = tracker.Remove(localizedRsrc1, mockDelService);
				NUnit.Framework.Assert.IsTrue(removeResult);
				Org.Mockito.Mockito.Verify(stateStore).RemoveLocalizedResource(Matchers.Eq(user), 
					Matchers.Eq(appId), Matchers.Eq(localizedPath1));
			}
			finally
			{
				if (dispatcher != null)
				{
					dispatcher.Stop();
				}
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestStateStoreFailedLocalization()
		{
			string user = "someuser";
			ApplicationId appId = ApplicationId.NewInstance(1, 1);
			// This is a random path. NO File creation will take place at this place.
			Path localDir = new Path("/tmp");
			Configuration conf = new YarnConfiguration();
			DrainDispatcher dispatcher = null;
			dispatcher = CreateDispatcher(conf);
			EventHandler<LocalizerEvent> localizerEventHandler = Org.Mockito.Mockito.Mock<EventHandler
				>();
			EventHandler<LocalizerEvent> containerEventHandler = Org.Mockito.Mockito.Mock<EventHandler
				>();
			dispatcher.Register(typeof(LocalizerEventType), localizerEventHandler);
			dispatcher.Register(typeof(ContainerEventType), containerEventHandler);
			NMStateStoreService stateStore = Org.Mockito.Mockito.Mock<NMStateStoreService>();
			try
			{
				LocalResourcesTracker tracker = new LocalResourcesTrackerImpl(user, appId, dispatcher
					, false, conf, stateStore);
				// Container 1 needs lr1 resource
				ContainerId cId1 = BuilderUtils.NewContainerId(1, 1, 1, 1);
				LocalResourceRequest lr1 = CreateLocalResourceRequest(user, 1, 1, LocalResourceVisibility
					.Application);
				LocalizerContext lc1 = new LocalizerContext(user, cId1, null);
				// Container 1 requests lr1 to be localized
				ResourceEvent reqEvent1 = new ResourceRequestEvent(lr1, LocalResourceVisibility.Application
					, lc1);
				tracker.Handle(reqEvent1);
				dispatcher.Await();
				// Simulate the process of localization of lr1
				Path hierarchicalPath1 = tracker.GetPathForLocalization(lr1, localDir, null);
				ArgumentCaptor<YarnProtos.LocalResourceProto> localResourceCaptor = ArgumentCaptor
					.ForClass<YarnProtos.LocalResourceProto>();
				ArgumentCaptor<Path> pathCaptor = ArgumentCaptor.ForClass<Path>();
				Org.Mockito.Mockito.Verify(stateStore).StartResourceLocalization(Matchers.Eq(user
					), Matchers.Eq(appId), localResourceCaptor.Capture(), pathCaptor.Capture());
				YarnProtos.LocalResourceProto lrProto = localResourceCaptor.GetValue();
				Path localizedPath1 = pathCaptor.GetValue();
				NUnit.Framework.Assert.AreEqual(lr1, new LocalResourceRequest(new LocalResourcePBImpl
					(lrProto)));
				NUnit.Framework.Assert.AreEqual(hierarchicalPath1, localizedPath1.GetParent());
				ResourceFailedLocalizationEvent rfe1 = new ResourceFailedLocalizationEvent(lr1, new 
					Exception("Test").ToString());
				tracker.Handle(rfe1);
				dispatcher.Await();
				Org.Mockito.Mockito.Verify(stateStore).RemoveLocalizedResource(Matchers.Eq(user), 
					Matchers.Eq(appId), Matchers.Eq(localizedPath1));
			}
			finally
			{
				if (dispatcher != null)
				{
					dispatcher.Stop();
				}
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRecoveredResource()
		{
			string user = "someuser";
			ApplicationId appId = ApplicationId.NewInstance(1, 1);
			// This is a random path. NO File creation will take place at this place.
			Path localDir = new Path("/tmp/localdir");
			Configuration conf = new YarnConfiguration();
			DrainDispatcher dispatcher = null;
			dispatcher = CreateDispatcher(conf);
			EventHandler<LocalizerEvent> localizerEventHandler = Org.Mockito.Mockito.Mock<EventHandler
				>();
			EventHandler<LocalizerEvent> containerEventHandler = Org.Mockito.Mockito.Mock<EventHandler
				>();
			dispatcher.Register(typeof(LocalizerEventType), localizerEventHandler);
			dispatcher.Register(typeof(ContainerEventType), containerEventHandler);
			NMStateStoreService stateStore = Org.Mockito.Mockito.Mock<NMStateStoreService>();
			try
			{
				LocalResourcesTracker tracker = new LocalResourcesTrackerImpl(user, appId, dispatcher
					, false, conf, stateStore);
				// Container 1 needs lr1 resource
				ContainerId cId1 = BuilderUtils.NewContainerId(1, 1, 1, 1);
				LocalResourceRequest lr1 = CreateLocalResourceRequest(user, 1, 1, LocalResourceVisibility
					.Application);
				NUnit.Framework.Assert.IsNull(tracker.GetLocalizedResource(lr1));
				long localizedId1 = 52;
				Path hierarchicalPath1 = new Path(localDir, System.Convert.ToString(localizedId1)
					);
				Path localizedPath1 = new Path(hierarchicalPath1, "resource.jar");
				tracker.Handle(new ResourceRecoveredEvent(lr1, localizedPath1, 120));
				dispatcher.Await();
				NUnit.Framework.Assert.IsNotNull(tracker.GetLocalizedResource(lr1));
				// verify new paths reflect recovery of previous resources
				LocalResourceRequest lr2 = CreateLocalResourceRequest(user, 2, 2, LocalResourceVisibility
					.Application);
				LocalizerContext lc2 = new LocalizerContext(user, cId1, null);
				ResourceEvent reqEvent2 = new ResourceRequestEvent(lr2, LocalResourceVisibility.Application
					, lc2);
				tracker.Handle(reqEvent2);
				dispatcher.Await();
				Path hierarchicalPath2 = tracker.GetPathForLocalization(lr2, localDir, null);
				long localizedId2 = long.Parse(hierarchicalPath2.GetName());
				NUnit.Framework.Assert.AreEqual(localizedId1 + 1, localizedId2);
			}
			finally
			{
				if (dispatcher != null)
				{
					dispatcher.Stop();
				}
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRecoveredResourceWithDirCacheMgr()
		{
			string user = "someuser";
			ApplicationId appId = ApplicationId.NewInstance(1, 1);
			// This is a random path. NO File creation will take place at this place.
			Path localDirRoot = new Path("/tmp/localdir");
			Configuration conf = new YarnConfiguration();
			DrainDispatcher dispatcher = null;
			dispatcher = CreateDispatcher(conf);
			EventHandler<LocalizerEvent> localizerEventHandler = Org.Mockito.Mockito.Mock<EventHandler
				>();
			EventHandler<LocalizerEvent> containerEventHandler = Org.Mockito.Mockito.Mock<EventHandler
				>();
			dispatcher.Register(typeof(LocalizerEventType), localizerEventHandler);
			dispatcher.Register(typeof(ContainerEventType), containerEventHandler);
			NMStateStoreService stateStore = Org.Mockito.Mockito.Mock<NMStateStoreService>();
			try
			{
				LocalResourcesTrackerImpl tracker = new LocalResourcesTrackerImpl(user, appId, dispatcher
					, true, conf, stateStore);
				LocalResourceRequest lr1 = CreateLocalResourceRequest(user, 1, 1, LocalResourceVisibility
					.Public);
				NUnit.Framework.Assert.IsNull(tracker.GetLocalizedResource(lr1));
				long localizedId1 = 52;
				Path hierarchicalPath1 = new Path(localDirRoot + "/4/2", System.Convert.ToString(
					localizedId1));
				Path localizedPath1 = new Path(hierarchicalPath1, "resource.jar");
				tracker.Handle(new ResourceRecoveredEvent(lr1, localizedPath1, 120));
				dispatcher.Await();
				NUnit.Framework.Assert.IsNotNull(tracker.GetLocalizedResource(lr1));
				LocalCacheDirectoryManager dirMgrRoot = tracker.GetDirectoryManager(localDirRoot);
				NUnit.Framework.Assert.AreEqual(0, dirMgrRoot.GetDirectory(string.Empty).GetCount
					());
				NUnit.Framework.Assert.AreEqual(1, dirMgrRoot.GetDirectory("4/2").GetCount());
				LocalResourceRequest lr2 = CreateLocalResourceRequest(user, 2, 2, LocalResourceVisibility
					.Public);
				NUnit.Framework.Assert.IsNull(tracker.GetLocalizedResource(lr2));
				long localizedId2 = localizedId1 + 1;
				Path hierarchicalPath2 = new Path(localDirRoot + "/4/2", System.Convert.ToString(
					localizedId2));
				Path localizedPath2 = new Path(hierarchicalPath2, "resource.jar");
				tracker.Handle(new ResourceRecoveredEvent(lr2, localizedPath2, 120));
				dispatcher.Await();
				NUnit.Framework.Assert.IsNotNull(tracker.GetLocalizedResource(lr2));
				NUnit.Framework.Assert.AreEqual(0, dirMgrRoot.GetDirectory(string.Empty).GetCount
					());
				NUnit.Framework.Assert.AreEqual(2, dirMgrRoot.GetDirectory("4/2").GetCount());
				LocalResourceRequest lr3 = CreateLocalResourceRequest(user, 3, 3, LocalResourceVisibility
					.Public);
				NUnit.Framework.Assert.IsNull(tracker.GetLocalizedResource(lr3));
				long localizedId3 = 128;
				Path hierarchicalPath3 = new Path(localDirRoot + "/4/3", System.Convert.ToString(
					localizedId3));
				Path localizedPath3 = new Path(hierarchicalPath3, "resource.jar");
				tracker.Handle(new ResourceRecoveredEvent(lr3, localizedPath3, 120));
				dispatcher.Await();
				NUnit.Framework.Assert.IsNotNull(tracker.GetLocalizedResource(lr3));
				NUnit.Framework.Assert.AreEqual(0, dirMgrRoot.GetDirectory(string.Empty).GetCount
					());
				NUnit.Framework.Assert.AreEqual(2, dirMgrRoot.GetDirectory("4/2").GetCount());
				NUnit.Framework.Assert.AreEqual(1, dirMgrRoot.GetDirectory("4/3").GetCount());
				LocalResourceRequest lr4 = CreateLocalResourceRequest(user, 4, 4, LocalResourceVisibility
					.Public);
				NUnit.Framework.Assert.IsNull(tracker.GetLocalizedResource(lr4));
				long localizedId4 = 256;
				Path hierarchicalPath4 = new Path(localDirRoot + "/4", System.Convert.ToString(localizedId4
					));
				Path localizedPath4 = new Path(hierarchicalPath4, "resource.jar");
				tracker.Handle(new ResourceRecoveredEvent(lr4, localizedPath4, 120));
				dispatcher.Await();
				NUnit.Framework.Assert.IsNotNull(tracker.GetLocalizedResource(lr4));
				NUnit.Framework.Assert.AreEqual(0, dirMgrRoot.GetDirectory(string.Empty).GetCount
					());
				NUnit.Framework.Assert.AreEqual(1, dirMgrRoot.GetDirectory("4").GetCount());
				NUnit.Framework.Assert.AreEqual(2, dirMgrRoot.GetDirectory("4/2").GetCount());
				NUnit.Framework.Assert.AreEqual(1, dirMgrRoot.GetDirectory("4/3").GetCount());
			}
			finally
			{
				if (dispatcher != null)
				{
					dispatcher.Stop();
				}
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGetPathForLocalization()
		{
			FileContext lfs = FileContext.GetLocalFSFileContext();
			Path base_path = new Path("target", typeof(TestLocalResourcesTrackerImpl).Name);
			string user = "someuser";
			ApplicationId appId = ApplicationId.NewInstance(1, 1);
			Configuration conf = new YarnConfiguration();
			DrainDispatcher dispatcher = null;
			dispatcher = CreateDispatcher(conf);
			EventHandler<LocalizerEvent> localizerEventHandler = Org.Mockito.Mockito.Mock<EventHandler
				>();
			EventHandler<LocalizerEvent> containerEventHandler = Org.Mockito.Mockito.Mock<EventHandler
				>();
			dispatcher.Register(typeof(LocalizerEventType), localizerEventHandler);
			dispatcher.Register(typeof(ContainerEventType), containerEventHandler);
			NMStateStoreService stateStore = Org.Mockito.Mockito.Mock<NMStateStoreService>();
			DeletionService delService = Org.Mockito.Mockito.Mock<DeletionService>();
			try
			{
				LocalResourceRequest req1 = CreateLocalResourceRequest(user, 1, 1, LocalResourceVisibility
					.Public);
				LocalizedResource lr1 = CreateLocalizedResource(req1, dispatcher);
				ConcurrentMap<LocalResourceRequest, LocalizedResource> localrsrc = new ConcurrentHashMap
					<LocalResourceRequest, LocalizedResource>();
				localrsrc[req1] = lr1;
				LocalResourcesTrackerImpl tracker = new LocalResourcesTrackerImpl(user, appId, dispatcher
					, localrsrc, true, conf, stateStore);
				Path conflictPath = new Path(base_path, "10");
				Path qualifiedConflictPath = lfs.MakeQualified(conflictPath);
				lfs.Mkdir(qualifiedConflictPath, null, true);
				Path rPath = tracker.GetPathForLocalization(req1, base_path, delService);
				NUnit.Framework.Assert.IsFalse(lfs.Util().Exists(rPath));
				Org.Mockito.Mockito.Verify(delService, Org.Mockito.Mockito.Times(1)).Delete(Matchers.Eq
					(user), Matchers.Eq(conflictPath));
			}
			finally
			{
				lfs.Delete(base_path, true);
				if (dispatcher != null)
				{
					dispatcher.Stop();
				}
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestReleaseWhileDownloading()
		{
			string user = "testuser";
			DrainDispatcher dispatcher = null;
			try
			{
				Configuration conf = new Configuration();
				dispatcher = CreateDispatcher(conf);
				EventHandler<LocalizerEvent> localizerEventHandler = Org.Mockito.Mockito.Mock<EventHandler
					>();
				EventHandler<LocalizerEvent> containerEventHandler = Org.Mockito.Mockito.Mock<EventHandler
					>();
				dispatcher.Register(typeof(LocalizerEventType), localizerEventHandler);
				dispatcher.Register(typeof(ContainerEventType), containerEventHandler);
				ContainerId cId = BuilderUtils.NewContainerId(1, 1, 1, 1);
				LocalizerContext lc = new LocalizerContext(user, cId, null);
				LocalResourceRequest req = CreateLocalResourceRequest(user, 1, 1, LocalResourceVisibility
					.Public);
				LocalizedResource lr = CreateLocalizedResource(req, dispatcher);
				ConcurrentMap<LocalResourceRequest, LocalizedResource> localrsrc = new ConcurrentHashMap
					<LocalResourceRequest, LocalizedResource>();
				localrsrc[req] = lr;
				LocalResourcesTracker tracker = new LocalResourcesTrackerImpl(user, null, dispatcher
					, localrsrc, false, conf, new NMNullStateStoreService());
				// request the resource
				ResourceEvent reqEvent = new ResourceRequestEvent(req, LocalResourceVisibility.Public
					, lc);
				tracker.Handle(reqEvent);
				// release the resource
				ResourceEvent relEvent = new ResourceReleaseEvent(req, cId);
				tracker.Handle(relEvent);
				// download completing after release
				ResourceLocalizedEvent rle = new ResourceLocalizedEvent(req, new Path("file:///tmp/r1"
					), 1);
				tracker.Handle(rle);
				dispatcher.Await();
			}
			finally
			{
				if (dispatcher != null)
				{
					dispatcher.Stop();
				}
			}
		}

		private bool Createdummylocalizefile(Path path)
		{
			bool ret = false;
			FilePath file = new FilePath(path.ToUri().GetRawPath().ToString());
			try
			{
				ret = file.CreateNewFile();
			}
			catch (IOException e)
			{
				Sharpen.Runtime.PrintStackTrace(e);
			}
			return ret;
		}

		private void VerifyTrackedResourceCount(LocalResourcesTracker tracker, int expected
			)
		{
			int count = 0;
			IEnumerator<LocalizedResource> iter = tracker.GetEnumerator();
			while (iter.HasNext())
			{
				iter.Next();
				count++;
			}
			NUnit.Framework.Assert.AreEqual("Tracker resource count does not match", expected
				, count);
		}

		private LocalResourceRequest CreateLocalResourceRequest(string user, int i, long 
			ts, LocalResourceVisibility vis)
		{
			LocalResourceRequest req = new LocalResourceRequest(new Path("file:///tmp/" + user
				 + "/rsrc" + i), ts + i * 2000, LocalResourceType.File, vis, null);
			return req;
		}

		private LocalizedResource CreateLocalizedResource(LocalResourceRequest req, Dispatcher
			 dispatcher)
		{
			LocalizedResource lr = new LocalizedResource(req, dispatcher);
			return lr;
		}

		private DrainDispatcher CreateDispatcher(Configuration conf)
		{
			DrainDispatcher dispatcher = new DrainDispatcher();
			dispatcher.Init(conf);
			dispatcher.Start();
			return dispatcher;
		}
	}
}
