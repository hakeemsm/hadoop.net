using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Event;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Recovery;
using Org.Mockito;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Localizer
{
	public class TestResourceRetention
	{
		[NUnit.Framework.Test]
		public virtual void TestRsrcUnused()
		{
			DeletionService delService = Org.Mockito.Mockito.Mock<DeletionService>();
			long TargetMb = 10 << 20;
			ResourceRetentionSet rss = new ResourceRetentionSet(delService, TargetMb);
			// 3MB files @{10, 15}
			LocalResourcesTracker pubTracker = CreateMockTracker(null, 3 * 1024 * 1024, 2, 10
				, 5);
			// 1MB files @{3, 6, 9, 12}
			LocalResourcesTracker trackerA = CreateMockTracker("A", 1 * 1024 * 1024, 4, 3, 3);
			// 4MB file @{1}
			LocalResourcesTracker trackerB = CreateMockTracker("B", 4 * 1024 * 1024, 1, 10, 5
				);
			// 2MB files @{7, 9, 11}
			LocalResourcesTracker trackerC = CreateMockTracker("C", 2 * 1024 * 1024, 3, 7, 2);
			// Total cache: 20MB; verify removed at least 10MB
			rss.AddResources(pubTracker);
			rss.AddResources(trackerA);
			rss.AddResources(trackerB);
			rss.AddResources(trackerC);
			long deleted = 0L;
			ArgumentCaptor<LocalizedResource> captor = ArgumentCaptor.ForClass<LocalizedResource
				>();
			Org.Mockito.Mockito.Verify(pubTracker, Org.Mockito.Mockito.AtMost(2)).Remove(captor
				.Capture(), IsA<DeletionService>());
			Org.Mockito.Mockito.Verify(trackerA, Org.Mockito.Mockito.AtMost(4)).Remove(captor
				.Capture(), IsA<DeletionService>());
			Org.Mockito.Mockito.Verify(trackerB, Org.Mockito.Mockito.AtMost(1)).Remove(captor
				.Capture(), IsA<DeletionService>());
			Org.Mockito.Mockito.Verify(trackerC, Org.Mockito.Mockito.AtMost(3)).Remove(captor
				.Capture(), IsA<DeletionService>());
			foreach (LocalizedResource rem in captor.GetAllValues())
			{
				deleted += rem.GetSize();
			}
			NUnit.Framework.Assert.IsTrue(deleted >= 10 * 1024 * 1024);
			NUnit.Framework.Assert.IsTrue(deleted < 15 * 1024 * 1024);
		}

		internal virtual LocalResourcesTracker CreateMockTracker(string user, long rsrcSize
			, long nRsrcs, long timestamp, long tsstep)
		{
			Configuration conf = new Configuration();
			ConcurrentMap<LocalResourceRequest, LocalizedResource> trackerResources = new ConcurrentHashMap
				<LocalResourceRequest, LocalizedResource>();
			LocalResourcesTracker ret = Org.Mockito.Mockito.Spy(new LocalResourcesTrackerImpl
				(user, null, null, trackerResources, false, conf, new NMNullStateStoreService())
				);
			for (int i = 0; i < nRsrcs; ++i)
			{
				LocalResourceRequest req = new LocalResourceRequest(new Path("file:///" + user + 
					"/rsrc" + i), timestamp + i * tsstep, LocalResourceType.File, LocalResourceVisibility
					.Public, null);
				long ts = timestamp + i * tsstep;
				Path p = new Path("file:///local/" + user + "/rsrc" + i);
				LocalizedResource rsrc = new _LocalizedResource_93(rsrcSize, p, ts, req, null);
				trackerResources[req] = rsrc;
			}
			return ret;
		}

		private sealed class _LocalizedResource_93 : LocalizedResource
		{
			public _LocalizedResource_93(long rsrcSize, Path p, long ts, LocalResourceRequest
				 baseArg1, Dispatcher baseArg2)
				: base(baseArg1, baseArg2)
			{
				this.rsrcSize = rsrcSize;
				this.p = p;
				this.ts = ts;
			}

			public override int GetRefCount()
			{
				return 0;
			}

			public override long GetSize()
			{
				return rsrcSize;
			}

			public override Path GetLocalPath()
			{
				return p;
			}

			public override long GetTimestamp()
			{
				return ts;
			}

			public override ResourceState GetState()
			{
				return ResourceState.Localized;
			}

			private readonly long rsrcSize;

			private readonly Path p;

			private readonly long ts;
		}
	}
}
