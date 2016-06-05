using System.Net;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Yarn.Api;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Factories;
using Org.Apache.Hadoop.Yarn.Factory.Providers;
using Org.Apache.Hadoop.Yarn.Ipc;
using Org.Apache.Hadoop.Yarn.Server.Sharedcachemanager.Metrics;
using Org.Apache.Hadoop.Yarn.Server.Sharedcachemanager.Store;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Sharedcachemanager
{
	/// <summary>Basic unit tests for the Client to SCM Protocol Service.</summary>
	public class TestClientSCMProtocolService
	{
		private static FilePath testDir = null;

		/// <exception cref="System.IO.IOException"/>
		[BeforeClass]
		public static void SetupTestDirs()
		{
			testDir = new FilePath("target", typeof(TestSharedCacheUploaderService).GetCanonicalName
				());
			testDir.Delete();
			testDir.Mkdirs();
			testDir = testDir.GetAbsoluteFile();
		}

		/// <exception cref="System.IO.IOException"/>
		[AfterClass]
		public static void CleanupTestDirs()
		{
			if (testDir != null)
			{
				testDir.Delete();
			}
		}

		private ClientProtocolService service;

		private ClientSCMProtocol clientSCMProxy;

		private SCMStore store;

		private readonly RecordFactory recordFactory = RecordFactoryProvider.GetRecordFactory
			(null);

		[SetUp]
		public virtual void StartUp()
		{
			Configuration conf = new Configuration();
			conf.Set(YarnConfiguration.ScmStoreClass, typeof(InMemorySCMStore).FullName);
			conf.Set(YarnConfiguration.SharedCacheRoot, testDir.GetPath());
			AppChecker appChecker = Org.Mockito.Mockito.Spy(new DummyAppChecker());
			store = new InMemorySCMStore(appChecker);
			store.Init(conf);
			store.Start();
			service = new ClientProtocolService(store);
			service.Init(conf);
			service.Start();
			YarnRPC rpc = YarnRPC.Create(new Configuration());
			IPEndPoint scmAddress = conf.GetSocketAddr(YarnConfiguration.ScmClientServerAddress
				, YarnConfiguration.DefaultScmClientServerAddress, YarnConfiguration.DefaultScmClientServerPort
				);
			clientSCMProxy = (ClientSCMProtocol)rpc.GetProxy(typeof(ClientSCMProtocol), scmAddress
				, conf);
		}

		[TearDown]
		public virtual void CleanUp()
		{
			if (store != null)
			{
				store.Stop();
				store = null;
			}
			if (service != null)
			{
				service.Stop();
				service = null;
			}
			if (clientSCMProxy != null)
			{
				RPC.StopProxy(clientSCMProxy);
				clientSCMProxy = null;
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestUse_MissingEntry()
		{
			long misses = ClientSCMMetrics.GetInstance().GetCacheMisses();
			UseSharedCacheResourceRequest request = recordFactory.NewRecordInstance<UseSharedCacheResourceRequest
				>();
			request.SetResourceKey("key1");
			request.SetAppId(CreateAppId(1, 1L));
			NUnit.Framework.Assert.IsNull(clientSCMProxy.Use(request).GetPath());
			NUnit.Framework.Assert.AreEqual("Client SCM metrics aren't updated.", 1, ClientSCMMetrics
				.GetInstance().GetCacheMisses() - misses);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestUse_ExistingEntry_NoAppIds()
		{
			// Pre-populate the SCM with one cache entry
			store.AddResource("key1", "foo.jar");
			long hits = ClientSCMMetrics.GetInstance().GetCacheHits();
			UseSharedCacheResourceRequest request = recordFactory.NewRecordInstance<UseSharedCacheResourceRequest
				>();
			request.SetResourceKey("key1");
			request.SetAppId(CreateAppId(2, 2L));
			// Expecting default depth of 3 and under the shared cache root dir
			string expectedPath = testDir.GetAbsolutePath() + "/k/e/y/key1/foo.jar";
			NUnit.Framework.Assert.AreEqual(expectedPath, clientSCMProxy.Use(request).GetPath
				());
			NUnit.Framework.Assert.AreEqual(1, store.GetResourceReferences("key1").Count);
			NUnit.Framework.Assert.AreEqual("Client SCM metrics aren't updated.", 1, ClientSCMMetrics
				.GetInstance().GetCacheHits() - hits);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestUse_ExistingEntry_OneId()
		{
			// Pre-populate the SCM with one cache entry
			store.AddResource("key1", "foo.jar");
			store.AddResourceReference("key1", new SharedCacheResourceReference(CreateAppId(1
				, 1L), "user"));
			NUnit.Framework.Assert.AreEqual(1, store.GetResourceReferences("key1").Count);
			long hits = ClientSCMMetrics.GetInstance().GetCacheHits();
			// Add a new distinct appId
			UseSharedCacheResourceRequest request = recordFactory.NewRecordInstance<UseSharedCacheResourceRequest
				>();
			request.SetResourceKey("key1");
			request.SetAppId(CreateAppId(2, 2L));
			// Expecting default depth of 3 under the shared cache root dir
			string expectedPath = testDir.GetAbsolutePath() + "/k/e/y/key1/foo.jar";
			NUnit.Framework.Assert.AreEqual(expectedPath, clientSCMProxy.Use(request).GetPath
				());
			NUnit.Framework.Assert.AreEqual(2, store.GetResourceReferences("key1").Count);
			NUnit.Framework.Assert.AreEqual("Client SCM metrics aren't updated.", 1, ClientSCMMetrics
				.GetInstance().GetCacheHits() - hits);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestUse_ExistingEntry_DupId()
		{
			// Pre-populate the SCM with one cache entry
			store.AddResource("key1", "foo.jar");
			UserGroupInformation testUGI = UserGroupInformation.GetCurrentUser();
			store.AddResourceReference("key1", new SharedCacheResourceReference(CreateAppId(1
				, 1L), testUGI.GetShortUserName()));
			NUnit.Framework.Assert.AreEqual(1, store.GetResourceReferences("key1").Count);
			long hits = ClientSCMMetrics.GetInstance().GetCacheHits();
			// Add a new duplicate appId
			UseSharedCacheResourceRequest request = recordFactory.NewRecordInstance<UseSharedCacheResourceRequest
				>();
			request.SetResourceKey("key1");
			request.SetAppId(CreateAppId(1, 1L));
			// Expecting default depth of 3 under the shared cache root dir
			string expectedPath = testDir.GetAbsolutePath() + "/k/e/y/key1/foo.jar";
			NUnit.Framework.Assert.AreEqual(expectedPath, clientSCMProxy.Use(request).GetPath
				());
			NUnit.Framework.Assert.AreEqual(1, store.GetResourceReferences("key1").Count);
			NUnit.Framework.Assert.AreEqual("Client SCM metrics aren't updated.", 1, ClientSCMMetrics
				.GetInstance().GetCacheHits() - hits);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRelease_ExistingEntry_NonExistantAppId()
		{
			// Pre-populate the SCM with one cache entry
			store.AddResource("key1", "foo.jar");
			store.AddResourceReference("key1", new SharedCacheResourceReference(CreateAppId(1
				, 1L), "user"));
			NUnit.Framework.Assert.AreEqual(1, store.GetResourceReferences("key1").Count);
			long releases = ClientSCMMetrics.GetInstance().GetCacheReleases();
			ReleaseSharedCacheResourceRequest request = recordFactory.NewRecordInstance<ReleaseSharedCacheResourceRequest
				>();
			request.SetResourceKey("key1");
			request.SetAppId(CreateAppId(2, 2L));
			clientSCMProxy.Release(request);
			NUnit.Framework.Assert.AreEqual(1, store.GetResourceReferences("key1").Count);
			NUnit.Framework.Assert.AreEqual("Client SCM metrics were updated when a release did not happen"
				, 0, ClientSCMMetrics.GetInstance().GetCacheReleases() - releases);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRelease_ExistingEntry_WithAppId()
		{
			// Pre-populate the SCM with one cache entry
			store.AddResource("key1", "foo.jar");
			UserGroupInformation testUGI = UserGroupInformation.GetCurrentUser();
			store.AddResourceReference("key1", new SharedCacheResourceReference(CreateAppId(1
				, 1L), testUGI.GetShortUserName()));
			NUnit.Framework.Assert.AreEqual(1, store.GetResourceReferences("key1").Count);
			long releases = ClientSCMMetrics.GetInstance().GetCacheReleases();
			ReleaseSharedCacheResourceRequest request = recordFactory.NewRecordInstance<ReleaseSharedCacheResourceRequest
				>();
			request.SetResourceKey("key1");
			request.SetAppId(CreateAppId(1, 1L));
			clientSCMProxy.Release(request);
			NUnit.Framework.Assert.AreEqual(0, store.GetResourceReferences("key1").Count);
			NUnit.Framework.Assert.AreEqual("Client SCM metrics aren't updated.", 1, ClientSCMMetrics
				.GetInstance().GetCacheReleases() - releases);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRelease_MissingEntry()
		{
			long releases = ClientSCMMetrics.GetInstance().GetCacheReleases();
			ReleaseSharedCacheResourceRequest request = recordFactory.NewRecordInstance<ReleaseSharedCacheResourceRequest
				>();
			request.SetResourceKey("key2");
			request.SetAppId(CreateAppId(2, 2L));
			clientSCMProxy.Release(request);
			NUnit.Framework.Assert.IsNotNull(store.GetResourceReferences("key2"));
			NUnit.Framework.Assert.AreEqual(0, store.GetResourceReferences("key2").Count);
			NUnit.Framework.Assert.AreEqual("Client SCM metrics were updated when a release did not happen."
				, 0, ClientSCMMetrics.GetInstance().GetCacheReleases() - releases);
		}

		private ApplicationId CreateAppId(int id, long timestamp)
		{
			return ApplicationId.NewInstance(timestamp, id);
		}
	}
}
