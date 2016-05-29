using System.Collections.Generic;
using System.Net;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Factories;
using Org.Apache.Hadoop.Yarn.Factory.Providers;
using Org.Apache.Hadoop.Yarn.Ipc;
using Org.Apache.Hadoop.Yarn.Server.Api;
using Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Server.Sharedcachemanager.Metrics;
using Org.Apache.Hadoop.Yarn.Server.Sharedcachemanager.Store;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Sharedcachemanager
{
	/// <summary>Basic unit tests for the NodeManger to SCM Protocol Service.</summary>
	public class TestSharedCacheUploaderService
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

		private SharedCacheUploaderService service;

		private SCMUploaderProtocol proxy;

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
			service = new SharedCacheUploaderService(store);
			service.Init(conf);
			service.Start();
			YarnRPC rpc = YarnRPC.Create(new Configuration());
			IPEndPoint scmAddress = conf.GetSocketAddr(YarnConfiguration.ScmUploaderServerAddress
				, YarnConfiguration.DefaultScmUploaderServerAddress, YarnConfiguration.DefaultScmUploaderServerPort
				);
			proxy = (SCMUploaderProtocol)rpc.GetProxy(typeof(SCMUploaderProtocol), scmAddress
				, conf);
		}

		[TearDown]
		public virtual void CleanUp()
		{
			if (store != null)
			{
				store.Stop();
			}
			if (service != null)
			{
				service.Stop();
			}
			if (proxy != null)
			{
				RPC.StopProxy(proxy);
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNotify_noEntry()
		{
			long accepted = SharedCacheUploaderMetrics.GetInstance().GetAcceptedUploads();
			SCMUploaderNotifyRequest request = recordFactory.NewRecordInstance<SCMUploaderNotifyRequest
				>();
			request.SetResourceKey("key1");
			request.SetFilename("foo.jar");
			NUnit.Framework.Assert.IsTrue(proxy.Notify(request).GetAccepted());
			ICollection<SharedCacheResourceReference> set = store.GetResourceReferences("key1"
				);
			NUnit.Framework.Assert.IsNotNull(set);
			NUnit.Framework.Assert.AreEqual(0, set.Count);
			NUnit.Framework.Assert.AreEqual("NM upload metrics aren't updated.", 1, SharedCacheUploaderMetrics
				.GetInstance().GetAcceptedUploads() - accepted);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNotify_entryExists_differentName()
		{
			long rejected = SharedCacheUploaderMetrics.GetInstance().GetRejectUploads();
			store.AddResource("key1", "foo.jar");
			SCMUploaderNotifyRequest request = recordFactory.NewRecordInstance<SCMUploaderNotifyRequest
				>();
			request.SetResourceKey("key1");
			request.SetFilename("foobar.jar");
			NUnit.Framework.Assert.IsFalse(proxy.Notify(request).GetAccepted());
			ICollection<SharedCacheResourceReference> set = store.GetResourceReferences("key1"
				);
			NUnit.Framework.Assert.IsNotNull(set);
			NUnit.Framework.Assert.AreEqual(0, set.Count);
			NUnit.Framework.Assert.AreEqual("NM upload metrics aren't updated.", 1, SharedCacheUploaderMetrics
				.GetInstance().GetRejectUploads() - rejected);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNotify_entryExists_sameName()
		{
			long accepted = SharedCacheUploaderMetrics.GetInstance().GetAcceptedUploads();
			store.AddResource("key1", "foo.jar");
			SCMUploaderNotifyRequest request = recordFactory.NewRecordInstance<SCMUploaderNotifyRequest
				>();
			request.SetResourceKey("key1");
			request.SetFilename("foo.jar");
			NUnit.Framework.Assert.IsTrue(proxy.Notify(request).GetAccepted());
			ICollection<SharedCacheResourceReference> set = store.GetResourceReferences("key1"
				);
			NUnit.Framework.Assert.IsNotNull(set);
			NUnit.Framework.Assert.AreEqual(0, set.Count);
			NUnit.Framework.Assert.AreEqual("NM upload metrics aren't updated.", 1, SharedCacheUploaderMetrics
				.GetInstance().GetAcceptedUploads() - accepted);
		}
	}
}
