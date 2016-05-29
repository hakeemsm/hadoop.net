using System.Net;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Yarn.Client;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Factories;
using Org.Apache.Hadoop.Yarn.Factory.Providers;
using Org.Apache.Hadoop.Yarn.Ipc;
using Org.Apache.Hadoop.Yarn.Server.Api;
using Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords.Impl.PB;
using Org.Apache.Hadoop.Yarn.Server.Sharedcachemanager.Store;
using Org.Mockito;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Sharedcachemanager
{
	/// <summary>Basic unit tests for the SCM Admin Protocol Service and SCMAdmin.</summary>
	public class TestSCMAdminProtocolService
	{
		internal static SCMAdminProtocolService service;

		internal static SCMAdminProtocol SCMAdminProxy;

		internal static SCMAdminProtocol mockAdmin;

		internal static SCMAdmin adminCLI;

		internal static SCMStore store;

		internal static CleanerService cleaner;

		private readonly RecordFactory recordFactory = RecordFactoryProvider.GetRecordFactory
			(null);

		[SetUp]
		public virtual void StartUp()
		{
			Configuration conf = new Configuration();
			conf.Set(YarnConfiguration.ScmStoreClass, typeof(InMemorySCMStore).FullName);
			cleaner = Org.Mockito.Mockito.Mock<CleanerService>();
			service = Org.Mockito.Mockito.Spy(new SCMAdminProtocolService(cleaner));
			service.Init(conf);
			service.Start();
			YarnRPC rpc = YarnRPC.Create(new Configuration());
			IPEndPoint scmAddress = conf.GetSocketAddr(YarnConfiguration.ScmAdminAddress, YarnConfiguration
				.DefaultScmAdminAddress, YarnConfiguration.DefaultScmAdminPort);
			SCMAdminProxy = (SCMAdminProtocol)rpc.GetProxy(typeof(SCMAdminProtocol), scmAddress
				, conf);
			mockAdmin = Org.Mockito.Mockito.Mock<SCMAdminProtocol>();
			adminCLI = new _SCMAdmin_90(new Configuration());
		}

		private sealed class _SCMAdmin_90 : SCMAdmin
		{
			public _SCMAdmin_90(Configuration baseArg1)
				: base(baseArg1)
			{
			}

			/// <exception cref="System.IO.IOException"/>
			protected override SCMAdminProtocol CreateSCMAdminProtocol()
			{
				return TestSCMAdminProtocolService.mockAdmin;
			}
		}

		[TearDown]
		public virtual void CleanUpTest()
		{
			if (service != null)
			{
				service.Stop();
			}
			if (SCMAdminProxy != null)
			{
				RPC.StopProxy(SCMAdminProxy);
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRunCleanerTask()
		{
			Org.Mockito.Mockito.DoNothing().When(cleaner).RunCleanerTask();
			RunSharedCacheCleanerTaskRequest request = recordFactory.NewRecordInstance<RunSharedCacheCleanerTaskRequest
				>();
			RunSharedCacheCleanerTaskResponse response = SCMAdminProxy.RunCleanerTask(request
				);
			NUnit.Framework.Assert.IsTrue("cleaner task request isn't accepted", response.GetAccepted
				());
			Org.Mockito.Mockito.Verify(service, Org.Mockito.Mockito.Times(1)).RunCleanerTask(
				Matchers.Any<RunSharedCacheCleanerTaskRequest>());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRunCleanerTaskCLI()
		{
			string[] args = new string[] { "-runCleanerTask" };
			RunSharedCacheCleanerTaskResponse rp = new RunSharedCacheCleanerTaskResponsePBImpl
				();
			rp.SetAccepted(true);
			Org.Mockito.Mockito.When(mockAdmin.RunCleanerTask(Matchers.IsA<RunSharedCacheCleanerTaskRequest
				>())).ThenReturn(rp);
			NUnit.Framework.Assert.AreEqual(0, adminCLI.Run(args));
			rp.SetAccepted(false);
			Org.Mockito.Mockito.When(mockAdmin.RunCleanerTask(Matchers.IsA<RunSharedCacheCleanerTaskRequest
				>())).ThenReturn(rp);
			NUnit.Framework.Assert.AreEqual(1, adminCLI.Run(args));
			Org.Mockito.Mockito.Verify(mockAdmin, Org.Mockito.Mockito.Times(2)).RunCleanerTask
				(Matchers.Any<RunSharedCacheCleanerTaskRequest>());
		}
	}
}
