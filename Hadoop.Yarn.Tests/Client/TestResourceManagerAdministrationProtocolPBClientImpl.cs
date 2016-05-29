using System.IO;
using System.Net;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Service;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Factories;
using Org.Apache.Hadoop.Yarn.Factory.Providers;
using Org.Apache.Hadoop.Yarn.Server.Api;
using Org.Apache.Hadoop.Yarn.Server.Api.Impl.PB.Client;
using Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Client
{
	/// <summary>Test ResourceManagerAdministrationProtocolPBClientImpl.</summary>
	/// <remarks>Test ResourceManagerAdministrationProtocolPBClientImpl. Test a methods and the proxy without  logic.
	/// 	</remarks>
	public class TestResourceManagerAdministrationProtocolPBClientImpl
	{
		private static ResourceManager resourceManager;

		private static readonly Log Log = LogFactory.GetLog(typeof(TestResourceManagerAdministrationProtocolPBClientImpl
			));

		private readonly RecordFactory recordFactory = RecordFactoryProvider.GetRecordFactory
			(null);

		private static ResourceManagerAdministrationProtocol client;

		/// <summary>Start resource manager server</summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		[BeforeClass]
		public static void SetUpResourceManager()
		{
			Configuration.AddDefaultResource("config-with-security.xml");
			Configuration configuration = new YarnConfiguration();
			resourceManager = new _ResourceManager_74();
			resourceManager.Init(configuration);
			new _Thread_80().Start();
			int waitCount = 0;
			while (resourceManager.GetServiceState() == Service.STATE.Inited && waitCount++ <
				 10)
			{
				Log.Info("Waiting for RM to start...");
				Sharpen.Thread.Sleep(1000);
			}
			if (resourceManager.GetServiceState() != Service.STATE.Started)
			{
				throw new IOException("ResourceManager failed to start. Final state is " + resourceManager
					.GetServiceState());
			}
			Log.Info("ResourceManager RMAdmin address: " + configuration.Get(YarnConfiguration
				.RmAdminAddress));
			client = new ResourceManagerAdministrationProtocolPBClientImpl(1L, GetProtocolAddress
				(configuration), configuration);
		}

		private sealed class _ResourceManager_74 : ResourceManager
		{
			public _ResourceManager_74()
			{
			}

			/// <exception cref="System.IO.IOException"/>
			protected override void DoSecureLogin()
			{
			}
		}

		private sealed class _Thread_80 : Sharpen.Thread
		{
			public _Thread_80()
			{
			}

			public override void Run()
			{
				TestResourceManagerAdministrationProtocolPBClientImpl.resourceManager.Start();
			}
		}

		/// <summary>Test method refreshQueues.</summary>
		/// <remarks>Test method refreshQueues. This method is present and it works.</remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRefreshQueues()
		{
			RefreshQueuesRequest request = recordFactory.NewRecordInstance<RefreshQueuesRequest
				>();
			RefreshQueuesResponse response = client.RefreshQueues(request);
			NUnit.Framework.Assert.IsNotNull(response);
		}

		/// <summary>Test method refreshNodes.</summary>
		/// <remarks>Test method refreshNodes. This method is present and it works.</remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRefreshNodes()
		{
			resourceManager.GetClientRMService();
			RefreshNodesRequest request = recordFactory.NewRecordInstance<RefreshNodesRequest
				>();
			RefreshNodesResponse response = client.RefreshNodes(request);
			NUnit.Framework.Assert.IsNotNull(response);
		}

		/// <summary>Test method refreshSuperUserGroupsConfiguration.</summary>
		/// <remarks>Test method refreshSuperUserGroupsConfiguration. This method present and it works.
		/// 	</remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRefreshSuperUserGroupsConfiguration()
		{
			RefreshSuperUserGroupsConfigurationRequest request = recordFactory.NewRecordInstance
				<RefreshSuperUserGroupsConfigurationRequest>();
			RefreshSuperUserGroupsConfigurationResponse response = client.RefreshSuperUserGroupsConfiguration
				(request);
			NUnit.Framework.Assert.IsNotNull(response);
		}

		/// <summary>Test method refreshUserToGroupsMappings.</summary>
		/// <remarks>Test method refreshUserToGroupsMappings. This method is present and it works.
		/// 	</remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRefreshUserToGroupsMappings()
		{
			RefreshUserToGroupsMappingsRequest request = recordFactory.NewRecordInstance<RefreshUserToGroupsMappingsRequest
				>();
			RefreshUserToGroupsMappingsResponse response = client.RefreshUserToGroupsMappings
				(request);
			NUnit.Framework.Assert.IsNotNull(response);
		}

		/// <summary>Test method refreshAdminAcls.</summary>
		/// <remarks>Test method refreshAdminAcls. This method is present and it works.</remarks>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRefreshAdminAcls()
		{
			RefreshAdminAclsRequest request = recordFactory.NewRecordInstance<RefreshAdminAclsRequest
				>();
			RefreshAdminAclsResponse response = client.RefreshAdminAcls(request);
			NUnit.Framework.Assert.IsNotNull(response);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestUpdateNodeResource()
		{
			UpdateNodeResourceRequest request = recordFactory.NewRecordInstance<UpdateNodeResourceRequest
				>();
			UpdateNodeResourceResponse response = client.UpdateNodeResource(request);
			NUnit.Framework.Assert.IsNotNull(response);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRefreshServiceAcls()
		{
			RefreshServiceAclsRequest request = recordFactory.NewRecordInstance<RefreshServiceAclsRequest
				>();
			RefreshServiceAclsResponse response = client.RefreshServiceAcls(request);
			NUnit.Framework.Assert.IsNotNull(response);
		}

		/// <summary>Stop server</summary>
		/// <exception cref="System.Exception"/>
		[AfterClass]
		public static void TearDownResourceManager()
		{
			if (resourceManager != null)
			{
				Log.Info("Stopping ResourceManager...");
				resourceManager.Stop();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private static IPEndPoint GetProtocolAddress(Configuration conf)
		{
			return conf.GetSocketAddr(YarnConfiguration.RmAdminAddress, YarnConfiguration.DefaultRmAdminAddress
				, YarnConfiguration.DefaultRmAdminPort);
		}
	}
}
