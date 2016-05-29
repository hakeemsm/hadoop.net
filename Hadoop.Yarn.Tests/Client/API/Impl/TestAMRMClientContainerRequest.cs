using System.Collections.Generic;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Client.Api;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Client.Api.Impl
{
	public class TestAMRMClientContainerRequest
	{
		[NUnit.Framework.Test]
		public virtual void TestFillInRacks()
		{
			AMRMClientImpl<AMRMClient.ContainerRequest> client = new AMRMClientImpl<AMRMClient.ContainerRequest
				>();
			Configuration conf = new Configuration();
			conf.SetClass(CommonConfigurationKeysPublic.NetTopologyNodeSwitchMappingImplKey, 
				typeof(TestAMRMClientContainerRequest.MyResolver), typeof(DNSToSwitchMapping));
			client.Init(conf);
			Resource capability = Resource.NewInstance(1024, 1);
			AMRMClient.ContainerRequest request = new AMRMClient.ContainerRequest(capability, 
				new string[] { "host1", "host2" }, new string[] { "/rack2" }, Priority.NewInstance
				(1));
			client.AddContainerRequest(request);
			VerifyResourceRequest(client, request, "host1", true);
			VerifyResourceRequest(client, request, "host2", true);
			VerifyResourceRequest(client, request, "/rack1", true);
			VerifyResourceRequest(client, request, "/rack2", true);
			VerifyResourceRequest(client, request, ResourceRequest.Any, true);
		}

		[NUnit.Framework.Test]
		public virtual void TestDisableLocalityRelaxation()
		{
			AMRMClientImpl<AMRMClient.ContainerRequest> client = new AMRMClientImpl<AMRMClient.ContainerRequest
				>();
			Configuration conf = new Configuration();
			conf.SetClass(CommonConfigurationKeysPublic.NetTopologyNodeSwitchMappingImplKey, 
				typeof(TestAMRMClientContainerRequest.MyResolver), typeof(DNSToSwitchMapping));
			client.Init(conf);
			Resource capability = Resource.NewInstance(1024, 1);
			AMRMClient.ContainerRequest nodeLevelRequest = new AMRMClient.ContainerRequest(capability
				, new string[] { "host1", "host2" }, null, Priority.NewInstance(1), false);
			client.AddContainerRequest(nodeLevelRequest);
			VerifyResourceRequest(client, nodeLevelRequest, ResourceRequest.Any, false);
			VerifyResourceRequest(client, nodeLevelRequest, "/rack1", false);
			VerifyResourceRequest(client, nodeLevelRequest, "host1", true);
			VerifyResourceRequest(client, nodeLevelRequest, "host2", true);
			// Make sure we don't get any errors with two node-level requests at the
			// same priority
			AMRMClient.ContainerRequest nodeLevelRequest2 = new AMRMClient.ContainerRequest(capability
				, new string[] { "host2", "host3" }, null, Priority.NewInstance(1), false);
			client.AddContainerRequest(nodeLevelRequest2);
			AMRMClient.ContainerRequest rackLevelRequest = new AMRMClient.ContainerRequest(capability
				, null, new string[] { "/rack3", "/rack4" }, Priority.NewInstance(2), false);
			client.AddContainerRequest(rackLevelRequest);
			VerifyResourceRequest(client, rackLevelRequest, ResourceRequest.Any, false);
			VerifyResourceRequest(client, rackLevelRequest, "/rack3", true);
			VerifyResourceRequest(client, rackLevelRequest, "/rack4", true);
			// Make sure we don't get any errors with two rack-level requests at the
			// same priority
			AMRMClient.ContainerRequest rackLevelRequest2 = new AMRMClient.ContainerRequest(capability
				, null, new string[] { "/rack4", "/rack5" }, Priority.NewInstance(2), false);
			client.AddContainerRequest(rackLevelRequest2);
			AMRMClient.ContainerRequest bothLevelRequest = new AMRMClient.ContainerRequest(capability
				, new string[] { "host3", "host4" }, new string[] { "rack1", "/otherrack" }, Priority
				.NewInstance(3), false);
			client.AddContainerRequest(bothLevelRequest);
			VerifyResourceRequest(client, bothLevelRequest, ResourceRequest.Any, false);
			VerifyResourceRequest(client, bothLevelRequest, "rack1", true);
			VerifyResourceRequest(client, bothLevelRequest, "/otherrack", true);
			VerifyResourceRequest(client, bothLevelRequest, "host3", true);
			VerifyResourceRequest(client, bothLevelRequest, "host4", true);
			// Make sure we don't get any errors with two both-level requests at the
			// same priority
			AMRMClient.ContainerRequest bothLevelRequest2 = new AMRMClient.ContainerRequest(capability
				, new string[] { "host4", "host5" }, new string[] { "rack1", "/otherrack2" }, Priority
				.NewInstance(3), false);
			client.AddContainerRequest(bothLevelRequest2);
		}

		public virtual void TestDifferentLocalityRelaxationSamePriority()
		{
			AMRMClientImpl<AMRMClient.ContainerRequest> client = new AMRMClientImpl<AMRMClient.ContainerRequest
				>();
			Configuration conf = new Configuration();
			conf.SetClass(CommonConfigurationKeysPublic.NetTopologyNodeSwitchMappingImplKey, 
				typeof(TestAMRMClientContainerRequest.MyResolver), typeof(DNSToSwitchMapping));
			client.Init(conf);
			Resource capability = Resource.NewInstance(1024, 1);
			AMRMClient.ContainerRequest request1 = new AMRMClient.ContainerRequest(capability
				, new string[] { "host1", "host2" }, null, Priority.NewInstance(1), false);
			client.AddContainerRequest(request1);
			AMRMClient.ContainerRequest request2 = new AMRMClient.ContainerRequest(capability
				, new string[] { "host3" }, null, Priority.NewInstance(1), true);
			client.AddContainerRequest(request2);
		}

		[NUnit.Framework.Test]
		public virtual void TestInvalidValidWhenOldRemoved()
		{
			AMRMClientImpl<AMRMClient.ContainerRequest> client = new AMRMClientImpl<AMRMClient.ContainerRequest
				>();
			Configuration conf = new Configuration();
			conf.SetClass(CommonConfigurationKeysPublic.NetTopologyNodeSwitchMappingImplKey, 
				typeof(TestAMRMClientContainerRequest.MyResolver), typeof(DNSToSwitchMapping));
			client.Init(conf);
			Resource capability = Resource.NewInstance(1024, 1);
			AMRMClient.ContainerRequest request1 = new AMRMClient.ContainerRequest(capability
				, new string[] { "host1", "host2" }, null, Priority.NewInstance(1), false);
			client.AddContainerRequest(request1);
			client.RemoveContainerRequest(request1);
			AMRMClient.ContainerRequest request2 = new AMRMClient.ContainerRequest(capability
				, new string[] { "host3" }, null, Priority.NewInstance(1), true);
			client.AddContainerRequest(request2);
			client.RemoveContainerRequest(request2);
			AMRMClient.ContainerRequest request3 = new AMRMClient.ContainerRequest(capability
				, new string[] { "host1", "host2" }, null, Priority.NewInstance(1), false);
			client.AddContainerRequest(request3);
			client.RemoveContainerRequest(request3);
			AMRMClient.ContainerRequest request4 = new AMRMClient.ContainerRequest(capability
				, null, new string[] { "rack1" }, Priority.NewInstance(1), true);
			client.AddContainerRequest(request4);
		}

		public virtual void TestLocalityRelaxationDifferentLevels()
		{
			AMRMClientImpl<AMRMClient.ContainerRequest> client = new AMRMClientImpl<AMRMClient.ContainerRequest
				>();
			Configuration conf = new Configuration();
			conf.SetClass(CommonConfigurationKeysPublic.NetTopologyNodeSwitchMappingImplKey, 
				typeof(TestAMRMClientContainerRequest.MyResolver), typeof(DNSToSwitchMapping));
			client.Init(conf);
			Resource capability = Resource.NewInstance(1024, 1);
			AMRMClient.ContainerRequest request1 = new AMRMClient.ContainerRequest(capability
				, new string[] { "host1", "host2" }, null, Priority.NewInstance(1), false);
			client.AddContainerRequest(request1);
			AMRMClient.ContainerRequest request2 = new AMRMClient.ContainerRequest(capability
				, null, new string[] { "rack1" }, Priority.NewInstance(1), true);
			client.AddContainerRequest(request2);
		}

		private class MyResolver : DNSToSwitchMapping
		{
			public virtual IList<string> Resolve(IList<string> names)
			{
				return Arrays.AsList("/rack1");
			}

			public virtual void ReloadCachedMappings()
			{
			}

			public virtual void ReloadCachedMappings(IList<string> names)
			{
			}
		}

		private void VerifyResourceRequest(AMRMClientImpl<AMRMClient.ContainerRequest> client
			, AMRMClient.ContainerRequest request, string location, bool expectedRelaxLocality
			)
		{
			ResourceRequest ask = client.remoteRequestsTable[request.GetPriority()][location]
				[request.GetCapability()].remoteRequest;
			NUnit.Framework.Assert.AreEqual(location, ask.GetResourceName());
			NUnit.Framework.Assert.AreEqual(1, ask.GetNumContainers());
			NUnit.Framework.Assert.AreEqual(expectedRelaxLocality, ask.GetRelaxLocality());
		}
	}
}
