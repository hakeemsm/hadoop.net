using System.Collections.Generic;
using System.IO;
using Com.Google.Inject;
using Com.Google.Inject.Servlet;
using Com.Sun.Jersey.Api.Client;
using Com.Sun.Jersey.Guice.Spi.Container.Servlet;
using Com.Sun.Jersey.Test.Framework;
using Javax.Servlet.Http;
using Javax.WS.RS.Core;
using Javax.Xml.Parsers;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Service;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fifo;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Webapp.Dao;
using Org.Apache.Hadoop.Yarn.Util;
using Org.Apache.Hadoop.Yarn.Webapp;
using Org.Codehaus.Jettison.Json;
using Org.Mockito;
using Org.W3c.Dom;
using Org.Xml.Sax;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Webapp
{
	public class TestRMWebServices : JerseyTestBase
	{
		private static MockRM rm;

		private sealed class _ServletModule_87 : ServletModule
		{
			public _ServletModule_87()
			{
			}

			protected override void ConfigureServlets()
			{
				this.Bind<JAXBContextResolver>();
				this.Bind<RMWebServices>();
				this.Bind<GenericExceptionHandler>();
				Configuration conf = new Configuration();
				conf.SetClass(YarnConfiguration.RmScheduler, typeof(FifoScheduler), typeof(ResourceScheduler
					));
				Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Webapp.TestRMWebServices.rm = new MockRM
					(conf);
				this.Bind<ResourceManager>().ToInstance(Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Webapp.TestRMWebServices
					.rm);
				this.Serve("/*").With(typeof(GuiceContainer));
			}
		}

		private Injector injector = Guice.CreateInjector(new _ServletModule_87());

		public class GuiceServletConfig : GuiceServletContextListener
		{
			protected override Injector GetInjector()
			{
				return this._enclosing.injector;
			}

			internal GuiceServletConfig(TestRMWebServices _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly TestRMWebServices _enclosing;
		}

		/// <exception cref="System.Exception"/>
		[SetUp]
		public override void SetUp()
		{
			base.SetUp();
		}

		public TestRMWebServices()
			: base(new WebAppDescriptor.Builder("org.apache.hadoop.yarn.server.resourcemanager.webapp"
				).ContextListenerClass(typeof(TestRMWebServices.GuiceServletConfig)).FilterClass
				(typeof(GuiceFilter)).ContextPath("jersey-guice-filter").ServletPath("/").Build(
				))
		{
		}

		[BeforeClass]
		public static void InitClusterMetrics()
		{
			ClusterMetrics clusterMetrics = ClusterMetrics.GetMetrics();
			clusterMetrics.IncrDecommisionedNMs();
			clusterMetrics.IncrNumActiveNodes();
			clusterMetrics.IncrNumLostNMs();
			clusterMetrics.IncrNumRebootedNMs();
			clusterMetrics.IncrNumUnhealthyNMs();
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestInfoXML()
		{
			WebResource r = Resource();
			ClientResponse response = r.Path("ws").Path("v1").Path("cluster").Path("info").Accept
				("application/xml").Get<ClientResponse>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationXmlType, response.GetType());
			string xml = response.GetEntity<string>();
			VerifyClusterInfoXML(xml);
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestInvalidUri()
		{
			WebResource r = Resource();
			string responseStr = string.Empty;
			try
			{
				responseStr = r.Path("ws").Path("v1").Path("cluster").Path("bogus").Accept(MediaType
					.ApplicationJson).Get<string>();
				NUnit.Framework.Assert.Fail("should have thrown exception on invalid uri");
			}
			catch (UniformInterfaceException ue)
			{
				ClientResponse response = ue.GetResponse();
				NUnit.Framework.Assert.AreEqual(ClientResponse.Status.NotFound, response.GetClientResponseStatus
					());
				WebServicesTestUtils.CheckStringMatch("error string exists and shouldn't", string.Empty
					, responseStr);
			}
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestInvalidUri2()
		{
			WebResource r = Resource();
			string responseStr = string.Empty;
			try
			{
				responseStr = r.Accept(MediaType.ApplicationJson).Get<string>();
				NUnit.Framework.Assert.Fail("should have thrown exception on invalid uri");
			}
			catch (UniformInterfaceException ue)
			{
				ClientResponse response = ue.GetResponse();
				NUnit.Framework.Assert.AreEqual(ClientResponse.Status.NotFound, response.GetClientResponseStatus
					());
				WebServicesTestUtils.CheckStringMatch("error string exists and shouldn't", string.Empty
					, responseStr);
			}
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestInvalidAccept()
		{
			WebResource r = Resource();
			string responseStr = string.Empty;
			try
			{
				responseStr = r.Path("ws").Path("v1").Path("cluster").Accept(MediaType.TextPlain)
					.Get<string>();
				NUnit.Framework.Assert.Fail("should have thrown exception on invalid uri");
			}
			catch (UniformInterfaceException ue)
			{
				ClientResponse response = ue.GetResponse();
				NUnit.Framework.Assert.AreEqual(ClientResponse.Status.InternalServerError, response
					.GetClientResponseStatus());
				WebServicesTestUtils.CheckStringMatch("error string exists and shouldn't", string.Empty
					, responseStr);
			}
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestCluster()
		{
			WebResource r = Resource();
			ClientResponse response = r.Path("ws").Path("v1").Path("cluster").Accept(MediaType
				.ApplicationJson).Get<ClientResponse>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			JSONObject json = response.GetEntity<JSONObject>();
			VerifyClusterInfo(json);
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestClusterSlash()
		{
			WebResource r = Resource();
			// test with trailing "/" to make sure acts same as without slash
			ClientResponse response = r.Path("ws").Path("v1").Path("cluster/").Accept(MediaType
				.ApplicationJson).Get<ClientResponse>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			JSONObject json = response.GetEntity<JSONObject>();
			VerifyClusterInfo(json);
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestClusterDefault()
		{
			WebResource r = Resource();
			// test with trailing "/" to make sure acts same as without slash
			ClientResponse response = r.Path("ws").Path("v1").Path("cluster").Get<ClientResponse
				>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			JSONObject json = response.GetEntity<JSONObject>();
			VerifyClusterInfo(json);
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestInfo()
		{
			WebResource r = Resource();
			ClientResponse response = r.Path("ws").Path("v1").Path("cluster").Path("info").Accept
				(MediaType.ApplicationJson).Get<ClientResponse>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			JSONObject json = response.GetEntity<JSONObject>();
			VerifyClusterInfo(json);
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestInfoSlash()
		{
			// test with trailing "/" to make sure acts same as without slash
			WebResource r = Resource();
			ClientResponse response = r.Path("ws").Path("v1").Path("cluster").Path("info/").Accept
				(MediaType.ApplicationJson).Get<ClientResponse>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			JSONObject json = response.GetEntity<JSONObject>();
			VerifyClusterInfo(json);
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestInfoDefault()
		{
			WebResource r = Resource();
			ClientResponse response = r.Path("ws").Path("v1").Path("cluster").Path("info").Get
				<ClientResponse>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			JSONObject json = response.GetEntity<JSONObject>();
			VerifyClusterInfo(json);
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		public virtual void VerifyClusterInfoXML(string xml)
		{
			DocumentBuilderFactory dbf = DocumentBuilderFactory.NewInstance();
			DocumentBuilder db = dbf.NewDocumentBuilder();
			InputSource @is = new InputSource();
			@is.SetCharacterStream(new StringReader(xml));
			Document dom = db.Parse(@is);
			NodeList nodes = dom.GetElementsByTagName("clusterInfo");
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, nodes.GetLength
				());
			for (int i = 0; i < nodes.GetLength(); i++)
			{
				Element element = (Element)nodes.Item(i);
				VerifyClusterGeneric(WebServicesTestUtils.GetXmlLong(element, "id"), WebServicesTestUtils
					.GetXmlLong(element, "startedOn"), WebServicesTestUtils.GetXmlString(element, "state"
					), WebServicesTestUtils.GetXmlString(element, "haState"), WebServicesTestUtils.GetXmlString
					(element, "haZooKeeperConnectionState"), WebServicesTestUtils.GetXmlString(element
					, "hadoopVersionBuiltOn"), WebServicesTestUtils.GetXmlString(element, "hadoopBuildVersion"
					), WebServicesTestUtils.GetXmlString(element, "hadoopVersion"), WebServicesTestUtils
					.GetXmlString(element, "resourceManagerVersionBuiltOn"), WebServicesTestUtils.GetXmlString
					(element, "resourceManagerBuildVersion"), WebServicesTestUtils.GetXmlString(element
					, "resourceManagerVersion"));
			}
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		public virtual void VerifyClusterInfo(JSONObject json)
		{
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, json.Length());
			JSONObject info = json.GetJSONObject("clusterInfo");
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 12, info.Length()
				);
			VerifyClusterGeneric(info.GetLong("id"), info.GetLong("startedOn"), info.GetString
				("state"), info.GetString("haState"), info.GetString("haZooKeeperConnectionState"
				), info.GetString("hadoopVersionBuiltOn"), info.GetString("hadoopBuildVersion"), 
				info.GetString("hadoopVersion"), info.GetString("resourceManagerVersionBuiltOn")
				, info.GetString("resourceManagerBuildVersion"), info.GetString("resourceManagerVersion"
				));
		}

		public virtual void VerifyClusterGeneric(long clusterid, long startedon, string state
			, string haState, string haZooKeeperConnectionState, string hadoopVersionBuiltOn
			, string hadoopBuildVersion, string hadoopVersion, string resourceManagerVersionBuiltOn
			, string resourceManagerBuildVersion, string resourceManagerVersion)
		{
			NUnit.Framework.Assert.AreEqual("clusterId doesn't match: ", ResourceManager.GetClusterTimeStamp
				(), clusterid);
			NUnit.Framework.Assert.AreEqual("startedOn doesn't match: ", ResourceManager.GetClusterTimeStamp
				(), startedon);
			NUnit.Framework.Assert.IsTrue("stated doesn't match: " + state, state.Matches(Service.STATE
				.Inited.ToString()));
			NUnit.Framework.Assert.IsTrue("HA state doesn't match: " + haState, haState.Matches
				("INITIALIZING"));
			WebServicesTestUtils.CheckStringMatch("hadoopVersionBuiltOn", VersionInfo.GetDate
				(), hadoopVersionBuiltOn);
			WebServicesTestUtils.CheckStringEqual("hadoopBuildVersion", VersionInfo.GetBuildVersion
				(), hadoopBuildVersion);
			WebServicesTestUtils.CheckStringMatch("hadoopVersion", VersionInfo.GetVersion(), 
				hadoopVersion);
			WebServicesTestUtils.CheckStringMatch("resourceManagerVersionBuiltOn", YarnVersionInfo
				.GetDate(), resourceManagerVersionBuiltOn);
			WebServicesTestUtils.CheckStringEqual("resourceManagerBuildVersion", YarnVersionInfo
				.GetBuildVersion(), resourceManagerBuildVersion);
			WebServicesTestUtils.CheckStringMatch("resourceManagerVersion", YarnVersionInfo.GetVersion
				(), resourceManagerVersion);
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestClusterMetrics()
		{
			WebResource r = Resource();
			ClientResponse response = r.Path("ws").Path("v1").Path("cluster").Path("metrics")
				.Accept(MediaType.ApplicationJson).Get<ClientResponse>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			JSONObject json = response.GetEntity<JSONObject>();
			VerifyClusterMetricsJSON(json);
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestClusterMetricsSlash()
		{
			WebResource r = Resource();
			ClientResponse response = r.Path("ws").Path("v1").Path("cluster").Path("metrics/"
				).Accept(MediaType.ApplicationJson).Get<ClientResponse>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			JSONObject json = response.GetEntity<JSONObject>();
			VerifyClusterMetricsJSON(json);
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestClusterMetricsDefault()
		{
			WebResource r = Resource();
			ClientResponse response = r.Path("ws").Path("v1").Path("cluster").Path("metrics")
				.Get<ClientResponse>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			JSONObject json = response.GetEntity<JSONObject>();
			VerifyClusterMetricsJSON(json);
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestClusterMetricsXML()
		{
			WebResource r = Resource();
			ClientResponse response = r.Path("ws").Path("v1").Path("cluster").Path("metrics")
				.Accept("application/xml").Get<ClientResponse>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationXmlType, response.GetType());
			string xml = response.GetEntity<string>();
			VerifyClusterMetricsXML(xml);
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		public virtual void VerifyClusterMetricsXML(string xml)
		{
			DocumentBuilderFactory dbf = DocumentBuilderFactory.NewInstance();
			DocumentBuilder db = dbf.NewDocumentBuilder();
			InputSource @is = new InputSource();
			@is.SetCharacterStream(new StringReader(xml));
			Document dom = db.Parse(@is);
			NodeList nodes = dom.GetElementsByTagName("clusterMetrics");
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, nodes.GetLength
				());
			for (int i = 0; i < nodes.GetLength(); i++)
			{
				Element element = (Element)nodes.Item(i);
				VerifyClusterMetrics(WebServicesTestUtils.GetXmlInt(element, "appsSubmitted"), WebServicesTestUtils
					.GetXmlInt(element, "appsCompleted"), WebServicesTestUtils.GetXmlInt(element, "reservedMB"
					), WebServicesTestUtils.GetXmlInt(element, "availableMB"), WebServicesTestUtils.
					GetXmlInt(element, "allocatedMB"), WebServicesTestUtils.GetXmlInt(element, "reservedVirtualCores"
					), WebServicesTestUtils.GetXmlInt(element, "availableVirtualCores"), WebServicesTestUtils
					.GetXmlInt(element, "allocatedVirtualCores"), WebServicesTestUtils.GetXmlInt(element
					, "totalVirtualCores"), WebServicesTestUtils.GetXmlInt(element, "containersAllocated"
					), WebServicesTestUtils.GetXmlInt(element, "totalMB"), WebServicesTestUtils.GetXmlInt
					(element, "totalNodes"), WebServicesTestUtils.GetXmlInt(element, "lostNodes"), WebServicesTestUtils
					.GetXmlInt(element, "unhealthyNodes"), WebServicesTestUtils.GetXmlInt(element, "decommissionedNodes"
					), WebServicesTestUtils.GetXmlInt(element, "rebootedNodes"), WebServicesTestUtils
					.GetXmlInt(element, "activeNodes"));
			}
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		public virtual void VerifyClusterMetricsJSON(JSONObject json)
		{
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, json.Length());
			JSONObject clusterinfo = json.GetJSONObject("clusterMetrics");
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 23, clusterinfo.Length
				());
			VerifyClusterMetrics(clusterinfo.GetInt("appsSubmitted"), clusterinfo.GetInt("appsCompleted"
				), clusterinfo.GetInt("reservedMB"), clusterinfo.GetInt("availableMB"), clusterinfo
				.GetInt("allocatedMB"), clusterinfo.GetInt("reservedVirtualCores"), clusterinfo.
				GetInt("availableVirtualCores"), clusterinfo.GetInt("allocatedVirtualCores"), clusterinfo
				.GetInt("totalVirtualCores"), clusterinfo.GetInt("containersAllocated"), clusterinfo
				.GetInt("totalMB"), clusterinfo.GetInt("totalNodes"), clusterinfo.GetInt("lostNodes"
				), clusterinfo.GetInt("unhealthyNodes"), clusterinfo.GetInt("decommissionedNodes"
				), clusterinfo.GetInt("rebootedNodes"), clusterinfo.GetInt("activeNodes"));
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		public virtual void VerifyClusterMetrics(int submittedApps, int completedApps, int
			 reservedMB, int availableMB, int allocMB, int reservedVirtualCores, int availableVirtualCores
			, int allocVirtualCores, int totalVirtualCores, int containersAlloc, int totalMB
			, int totalNodes, int lostNodes, int unhealthyNodes, int decommissionedNodes, int
			 rebootedNodes, int activeNodes)
		{
			ResourceScheduler rs = rm.GetResourceScheduler();
			QueueMetrics metrics = rs.GetRootQueueMetrics();
			ClusterMetrics clusterMetrics = ClusterMetrics.GetMetrics();
			long totalMBExpect = metrics.GetAvailableMB() + metrics.GetAllocatedMB();
			long totalVirtualCoresExpect = metrics.GetAvailableVirtualCores() + metrics.GetAllocatedVirtualCores
				();
			NUnit.Framework.Assert.AreEqual("appsSubmitted doesn't match", metrics.GetAppsSubmitted
				(), submittedApps);
			NUnit.Framework.Assert.AreEqual("appsCompleted doesn't match", metrics.GetAppsCompleted
				(), completedApps);
			NUnit.Framework.Assert.AreEqual("reservedMB doesn't match", metrics.GetReservedMB
				(), reservedMB);
			NUnit.Framework.Assert.AreEqual("availableMB doesn't match", metrics.GetAvailableMB
				(), availableMB);
			NUnit.Framework.Assert.AreEqual("allocatedMB doesn't match", metrics.GetAllocatedMB
				(), allocMB);
			NUnit.Framework.Assert.AreEqual("reservedVirtualCores doesn't match", metrics.GetReservedVirtualCores
				(), reservedVirtualCores);
			NUnit.Framework.Assert.AreEqual("availableVirtualCores doesn't match", metrics.GetAvailableVirtualCores
				(), availableVirtualCores);
			NUnit.Framework.Assert.AreEqual("allocatedVirtualCores doesn't match", totalVirtualCoresExpect
				, allocVirtualCores);
			NUnit.Framework.Assert.AreEqual("containersAllocated doesn't match", 0, containersAlloc
				);
			NUnit.Framework.Assert.AreEqual("totalMB doesn't match", totalMBExpect, totalMB);
			NUnit.Framework.Assert.AreEqual("totalNodes doesn't match", clusterMetrics.GetNumActiveNMs
				() + clusterMetrics.GetNumLostNMs() + clusterMetrics.GetNumDecommisionedNMs() + 
				clusterMetrics.GetNumRebootedNMs() + clusterMetrics.GetUnhealthyNMs(), totalNodes
				);
			NUnit.Framework.Assert.AreEqual("lostNodes doesn't match", clusterMetrics.GetNumLostNMs
				(), lostNodes);
			NUnit.Framework.Assert.AreEqual("unhealthyNodes doesn't match", clusterMetrics.GetUnhealthyNMs
				(), unhealthyNodes);
			NUnit.Framework.Assert.AreEqual("decommissionedNodes doesn't match", clusterMetrics
				.GetNumDecommisionedNMs(), decommissionedNodes);
			NUnit.Framework.Assert.AreEqual("rebootedNodes doesn't match", clusterMetrics.GetNumRebootedNMs
				(), rebootedNodes);
			NUnit.Framework.Assert.AreEqual("activeNodes doesn't match", clusterMetrics.GetNumActiveNMs
				(), activeNodes);
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestClusterSchedulerFifo()
		{
			WebResource r = Resource();
			ClientResponse response = r.Path("ws").Path("v1").Path("cluster").Path("scheduler"
				).Accept(MediaType.ApplicationJson).Get<ClientResponse>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			JSONObject json = response.GetEntity<JSONObject>();
			VerifyClusterSchedulerFifo(json);
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestClusterSchedulerFifoSlash()
		{
			WebResource r = Resource();
			ClientResponse response = r.Path("ws").Path("v1").Path("cluster").Path("scheduler/"
				).Accept(MediaType.ApplicationJson).Get<ClientResponse>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			JSONObject json = response.GetEntity<JSONObject>();
			VerifyClusterSchedulerFifo(json);
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestClusterSchedulerFifoDefault()
		{
			WebResource r = Resource();
			ClientResponse response = r.Path("ws").Path("v1").Path("cluster").Path("scheduler"
				).Get<ClientResponse>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			JSONObject json = response.GetEntity<JSONObject>();
			VerifyClusterSchedulerFifo(json);
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestClusterSchedulerFifoXML()
		{
			WebResource r = Resource();
			ClientResponse response = r.Path("ws").Path("v1").Path("cluster").Path("scheduler"
				).Accept(MediaType.ApplicationXml).Get<ClientResponse>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationXmlType, response.GetType());
			string xml = response.GetEntity<string>();
			VerifySchedulerFifoXML(xml);
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		public virtual void VerifySchedulerFifoXML(string xml)
		{
			DocumentBuilderFactory dbf = DocumentBuilderFactory.NewInstance();
			DocumentBuilder db = dbf.NewDocumentBuilder();
			InputSource @is = new InputSource();
			@is.SetCharacterStream(new StringReader(xml));
			Document dom = db.Parse(@is);
			NodeList nodesSched = dom.GetElementsByTagName("scheduler");
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, nodesSched.GetLength
				());
			NodeList nodes = dom.GetElementsByTagName("schedulerInfo");
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, nodes.GetLength
				());
			for (int i = 0; i < nodes.GetLength(); i++)
			{
				Element element = (Element)nodes.Item(i);
				VerifyClusterSchedulerFifoGeneric(WebServicesTestUtils.GetXmlAttrString(element, 
					"xsi:type"), WebServicesTestUtils.GetXmlString(element, "qstate"), WebServicesTestUtils
					.GetXmlFloat(element, "capacity"), WebServicesTestUtils.GetXmlFloat(element, "usedCapacity"
					), WebServicesTestUtils.GetXmlInt(element, "minQueueMemoryCapacity"), WebServicesTestUtils
					.GetXmlInt(element, "maxQueueMemoryCapacity"), WebServicesTestUtils.GetXmlInt(element
					, "numNodes"), WebServicesTestUtils.GetXmlInt(element, "usedNodeCapacity"), WebServicesTestUtils
					.GetXmlInt(element, "availNodeCapacity"), WebServicesTestUtils.GetXmlInt(element
					, "totalNodeCapacity"), WebServicesTestUtils.GetXmlInt(element, "numContainers")
					);
			}
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		public virtual void VerifyClusterSchedulerFifo(JSONObject json)
		{
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, json.Length());
			JSONObject info = json.GetJSONObject("scheduler");
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, info.Length());
			info = info.GetJSONObject("schedulerInfo");
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 11, info.Length()
				);
			VerifyClusterSchedulerFifoGeneric(info.GetString("type"), info.GetString("qstate"
				), (float)info.GetDouble("capacity"), (float)info.GetDouble("usedCapacity"), info
				.GetInt("minQueueMemoryCapacity"), info.GetInt("maxQueueMemoryCapacity"), info.GetInt
				("numNodes"), info.GetInt("usedNodeCapacity"), info.GetInt("availNodeCapacity"), 
				info.GetInt("totalNodeCapacity"), info.GetInt("numContainers"));
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		public virtual void VerifyClusterSchedulerFifoGeneric(string type, string state, 
			float capacity, float usedCapacity, int minQueueCapacity, int maxQueueCapacity, 
			int numNodes, int usedNodeCapacity, int availNodeCapacity, int totalNodeCapacity
			, int numContainers)
		{
			NUnit.Framework.Assert.AreEqual("type doesn't match", "fifoScheduler", type);
			NUnit.Framework.Assert.AreEqual("qstate doesn't match", QueueState.Running.ToString
				(), state);
			NUnit.Framework.Assert.AreEqual("capacity doesn't match", 1.0, capacity, 0.0);
			NUnit.Framework.Assert.AreEqual("usedCapacity doesn't match", 0.0, usedCapacity, 
				0.0);
			NUnit.Framework.Assert.AreEqual("minQueueMemoryCapacity doesn't match", YarnConfiguration
				.DefaultRmSchedulerMinimumAllocationMb, minQueueCapacity);
			NUnit.Framework.Assert.AreEqual("maxQueueMemoryCapacity doesn't match", YarnConfiguration
				.DefaultRmSchedulerMaximumAllocationMb, maxQueueCapacity);
			NUnit.Framework.Assert.AreEqual("numNodes doesn't match", 0, numNodes);
			NUnit.Framework.Assert.AreEqual("usedNodeCapacity doesn't match", 0, usedNodeCapacity
				);
			NUnit.Framework.Assert.AreEqual("availNodeCapacity doesn't match", 0, availNodeCapacity
				);
			NUnit.Framework.Assert.AreEqual("totalNodeCapacity doesn't match", 0, totalNodeCapacity
				);
			NUnit.Framework.Assert.AreEqual("numContainers doesn't match", 0, numContainers);
		}

		// Test the scenario where the RM removes an app just as we try to
		// look at it in the apps list
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAppsRace()
		{
			// mock up an RM that returns app reports for apps that don't exist
			// in the RMApps list
			ApplicationId appId = ApplicationId.NewInstance(1, 1);
			ApplicationReport mockReport = Org.Mockito.Mockito.Mock<ApplicationReport>();
			Org.Mockito.Mockito.When(mockReport.GetApplicationId()).ThenReturn(appId);
			GetApplicationsResponse mockAppsResponse = Org.Mockito.Mockito.Mock<GetApplicationsResponse
				>();
			Org.Mockito.Mockito.When(mockAppsResponse.GetApplicationList()).ThenReturn(Arrays
				.AsList(new ApplicationReport[] { mockReport }));
			ClientRMService mockClientSvc = Org.Mockito.Mockito.Mock<ClientRMService>();
			Org.Mockito.Mockito.When(mockClientSvc.GetApplications(Matchers.IsA<GetApplicationsRequest
				>(), Matchers.AnyBoolean())).ThenReturn(mockAppsResponse);
			ResourceManager mockRM = Org.Mockito.Mockito.Mock<ResourceManager>();
			RMContextImpl rmContext = new RMContextImpl(null, null, null, null, null, null, null
				, null, null, null);
			Org.Mockito.Mockito.When(mockRM.GetRMContext()).ThenReturn(rmContext);
			Org.Mockito.Mockito.When(mockRM.GetClientRMService()).ThenReturn(mockClientSvc);
			RMWebServices webSvc = new RMWebServices(mockRM, new Configuration(), Org.Mockito.Mockito.Mock
				<HttpServletResponse>());
			ICollection<string> emptySet = Collections.UnmodifiableSet(Collections.EmptySet<string
				>());
			// verify we don't get any apps when querying
			HttpServletRequest mockHsr = Org.Mockito.Mockito.Mock<HttpServletRequest>();
			AppsInfo appsInfo = webSvc.GetApps(mockHsr, null, emptySet, null, null, null, null
				, null, null, null, null, emptySet, emptySet);
			NUnit.Framework.Assert.IsTrue(appsInfo.GetApps().IsEmpty());
			// verify we don't get an NPE when specifying a final status query
			appsInfo = webSvc.GetApps(mockHsr, null, emptySet, "FAILED", null, null, null, null
				, null, null, null, emptySet, emptySet);
			NUnit.Framework.Assert.IsTrue(appsInfo.GetApps().IsEmpty());
		}
	}
}
