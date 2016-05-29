using System.IO;
using Com.Google.Inject;
using Com.Google.Inject.Servlet;
using Com.Sun.Jersey.Api.Client;
using Com.Sun.Jersey.Guice.Spi.Container.Servlet;
using Com.Sun.Jersey.Test.Framework;
using Javax.WS.RS.Core;
using Javax.Xml.Parsers;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Event;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Launcher;
using Org.Apache.Hadoop.Yarn.Server.Security;
using Org.Apache.Hadoop.Yarn.Server.Utils;
using Org.Apache.Hadoop.Yarn.Util;
using Org.Apache.Hadoop.Yarn.Webapp;
using Org.Codehaus.Jettison.Json;
using Org.W3c.Dom;
using Org.Xml.Sax;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Webapp
{
	/// <summary>Test the nodemanager node info web services api's</summary>
	public class TestNMWebServices : JerseyTestBase
	{
		private static Context nmContext;

		private static ResourceView resourceView;

		private static ApplicationACLsManager aclsManager;

		private static LocalDirsHandlerService dirsHandler;

		private static WebApp nmWebApp;

		private static readonly FilePath testRootDir = new FilePath("target", typeof(Org.Apache.Hadoop.Yarn.Server.Nodemanager.Webapp.TestNMWebServices
			).Name);

		private static FilePath testLogDir = new FilePath("target", typeof(Org.Apache.Hadoop.Yarn.Server.Nodemanager.Webapp.TestNMWebServices
			).Name + "LogDir");

		private sealed class _ServletModule_100 : ServletModule
		{
			public _ServletModule_100()
			{
			}

			protected override void ConfigureServlets()
			{
				Configuration conf = new Configuration();
				conf.Set(YarnConfiguration.NmLocalDirs, Org.Apache.Hadoop.Yarn.Server.Nodemanager.Webapp.TestNMWebServices
					.testRootDir.GetAbsolutePath());
				conf.Set(YarnConfiguration.NmLogDirs, Org.Apache.Hadoop.Yarn.Server.Nodemanager.Webapp.TestNMWebServices
					.testLogDir.GetAbsolutePath());
				NodeHealthCheckerService healthChecker = new NodeHealthCheckerService();
				healthChecker.Init(conf);
				Org.Apache.Hadoop.Yarn.Server.Nodemanager.Webapp.TestNMWebServices.dirsHandler = 
					healthChecker.GetDiskHandler();
				Org.Apache.Hadoop.Yarn.Server.Nodemanager.Webapp.TestNMWebServices.aclsManager = 
					new ApplicationACLsManager(conf);
				Org.Apache.Hadoop.Yarn.Server.Nodemanager.Webapp.TestNMWebServices.nmContext = new 
					NodeManager.NMContext(null, null, Org.Apache.Hadoop.Yarn.Server.Nodemanager.Webapp.TestNMWebServices
					.dirsHandler, Org.Apache.Hadoop.Yarn.Server.Nodemanager.Webapp.TestNMWebServices
					.aclsManager, null);
				NodeId nodeId = NodeId.NewInstance("testhost.foo.com", 8042);
				((NodeManager.NMContext)Org.Apache.Hadoop.Yarn.Server.Nodemanager.Webapp.TestNMWebServices
					.nmContext).SetNodeId(nodeId);
				Org.Apache.Hadoop.Yarn.Server.Nodemanager.Webapp.TestNMWebServices.resourceView =
					 new _ResourceView_114();
				// 15.5G in bytes
				// 16G in bytes
				Org.Apache.Hadoop.Yarn.Server.Nodemanager.Webapp.TestNMWebServices.nmWebApp = new 
					WebServer.NMWebApp(Org.Apache.Hadoop.Yarn.Server.Nodemanager.Webapp.TestNMWebServices
					.resourceView, Org.Apache.Hadoop.Yarn.Server.Nodemanager.Webapp.TestNMWebServices
					.aclsManager, Org.Apache.Hadoop.Yarn.Server.Nodemanager.Webapp.TestNMWebServices
					.dirsHandler);
				this.Bind<JAXBContextResolver>();
				this.Bind<NMWebServices>();
				this.Bind<GenericExceptionHandler>();
				this.Bind<Context>().ToInstance(Org.Apache.Hadoop.Yarn.Server.Nodemanager.Webapp.TestNMWebServices
					.nmContext);
				this.Bind<WebApp>().ToInstance(Org.Apache.Hadoop.Yarn.Server.Nodemanager.Webapp.TestNMWebServices
					.nmWebApp);
				this.Bind<ResourceView>().ToInstance(Org.Apache.Hadoop.Yarn.Server.Nodemanager.Webapp.TestNMWebServices
					.resourceView);
				this.Bind<ApplicationACLsManager>().ToInstance(Org.Apache.Hadoop.Yarn.Server.Nodemanager.Webapp.TestNMWebServices
					.aclsManager);
				this.Bind<LocalDirsHandlerService>().ToInstance(Org.Apache.Hadoop.Yarn.Server.Nodemanager.Webapp.TestNMWebServices
					.dirsHandler);
				this.Serve("/*").With(typeof(GuiceContainer));
			}

			private sealed class _ResourceView_114 : ResourceView
			{
				public _ResourceView_114()
				{
				}

				public long GetVmemAllocatedForContainers()
				{
					return System.Convert.ToInt64("16642998272");
				}

				public long GetPmemAllocatedForContainers()
				{
					return System.Convert.ToInt64("17179869184");
				}

				public long GetVCoresAllocatedForContainers()
				{
					return System.Convert.ToInt64("4000");
				}

				public bool IsVmemCheckEnabled()
				{
					return true;
				}

				public bool IsPmemCheckEnabled()
				{
					return true;
				}
			}
		}

		private Injector injector = Guice.CreateInjector(new _ServletModule_100());

		public class GuiceServletConfig : GuiceServletContextListener
		{
			protected override Injector GetInjector()
			{
				return this._enclosing.injector;
			}

			internal GuiceServletConfig(TestNMWebServices _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly TestNMWebServices _enclosing;
		}

		/// <exception cref="System.Exception"/>
		[SetUp]
		public override void SetUp()
		{
			base.SetUp();
			testRootDir.Mkdirs();
			testLogDir.Mkdir();
		}

		[AfterClass]
		public static void Stop()
		{
			FileUtil.FullyDelete(testRootDir);
			FileUtil.FullyDelete(testLogDir);
		}

		public TestNMWebServices()
			: base(new WebAppDescriptor.Builder("org.apache.hadoop.yarn.server.nodemanager.webapp"
				).ContextListenerClass(typeof(TestNMWebServices.GuiceServletConfig)).FilterClass
				(typeof(GuiceFilter)).ContextPath("jersey-guice-filter").ServletPath("/").Build(
				))
		{
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
				responseStr = r.Path("ws").Path("v1").Path("node").Path("bogus").Accept(MediaType
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
		public virtual void TestInvalidAccept()
		{
			WebResource r = Resource();
			string responseStr = string.Empty;
			try
			{
				responseStr = r.Path("ws").Path("v1").Path("node").Accept(MediaType.TextPlain).Get
					<string>();
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
		public virtual void TestNode()
		{
			WebResource r = Resource();
			ClientResponse response = r.Path("ws").Path("v1").Path("node").Accept(MediaType.ApplicationJson
				).Get<ClientResponse>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			JSONObject json = response.GetEntity<JSONObject>();
			VerifyNodeInfo(json);
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNodeSlash()
		{
			WebResource r = Resource();
			ClientResponse response = r.Path("ws").Path("v1").Path("node/").Accept(MediaType.
				ApplicationJson).Get<ClientResponse>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			JSONObject json = response.GetEntity<JSONObject>();
			VerifyNodeInfo(json);
		}

		// make sure default is json output
		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNodeDefault()
		{
			WebResource r = Resource();
			ClientResponse response = r.Path("ws").Path("v1").Path("node").Get<ClientResponse
				>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			JSONObject json = response.GetEntity<JSONObject>();
			VerifyNodeInfo(json);
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNodeInfo()
		{
			WebResource r = Resource();
			ClientResponse response = r.Path("ws").Path("v1").Path("node").Path("info").Accept
				(MediaType.ApplicationJson).Get<ClientResponse>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			JSONObject json = response.GetEntity<JSONObject>();
			VerifyNodeInfo(json);
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNodeInfoSlash()
		{
			WebResource r = Resource();
			ClientResponse response = r.Path("ws").Path("v1").Path("node").Path("info/").Accept
				(MediaType.ApplicationJson).Get<ClientResponse>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			JSONObject json = response.GetEntity<JSONObject>();
			VerifyNodeInfo(json);
		}

		// make sure default is json output
		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNodeInfoDefault()
		{
			WebResource r = Resource();
			ClientResponse response = r.Path("ws").Path("v1").Path("node").Path("info").Get<ClientResponse
				>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			JSONObject json = response.GetEntity<JSONObject>();
			VerifyNodeInfo(json);
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestSingleNodesXML()
		{
			WebResource r = Resource();
			ClientResponse response = r.Path("ws").Path("v1").Path("node").Path("info/").Accept
				(MediaType.ApplicationXml).Get<ClientResponse>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationXmlType, response.GetType());
			string xml = response.GetEntity<string>();
			DocumentBuilderFactory dbf = DocumentBuilderFactory.NewInstance();
			DocumentBuilder db = dbf.NewDocumentBuilder();
			InputSource @is = new InputSource();
			@is.SetCharacterStream(new StringReader(xml));
			Document dom = db.Parse(@is);
			NodeList nodes = dom.GetElementsByTagName("nodeInfo");
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, nodes.GetLength
				());
			VerifyNodesXML(nodes);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestContainerLogs()
		{
			WebResource r = Resource();
			ContainerId containerId = BuilderUtils.NewContainerId(0, 0, 0, 0);
			string containerIdStr = BuilderUtils.NewContainerId(0, 0, 0, 0).ToString();
			ApplicationAttemptId appAttemptId = containerId.GetApplicationAttemptId();
			ApplicationId appId = appAttemptId.GetApplicationId();
			string appIdStr = appId.ToString();
			string filename = "logfile1";
			string logMessage = "log message\n";
			nmContext.GetApplications()[appId] = new ApplicationImpl(null, "user", appId, null
				, nmContext);
			MockContainer container = new MockContainer(appAttemptId, new AsyncDispatcher(), 
				new Configuration(), "user", appId, 1);
			container.SetState(ContainerState.Running);
			nmContext.GetContainers()[containerId] = container;
			// write out log file
			Path path = dirsHandler.GetLogPathForWrite(ContainerLaunch.GetRelativeContainerLogDir
				(appIdStr, containerIdStr) + "/" + filename, false);
			FilePath logFile = new FilePath(path.ToUri().GetPath());
			logFile.DeleteOnExit();
			NUnit.Framework.Assert.IsTrue("Failed to create log dir", logFile.GetParentFile()
				.Mkdirs());
			PrintWriter pw = new PrintWriter(logFile);
			pw.Write(logMessage);
			pw.Close();
			// ask for it
			ClientResponse response = r.Path("ws").Path("v1").Path("node").Path("containerlogs"
				).Path(containerIdStr).Path(filename).Accept(MediaType.TextPlain).Get<ClientResponse
				>();
			string responseText = response.GetEntity<string>();
			NUnit.Framework.Assert.AreEqual(logMessage, responseText);
			// ask for file that doesn't exist
			response = r.Path("ws").Path("v1").Path("node").Path("containerlogs").Path(containerIdStr
				).Path("uhhh").Accept(MediaType.TextPlain).Get<ClientResponse>();
			NUnit.Framework.Assert.AreEqual(ClientResponse.Status.NotFound.GetStatusCode(), response
				.GetStatus());
			responseText = response.GetEntity<string>();
			NUnit.Framework.Assert.IsTrue(responseText.Contains("Cannot find this log on the local disk."
				));
			// After container is completed, it is removed from nmContext
			Sharpen.Collections.Remove(nmContext.GetContainers(), containerId);
			NUnit.Framework.Assert.IsNull(nmContext.GetContainers()[containerId]);
			response = r.Path("ws").Path("v1").Path("node").Path("containerlogs").Path(containerIdStr
				).Path(filename).Accept(MediaType.TextPlain).Get<ClientResponse>();
			responseText = response.GetEntity<string>();
			NUnit.Framework.Assert.AreEqual(logMessage, responseText);
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		public virtual void VerifyNodesXML(NodeList nodes)
		{
			for (int i = 0; i < nodes.GetLength(); i++)
			{
				Element element = (Element)nodes.Item(i);
				VerifyNodeInfoGeneric(WebServicesTestUtils.GetXmlString(element, "id"), WebServicesTestUtils
					.GetXmlString(element, "healthReport"), WebServicesTestUtils.GetXmlLong(element, 
					"totalVmemAllocatedContainersMB"), WebServicesTestUtils.GetXmlLong(element, "totalPmemAllocatedContainersMB"
					), WebServicesTestUtils.GetXmlLong(element, "totalVCoresAllocatedContainers"), WebServicesTestUtils
					.GetXmlBoolean(element, "vmemCheckEnabled"), WebServicesTestUtils.GetXmlBoolean(
					element, "pmemCheckEnabled"), WebServicesTestUtils.GetXmlLong(element, "lastNodeUpdateTime"
					), WebServicesTestUtils.GetXmlBoolean(element, "nodeHealthy"), WebServicesTestUtils
					.GetXmlString(element, "nodeHostName"), WebServicesTestUtils.GetXmlString(element
					, "hadoopVersionBuiltOn"), WebServicesTestUtils.GetXmlString(element, "hadoopBuildVersion"
					), WebServicesTestUtils.GetXmlString(element, "hadoopVersion"), WebServicesTestUtils
					.GetXmlString(element, "nodeManagerVersionBuiltOn"), WebServicesTestUtils.GetXmlString
					(element, "nodeManagerBuildVersion"), WebServicesTestUtils.GetXmlString(element, 
					"nodeManagerVersion"));
			}
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		public virtual void VerifyNodeInfo(JSONObject json)
		{
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, json.Length());
			JSONObject info = json.GetJSONObject("nodeInfo");
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 16, info.Length()
				);
			VerifyNodeInfoGeneric(info.GetString("id"), info.GetString("healthReport"), info.
				GetLong("totalVmemAllocatedContainersMB"), info.GetLong("totalPmemAllocatedContainersMB"
				), info.GetLong("totalVCoresAllocatedContainers"), info.GetBoolean("vmemCheckEnabled"
				), info.GetBoolean("pmemCheckEnabled"), info.GetLong("lastNodeUpdateTime"), info
				.GetBoolean("nodeHealthy"), info.GetString("nodeHostName"), info.GetString("hadoopVersionBuiltOn"
				), info.GetString("hadoopBuildVersion"), info.GetString("hadoopVersion"), info.GetString
				("nodeManagerVersionBuiltOn"), info.GetString("nodeManagerBuildVersion"), info.GetString
				("nodeManagerVersion"));
		}

		public virtual void VerifyNodeInfoGeneric(string id, string healthReport, long totalVmemAllocatedContainersMB
			, long totalPmemAllocatedContainersMB, long totalVCoresAllocatedContainers, bool
			 vmemCheckEnabled, bool pmemCheckEnabled, long lastNodeUpdateTime, bool nodeHealthy
			, string nodeHostName, string hadoopVersionBuiltOn, string hadoopBuildVersion, string
			 hadoopVersion, string resourceManagerVersionBuiltOn, string resourceManagerBuildVersion
			, string resourceManagerVersion)
		{
			WebServicesTestUtils.CheckStringMatch("id", "testhost.foo.com:8042", id);
			WebServicesTestUtils.CheckStringMatch("healthReport", "Healthy", healthReport);
			NUnit.Framework.Assert.AreEqual("totalVmemAllocatedContainersMB incorrect", 15872
				, totalVmemAllocatedContainersMB);
			NUnit.Framework.Assert.AreEqual("totalPmemAllocatedContainersMB incorrect", 16384
				, totalPmemAllocatedContainersMB);
			NUnit.Framework.Assert.AreEqual("totalVCoresAllocatedContainers incorrect", 4000, 
				totalVCoresAllocatedContainers);
			NUnit.Framework.Assert.AreEqual("vmemCheckEnabled incorrect", true, vmemCheckEnabled
				);
			NUnit.Framework.Assert.AreEqual("pmemCheckEnabled incorrect", true, pmemCheckEnabled
				);
			NUnit.Framework.Assert.IsTrue("lastNodeUpdateTime incorrect", lastNodeUpdateTime 
				== nmContext.GetNodeHealthStatus().GetLastHealthReportTime());
			NUnit.Framework.Assert.IsTrue("nodeHealthy isn't true", nodeHealthy);
			WebServicesTestUtils.CheckStringMatch("nodeHostName", "testhost.foo.com", nodeHostName
				);
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
	}
}
