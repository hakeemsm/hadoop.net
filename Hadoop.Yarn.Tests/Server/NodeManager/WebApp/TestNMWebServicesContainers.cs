using System.Collections.Generic;
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
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Event;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Recovery;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Security;
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
	public class TestNMWebServicesContainers : JerseyTestBase
	{
		private static Context nmContext;

		private static ResourceView resourceView;

		private static ApplicationACLsManager aclsManager;

		private static LocalDirsHandlerService dirsHandler;

		private static WebApp nmWebApp;

		private static Configuration conf = new Configuration();

		private static readonly FilePath testRootDir = new FilePath("target", typeof(Org.Apache.Hadoop.Yarn.Server.Nodemanager.Webapp.TestNMWebServicesContainers
			).Name);

		private static FilePath testLogDir = new FilePath("target", typeof(Org.Apache.Hadoop.Yarn.Server.Nodemanager.Webapp.TestNMWebServicesContainers
			).Name + "LogDir");

		private sealed class _ServletModule_93 : ServletModule
		{
			public _ServletModule_93()
			{
			}

			protected override void ConfigureServlets()
			{
				Org.Apache.Hadoop.Yarn.Server.Nodemanager.Webapp.TestNMWebServicesContainers.resourceView
					 = new _ResourceView_96();
				// 15.5G in bytes
				// 16G in bytes
				Org.Apache.Hadoop.Yarn.Server.Nodemanager.Webapp.TestNMWebServicesContainers.conf
					.Set(YarnConfiguration.NmLocalDirs, Org.Apache.Hadoop.Yarn.Server.Nodemanager.Webapp.TestNMWebServicesContainers
					.testRootDir.GetAbsolutePath());
				Org.Apache.Hadoop.Yarn.Server.Nodemanager.Webapp.TestNMWebServicesContainers.conf
					.Set(YarnConfiguration.NmLogDirs, Org.Apache.Hadoop.Yarn.Server.Nodemanager.Webapp.TestNMWebServicesContainers
					.testLogDir.GetAbsolutePath());
				NodeHealthCheckerService healthChecker = new NodeHealthCheckerService();
				healthChecker.Init(Org.Apache.Hadoop.Yarn.Server.Nodemanager.Webapp.TestNMWebServicesContainers
					.conf);
				Org.Apache.Hadoop.Yarn.Server.Nodemanager.Webapp.TestNMWebServicesContainers.dirsHandler
					 = healthChecker.GetDiskHandler();
				Org.Apache.Hadoop.Yarn.Server.Nodemanager.Webapp.TestNMWebServicesContainers.aclsManager
					 = new ApplicationACLsManager(Org.Apache.Hadoop.Yarn.Server.Nodemanager.Webapp.TestNMWebServicesContainers
					.conf);
				Org.Apache.Hadoop.Yarn.Server.Nodemanager.Webapp.TestNMWebServicesContainers.nmContext
					 = new _NMContext_131(null, null, Org.Apache.Hadoop.Yarn.Server.Nodemanager.Webapp.TestNMWebServicesContainers
					.dirsHandler, Org.Apache.Hadoop.Yarn.Server.Nodemanager.Webapp.TestNMWebServicesContainers
					.aclsManager, null);
				Org.Apache.Hadoop.Yarn.Server.Nodemanager.Webapp.TestNMWebServicesContainers.nmWebApp
					 = new WebServer.NMWebApp(Org.Apache.Hadoop.Yarn.Server.Nodemanager.Webapp.TestNMWebServicesContainers
					.resourceView, Org.Apache.Hadoop.Yarn.Server.Nodemanager.Webapp.TestNMWebServicesContainers
					.aclsManager, Org.Apache.Hadoop.Yarn.Server.Nodemanager.Webapp.TestNMWebServicesContainers
					.dirsHandler);
				this.Bind<JAXBContextResolver>();
				this.Bind<NMWebServices>();
				this.Bind<GenericExceptionHandler>();
				this.Bind<Context>().ToInstance(Org.Apache.Hadoop.Yarn.Server.Nodemanager.Webapp.TestNMWebServicesContainers
					.nmContext);
				this.Bind<WebApp>().ToInstance(Org.Apache.Hadoop.Yarn.Server.Nodemanager.Webapp.TestNMWebServicesContainers
					.nmWebApp);
				this.Bind<ResourceView>().ToInstance(Org.Apache.Hadoop.Yarn.Server.Nodemanager.Webapp.TestNMWebServicesContainers
					.resourceView);
				this.Bind<ApplicationACLsManager>().ToInstance(Org.Apache.Hadoop.Yarn.Server.Nodemanager.Webapp.TestNMWebServicesContainers
					.aclsManager);
				this.Bind<LocalDirsHandlerService>().ToInstance(Org.Apache.Hadoop.Yarn.Server.Nodemanager.Webapp.TestNMWebServicesContainers
					.dirsHandler);
				this.Serve("/*").With(typeof(GuiceContainer));
			}

			private sealed class _ResourceView_96 : ResourceView
			{
				public _ResourceView_96()
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

			private sealed class _NMContext_131 : NodeManager.NMContext
			{
				public _NMContext_131(NMContainerTokenSecretManager baseArg1, NMTokenSecretManagerInNM
					 baseArg2, LocalDirsHandlerService baseArg3, ApplicationACLsManager baseArg4, NMStateStoreService
					 baseArg5)
					: base(baseArg1, baseArg2, baseArg3, baseArg4, baseArg5)
				{
				}

				public override NodeId GetNodeId()
				{
					return NodeId.NewInstance("testhost.foo.com", 8042);
				}

				public override int GetHttpPort()
				{
					return 1234;
				}
			}
		}

		private Injector injector = Guice.CreateInjector(new _ServletModule_93());

		public class GuiceServletConfig : GuiceServletContextListener
		{
			protected override Injector GetInjector()
			{
				return this._enclosing.injector;
			}

			internal GuiceServletConfig(TestNMWebServicesContainers _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly TestNMWebServicesContainers _enclosing;
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
		public static void Cleanup()
		{
			FileUtil.FullyDelete(testRootDir);
			FileUtil.FullyDelete(testLogDir);
		}

		public TestNMWebServicesContainers()
			: base(new WebAppDescriptor.Builder("org.apache.hadoop.yarn.server.nodemanager.webapp"
				).ContextListenerClass(typeof(TestNMWebServicesContainers.GuiceServletConfig)).FilterClass
				(typeof(GuiceFilter)).ContextPath("jersey-guice-filter").ServletPath("/").Build(
				))
		{
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNodeContainersNone()
		{
			WebResource r = Resource();
			ClientResponse response = r.Path("ws").Path("v1").Path("node").Path("containers")
				.Accept(MediaType.ApplicationJson).Get<ClientResponse>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			JSONObject json = response.GetEntity<JSONObject>();
			NUnit.Framework.Assert.AreEqual("apps isn't NULL", JSONObject.Null, json.Get("containers"
				));
		}

		/// <exception cref="System.IO.IOException"/>
		private Dictionary<string, string> AddAppContainers(Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application.Application
			 app)
		{
			Dispatcher dispatcher = new AsyncDispatcher();
			ApplicationAttemptId appAttemptId = BuilderUtils.NewApplicationAttemptId(app.GetAppId
				(), 1);
			Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container container1
				 = new MockContainer(appAttemptId, dispatcher, conf, app.GetUser(), app.GetAppId
				(), 1);
			Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container container2
				 = new MockContainer(appAttemptId, dispatcher, conf, app.GetUser(), app.GetAppId
				(), 2);
			nmContext.GetContainers()[container1.GetContainerId()] = container1;
			nmContext.GetContainers()[container2.GetContainerId()] = container2;
			app.GetContainers()[container1.GetContainerId()] = container1;
			app.GetContainers()[container2.GetContainerId()] = container2;
			Dictionary<string, string> hash = new Dictionary<string, string>();
			hash[container1.GetContainerId().ToString()] = container1.GetContainerId().ToString
				();
			hash[container2.GetContainerId().ToString()] = container2.GetContainerId().ToString
				();
			return hash;
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNodeContainers()
		{
			TestNodeHelper("containers", MediaType.ApplicationJson);
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNodeContainersSlash()
		{
			TestNodeHelper("containers/", MediaType.ApplicationJson);
		}

		// make sure default is json output
		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNodeContainersDefault()
		{
			TestNodeHelper("containers/", string.Empty);
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		public virtual void TestNodeHelper(string path, string media)
		{
			WebResource r = Resource();
			Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application.Application
				 app = new MockApp(1);
			nmContext.GetApplications()[app.GetAppId()] = app;
			AddAppContainers(app);
			Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application.Application
				 app2 = new MockApp(2);
			nmContext.GetApplications()[app2.GetAppId()] = app2;
			AddAppContainers(app2);
			ClientResponse response = r.Path("ws").Path("v1").Path("node").Path(path).Accept(
				media).Get<ClientResponse>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			JSONObject json = response.GetEntity<JSONObject>();
			JSONObject info = json.GetJSONObject("containers");
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, info.Length());
			JSONArray conInfo = info.GetJSONArray("container");
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 4, conInfo.Length
				());
			for (int i = 0; i < conInfo.Length(); i++)
			{
				VerifyNodeContainerInfo(conInfo.GetJSONObject(i), nmContext.GetContainers()[ConverterUtils
					.ToContainerId(conInfo.GetJSONObject(i).GetString("id"))]);
			}
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNodeSingleContainers()
		{
			TestNodeSingleContainersHelper(MediaType.ApplicationJson);
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNodeSingleContainersSlash()
		{
			TestNodeSingleContainersHelper(MediaType.ApplicationJson);
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNodeSingleContainersDefault()
		{
			TestNodeSingleContainersHelper(string.Empty);
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		public virtual void TestNodeSingleContainersHelper(string media)
		{
			WebResource r = Resource();
			Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application.Application
				 app = new MockApp(1);
			nmContext.GetApplications()[app.GetAppId()] = app;
			Dictionary<string, string> hash = AddAppContainers(app);
			Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application.Application
				 app2 = new MockApp(2);
			nmContext.GetApplications()[app2.GetAppId()] = app2;
			AddAppContainers(app2);
			foreach (string id in hash.Keys)
			{
				ClientResponse response = r.Path("ws").Path("v1").Path("node").Path("containers")
					.Path(id).Accept(media).Get<ClientResponse>();
				NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
					);
				JSONObject json = response.GetEntity<JSONObject>();
				VerifyNodeContainerInfo(json.GetJSONObject("container"), nmContext.GetContainers(
					)[ConverterUtils.ToContainerId(id)]);
			}
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestSingleContainerInvalid()
		{
			WebResource r = Resource();
			Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application.Application
				 app = new MockApp(1);
			nmContext.GetApplications()[app.GetAppId()] = app;
			AddAppContainers(app);
			Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application.Application
				 app2 = new MockApp(2);
			nmContext.GetApplications()[app2.GetAppId()] = app2;
			AddAppContainers(app2);
			try
			{
				r.Path("ws").Path("v1").Path("node").Path("containers").Path("container_foo_1234"
					).Accept(MediaType.ApplicationJson).Get<JSONObject>();
				NUnit.Framework.Assert.Fail("should have thrown exception on invalid user query");
			}
			catch (UniformInterfaceException ue)
			{
				ClientResponse response = ue.GetResponse();
				NUnit.Framework.Assert.AreEqual(ClientResponse.Status.BadRequest, response.GetClientResponseStatus
					());
				NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
					);
				JSONObject msg = response.GetEntity<JSONObject>();
				JSONObject exception = msg.GetJSONObject("RemoteException");
				NUnit.Framework.Assert.AreEqual("incorrect number of elements", 3, exception.Length
					());
				string message = exception.GetString("message");
				string type = exception.GetString("exception");
				string classname = exception.GetString("javaClassName");
				WebServicesTestUtils.CheckStringMatch("exception message", "java.lang.Exception: invalid container id, container_foo_1234"
					, message);
				WebServicesTestUtils.CheckStringMatch("exception type", "BadRequestException", type
					);
				WebServicesTestUtils.CheckStringMatch("exception classname", "org.apache.hadoop.yarn.webapp.BadRequestException"
					, classname);
			}
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestSingleContainerInvalid2()
		{
			WebResource r = Resource();
			Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application.Application
				 app = new MockApp(1);
			nmContext.GetApplications()[app.GetAppId()] = app;
			AddAppContainers(app);
			Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application.Application
				 app2 = new MockApp(2);
			nmContext.GetApplications()[app2.GetAppId()] = app2;
			AddAppContainers(app2);
			try
			{
				r.Path("ws").Path("v1").Path("node").Path("containers").Path("container_1234_0001"
					).Accept(MediaType.ApplicationJson).Get<JSONObject>();
				NUnit.Framework.Assert.Fail("should have thrown exception on invalid user query");
			}
			catch (UniformInterfaceException ue)
			{
				ClientResponse response = ue.GetResponse();
				NUnit.Framework.Assert.AreEqual(ClientResponse.Status.BadRequest, response.GetClientResponseStatus
					());
				NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
					);
				JSONObject msg = response.GetEntity<JSONObject>();
				JSONObject exception = msg.GetJSONObject("RemoteException");
				NUnit.Framework.Assert.AreEqual("incorrect number of elements", 3, exception.Length
					());
				string message = exception.GetString("message");
				string type = exception.GetString("exception");
				string classname = exception.GetString("javaClassName");
				WebServicesTestUtils.CheckStringMatch("exception message", "java.lang.Exception: invalid container id, container_1234_0001"
					, message);
				WebServicesTestUtils.CheckStringMatch("exception type", "BadRequestException", type
					);
				WebServicesTestUtils.CheckStringMatch("exception classname", "org.apache.hadoop.yarn.webapp.BadRequestException"
					, classname);
			}
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestSingleContainerWrong()
		{
			WebResource r = Resource();
			Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application.Application
				 app = new MockApp(1);
			nmContext.GetApplications()[app.GetAppId()] = app;
			AddAppContainers(app);
			Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application.Application
				 app2 = new MockApp(2);
			nmContext.GetApplications()[app2.GetAppId()] = app2;
			AddAppContainers(app2);
			try
			{
				r.Path("ws").Path("v1").Path("node").Path("containers").Path("container_1234_0001_01_000005"
					).Accept(MediaType.ApplicationJson).Get<JSONObject>();
				NUnit.Framework.Assert.Fail("should have thrown exception on invalid user query");
			}
			catch (UniformInterfaceException ue)
			{
				ClientResponse response = ue.GetResponse();
				NUnit.Framework.Assert.AreEqual(ClientResponse.Status.NotFound, response.GetClientResponseStatus
					());
				NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
					);
				JSONObject msg = response.GetEntity<JSONObject>();
				JSONObject exception = msg.GetJSONObject("RemoteException");
				NUnit.Framework.Assert.AreEqual("incorrect number of elements", 3, exception.Length
					());
				string message = exception.GetString("message");
				string type = exception.GetString("exception");
				string classname = exception.GetString("javaClassName");
				WebServicesTestUtils.CheckStringMatch("exception message", "java.lang.Exception: container with id, container_1234_0001_01_000005, not found"
					, message);
				WebServicesTestUtils.CheckStringMatch("exception type", "NotFoundException", type
					);
				WebServicesTestUtils.CheckStringMatch("exception classname", "org.apache.hadoop.yarn.webapp.NotFoundException"
					, classname);
			}
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNodeSingleContainerXML()
		{
			WebResource r = Resource();
			Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application.Application
				 app = new MockApp(1);
			nmContext.GetApplications()[app.GetAppId()] = app;
			Dictionary<string, string> hash = AddAppContainers(app);
			Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application.Application
				 app2 = new MockApp(2);
			nmContext.GetApplications()[app2.GetAppId()] = app2;
			AddAppContainers(app2);
			foreach (string id in hash.Keys)
			{
				ClientResponse response = r.Path("ws").Path("v1").Path("node").Path("containers")
					.Path(id).Accept(MediaType.ApplicationXml).Get<ClientResponse>();
				NUnit.Framework.Assert.AreEqual(MediaType.ApplicationXmlType, response.GetType());
				string xml = response.GetEntity<string>();
				DocumentBuilderFactory dbf = DocumentBuilderFactory.NewInstance();
				DocumentBuilder db = dbf.NewDocumentBuilder();
				InputSource @is = new InputSource();
				@is.SetCharacterStream(new StringReader(xml));
				Document dom = db.Parse(@is);
				NodeList nodes = dom.GetElementsByTagName("container");
				NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, nodes.GetLength
					());
				VerifyContainersInfoXML(nodes, nmContext.GetContainers()[ConverterUtils.ToContainerId
					(id)]);
			}
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNodeContainerXML()
		{
			WebResource r = Resource();
			Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application.Application
				 app = new MockApp(1);
			nmContext.GetApplications()[app.GetAppId()] = app;
			AddAppContainers(app);
			Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application.Application
				 app2 = new MockApp(2);
			nmContext.GetApplications()[app2.GetAppId()] = app2;
			AddAppContainers(app2);
			ClientResponse response = r.Path("ws").Path("v1").Path("node").Path("containers")
				.Accept(MediaType.ApplicationXml).Get<ClientResponse>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationXmlType, response.GetType());
			string xml = response.GetEntity<string>();
			DocumentBuilderFactory dbf = DocumentBuilderFactory.NewInstance();
			DocumentBuilder db = dbf.NewDocumentBuilder();
			InputSource @is = new InputSource();
			@is.SetCharacterStream(new StringReader(xml));
			Document dom = db.Parse(@is);
			NodeList nodes = dom.GetElementsByTagName("container");
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 4, nodes.GetLength
				());
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		public virtual void VerifyContainersInfoXML(NodeList nodes, Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container
			 cont)
		{
			for (int i = 0; i < nodes.GetLength(); i++)
			{
				Element element = (Element)nodes.Item(i);
				VerifyNodeContainerInfoGeneric(cont, WebServicesTestUtils.GetXmlString(element, "id"
					), WebServicesTestUtils.GetXmlString(element, "state"), WebServicesTestUtils.GetXmlString
					(element, "user"), WebServicesTestUtils.GetXmlInt(element, "exitCode"), WebServicesTestUtils
					.GetXmlString(element, "diagnostics"), WebServicesTestUtils.GetXmlString(element
					, "nodeId"), WebServicesTestUtils.GetXmlInt(element, "totalMemoryNeededMB"), WebServicesTestUtils
					.GetXmlInt(element, "totalVCoresNeeded"), WebServicesTestUtils.GetXmlString(element
					, "containerLogsLink"));
			}
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		public virtual void VerifyNodeContainerInfo(JSONObject info, Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container
			 cont)
		{
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 9, info.Length());
			VerifyNodeContainerInfoGeneric(cont, info.GetString("id"), info.GetString("state"
				), info.GetString("user"), info.GetInt("exitCode"), info.GetString("diagnostics"
				), info.GetString("nodeId"), info.GetInt("totalMemoryNeededMB"), info.GetInt("totalVCoresNeeded"
				), info.GetString("containerLogsLink"));
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		public virtual void VerifyNodeContainerInfoGeneric(Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container
			 cont, string id, string state, string user, int exitCode, string diagnostics, string
			 nodeId, int totalMemoryNeededMB, int totalVCoresNeeded, string logsLink)
		{
			WebServicesTestUtils.CheckStringMatch("id", cont.GetContainerId().ToString(), id);
			WebServicesTestUtils.CheckStringMatch("state", cont.GetContainerState().ToString(
				), state);
			WebServicesTestUtils.CheckStringMatch("user", cont.GetUser().ToString(), user);
			NUnit.Framework.Assert.AreEqual("exitCode wrong", 0, exitCode);
			WebServicesTestUtils.CheckStringMatch("diagnostics", "testing", diagnostics);
			WebServicesTestUtils.CheckStringMatch("nodeId", nmContext.GetNodeId().ToString(), 
				nodeId);
			NUnit.Framework.Assert.AreEqual("totalMemoryNeededMB wrong", YarnConfiguration.DefaultRmSchedulerMinimumAllocationMb
				, totalMemoryNeededMB);
			NUnit.Framework.Assert.AreEqual("totalVCoresNeeded wrong", YarnConfiguration.DefaultRmSchedulerMinimumAllocationVcores
				, totalVCoresNeeded);
			string shortLink = StringHelper.Ujoin("containerlogs", cont.GetContainerId().ToString
				(), cont.GetUser());
			NUnit.Framework.Assert.IsTrue("containerLogsLink wrong", logsLink.Contains(shortLink
				));
		}
	}
}
