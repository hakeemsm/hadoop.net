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
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application;
using Org.Apache.Hadoop.Yarn.Server.Security;
using Org.Apache.Hadoop.Yarn.Server.Utils;
using Org.Apache.Hadoop.Yarn.Webapp;
using Org.Codehaus.Jettison.Json;
using Org.W3c.Dom;
using Org.Xml.Sax;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Webapp
{
	public class TestNMWebServicesApps : JerseyTestBase
	{
		private static Context nmContext;

		private static ResourceView resourceView;

		private static ApplicationACLsManager aclsManager;

		private static LocalDirsHandlerService dirsHandler;

		private static WebApp nmWebApp;

		private static Configuration conf = new Configuration();

		private static readonly FilePath testRootDir = new FilePath("target", typeof(Org.Apache.Hadoop.Yarn.Server.Nodemanager.Webapp.TestNMWebServicesApps
			).Name);

		private static FilePath testLogDir = new FilePath("target", typeof(Org.Apache.Hadoop.Yarn.Server.Nodemanager.Webapp.TestNMWebServicesApps
			).Name + "LogDir");

		private sealed class _ServletModule_93 : ServletModule
		{
			public _ServletModule_93()
			{
			}

			protected override void ConfigureServlets()
			{
				Org.Apache.Hadoop.Yarn.Server.Nodemanager.Webapp.TestNMWebServicesApps.conf.Set(YarnConfiguration
					.NmLocalDirs, Org.Apache.Hadoop.Yarn.Server.Nodemanager.Webapp.TestNMWebServicesApps
					.testRootDir.GetAbsolutePath());
				Org.Apache.Hadoop.Yarn.Server.Nodemanager.Webapp.TestNMWebServicesApps.conf.Set(YarnConfiguration
					.NmLogDirs, Org.Apache.Hadoop.Yarn.Server.Nodemanager.Webapp.TestNMWebServicesApps
					.testLogDir.GetAbsolutePath());
				NodeHealthCheckerService healthChecker = new NodeHealthCheckerService();
				healthChecker.Init(Org.Apache.Hadoop.Yarn.Server.Nodemanager.Webapp.TestNMWebServicesApps
					.conf);
				Org.Apache.Hadoop.Yarn.Server.Nodemanager.Webapp.TestNMWebServicesApps.dirsHandler
					 = healthChecker.GetDiskHandler();
				Org.Apache.Hadoop.Yarn.Server.Nodemanager.Webapp.TestNMWebServicesApps.aclsManager
					 = new ApplicationACLsManager(Org.Apache.Hadoop.Yarn.Server.Nodemanager.Webapp.TestNMWebServicesApps
					.conf);
				Org.Apache.Hadoop.Yarn.Server.Nodemanager.Webapp.TestNMWebServicesApps.nmContext 
					= new NodeManager.NMContext(null, null, Org.Apache.Hadoop.Yarn.Server.Nodemanager.Webapp.TestNMWebServicesApps
					.dirsHandler, Org.Apache.Hadoop.Yarn.Server.Nodemanager.Webapp.TestNMWebServicesApps
					.aclsManager, null);
				NodeId nodeId = NodeId.NewInstance("testhost.foo.com", 9999);
				((NodeManager.NMContext)Org.Apache.Hadoop.Yarn.Server.Nodemanager.Webapp.TestNMWebServicesApps
					.nmContext).SetNodeId(nodeId);
				Org.Apache.Hadoop.Yarn.Server.Nodemanager.Webapp.TestNMWebServicesApps.resourceView
					 = new _ResourceView_106();
				// 15.5G in bytes
				// 16G in bytes
				Org.Apache.Hadoop.Yarn.Server.Nodemanager.Webapp.TestNMWebServicesApps.nmWebApp =
					 new WebServer.NMWebApp(Org.Apache.Hadoop.Yarn.Server.Nodemanager.Webapp.TestNMWebServicesApps
					.resourceView, Org.Apache.Hadoop.Yarn.Server.Nodemanager.Webapp.TestNMWebServicesApps
					.aclsManager, Org.Apache.Hadoop.Yarn.Server.Nodemanager.Webapp.TestNMWebServicesApps
					.dirsHandler);
				this.Bind<JAXBContextResolver>();
				this.Bind<NMWebServices>();
				this.Bind<GenericExceptionHandler>();
				this.Bind<Context>().ToInstance(Org.Apache.Hadoop.Yarn.Server.Nodemanager.Webapp.TestNMWebServicesApps
					.nmContext);
				this.Bind<WebApp>().ToInstance(Org.Apache.Hadoop.Yarn.Server.Nodemanager.Webapp.TestNMWebServicesApps
					.nmWebApp);
				this.Bind<ResourceView>().ToInstance(Org.Apache.Hadoop.Yarn.Server.Nodemanager.Webapp.TestNMWebServicesApps
					.resourceView);
				this.Bind<ApplicationACLsManager>().ToInstance(Org.Apache.Hadoop.Yarn.Server.Nodemanager.Webapp.TestNMWebServicesApps
					.aclsManager);
				this.Bind<LocalDirsHandlerService>().ToInstance(Org.Apache.Hadoop.Yarn.Server.Nodemanager.Webapp.TestNMWebServicesApps
					.dirsHandler);
				this.Serve("/*").With(typeof(GuiceContainer));
			}

			private sealed class _ResourceView_106 : ResourceView
			{
				public _ResourceView_106()
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

		private Injector injector = Guice.CreateInjector(new _ServletModule_93());

		public class GuiceServletConfig : GuiceServletContextListener
		{
			protected override Injector GetInjector()
			{
				return this._enclosing.injector;
			}

			internal GuiceServletConfig(TestNMWebServicesApps _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly TestNMWebServicesApps _enclosing;
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

		public TestNMWebServicesApps()
			: base(new WebAppDescriptor.Builder("org.apache.hadoop.yarn.server.nodemanager.webapp"
				).ContextListenerClass(typeof(TestNMWebServicesApps.GuiceServletConfig)).FilterClass
				(typeof(GuiceFilter)).ContextPath("jersey-guice-filter").ServletPath("/").Build(
				))
		{
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNodeAppsNone()
		{
			WebResource r = Resource();
			ClientResponse response = r.Path("ws").Path("v1").Path("node").Path("apps").Accept
				(MediaType.ApplicationJson).Get<ClientResponse>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			JSONObject json = response.GetEntity<JSONObject>();
			NUnit.Framework.Assert.AreEqual("apps isn't NULL", JSONObject.Null, json.Get("apps"
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
		public virtual void TestNodeApps()
		{
			TestNodeHelper("apps", MediaType.ApplicationJson);
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNodeAppsSlash()
		{
			TestNodeHelper("apps/", MediaType.ApplicationJson);
		}

		// make sure default is json output
		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNodeAppsDefault()
		{
			TestNodeHelper("apps/", string.Empty);
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		public virtual void TestNodeHelper(string path, string media)
		{
			WebResource r = Resource();
			Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application.Application
				 app = new MockApp(1);
			nmContext.GetApplications()[app.GetAppId()] = app;
			Dictionary<string, string> hash = AddAppContainers(app);
			Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application.Application
				 app2 = new MockApp(2);
			nmContext.GetApplications()[app2.GetAppId()] = app2;
			Dictionary<string, string> hash2 = AddAppContainers(app2);
			ClientResponse response = r.Path("ws").Path("v1").Path("node").Path(path).Accept(
				media).Get<ClientResponse>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			JSONObject json = response.GetEntity<JSONObject>();
			JSONObject info = json.GetJSONObject("apps");
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, info.Length());
			JSONArray appInfo = info.GetJSONArray("app");
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 2, appInfo.Length
				());
			string id = appInfo.GetJSONObject(0).GetString("id");
			if (id.Matches(app.GetAppId().ToString()))
			{
				VerifyNodeAppInfo(appInfo.GetJSONObject(0), app, hash);
				VerifyNodeAppInfo(appInfo.GetJSONObject(1), app2, hash2);
			}
			else
			{
				VerifyNodeAppInfo(appInfo.GetJSONObject(0), app2, hash2);
				VerifyNodeAppInfo(appInfo.GetJSONObject(1), app, hash);
			}
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNodeAppsUser()
		{
			WebResource r = Resource();
			Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application.Application
				 app = new MockApp(1);
			nmContext.GetApplications()[app.GetAppId()] = app;
			Dictionary<string, string> hash = AddAppContainers(app);
			Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application.Application
				 app2 = new MockApp("foo", 1234, 2);
			nmContext.GetApplications()[app2.GetAppId()] = app2;
			AddAppContainers(app2);
			ClientResponse response = r.Path("ws").Path("v1").Path("node").Path("apps").QueryParam
				("user", "mockUser").Accept(MediaType.ApplicationJson).Get<ClientResponse>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			JSONObject json = response.GetEntity<JSONObject>();
			JSONObject info = json.GetJSONObject("apps");
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, info.Length());
			JSONArray appInfo = info.GetJSONArray("app");
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, appInfo.Length
				());
			VerifyNodeAppInfo(appInfo.GetJSONObject(0), app, hash);
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNodeAppsUserNone()
		{
			WebResource r = Resource();
			Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application.Application
				 app = new MockApp(1);
			nmContext.GetApplications()[app.GetAppId()] = app;
			AddAppContainers(app);
			Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application.Application
				 app2 = new MockApp("foo", 1234, 2);
			nmContext.GetApplications()[app2.GetAppId()] = app2;
			AddAppContainers(app2);
			ClientResponse response = r.Path("ws").Path("v1").Path("node").Path("apps").QueryParam
				("user", "george").Accept(MediaType.ApplicationJson).Get<ClientResponse>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			JSONObject json = response.GetEntity<JSONObject>();
			NUnit.Framework.Assert.AreEqual("apps is not null", JSONObject.Null, json.Get("apps"
				));
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNodeAppsUserEmpty()
		{
			WebResource r = Resource();
			Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application.Application
				 app = new MockApp(1);
			nmContext.GetApplications()[app.GetAppId()] = app;
			AddAppContainers(app);
			Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application.Application
				 app2 = new MockApp("foo", 1234, 2);
			nmContext.GetApplications()[app2.GetAppId()] = app2;
			AddAppContainers(app2);
			try
			{
				r.Path("ws").Path("v1").Path("node").Path("apps").QueryParam("user", string.Empty
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
				WebServicesTestUtils.CheckStringMatch("exception message", "java.lang.Exception: Error: You must specify a non-empty string for the user"
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
		public virtual void TestNodeAppsState()
		{
			WebResource r = Resource();
			Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application.Application
				 app = new MockApp(1);
			nmContext.GetApplications()[app.GetAppId()] = app;
			AddAppContainers(app);
			MockApp app2 = new MockApp("foo", 1234, 2);
			nmContext.GetApplications()[app2.GetAppId()] = app2;
			Dictionary<string, string> hash2 = AddAppContainers(app2);
			app2.SetState(ApplicationState.Running);
			ClientResponse response = r.Path("ws").Path("v1").Path("node").Path("apps").QueryParam
				("state", ApplicationState.Running.ToString()).Accept(MediaType.ApplicationJson)
				.Get<ClientResponse>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			JSONObject json = response.GetEntity<JSONObject>();
			JSONObject info = json.GetJSONObject("apps");
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, info.Length());
			JSONArray appInfo = info.GetJSONArray("app");
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, appInfo.Length
				());
			VerifyNodeAppInfo(appInfo.GetJSONObject(0), app2, hash2);
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNodeAppsStateNone()
		{
			WebResource r = Resource();
			Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application.Application
				 app = new MockApp(1);
			nmContext.GetApplications()[app.GetAppId()] = app;
			AddAppContainers(app);
			Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application.Application
				 app2 = new MockApp("foo", 1234, 2);
			nmContext.GetApplications()[app2.GetAppId()] = app2;
			AddAppContainers(app2);
			ClientResponse response = r.Path("ws").Path("v1").Path("node").Path("apps").QueryParam
				("state", ApplicationState.Initing.ToString()).Accept(MediaType.ApplicationJson)
				.Get<ClientResponse>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			JSONObject json = response.GetEntity<JSONObject>();
			NUnit.Framework.Assert.AreEqual("apps is not null", JSONObject.Null, json.Get("apps"
				));
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNodeAppsStateInvalid()
		{
			WebResource r = Resource();
			Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application.Application
				 app = new MockApp(1);
			nmContext.GetApplications()[app.GetAppId()] = app;
			AddAppContainers(app);
			Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application.Application
				 app2 = new MockApp("foo", 1234, 2);
			nmContext.GetApplications()[app2.GetAppId()] = app2;
			AddAppContainers(app2);
			try
			{
				r.Path("ws").Path("v1").Path("node").Path("apps").QueryParam("state", "FOO_STATE"
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
				VerifyStateInvalidException(message, type, classname);
			}
		}

		// verify the exception object default format is JSON
		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNodeAppsStateInvalidDefault()
		{
			WebResource r = Resource();
			Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application.Application
				 app = new MockApp(1);
			nmContext.GetApplications()[app.GetAppId()] = app;
			AddAppContainers(app);
			Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application.Application
				 app2 = new MockApp("foo", 1234, 2);
			nmContext.GetApplications()[app2.GetAppId()] = app2;
			AddAppContainers(app2);
			try
			{
				r.Path("ws").Path("v1").Path("node").Path("apps").QueryParam("state", "FOO_STATE"
					).Get<JSONObject>();
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
				VerifyStateInvalidException(message, type, classname);
			}
		}

		// test that the exception output also returns XML
		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNodeAppsStateInvalidXML()
		{
			WebResource r = Resource();
			Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application.Application
				 app = new MockApp(1);
			nmContext.GetApplications()[app.GetAppId()] = app;
			AddAppContainers(app);
			Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application.Application
				 app2 = new MockApp("foo", 1234, 2);
			nmContext.GetApplications()[app2.GetAppId()] = app2;
			AddAppContainers(app2);
			try
			{
				r.Path("ws").Path("v1").Path("node").Path("apps").QueryParam("state", "FOO_STATE"
					).Accept(MediaType.ApplicationXml).Get<JSONObject>();
				NUnit.Framework.Assert.Fail("should have thrown exception on invalid user query");
			}
			catch (UniformInterfaceException ue)
			{
				ClientResponse response = ue.GetResponse();
				NUnit.Framework.Assert.AreEqual(ClientResponse.Status.BadRequest, response.GetClientResponseStatus
					());
				NUnit.Framework.Assert.AreEqual(MediaType.ApplicationXmlType, response.GetType());
				string msg = response.GetEntity<string>();
				DocumentBuilderFactory dbf = DocumentBuilderFactory.NewInstance();
				DocumentBuilder db = dbf.NewDocumentBuilder();
				InputSource @is = new InputSource();
				@is.SetCharacterStream(new StringReader(msg));
				Document dom = db.Parse(@is);
				NodeList nodes = dom.GetElementsByTagName("RemoteException");
				Element element = (Element)nodes.Item(0);
				string message = WebServicesTestUtils.GetXmlString(element, "message");
				string type = WebServicesTestUtils.GetXmlString(element, "exception");
				string classname = WebServicesTestUtils.GetXmlString(element, "javaClassName");
				VerifyStateInvalidException(message, type, classname);
			}
		}

		private void VerifyStateInvalidException(string message, string type, string classname
			)
		{
			WebServicesTestUtils.CheckStringContains("exception message", "org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationState.FOO_STATE"
				, message);
			WebServicesTestUtils.CheckStringMatch("exception type", "IllegalArgumentException"
				, type);
			WebServicesTestUtils.CheckStringMatch("exception classname", "java.lang.IllegalArgumentException"
				, classname);
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNodeSingleApps()
		{
			TestNodeSingleAppHelper(MediaType.ApplicationJson);
		}

		// make sure default is json output
		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNodeSingleAppsDefault()
		{
			TestNodeSingleAppHelper(string.Empty);
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		public virtual void TestNodeSingleAppHelper(string media)
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
			ClientResponse response = r.Path("ws").Path("v1").Path("node").Path("apps").Path(
				app.GetAppId().ToString()).Accept(media).Get<ClientResponse>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			JSONObject json = response.GetEntity<JSONObject>();
			VerifyNodeAppInfo(json.GetJSONObject("app"), app, hash);
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNodeSingleAppsSlash()
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
			ClientResponse response = r.Path("ws").Path("v1").Path("node").Path("apps").Path(
				app.GetAppId().ToString() + "/").Accept(MediaType.ApplicationJson).Get<ClientResponse
				>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			JSONObject json = response.GetEntity<JSONObject>();
			VerifyNodeAppInfo(json.GetJSONObject("app"), app, hash);
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNodeSingleAppsInvalid()
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
				r.Path("ws").Path("v1").Path("node").Path("apps").Path("app_foo_0000").Accept(MediaType
					.ApplicationJson).Get<JSONObject>();
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
				WebServicesTestUtils.CheckStringMatch("exception message", "For input string: \"foo\""
					, message);
				WebServicesTestUtils.CheckStringMatch("exception type", "NumberFormatException", 
					type);
				WebServicesTestUtils.CheckStringMatch("exception classname", "java.lang.NumberFormatException"
					, classname);
			}
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNodeSingleAppsMissing()
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
				r.Path("ws").Path("v1").Path("node").Path("apps").Path("application_1234_0009").Accept
					(MediaType.ApplicationJson).Get<JSONObject>();
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
				WebServicesTestUtils.CheckStringMatch("exception message", "java.lang.Exception: app with id application_1234_0009 not found"
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
		public virtual void TestNodeAppsXML()
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
			ClientResponse response = r.Path("ws").Path("v1").Path("node").Path("apps").Accept
				(MediaType.ApplicationXml).Get<ClientResponse>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationXmlType, response.GetType());
			string xml = response.GetEntity<string>();
			DocumentBuilderFactory dbf = DocumentBuilderFactory.NewInstance();
			DocumentBuilder db = dbf.NewDocumentBuilder();
			InputSource @is = new InputSource();
			@is.SetCharacterStream(new StringReader(xml));
			Document dom = db.Parse(@is);
			NodeList nodes = dom.GetElementsByTagName("app");
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 2, nodes.GetLength
				());
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNodeSingleAppsXML()
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
			ClientResponse response = r.Path("ws").Path("v1").Path("node").Path("apps").Path(
				app.GetAppId().ToString() + "/").Accept(MediaType.ApplicationXml).Get<ClientResponse
				>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationXmlType, response.GetType());
			string xml = response.GetEntity<string>();
			DocumentBuilderFactory dbf = DocumentBuilderFactory.NewInstance();
			DocumentBuilder db = dbf.NewDocumentBuilder();
			InputSource @is = new InputSource();
			@is.SetCharacterStream(new StringReader(xml));
			Document dom = db.Parse(@is);
			NodeList nodes = dom.GetElementsByTagName("app");
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, nodes.GetLength
				());
			VerifyNodeAppInfoXML(nodes, app, hash);
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		public virtual void VerifyNodeAppInfoXML(NodeList nodes, Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application.Application
			 app, Dictionary<string, string> hash)
		{
			for (int i = 0; i < nodes.GetLength(); i++)
			{
				Element element = (Element)nodes.Item(i);
				VerifyNodeAppInfoGeneric(app, WebServicesTestUtils.GetXmlString(element, "id"), WebServicesTestUtils
					.GetXmlString(element, "state"), WebServicesTestUtils.GetXmlString(element, "user"
					));
				NodeList ids = element.GetElementsByTagName("containerids");
				for (int j = 0; j < ids.GetLength(); j++)
				{
					Element line = (Element)ids.Item(j);
					Node first = line.GetFirstChild();
					string val = first.GetNodeValue();
					NUnit.Framework.Assert.AreEqual("extra containerid: " + val, val, Sharpen.Collections.Remove
						(hash, val));
				}
				NUnit.Framework.Assert.IsTrue("missing containerids", hash.IsEmpty());
			}
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		public virtual void VerifyNodeAppInfo(JSONObject info, Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application.Application
			 app, Dictionary<string, string> hash)
		{
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 4, info.Length());
			VerifyNodeAppInfoGeneric(app, info.GetString("id"), info.GetString("state"), info
				.GetString("user"));
			JSONArray containerids = info.GetJSONArray("containerids");
			for (int i = 0; i < containerids.Length(); i++)
			{
				string id = containerids.GetString(i);
				NUnit.Framework.Assert.AreEqual("extra containerid: " + id, id, Sharpen.Collections.Remove
					(hash, id));
			}
			NUnit.Framework.Assert.IsTrue("missing containerids", hash.IsEmpty());
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		public virtual void VerifyNodeAppInfoGeneric(Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application.Application
			 app, string id, string state, string user)
		{
			WebServicesTestUtils.CheckStringMatch("id", app.GetAppId().ToString(), id);
			WebServicesTestUtils.CheckStringMatch("state", app.GetApplicationState().ToString
				(), state);
			WebServicesTestUtils.CheckStringMatch("user", app.GetUser().ToString(), user);
		}
	}
}
