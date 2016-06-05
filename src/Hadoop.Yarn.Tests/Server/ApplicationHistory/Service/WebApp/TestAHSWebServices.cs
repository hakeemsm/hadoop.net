using System.Collections.Generic;
using Com.Google.Inject;
using Com.Google.Inject.Servlet;
using Com.Sun.Jersey.Api.Client;
using Com.Sun.Jersey.Guice.Spi.Container.Servlet;
using Com.Sun.Jersey.Test.Framework;
using Javax.Servlet;
using Javax.WS.RS.Core;
using NUnit.Framework;
using NUnit.Framework.Runners;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Security.Authentication.Server;
using Org.Apache.Hadoop.Yarn.Api;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Server.Applicationhistoryservice;
using Org.Apache.Hadoop.Yarn.Server.Security;
using Org.Apache.Hadoop.Yarn.Server.Timeline;
using Org.Apache.Hadoop.Yarn.Server.Timeline.Security;
using Org.Apache.Hadoop.Yarn.Webapp;
using Org.Apache.Hadoop.Yarn.Webapp.Util;
using Org.Codehaus.Jettison.Json;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Applicationhistoryservice.Webapp
{
	public class TestAHSWebServices : JerseyTestBase
	{
		private static ApplicationHistoryClientService historyClientService;

		private static readonly string[] Users = new string[] { "foo", "bar" };

		private const int MaxApps = 5;

		/// <exception cref="System.Exception"/>
		[BeforeClass]
		public static void SetupClass()
		{
			Configuration conf = new YarnConfiguration();
			TimelineStore store = TestApplicationHistoryManagerOnTimelineStore.CreateStore(MaxApps
				);
			TimelineACLsManager aclsManager = new TimelineACLsManager(conf);
			TimelineDataManager dataManager = new TimelineDataManager(store, aclsManager);
			conf.SetBoolean(YarnConfiguration.YarnAclEnable, true);
			conf.Set(YarnConfiguration.YarnAdminAcl, "foo");
			ApplicationACLsManager appAclsManager = new ApplicationACLsManager(conf);
			ApplicationHistoryManagerOnTimelineStore historyManager = new ApplicationHistoryManagerOnTimelineStore
				(dataManager, appAclsManager);
			historyManager.Init(conf);
			historyClientService = new _ApplicationHistoryClientService_98(historyManager);
			// Do Nothing
			historyClientService.Init(conf);
			historyClientService.Start();
		}

		private sealed class _ApplicationHistoryClientService_98 : ApplicationHistoryClientService
		{
			public _ApplicationHistoryClientService_98(ApplicationHistoryManager baseArg1)
				: base(baseArg1)
			{
			}

			/// <exception cref="System.Exception"/>
			protected override void ServiceStart()
			{
			}
		}

		/// <exception cref="System.Exception"/>
		[AfterClass]
		public static void TearDownClass()
		{
			if (historyClientService != null)
			{
				historyClientService.Stop();
			}
		}

		[Parameterized.Parameters]
		public static ICollection<object[]> Rounds()
		{
			return Arrays.AsList(new object[][] { new object[] { 0 }, new object[] { 1 } });
		}

		private sealed class _ServletModule_120 : ServletModule
		{
			public _ServletModule_120()
			{
			}

			protected override void ConfigureServlets()
			{
				this.Bind<JAXBContextResolver>();
				this.Bind<AHSWebServices>();
				this.Bind<GenericExceptionHandler>();
				this.Bind<ApplicationBaseProtocol>().ToInstance(Org.Apache.Hadoop.Yarn.Server.Applicationhistoryservice.Webapp.TestAHSWebServices
					.historyClientService);
				this.Serve("/*").With(typeof(GuiceContainer));
				this.Filter("/*").Through(typeof(TestAHSWebServices.TestSimpleAuthFilter));
			}
		}

		private Injector injector = Guice.CreateInjector(new _ServletModule_120());

		public class TestSimpleAuthFilter : AuthenticationFilter
		{
			/// <exception cref="Javax.Servlet.ServletException"/>
			protected override Properties GetConfiguration(string configPrefix, FilterConfig 
				filterConfig)
			{
				Properties properties = base.GetConfiguration(configPrefix, filterConfig);
				properties[AuthenticationFilter.AuthType] = "simple";
				properties[PseudoAuthenticationHandler.AnonymousAllowed] = "false";
				return properties;
			}
		}

		public class GuiceServletConfig : GuiceServletContextListener
		{
			protected override Injector GetInjector()
			{
				return this._enclosing.injector;
			}

			internal GuiceServletConfig(TestAHSWebServices _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly TestAHSWebServices _enclosing;
		}

		private int round;

		public TestAHSWebServices(int round)
			: base(new WebAppDescriptor.Builder("org.apache.hadoop.yarn.server.applicationhistoryservice.webapp"
				).ContextListenerClass(typeof(TestAHSWebServices.GuiceServletConfig)).FilterClass
				(typeof(GuiceFilter)).ContextPath("jersey-guice-filter").ServletPath("/").Build(
				))
		{
			this.round = round;
		}

		[NUnit.Framework.Test]
		public virtual void TestInvalidApp()
		{
			ApplicationId appId = ApplicationId.NewInstance(0, MaxApps + 1);
			WebResource r = Resource();
			ClientResponse response = r.Path("ws").Path("v1").Path("applicationhistory").Path
				("apps").Path(appId.ToString()).QueryParam("user.name", Users[round]).Accept(MediaType
				.ApplicationJson).Get<ClientResponse>();
			NUnit.Framework.Assert.AreEqual("404 not found expected", ClientResponse.Status.NotFound
				, response.GetClientResponseStatus());
		}

		[NUnit.Framework.Test]
		public virtual void TestInvalidAttempt()
		{
			ApplicationId appId = ApplicationId.NewInstance(0, 1);
			ApplicationAttemptId appAttemptId = ApplicationAttemptId.NewInstance(appId, MaxApps
				 + 1);
			WebResource r = Resource();
			ClientResponse response = r.Path("ws").Path("v1").Path("applicationhistory").Path
				("apps").Path(appId.ToString()).Path("appattempts").Path(appAttemptId.ToString()
				).QueryParam("user.name", Users[round]).Accept(MediaType.ApplicationJson).Get<ClientResponse
				>();
			if (round == 1)
			{
				NUnit.Framework.Assert.AreEqual(ClientResponse.Status.Forbidden, response.GetClientResponseStatus
					());
				return;
			}
			NUnit.Framework.Assert.AreEqual("404 not found expected", ClientResponse.Status.NotFound
				, response.GetClientResponseStatus());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestInvalidContainer()
		{
			ApplicationId appId = ApplicationId.NewInstance(0, 1);
			ApplicationAttemptId appAttemptId = ApplicationAttemptId.NewInstance(appId, 1);
			ContainerId containerId = ContainerId.NewContainerId(appAttemptId, MaxApps + 1);
			WebResource r = Resource();
			ClientResponse response = r.Path("ws").Path("v1").Path("applicationhistory").Path
				("apps").Path(appId.ToString()).Path("appattempts").Path(appAttemptId.ToString()
				).Path("containers").Path(containerId.ToString()).QueryParam("user.name", Users[
				round]).Accept(MediaType.ApplicationJson).Get<ClientResponse>();
			if (round == 1)
			{
				NUnit.Framework.Assert.AreEqual(ClientResponse.Status.Forbidden, response.GetClientResponseStatus
					());
				return;
			}
			NUnit.Framework.Assert.AreEqual("404 not found expected", ClientResponse.Status.NotFound
				, response.GetClientResponseStatus());
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
				responseStr = r.Path("ws").Path("v1").Path("applicationhistory").Path("bogus").QueryParam
					("user.name", Users[round]).Accept(MediaType.ApplicationJson).Get<string>();
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
				responseStr = r.QueryParam("user.name", Users[round]).Accept(MediaType.ApplicationJson
					).Get<string>();
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
				responseStr = r.Path("ws").Path("v1").Path("applicationhistory").QueryParam("user.name"
					, Users[round]).Accept(MediaType.TextPlain).Get<string>();
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

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAppsQuery()
		{
			WebResource r = Resource();
			ClientResponse response = r.Path("ws").Path("v1").Path("applicationhistory").Path
				("apps").QueryParam("state", YarnApplicationState.Finished.ToString()).QueryParam
				("user.name", Users[round]).Accept(MediaType.ApplicationJson).Get<ClientResponse
				>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			JSONObject json = response.GetEntity<JSONObject>();
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, json.Length());
			JSONObject apps = json.GetJSONObject("apps");
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, apps.Length());
			JSONArray array = apps.GetJSONArray("app");
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 5, array.Length()
				);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestSingleApp()
		{
			ApplicationId appId = ApplicationId.NewInstance(0, 1);
			WebResource r = Resource();
			ClientResponse response = r.Path("ws").Path("v1").Path("applicationhistory").Path
				("apps").Path(appId.ToString()).QueryParam("user.name", Users[round]).Accept(MediaType
				.ApplicationJson).Get<ClientResponse>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			JSONObject json = response.GetEntity<JSONObject>();
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, json.Length());
			JSONObject app = json.GetJSONObject("app");
			NUnit.Framework.Assert.AreEqual(appId.ToString(), app.GetString("appId"));
			NUnit.Framework.Assert.AreEqual("test app", app.Get("name"));
			NUnit.Framework.Assert.AreEqual(round == 0 ? "test diagnostics info" : string.Empty
				, app.Get("diagnosticsInfo"));
			NUnit.Framework.Assert.AreEqual("test queue", app.Get("queue"));
			NUnit.Framework.Assert.AreEqual("user1", app.Get("user"));
			NUnit.Framework.Assert.AreEqual("test app type", app.Get("type"));
			NUnit.Framework.Assert.AreEqual(FinalApplicationStatus.Undefined.ToString(), app.
				Get("finalAppStatus"));
			NUnit.Framework.Assert.AreEqual(YarnApplicationState.Finished.ToString(), app.Get
				("appState"));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestMultipleAttempts()
		{
			ApplicationId appId = ApplicationId.NewInstance(0, 1);
			WebResource r = Resource();
			ClientResponse response = r.Path("ws").Path("v1").Path("applicationhistory").Path
				("apps").Path(appId.ToString()).Path("appattempts").QueryParam("user.name", Users
				[round]).Accept(MediaType.ApplicationJson).Get<ClientResponse>();
			if (round == 1)
			{
				NUnit.Framework.Assert.AreEqual(ClientResponse.Status.Forbidden, response.GetClientResponseStatus
					());
				return;
			}
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			JSONObject json = response.GetEntity<JSONObject>();
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, json.Length());
			JSONObject appAttempts = json.GetJSONObject("appAttempts");
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, appAttempts.Length
				());
			JSONArray array = appAttempts.GetJSONArray("appAttempt");
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 5, array.Length()
				);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestSingleAttempt()
		{
			ApplicationId appId = ApplicationId.NewInstance(0, 1);
			ApplicationAttemptId appAttemptId = ApplicationAttemptId.NewInstance(appId, 1);
			WebResource r = Resource();
			ClientResponse response = r.Path("ws").Path("v1").Path("applicationhistory").Path
				("apps").Path(appId.ToString()).Path("appattempts").Path(appAttemptId.ToString()
				).QueryParam("user.name", Users[round]).Accept(MediaType.ApplicationJson).Get<ClientResponse
				>();
			if (round == 1)
			{
				NUnit.Framework.Assert.AreEqual(ClientResponse.Status.Forbidden, response.GetClientResponseStatus
					());
				return;
			}
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			JSONObject json = response.GetEntity<JSONObject>();
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, json.Length());
			JSONObject appAttempt = json.GetJSONObject("appAttempt");
			NUnit.Framework.Assert.AreEqual(appAttemptId.ToString(), appAttempt.GetString("appAttemptId"
				));
			NUnit.Framework.Assert.AreEqual("test host", appAttempt.GetString("host"));
			NUnit.Framework.Assert.AreEqual("test diagnostics info", appAttempt.GetString("diagnosticsInfo"
				));
			NUnit.Framework.Assert.AreEqual("test tracking url", appAttempt.GetString("trackingUrl"
				));
			NUnit.Framework.Assert.AreEqual(YarnApplicationAttemptState.Finished.ToString(), 
				appAttempt.Get("appAttemptState"));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestMultipleContainers()
		{
			ApplicationId appId = ApplicationId.NewInstance(0, 1);
			ApplicationAttemptId appAttemptId = ApplicationAttemptId.NewInstance(appId, 1);
			WebResource r = Resource();
			ClientResponse response = r.Path("ws").Path("v1").Path("applicationhistory").Path
				("apps").Path(appId.ToString()).Path("appattempts").Path(appAttemptId.ToString()
				).Path("containers").QueryParam("user.name", Users[round]).Accept(MediaType.ApplicationJson
				).Get<ClientResponse>();
			if (round == 1)
			{
				NUnit.Framework.Assert.AreEqual(ClientResponse.Status.Forbidden, response.GetClientResponseStatus
					());
				return;
			}
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			JSONObject json = response.GetEntity<JSONObject>();
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, json.Length());
			JSONObject containers = json.GetJSONObject("containers");
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, containers.Length
				());
			JSONArray array = containers.GetJSONArray("container");
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 5, array.Length()
				);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestSingleContainer()
		{
			ApplicationId appId = ApplicationId.NewInstance(0, 1);
			ApplicationAttemptId appAttemptId = ApplicationAttemptId.NewInstance(appId, 1);
			ContainerId containerId = ContainerId.NewContainerId(appAttemptId, 1);
			WebResource r = Resource();
			ClientResponse response = r.Path("ws").Path("v1").Path("applicationhistory").Path
				("apps").Path(appId.ToString()).Path("appattempts").Path(appAttemptId.ToString()
				).Path("containers").Path(containerId.ToString()).QueryParam("user.name", Users[
				round]).Accept(MediaType.ApplicationJson).Get<ClientResponse>();
			if (round == 1)
			{
				NUnit.Framework.Assert.AreEqual(ClientResponse.Status.Forbidden, response.GetClientResponseStatus
					());
				return;
			}
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			JSONObject json = response.GetEntity<JSONObject>();
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, json.Length());
			JSONObject container = json.GetJSONObject("container");
			NUnit.Framework.Assert.AreEqual(containerId.ToString(), container.GetString("containerId"
				));
			NUnit.Framework.Assert.AreEqual("test diagnostics info", container.GetString("diagnosticsInfo"
				));
			NUnit.Framework.Assert.AreEqual("-1", container.GetString("allocatedMB"));
			NUnit.Framework.Assert.AreEqual("-1", container.GetString("allocatedVCores"));
			NUnit.Framework.Assert.AreEqual(NodeId.NewInstance("test host", 100).ToString(), 
				container.GetString("assignedNodeId"));
			NUnit.Framework.Assert.AreEqual("-1", container.GetString("priority"));
			Configuration conf = new YarnConfiguration();
			NUnit.Framework.Assert.AreEqual(WebAppUtils.GetHttpSchemePrefix(conf) + WebAppUtils
				.GetAHSWebAppURLWithoutScheme(conf) + "/applicationhistory/logs/test host:100/container_0_0001_01_000001/"
				 + "container_0_0001_01_000001/user1", container.GetString("logUrl"));
			NUnit.Framework.Assert.AreEqual(ContainerState.Complete.ToString(), container.GetString
				("containerState"));
		}
	}
}
