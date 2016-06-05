using System.Collections.Generic;
using System.IO;
using Com.Google.Inject;
using Com.Google.Inject.Servlet;
using Com.Sun.Jersey.Api.Client;
using Com.Sun.Jersey.Api.Client.Config;
using Com.Sun.Jersey.Api.Client.Filter;
using Com.Sun.Jersey.Api.Json;
using Com.Sun.Jersey.Guice.Spi.Container.Servlet;
using Com.Sun.Jersey.Test.Framework;
using Javax.Servlet;
using Javax.WS.RS.Core;
using Javax.Xml.Parsers;
using NUnit.Framework;
using NUnit.Framework.Runners;
using Org.Apache.Commons.Codec.Binary;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Authentication.Server;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Capacity;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fair;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Webapp.Dao;
using Org.Apache.Hadoop.Yarn.Util;
using Org.Apache.Hadoop.Yarn.Webapp;
using Org.Codehaus.Jettison.Json;
using Org.W3c.Dom;
using Org.Xml.Sax;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Webapp
{
	public class TestRMWebServicesAppsModification : JerseyTestBase
	{
		private static MockRM rm;

		private const int ContainerMb = 1024;

		private static Injector injector;

		private string webserviceUserName = "testuser";

		private bool setAuthFilter = false;

		private static readonly string TestDir = new FilePath(Runtime.GetProperty("test.build.data"
			, "/tmp")).GetAbsolutePath();

		private static readonly string FsAllocFile = new FilePath(TestDir, "test-fs-queues.xml"
			).GetAbsolutePath();

		public class GuiceServletConfig : GuiceServletContextListener
		{
			protected override Injector GetInjector()
			{
				return injector;
			}
		}

		public class TestRMCustomAuthFilter : AuthenticationFilter
		{
			/*
			* Helper class to allow testing of RM web services which require
			* authorization Add this class as a filter in the Guice injector for the
			* MockRM
			*/
			/// <exception cref="Javax.Servlet.ServletException"/>
			protected override Properties GetConfiguration(string configPrefix, FilterConfig 
				filterConfig)
			{
				Properties props = new Properties();
				Enumeration<object> names = filterConfig.GetInitParameterNames();
				while (names.MoveNext())
				{
					string name = (string)names.Current;
					if (name.StartsWith(configPrefix))
					{
						string value = filterConfig.GetInitParameter(name);
						props[Sharpen.Runtime.Substring(name, configPrefix.Length)] = value;
					}
				}
				props[AuthenticationFilter.AuthType] = "simple";
				props[PseudoAuthenticationHandler.AnonymousAllowed] = "false";
				return props;
			}
		}

		private abstract class TestServletModule : ServletModule
		{
			public Configuration conf = new Configuration();

			public abstract void ConfigureScheduler();

			protected override void ConfigureServlets()
			{
				this.ConfigureScheduler();
				this.Bind<JAXBContextResolver>();
				this.Bind<RMWebServices>();
				this.Bind<GenericExceptionHandler>();
				this.conf.SetInt(YarnConfiguration.RmAmMaxAttempts, YarnConfiguration.DefaultRmAmMaxAttempts
					);
				TestRMWebServicesAppsModification.rm = new MockRM(this.conf);
				this.Bind<ResourceManager>().ToInstance(TestRMWebServicesAppsModification.rm);
				if (this._enclosing.setAuthFilter)
				{
					this.Filter("/*").Through(typeof(TestRMWebServicesAppsModification.TestRMCustomAuthFilter
						));
				}
				this.Serve("/*").With(typeof(GuiceContainer));
			}

			internal TestServletModule(TestRMWebServicesAppsModification _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly TestRMWebServicesAppsModification _enclosing;
		}

		private class CapTestServletModule : TestRMWebServicesAppsModification.TestServletModule
		{
			public override void ConfigureScheduler()
			{
				this.conf.Set("yarn.resourcemanager.scheduler.class", typeof(CapacityScheduler).FullName
					);
			}

			internal CapTestServletModule(TestRMWebServicesAppsModification _enclosing)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly TestRMWebServicesAppsModification _enclosing;
		}

		private class FairTestServletModule : TestRMWebServicesAppsModification.TestServletModule
		{
			public override void ConfigureScheduler()
			{
				try
				{
					PrintWriter @out = new PrintWriter(new FileWriter(TestRMWebServicesAppsModification
						.FsAllocFile));
					@out.WriteLine("<?xml version=\"1.0\"?>");
					@out.WriteLine("<allocations>");
					@out.WriteLine("<queue name=\"root\">");
					@out.WriteLine("  <aclAdministerApps>someuser </aclAdministerApps>");
					@out.WriteLine("  <queue name=\"default\">");
					@out.WriteLine("    <aclAdministerApps>someuser </aclAdministerApps>");
					@out.WriteLine("  </queue>");
					@out.WriteLine("  <queue name=\"test\">");
					@out.WriteLine("    <aclAdministerApps>someuser </aclAdministerApps>");
					@out.WriteLine("  </queue>");
					@out.WriteLine("</queue>");
					@out.WriteLine("</allocations>");
					@out.Close();
				}
				catch (IOException)
				{
				}
				this.conf.Set(FairSchedulerConfiguration.AllocationFile, TestRMWebServicesAppsModification
					.FsAllocFile);
				this.conf.Set("yarn.resourcemanager.scheduler.class", typeof(FairScheduler).FullName
					);
			}

			internal FairTestServletModule(TestRMWebServicesAppsModification _enclosing)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly TestRMWebServicesAppsModification _enclosing;
		}

		private Injector GetNoAuthInjectorCap()
		{
			return Com.Google.Inject.Guice.CreateInjector(new _CapTestServletModule_218(this)
				);
		}

		private sealed class _CapTestServletModule_218 : TestRMWebServicesAppsModification.CapTestServletModule
		{
			public _CapTestServletModule_218(TestRMWebServicesAppsModification _enclosing)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
			}

			protected override void ConfigureServlets()
			{
				this._enclosing.setAuthFilter = false;
				base.ConfigureServlets();
			}

			private readonly TestRMWebServicesAppsModification _enclosing;
		}

		private Injector GetSimpleAuthInjectorCap()
		{
			return Com.Google.Inject.Guice.CreateInjector(new _CapTestServletModule_228(this)
				);
		}

		private sealed class _CapTestServletModule_228 : TestRMWebServicesAppsModification.CapTestServletModule
		{
			public _CapTestServletModule_228(TestRMWebServicesAppsModification _enclosing)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
			}

			protected override void ConfigureServlets()
			{
				this._enclosing.setAuthFilter = true;
				this.conf.SetBoolean(YarnConfiguration.YarnAclEnable, true);
				// set the admin acls otherwise all users are considered admins
				// and we can't test authorization
				this.conf.SetStrings(YarnConfiguration.YarnAdminAcl, "testuser1");
				base.ConfigureServlets();
			}

			private readonly TestRMWebServicesAppsModification _enclosing;
		}

		private Injector GetNoAuthInjectorFair()
		{
			return Com.Google.Inject.Guice.CreateInjector(new _FairTestServletModule_242(this
				));
		}

		private sealed class _FairTestServletModule_242 : TestRMWebServicesAppsModification.FairTestServletModule
		{
			public _FairTestServletModule_242(TestRMWebServicesAppsModification _enclosing)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
			}

			protected override void ConfigureServlets()
			{
				this._enclosing.setAuthFilter = false;
				base.ConfigureServlets();
			}

			private readonly TestRMWebServicesAppsModification _enclosing;
		}

		private Injector GetSimpleAuthInjectorFair()
		{
			return Com.Google.Inject.Guice.CreateInjector(new _FairTestServletModule_252(this
				));
		}

		private sealed class _FairTestServletModule_252 : TestRMWebServicesAppsModification.FairTestServletModule
		{
			public _FairTestServletModule_252(TestRMWebServicesAppsModification _enclosing)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
			}

			protected override void ConfigureServlets()
			{
				this._enclosing.setAuthFilter = true;
				this.conf.SetBoolean(YarnConfiguration.YarnAclEnable, true);
				// set the admin acls otherwise all users are considered admins
				// and we can't test authorization
				this.conf.SetStrings(YarnConfiguration.YarnAdminAcl, "testuser1");
				base.ConfigureServlets();
			}

			private readonly TestRMWebServicesAppsModification _enclosing;
		}

		[Parameterized.Parameters]
		public static ICollection<object[]> GuiceConfigs()
		{
			return Arrays.AsList(new object[][] { new object[] { 0 }, new object[] { 1 }, new 
				object[] { 2 }, new object[] { 3 } });
		}

		/// <exception cref="System.Exception"/>
		[SetUp]
		public override void SetUp()
		{
			base.SetUp();
		}

		public TestRMWebServicesAppsModification(int run)
			: base(new WebAppDescriptor.Builder("org.apache.hadoop.yarn.server.resourcemanager.webapp"
				).ContextListenerClass(typeof(TestRMWebServicesAppsModification.GuiceServletConfig
				)).FilterClass(typeof(GuiceFilter)).ClientConfig(new DefaultClientConfig(typeof(
				JAXBContextResolver))).ContextPath("jersey-guice-filter").ServletPath("/").Build
				())
		{
			switch (run)
			{
				case 0:
				default:
				{
					// No Auth Capacity Scheduler
					injector = GetNoAuthInjectorCap();
					break;
				}

				case 1:
				{
					// Simple Auth Capacity Scheduler
					injector = GetSimpleAuthInjectorCap();
					break;
				}

				case 2:
				{
					// No Auth Fair Scheduler
					injector = GetNoAuthInjectorFair();
					break;
				}

				case 3:
				{
					// Simple Auth Fair Scheduler
					injector = GetSimpleAuthInjectorFair();
					break;
				}
			}
		}

		private bool IsAuthenticationEnabled()
		{
			return setAuthFilter;
		}

		private WebResource ConstructWebResource(WebResource r, params string[] paths)
		{
			WebResource rt = r;
			foreach (string path in paths)
			{
				rt = rt.Path(path);
			}
			if (IsAuthenticationEnabled())
			{
				rt = rt.QueryParam("user.name", webserviceUserName);
			}
			return rt;
		}

		private WebResource ConstructWebResource(params string[] paths)
		{
			WebResource r = Resource();
			WebResource ws = r.Path("ws").Path("v1").Path("cluster");
			return this.ConstructWebResource(ws, paths);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestSingleAppState()
		{
			rm.Start();
			MockNM amNodeManager = rm.RegisterNode("127.0.0.1:1234", 2048);
			string[] mediaTypes = new string[] { MediaType.ApplicationJson, MediaType.ApplicationXml
				 };
			foreach (string mediaType in mediaTypes)
			{
				RMApp app = rm.SubmitApp(ContainerMb, string.Empty, webserviceUserName);
				amNodeManager.NodeHeartbeat(true);
				ClientResponse response = this.ConstructWebResource("apps", app.GetApplicationId(
					).ToString(), "state").Accept(mediaType).Get<ClientResponse>();
				NUnit.Framework.Assert.AreEqual(ClientResponse.Status.Ok, response.GetClientResponseStatus
					());
				if (mediaType.Equals(MediaType.ApplicationJson))
				{
					VerifyAppStateJson(response, RMAppState.Accepted);
				}
				else
				{
					if (mediaType.Equals(MediaType.ApplicationXml))
					{
						VerifyAppStateXML(response, RMAppState.Accepted);
					}
				}
			}
			rm.Stop();
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestSingleAppKill()
		{
			rm.Start();
			MockNM amNodeManager = rm.RegisterNode("127.0.0.1:1234", 2048);
			string[] mediaTypes = new string[] { MediaType.ApplicationJson, MediaType.ApplicationXml
				 };
			MediaType[] contentTypes = new MediaType[] { MediaType.ApplicationJsonType, MediaType
				.ApplicationXmlType };
			foreach (string mediaType in mediaTypes)
			{
				foreach (MediaType contentType in contentTypes)
				{
					RMApp app = rm.SubmitApp(ContainerMb, string.Empty, webserviceUserName);
					amNodeManager.NodeHeartbeat(true);
					AppState targetState = new AppState(YarnApplicationState.Killed.ToString());
					object entity;
					if (contentType.Equals(MediaType.ApplicationJsonType))
					{
						entity = AppStateToJSON(targetState);
					}
					else
					{
						entity = targetState;
					}
					ClientResponse response = this.ConstructWebResource("apps", app.GetApplicationId(
						).ToString(), "state").Entity(entity, contentType).Accept(mediaType).Put<ClientResponse
						>();
					if (!IsAuthenticationEnabled())
					{
						NUnit.Framework.Assert.AreEqual(ClientResponse.Status.Unauthorized, response.GetClientResponseStatus
							());
						continue;
					}
					NUnit.Framework.Assert.AreEqual(ClientResponse.Status.Accepted, response.GetClientResponseStatus
						());
					if (mediaType.Equals(MediaType.ApplicationJson))
					{
						VerifyAppStateJson(response, RMAppState.FinalSaving, RMAppState.Killed, RMAppState
							.Killing, RMAppState.Accepted);
					}
					else
					{
						VerifyAppStateXML(response, RMAppState.FinalSaving, RMAppState.Killed, RMAppState
							.Killing, RMAppState.Accepted);
					}
					string locationHeaderValue = response.GetHeaders().GetFirst(HttpHeaders.Location);
					Com.Sun.Jersey.Api.Client.Client c = Com.Sun.Jersey.Api.Client.Client.Create();
					WebResource tmp = c.Resource(locationHeaderValue);
					if (IsAuthenticationEnabled())
					{
						tmp = tmp.QueryParam("user.name", webserviceUserName);
					}
					response = tmp.Get<ClientResponse>();
					NUnit.Framework.Assert.AreEqual(ClientResponse.Status.Ok, response.GetClientResponseStatus
						());
					NUnit.Framework.Assert.IsTrue(locationHeaderValue.EndsWith("/ws/v1/cluster/apps/"
						 + app.GetApplicationId().ToString() + "/state"));
					while (true)
					{
						Sharpen.Thread.Sleep(100);
						response = this.ConstructWebResource("apps", app.GetApplicationId().ToString(), "state"
							).Accept(mediaType).Entity(entity, contentType).Put<ClientResponse>();
						NUnit.Framework.Assert.IsTrue((response.GetClientResponseStatus() == ClientResponse.Status
							.Accepted) || (response.GetClientResponseStatus() == ClientResponse.Status.Ok));
						if (response.GetClientResponseStatus() == ClientResponse.Status.Ok)
						{
							NUnit.Framework.Assert.AreEqual(RMAppState.Killed, app.GetState());
							if (mediaType.Equals(MediaType.ApplicationJson))
							{
								VerifyAppStateJson(response, RMAppState.Killed);
							}
							else
							{
								VerifyAppStateXML(response, RMAppState.Killed);
							}
							break;
						}
					}
				}
			}
			rm.Stop();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestSingleAppKillInvalidState()
		{
			rm.Start();
			MockNM amNodeManager = rm.RegisterNode("127.0.0.1:1234", 2048);
			string[] mediaTypes = new string[] { MediaType.ApplicationJson, MediaType.ApplicationXml
				 };
			MediaType[] contentTypes = new MediaType[] { MediaType.ApplicationJsonType, MediaType
				.ApplicationXmlType };
			string[] targetStates = new string[] { YarnApplicationState.Finished.ToString(), 
				"blah" };
			foreach (string mediaType in mediaTypes)
			{
				foreach (MediaType contentType in contentTypes)
				{
					foreach (string targetStateString in targetStates)
					{
						RMApp app = rm.SubmitApp(ContainerMb, string.Empty, webserviceUserName);
						amNodeManager.NodeHeartbeat(true);
						ClientResponse response;
						AppState targetState = new AppState(targetStateString);
						object entity;
						if (contentType.Equals(MediaType.ApplicationJsonType))
						{
							entity = AppStateToJSON(targetState);
						}
						else
						{
							entity = targetState;
						}
						response = this.ConstructWebResource("apps", app.GetApplicationId().ToString(), "state"
							).Entity(entity, contentType).Accept(mediaType).Put<ClientResponse>();
						if (!IsAuthenticationEnabled())
						{
							NUnit.Framework.Assert.AreEqual(ClientResponse.Status.Unauthorized, response.GetClientResponseStatus
								());
							continue;
						}
						NUnit.Framework.Assert.AreEqual(ClientResponse.Status.BadRequest, response.GetClientResponseStatus
							());
					}
				}
			}
			rm.Stop();
		}

		/// <exception cref="System.Exception"/>
		private static string AppStateToJSON(AppState state)
		{
			StringWriter sw = new StringWriter();
			JSONJAXBContext ctx = new JSONJAXBContext(typeof(AppState));
			JSONMarshaller jm = ctx.CreateJSONMarshaller();
			jm.MarshallToJSON(state, sw);
			return sw.ToString();
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		protected internal static void VerifyAppStateJson(ClientResponse response, params 
			RMAppState[] states)
		{
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			JSONObject json = response.GetEntity<JSONObject>();
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, json.Length());
			string responseState = json.GetString("state");
			bool valid = false;
			foreach (RMAppState state in states)
			{
				if (state.ToString().Equals(responseState))
				{
					valid = true;
				}
			}
			string msg = "app state incorrect, got " + responseState;
			NUnit.Framework.Assert.IsTrue(msg, valid);
		}

		/// <exception cref="Javax.Xml.Parsers.ParserConfigurationException"/>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Xml.Sax.SAXException"/>
		protected internal static void VerifyAppStateXML(ClientResponse response, params 
			RMAppState[] appStates)
		{
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationXmlType, response.GetType());
			string xml = response.GetEntity<string>();
			DocumentBuilderFactory dbf = DocumentBuilderFactory.NewInstance();
			DocumentBuilder db = dbf.NewDocumentBuilder();
			InputSource @is = new InputSource();
			@is.SetCharacterStream(new StringReader(xml));
			Document dom = db.Parse(@is);
			NodeList nodes = dom.GetElementsByTagName("appstate");
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, nodes.GetLength
				());
			Element element = (Element)nodes.Item(0);
			string state = WebServicesTestUtils.GetXmlString(element, "state");
			bool valid = false;
			foreach (RMAppState appState in appStates)
			{
				if (appState.ToString().Equals(state))
				{
					valid = true;
				}
			}
			string msg = "app state incorrect, got " + state;
			NUnit.Framework.Assert.IsTrue(msg, valid);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestSingleAppKillUnauthorized()
		{
			bool isCapacityScheduler = rm.GetResourceScheduler() is CapacityScheduler;
			bool isFairScheduler = rm.GetResourceScheduler() is FairScheduler;
			Assume.AssumeTrue("This test is only supported on Capacity and Fair Scheduler", isCapacityScheduler
				 || isFairScheduler);
			// FairScheduler use ALLOCATION_FILE to configure ACL
			if (isCapacityScheduler)
			{
				// default root queue allows anyone to have admin acl
				CapacitySchedulerConfiguration csconf = new CapacitySchedulerConfiguration();
				csconf.SetAcl("root", QueueACL.AdministerQueue, "someuser");
				csconf.SetAcl("root.default", QueueACL.AdministerQueue, "someuser");
				rm.GetResourceScheduler().Reinitialize(csconf, rm.GetRMContext());
			}
			rm.Start();
			MockNM amNodeManager = rm.RegisterNode("127.0.0.1:1234", 2048);
			string[] mediaTypes = new string[] { MediaType.ApplicationJson, MediaType.ApplicationXml
				 };
			foreach (string mediaType in mediaTypes)
			{
				RMApp app = rm.SubmitApp(ContainerMb, "test", "someuser");
				amNodeManager.NodeHeartbeat(true);
				ClientResponse response = this.ConstructWebResource("apps", app.GetApplicationId(
					).ToString(), "state").Accept(mediaType).Get<ClientResponse>();
				AppState info = response.GetEntity<AppState>();
				info.SetState(YarnApplicationState.Killed.ToString());
				response = this.ConstructWebResource("apps", app.GetApplicationId().ToString(), "state"
					).Accept(mediaType).Entity(info, MediaType.ApplicationXml).Put<ClientResponse>();
				ValidateResponseStatus(response, ClientResponse.Status.Forbidden);
			}
			rm.Stop();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestSingleAppKillInvalidId()
		{
			rm.Start();
			MockNM amNodeManager = rm.RegisterNode("127.0.0.1:1234", 2048);
			amNodeManager.NodeHeartbeat(true);
			string[] testAppIds = new string[] { "application_1391705042196_0001", "random_string"
				 };
			foreach (string testAppId in testAppIds)
			{
				AppState info = new AppState("KILLED");
				ClientResponse response = this.ConstructWebResource("apps", testAppId, "state").Accept
					(MediaType.ApplicationXml).Entity(info, MediaType.ApplicationXml).Put<ClientResponse
					>();
				if (!IsAuthenticationEnabled())
				{
					NUnit.Framework.Assert.AreEqual(ClientResponse.Status.Unauthorized, response.GetClientResponseStatus
						());
					continue;
				}
				NUnit.Framework.Assert.AreEqual(ClientResponse.Status.NotFound, response.GetClientResponseStatus
					());
			}
			rm.Stop();
		}

		/// <exception cref="System.Exception"/>
		[TearDown]
		public override void TearDown()
		{
			if (rm != null)
			{
				rm.Stop();
			}
			base.TearDown();
		}

		/// <summary>Helper function to wrap frequently used code.</summary>
		/// <remarks>
		/// Helper function to wrap frequently used code. It checks the response status
		/// and checks if it UNAUTHORIZED if we are running with authorization turned
		/// off or the param passed if we are running with authorization turned on.
		/// </remarks>
		/// <param name="response">the ClientResponse object to be checked</param>
		/// <param name="expectedAuthorizedMode">the expected Status in authorized mode.</param>
		public virtual void ValidateResponseStatus(ClientResponse response, ClientResponse.Status
			 expectedAuthorizedMode)
		{
			ValidateResponseStatus(response, ClientResponse.Status.Unauthorized, expectedAuthorizedMode
				);
		}

		/// <summary>Helper function to wrap frequently used code.</summary>
		/// <remarks>
		/// Helper function to wrap frequently used code. It checks the response status
		/// and checks if it is the param expectedUnauthorizedMode if we are running
		/// with authorization turned off or the param expectedAuthorizedMode passed if
		/// we are running with authorization turned on.
		/// </remarks>
		/// <param name="response">the ClientResponse object to be checked</param>
		/// <param name="expectedUnauthorizedMode">the expected Status in unauthorized mode.</param>
		/// <param name="expectedAuthorizedMode">the expected Status in authorized mode.</param>
		public virtual void ValidateResponseStatus(ClientResponse response, ClientResponse.Status
			 expectedUnauthorizedMode, ClientResponse.Status expectedAuthorizedMode)
		{
			if (!IsAuthenticationEnabled())
			{
				NUnit.Framework.Assert.AreEqual(expectedUnauthorizedMode, response.GetClientResponseStatus
					());
			}
			else
			{
				NUnit.Framework.Assert.AreEqual(expectedAuthorizedMode, response.GetClientResponseStatus
					());
			}
		}

		// Simple test - just post to /apps/new-application and validate the response
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGetNewApplication()
		{
			Client().AddFilter(new LoggingFilter(System.Console.Out));
			rm.Start();
			string[] mediaTypes = new string[] { MediaType.ApplicationJson, MediaType.ApplicationXml
				 };
			foreach (string acceptMedia in mediaTypes)
			{
				TestGetNewApplication(acceptMedia);
			}
			rm.Stop();
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="Javax.Xml.Parsers.ParserConfigurationException"/>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Xml.Sax.SAXException"/>
		protected internal virtual string TestGetNewApplication(string mediaType)
		{
			ClientResponse response = this.ConstructWebResource("apps", "new-application").Accept
				(mediaType).Post<ClientResponse>();
			ValidateResponseStatus(response, ClientResponse.Status.Ok);
			if (!IsAuthenticationEnabled())
			{
				return string.Empty;
			}
			return ValidateGetNewApplicationResponse(response);
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="Javax.Xml.Parsers.ParserConfigurationException"/>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Xml.Sax.SAXException"/>
		protected internal virtual string ValidateGetNewApplicationResponse(ClientResponse
			 resp)
		{
			string ret = string.Empty;
			if (resp.GetType().Equals(MediaType.ApplicationJsonType))
			{
				JSONObject json = resp.GetEntity<JSONObject>();
				ret = ValidateGetNewApplicationJsonResponse(json);
			}
			else
			{
				if (resp.GetType().Equals(MediaType.ApplicationXmlType))
				{
					string xml = resp.GetEntity<string>();
					ret = ValidateGetNewApplicationXMLResponse(xml);
				}
				else
				{
					// we should not be here
					NUnit.Framework.Assert.IsTrue(false);
				}
			}
			return ret;
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		protected internal virtual string ValidateGetNewApplicationJsonResponse(JSONObject
			 json)
		{
			string appId = json.GetString("application-id");
			NUnit.Framework.Assert.IsTrue(!appId.IsEmpty());
			JSONObject maxResources = json.GetJSONObject("maximum-resource-capability");
			long memory = maxResources.GetLong("memory");
			long vCores = maxResources.GetLong("vCores");
			NUnit.Framework.Assert.IsTrue(memory != 0);
			NUnit.Framework.Assert.IsTrue(vCores != 0);
			return appId;
		}

		/// <exception cref="Javax.Xml.Parsers.ParserConfigurationException"/>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Xml.Sax.SAXException"/>
		protected internal virtual string ValidateGetNewApplicationXMLResponse(string response
			)
		{
			DocumentBuilderFactory dbf = DocumentBuilderFactory.NewInstance();
			DocumentBuilder db = dbf.NewDocumentBuilder();
			InputSource @is = new InputSource();
			@is.SetCharacterStream(new StringReader(response));
			Document dom = db.Parse(@is);
			NodeList nodes = dom.GetElementsByTagName("NewApplication");
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, nodes.GetLength
				());
			Element element = (Element)nodes.Item(0);
			string appId = WebServicesTestUtils.GetXmlString(element, "application-id");
			NUnit.Framework.Assert.IsTrue(!appId.IsEmpty());
			NodeList maxResourceNodes = element.GetElementsByTagName("maximum-resource-capability"
				);
			NUnit.Framework.Assert.AreEqual(1, maxResourceNodes.GetLength());
			Element maxResourceCapability = (Element)maxResourceNodes.Item(0);
			long memory = WebServicesTestUtils.GetXmlLong(maxResourceCapability, "memory");
			long vCores = WebServicesTestUtils.GetXmlLong(maxResourceCapability, "vCores");
			NUnit.Framework.Assert.IsTrue(memory != 0);
			NUnit.Framework.Assert.IsTrue(vCores != 0);
			return appId;
		}

		// Test to validate the process of submitting apps - test for appropriate
		// errors as well
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGetNewApplicationAndSubmit()
		{
			rm.Start();
			MockNM amNodeManager = rm.RegisterNode("127.0.0.1:1234", 2048);
			amNodeManager.NodeHeartbeat(true);
			string[] mediaTypes = new string[] { MediaType.ApplicationJson, MediaType.ApplicationXml
				 };
			foreach (string acceptMedia in mediaTypes)
			{
				foreach (string contentMedia in mediaTypes)
				{
					TestAppSubmit(acceptMedia, contentMedia);
					TestAppSubmitErrors(acceptMedia, contentMedia);
				}
			}
			rm.Stop();
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestAppSubmit(string acceptMedia, string contentMedia)
		{
			// create a test app and submit it via rest(after getting an app-id) then
			// get the app details from the rmcontext and check that everything matches
			Client().AddFilter(new LoggingFilter(System.Console.Out));
			string lrKey = "example";
			string queueName = "testqueue";
			string appName = "test";
			string appType = "test-type";
			string urlPath = "apps";
			string appId = TestGetNewApplication(acceptMedia);
			IList<string> commands = new AList<string>();
			commands.AddItem("/bin/sleep 5");
			Dictionary<string, string> environment = new Dictionary<string, string>();
			environment["APP_VAR"] = "ENV_SETTING";
			Dictionary<ApplicationAccessType, string> acls = new Dictionary<ApplicationAccessType
				, string>();
			acls[ApplicationAccessType.ModifyApp] = "testuser1, testuser2";
			acls[ApplicationAccessType.ViewApp] = "testuser3, testuser4";
			ICollection<string> tags = new HashSet<string>();
			tags.AddItem("tag1");
			tags.AddItem("tag 2");
			CredentialsInfo credentials = new CredentialsInfo();
			Dictionary<string, string> tokens = new Dictionary<string, string>();
			Dictionary<string, string> secrets = new Dictionary<string, string>();
			secrets["secret1"] = Base64.EncodeBase64String(Sharpen.Runtime.GetBytesForString(
				"mysecret", "UTF8"));
			credentials.SetSecrets(secrets);
			credentials.SetTokens(tokens);
			ApplicationSubmissionContextInfo appInfo = new ApplicationSubmissionContextInfo();
			appInfo.SetApplicationId(appId);
			appInfo.SetApplicationName(appName);
			appInfo.SetPriority(3);
			appInfo.SetMaxAppAttempts(2);
			appInfo.SetQueue(queueName);
			appInfo.SetApplicationType(appType);
			Dictionary<string, LocalResourceInfo> lr = new Dictionary<string, LocalResourceInfo
				>();
			LocalResourceInfo y = new LocalResourceInfo();
			y.SetUrl(new URI("http://www.test.com/file.txt"));
			y.SetSize(100);
			y.SetTimestamp(Runtime.CurrentTimeMillis());
			y.SetType(LocalResourceType.File);
			y.SetVisibility(LocalResourceVisibility.Application);
			lr[lrKey] = y;
			appInfo.GetContainerLaunchContextInfo().SetResources(lr);
			appInfo.GetContainerLaunchContextInfo().SetCommands(commands);
			appInfo.GetContainerLaunchContextInfo().SetEnvironment(environment);
			appInfo.GetContainerLaunchContextInfo().SetAcls(acls);
			appInfo.GetContainerLaunchContextInfo().GetAuxillaryServiceData()["test"] = Base64
				.EncodeBase64URLSafeString(Sharpen.Runtime.GetBytesForString("value12", "UTF8"));
			appInfo.GetContainerLaunchContextInfo().SetCredentials(credentials);
			appInfo.GetResource().SetMemory(1024);
			appInfo.GetResource().SetvCores(1);
			appInfo.SetApplicationTags(tags);
			ClientResponse response = this.ConstructWebResource(urlPath).Accept(acceptMedia).
				Entity(appInfo, contentMedia).Post<ClientResponse>();
			if (!this.IsAuthenticationEnabled())
			{
				NUnit.Framework.Assert.AreEqual(ClientResponse.Status.Unauthorized, response.GetClientResponseStatus
					());
				return;
			}
			NUnit.Framework.Assert.AreEqual(ClientResponse.Status.Accepted, response.GetClientResponseStatus
				());
			NUnit.Framework.Assert.IsTrue(!response.GetHeaders().GetFirst(HttpHeaders.Location
				).IsEmpty());
			string locURL = response.GetHeaders().GetFirst(HttpHeaders.Location);
			NUnit.Framework.Assert.IsTrue(locURL.Contains("/apps/application"));
			appId = Sharpen.Runtime.Substring(locURL, locURL.IndexOf("/apps/") + "/apps/".Length
				);
			WebResource res = Resource().Uri(new URI(locURL));
			res = res.QueryParam("user.name", webserviceUserName);
			response = res.Get<ClientResponse>();
			NUnit.Framework.Assert.AreEqual(ClientResponse.Status.Ok, response.GetClientResponseStatus
				());
			RMApp app = rm.GetRMContext().GetRMApps()[ConverterUtils.ToApplicationId(appId)];
			NUnit.Framework.Assert.AreEqual(appName, app.GetName());
			NUnit.Framework.Assert.AreEqual(webserviceUserName, app.GetUser());
			NUnit.Framework.Assert.AreEqual(2, app.GetMaxAppAttempts());
			if (app.GetQueue().Contains("root."))
			{
				queueName = "root." + queueName;
			}
			NUnit.Framework.Assert.AreEqual(queueName, app.GetQueue());
			NUnit.Framework.Assert.AreEqual(appType, app.GetApplicationType());
			NUnit.Framework.Assert.AreEqual(tags, app.GetApplicationTags());
			ContainerLaunchContext ctx = app.GetApplicationSubmissionContext().GetAMContainerSpec
				();
			NUnit.Framework.Assert.AreEqual(commands, ctx.GetCommands());
			NUnit.Framework.Assert.AreEqual(environment, ctx.GetEnvironment());
			NUnit.Framework.Assert.AreEqual(acls, ctx.GetApplicationACLs());
			IDictionary<string, LocalResource> appLRs = ctx.GetLocalResources();
			NUnit.Framework.Assert.IsTrue(appLRs.Contains(lrKey));
			LocalResource exampleLR = appLRs[lrKey];
			NUnit.Framework.Assert.AreEqual(ConverterUtils.GetYarnUrlFromURI(y.GetUrl()), exampleLR
				.GetResource());
			NUnit.Framework.Assert.AreEqual(y.GetSize(), exampleLR.GetSize());
			NUnit.Framework.Assert.AreEqual(y.GetTimestamp(), exampleLR.GetTimestamp());
			NUnit.Framework.Assert.AreEqual(y.GetType(), exampleLR.GetType());
			NUnit.Framework.Assert.AreEqual(y.GetPattern(), exampleLR.GetPattern());
			NUnit.Framework.Assert.AreEqual(y.GetVisibility(), exampleLR.GetVisibility());
			Credentials cs = new Credentials();
			ByteArrayInputStream str = new ByteArrayInputStream(((byte[])app.GetApplicationSubmissionContext
				().GetAMContainerSpec().GetTokens().Array()));
			DataInputStream di = new DataInputStream(str);
			cs.ReadTokenStorageStream(di);
			Text key = new Text("secret1");
			NUnit.Framework.Assert.IsTrue("Secrets missing from credentials object", cs.GetAllSecretKeys
				().Contains(key));
			NUnit.Framework.Assert.AreEqual("mysecret", Sharpen.Runtime.GetStringForBytes(cs.
				GetSecretKey(key), "UTF-8"));
			response = this.ConstructWebResource("apps", appId).Accept(acceptMedia).Get<ClientResponse
				>();
			NUnit.Framework.Assert.AreEqual(ClientResponse.Status.Ok, response.GetClientResponseStatus
				());
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestAppSubmitErrors(string acceptMedia, string contentMedia)
		{
			// submit a bunch of bad requests(correct format but bad values) via the
			// REST API and make sure we get the right error response codes
			string urlPath = "apps";
			ApplicationSubmissionContextInfo appInfo = new ApplicationSubmissionContextInfo();
			ClientResponse response = this.ConstructWebResource(urlPath).Accept(acceptMedia).
				Entity(appInfo, contentMedia).Post<ClientResponse>();
			ValidateResponseStatus(response, ClientResponse.Status.BadRequest);
			string appId = "random";
			appInfo.SetApplicationId(appId);
			response = this.ConstructWebResource(urlPath).Accept(acceptMedia).Entity(appInfo, 
				contentMedia).Post<ClientResponse>();
			ValidateResponseStatus(response, ClientResponse.Status.BadRequest);
			appId = "random_junk";
			appInfo.SetApplicationId(appId);
			response = this.ConstructWebResource(urlPath).Accept(acceptMedia).Entity(appInfo, 
				contentMedia).Post<ClientResponse>();
			ValidateResponseStatus(response, ClientResponse.Status.BadRequest);
			// bad resource info
			appInfo.GetResource().SetMemory(rm.GetConfig().GetInt(YarnConfiguration.RmSchedulerMaximumAllocationMb
				, YarnConfiguration.DefaultRmSchedulerMaximumAllocationMb) + 1);
			appInfo.GetResource().SetvCores(1);
			response = this.ConstructWebResource(urlPath).Accept(acceptMedia).Entity(appInfo, 
				contentMedia).Post<ClientResponse>();
			ValidateResponseStatus(response, ClientResponse.Status.BadRequest);
			appInfo.GetResource().SetvCores(rm.GetConfig().GetInt(YarnConfiguration.RmSchedulerMaximumAllocationVcores
				, YarnConfiguration.DefaultRmSchedulerMaximumAllocationVcores) + 1);
			appInfo.GetResource().SetMemory(ContainerMb);
			response = this.ConstructWebResource(urlPath).Accept(acceptMedia).Entity(appInfo, 
				contentMedia).Post<ClientResponse>();
			ValidateResponseStatus(response, ClientResponse.Status.BadRequest);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAppSubmitBadJsonAndXML()
		{
			// submit a bunch of bad XML and JSON via the
			// REST API and make sure we get error response codes
			string urlPath = "apps";
			rm.Start();
			MockNM amNodeManager = rm.RegisterNode("127.0.0.1:1234", 2048);
			amNodeManager.NodeHeartbeat(true);
			ApplicationSubmissionContextInfo appInfo = new ApplicationSubmissionContextInfo();
			appInfo.SetApplicationName("test");
			appInfo.SetPriority(3);
			appInfo.SetMaxAppAttempts(2);
			appInfo.SetQueue("testqueue");
			appInfo.SetApplicationType("test-type");
			Dictionary<string, LocalResourceInfo> lr = new Dictionary<string, LocalResourceInfo
				>();
			LocalResourceInfo y = new LocalResourceInfo();
			y.SetUrl(new URI("http://www.test.com/file.txt"));
			y.SetSize(100);
			y.SetTimestamp(Runtime.CurrentTimeMillis());
			y.SetType(LocalResourceType.File);
			y.SetVisibility(LocalResourceVisibility.Application);
			lr["example"] = y;
			appInfo.GetContainerLaunchContextInfo().SetResources(lr);
			appInfo.GetResource().SetMemory(1024);
			appInfo.GetResource().SetvCores(1);
			string body = "<?xml version=\"1.0\" encoding=\"UTF-8\" " + "standalone=\"yes\"?><blah/>";
			ClientResponse response = this.ConstructWebResource(urlPath).Accept(MediaType.ApplicationXml
				).Entity(body, MediaType.ApplicationXml).Post<ClientResponse>();
			NUnit.Framework.Assert.AreEqual(ClientResponse.Status.BadRequest, response.GetClientResponseStatus
				());
			body = "{\"a\" : \"b\"}";
			response = this.ConstructWebResource(urlPath).Accept(MediaType.ApplicationXml).Entity
				(body, MediaType.ApplicationJson).Post<ClientResponse>();
			ValidateResponseStatus(response, ClientResponse.Status.BadRequest);
			rm.Stop();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGetAppQueue()
		{
			Client().AddFilter(new LoggingFilter(System.Console.Out));
			bool isCapacityScheduler = rm.GetResourceScheduler() is CapacityScheduler;
			rm.Start();
			MockNM amNodeManager = rm.RegisterNode("127.0.0.1:1234", 2048);
			string[] contentTypes = new string[] { MediaType.ApplicationJson, MediaType.ApplicationXml
				 };
			foreach (string contentType in contentTypes)
			{
				RMApp app = rm.SubmitApp(ContainerMb, string.Empty, webserviceUserName);
				amNodeManager.NodeHeartbeat(true);
				ClientResponse response = this.ConstructWebResource("apps", app.GetApplicationId(
					).ToString(), "queue").Accept(contentType).Get<ClientResponse>();
				NUnit.Framework.Assert.AreEqual(ClientResponse.Status.Ok, response.GetClientResponseStatus
					());
				string expectedQueue = "default";
				if (!isCapacityScheduler)
				{
					expectedQueue = "root." + webserviceUserName;
				}
				if (contentType.Equals(MediaType.ApplicationJson))
				{
					VerifyAppQueueJson(response, expectedQueue);
				}
				else
				{
					VerifyAppQueueXML(response, expectedQueue);
				}
			}
			rm.Stop();
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestAppMove()
		{
			Client().AddFilter(new LoggingFilter(System.Console.Out));
			bool isCapacityScheduler = rm.GetResourceScheduler() is CapacityScheduler;
			// default root queue allows anyone to have admin acl
			CapacitySchedulerConfiguration csconf = new CapacitySchedulerConfiguration();
			string[] queues = new string[] { "default", "test" };
			csconf.SetQueues("root", queues);
			csconf.SetCapacity("root.default", 50.0f);
			csconf.SetCapacity("root.test", 50.0f);
			csconf.SetAcl("root", QueueACL.AdministerQueue, "someuser");
			csconf.SetAcl("root.default", QueueACL.AdministerQueue, "someuser");
			csconf.SetAcl("root.test", QueueACL.AdministerQueue, "someuser");
			rm.GetResourceScheduler().Reinitialize(csconf, rm.GetRMContext());
			rm.Start();
			MockNM amNodeManager = rm.RegisterNode("127.0.0.1:1234", 2048);
			string[] mediaTypes = new string[] { MediaType.ApplicationJson, MediaType.ApplicationXml
				 };
			MediaType[] contentTypes = new MediaType[] { MediaType.ApplicationJsonType, MediaType
				.ApplicationXmlType };
			foreach (string mediaType in mediaTypes)
			{
				foreach (MediaType contentType in contentTypes)
				{
					RMApp app = rm.SubmitApp(ContainerMb, string.Empty, webserviceUserName);
					amNodeManager.NodeHeartbeat(true);
					AppQueue targetQueue = new AppQueue("test");
					object entity;
					if (contentType.Equals(MediaType.ApplicationJsonType))
					{
						entity = AppQueueToJSON(targetQueue);
					}
					else
					{
						entity = targetQueue;
					}
					ClientResponse response = this.ConstructWebResource("apps", app.GetApplicationId(
						).ToString(), "queue").Entity(entity, contentType).Accept(mediaType).Put<ClientResponse
						>();
					if (!IsAuthenticationEnabled())
					{
						NUnit.Framework.Assert.AreEqual(ClientResponse.Status.Unauthorized, response.GetClientResponseStatus
							());
						continue;
					}
					NUnit.Framework.Assert.AreEqual(ClientResponse.Status.Ok, response.GetClientResponseStatus
						());
					string expectedQueue = "test";
					if (!isCapacityScheduler)
					{
						expectedQueue = "root.test";
					}
					if (mediaType.Equals(MediaType.ApplicationJson))
					{
						VerifyAppQueueJson(response, expectedQueue);
					}
					else
					{
						VerifyAppQueueXML(response, expectedQueue);
					}
					NUnit.Framework.Assert.AreEqual(expectedQueue, app.GetQueue());
					// check unauthorized
					app = rm.SubmitApp(ContainerMb, string.Empty, "someuser");
					amNodeManager.NodeHeartbeat(true);
					response = this.ConstructWebResource("apps", app.GetApplicationId().ToString(), "queue"
						).Entity(entity, contentType).Accept(mediaType).Put<ClientResponse>();
					NUnit.Framework.Assert.AreEqual(ClientResponse.Status.Forbidden, response.GetClientResponseStatus
						());
					if (isCapacityScheduler)
					{
						NUnit.Framework.Assert.AreEqual("default", app.GetQueue());
					}
					else
					{
						NUnit.Framework.Assert.AreEqual("root.someuser", app.GetQueue());
					}
				}
			}
			rm.Stop();
		}

		/// <exception cref="System.Exception"/>
		protected internal static string AppQueueToJSON(AppQueue targetQueue)
		{
			StringWriter sw = new StringWriter();
			JSONJAXBContext ctx = new JSONJAXBContext(typeof(AppQueue));
			JSONMarshaller jm = ctx.CreateJSONMarshaller();
			jm.MarshallToJSON(targetQueue, sw);
			return sw.ToString();
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		protected internal static void VerifyAppQueueJson(ClientResponse response, string
			 queue)
		{
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			JSONObject json = response.GetEntity<JSONObject>();
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, json.Length());
			string responseQueue = json.GetString("queue");
			NUnit.Framework.Assert.AreEqual(queue, responseQueue);
		}

		/// <exception cref="Javax.Xml.Parsers.ParserConfigurationException"/>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Xml.Sax.SAXException"/>
		protected internal static void VerifyAppQueueXML(ClientResponse response, string 
			queue)
		{
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationXmlType, response.GetType());
			string xml = response.GetEntity<string>();
			DocumentBuilderFactory dbf = DocumentBuilderFactory.NewInstance();
			DocumentBuilder db = dbf.NewDocumentBuilder();
			InputSource @is = new InputSource();
			@is.SetCharacterStream(new StringReader(xml));
			Document dom = db.Parse(@is);
			NodeList nodes = dom.GetElementsByTagName("appqueue");
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, nodes.GetLength
				());
			Element element = (Element)nodes.Item(0);
			string responseQueue = WebServicesTestUtils.GetXmlString(element, "queue");
			NUnit.Framework.Assert.AreEqual(queue, responseQueue);
		}
	}
}
