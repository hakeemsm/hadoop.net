using System.Collections.Generic;
using System.IO;
using Com.Google.Inject;
using Com.Google.Inject.Servlet;
using Com.Sun.Jersey.Api.Client;
using Com.Sun.Jersey.Core.Util;
using Com.Sun.Jersey.Guice.Spi.Container.Servlet;
using Com.Sun.Jersey.Test.Framework;
using Javax.WS.RS.Core;
using Javax.Xml.Parsers;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmapp.Attempt;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Fifo;
using Org.Apache.Hadoop.Yarn.Webapp;
using Org.Codehaus.Jettison.Json;
using Org.W3c.Dom;
using Org.Xml.Sax;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Webapp
{
	public class TestRMWebServicesApps : JerseyTestBase
	{
		private static MockRM rm;

		private const int ContainerMb = 1024;

		private sealed class _ServletModule_79 : ServletModule
		{
			public _ServletModule_79()
			{
			}

			protected override void ConfigureServlets()
			{
				this.Bind<JAXBContextResolver>();
				this.Bind<RMWebServices>();
				this.Bind<GenericExceptionHandler>();
				Configuration conf = new Configuration();
				conf.SetInt(YarnConfiguration.RmAmMaxAttempts, YarnConfiguration.DefaultRmAmMaxAttempts
					);
				conf.SetClass(YarnConfiguration.RmScheduler, typeof(FifoScheduler), typeof(ResourceScheduler
					));
				Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Webapp.TestRMWebServicesApps.rm = new 
					MockRM(conf);
				this.Bind<ResourceManager>().ToInstance(Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Webapp.TestRMWebServicesApps
					.rm);
				this.Serve("/*").With(typeof(GuiceContainer));
			}
		}

		private Injector injector = Guice.CreateInjector(new _ServletModule_79());

		public class GuiceServletConfig : GuiceServletContextListener
		{
			protected override Injector GetInjector()
			{
				return this._enclosing.injector;
			}

			internal GuiceServletConfig(TestRMWebServicesApps _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly TestRMWebServicesApps _enclosing;
		}

		/// <exception cref="System.Exception"/>
		[SetUp]
		public override void SetUp()
		{
			base.SetUp();
		}

		public TestRMWebServicesApps()
			: base(new WebAppDescriptor.Builder("org.apache.hadoop.yarn.server.resourcemanager.webapp"
				).ContextListenerClass(typeof(TestRMWebServicesApps.GuiceServletConfig)).FilterClass
				(typeof(GuiceFilter)).ContextPath("jersey-guice-filter").ServletPath("/").Build(
				))
		{
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestApps()
		{
			rm.Start();
			MockNM amNodeManager = rm.RegisterNode("127.0.0.1:1234", 2048);
			RMApp app1 = rm.SubmitApp(ContainerMb);
			amNodeManager.NodeHeartbeat(true);
			TestAppsHelper("apps", app1, MediaType.ApplicationJson);
			rm.Stop();
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAppsSlash()
		{
			rm.Start();
			MockNM amNodeManager = rm.RegisterNode("127.0.0.1:1234", 2048);
			RMApp app1 = rm.SubmitApp(ContainerMb);
			amNodeManager.NodeHeartbeat(true);
			TestAppsHelper("apps/", app1, MediaType.ApplicationJson);
			rm.Stop();
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAppsDefault()
		{
			rm.Start();
			MockNM amNodeManager = rm.RegisterNode("127.0.0.1:1234", 2048);
			RMApp app1 = rm.SubmitApp(ContainerMb);
			amNodeManager.NodeHeartbeat(true);
			TestAppsHelper("apps/", app1, string.Empty);
			rm.Stop();
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAppsXML()
		{
			rm.Start();
			MockNM amNodeManager = rm.RegisterNode("127.0.0.1:1234", 2048);
			RMApp app1 = rm.SubmitApp(ContainerMb, "testwordcount", "user1");
			amNodeManager.NodeHeartbeat(true);
			WebResource r = Resource();
			ClientResponse response = r.Path("ws").Path("v1").Path("cluster").Path("apps").Accept
				(MediaType.ApplicationXml).Get<ClientResponse>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationXmlType, response.GetType());
			string xml = response.GetEntity<string>();
			DocumentBuilderFactory dbf = DocumentBuilderFactory.NewInstance();
			DocumentBuilder db = dbf.NewDocumentBuilder();
			InputSource @is = new InputSource();
			@is.SetCharacterStream(new StringReader(xml));
			Document dom = db.Parse(@is);
			NodeList nodesApps = dom.GetElementsByTagName("apps");
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, nodesApps.GetLength
				());
			NodeList nodes = dom.GetElementsByTagName("app");
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, nodes.GetLength
				());
			VerifyAppsXML(nodes, app1);
			rm.Stop();
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAppsXMLMulti()
		{
			rm.Start();
			MockNM amNodeManager = rm.RegisterNode("127.0.0.1:1234", 2048);
			rm.SubmitApp(ContainerMb, "testwordcount", "user1");
			rm.SubmitApp(2048, "testwordcount2", "user1");
			amNodeManager.NodeHeartbeat(true);
			WebResource r = Resource();
			ClientResponse response = r.Path("ws").Path("v1").Path("cluster").Path("apps").Accept
				(MediaType.ApplicationXml).Get<ClientResponse>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationXmlType, response.GetType());
			string xml = response.GetEntity<string>();
			DocumentBuilderFactory dbf = DocumentBuilderFactory.NewInstance();
			DocumentBuilder db = dbf.NewDocumentBuilder();
			InputSource @is = new InputSource();
			@is.SetCharacterStream(new StringReader(xml));
			Document dom = db.Parse(@is);
			NodeList nodesApps = dom.GetElementsByTagName("apps");
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, nodesApps.GetLength
				());
			NodeList nodes = dom.GetElementsByTagName("app");
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 2, nodes.GetLength
				());
			rm.Stop();
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		public virtual void TestAppsHelper(string path, RMApp app, string media)
		{
			WebResource r = Resource();
			ClientResponse response = r.Path("ws").Path("v1").Path("cluster").Path(path).Accept
				(media).Get<ClientResponse>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			JSONObject json = response.GetEntity<JSONObject>();
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, json.Length());
			JSONObject apps = json.GetJSONObject("apps");
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, apps.Length());
			JSONArray array = apps.GetJSONArray("app");
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, array.Length()
				);
			VerifyAppInfo(array.GetJSONObject(0), app);
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAppsQueryState()
		{
			rm.Start();
			MockNM amNodeManager = rm.RegisterNode("127.0.0.1:1234", 2048);
			RMApp app1 = rm.SubmitApp(ContainerMb);
			amNodeManager.NodeHeartbeat(true);
			WebResource r = Resource();
			ClientResponse response = r.Path("ws").Path("v1").Path("cluster").Path("apps").QueryParam
				("state", YarnApplicationState.Accepted.ToString()).Accept(MediaType.ApplicationJson
				).Get<ClientResponse>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			JSONObject json = response.GetEntity<JSONObject>();
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, json.Length());
			JSONObject apps = json.GetJSONObject("apps");
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, apps.Length());
			JSONArray array = apps.GetJSONArray("app");
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, array.Length()
				);
			VerifyAppInfo(array.GetJSONObject(0), app1);
			rm.Stop();
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAppsQueryStates()
		{
			rm.Start();
			MockNM amNodeManager = rm.RegisterNode("127.0.0.1:1234", 2048);
			rm.SubmitApp(ContainerMb);
			RMApp killedApp = rm.SubmitApp(ContainerMb);
			rm.KillApp(killedApp.GetApplicationId());
			amNodeManager.NodeHeartbeat(true);
			WebResource r = Resource();
			MultivaluedMapImpl @params = new MultivaluedMapImpl();
			@params.Add("states", YarnApplicationState.Accepted.ToString());
			ClientResponse response = r.Path("ws").Path("v1").Path("cluster").Path("apps").QueryParams
				(@params).Accept(MediaType.ApplicationJson).Get<ClientResponse>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			JSONObject json = response.GetEntity<JSONObject>();
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, json.Length());
			JSONObject apps = json.GetJSONObject("apps");
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, apps.Length());
			JSONArray array = apps.GetJSONArray("app");
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, array.Length()
				);
			NUnit.Framework.Assert.AreEqual("state not equal to ACCEPTED", "ACCEPTED", array.
				GetJSONObject(0).GetString("state"));
			r = Resource();
			@params = new MultivaluedMapImpl();
			@params.Add("states", YarnApplicationState.Accepted.ToString());
			@params.Add("states", YarnApplicationState.Killed.ToString());
			response = r.Path("ws").Path("v1").Path("cluster").Path("apps").QueryParams(@params
				).Accept(MediaType.ApplicationJson).Get<ClientResponse>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			json = response.GetEntity<JSONObject>();
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, json.Length());
			apps = json.GetJSONObject("apps");
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, apps.Length());
			array = apps.GetJSONArray("app");
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 2, array.Length()
				);
			NUnit.Framework.Assert.IsTrue("both app states of ACCEPTED and KILLED are not present"
				, (array.GetJSONObject(0).GetString("state").Equals("ACCEPTED") && array.GetJSONObject
				(1).GetString("state").Equals("KILLED")) || (array.GetJSONObject(0).GetString("state"
				).Equals("KILLED") && array.GetJSONObject(1).GetString("state").Equals("ACCEPTED"
				)));
			rm.Stop();
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAppsQueryStatesComma()
		{
			rm.Start();
			MockNM amNodeManager = rm.RegisterNode("127.0.0.1:1234", 2048);
			rm.SubmitApp(ContainerMb);
			RMApp killedApp = rm.SubmitApp(ContainerMb);
			rm.KillApp(killedApp.GetApplicationId());
			amNodeManager.NodeHeartbeat(true);
			WebResource r = Resource();
			MultivaluedMapImpl @params = new MultivaluedMapImpl();
			@params.Add("states", YarnApplicationState.Accepted.ToString());
			ClientResponse response = r.Path("ws").Path("v1").Path("cluster").Path("apps").QueryParams
				(@params).Accept(MediaType.ApplicationJson).Get<ClientResponse>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			JSONObject json = response.GetEntity<JSONObject>();
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, json.Length());
			JSONObject apps = json.GetJSONObject("apps");
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, apps.Length());
			JSONArray array = apps.GetJSONArray("app");
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, array.Length()
				);
			NUnit.Framework.Assert.AreEqual("state not equal to ACCEPTED", "ACCEPTED", array.
				GetJSONObject(0).GetString("state"));
			r = Resource();
			@params = new MultivaluedMapImpl();
			@params.Add("states", YarnApplicationState.Accepted.ToString() + "," + YarnApplicationState
				.Killed.ToString());
			response = r.Path("ws").Path("v1").Path("cluster").Path("apps").QueryParams(@params
				).Accept(MediaType.ApplicationJson).Get<ClientResponse>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			json = response.GetEntity<JSONObject>();
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, json.Length());
			apps = json.GetJSONObject("apps");
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, apps.Length());
			array = apps.GetJSONArray("app");
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 2, array.Length()
				);
			NUnit.Framework.Assert.IsTrue("both app states of ACCEPTED and KILLED are not present"
				, (array.GetJSONObject(0).GetString("state").Equals("ACCEPTED") && array.GetJSONObject
				(1).GetString("state").Equals("KILLED")) || (array.GetJSONObject(0).GetString("state"
				).Equals("KILLED") && array.GetJSONObject(1).GetString("state").Equals("ACCEPTED"
				)));
			rm.Stop();
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAppsQueryStatesNone()
		{
			rm.Start();
			MockNM amNodeManager = rm.RegisterNode("127.0.0.1:1234", 2048);
			rm.SubmitApp(ContainerMb);
			amNodeManager.NodeHeartbeat(true);
			WebResource r = Resource();
			ClientResponse response = r.Path("ws").Path("v1").Path("cluster").Path("apps").QueryParam
				("states", YarnApplicationState.Running.ToString()).Accept(MediaType.ApplicationJson
				).Get<ClientResponse>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			JSONObject json = response.GetEntity<JSONObject>();
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, json.Length());
			NUnit.Framework.Assert.AreEqual("apps is not null", JSONObject.Null, json.Get("apps"
				));
			rm.Stop();
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAppsQueryStateNone()
		{
			rm.Start();
			MockNM amNodeManager = rm.RegisterNode("127.0.0.1:1234", 2048);
			rm.SubmitApp(ContainerMb);
			amNodeManager.NodeHeartbeat(true);
			WebResource r = Resource();
			ClientResponse response = r.Path("ws").Path("v1").Path("cluster").Path("apps").QueryParam
				("state", YarnApplicationState.Running.ToString()).Accept(MediaType.ApplicationJson
				).Get<ClientResponse>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			JSONObject json = response.GetEntity<JSONObject>();
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, json.Length());
			NUnit.Framework.Assert.AreEqual("apps is not null", JSONObject.Null, json.Get("apps"
				));
			rm.Stop();
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAppsQueryStatesInvalid()
		{
			rm.Start();
			MockNM amNodeManager = rm.RegisterNode("127.0.0.1:1234", 2048);
			rm.SubmitApp(ContainerMb);
			amNodeManager.NodeHeartbeat(true);
			WebResource r = Resource();
			try
			{
				r.Path("ws").Path("v1").Path("cluster").Path("apps").QueryParam("states", "INVALID_test"
					).Accept(MediaType.ApplicationJson).Get<JSONObject>();
				NUnit.Framework.Assert.Fail("should have thrown exception on invalid state query"
					);
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
				WebServicesTestUtils.CheckStringContains("exception message", "Invalid application-state INVALID_test"
					, message);
				WebServicesTestUtils.CheckStringMatch("exception type", "BadRequestException", type
					);
				WebServicesTestUtils.CheckStringMatch("exception classname", "org.apache.hadoop.yarn.webapp.BadRequestException"
					, classname);
			}
			finally
			{
				rm.Stop();
			}
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAppsQueryStateInvalid()
		{
			rm.Start();
			MockNM amNodeManager = rm.RegisterNode("127.0.0.1:1234", 2048);
			rm.SubmitApp(ContainerMb);
			amNodeManager.NodeHeartbeat(true);
			WebResource r = Resource();
			try
			{
				r.Path("ws").Path("v1").Path("cluster").Path("apps").QueryParam("state", "INVALID_test"
					).Accept(MediaType.ApplicationJson).Get<JSONObject>();
				NUnit.Framework.Assert.Fail("should have thrown exception on invalid state query"
					);
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
				WebServicesTestUtils.CheckStringContains("exception message", "Invalid application-state INVALID_test"
					, message);
				WebServicesTestUtils.CheckStringMatch("exception type", "BadRequestException", type
					);
				WebServicesTestUtils.CheckStringMatch("exception classname", "org.apache.hadoop.yarn.webapp.BadRequestException"
					, classname);
			}
			finally
			{
				rm.Stop();
			}
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAppsQueryFinalStatus()
		{
			rm.Start();
			MockNM amNodeManager = rm.RegisterNode("127.0.0.1:1234", 2048);
			RMApp app1 = rm.SubmitApp(ContainerMb);
			amNodeManager.NodeHeartbeat(true);
			WebResource r = Resource();
			ClientResponse response = r.Path("ws").Path("v1").Path("cluster").Path("apps").QueryParam
				("finalStatus", FinalApplicationStatus.Undefined.ToString()).Accept(MediaType.ApplicationJson
				).Get<ClientResponse>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			JSONObject json = response.GetEntity<JSONObject>();
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, json.Length());
			System.Console.Out.WriteLine(json.ToString());
			JSONObject apps = json.GetJSONObject("apps");
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, apps.Length());
			JSONArray array = apps.GetJSONArray("app");
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, array.Length()
				);
			VerifyAppInfo(array.GetJSONObject(0), app1);
			rm.Stop();
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAppsQueryFinalStatusNone()
		{
			rm.Start();
			MockNM amNodeManager = rm.RegisterNode("127.0.0.1:1234", 2048);
			rm.SubmitApp(ContainerMb);
			amNodeManager.NodeHeartbeat(true);
			WebResource r = Resource();
			ClientResponse response = r.Path("ws").Path("v1").Path("cluster").Path("apps").QueryParam
				("finalStatus", FinalApplicationStatus.Killed.ToString()).Accept(MediaType.ApplicationJson
				).Get<ClientResponse>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			JSONObject json = response.GetEntity<JSONObject>();
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, json.Length());
			NUnit.Framework.Assert.AreEqual("apps is not null", JSONObject.Null, json.Get("apps"
				));
			rm.Stop();
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAppsQueryFinalStatusInvalid()
		{
			rm.Start();
			MockNM amNodeManager = rm.RegisterNode("127.0.0.1:1234", 2048);
			rm.SubmitApp(ContainerMb);
			amNodeManager.NodeHeartbeat(true);
			WebResource r = Resource();
			try
			{
				r.Path("ws").Path("v1").Path("cluster").Path("apps").QueryParam("finalStatus", "INVALID_test"
					).Accept(MediaType.ApplicationJson).Get<JSONObject>();
				NUnit.Framework.Assert.Fail("should have thrown exception on invalid state query"
					);
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
				WebServicesTestUtils.CheckStringContains("exception message", "org.apache.hadoop.yarn.api.records.FinalApplicationStatus.INVALID_test"
					, message);
				WebServicesTestUtils.CheckStringMatch("exception type", "IllegalArgumentException"
					, type);
				WebServicesTestUtils.CheckStringMatch("exception classname", "java.lang.IllegalArgumentException"
					, classname);
			}
			finally
			{
				rm.Stop();
			}
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAppsQueryUser()
		{
			rm.Start();
			MockNM amNodeManager = rm.RegisterNode("127.0.0.1:1234", 2048);
			rm.SubmitApp(ContainerMb);
			rm.SubmitApp(ContainerMb);
			amNodeManager.NodeHeartbeat(true);
			WebResource r = Resource();
			ClientResponse response = r.Path("ws").Path("v1").Path("cluster").Path("apps").QueryParam
				("user", UserGroupInformation.GetCurrentUser().GetShortUserName()).Accept(MediaType
				.ApplicationJson).Get<ClientResponse>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			JSONObject json = response.GetEntity<JSONObject>();
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, json.Length());
			JSONObject apps = json.GetJSONObject("apps");
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, apps.Length());
			JSONArray array = apps.GetJSONArray("app");
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 2, array.Length()
				);
			rm.Stop();
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAppsQueryQueue()
		{
			rm.Start();
			MockNM amNodeManager = rm.RegisterNode("127.0.0.1:1234", 2048);
			rm.SubmitApp(ContainerMb);
			rm.SubmitApp(ContainerMb);
			amNodeManager.NodeHeartbeat(true);
			WebResource r = Resource();
			ClientResponse response = r.Path("ws").Path("v1").Path("cluster").Path("apps").QueryParam
				("queue", "default").Accept(MediaType.ApplicationJson).Get<ClientResponse>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			JSONObject json = response.GetEntity<JSONObject>();
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, json.Length());
			JSONObject apps = json.GetJSONObject("apps");
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, apps.Length());
			JSONArray array = apps.GetJSONArray("app");
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 2, array.Length()
				);
			rm.Stop();
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAppsQueryLimit()
		{
			rm.Start();
			rm.RegisterNode("127.0.0.1:1234", 2048);
			rm.SubmitApp(ContainerMb);
			rm.SubmitApp(ContainerMb);
			rm.SubmitApp(ContainerMb);
			WebResource r = Resource();
			ClientResponse response = r.Path("ws").Path("v1").Path("cluster").Path("apps").QueryParam
				("limit", "2").Accept(MediaType.ApplicationJson).Get<ClientResponse>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			JSONObject json = response.GetEntity<JSONObject>();
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, json.Length());
			JSONObject apps = json.GetJSONObject("apps");
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, apps.Length());
			JSONArray array = apps.GetJSONArray("app");
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 2, array.Length()
				);
			rm.Stop();
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAppsQueryStartBegin()
		{
			rm.Start();
			long start = Runtime.CurrentTimeMillis();
			Sharpen.Thread.Sleep(1);
			rm.RegisterNode("127.0.0.1:1234", 2048);
			rm.SubmitApp(ContainerMb);
			rm.SubmitApp(ContainerMb);
			rm.SubmitApp(ContainerMb);
			WebResource r = Resource();
			ClientResponse response = r.Path("ws").Path("v1").Path("cluster").Path("apps").QueryParam
				("startedTimeBegin", start.ToString()).Accept(MediaType.ApplicationJson).Get<ClientResponse
				>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			JSONObject json = response.GetEntity<JSONObject>();
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, json.Length());
			JSONObject apps = json.GetJSONObject("apps");
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, apps.Length());
			JSONArray array = apps.GetJSONArray("app");
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 3, array.Length()
				);
			rm.Stop();
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAppsQueryStartBeginSome()
		{
			rm.Start();
			rm.RegisterNode("127.0.0.1:1234", 2048);
			rm.SubmitApp(ContainerMb);
			rm.SubmitApp(ContainerMb);
			long start = Runtime.CurrentTimeMillis();
			Sharpen.Thread.Sleep(1);
			rm.SubmitApp(ContainerMb);
			WebResource r = Resource();
			ClientResponse response = r.Path("ws").Path("v1").Path("cluster").Path("apps").QueryParam
				("startedTimeBegin", start.ToString()).Accept(MediaType.ApplicationJson).Get<ClientResponse
				>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			JSONObject json = response.GetEntity<JSONObject>();
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, json.Length());
			JSONObject apps = json.GetJSONObject("apps");
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, apps.Length());
			JSONArray array = apps.GetJSONArray("app");
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, array.Length()
				);
			rm.Stop();
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAppsQueryStartEnd()
		{
			rm.Start();
			rm.RegisterNode("127.0.0.1:1234", 2048);
			long end = Runtime.CurrentTimeMillis();
			Sharpen.Thread.Sleep(1);
			rm.SubmitApp(ContainerMb);
			rm.SubmitApp(ContainerMb);
			rm.SubmitApp(ContainerMb);
			WebResource r = Resource();
			ClientResponse response = r.Path("ws").Path("v1").Path("cluster").Path("apps").QueryParam
				("startedTimeEnd", end.ToString()).Accept(MediaType.ApplicationJson).Get<ClientResponse
				>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			JSONObject json = response.GetEntity<JSONObject>();
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, json.Length());
			NUnit.Framework.Assert.AreEqual("apps is not null", JSONObject.Null, json.Get("apps"
				));
			rm.Stop();
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAppsQueryStartBeginEnd()
		{
			rm.Start();
			rm.RegisterNode("127.0.0.1:1234", 2048);
			long start = Runtime.CurrentTimeMillis();
			Sharpen.Thread.Sleep(1);
			rm.SubmitApp(ContainerMb);
			rm.SubmitApp(ContainerMb);
			long end = Runtime.CurrentTimeMillis();
			Sharpen.Thread.Sleep(1);
			rm.SubmitApp(ContainerMb);
			WebResource r = Resource();
			ClientResponse response = r.Path("ws").Path("v1").Path("cluster").Path("apps").QueryParam
				("startedTimeBegin", start.ToString()).QueryParam("startedTimeEnd", end.ToString
				()).Accept(MediaType.ApplicationJson).Get<ClientResponse>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			JSONObject json = response.GetEntity<JSONObject>();
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, json.Length());
			JSONObject apps = json.GetJSONObject("apps");
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, apps.Length());
			JSONArray array = apps.GetJSONArray("app");
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 2, array.Length()
				);
			rm.Stop();
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAppsQueryFinishBegin()
		{
			rm.Start();
			MockNM amNodeManager = rm.RegisterNode("127.0.0.1:1234", 2048);
			long start = Runtime.CurrentTimeMillis();
			Sharpen.Thread.Sleep(1);
			RMApp app1 = rm.SubmitApp(ContainerMb);
			amNodeManager.NodeHeartbeat(true);
			// finish App
			MockAM am = rm.SendAMLaunched(app1.GetCurrentAppAttempt().GetAppAttemptId());
			am.RegisterAppAttempt();
			am.UnregisterAppAttempt();
			amNodeManager.NodeHeartbeat(app1.GetCurrentAppAttempt().GetAppAttemptId(), 1, ContainerState
				.Complete);
			rm.SubmitApp(ContainerMb);
			rm.SubmitApp(ContainerMb);
			WebResource r = Resource();
			ClientResponse response = r.Path("ws").Path("v1").Path("cluster").Path("apps").QueryParam
				("finishedTimeBegin", start.ToString()).Accept(MediaType.ApplicationJson).Get<ClientResponse
				>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			JSONObject json = response.GetEntity<JSONObject>();
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, json.Length());
			JSONObject apps = json.GetJSONObject("apps");
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, apps.Length());
			JSONArray array = apps.GetJSONArray("app");
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, array.Length()
				);
			rm.Stop();
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAppsQueryFinishEnd()
		{
			rm.Start();
			MockNM amNodeManager = rm.RegisterNode("127.0.0.1:1234", 2048);
			RMApp app1 = rm.SubmitApp(ContainerMb);
			amNodeManager.NodeHeartbeat(true);
			// finish App
			MockAM am = rm.SendAMLaunched(app1.GetCurrentAppAttempt().GetAppAttemptId());
			am.RegisterAppAttempt();
			am.UnregisterAppAttempt();
			amNodeManager.NodeHeartbeat(app1.GetCurrentAppAttempt().GetAppAttemptId(), 1, ContainerState
				.Complete);
			rm.SubmitApp(ContainerMb);
			rm.SubmitApp(ContainerMb);
			long end = Runtime.CurrentTimeMillis();
			WebResource r = Resource();
			ClientResponse response = r.Path("ws").Path("v1").Path("cluster").Path("apps").QueryParam
				("finishedTimeEnd", end.ToString()).Accept(MediaType.ApplicationJson).Get<ClientResponse
				>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			JSONObject json = response.GetEntity<JSONObject>();
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, json.Length());
			JSONObject apps = json.GetJSONObject("apps");
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, apps.Length());
			JSONArray array = apps.GetJSONArray("app");
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 3, array.Length()
				);
			rm.Stop();
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAppsQueryFinishBeginEnd()
		{
			rm.Start();
			MockNM amNodeManager = rm.RegisterNode("127.0.0.1:1234", 2048);
			long start = Runtime.CurrentTimeMillis();
			Sharpen.Thread.Sleep(1);
			RMApp app1 = rm.SubmitApp(ContainerMb);
			amNodeManager.NodeHeartbeat(true);
			// finish App
			MockAM am = rm.SendAMLaunched(app1.GetCurrentAppAttempt().GetAppAttemptId());
			am.RegisterAppAttempt();
			am.UnregisterAppAttempt();
			amNodeManager.NodeHeartbeat(app1.GetCurrentAppAttempt().GetAppAttemptId(), 1, ContainerState
				.Complete);
			rm.SubmitApp(ContainerMb);
			rm.SubmitApp(ContainerMb);
			long end = Runtime.CurrentTimeMillis();
			WebResource r = Resource();
			ClientResponse response = r.Path("ws").Path("v1").Path("cluster").Path("apps").QueryParam
				("finishedTimeBegin", start.ToString()).QueryParam("finishedTimeEnd", end.ToString
				()).Accept(MediaType.ApplicationJson).Get<ClientResponse>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			JSONObject json = response.GetEntity<JSONObject>();
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, json.Length());
			JSONObject apps = json.GetJSONObject("apps");
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, apps.Length());
			JSONArray array = apps.GetJSONArray("app");
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, array.Length()
				);
			rm.Stop();
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAppsQueryAppTypes()
		{
			rm.Start();
			MockNM amNodeManager = rm.RegisterNode("127.0.0.1:1234", 2048);
			Sharpen.Thread.Sleep(1);
			RMApp app1 = rm.SubmitApp(ContainerMb);
			amNodeManager.NodeHeartbeat(true);
			// finish App
			MockAM am = rm.SendAMLaunched(app1.GetCurrentAppAttempt().GetAppAttemptId());
			am.RegisterAppAttempt();
			am.UnregisterAppAttempt();
			amNodeManager.NodeHeartbeat(app1.GetCurrentAppAttempt().GetAppAttemptId(), 1, ContainerState
				.Complete);
			rm.SubmitApp(ContainerMb, string.Empty, UserGroupInformation.GetCurrentUser().GetShortUserName
				(), null, false, null, 2, null, "MAPREDUCE");
			rm.SubmitApp(ContainerMb, string.Empty, UserGroupInformation.GetCurrentUser().GetShortUserName
				(), null, false, null, 2, null, "NON-YARN");
			WebResource r = Resource();
			ClientResponse response = r.Path("ws").Path("v1").Path("cluster").Path("apps").QueryParam
				("applicationTypes", "MAPREDUCE").Accept(MediaType.ApplicationJson).Get<ClientResponse
				>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			JSONObject json = response.GetEntity<JSONObject>();
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, json.Length());
			JSONObject apps = json.GetJSONObject("apps");
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, apps.Length());
			JSONArray array = apps.GetJSONArray("app");
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, array.Length()
				);
			NUnit.Framework.Assert.AreEqual("MAPREDUCE", array.GetJSONObject(0).GetString("applicationType"
				));
			r = Resource();
			response = r.Path("ws").Path("v1").Path("cluster").Path("apps").QueryParam("applicationTypes"
				, "YARN").QueryParam("applicationTypes", "MAPREDUCE").Accept(MediaType.ApplicationJson
				).Get<ClientResponse>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			json = response.GetEntity<JSONObject>();
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, json.Length());
			apps = json.GetJSONObject("apps");
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, apps.Length());
			array = apps.GetJSONArray("app");
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 2, array.Length()
				);
			NUnit.Framework.Assert.IsTrue((array.GetJSONObject(0).GetString("applicationType"
				).Equals("YARN") && array.GetJSONObject(1).GetString("applicationType").Equals("MAPREDUCE"
				)) || (array.GetJSONObject(1).GetString("applicationType").Equals("YARN") && array
				.GetJSONObject(0).GetString("applicationType").Equals("MAPREDUCE")));
			r = Resource();
			response = r.Path("ws").Path("v1").Path("cluster").Path("apps").QueryParam("applicationTypes"
				, "YARN,NON-YARN").Accept(MediaType.ApplicationJson).Get<ClientResponse>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			json = response.GetEntity<JSONObject>();
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, json.Length());
			apps = json.GetJSONObject("apps");
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, apps.Length());
			array = apps.GetJSONArray("app");
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 2, array.Length()
				);
			NUnit.Framework.Assert.IsTrue((array.GetJSONObject(0).GetString("applicationType"
				).Equals("YARN") && array.GetJSONObject(1).GetString("applicationType").Equals("NON-YARN"
				)) || (array.GetJSONObject(1).GetString("applicationType").Equals("YARN") && array
				.GetJSONObject(0).GetString("applicationType").Equals("NON-YARN")));
			r = Resource();
			response = r.Path("ws").Path("v1").Path("cluster").Path("apps").QueryParam("applicationTypes"
				, string.Empty).Accept(MediaType.ApplicationJson).Get<ClientResponse>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			json = response.GetEntity<JSONObject>();
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, json.Length());
			apps = json.GetJSONObject("apps");
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, apps.Length());
			array = apps.GetJSONArray("app");
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 3, array.Length()
				);
			r = Resource();
			response = r.Path("ws").Path("v1").Path("cluster").Path("apps").QueryParam("applicationTypes"
				, "YARN,NON-YARN").QueryParam("applicationTypes", "MAPREDUCE").Accept(MediaType.
				ApplicationJson).Get<ClientResponse>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			json = response.GetEntity<JSONObject>();
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, json.Length());
			apps = json.GetJSONObject("apps");
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, apps.Length());
			array = apps.GetJSONArray("app");
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 3, array.Length()
				);
			r = Resource();
			response = r.Path("ws").Path("v1").Path("cluster").Path("apps").QueryParam("applicationTypes"
				, "YARN").QueryParam("applicationTypes", string.Empty).Accept(MediaType.ApplicationJson
				).Get<ClientResponse>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			json = response.GetEntity<JSONObject>();
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, json.Length());
			apps = json.GetJSONObject("apps");
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, apps.Length());
			array = apps.GetJSONArray("app");
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, array.Length()
				);
			NUnit.Framework.Assert.AreEqual("YARN", array.GetJSONObject(0).GetString("applicationType"
				));
			r = Resource();
			response = r.Path("ws").Path("v1").Path("cluster").Path("apps").QueryParam("applicationTypes"
				, ",,, ,, YARN ,, ,").Accept(MediaType.ApplicationJson).Get<ClientResponse>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			json = response.GetEntity<JSONObject>();
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, json.Length());
			apps = json.GetJSONObject("apps");
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, apps.Length());
			array = apps.GetJSONArray("app");
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, array.Length()
				);
			NUnit.Framework.Assert.AreEqual("YARN", array.GetJSONObject(0).GetString("applicationType"
				));
			r = Resource();
			response = r.Path("ws").Path("v1").Path("cluster").Path("apps").QueryParam("applicationTypes"
				, ",,, ,,  ,, ,").Accept(MediaType.ApplicationJson).Get<ClientResponse>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			json = response.GetEntity<JSONObject>();
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, json.Length());
			apps = json.GetJSONObject("apps");
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, apps.Length());
			array = apps.GetJSONArray("app");
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 3, array.Length()
				);
			r = Resource();
			response = r.Path("ws").Path("v1").Path("cluster").Path("apps").QueryParam("applicationTypes"
				, "YARN, ,NON-YARN, ,,").Accept(MediaType.ApplicationJson).Get<ClientResponse>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			json = response.GetEntity<JSONObject>();
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, json.Length());
			apps = json.GetJSONObject("apps");
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, apps.Length());
			array = apps.GetJSONArray("app");
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 2, array.Length()
				);
			NUnit.Framework.Assert.IsTrue((array.GetJSONObject(0).GetString("applicationType"
				).Equals("YARN") && array.GetJSONObject(1).GetString("applicationType").Equals("NON-YARN"
				)) || (array.GetJSONObject(1).GetString("applicationType").Equals("YARN") && array
				.GetJSONObject(0).GetString("applicationType").Equals("NON-YARN")));
			r = Resource();
			response = r.Path("ws").Path("v1").Path("cluster").Path("apps").QueryParam("applicationTypes"
				, " YARN, ,  ,,,").QueryParam("applicationTypes", "MAPREDUCE , ,, ,").Accept(MediaType
				.ApplicationJson).Get<ClientResponse>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			json = response.GetEntity<JSONObject>();
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, json.Length());
			apps = json.GetJSONObject("apps");
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, apps.Length());
			array = apps.GetJSONArray("app");
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 2, array.Length()
				);
			NUnit.Framework.Assert.IsTrue((array.GetJSONObject(0).GetString("applicationType"
				).Equals("YARN") && array.GetJSONObject(1).GetString("applicationType").Equals("MAPREDUCE"
				)) || (array.GetJSONObject(1).GetString("applicationType").Equals("YARN") && array
				.GetJSONObject(0).GetString("applicationType").Equals("MAPREDUCE")));
			rm.Stop();
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAppStatistics()
		{
			try
			{
				rm.Start();
				MockNM amNodeManager = rm.RegisterNode("127.0.0.1:1234", 4096);
				Sharpen.Thread.Sleep(1);
				RMApp app1 = rm.SubmitApp(ContainerMb, string.Empty, UserGroupInformation.GetCurrentUser
					().GetShortUserName(), null, false, null, 2, null, "MAPREDUCE");
				amNodeManager.NodeHeartbeat(true);
				// finish App
				MockAM am = rm.SendAMLaunched(app1.GetCurrentAppAttempt().GetAppAttemptId());
				am.RegisterAppAttempt();
				am.UnregisterAppAttempt();
				amNodeManager.NodeHeartbeat(app1.GetCurrentAppAttempt().GetAppAttemptId(), 1, ContainerState
					.Complete);
				rm.SubmitApp(ContainerMb, string.Empty, UserGroupInformation.GetCurrentUser().GetShortUserName
					(), null, false, null, 2, null, "MAPREDUCE");
				rm.SubmitApp(ContainerMb, string.Empty, UserGroupInformation.GetCurrentUser().GetShortUserName
					(), null, false, null, 2, null, "OTHER");
				// zero type, zero state
				WebResource r = Resource();
				ClientResponse response = r.Path("ws").Path("v1").Path("cluster").Path("appstatistics"
					).Accept(MediaType.ApplicationJson).Get<ClientResponse>();
				NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
					);
				JSONObject json = response.GetEntity<JSONObject>();
				NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, json.Length());
				JSONObject appsStatInfo = json.GetJSONObject("appStatInfo");
				NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, appsStatInfo.Length
					());
				JSONArray statItems = appsStatInfo.GetJSONArray("statItem");
				NUnit.Framework.Assert.AreEqual("incorrect number of elements", YarnApplicationState
					.Values().Length, statItems.Length());
				for (int i = 0; i < YarnApplicationState.Values().Length; ++i)
				{
					NUnit.Framework.Assert.AreEqual("*", statItems.GetJSONObject(0).GetString("type")
						);
					if (statItems.GetJSONObject(0).GetString("state").Equals("ACCEPTED"))
					{
						NUnit.Framework.Assert.AreEqual("2", statItems.GetJSONObject(0).GetString("count"
							));
					}
					else
					{
						if (statItems.GetJSONObject(0).GetString("state").Equals("FINISHED"))
						{
							NUnit.Framework.Assert.AreEqual("1", statItems.GetJSONObject(0).GetString("count"
								));
						}
						else
						{
							NUnit.Framework.Assert.AreEqual("0", statItems.GetJSONObject(0).GetString("count"
								));
						}
					}
				}
				// zero type, one state
				r = Resource();
				response = r.Path("ws").Path("v1").Path("cluster").Path("appstatistics").QueryParam
					("states", YarnApplicationState.Accepted.ToString()).Accept(MediaType.ApplicationJson
					).Get<ClientResponse>();
				NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
					);
				json = response.GetEntity<JSONObject>();
				NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, json.Length());
				appsStatInfo = json.GetJSONObject("appStatInfo");
				NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, appsStatInfo.Length
					());
				statItems = appsStatInfo.GetJSONArray("statItem");
				NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, statItems.Length
					());
				NUnit.Framework.Assert.AreEqual("ACCEPTED", statItems.GetJSONObject(0).GetString(
					"state"));
				NUnit.Framework.Assert.AreEqual("*", statItems.GetJSONObject(0).GetString("type")
					);
				NUnit.Framework.Assert.AreEqual("2", statItems.GetJSONObject(0).GetString("count"
					));
				// one type, zero state
				r = Resource();
				response = r.Path("ws").Path("v1").Path("cluster").Path("appstatistics").QueryParam
					("applicationTypes", "MAPREDUCE").Accept(MediaType.ApplicationJson).Get<ClientResponse
					>();
				NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
					);
				json = response.GetEntity<JSONObject>();
				NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, json.Length());
				appsStatInfo = json.GetJSONObject("appStatInfo");
				NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, appsStatInfo.Length
					());
				statItems = appsStatInfo.GetJSONArray("statItem");
				NUnit.Framework.Assert.AreEqual("incorrect number of elements", YarnApplicationState
					.Values().Length, statItems.Length());
				for (int i_1 = 0; i_1 < YarnApplicationState.Values().Length; ++i_1)
				{
					NUnit.Framework.Assert.AreEqual("mapreduce", statItems.GetJSONObject(0).GetString
						("type"));
					if (statItems.GetJSONObject(0).GetString("state").Equals("ACCEPTED"))
					{
						NUnit.Framework.Assert.AreEqual("1", statItems.GetJSONObject(0).GetString("count"
							));
					}
					else
					{
						if (statItems.GetJSONObject(0).GetString("state").Equals("FINISHED"))
						{
							NUnit.Framework.Assert.AreEqual("1", statItems.GetJSONObject(0).GetString("count"
								));
						}
						else
						{
							NUnit.Framework.Assert.AreEqual("0", statItems.GetJSONObject(0).GetString("count"
								));
						}
					}
				}
				// two types, zero state
				r = Resource();
				response = r.Path("ws").Path("v1").Path("cluster").Path("appstatistics").QueryParam
					("applicationTypes", "MAPREDUCE,OTHER").Accept(MediaType.ApplicationJson).Get<ClientResponse
					>();
				NUnit.Framework.Assert.AreEqual(ClientResponse.Status.BadRequest, response.GetClientResponseStatus
					());
				NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
					);
				json = response.GetEntity<JSONObject>();
				NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, json.Length());
				JSONObject exception = json.GetJSONObject("RemoteException");
				NUnit.Framework.Assert.AreEqual("incorrect number of elements", 3, exception.Length
					());
				string message = exception.GetString("message");
				string type = exception.GetString("exception");
				string className = exception.GetString("javaClassName");
				WebServicesTestUtils.CheckStringContains("exception message", "we temporarily support at most one applicationType"
					, message);
				WebServicesTestUtils.CheckStringEqual("exception type", "BadRequestException", type
					);
				WebServicesTestUtils.CheckStringEqual("exception className", "org.apache.hadoop.yarn.webapp.BadRequestException"
					, className);
				// one type, two states
				r = Resource();
				response = r.Path("ws").Path("v1").Path("cluster").Path("appstatistics").QueryParam
					("states", YarnApplicationState.Finished.ToString() + "," + YarnApplicationState
					.Accepted.ToString()).QueryParam("applicationTypes", "MAPREDUCE").Accept(MediaType
					.ApplicationJson).Get<ClientResponse>();
				NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
					);
				json = response.GetEntity<JSONObject>();
				NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, json.Length());
				appsStatInfo = json.GetJSONObject("appStatInfo");
				NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, appsStatInfo.Length
					());
				statItems = appsStatInfo.GetJSONArray("statItem");
				NUnit.Framework.Assert.AreEqual("incorrect number of elements", 2, statItems.Length
					());
				JSONObject statItem1 = statItems.GetJSONObject(0);
				JSONObject statItem2 = statItems.GetJSONObject(1);
				NUnit.Framework.Assert.IsTrue((statItem1.GetString("state").Equals("ACCEPTED") &&
					 statItem2.GetString("state").Equals("FINISHED")) || (statItem2.GetString("state"
					).Equals("ACCEPTED") && statItem1.GetString("state").Equals("FINISHED")));
				NUnit.Framework.Assert.AreEqual("mapreduce", statItem1.GetString("type"));
				NUnit.Framework.Assert.AreEqual("1", statItem1.GetString("count"));
				NUnit.Framework.Assert.AreEqual("mapreduce", statItem2.GetString("type"));
				NUnit.Framework.Assert.AreEqual("1", statItem2.GetString("count"));
				// invalid state
				r = Resource();
				response = r.Path("ws").Path("v1").Path("cluster").Path("appstatistics").QueryParam
					("states", "wrong_state").Accept(MediaType.ApplicationJson).Get<ClientResponse>(
					);
				NUnit.Framework.Assert.AreEqual(ClientResponse.Status.BadRequest, response.GetClientResponseStatus
					());
				NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
					);
				json = response.GetEntity<JSONObject>();
				NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, json.Length());
				exception = json.GetJSONObject("RemoteException");
				NUnit.Framework.Assert.AreEqual("incorrect number of elements", 3, exception.Length
					());
				message = exception.GetString("message");
				type = exception.GetString("exception");
				className = exception.GetString("javaClassName");
				WebServicesTestUtils.CheckStringContains("exception message", "Invalid application-state wrong_state"
					, message);
				WebServicesTestUtils.CheckStringEqual("exception type", "BadRequestException", type
					);
				WebServicesTestUtils.CheckStringEqual("exception className", "org.apache.hadoop.yarn.webapp.BadRequestException"
					, className);
			}
			finally
			{
				rm.Stop();
			}
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestSingleApp()
		{
			rm.Start();
			MockNM amNodeManager = rm.RegisterNode("127.0.0.1:1234", 2048);
			RMApp app1 = rm.SubmitApp(ContainerMb, "testwordcount", "user1");
			amNodeManager.NodeHeartbeat(true);
			TestSingleAppsHelper(app1.GetApplicationId().ToString(), app1, MediaType.ApplicationJson
				);
			rm.Stop();
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestSingleAppsSlash()
		{
			rm.Start();
			MockNM amNodeManager = rm.RegisterNode("127.0.0.1:1234", 2048);
			RMApp app1 = rm.SubmitApp(ContainerMb);
			amNodeManager.NodeHeartbeat(true);
			TestSingleAppsHelper(app1.GetApplicationId().ToString() + "/", app1, MediaType.ApplicationJson
				);
			rm.Stop();
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestSingleAppsDefault()
		{
			rm.Start();
			MockNM amNodeManager = rm.RegisterNode("127.0.0.1:1234", 2048);
			RMApp app1 = rm.SubmitApp(ContainerMb);
			amNodeManager.NodeHeartbeat(true);
			TestSingleAppsHelper(app1.GetApplicationId().ToString() + "/", app1, string.Empty
				);
			rm.Stop();
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestInvalidApp()
		{
			rm.Start();
			MockNM amNodeManager = rm.RegisterNode("127.0.0.1:1234", 2048);
			rm.SubmitApp(ContainerMb);
			amNodeManager.NodeHeartbeat(true);
			WebResource r = Resource();
			try
			{
				r.Path("ws").Path("v1").Path("cluster").Path("apps").Path("application_invalid_12"
					).Accept(MediaType.ApplicationJson).Get<JSONObject>();
				NUnit.Framework.Assert.Fail("should have thrown exception on invalid appid");
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
				WebServicesTestUtils.CheckStringMatch("exception message", "For input string: \"invalid\""
					, message);
				WebServicesTestUtils.CheckStringMatch("exception type", "NumberFormatException", 
					type);
				WebServicesTestUtils.CheckStringMatch("exception classname", "java.lang.NumberFormatException"
					, classname);
			}
			finally
			{
				rm.Stop();
			}
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNonexistApp()
		{
			rm.Start();
			MockNM amNodeManager = rm.RegisterNode("127.0.0.1:1234", 2048);
			rm.SubmitApp(ContainerMb, "testwordcount", "user1");
			amNodeManager.NodeHeartbeat(true);
			WebResource r = Resource();
			try
			{
				r.Path("ws").Path("v1").Path("cluster").Path("apps").Path("application_00000_0099"
					).Accept(MediaType.ApplicationJson).Get<JSONObject>();
				NUnit.Framework.Assert.Fail("should have thrown exception on invalid appid");
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
				WebServicesTestUtils.CheckStringMatch("exception message", "java.lang.Exception: app with id: application_00000_0099 not found"
					, message);
				WebServicesTestUtils.CheckStringMatch("exception type", "NotFoundException", type
					);
				WebServicesTestUtils.CheckStringMatch("exception classname", "org.apache.hadoop.yarn.webapp.NotFoundException"
					, classname);
			}
			finally
			{
				rm.Stop();
			}
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		public virtual void TestSingleAppsHelper(string path, RMApp app, string media)
		{
			WebResource r = Resource();
			ClientResponse response = r.Path("ws").Path("v1").Path("cluster").Path("apps").Path
				(path).Accept(media).Get<ClientResponse>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			JSONObject json = response.GetEntity<JSONObject>();
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, json.Length());
			VerifyAppInfo(json.GetJSONObject("app"), app);
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestSingleAppsXML()
		{
			rm.Start();
			MockNM amNodeManager = rm.RegisterNode("127.0.0.1:1234", 2048);
			RMApp app1 = rm.SubmitApp(ContainerMb, "testwordcount", "user1");
			amNodeManager.NodeHeartbeat(true);
			WebResource r = Resource();
			ClientResponse response = r.Path("ws").Path("v1").Path("cluster").Path("apps").Path
				(app1.GetApplicationId().ToString()).Accept(MediaType.ApplicationXml).Get<ClientResponse
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
			VerifyAppsXML(nodes, app1);
			rm.Stop();
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		public virtual void VerifyAppsXML(NodeList nodes, RMApp app)
		{
			for (int i = 0; i < nodes.GetLength(); i++)
			{
				Element element = (Element)nodes.Item(i);
				VerifyAppInfoGeneric(app, WebServicesTestUtils.GetXmlString(element, "id"), WebServicesTestUtils
					.GetXmlString(element, "user"), WebServicesTestUtils.GetXmlString(element, "name"
					), WebServicesTestUtils.GetXmlString(element, "applicationType"), WebServicesTestUtils
					.GetXmlString(element, "queue"), WebServicesTestUtils.GetXmlString(element, "state"
					), WebServicesTestUtils.GetXmlString(element, "finalStatus"), WebServicesTestUtils
					.GetXmlFloat(element, "progress"), WebServicesTestUtils.GetXmlString(element, "trackingUI"
					), WebServicesTestUtils.GetXmlString(element, "diagnostics"), WebServicesTestUtils
					.GetXmlLong(element, "clusterId"), WebServicesTestUtils.GetXmlLong(element, "startedTime"
					), WebServicesTestUtils.GetXmlLong(element, "finishedTime"), WebServicesTestUtils
					.GetXmlLong(element, "elapsedTime"), WebServicesTestUtils.GetXmlString(element, 
					"amHostHttpAddress"), WebServicesTestUtils.GetXmlString(element, "amContainerLogs"
					), WebServicesTestUtils.GetXmlInt(element, "allocatedMB"), WebServicesTestUtils.
					GetXmlInt(element, "allocatedVCores"), WebServicesTestUtils.GetXmlInt(element, "runningContainers"
					), WebServicesTestUtils.GetXmlInt(element, "preemptedResourceMB"), WebServicesTestUtils
					.GetXmlInt(element, "preemptedResourceVCores"), WebServicesTestUtils.GetXmlInt(element
					, "numNonAMContainerPreempted"), WebServicesTestUtils.GetXmlInt(element, "numAMContainerPreempted"
					));
			}
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		public virtual void VerifyAppInfo(JSONObject info, RMApp app)
		{
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 27, info.Length()
				);
			VerifyAppInfoGeneric(app, info.GetString("id"), info.GetString("user"), info.GetString
				("name"), info.GetString("applicationType"), info.GetString("queue"), info.GetString
				("state"), info.GetString("finalStatus"), (float)info.GetDouble("progress"), info
				.GetString("trackingUI"), info.GetString("diagnostics"), info.GetLong("clusterId"
				), info.GetLong("startedTime"), info.GetLong("finishedTime"), info.GetLong("elapsedTime"
				), info.GetString("amHostHttpAddress"), info.GetString("amContainerLogs"), info.
				GetInt("allocatedMB"), info.GetInt("allocatedVCores"), info.GetInt("runningContainers"
				), info.GetInt("preemptedResourceMB"), info.GetInt("preemptedResourceVCores"), info
				.GetInt("numNonAMContainerPreempted"), info.GetInt("numAMContainerPreempted"));
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		public virtual void VerifyAppInfoGeneric(RMApp app, string id, string user, string
			 name, string applicationType, string queue, string state, string finalStatus, float
			 progress, string trackingUI, string diagnostics, long clusterId, long startedTime
			, long finishedTime, long elapsedTime, string amHostHttpAddress, string amContainerLogs
			, int allocatedMB, int allocatedVCores, int numContainers, int preemptedResourceMB
			, int preemptedResourceVCores, int numNonAMContainerPreempted, int numAMContainerPreempted
			)
		{
			WebServicesTestUtils.CheckStringMatch("id", app.GetApplicationId().ToString(), id
				);
			WebServicesTestUtils.CheckStringMatch("user", app.GetUser(), user);
			WebServicesTestUtils.CheckStringMatch("name", app.GetName(), name);
			WebServicesTestUtils.CheckStringMatch("applicationType", app.GetApplicationType()
				, applicationType);
			WebServicesTestUtils.CheckStringMatch("queue", app.GetQueue(), queue);
			WebServicesTestUtils.CheckStringMatch("state", app.GetState().ToString(), state);
			WebServicesTestUtils.CheckStringMatch("finalStatus", app.GetFinalApplicationStatus
				().ToString(), finalStatus);
			NUnit.Framework.Assert.AreEqual("progress doesn't match", 0, progress, 0.0);
			WebServicesTestUtils.CheckStringMatch("trackingUI", "UNASSIGNED", trackingUI);
			WebServicesTestUtils.CheckStringMatch("diagnostics", app.GetDiagnostics().ToString
				(), diagnostics);
			NUnit.Framework.Assert.AreEqual("clusterId doesn't match", ResourceManager.GetClusterTimeStamp
				(), clusterId);
			NUnit.Framework.Assert.AreEqual("startedTime doesn't match", app.GetStartTime(), 
				startedTime);
			NUnit.Framework.Assert.AreEqual("finishedTime doesn't match", app.GetFinishTime()
				, finishedTime);
			NUnit.Framework.Assert.IsTrue("elapsed time not greater than 0", elapsedTime > 0);
			WebServicesTestUtils.CheckStringMatch("amHostHttpAddress", app.GetCurrentAppAttempt
				().GetMasterContainer().GetNodeHttpAddress(), amHostHttpAddress);
			NUnit.Framework.Assert.IsTrue("amContainerLogs doesn't match", amContainerLogs.StartsWith
				("http://"));
			NUnit.Framework.Assert.IsTrue("amContainerLogs doesn't contain user info", amContainerLogs
				.EndsWith("/" + app.GetUser()));
			NUnit.Framework.Assert.AreEqual("allocatedMB doesn't match", 1024, allocatedMB);
			NUnit.Framework.Assert.AreEqual("allocatedVCores doesn't match", 1, allocatedVCores
				);
			NUnit.Framework.Assert.AreEqual("numContainers doesn't match", 1, numContainers);
			NUnit.Framework.Assert.AreEqual("preemptedResourceMB doesn't match", app.GetRMAppMetrics
				().GetResourcePreempted().GetMemory(), preemptedResourceMB);
			NUnit.Framework.Assert.AreEqual("preemptedResourceVCores doesn't match", app.GetRMAppMetrics
				().GetResourcePreempted().GetVirtualCores(), preemptedResourceVCores);
			NUnit.Framework.Assert.AreEqual("numNonAMContainerPreempted doesn't match", app.GetRMAppMetrics
				().GetNumNonAMContainersPreempted(), numNonAMContainerPreempted);
			NUnit.Framework.Assert.AreEqual("numAMContainerPreempted doesn't match", app.GetRMAppMetrics
				().GetNumAMContainersPreempted(), numAMContainerPreempted);
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAppAttempts()
		{
			rm.Start();
			MockNM amNodeManager = rm.RegisterNode("127.0.0.1:1234", 2048);
			RMApp app1 = rm.SubmitApp(ContainerMb, "testwordcount", "user1");
			amNodeManager.NodeHeartbeat(true);
			TestAppAttemptsHelper(app1.GetApplicationId().ToString(), app1, MediaType.ApplicationJson
				);
			rm.Stop();
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		public virtual void TestMultipleAppAttempts()
		{
			rm.Start();
			MockNM amNodeManager = rm.RegisterNode("127.0.0.1:1234", 8192);
			RMApp app1 = rm.SubmitApp(ContainerMb, "testwordcount", "user1");
			MockAM am = MockRM.LaunchAndRegisterAM(app1, rm, amNodeManager);
			int maxAppAttempts = rm.GetConfig().GetInt(YarnConfiguration.RmAmMaxAttempts, YarnConfiguration
				.DefaultRmAmMaxAttempts);
			NUnit.Framework.Assert.IsTrue(maxAppAttempts > 1);
			int numAttempt = 1;
			while (true)
			{
				// fail the AM by sending CONTAINER_FINISHED event without registering.
				amNodeManager.NodeHeartbeat(am.GetApplicationAttemptId(), 1, ContainerState.Complete
					);
				am.WaitForState(RMAppAttemptState.Failed);
				if (numAttempt == maxAppAttempts)
				{
					rm.WaitForState(app1.GetApplicationId(), RMAppState.Failed);
					break;
				}
				// wait for app to start a new attempt.
				rm.WaitForState(app1.GetApplicationId(), RMAppState.Accepted);
				am = MockRM.LaunchAndRegisterAM(app1, rm, amNodeManager);
				numAttempt++;
			}
			NUnit.Framework.Assert.AreEqual("incorrect number of attempts", maxAppAttempts, app1
				.GetAppAttempts().Values.Count);
			TestAppAttemptsHelper(app1.GetApplicationId().ToString(), app1, MediaType.ApplicationJson
				);
			rm.Stop();
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAppAttemptsSlash()
		{
			rm.Start();
			MockNM amNodeManager = rm.RegisterNode("127.0.0.1:1234", 2048);
			RMApp app1 = rm.SubmitApp(ContainerMb);
			amNodeManager.NodeHeartbeat(true);
			TestAppAttemptsHelper(app1.GetApplicationId().ToString() + "/", app1, MediaType.ApplicationJson
				);
			rm.Stop();
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAppAttemtpsDefault()
		{
			rm.Start();
			MockNM amNodeManager = rm.RegisterNode("127.0.0.1:1234", 2048);
			RMApp app1 = rm.SubmitApp(ContainerMb);
			amNodeManager.NodeHeartbeat(true);
			TestAppAttemptsHelper(app1.GetApplicationId().ToString() + "/", app1, string.Empty
				);
			rm.Stop();
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestInvalidAppAttempts()
		{
			rm.Start();
			MockNM amNodeManager = rm.RegisterNode("127.0.0.1:1234", 2048);
			rm.SubmitApp(ContainerMb);
			amNodeManager.NodeHeartbeat(true);
			WebResource r = Resource();
			try
			{
				r.Path("ws").Path("v1").Path("cluster").Path("apps").Path("application_invalid_12"
					).Accept(MediaType.ApplicationJson).Get<JSONObject>();
				NUnit.Framework.Assert.Fail("should have thrown exception on invalid appid");
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
				WebServicesTestUtils.CheckStringMatch("exception message", "For input string: \"invalid\""
					, message);
				WebServicesTestUtils.CheckStringMatch("exception type", "NumberFormatException", 
					type);
				WebServicesTestUtils.CheckStringMatch("exception classname", "java.lang.NumberFormatException"
					, classname);
			}
			finally
			{
				rm.Stop();
			}
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNonexistAppAttempts()
		{
			rm.Start();
			MockNM amNodeManager = rm.RegisterNode("127.0.0.1:1234", 2048);
			rm.SubmitApp(ContainerMb, "testwordcount", "user1");
			amNodeManager.NodeHeartbeat(true);
			WebResource r = Resource();
			try
			{
				r.Path("ws").Path("v1").Path("cluster").Path("apps").Path("application_00000_0099"
					).Accept(MediaType.ApplicationJson).Get<JSONObject>();
				NUnit.Framework.Assert.Fail("should have thrown exception on invalid appid");
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
				WebServicesTestUtils.CheckStringMatch("exception message", "java.lang.Exception: app with id: application_00000_0099 not found"
					, message);
				WebServicesTestUtils.CheckStringMatch("exception type", "NotFoundException", type
					);
				WebServicesTestUtils.CheckStringMatch("exception classname", "org.apache.hadoop.yarn.webapp.NotFoundException"
					, classname);
			}
			finally
			{
				rm.Stop();
			}
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		public virtual void TestAppAttemptsHelper(string path, RMApp app, string media)
		{
			WebResource r = Resource();
			ClientResponse response = r.Path("ws").Path("v1").Path("cluster").Path("apps").Path
				(path).Path("appattempts").Accept(media).Get<ClientResponse>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			JSONObject json = response.GetEntity<JSONObject>();
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, json.Length());
			JSONObject jsonAppAttempts = json.GetJSONObject("appAttempts");
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, jsonAppAttempts
				.Length());
			JSONArray jsonArray = jsonAppAttempts.GetJSONArray("appAttempt");
			ICollection<RMAppAttempt> attempts = app.GetAppAttempts().Values;
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", attempts.Count, jsonArray
				.Length());
			// Verify these parallel arrays are the same
			int i = 0;
			foreach (RMAppAttempt attempt in attempts)
			{
				VerifyAppAttemptsInfo(jsonArray.GetJSONObject(i), attempt, app.GetUser());
				++i;
			}
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAppAttemptsXML()
		{
			rm.Start();
			string user = "user1";
			MockNM amNodeManager = rm.RegisterNode("127.0.0.1:1234", 2048);
			RMApp app1 = rm.SubmitApp(ContainerMb, "testwordcount", user);
			amNodeManager.NodeHeartbeat(true);
			WebResource r = Resource();
			ClientResponse response = r.Path("ws").Path("v1").Path("cluster").Path("apps").Path
				(app1.GetApplicationId().ToString()).Path("appattempts").Accept(MediaType.ApplicationXml
				).Get<ClientResponse>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationXmlType, response.GetType());
			string xml = response.GetEntity<string>();
			DocumentBuilderFactory dbf = DocumentBuilderFactory.NewInstance();
			DocumentBuilder db = dbf.NewDocumentBuilder();
			InputSource @is = new InputSource();
			@is.SetCharacterStream(new StringReader(xml));
			Document dom = db.Parse(@is);
			NodeList nodes = dom.GetElementsByTagName("appAttempts");
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, nodes.GetLength
				());
			NodeList attempt = dom.GetElementsByTagName("appAttempt");
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, attempt.GetLength
				());
			VerifyAppAttemptsXML(attempt, app1.GetCurrentAppAttempt(), user);
			rm.Stop();
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		public virtual void VerifyAppAttemptsXML(NodeList nodes, RMAppAttempt appAttempt, 
			string user)
		{
			for (int i = 0; i < nodes.GetLength(); i++)
			{
				Element element = (Element)nodes.Item(i);
				VerifyAppAttemptInfoGeneric(appAttempt, WebServicesTestUtils.GetXmlInt(element, "id"
					), WebServicesTestUtils.GetXmlLong(element, "startTime"), WebServicesTestUtils.GetXmlString
					(element, "containerId"), WebServicesTestUtils.GetXmlString(element, "nodeHttpAddress"
					), WebServicesTestUtils.GetXmlString(element, "nodeId"), WebServicesTestUtils.GetXmlString
					(element, "logsLink"), user);
			}
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		public virtual void VerifyAppAttemptsInfo(JSONObject info, RMAppAttempt appAttempt
			, string user)
		{
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 7, info.Length());
			VerifyAppAttemptInfoGeneric(appAttempt, info.GetInt("id"), info.GetLong("startTime"
				), info.GetString("containerId"), info.GetString("nodeHttpAddress"), info.GetString
				("nodeId"), info.GetString("logsLink"), user);
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		public virtual void VerifyAppAttemptInfoGeneric(RMAppAttempt appAttempt, int id, 
			long startTime, string containerId, string nodeHttpAddress, string nodeId, string
			 logsLink, string user)
		{
			NUnit.Framework.Assert.AreEqual("id doesn't match", appAttempt.GetAppAttemptId().
				GetAttemptId(), id);
			NUnit.Framework.Assert.AreEqual("startedTime doesn't match", appAttempt.GetStartTime
				(), startTime);
			WebServicesTestUtils.CheckStringMatch("containerId", appAttempt.GetMasterContainer
				().GetId().ToString(), containerId);
			WebServicesTestUtils.CheckStringMatch("nodeHttpAddress", appAttempt.GetMasterContainer
				().GetNodeHttpAddress(), nodeHttpAddress);
			WebServicesTestUtils.CheckStringMatch("nodeId", appAttempt.GetMasterContainer().GetNodeId
				().ToString(), nodeId);
			NUnit.Framework.Assert.IsTrue("logsLink doesn't match ", logsLink.StartsWith("http://"
				));
			NUnit.Framework.Assert.IsTrue("logsLink doesn't contain user info", logsLink.EndsWith
				("/" + user));
		}
	}
}
