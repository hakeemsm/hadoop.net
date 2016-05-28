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
using Org.Apache.Hadoop.Mapreduce.V2.App;
using Org.Apache.Hadoop.Mapreduce.V2.HS;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn.Webapp;
using Org.Codehaus.Jettison.Json;
using Org.W3c.Dom;
using Org.Xml.Sax;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.HS.Webapp
{
	/// <summary>Test the History Server info web services api's.</summary>
	/// <remarks>
	/// Test the History Server info web services api's. Also test non-existent urls.
	/// /ws/v1/history
	/// /ws/v1/history/info
	/// </remarks>
	public class TestHsWebServices : JerseyTest
	{
		private static Configuration conf = new Configuration();

		private static HistoryContext appContext;

		private static HsWebApp webApp;

		private sealed class _ServletModule_73 : ServletModule
		{
			public _ServletModule_73()
			{
			}

			protected override void ConfigureServlets()
			{
				Org.Apache.Hadoop.Mapreduce.V2.HS.Webapp.TestHsWebServices.appContext = new MockHistoryContext
					(0, 1, 1, 1);
				JobHistory jobHistoryService = new JobHistory();
				HistoryContext historyContext = (HistoryContext)jobHistoryService;
				Org.Apache.Hadoop.Mapreduce.V2.HS.Webapp.TestHsWebServices.webApp = new HsWebApp(
					historyContext);
				this.Bind<JAXBContextResolver>();
				this.Bind<HsWebServices>();
				this.Bind<GenericExceptionHandler>();
				this.Bind<WebApp>().ToInstance(Org.Apache.Hadoop.Mapreduce.V2.HS.Webapp.TestHsWebServices
					.webApp);
				this.Bind<AppContext>().ToInstance(Org.Apache.Hadoop.Mapreduce.V2.HS.Webapp.TestHsWebServices
					.appContext);
				this.Bind<HistoryContext>().ToInstance(Org.Apache.Hadoop.Mapreduce.V2.HS.Webapp.TestHsWebServices
					.appContext);
				this.Bind<Configuration>().ToInstance(Org.Apache.Hadoop.Mapreduce.V2.HS.Webapp.TestHsWebServices
					.conf);
				this.Serve("/*").With(typeof(GuiceContainer));
			}
		}

		private Injector injector = Guice.CreateInjector(new _ServletModule_73());

		public class GuiceServletConfig : GuiceServletContextListener
		{
			protected override Injector GetInjector()
			{
				return this._enclosing.injector;
			}

			internal GuiceServletConfig(TestHsWebServices _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly TestHsWebServices _enclosing;
		}

		/// <exception cref="System.Exception"/>
		[SetUp]
		public override void SetUp()
		{
			base.SetUp();
		}

		public TestHsWebServices()
			: base(new WebAppDescriptor.Builder("org.apache.hadoop.mapreduce.v2.hs.webapp").ContextListenerClass
				(typeof(TestHsWebServices.GuiceServletConfig)).FilterClass(typeof(GuiceFilter)).
				ContextPath("jersey-guice-filter").ServletPath("/").Build())
		{
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestHS()
		{
			WebResource r = Resource();
			ClientResponse response = r.Path("ws").Path("v1").Path("history").Accept(MediaType
				.ApplicationJson).Get<ClientResponse>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			JSONObject json = response.GetEntity<JSONObject>();
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, json.Length());
			VerifyHSInfo(json.GetJSONObject("historyInfo"), appContext);
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestHSSlash()
		{
			WebResource r = Resource();
			ClientResponse response = r.Path("ws").Path("v1").Path("history/").Accept(MediaType
				.ApplicationJson).Get<ClientResponse>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			JSONObject json = response.GetEntity<JSONObject>();
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, json.Length());
			VerifyHSInfo(json.GetJSONObject("historyInfo"), appContext);
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestHSDefault()
		{
			WebResource r = Resource();
			ClientResponse response = r.Path("ws").Path("v1").Path("history/").Get<ClientResponse
				>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			JSONObject json = response.GetEntity<JSONObject>();
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, json.Length());
			VerifyHSInfo(json.GetJSONObject("historyInfo"), appContext);
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestHSXML()
		{
			WebResource r = Resource();
			ClientResponse response = r.Path("ws").Path("v1").Path("history").Accept(MediaType
				.ApplicationXml).Get<ClientResponse>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationXmlType, response.GetType());
			string xml = response.GetEntity<string>();
			VerifyHSInfoXML(xml, appContext);
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestInfo()
		{
			WebResource r = Resource();
			ClientResponse response = r.Path("ws").Path("v1").Path("history").Path("info").Accept
				(MediaType.ApplicationJson).Get<ClientResponse>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			JSONObject json = response.GetEntity<JSONObject>();
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, json.Length());
			VerifyHSInfo(json.GetJSONObject("historyInfo"), appContext);
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestInfoSlash()
		{
			WebResource r = Resource();
			ClientResponse response = r.Path("ws").Path("v1").Path("history").Path("info/").Accept
				(MediaType.ApplicationJson).Get<ClientResponse>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			JSONObject json = response.GetEntity<JSONObject>();
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, json.Length());
			VerifyHSInfo(json.GetJSONObject("historyInfo"), appContext);
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestInfoDefault()
		{
			WebResource r = Resource();
			ClientResponse response = r.Path("ws").Path("v1").Path("history").Path("info/").Get
				<ClientResponse>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			JSONObject json = response.GetEntity<JSONObject>();
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, json.Length());
			VerifyHSInfo(json.GetJSONObject("historyInfo"), appContext);
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestInfoXML()
		{
			WebResource r = Resource();
			ClientResponse response = r.Path("ws").Path("v1").Path("history").Path("info/").Accept
				(MediaType.ApplicationXml).Get<ClientResponse>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationXmlType, response.GetType());
			string xml = response.GetEntity<string>();
			VerifyHSInfoXML(xml, appContext);
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
				responseStr = r.Path("ws").Path("v1").Path("history").Path("bogus").Accept(MediaType
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
				responseStr = r.Path("ws").Path("v1").Path("invalid").Accept(MediaType.ApplicationJson
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
				responseStr = r.Path("ws").Path("v1").Path("history").Accept(MediaType.TextPlain)
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

		public virtual void VerifyHsInfoGeneric(string hadoopVersionBuiltOn, string hadoopBuildVersion
			, string hadoopVersion, long startedon)
		{
			WebServicesTestUtils.CheckStringMatch("hadoopVersionBuiltOn", VersionInfo.GetDate
				(), hadoopVersionBuiltOn);
			WebServicesTestUtils.CheckStringEqual("hadoopBuildVersion", VersionInfo.GetBuildVersion
				(), hadoopBuildVersion);
			WebServicesTestUtils.CheckStringMatch("hadoopVersion", VersionInfo.GetVersion(), 
				hadoopVersion);
			NUnit.Framework.Assert.AreEqual("startedOn doesn't match: ", JobHistoryServer.historyServerTimeStamp
				, startedon);
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		public virtual void VerifyHSInfo(JSONObject info, AppContext ctx)
		{
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 4, info.Length());
			VerifyHsInfoGeneric(info.GetString("hadoopVersionBuiltOn"), info.GetString("hadoopBuildVersion"
				), info.GetString("hadoopVersion"), info.GetLong("startedOn"));
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		public virtual void VerifyHSInfoXML(string xml, AppContext ctx)
		{
			DocumentBuilderFactory dbf = DocumentBuilderFactory.NewInstance();
			DocumentBuilder db = dbf.NewDocumentBuilder();
			InputSource @is = new InputSource();
			@is.SetCharacterStream(new StringReader(xml));
			Document dom = db.Parse(@is);
			NodeList nodes = dom.GetElementsByTagName("historyInfo");
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, nodes.GetLength
				());
			for (int i = 0; i < nodes.GetLength(); i++)
			{
				Element element = (Element)nodes.Item(i);
				VerifyHsInfoGeneric(WebServicesTestUtils.GetXmlString(element, "hadoopVersionBuiltOn"
					), WebServicesTestUtils.GetXmlString(element, "hadoopBuildVersion"), WebServicesTestUtils
					.GetXmlString(element, "hadoopVersion"), WebServicesTestUtils.GetXmlLong(element
					, "startedOn"));
			}
		}
	}
}
