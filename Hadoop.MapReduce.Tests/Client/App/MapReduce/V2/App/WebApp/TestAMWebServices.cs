using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Collect;
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
using Org.Apache.Hadoop.Yarn.Webapp;
using Org.Codehaus.Jettison.Json;
using Org.W3c.Dom;
using Org.Xml.Sax;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App.Webapp
{
	/// <summary>Test the MapReduce Application master info web services api's.</summary>
	/// <remarks>
	/// Test the MapReduce Application master info web services api's. Also test
	/// non-existent urls.
	/// /ws/v1/mapreduce
	/// /ws/v1/mapreduce/info
	/// </remarks>
	public class TestAMWebServices : JerseyTest
	{
		private static Configuration conf = new Configuration();

		private static MockAppContext appContext;

		private sealed class _ServletModule_72 : ServletModule
		{
			public _ServletModule_72()
			{
			}

			protected override void ConfigureServlets()
			{
				Org.Apache.Hadoop.Mapreduce.V2.App.Webapp.TestAMWebServices.appContext = new MockAppContext
					(0, 1, 1, 1);
				Org.Apache.Hadoop.Mapreduce.V2.App.Webapp.TestAMWebServices.appContext.SetBlacklistedNodes
					(Sets.NewHashSet("badnode1", "badnode2"));
				this.Bind<JAXBContextResolver>();
				this.Bind<AMWebServices>();
				this.Bind<GenericExceptionHandler>();
				this.Bind<AppContext>().ToInstance(Org.Apache.Hadoop.Mapreduce.V2.App.Webapp.TestAMWebServices
					.appContext);
				this.Bind<Configuration>().ToInstance(Org.Apache.Hadoop.Mapreduce.V2.App.Webapp.TestAMWebServices
					.conf);
				this.Serve("/*").With(typeof(GuiceContainer));
			}
		}

		private Injector injector = Guice.CreateInjector(new _ServletModule_72());

		public class GuiceServletConfig : GuiceServletContextListener
		{
			protected override Injector GetInjector()
			{
				return this._enclosing.injector;
			}

			internal GuiceServletConfig(TestAMWebServices _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly TestAMWebServices _enclosing;
		}

		/// <exception cref="System.Exception"/>
		[SetUp]
		public override void SetUp()
		{
			base.SetUp();
		}

		public TestAMWebServices()
			: base(new WebAppDescriptor.Builder("org.apache.hadoop.mapreduce.v2.app.webapp").
				ContextListenerClass(typeof(TestAMWebServices.GuiceServletConfig)).FilterClass(typeof(
				GuiceFilter)).ContextPath("jersey-guice-filter").ServletPath("/").Build())
		{
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAM()
		{
			WebResource r = Resource();
			ClientResponse response = r.Path("ws").Path("v1").Path("mapreduce").Accept(MediaType
				.ApplicationJson).Get<ClientResponse>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			JSONObject json = response.GetEntity<JSONObject>();
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, json.Length());
			VerifyAMInfo(json.GetJSONObject("info"), appContext);
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAMSlash()
		{
			WebResource r = Resource();
			ClientResponse response = r.Path("ws").Path("v1").Path("mapreduce/").Accept(MediaType
				.ApplicationJson).Get<ClientResponse>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			JSONObject json = response.GetEntity<JSONObject>();
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, json.Length());
			VerifyAMInfo(json.GetJSONObject("info"), appContext);
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAMDefault()
		{
			WebResource r = Resource();
			ClientResponse response = r.Path("ws").Path("v1").Path("mapreduce/").Get<ClientResponse
				>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			JSONObject json = response.GetEntity<JSONObject>();
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, json.Length());
			VerifyAMInfo(json.GetJSONObject("info"), appContext);
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAMXML()
		{
			WebResource r = Resource();
			ClientResponse response = r.Path("ws").Path("v1").Path("mapreduce").Accept(MediaType
				.ApplicationXml).Get<ClientResponse>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationXmlType, response.GetType());
			string xml = response.GetEntity<string>();
			VerifyAMInfoXML(xml, appContext);
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestInfo()
		{
			WebResource r = Resource();
			ClientResponse response = r.Path("ws").Path("v1").Path("mapreduce").Path("info").
				Accept(MediaType.ApplicationJson).Get<ClientResponse>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			JSONObject json = response.GetEntity<JSONObject>();
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, json.Length());
			VerifyAMInfo(json.GetJSONObject("info"), appContext);
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestInfoSlash()
		{
			WebResource r = Resource();
			ClientResponse response = r.Path("ws").Path("v1").Path("mapreduce").Path("info/")
				.Accept(MediaType.ApplicationJson).Get<ClientResponse>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			JSONObject json = response.GetEntity<JSONObject>();
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, json.Length());
			VerifyAMInfo(json.GetJSONObject("info"), appContext);
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestInfoDefault()
		{
			WebResource r = Resource();
			ClientResponse response = r.Path("ws").Path("v1").Path("mapreduce").Path("info/")
				.Get<ClientResponse>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			JSONObject json = response.GetEntity<JSONObject>();
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, json.Length());
			VerifyAMInfo(json.GetJSONObject("info"), appContext);
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestInfoXML()
		{
			WebResource r = Resource();
			ClientResponse response = r.Path("ws").Path("v1").Path("mapreduce").Path("info/")
				.Accept(MediaType.ApplicationXml).Get<ClientResponse>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationXmlType, response.GetType());
			string xml = response.GetEntity<string>();
			VerifyAMInfoXML(xml, appContext);
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
				responseStr = r.Path("ws").Path("v1").Path("mapreduce").Path("bogus").Accept(MediaType
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
				responseStr = r.Path("ws").Path("v1").Path("mapreduce").Accept(MediaType.TextPlain
					).Get<string>();
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
		public virtual void TestBlacklistedNodes()
		{
			WebResource r = Resource();
			ClientResponse response = r.Path("ws").Path("v1").Path("mapreduce").Path("blacklistednodes"
				).Accept(MediaType.ApplicationJson).Get<ClientResponse>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			JSONObject json = response.GetEntity<JSONObject>();
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, json.Length());
			VerifyBlacklistedNodesInfo(json, appContext);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestBlacklistedNodesXML()
		{
			WebResource r = Resource();
			ClientResponse response = r.Path("ws").Path("v1").Path("mapreduce").Path("blacklistednodes"
				).Accept(MediaType.ApplicationXml).Get<ClientResponse>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationXmlType, response.GetType());
			string xml = response.GetEntity<string>();
			VerifyBlacklistedNodesInfoXML(xml, appContext);
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		public virtual void VerifyAMInfo(JSONObject info, AppContext ctx)
		{
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 5, info.Length());
			VerifyAMInfoGeneric(ctx, info.GetString("appId"), info.GetString("user"), info.GetString
				("name"), info.GetLong("startedOn"), info.GetLong("elapsedTime"));
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		public virtual void VerifyAMInfoXML(string xml, AppContext ctx)
		{
			DocumentBuilderFactory dbf = DocumentBuilderFactory.NewInstance();
			DocumentBuilder db = dbf.NewDocumentBuilder();
			InputSource @is = new InputSource();
			@is.SetCharacterStream(new StringReader(xml));
			Document dom = db.Parse(@is);
			NodeList nodes = dom.GetElementsByTagName("info");
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, nodes.GetLength
				());
			for (int i = 0; i < nodes.GetLength(); i++)
			{
				Element element = (Element)nodes.Item(i);
				VerifyAMInfoGeneric(ctx, WebServicesTestUtils.GetXmlString(element, "appId"), WebServicesTestUtils
					.GetXmlString(element, "user"), WebServicesTestUtils.GetXmlString(element, "name"
					), WebServicesTestUtils.GetXmlLong(element, "startedOn"), WebServicesTestUtils.GetXmlLong
					(element, "elapsedTime"));
			}
		}

		public virtual void VerifyAMInfoGeneric(AppContext ctx, string id, string user, string
			 name, long startedOn, long elapsedTime)
		{
			WebServicesTestUtils.CheckStringMatch("id", ctx.GetApplicationID().ToString(), id
				);
			WebServicesTestUtils.CheckStringMatch("user", ctx.GetUser().ToString(), user);
			WebServicesTestUtils.CheckStringMatch("name", ctx.GetApplicationName(), name);
			NUnit.Framework.Assert.AreEqual("startedOn incorrect", ctx.GetStartTime(), startedOn
				);
			NUnit.Framework.Assert.IsTrue("elapsedTime not greater then 0", (elapsedTime > 0)
				);
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		public virtual void VerifyBlacklistedNodesInfo(JSONObject blacklist, AppContext ctx
			)
		{
			JSONArray array = blacklist.GetJSONArray("blacklistedNodes");
			NUnit.Framework.Assert.AreEqual(array.Length(), ctx.GetBlacklistedNodes().Count);
			for (int i = 0; i < array.Length(); i++)
			{
				NUnit.Framework.Assert.IsTrue(ctx.GetBlacklistedNodes().Contains(array.GetString(
					i)));
			}
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		public virtual void VerifyBlacklistedNodesInfoXML(string xml, AppContext ctx)
		{
			DocumentBuilderFactory dbf = DocumentBuilderFactory.NewInstance();
			DocumentBuilder db = dbf.NewDocumentBuilder();
			InputSource @is = new InputSource();
			@is.SetCharacterStream(new StringReader(xml));
			Document dom = db.Parse(@is);
			NodeList infonodes = dom.GetElementsByTagName("blacklistednodesinfo");
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, infonodes.GetLength
				());
			NodeList nodes = dom.GetElementsByTagName("blacklistedNodes");
			ICollection<string> blacklistedNodes = ctx.GetBlacklistedNodes();
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", blacklistedNodes.
				Count, nodes.GetLength());
			for (int i = 0; i < nodes.GetLength(); i++)
			{
				Element element = (Element)nodes.Item(i);
				NUnit.Framework.Assert.IsTrue(blacklistedNodes.Contains(element.GetFirstChild().GetNodeValue
					()));
			}
		}
	}
}
