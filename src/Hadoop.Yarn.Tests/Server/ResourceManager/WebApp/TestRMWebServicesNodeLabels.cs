using System;
using System.IO;
using Com.Google.Inject;
using Com.Google.Inject.Servlet;
using Com.Sun.Jersey.Api.Client;
using Com.Sun.Jersey.Api.Json;
using Com.Sun.Jersey.Guice.Spi.Container.Servlet;
using Com.Sun.Jersey.Test.Framework;
using Javax.WS.RS.Core;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Webapp.Dao;
using Org.Apache.Hadoop.Yarn.Webapp;
using Org.Codehaus.Jettison.Json;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Webapp
{
	public class TestRMWebServicesNodeLabels : JerseyTestBase
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Webapp.TestRMWebServicesNodeLabels
			));

		private static MockRM rm;

		private YarnConfiguration conf;

		private string userName;

		private string notUserName;

		private sealed class _ServletModule_68 : ServletModule
		{
			public _ServletModule_68(TestRMWebServicesNodeLabels _enclosing)
			{
				this._enclosing = _enclosing;
			}

			protected override void ConfigureServlets()
			{
				this.Bind<JAXBContextResolver>();
				this.Bind<RMWebServices>();
				this.Bind<GenericExceptionHandler>();
				try
				{
					this._enclosing.userName = UserGroupInformation.GetCurrentUser().GetShortUserName
						();
				}
				catch (IOException ioe)
				{
					throw new RuntimeException("Unable to get current user name " + ioe.Message, ioe);
				}
				this._enclosing.notUserName = this._enclosing.userName + "abc123";
				this._enclosing.conf = new YarnConfiguration();
				this._enclosing.conf.Set(YarnConfiguration.YarnAdminAcl, this._enclosing.userName
					);
				Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Webapp.TestRMWebServicesNodeLabels.
					rm = new MockRM(this._enclosing.conf);
				this.Bind<ResourceManager>().ToInstance(Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Webapp.TestRMWebServicesNodeLabels
					.rm);
				this.Filter("/*").Through(typeof(TestRMWebServicesAppsModification.TestRMCustomAuthFilter
					));
				this.Serve("/*").With(typeof(GuiceContainer));
			}

			private readonly TestRMWebServicesNodeLabels _enclosing;
		}

		private Injector injector;

		public class GuiceServletConfig : GuiceServletContextListener
		{
			protected override Injector GetInjector()
			{
				return this._enclosing.injector;
			}

			internal GuiceServletConfig(TestRMWebServicesNodeLabels _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly TestRMWebServicesNodeLabels _enclosing;
		}

		public TestRMWebServicesNodeLabels()
			: base(new WebAppDescriptor.Builder("org.apache.hadoop.yarn.server.resourcemanager.webapp"
				).ContextListenerClass(typeof(TestRMWebServicesNodeLabels.GuiceServletConfig)).FilterClass
				(typeof(GuiceFilter)).ContextPath("jersey-guice-filter").ServletPath("/").Build(
				))
		{
			injector = Guice.CreateInjector(new _ServletModule_68(this));
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNodeLabels()
		{
			WebResource r = Resource();
			ClientResponse response;
			JSONObject json;
			JSONArray jarr;
			string responseString;
			// Add a label
			response = r.Path("ws").Path("v1").Path("cluster").Path("add-node-labels").QueryParam
				("user.name", userName).Accept(MediaType.ApplicationJson).Entity("{\"nodeLabels\":\"a\"}"
				, MediaType.ApplicationJson).Post<ClientResponse>();
			// Verify
			response = r.Path("ws").Path("v1").Path("cluster").Path("get-node-labels").QueryParam
				("user.name", userName).Accept(MediaType.ApplicationJson).Get<ClientResponse>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			json = response.GetEntity<JSONObject>();
			NUnit.Framework.Assert.AreEqual("a", json.GetString("nodeLabels"));
			// Add another
			response = r.Path("ws").Path("v1").Path("cluster").Path("add-node-labels").QueryParam
				("user.name", userName).Accept(MediaType.ApplicationJson).Entity("{\"nodeLabels\":\"b\"}"
				, MediaType.ApplicationJson).Post<ClientResponse>();
			// Verify
			response = r.Path("ws").Path("v1").Path("cluster").Path("get-node-labels").QueryParam
				("user.name", userName).Accept(MediaType.ApplicationJson).Get<ClientResponse>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			json = response.GetEntity<JSONObject>();
			jarr = json.GetJSONArray("nodeLabels");
			NUnit.Framework.Assert.AreEqual(2, jarr.Length());
			// Add labels to a node
			response = r.Path("ws").Path("v1").Path("cluster").Path("nodes").Path("nid:0").Path
				("replace-labels").QueryParam("user.name", userName).Accept(MediaType.ApplicationJson
				).Entity("{\"nodeLabels\": [\"a\"]}", MediaType.ApplicationJson).Post<ClientResponse
				>();
			Log.Info("posted node nodelabel");
			// Verify
			response = r.Path("ws").Path("v1").Path("cluster").Path("nodes").Path("nid:0").Path
				("get-labels").QueryParam("user.name", userName).Accept(MediaType.ApplicationJson
				).Get<ClientResponse>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			json = response.GetEntity<JSONObject>();
			NUnit.Framework.Assert.AreEqual("a", json.GetString("nodeLabels"));
			// Replace
			response = r.Path("ws").Path("v1").Path("cluster").Path("nodes").Path("nid:0").Path
				("replace-labels").QueryParam("user.name", userName).Accept(MediaType.ApplicationJson
				).Entity("{\"nodeLabels\":\"b\"}", MediaType.ApplicationJson).Post<ClientResponse
				>();
			Log.Info("posted node nodelabel");
			// Verify
			response = r.Path("ws").Path("v1").Path("cluster").Path("nodes").Path("nid:0").Path
				("get-labels").QueryParam("user.name", userName).Accept(MediaType.ApplicationJson
				).Get<ClientResponse>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			json = response.GetEntity<JSONObject>();
			NUnit.Framework.Assert.AreEqual("b", json.GetString("nodeLabels"));
			// Replace labels using node-to-labels
			NodeToLabelsInfo ntli = new NodeToLabelsInfo();
			NodeLabelsInfo nli = new NodeLabelsInfo();
			nli.GetNodeLabels().AddItem("a");
			ntli.GetNodeToLabels()["nid:0"] = nli;
			response = r.Path("ws").Path("v1").Path("cluster").Path("replace-node-to-labels")
				.QueryParam("user.name", userName).Accept(MediaType.ApplicationJson).Entity(ToJson
				(ntli, typeof(NodeToLabelsInfo)), MediaType.ApplicationJson).Post<ClientResponse
				>();
			// Verify, using node-to-labels
			response = r.Path("ws").Path("v1").Path("cluster").Path("get-node-to-labels").QueryParam
				("user.name", userName).Accept(MediaType.ApplicationJson).Get<ClientResponse>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			ntli = response.GetEntity<NodeToLabelsInfo>();
			nli = ntli.GetNodeToLabels()["nid:0"];
			NUnit.Framework.Assert.AreEqual(1, nli.GetNodeLabels().Count);
			NUnit.Framework.Assert.IsTrue(nli.GetNodeLabels().Contains("a"));
			// Remove all
			response = r.Path("ws").Path("v1").Path("cluster").Path("nodes").Path("nid:0").Path
				("replace-labels").QueryParam("user.name", userName).Accept(MediaType.ApplicationJson
				).Entity("{\"nodeLabels\"}", MediaType.ApplicationJson).Post<ClientResponse>();
			Log.Info("posted node nodelabel");
			// Verify
			response = r.Path("ws").Path("v1").Path("cluster").Path("nodes").Path("nid:0").Path
				("get-labels").QueryParam("user.name", userName).Accept(MediaType.ApplicationJson
				).Get<ClientResponse>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			json = response.GetEntity<JSONObject>();
			NUnit.Framework.Assert.AreEqual(string.Empty, json.GetString("nodeLabels"));
			// Add a label back for auth tests
			response = r.Path("ws").Path("v1").Path("cluster").Path("nodes").Path("nid:0").Path
				("replace-labels").QueryParam("user.name", userName).Accept(MediaType.ApplicationJson
				).Entity("{\"nodeLabels\": \"a\"}", MediaType.ApplicationJson).Post<ClientResponse
				>();
			Log.Info("posted node nodelabel");
			// Verify
			response = r.Path("ws").Path("v1").Path("cluster").Path("nodes").Path("nid:0").Path
				("get-labels").QueryParam("user.name", userName).Accept(MediaType.ApplicationJson
				).Get<ClientResponse>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			json = response.GetEntity<JSONObject>();
			NUnit.Framework.Assert.AreEqual("a", json.GetString("nodeLabels"));
			// Auth fail replace labels on node
			response = r.Path("ws").Path("v1").Path("cluster").Path("nodes").Path("nid:0").Path
				("replace-labels").QueryParam("user.name", notUserName).Accept(MediaType.ApplicationJson
				).Entity("{\"nodeLabels\": [\"b\"]}", MediaType.ApplicationJson).Post<ClientResponse
				>();
			// Verify
			response = r.Path("ws").Path("v1").Path("cluster").Path("nodes").Path("nid:0").Path
				("get-labels").QueryParam("user.name", userName).Accept(MediaType.ApplicationJson
				).Get<ClientResponse>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			json = response.GetEntity<JSONObject>();
			NUnit.Framework.Assert.AreEqual("a", json.GetString("nodeLabels"));
			// Fail to add a label with post
			response = r.Path("ws").Path("v1").Path("cluster").Path("add-node-labels").QueryParam
				("user.name", notUserName).Accept(MediaType.ApplicationJson).Entity("{\"nodeLabels\":\"c\"}"
				, MediaType.ApplicationJson).Post<ClientResponse>();
			// Verify
			response = r.Path("ws").Path("v1").Path("cluster").Path("get-node-labels").QueryParam
				("user.name", userName).Accept(MediaType.ApplicationJson).Get<ClientResponse>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			json = response.GetEntity<JSONObject>();
			jarr = json.GetJSONArray("nodeLabels");
			NUnit.Framework.Assert.AreEqual(2, jarr.Length());
			// Remove cluster label (succeed, we no longer need it)
			response = r.Path("ws").Path("v1").Path("cluster").Path("remove-node-labels").QueryParam
				("user.name", userName).Accept(MediaType.ApplicationJson).Entity("{\"nodeLabels\":\"b\"}"
				, MediaType.ApplicationJson).Post<ClientResponse>();
			// Verify
			response = r.Path("ws").Path("v1").Path("cluster").Path("get-node-labels").QueryParam
				("user.name", userName).Accept(MediaType.ApplicationJson).Get<ClientResponse>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			json = response.GetEntity<JSONObject>();
			NUnit.Framework.Assert.AreEqual("a", json.GetString("nodeLabels"));
			// Remove cluster label with post
			response = r.Path("ws").Path("v1").Path("cluster").Path("remove-node-labels").QueryParam
				("user.name", userName).Accept(MediaType.ApplicationJson).Entity("{\"nodeLabels\":\"a\"}"
				, MediaType.ApplicationJson).Post<ClientResponse>();
			// Verify
			response = r.Path("ws").Path("v1").Path("cluster").Path("get-node-labels").QueryParam
				("user.name", userName).Accept(MediaType.ApplicationJson).Get<ClientResponse>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			string res = response.GetEntity<string>();
			NUnit.Framework.Assert.IsTrue(res.Equals("null"));
		}

		/// <exception cref="System.Exception"/>
		private string ToJson(object nsli, Type klass)
		{
			StringWriter sw = new StringWriter();
			JSONJAXBContext ctx = new JSONJAXBContext(klass);
			JSONMarshaller jm = ctx.CreateJSONMarshaller();
			jm.MarshallToJSON(nsli, sw);
			return sw.ToString();
		}

		/// <exception cref="System.Exception"/>
		private object FromJson(string json, Type klass)
		{
			StringReader sr = new StringReader(json);
			JSONJAXBContext ctx = new JSONJAXBContext(klass);
			JSONUnmarshaller jm = ctx.CreateJSONUnmarshaller();
			return jm.UnmarshalFromJSON(sr, klass);
		}
	}
}
