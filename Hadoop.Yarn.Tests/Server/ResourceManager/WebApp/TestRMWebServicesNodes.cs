using System.IO;
using Com.Google.Common.Base;
using Com.Google.Inject;
using Com.Google.Inject.Servlet;
using Com.Sun.Jersey.Api.Client;
using Com.Sun.Jersey.Guice.Spi.Container.Servlet;
using Com.Sun.Jersey.Test.Framework;
using Javax.WS.RS.Core;
using Javax.Xml.Parsers;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Server.Api.Records;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Rmnode;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler;
using Org.Apache.Hadoop.Yarn.Webapp;
using Org.Codehaus.Jettison.Json;
using Org.W3c.Dom;
using Org.Xml.Sax;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Webapp
{
	public class TestRMWebServicesNodes : JerseyTestBase
	{
		private static MockRM rm;

		private sealed class _ServletModule_74 : ServletModule
		{
			public _ServletModule_74()
			{
			}

			protected override void ConfigureServlets()
			{
				this.Bind<JAXBContextResolver>();
				this.Bind<RMWebServices>();
				this.Bind<GenericExceptionHandler>();
				Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Webapp.TestRMWebServicesNodes.rm = 
					new MockRM(new Configuration());
				Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Webapp.TestRMWebServicesNodes.rm.GetRMContext
					().GetContainerTokenSecretManager().RollMasterKey();
				Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Webapp.TestRMWebServicesNodes.rm.GetRMContext
					().GetNMTokenSecretManager().RollMasterKey();
				this.Bind<ResourceManager>().ToInstance(Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Webapp.TestRMWebServicesNodes
					.rm);
				this.Serve("/*").With(typeof(GuiceContainer));
			}
		}

		private Injector injector = Guice.CreateInjector(new _ServletModule_74());

		public class GuiceServletConfig : GuiceServletContextListener
		{
			protected override Injector GetInjector()
			{
				return this._enclosing.injector;
			}

			internal GuiceServletConfig(TestRMWebServicesNodes _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly TestRMWebServicesNodes _enclosing;
		}

		/// <exception cref="System.Exception"/>
		[SetUp]
		public override void SetUp()
		{
			base.SetUp();
		}

		public TestRMWebServicesNodes()
			: base(new WebAppDescriptor.Builder("org.apache.hadoop.yarn.server.resourcemanager.webapp"
				).ContextListenerClass(typeof(TestRMWebServicesNodes.GuiceServletConfig)).FilterClass
				(typeof(GuiceFilter)).ContextPath("jersey-guice-filter").ServletPath("/").Build(
				))
		{
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNodes()
		{
			TestNodesHelper("nodes", MediaType.ApplicationJson);
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNodesSlash()
		{
			TestNodesHelper("nodes/", MediaType.ApplicationJson);
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNodesDefault()
		{
			TestNodesHelper("nodes/", string.Empty);
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNodesDefaultWithUnHealthyNode()
		{
			WebResource r = Resource();
			MockNM nm1 = rm.RegisterNode("h1:1234", 5120);
			MockNM nm2 = rm.RegisterNode("h2:1235", 5121);
			rm.SendNodeStarted(nm1);
			rm.NMwaitForState(nm1.GetNodeId(), NodeState.Running);
			rm.NMwaitForState(nm2.GetNodeId(), NodeState.New);
			MockNM nm3 = rm.RegisterNode("h3:1236", 5122);
			rm.NMwaitForState(nm3.GetNodeId(), NodeState.New);
			rm.SendNodeStarted(nm3);
			rm.NMwaitForState(nm3.GetNodeId(), NodeState.Running);
			RMNodeImpl node = (RMNodeImpl)rm.GetRMContext().GetRMNodes()[nm3.GetNodeId()];
			NodeHealthStatus nodeHealth = NodeHealthStatus.NewInstance(false, "test health report"
				, Runtime.CurrentTimeMillis());
			node.Handle(new RMNodeStatusEvent(nm3.GetNodeId(), nodeHealth, new AList<ContainerStatus
				>(), null, null));
			rm.NMwaitForState(nm3.GetNodeId(), NodeState.Unhealthy);
			ClientResponse response = r.Path("ws").Path("v1").Path("cluster").Path("nodes").Accept
				(MediaType.ApplicationJson).Get<ClientResponse>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			JSONObject json = response.GetEntity<JSONObject>();
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, json.Length());
			JSONObject nodes = json.GetJSONObject("nodes");
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, nodes.Length()
				);
			JSONArray nodeArray = nodes.GetJSONArray("node");
			// 3 nodes, including the unhealthy node and the new node.
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 3, nodeArray.Length
				());
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNodesQueryNew()
		{
			WebResource r = Resource();
			MockNM nm1 = rm.RegisterNode("h1:1234", 5120);
			MockNM nm2 = rm.RegisterNode("h2:1235", 5121);
			rm.SendNodeStarted(nm1);
			rm.NMwaitForState(nm1.GetNodeId(), NodeState.Running);
			rm.NMwaitForState(nm2.GetNodeId(), NodeState.New);
			ClientResponse response = r.Path("ws").Path("v1").Path("cluster").Path("nodes").QueryParam
				("states", NodeState.New.ToString()).Accept(MediaType.ApplicationJson).Get<ClientResponse
				>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			JSONObject json = response.GetEntity<JSONObject>();
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, json.Length());
			JSONObject nodes = json.GetJSONObject("nodes");
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, nodes.Length()
				);
			JSONArray nodeArray = nodes.GetJSONArray("node");
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, nodeArray.Length
				());
			JSONObject info = nodeArray.GetJSONObject(0);
			VerifyNodeInfo(info, nm2);
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNodesQueryStateNone()
		{
			WebResource r = Resource();
			rm.RegisterNode("h1:1234", 5120);
			rm.RegisterNode("h2:1235", 5121);
			ClientResponse response = r.Path("ws").Path("v1").Path("cluster").Path("nodes").QueryParam
				("states", NodeState.Decommissioned.ToString()).Accept(MediaType.ApplicationJson
				).Get<ClientResponse>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			JSONObject json = response.GetEntity<JSONObject>();
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, json.Length());
			NUnit.Framework.Assert.AreEqual("nodes is not null", JSONObject.Null, json.Get("nodes"
				));
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNodesQueryStateInvalid()
		{
			WebResource r = Resource();
			rm.RegisterNode("h1:1234", 5120);
			rm.RegisterNode("h2:1235", 5121);
			try
			{
				r.Path("ws").Path("v1").Path("cluster").Path("nodes").QueryParam("states", "BOGUSSTATE"
					).Accept(MediaType.ApplicationJson).Get<JSONObject>();
				NUnit.Framework.Assert.Fail("should have thrown exception querying invalid state"
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
				WebServicesTestUtils.CheckStringContains("exception message", "org.apache.hadoop.yarn.api.records.NodeState.BOGUSSTATE"
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
		public virtual void TestNodesQueryStateLost()
		{
			WebResource r = Resource();
			MockNM nm1 = rm.RegisterNode("h1:1234", 5120);
			MockNM nm2 = rm.RegisterNode("h2:1234", 5120);
			rm.SendNodeStarted(nm1);
			rm.SendNodeStarted(nm2);
			rm.NMwaitForState(nm1.GetNodeId(), NodeState.Running);
			rm.NMwaitForState(nm2.GetNodeId(), NodeState.Running);
			rm.SendNodeLost(nm1);
			rm.SendNodeLost(nm2);
			ClientResponse response = r.Path("ws").Path("v1").Path("cluster").Path("nodes").QueryParam
				("states", NodeState.Lost.ToString()).Accept(MediaType.ApplicationJson).Get<ClientResponse
				>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			JSONObject json = response.GetEntity<JSONObject>();
			JSONObject nodes = json.GetJSONObject("nodes");
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, nodes.Length()
				);
			JSONArray nodeArray = nodes.GetJSONArray("node");
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 2, nodeArray.Length
				());
			for (int i = 0; i < nodeArray.Length(); ++i)
			{
				JSONObject info = nodeArray.GetJSONObject(i);
				string host = info.Get("id").ToString().Split(":")[0];
				RMNode rmNode = rm.GetRMContext().GetInactiveRMNodes()[host];
				WebServicesTestUtils.CheckStringMatch("nodeHTTPAddress", string.Empty, info.GetString
					("nodeHTTPAddress"));
				WebServicesTestUtils.CheckStringMatch("state", rmNode.GetState().ToString(), info
					.GetString("state"));
			}
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestSingleNodeQueryStateLost()
		{
			WebResource r = Resource();
			MockNM nm1 = rm.RegisterNode("h1:1234", 5120);
			MockNM nm2 = rm.RegisterNode("h2:1234", 5120);
			rm.SendNodeStarted(nm1);
			rm.SendNodeStarted(nm2);
			rm.NMwaitForState(nm1.GetNodeId(), NodeState.Running);
			rm.NMwaitForState(nm2.GetNodeId(), NodeState.Running);
			rm.SendNodeLost(nm1);
			rm.SendNodeLost(nm2);
			ClientResponse response = r.Path("ws").Path("v1").Path("cluster").Path("nodes").Path
				("h2:1234").Accept(MediaType.ApplicationJson).Get<ClientResponse>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			JSONObject json = response.GetEntity<JSONObject>();
			JSONObject info = json.GetJSONObject("node");
			string id = info.Get("id").ToString();
			NUnit.Framework.Assert.AreEqual("Incorrect Node Information.", "h2:1234", id);
			RMNode rmNode = rm.GetRMContext().GetInactiveRMNodes()["h2"];
			WebServicesTestUtils.CheckStringMatch("nodeHTTPAddress", string.Empty, info.GetString
				("nodeHTTPAddress"));
			WebServicesTestUtils.CheckStringMatch("state", rmNode.GetState().ToString(), info
				.GetString("state"));
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNodesQueryRunning()
		{
			WebResource r = Resource();
			MockNM nm1 = rm.RegisterNode("h1:1234", 5120);
			MockNM nm2 = rm.RegisterNode("h2:1235", 5121);
			rm.SendNodeStarted(nm1);
			rm.NMwaitForState(nm1.GetNodeId(), NodeState.Running);
			rm.NMwaitForState(nm2.GetNodeId(), NodeState.New);
			ClientResponse response = r.Path("ws").Path("v1").Path("cluster").Path("nodes").QueryParam
				("states", "running").Accept(MediaType.ApplicationJson).Get<ClientResponse>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			JSONObject json = response.GetEntity<JSONObject>();
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, json.Length());
			JSONObject nodes = json.GetJSONObject("nodes");
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, nodes.Length()
				);
			JSONArray nodeArray = nodes.GetJSONArray("node");
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, nodeArray.Length
				());
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNodesQueryHealthyFalse()
		{
			WebResource r = Resource();
			MockNM nm1 = rm.RegisterNode("h1:1234", 5120);
			MockNM nm2 = rm.RegisterNode("h2:1235", 5121);
			rm.SendNodeStarted(nm1);
			rm.NMwaitForState(nm1.GetNodeId(), NodeState.Running);
			rm.NMwaitForState(nm2.GetNodeId(), NodeState.New);
			ClientResponse response = r.Path("ws").Path("v1").Path("cluster").Path("nodes").QueryParam
				("states", "UNHEALTHY").Accept(MediaType.ApplicationJson).Get<ClientResponse>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			JSONObject json = response.GetEntity<JSONObject>();
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, json.Length());
			NUnit.Framework.Assert.AreEqual("nodes is not null", JSONObject.Null, json.Get("nodes"
				));
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		public virtual void TestNodesHelper(string path, string media)
		{
			WebResource r = Resource();
			MockNM nm1 = rm.RegisterNode("h1:1234", 5120);
			MockNM nm2 = rm.RegisterNode("h2:1235", 5121);
			rm.SendNodeStarted(nm1);
			rm.SendNodeStarted(nm2);
			rm.NMwaitForState(nm1.GetNodeId(), NodeState.Running);
			rm.NMwaitForState(nm2.GetNodeId(), NodeState.Running);
			ClientResponse response = r.Path("ws").Path("v1").Path("cluster").Path(path).Accept
				(media).Get<ClientResponse>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			JSONObject json = response.GetEntity<JSONObject>();
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, json.Length());
			JSONObject nodes = json.GetJSONObject("nodes");
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, nodes.Length()
				);
			JSONArray nodeArray = nodes.GetJSONArray("node");
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 2, nodeArray.Length
				());
			JSONObject info = nodeArray.GetJSONObject(0);
			string id = info.Get("id").ToString();
			if (id.Matches("h1:1234"))
			{
				VerifyNodeInfo(info, nm1);
				VerifyNodeInfo(nodeArray.GetJSONObject(1), nm2);
			}
			else
			{
				VerifyNodeInfo(info, nm2);
				VerifyNodeInfo(nodeArray.GetJSONObject(1), nm1);
			}
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestSingleNode()
		{
			rm.RegisterNode("h1:1234", 5120);
			MockNM nm2 = rm.RegisterNode("h2:1235", 5121);
			TestSingleNodeHelper("h2:1235", nm2, MediaType.ApplicationJson);
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestSingleNodeSlash()
		{
			MockNM nm1 = rm.RegisterNode("h1:1234", 5120);
			rm.RegisterNode("h2:1235", 5121);
			TestSingleNodeHelper("h1:1234/", nm1, MediaType.ApplicationJson);
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestSingleNodeDefault()
		{
			MockNM nm1 = rm.RegisterNode("h1:1234", 5120);
			rm.RegisterNode("h2:1235", 5121);
			TestSingleNodeHelper("h1:1234/", nm1, string.Empty);
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		public virtual void TestSingleNodeHelper(string nodeid, MockNM nm, string media)
		{
			WebResource r = Resource();
			ClientResponse response = r.Path("ws").Path("v1").Path("cluster").Path("nodes").Path
				(nodeid).Accept(media).Get<ClientResponse>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			JSONObject json = response.GetEntity<JSONObject>();
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, json.Length());
			JSONObject info = json.GetJSONObject("node");
			VerifyNodeInfo(info, nm);
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNonexistNode()
		{
			rm.RegisterNode("h1:1234", 5120);
			rm.RegisterNode("h2:1235", 5121);
			WebResource r = Resource();
			try
			{
				r.Path("ws").Path("v1").Path("cluster").Path("nodes").Path("node_invalid:99").Accept
					(MediaType.ApplicationJson).Get<JSONObject>();
				NUnit.Framework.Assert.Fail("should have thrown exception on non-existent nodeid"
					);
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
				VerifyNonexistNodeException(message, type, classname);
			}
			finally
			{
				rm.Stop();
			}
		}

		// test that the exception output defaults to JSON
		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNonexistNodeDefault()
		{
			rm.RegisterNode("h1:1234", 5120);
			rm.RegisterNode("h2:1235", 5121);
			WebResource r = Resource();
			try
			{
				r.Path("ws").Path("v1").Path("cluster").Path("nodes").Path("node_invalid:99").Get
					<JSONObject>();
				NUnit.Framework.Assert.Fail("should have thrown exception on non-existent nodeid"
					);
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
				VerifyNonexistNodeException(message, type, classname);
			}
			finally
			{
				rm.Stop();
			}
		}

		// test that the exception output works in XML
		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNonexistNodeXML()
		{
			rm.RegisterNode("h1:1234", 5120);
			rm.RegisterNode("h2:1235", 5121);
			WebResource r = Resource();
			try
			{
				r.Path("ws").Path("v1").Path("cluster").Path("nodes").Path("node_invalid:99").Accept
					(MediaType.ApplicationXml).Get<JSONObject>();
				NUnit.Framework.Assert.Fail("should have thrown exception on non-existent nodeid"
					);
			}
			catch (UniformInterfaceException ue)
			{
				ClientResponse response = ue.GetResponse();
				NUnit.Framework.Assert.AreEqual(ClientResponse.Status.NotFound, response.GetClientResponseStatus
					());
				NUnit.Framework.Assert.AreEqual(MediaType.ApplicationXmlType, response.GetType());
				string msg = response.GetEntity<string>();
				System.Console.Out.WriteLine(msg);
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
				VerifyNonexistNodeException(message, type, classname);
			}
			finally
			{
				rm.Stop();
			}
		}

		private void VerifyNonexistNodeException(string message, string type, string classname
			)
		{
			NUnit.Framework.Assert.IsTrue("exception message incorrect", "java.lang.Exception: nodeId, node_invalid:99, is not found"
				.Matches(message));
			NUnit.Framework.Assert.IsTrue("exception type incorrect", "NotFoundException".Matches
				(type));
			NUnit.Framework.Assert.IsTrue("exception className incorrect", "org.apache.hadoop.yarn.webapp.NotFoundException"
				.Matches(classname));
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestInvalidNode()
		{
			rm.RegisterNode("h1:1234", 5120);
			rm.RegisterNode("h2:1235", 5121);
			WebResource r = Resource();
			try
			{
				r.Path("ws").Path("v1").Path("cluster").Path("nodes").Path("node_invalid_foo").Accept
					(MediaType.ApplicationJson).Get<JSONObject>();
				NUnit.Framework.Assert.Fail("should have thrown exception on non-existent nodeid"
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
				WebServicesTestUtils.CheckStringMatch("exception message", "Invalid NodeId \\[node_invalid_foo\\]. Expected host:port"
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
		public virtual void TestNodesXML()
		{
			rm.Start();
			WebResource r = Resource();
			MockNM nm1 = rm.RegisterNode("h1:1234", 5120);
			// MockNM nm2 = rm.registerNode("h2:1235", 5121);
			ClientResponse response = r.Path("ws").Path("v1").Path("cluster").Path("nodes").Accept
				(MediaType.ApplicationXml).Get<ClientResponse>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationXmlType, response.GetType());
			string xml = response.GetEntity<string>();
			DocumentBuilderFactory dbf = DocumentBuilderFactory.NewInstance();
			DocumentBuilder db = dbf.NewDocumentBuilder();
			InputSource @is = new InputSource();
			@is.SetCharacterStream(new StringReader(xml));
			Document dom = db.Parse(@is);
			NodeList nodesApps = dom.GetElementsByTagName("nodes");
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, nodesApps.GetLength
				());
			NodeList nodes = dom.GetElementsByTagName("node");
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, nodes.GetLength
				());
			VerifyNodesXML(nodes, nm1);
			rm.Stop();
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestSingleNodesXML()
		{
			rm.Start();
			WebResource r = Resource();
			MockNM nm1 = rm.RegisterNode("h1:1234", 5120);
			// MockNM nm2 = rm.registerNode("h2:1235", 5121);
			ClientResponse response = r.Path("ws").Path("v1").Path("cluster").Path("nodes").Path
				("h1:1234").Accept(MediaType.ApplicationXml).Get<ClientResponse>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationXmlType, response.GetType());
			string xml = response.GetEntity<string>();
			DocumentBuilderFactory dbf = DocumentBuilderFactory.NewInstance();
			DocumentBuilder db = dbf.NewDocumentBuilder();
			InputSource @is = new InputSource();
			@is.SetCharacterStream(new StringReader(xml));
			Document dom = db.Parse(@is);
			NodeList nodes = dom.GetElementsByTagName("node");
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, nodes.GetLength
				());
			VerifyNodesXML(nodes, nm1);
			rm.Stop();
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNodes2XML()
		{
			rm.Start();
			WebResource r = Resource();
			rm.RegisterNode("h1:1234", 5120);
			rm.RegisterNode("h2:1235", 5121);
			ClientResponse response = r.Path("ws").Path("v1").Path("cluster").Path("nodes").Accept
				(MediaType.ApplicationXml).Get<ClientResponse>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationXmlType, response.GetType());
			string xml = response.GetEntity<string>();
			DocumentBuilderFactory dbf = DocumentBuilderFactory.NewInstance();
			DocumentBuilder db = dbf.NewDocumentBuilder();
			InputSource @is = new InputSource();
			@is.SetCharacterStream(new StringReader(xml));
			Document dom = db.Parse(@is);
			NodeList nodesApps = dom.GetElementsByTagName("nodes");
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, nodesApps.GetLength
				());
			NodeList nodes = dom.GetElementsByTagName("node");
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 2, nodes.GetLength
				());
			rm.Stop();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestQueryAll()
		{
			WebResource r = Resource();
			MockNM nm1 = rm.RegisterNode("h1:1234", 5120);
			MockNM nm2 = rm.RegisterNode("h2:1235", 5121);
			MockNM nm3 = rm.RegisterNode("h3:1236", 5122);
			rm.SendNodeStarted(nm1);
			rm.SendNodeStarted(nm3);
			rm.NMwaitForState(nm1.GetNodeId(), NodeState.Running);
			rm.NMwaitForState(nm2.GetNodeId(), NodeState.New);
			rm.SendNodeLost(nm3);
			ClientResponse response = r.Path("ws").Path("v1").Path("cluster").Path("nodes").QueryParam
				("states", Joiner.On(',').Join(EnumSet.AllOf<NodeState>())).Accept(MediaType.ApplicationJson
				).Get<ClientResponse>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			JSONObject json = response.GetEntity<JSONObject>();
			JSONObject nodes = json.GetJSONObject("nodes");
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, nodes.Length()
				);
			JSONArray nodeArray = nodes.GetJSONArray("node");
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 3, nodeArray.Length
				());
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		public virtual void VerifyNodesXML(NodeList nodes, MockNM nm)
		{
			for (int i = 0; i < nodes.GetLength(); i++)
			{
				Element element = (Element)nodes.Item(i);
				VerifyNodeInfoGeneric(nm, WebServicesTestUtils.GetXmlString(element, "state"), WebServicesTestUtils
					.GetXmlString(element, "rack"), WebServicesTestUtils.GetXmlString(element, "id")
					, WebServicesTestUtils.GetXmlString(element, "nodeHostName"), WebServicesTestUtils
					.GetXmlString(element, "nodeHTTPAddress"), WebServicesTestUtils.GetXmlLong(element
					, "lastHealthUpdate"), WebServicesTestUtils.GetXmlString(element, "healthReport"
					), WebServicesTestUtils.GetXmlInt(element, "numContainers"), WebServicesTestUtils
					.GetXmlLong(element, "usedMemoryMB"), WebServicesTestUtils.GetXmlLong(element, "availMemoryMB"
					), WebServicesTestUtils.GetXmlLong(element, "usedVirtualCores"), WebServicesTestUtils
					.GetXmlLong(element, "availableVirtualCores"), WebServicesTestUtils.GetXmlString
					(element, "version"));
			}
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		public virtual void VerifyNodeInfo(JSONObject nodeInfo, MockNM nm)
		{
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 13, nodeInfo.Length
				());
			VerifyNodeInfoGeneric(nm, nodeInfo.GetString("state"), nodeInfo.GetString("rack")
				, nodeInfo.GetString("id"), nodeInfo.GetString("nodeHostName"), nodeInfo.GetString
				("nodeHTTPAddress"), nodeInfo.GetLong("lastHealthUpdate"), nodeInfo.GetString("healthReport"
				), nodeInfo.GetInt("numContainers"), nodeInfo.GetLong("usedMemoryMB"), nodeInfo.
				GetLong("availMemoryMB"), nodeInfo.GetLong("usedVirtualCores"), nodeInfo.GetLong
				("availableVirtualCores"), nodeInfo.GetString("version"));
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		public virtual void VerifyNodeInfoGeneric(MockNM nm, string state, string rack, string
			 id, string nodeHostName, string nodeHTTPAddress, long lastHealthUpdate, string 
			healthReport, int numContainers, long usedMemoryMB, long availMemoryMB, long usedVirtualCores
			, long availVirtualCores, string version)
		{
			RMNode node = rm.GetRMContext().GetRMNodes()[nm.GetNodeId()];
			ResourceScheduler sched = rm.GetResourceScheduler();
			SchedulerNodeReport report = sched.GetNodeReport(nm.GetNodeId());
			WebServicesTestUtils.CheckStringMatch("state", node.GetState().ToString(), state);
			WebServicesTestUtils.CheckStringMatch("rack", node.GetRackName(), rack);
			WebServicesTestUtils.CheckStringMatch("id", nm.GetNodeId().ToString(), id);
			WebServicesTestUtils.CheckStringMatch("nodeHostName", nm.GetNodeId().GetHost(), nodeHostName
				);
			WebServicesTestUtils.CheckStringMatch("healthReport", node.GetHealthReport().ToString
				(), healthReport);
			string expectedHttpAddress = nm.GetNodeId().GetHost() + ":" + nm.GetHttpPort();
			WebServicesTestUtils.CheckStringMatch("nodeHTTPAddress", expectedHttpAddress, nodeHTTPAddress
				);
			WebServicesTestUtils.CheckStringMatch("version", node.GetNodeManagerVersion(), version
				);
			long expectedHealthUpdate = node.GetLastHealthReportTime();
			NUnit.Framework.Assert.AreEqual("lastHealthUpdate doesn't match, got: " + lastHealthUpdate
				 + " expected: " + expectedHealthUpdate, expectedHealthUpdate, lastHealthUpdate);
			if (report != null)
			{
				NUnit.Framework.Assert.AreEqual("numContainers doesn't match: " + numContainers, 
					report.GetNumContainers(), numContainers);
				NUnit.Framework.Assert.AreEqual("usedMemoryMB doesn't match: " + usedMemoryMB, report
					.GetUsedResource().GetMemory(), usedMemoryMB);
				NUnit.Framework.Assert.AreEqual("availMemoryMB doesn't match: " + availMemoryMB, 
					report.GetAvailableResource().GetMemory(), availMemoryMB);
				NUnit.Framework.Assert.AreEqual("usedVirtualCores doesn't match: " + usedVirtualCores
					, report.GetUsedResource().GetVirtualCores(), usedVirtualCores);
				NUnit.Framework.Assert.AreEqual("availVirtualCores doesn't match: " + availVirtualCores
					, report.GetAvailableResource().GetVirtualCores(), availVirtualCores);
			}
		}
	}
}
