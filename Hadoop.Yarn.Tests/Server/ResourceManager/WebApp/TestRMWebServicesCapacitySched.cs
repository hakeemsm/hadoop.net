using System.IO;
using Com.Google.Inject;
using Com.Google.Inject.Servlet;
using Com.Sun.Jersey.Api.Client;
using Com.Sun.Jersey.Guice.Spi.Container.Servlet;
using Com.Sun.Jersey.Test.Framework;
using Javax.WS.RS.Core;
using Javax.Xml.Parsers;
using NUnit.Framework;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Capacity;
using Org.Apache.Hadoop.Yarn.Util.Resource;
using Org.Apache.Hadoop.Yarn.Webapp;
using Org.Codehaus.Jettison.Json;
using Org.W3c.Dom;
using Org.Xml.Sax;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Webapp
{
	public class TestRMWebServicesCapacitySched : JerseyTestBase
	{
		private static MockRM rm;

		private CapacitySchedulerConfiguration csConf;

		private YarnConfiguration conf;

		private class QueueInfo
		{
			internal float capacity;

			internal float usedCapacity;

			internal float maxCapacity;

			internal float absoluteCapacity;

			internal float absoluteMaxCapacity;

			internal float absoluteUsedCapacity;

			internal int numApplications;

			internal string queueName;

			internal string state;

			internal QueueInfo(TestRMWebServicesCapacitySched _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly TestRMWebServicesCapacitySched _enclosing;
		}

		private class LeafQueueInfo : TestRMWebServicesCapacitySched.QueueInfo
		{
			internal int numActiveApplications;

			internal int numPendingApplications;

			internal int numContainers;

			internal int maxApplications;

			internal int maxApplicationsPerUser;

			internal int userLimit;

			internal float userLimitFactor;

			internal LeafQueueInfo(TestRMWebServicesCapacitySched _enclosing)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly TestRMWebServicesCapacitySched _enclosing;
		}

		private sealed class _ServletModule_89 : ServletModule
		{
			public _ServletModule_89(TestRMWebServicesCapacitySched _enclosing)
			{
				this._enclosing = _enclosing;
			}

			protected override void ConfigureServlets()
			{
				this.Bind<JAXBContextResolver>();
				this.Bind<RMWebServices>();
				this.Bind<GenericExceptionHandler>();
				this._enclosing.csConf = new CapacitySchedulerConfiguration();
				TestRMWebServicesCapacitySched.SetupQueueConfiguration(this._enclosing.csConf);
				this._enclosing.conf = new YarnConfiguration(this._enclosing.csConf);
				this._enclosing.conf.SetClass(YarnConfiguration.RmScheduler, typeof(CapacityScheduler
					), typeof(ResourceScheduler));
				TestRMWebServicesCapacitySched.rm = new MockRM(this._enclosing.conf);
				this.Bind<ResourceManager>().ToInstance(TestRMWebServicesCapacitySched.rm);
				this.Serve("/*").With(typeof(GuiceContainer));
			}

			private readonly TestRMWebServicesCapacitySched _enclosing;
		}

		private Injector injector;

		public class GuiceServletConfig : GuiceServletContextListener
		{
			protected override Injector GetInjector()
			{
				return this._enclosing.injector;
			}

			internal GuiceServletConfig(TestRMWebServicesCapacitySched _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly TestRMWebServicesCapacitySched _enclosing;
		}

		private static void SetupQueueConfiguration(CapacitySchedulerConfiguration conf)
		{
			// Define top-level queues
			conf.SetQueues(CapacitySchedulerConfiguration.Root, new string[] { "a", "b" });
			string A = CapacitySchedulerConfiguration.Root + ".a";
			conf.SetCapacity(A, 10.5f);
			conf.SetMaximumCapacity(A, 50);
			string B = CapacitySchedulerConfiguration.Root + ".b";
			conf.SetCapacity(B, 89.5f);
			// Define 2nd-level queues
			string A1 = A + ".a1";
			string A2 = A + ".a2";
			conf.SetQueues(A, new string[] { "a1", "a2" });
			conf.SetCapacity(A1, 30);
			conf.SetMaximumCapacity(A1, 50);
			conf.SetUserLimitFactor(A1, 100.0f);
			conf.SetCapacity(A2, 70);
			conf.SetUserLimitFactor(A2, 100.0f);
			string B1 = B + ".b1";
			string B2 = B + ".b2";
			string B3 = B + ".b3";
			conf.SetQueues(B, new string[] { "b1", "b2", "b3" });
			conf.SetCapacity(B1, 60);
			conf.SetUserLimitFactor(B1, 100.0f);
			conf.SetCapacity(B2, 39.5f);
			conf.SetUserLimitFactor(B2, 100.0f);
			conf.SetCapacity(B3, 0.5f);
			conf.SetUserLimitFactor(B3, 100.0f);
			conf.SetQueues(A1, new string[] { "a1a", "a1b" });
			string A1a = A1 + ".a1a";
			conf.SetCapacity(A1a, 85);
			string A1b = A1 + ".a1b";
			conf.SetCapacity(A1b, 15);
		}

		/// <exception cref="System.Exception"/>
		[SetUp]
		public override void SetUp()
		{
			base.SetUp();
		}

		public TestRMWebServicesCapacitySched()
			: base(new WebAppDescriptor.Builder("org.apache.hadoop.yarn.server.resourcemanager.webapp"
				).ContextListenerClass(typeof(TestRMWebServicesCapacitySched.GuiceServletConfig)
				).FilterClass(typeof(GuiceFilter)).ContextPath("jersey-guice-filter").ServletPath
				("/").Build())
		{
			injector = Guice.CreateInjector(new _ServletModule_89(this));
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestClusterScheduler()
		{
			WebResource r = Resource();
			ClientResponse response = r.Path("ws").Path("v1").Path("cluster").Path("scheduler"
				).Accept(MediaType.ApplicationJson).Get<ClientResponse>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			JSONObject json = response.GetEntity<JSONObject>();
			VerifyClusterScheduler(json);
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestClusterSchedulerSlash()
		{
			WebResource r = Resource();
			ClientResponse response = r.Path("ws").Path("v1").Path("cluster").Path("scheduler/"
				).Accept(MediaType.ApplicationJson).Get<ClientResponse>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			JSONObject json = response.GetEntity<JSONObject>();
			VerifyClusterScheduler(json);
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestClusterSchedulerDefault()
		{
			WebResource r = Resource();
			ClientResponse response = r.Path("ws").Path("v1").Path("cluster").Path("scheduler"
				).Get<ClientResponse>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			JSONObject json = response.GetEntity<JSONObject>();
			VerifyClusterScheduler(json);
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestClusterSchedulerXML()
		{
			WebResource r = Resource();
			ClientResponse response = r.Path("ws").Path("v1").Path("cluster").Path("scheduler/"
				).Accept(MediaType.ApplicationXml).Get<ClientResponse>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationXmlType, response.GetType());
			string xml = response.GetEntity<string>();
			DocumentBuilderFactory dbf = DocumentBuilderFactory.NewInstance();
			DocumentBuilder db = dbf.NewDocumentBuilder();
			InputSource @is = new InputSource();
			@is.SetCharacterStream(new StringReader(xml));
			Document dom = db.Parse(@is);
			NodeList scheduler = dom.GetElementsByTagName("scheduler");
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, scheduler.GetLength
				());
			NodeList schedulerInfo = dom.GetElementsByTagName("schedulerInfo");
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, schedulerInfo.
				GetLength());
			VerifyClusterSchedulerXML(schedulerInfo);
		}

		/// <exception cref="System.Exception"/>
		public virtual void VerifyClusterSchedulerXML(NodeList nodes)
		{
			for (int i = 0; i < nodes.GetLength(); i++)
			{
				Element element = (Element)nodes.Item(i);
				VerifyClusterSchedulerGeneric(WebServicesTestUtils.GetXmlAttrString(element, "xsi:type"
					), WebServicesTestUtils.GetXmlFloat(element, "usedCapacity"), WebServicesTestUtils
					.GetXmlFloat(element, "capacity"), WebServicesTestUtils.GetXmlFloat(element, "maxCapacity"
					), WebServicesTestUtils.GetXmlString(element, "queueName"));
				NodeList children = element.GetChildNodes();
				for (int j = 0; j < children.GetLength(); j++)
				{
					Element qElem = (Element)children.Item(j);
					if (qElem.GetTagName().Equals("queues"))
					{
						NodeList qListInfos = qElem.GetChildNodes();
						for (int k = 0; k < qListInfos.GetLength(); k++)
						{
							Element qElem2 = (Element)qListInfos.Item(k);
							string qName2 = WebServicesTestUtils.GetXmlString(qElem2, "queueName");
							string q2 = CapacitySchedulerConfiguration.Root + "." + qName2;
							VerifySubQueueXML(qElem2, q2, 100, 100);
						}
					}
				}
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void VerifySubQueueXML(Element qElem, string q, float parentAbsCapacity
			, float parentAbsMaxCapacity)
		{
			NodeList children = qElem.GetChildNodes();
			bool hasSubQueues = false;
			for (int j = 0; j < children.GetLength(); j++)
			{
				Element qElem2 = (Element)children.Item(j);
				if (qElem2.GetTagName().Equals("queues"))
				{
					NodeList qListInfos = qElem2.GetChildNodes();
					if (qListInfos.GetLength() > 0)
					{
						hasSubQueues = true;
					}
				}
			}
			TestRMWebServicesCapacitySched.QueueInfo qi = (hasSubQueues) ? new TestRMWebServicesCapacitySched.QueueInfo
				(this) : new TestRMWebServicesCapacitySched.LeafQueueInfo(this);
			qi.capacity = WebServicesTestUtils.GetXmlFloat(qElem, "capacity");
			qi.usedCapacity = WebServicesTestUtils.GetXmlFloat(qElem, "usedCapacity");
			qi.maxCapacity = WebServicesTestUtils.GetXmlFloat(qElem, "maxCapacity");
			qi.absoluteCapacity = WebServicesTestUtils.GetXmlFloat(qElem, "absoluteCapacity");
			qi.absoluteMaxCapacity = WebServicesTestUtils.GetXmlFloat(qElem, "absoluteMaxCapacity"
				);
			qi.absoluteUsedCapacity = WebServicesTestUtils.GetXmlFloat(qElem, "absoluteUsedCapacity"
				);
			qi.numApplications = WebServicesTestUtils.GetXmlInt(qElem, "numApplications");
			qi.queueName = WebServicesTestUtils.GetXmlString(qElem, "queueName");
			qi.state = WebServicesTestUtils.GetXmlString(qElem, "state");
			VerifySubQueueGeneric(q, qi, parentAbsCapacity, parentAbsMaxCapacity);
			if (hasSubQueues)
			{
				for (int j_1 = 0; j_1 < children.GetLength(); j_1++)
				{
					Element qElem2 = (Element)children.Item(j_1);
					if (qElem2.GetTagName().Equals("queues"))
					{
						NodeList qListInfos = qElem2.GetChildNodes();
						for (int k = 0; k < qListInfos.GetLength(); k++)
						{
							Element qElem3 = (Element)qListInfos.Item(k);
							string qName3 = WebServicesTestUtils.GetXmlString(qElem3, "queueName");
							string q3 = q + "." + qName3;
							VerifySubQueueXML(qElem3, q3, qi.absoluteCapacity, qi.absoluteMaxCapacity);
						}
					}
				}
			}
			else
			{
				TestRMWebServicesCapacitySched.LeafQueueInfo lqi = (TestRMWebServicesCapacitySched.LeafQueueInfo
					)qi;
				lqi.numActiveApplications = WebServicesTestUtils.GetXmlInt(qElem, "numActiveApplications"
					);
				lqi.numPendingApplications = WebServicesTestUtils.GetXmlInt(qElem, "numPendingApplications"
					);
				lqi.numContainers = WebServicesTestUtils.GetXmlInt(qElem, "numContainers");
				lqi.maxApplications = WebServicesTestUtils.GetXmlInt(qElem, "maxApplications");
				lqi.maxApplicationsPerUser = WebServicesTestUtils.GetXmlInt(qElem, "maxApplicationsPerUser"
					);
				lqi.userLimit = WebServicesTestUtils.GetXmlInt(qElem, "userLimit");
				lqi.userLimitFactor = WebServicesTestUtils.GetXmlFloat(qElem, "userLimitFactor");
				VerifyLeafQueueGeneric(q, lqi);
			}
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		private void VerifyClusterScheduler(JSONObject json)
		{
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, json.Length());
			JSONObject info = json.GetJSONObject("scheduler");
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, info.Length());
			info = info.GetJSONObject("schedulerInfo");
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 6, info.Length());
			VerifyClusterSchedulerGeneric(info.GetString("type"), (float)info.GetDouble("usedCapacity"
				), (float)info.GetDouble("capacity"), (float)info.GetDouble("maxCapacity"), info
				.GetString("queueName"));
			JSONArray arr = info.GetJSONObject("queues").GetJSONArray("queue");
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 2, arr.Length());
			// test subqueues
			for (int i = 0; i < arr.Length(); i++)
			{
				JSONObject obj = arr.GetJSONObject(i);
				string q = CapacitySchedulerConfiguration.Root + "." + obj.GetString("queueName");
				VerifySubQueue(obj, q, 100, 100);
			}
		}

		/// <exception cref="System.Exception"/>
		private void VerifyClusterSchedulerGeneric(string type, float usedCapacity, float
			 capacity, float maxCapacity, string queueName)
		{
			NUnit.Framework.Assert.IsTrue("type doesn't match", "capacityScheduler".Matches(type
				));
			NUnit.Framework.Assert.AreEqual("usedCapacity doesn't match", 0, usedCapacity, 1e-3f
				);
			NUnit.Framework.Assert.AreEqual("capacity doesn't match", 100, capacity, 1e-3f);
			NUnit.Framework.Assert.AreEqual("maxCapacity doesn't match", 100, maxCapacity, 1e-3f
				);
			NUnit.Framework.Assert.IsTrue("queueName doesn't match", "root".Matches(queueName
				));
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		private void VerifySubQueue(JSONObject info, string q, float parentAbsCapacity, float
			 parentAbsMaxCapacity)
		{
			int numExpectedElements = 13;
			bool isParentQueue = true;
			if (!info.Has("queues"))
			{
				numExpectedElements = 25;
				isParentQueue = false;
			}
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", numExpectedElements
				, info.Length());
			TestRMWebServicesCapacitySched.QueueInfo qi = isParentQueue ? new TestRMWebServicesCapacitySched.QueueInfo
				(this) : new TestRMWebServicesCapacitySched.LeafQueueInfo(this);
			qi.capacity = (float)info.GetDouble("capacity");
			qi.usedCapacity = (float)info.GetDouble("usedCapacity");
			qi.maxCapacity = (float)info.GetDouble("maxCapacity");
			qi.absoluteCapacity = (float)info.GetDouble("absoluteCapacity");
			qi.absoluteMaxCapacity = (float)info.GetDouble("absoluteMaxCapacity");
			qi.absoluteUsedCapacity = (float)info.GetDouble("absoluteUsedCapacity");
			qi.numApplications = info.GetInt("numApplications");
			qi.queueName = info.GetString("queueName");
			qi.state = info.GetString("state");
			VerifySubQueueGeneric(q, qi, parentAbsCapacity, parentAbsMaxCapacity);
			if (isParentQueue)
			{
				JSONArray arr = info.GetJSONObject("queues").GetJSONArray("queue");
				// test subqueues
				for (int i = 0; i < arr.Length(); i++)
				{
					JSONObject obj = arr.GetJSONObject(i);
					string q2 = q + "." + obj.GetString("queueName");
					VerifySubQueue(obj, q2, qi.absoluteCapacity, qi.absoluteMaxCapacity);
				}
			}
			else
			{
				TestRMWebServicesCapacitySched.LeafQueueInfo lqi = (TestRMWebServicesCapacitySched.LeafQueueInfo
					)qi;
				lqi.numActiveApplications = info.GetInt("numActiveApplications");
				lqi.numPendingApplications = info.GetInt("numPendingApplications");
				lqi.numContainers = info.GetInt("numContainers");
				lqi.maxApplications = info.GetInt("maxApplications");
				lqi.maxApplicationsPerUser = info.GetInt("maxApplicationsPerUser");
				lqi.userLimit = info.GetInt("userLimit");
				lqi.userLimitFactor = (float)info.GetDouble("userLimitFactor");
				VerifyLeafQueueGeneric(q, lqi);
			}
		}

		// resourcesUsed and users (per-user resources used) are checked in
		// testPerUserResource()
		/// <exception cref="System.Exception"/>
		private void VerifySubQueueGeneric(string q, TestRMWebServicesCapacitySched.QueueInfo
			 info, float parentAbsCapacity, float parentAbsMaxCapacity)
		{
			string[] qArr = q.Split("\\.");
			NUnit.Framework.Assert.IsTrue("q name invalid: " + q, qArr.Length > 1);
			string qshortName = qArr[qArr.Length - 1];
			NUnit.Framework.Assert.AreEqual("usedCapacity doesn't match", 0, info.usedCapacity
				, 1e-3f);
			NUnit.Framework.Assert.AreEqual("capacity doesn't match", csConf.GetNonLabeledQueueCapacity
				(q), info.capacity, 1e-3f);
			float expectCapacity = csConf.GetNonLabeledQueueMaximumCapacity(q);
			float expectAbsMaxCapacity = parentAbsMaxCapacity * (info.maxCapacity / 100);
			if (CapacitySchedulerConfiguration.Undefined == expectCapacity)
			{
				expectCapacity = 100;
				expectAbsMaxCapacity = 100;
			}
			NUnit.Framework.Assert.AreEqual("maxCapacity doesn't match", expectCapacity, info
				.maxCapacity, 1e-3f);
			NUnit.Framework.Assert.AreEqual("absoluteCapacity doesn't match", parentAbsCapacity
				 * (info.capacity / 100), info.absoluteCapacity, 1e-3f);
			NUnit.Framework.Assert.AreEqual("absoluteMaxCapacity doesn't match", expectAbsMaxCapacity
				, info.absoluteMaxCapacity, 1e-3f);
			NUnit.Framework.Assert.AreEqual("absoluteUsedCapacity doesn't match", 0, info.absoluteUsedCapacity
				, 1e-3f);
			NUnit.Framework.Assert.AreEqual("numApplications doesn't match", 0, info.numApplications
				);
			NUnit.Framework.Assert.IsTrue("queueName doesn't match, got: " + info.queueName +
				 " expected: " + q, qshortName.Matches(info.queueName));
			NUnit.Framework.Assert.IsTrue("state doesn't match", (csConf.GetState(q).ToString
				()).Matches(info.state));
		}

		/// <exception cref="System.Exception"/>
		private void VerifyLeafQueueGeneric(string q, TestRMWebServicesCapacitySched.LeafQueueInfo
			 info)
		{
			NUnit.Framework.Assert.AreEqual("numActiveApplications doesn't match", 0, info.numActiveApplications
				);
			NUnit.Framework.Assert.AreEqual("numPendingApplications doesn't match", 0, info.numPendingApplications
				);
			NUnit.Framework.Assert.AreEqual("numContainers doesn't match", 0, info.numContainers
				);
			int maxSystemApps = csConf.GetMaximumSystemApplications();
			int expectedMaxApps = (int)(maxSystemApps * (info.absoluteCapacity / 100));
			int expectedMaxAppsPerUser = (int)(expectedMaxApps * (info.userLimit / 100.0f) * 
				info.userLimitFactor);
			// TODO: would like to use integer comparisons here but can't due to
			//       roundoff errors in absolute capacity calculations
			NUnit.Framework.Assert.AreEqual("maxApplications doesn't match", (float)expectedMaxApps
				, (float)info.maxApplications, 1.0f);
			NUnit.Framework.Assert.AreEqual("maxApplicationsPerUser doesn't match", (float)expectedMaxAppsPerUser
				, (float)info.maxApplicationsPerUser, info.userLimitFactor);
			NUnit.Framework.Assert.AreEqual("userLimit doesn't match", csConf.GetUserLimit(q)
				, info.userLimit);
			NUnit.Framework.Assert.AreEqual("userLimitFactor doesn't match", csConf.GetUserLimitFactor
				(q), info.userLimitFactor, 1e-3f);
		}

		//Return a child Node of node with the tagname or null if none exists 
		private Node GetChildNodeByName(Node node, string tagname)
		{
			NodeList nodeList = node.GetChildNodes();
			for (int i = 0; i < nodeList.GetLength(); ++i)
			{
				if (nodeList.Item(i).GetNodeName().Equals(tagname))
				{
					return nodeList.Item(i);
				}
			}
			return null;
		}

		/// <summary>Test per user resources and resourcesUsed elements in the web services XML
		/// 	</summary>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestPerUserResourcesXML()
		{
			//Start RM so that it accepts app submissions
			rm.Start();
			try
			{
				rm.SubmitApp(10, "app1", "user1", null, "b1");
				rm.SubmitApp(20, "app2", "user2", null, "b1");
				//Get the XML from ws/v1/cluster/scheduler
				WebResource r = Resource();
				ClientResponse response = r.Path("ws/v1/cluster/scheduler").Accept(MediaType.ApplicationXml
					).Get<ClientResponse>();
				NUnit.Framework.Assert.AreEqual(MediaType.ApplicationXmlType, response.GetType());
				string xml = response.GetEntity<string>();
				DocumentBuilder db = DocumentBuilderFactory.NewInstance().NewDocumentBuilder();
				InputSource @is = new InputSource();
				@is.SetCharacterStream(new StringReader(xml));
				//Parse the XML we got
				Document dom = db.Parse(@is);
				//Get all users elements (1 for each leaf queue)
				NodeList allUsers = dom.GetElementsByTagName("users");
				for (int i = 0; i < allUsers.GetLength(); ++i)
				{
					Node perUserResources = allUsers.Item(i);
					string queueName = GetChildNodeByName(perUserResources.GetParentNode(), "queueName"
						).GetTextContent();
					if (queueName.Equals("b1"))
					{
						//b1 should have two users (user1 and user2) which submitted jobs
						NUnit.Framework.Assert.AreEqual(2, perUserResources.GetChildNodes().GetLength());
						NodeList users = perUserResources.GetChildNodes();
						for (int j = 0; j < users.GetLength(); ++j)
						{
							Node user = users.Item(j);
							string username = GetChildNodeByName(user, "username").GetTextContent();
							NUnit.Framework.Assert.IsTrue(username.Equals("user1") || username.Equals("user2"
								));
							//Should be a parsable integer
							System.Convert.ToInt32(GetChildNodeByName(GetChildNodeByName(user, "resourcesUsed"
								), "memory").GetTextContent());
							System.Convert.ToInt32(GetChildNodeByName(user, "numActiveApplications").GetTextContent
								());
							System.Convert.ToInt32(GetChildNodeByName(user, "numPendingApplications").GetTextContent
								());
						}
					}
					else
					{
						//Queues other than b1 should have 0 users
						NUnit.Framework.Assert.AreEqual(0, perUserResources.GetChildNodes().GetLength());
					}
				}
				NodeList allResourcesUsed = dom.GetElementsByTagName("resourcesUsed");
				for (int i_1 = 0; i_1 < allResourcesUsed.GetLength(); ++i_1)
				{
					Node resourcesUsed = allResourcesUsed.Item(i_1);
					System.Convert.ToInt32(GetChildNodeByName(resourcesUsed, "memory").GetTextContent
						());
					System.Convert.ToInt32(GetChildNodeByName(resourcesUsed, "vCores").GetTextContent
						());
				}
			}
			finally
			{
				rm.Stop();
			}
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		private void CheckResourcesUsed(JSONObject queue)
		{
			queue.GetJSONObject("resourcesUsed").GetInt("memory");
			queue.GetJSONObject("resourcesUsed").GetInt("vCores");
		}

		//Also checks resourcesUsed
		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		private JSONObject GetSubQueue(JSONObject queue, string subQueue)
		{
			JSONArray queues = queue.GetJSONObject("queues").GetJSONArray("queue");
			for (int i = 0; i < queues.Length(); ++i)
			{
				CheckResourcesUsed(queues.GetJSONObject(i));
				if (queues.GetJSONObject(i).GetString("queueName").Equals(subQueue))
				{
					return queues.GetJSONObject(i);
				}
			}
			return null;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestPerUserResourcesJSON()
		{
			//Start RM so that it accepts app submissions
			rm.Start();
			try
			{
				rm.SubmitApp(10, "app1", "user1", null, "b1");
				rm.SubmitApp(20, "app2", "user2", null, "b1");
				//Get JSON
				WebResource r = Resource();
				ClientResponse response = r.Path("ws").Path("v1").Path("cluster").Path("scheduler/"
					).Accept(MediaType.ApplicationJson).Get<ClientResponse>();
				NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
					);
				JSONObject json = response.GetEntity<JSONObject>();
				JSONObject schedulerInfo = json.GetJSONObject("scheduler").GetJSONObject("schedulerInfo"
					);
				JSONObject b1 = GetSubQueue(GetSubQueue(schedulerInfo, "b"), "b1");
				//Check users user1 and user2 exist in b1
				JSONArray users = b1.GetJSONObject("users").GetJSONArray("user");
				for (int i = 0; i < 2; ++i)
				{
					JSONObject user = users.GetJSONObject(i);
					NUnit.Framework.Assert.IsTrue("User isn't user1 or user2", user.GetString("username"
						).Equals("user1") || user.GetString("username").Equals("user2"));
					user.GetInt("numActiveApplications");
					user.GetInt("numPendingApplications");
					CheckResourcesUsed(user);
				}
			}
			finally
			{
				rm.Stop();
			}
		}

		[NUnit.Framework.Test]
		public virtual void TestResourceInfo()
		{
			Org.Apache.Hadoop.Yarn.Api.Records.Resource res = Resources.CreateResource(10, 1);
			// If we add a new resource (e.g disks), then
			// CapacitySchedulerPage and these RM WebServices + docs need to be updated
			// eg. ResourceInfo
			NUnit.Framework.Assert.AreEqual("<memory:10, vCores:1>", res.ToString());
		}
	}
}
