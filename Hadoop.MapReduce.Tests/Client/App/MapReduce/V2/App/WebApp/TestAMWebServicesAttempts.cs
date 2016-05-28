using System.Collections.Generic;
using System.IO;
using System.Text;
using Com.Google.Inject;
using Com.Google.Inject.Servlet;
using Com.Sun.Jersey.Api.Client;
using Com.Sun.Jersey.Guice.Spi.Container.Servlet;
using Com.Sun.Jersey.Test.Framework;
using Javax.WS.RS.Core;
using Javax.Xml.Parsers;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Mapreduce.V2.App;
using Org.Apache.Hadoop.Mapreduce.V2.App.Job;
using Org.Apache.Hadoop.Mapreduce.V2.Util;
using Org.Apache.Hadoop.Yarn.Util;
using Org.Apache.Hadoop.Yarn.Webapp;
using Org.Codehaus.Jettison.Json;
using Org.W3c.Dom;
using Org.Xml.Sax;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App.Webapp
{
	/// <summary>
	/// Test the app master web service Rest API for getting task attempts, a
	/// specific task attempt, and task attempt counters
	/// /ws/v1/mapreduce/jobs/{jobid}/tasks/{taskid}/attempts
	/// /ws/v1/mapreduce/jobs/{jobid}/tasks/{taskid}/attempts/{attemptid}
	/// /ws/v1/mapreduce/jobs/{jobid}/tasks/{taskid}/attempts/{attemptid}/counters
	/// </summary>
	public class TestAMWebServicesAttempts : JerseyTest
	{
		private static Configuration conf = new Configuration();

		private static AppContext appContext;

		private sealed class _ServletModule_83 : ServletModule
		{
			public _ServletModule_83()
			{
			}

			protected override void ConfigureServlets()
			{
				Org.Apache.Hadoop.Mapreduce.V2.App.Webapp.TestAMWebServicesAttempts.appContext = 
					new MockAppContext(0, 1, 2, 1);
				this.Bind<JAXBContextResolver>();
				this.Bind<AMWebServices>();
				this.Bind<GenericExceptionHandler>();
				this.Bind<AppContext>().ToInstance(Org.Apache.Hadoop.Mapreduce.V2.App.Webapp.TestAMWebServicesAttempts
					.appContext);
				this.Bind<Configuration>().ToInstance(Org.Apache.Hadoop.Mapreduce.V2.App.Webapp.TestAMWebServicesAttempts
					.conf);
				this.Serve("/*").With(typeof(GuiceContainer));
			}
		}

		private Injector injector = Guice.CreateInjector(new _ServletModule_83());

		public class GuiceServletConfig : GuiceServletContextListener
		{
			protected override Injector GetInjector()
			{
				return this._enclosing.injector;
			}

			internal GuiceServletConfig(TestAMWebServicesAttempts _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly TestAMWebServicesAttempts _enclosing;
		}

		/// <exception cref="System.Exception"/>
		[SetUp]
		public override void SetUp()
		{
			base.SetUp();
		}

		public TestAMWebServicesAttempts()
			: base(new WebAppDescriptor.Builder("org.apache.hadoop.mapreduce.v2.app.webapp").
				ContextListenerClass(typeof(TestAMWebServicesAttempts.GuiceServletConfig)).FilterClass
				(typeof(GuiceFilter)).ContextPath("jersey-guice-filter").ServletPath("/").Build(
				))
		{
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestTaskAttempts()
		{
			WebResource r = Resource();
			IDictionary<JobId, Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job> jobsMap = appContext
				.GetAllJobs();
			foreach (JobId id in jobsMap.Keys)
			{
				string jobId = MRApps.ToString(id);
				foreach (Task task in jobsMap[id].GetTasks().Values)
				{
					string tid = MRApps.ToString(task.GetID());
					ClientResponse response = r.Path("ws").Path("v1").Path("mapreduce").Path("jobs").
						Path(jobId).Path("tasks").Path(tid).Path("attempts").Accept(MediaType.ApplicationJson
						).Get<ClientResponse>();
					NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
						);
					JSONObject json = response.GetEntity<JSONObject>();
					VerifyAMTaskAttempts(json, task);
				}
			}
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestTaskAttemptsSlash()
		{
			WebResource r = Resource();
			IDictionary<JobId, Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job> jobsMap = appContext
				.GetAllJobs();
			foreach (JobId id in jobsMap.Keys)
			{
				string jobId = MRApps.ToString(id);
				foreach (Task task in jobsMap[id].GetTasks().Values)
				{
					string tid = MRApps.ToString(task.GetID());
					ClientResponse response = r.Path("ws").Path("v1").Path("mapreduce").Path("jobs").
						Path(jobId).Path("tasks").Path(tid).Path("attempts/").Accept(MediaType.ApplicationJson
						).Get<ClientResponse>();
					NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
						);
					JSONObject json = response.GetEntity<JSONObject>();
					VerifyAMTaskAttempts(json, task);
				}
			}
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestTaskAttemptsDefault()
		{
			WebResource r = Resource();
			IDictionary<JobId, Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job> jobsMap = appContext
				.GetAllJobs();
			foreach (JobId id in jobsMap.Keys)
			{
				string jobId = MRApps.ToString(id);
				foreach (Task task in jobsMap[id].GetTasks().Values)
				{
					string tid = MRApps.ToString(task.GetID());
					ClientResponse response = r.Path("ws").Path("v1").Path("mapreduce").Path("jobs").
						Path(jobId).Path("tasks").Path(tid).Path("attempts").Get<ClientResponse>();
					NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
						);
					JSONObject json = response.GetEntity<JSONObject>();
					VerifyAMTaskAttempts(json, task);
				}
			}
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestTaskAttemptsXML()
		{
			WebResource r = Resource();
			IDictionary<JobId, Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job> jobsMap = appContext
				.GetAllJobs();
			foreach (JobId id in jobsMap.Keys)
			{
				string jobId = MRApps.ToString(id);
				foreach (Task task in jobsMap[id].GetTasks().Values)
				{
					string tid = MRApps.ToString(task.GetID());
					ClientResponse response = r.Path("ws").Path("v1").Path("mapreduce").Path("jobs").
						Path(jobId).Path("tasks").Path(tid).Path("attempts").Accept(MediaType.ApplicationXml
						).Get<ClientResponse>();
					NUnit.Framework.Assert.AreEqual(MediaType.ApplicationXmlType, response.GetType());
					string xml = response.GetEntity<string>();
					DocumentBuilderFactory dbf = DocumentBuilderFactory.NewInstance();
					DocumentBuilder db = dbf.NewDocumentBuilder();
					InputSource @is = new InputSource();
					@is.SetCharacterStream(new StringReader(xml));
					Document dom = db.Parse(@is);
					NodeList attempts = dom.GetElementsByTagName("taskAttempts");
					NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, attempts.GetLength
						());
					NodeList nodes = dom.GetElementsByTagName("taskAttempt");
					VerifyAMTaskAttemptsXML(nodes, task);
				}
			}
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestTaskAttemptId()
		{
			WebResource r = Resource();
			IDictionary<JobId, Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job> jobsMap = appContext
				.GetAllJobs();
			foreach (JobId id in jobsMap.Keys)
			{
				string jobId = MRApps.ToString(id);
				foreach (Task task in jobsMap[id].GetTasks().Values)
				{
					string tid = MRApps.ToString(task.GetID());
					foreach (TaskAttempt att in task.GetAttempts().Values)
					{
						TaskAttemptId attemptid = att.GetID();
						string attid = MRApps.ToString(attemptid);
						ClientResponse response = r.Path("ws").Path("v1").Path("mapreduce").Path("jobs").
							Path(jobId).Path("tasks").Path(tid).Path("attempts").Path(attid).Accept(MediaType
							.ApplicationJson).Get<ClientResponse>();
						NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
							);
						JSONObject json = response.GetEntity<JSONObject>();
						NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, json.Length());
						JSONObject info = json.GetJSONObject("taskAttempt");
						VerifyAMTaskAttempt(info, att, task.GetType());
					}
				}
			}
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestTaskAttemptIdSlash()
		{
			WebResource r = Resource();
			IDictionary<JobId, Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job> jobsMap = appContext
				.GetAllJobs();
			foreach (JobId id in jobsMap.Keys)
			{
				string jobId = MRApps.ToString(id);
				foreach (Task task in jobsMap[id].GetTasks().Values)
				{
					string tid = MRApps.ToString(task.GetID());
					foreach (TaskAttempt att in task.GetAttempts().Values)
					{
						TaskAttemptId attemptid = att.GetID();
						string attid = MRApps.ToString(attemptid);
						ClientResponse response = r.Path("ws").Path("v1").Path("mapreduce").Path("jobs").
							Path(jobId).Path("tasks").Path(tid).Path("attempts").Path(attid + "/").Accept(MediaType
							.ApplicationJson).Get<ClientResponse>();
						NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
							);
						JSONObject json = response.GetEntity<JSONObject>();
						NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, json.Length());
						JSONObject info = json.GetJSONObject("taskAttempt");
						VerifyAMTaskAttempt(info, att, task.GetType());
					}
				}
			}
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestTaskAttemptIdDefault()
		{
			WebResource r = Resource();
			IDictionary<JobId, Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job> jobsMap = appContext
				.GetAllJobs();
			foreach (JobId id in jobsMap.Keys)
			{
				string jobId = MRApps.ToString(id);
				foreach (Task task in jobsMap[id].GetTasks().Values)
				{
					string tid = MRApps.ToString(task.GetID());
					foreach (TaskAttempt att in task.GetAttempts().Values)
					{
						TaskAttemptId attemptid = att.GetID();
						string attid = MRApps.ToString(attemptid);
						ClientResponse response = r.Path("ws").Path("v1").Path("mapreduce").Path("jobs").
							Path(jobId).Path("tasks").Path(tid).Path("attempts").Path(attid).Get<ClientResponse
							>();
						NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
							);
						JSONObject json = response.GetEntity<JSONObject>();
						NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, json.Length());
						JSONObject info = json.GetJSONObject("taskAttempt");
						VerifyAMTaskAttempt(info, att, task.GetType());
					}
				}
			}
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestTaskAttemptIdXML()
		{
			WebResource r = Resource();
			IDictionary<JobId, Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job> jobsMap = appContext
				.GetAllJobs();
			foreach (JobId id in jobsMap.Keys)
			{
				string jobId = MRApps.ToString(id);
				foreach (Task task in jobsMap[id].GetTasks().Values)
				{
					string tid = MRApps.ToString(task.GetID());
					foreach (TaskAttempt att in task.GetAttempts().Values)
					{
						TaskAttemptId attemptid = att.GetID();
						string attid = MRApps.ToString(attemptid);
						ClientResponse response = r.Path("ws").Path("v1").Path("mapreduce").Path("jobs").
							Path(jobId).Path("tasks").Path(tid).Path("attempts").Path(attid).Accept(MediaType
							.ApplicationXml).Get<ClientResponse>();
						NUnit.Framework.Assert.AreEqual(MediaType.ApplicationXmlType, response.GetType());
						string xml = response.GetEntity<string>();
						DocumentBuilderFactory dbf = DocumentBuilderFactory.NewInstance();
						DocumentBuilder db = dbf.NewDocumentBuilder();
						InputSource @is = new InputSource();
						@is.SetCharacterStream(new StringReader(xml));
						Document dom = db.Parse(@is);
						NodeList nodes = dom.GetElementsByTagName("taskAttempt");
						for (int i = 0; i < nodes.GetLength(); i++)
						{
							Element element = (Element)nodes.Item(i);
							VerifyAMTaskAttemptXML(element, att, task.GetType());
						}
					}
				}
			}
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestTaskAttemptIdBogus()
		{
			TestTaskAttemptIdErrorGeneric("bogusid", "java.lang.Exception: TaskAttemptId string : bogusid is not properly formed"
				);
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestTaskAttemptIdNonExist()
		{
			TestTaskAttemptIdErrorGeneric("attempt_0_12345_m_000000_0", "java.lang.Exception: Error getting info on task attempt id attempt_0_12345_m_000000_0"
				);
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestTaskAttemptIdInvalid()
		{
			TestTaskAttemptIdErrorGeneric("attempt_0_12345_d_000000_0", "java.lang.Exception: Bad TaskType identifier. TaskAttemptId string : attempt_0_12345_d_000000_0 is not properly formed."
				);
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestTaskAttemptIdInvalid2()
		{
			TestTaskAttemptIdErrorGeneric("attempt_12345_m_000000_0", "java.lang.Exception: TaskAttemptId string : attempt_12345_m_000000_0 is not properly formed"
				);
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestTaskAttemptIdInvalid3()
		{
			TestTaskAttemptIdErrorGeneric("attempt_0_12345_m_000000", "java.lang.Exception: TaskAttemptId string : attempt_0_12345_m_000000 is not properly formed"
				);
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		private void TestTaskAttemptIdErrorGeneric(string attid, string error)
		{
			WebResource r = Resource();
			IDictionary<JobId, Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job> jobsMap = appContext
				.GetAllJobs();
			foreach (JobId id in jobsMap.Keys)
			{
				string jobId = MRApps.ToString(id);
				foreach (Task task in jobsMap[id].GetTasks().Values)
				{
					string tid = MRApps.ToString(task.GetID());
					try
					{
						r.Path("ws").Path("v1").Path("mapreduce").Path("jobs").Path(jobId).Path("tasks").
							Path(tid).Path("attempts").Path(attid).Accept(MediaType.ApplicationJson).Get<JSONObject
							>();
						NUnit.Framework.Assert.Fail("should have thrown exception on invalid uri");
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
						WebServicesTestUtils.CheckStringMatch("exception message", error, message);
						WebServicesTestUtils.CheckStringMatch("exception type", "NotFoundException", type
							);
						WebServicesTestUtils.CheckStringMatch("exception classname", "org.apache.hadoop.yarn.webapp.NotFoundException"
							, classname);
					}
				}
			}
		}

		public virtual void VerifyAMTaskAttemptXML(Element element, TaskAttempt att, TaskType
			 ttype)
		{
			VerifyTaskAttemptGeneric(att, ttype, WebServicesTestUtils.GetXmlString(element, "id"
				), WebServicesTestUtils.GetXmlString(element, "state"), WebServicesTestUtils.GetXmlString
				(element, "type"), WebServicesTestUtils.GetXmlString(element, "rack"), WebServicesTestUtils
				.GetXmlString(element, "nodeHttpAddress"), WebServicesTestUtils.GetXmlString(element
				, "diagnostics"), WebServicesTestUtils.GetXmlString(element, "assignedContainerId"
				), WebServicesTestUtils.GetXmlLong(element, "startTime"), WebServicesTestUtils.GetXmlLong
				(element, "finishTime"), WebServicesTestUtils.GetXmlLong(element, "elapsedTime")
				, WebServicesTestUtils.GetXmlFloat(element, "progress"));
			if (ttype == TaskType.Reduce)
			{
				VerifyReduceTaskAttemptGeneric(att, WebServicesTestUtils.GetXmlLong(element, "shuffleFinishTime"
					), WebServicesTestUtils.GetXmlLong(element, "mergeFinishTime"), WebServicesTestUtils
					.GetXmlLong(element, "elapsedShuffleTime"), WebServicesTestUtils.GetXmlLong(element
					, "elapsedMergeTime"), WebServicesTestUtils.GetXmlLong(element, "elapsedReduceTime"
					));
			}
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		public virtual void VerifyAMTaskAttempt(JSONObject info, TaskAttempt att, TaskType
			 ttype)
		{
			if (ttype == TaskType.Reduce)
			{
				NUnit.Framework.Assert.AreEqual("incorrect number of elements", 17, info.Length()
					);
			}
			else
			{
				NUnit.Framework.Assert.AreEqual("incorrect number of elements", 12, info.Length()
					);
			}
			VerifyTaskAttemptGeneric(att, ttype, info.GetString("id"), info.GetString("state"
				), info.GetString("type"), info.GetString("rack"), info.GetString("nodeHttpAddress"
				), info.GetString("diagnostics"), info.GetString("assignedContainerId"), info.GetLong
				("startTime"), info.GetLong("finishTime"), info.GetLong("elapsedTime"), (float)info
				.GetDouble("progress"));
			if (ttype == TaskType.Reduce)
			{
				VerifyReduceTaskAttemptGeneric(att, info.GetLong("shuffleFinishTime"), info.GetLong
					("mergeFinishTime"), info.GetLong("elapsedShuffleTime"), info.GetLong("elapsedMergeTime"
					), info.GetLong("elapsedReduceTime"));
			}
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		public virtual void VerifyAMTaskAttempts(JSONObject json, Task task)
		{
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, json.Length());
			JSONObject attempts = json.GetJSONObject("taskAttempts");
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, json.Length());
			JSONArray arr = attempts.GetJSONArray("taskAttempt");
			foreach (TaskAttempt att in task.GetAttempts().Values)
			{
				TaskAttemptId id = att.GetID();
				string attid = MRApps.ToString(id);
				bool found = false;
				for (int i = 0; i < arr.Length(); i++)
				{
					JSONObject info = arr.GetJSONObject(i);
					if (attid.Matches(info.GetString("id")))
					{
						found = true;
						VerifyAMTaskAttempt(info, att, task.GetType());
					}
				}
				NUnit.Framework.Assert.IsTrue("task attempt with id: " + attid + " not in web service output"
					, found);
			}
		}

		public virtual void VerifyAMTaskAttemptsXML(NodeList nodes, Task task)
		{
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, nodes.GetLength
				());
			foreach (TaskAttempt att in task.GetAttempts().Values)
			{
				TaskAttemptId id = att.GetID();
				string attid = MRApps.ToString(id);
				bool found = false;
				for (int i = 0; i < nodes.GetLength(); i++)
				{
					Element element = (Element)nodes.Item(i);
					if (attid.Matches(WebServicesTestUtils.GetXmlString(element, "id")))
					{
						found = true;
						VerifyAMTaskAttemptXML(element, att, task.GetType());
					}
				}
				NUnit.Framework.Assert.IsTrue("task with id: " + attid + " not in web service output"
					, found);
			}
		}

		public virtual void VerifyTaskAttemptGeneric(TaskAttempt ta, TaskType ttype, string
			 id, string state, string type, string rack, string nodeHttpAddress, string diagnostics
			, string assignedContainerId, long startTime, long finishTime, long elapsedTime, 
			float progress)
		{
			TaskAttemptId attid = ta.GetID();
			string attemptId = MRApps.ToString(attid);
			WebServicesTestUtils.CheckStringMatch("id", attemptId, id);
			WebServicesTestUtils.CheckStringMatch("type", ttype.ToString(), type);
			WebServicesTestUtils.CheckStringMatch("state", ta.GetState().ToString(), state);
			WebServicesTestUtils.CheckStringMatch("rack", ta.GetNodeRackName(), rack);
			WebServicesTestUtils.CheckStringMatch("nodeHttpAddress", ta.GetNodeHttpAddress(), 
				nodeHttpAddress);
			string expectDiag = string.Empty;
			IList<string> diagnosticsList = ta.GetDiagnostics();
			if (diagnosticsList != null && !diagnostics.IsEmpty())
			{
				StringBuilder b = new StringBuilder();
				foreach (string diag in diagnosticsList)
				{
					b.Append(diag);
				}
				expectDiag = b.ToString();
			}
			WebServicesTestUtils.CheckStringMatch("diagnostics", expectDiag, diagnostics);
			WebServicesTestUtils.CheckStringMatch("assignedContainerId", ConverterUtils.ToString
				(ta.GetAssignedContainerID()), assignedContainerId);
			NUnit.Framework.Assert.AreEqual("startTime wrong", ta.GetLaunchTime(), startTime);
			NUnit.Framework.Assert.AreEqual("finishTime wrong", ta.GetFinishTime(), finishTime
				);
			NUnit.Framework.Assert.AreEqual("elapsedTime wrong", finishTime - startTime, elapsedTime
				);
			NUnit.Framework.Assert.AreEqual("progress wrong", ta.GetProgress() * 100, progress
				, 1e-3f);
		}

		public virtual void VerifyReduceTaskAttemptGeneric(TaskAttempt ta, long shuffleFinishTime
			, long mergeFinishTime, long elapsedShuffleTime, long elapsedMergeTime, long elapsedReduceTime
			)
		{
			NUnit.Framework.Assert.AreEqual("shuffleFinishTime wrong", ta.GetShuffleFinishTime
				(), shuffleFinishTime);
			NUnit.Framework.Assert.AreEqual("mergeFinishTime wrong", ta.GetSortFinishTime(), 
				mergeFinishTime);
			NUnit.Framework.Assert.AreEqual("elapsedShuffleTime wrong", ta.GetShuffleFinishTime
				() - ta.GetLaunchTime(), elapsedShuffleTime);
			NUnit.Framework.Assert.AreEqual("elapsedMergeTime wrong", ta.GetSortFinishTime() 
				- ta.GetShuffleFinishTime(), elapsedMergeTime);
			NUnit.Framework.Assert.AreEqual("elapsedReduceTime wrong", ta.GetFinishTime() - ta
				.GetSortFinishTime(), elapsedReduceTime);
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestTaskAttemptIdCounters()
		{
			WebResource r = Resource();
			IDictionary<JobId, Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job> jobsMap = appContext
				.GetAllJobs();
			foreach (JobId id in jobsMap.Keys)
			{
				string jobId = MRApps.ToString(id);
				foreach (Task task in jobsMap[id].GetTasks().Values)
				{
					string tid = MRApps.ToString(task.GetID());
					foreach (TaskAttempt att in task.GetAttempts().Values)
					{
						TaskAttemptId attemptid = att.GetID();
						string attid = MRApps.ToString(attemptid);
						ClientResponse response = r.Path("ws").Path("v1").Path("mapreduce").Path("jobs").
							Path(jobId).Path("tasks").Path(tid).Path("attempts").Path(attid).Path("counters"
							).Accept(MediaType.ApplicationJson).Get<ClientResponse>();
						NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
							);
						JSONObject json = response.GetEntity<JSONObject>();
						NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, json.Length());
						JSONObject info = json.GetJSONObject("jobTaskAttemptCounters");
						VerifyAMJobTaskAttemptCounters(info, att);
					}
				}
			}
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestTaskAttemptIdXMLCounters()
		{
			WebResource r = Resource();
			IDictionary<JobId, Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job> jobsMap = appContext
				.GetAllJobs();
			foreach (JobId id in jobsMap.Keys)
			{
				string jobId = MRApps.ToString(id);
				foreach (Task task in jobsMap[id].GetTasks().Values)
				{
					string tid = MRApps.ToString(task.GetID());
					foreach (TaskAttempt att in task.GetAttempts().Values)
					{
						TaskAttemptId attemptid = att.GetID();
						string attid = MRApps.ToString(attemptid);
						ClientResponse response = r.Path("ws").Path("v1").Path("mapreduce").Path("jobs").
							Path(jobId).Path("tasks").Path(tid).Path("attempts").Path(attid).Path("counters"
							).Accept(MediaType.ApplicationXml).Get<ClientResponse>();
						NUnit.Framework.Assert.AreEqual(MediaType.ApplicationXmlType, response.GetType());
						string xml = response.GetEntity<string>();
						DocumentBuilderFactory dbf = DocumentBuilderFactory.NewInstance();
						DocumentBuilder db = dbf.NewDocumentBuilder();
						InputSource @is = new InputSource();
						@is.SetCharacterStream(new StringReader(xml));
						Document dom = db.Parse(@is);
						NodeList nodes = dom.GetElementsByTagName("jobTaskAttemptCounters");
						VerifyAMTaskCountersXML(nodes, att);
					}
				}
			}
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		public virtual void VerifyAMJobTaskAttemptCounters(JSONObject info, TaskAttempt att
			)
		{
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 2, info.Length());
			WebServicesTestUtils.CheckStringMatch("id", MRApps.ToString(att.GetID()), info.GetString
				("id"));
			// just do simple verification of fields - not data is correct
			// in the fields
			JSONArray counterGroups = info.GetJSONArray("taskAttemptCounterGroup");
			for (int i = 0; i < counterGroups.Length(); i++)
			{
				JSONObject counterGroup = counterGroups.GetJSONObject(i);
				string name = counterGroup.GetString("counterGroupName");
				NUnit.Framework.Assert.IsTrue("name not set", (name != null && !name.IsEmpty()));
				JSONArray counters = counterGroup.GetJSONArray("counter");
				for (int j = 0; j < counters.Length(); j++)
				{
					JSONObject counter = counters.GetJSONObject(j);
					string counterName = counter.GetString("name");
					NUnit.Framework.Assert.IsTrue("name not set", (counterName != null && !counterName
						.IsEmpty()));
					long value = counter.GetLong("value");
					NUnit.Framework.Assert.IsTrue("value  >= 0", value >= 0);
				}
			}
		}

		public virtual void VerifyAMTaskCountersXML(NodeList nodes, TaskAttempt att)
		{
			for (int i = 0; i < nodes.GetLength(); i++)
			{
				Element element = (Element)nodes.Item(i);
				WebServicesTestUtils.CheckStringMatch("id", MRApps.ToString(att.GetID()), WebServicesTestUtils
					.GetXmlString(element, "id"));
				// just do simple verification of fields - not data is correct
				// in the fields
				NodeList groups = element.GetElementsByTagName("taskAttemptCounterGroup");
				for (int j = 0; j < groups.GetLength(); j++)
				{
					Element counters = (Element)groups.Item(j);
					NUnit.Framework.Assert.IsNotNull("should have counters in the web service info", 
						counters);
					string name = WebServicesTestUtils.GetXmlString(counters, "counterGroupName");
					NUnit.Framework.Assert.IsTrue("name not set", (name != null && !name.IsEmpty()));
					NodeList counterArr = counters.GetElementsByTagName("counter");
					for (int z = 0; z < counterArr.GetLength(); z++)
					{
						Element counter = (Element)counterArr.Item(z);
						string counterName = WebServicesTestUtils.GetXmlString(counter, "name");
						NUnit.Framework.Assert.IsTrue("counter name not set", (counterName != null && !counterName
							.IsEmpty()));
						long value = WebServicesTestUtils.GetXmlLong(counter, "value");
						NUnit.Framework.Assert.IsTrue("value not >= 0", value >= 0);
					}
				}
			}
		}
	}
}
