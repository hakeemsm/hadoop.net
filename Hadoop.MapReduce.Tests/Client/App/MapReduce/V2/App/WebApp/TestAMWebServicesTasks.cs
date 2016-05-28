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
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Mapreduce.V2.App;
using Org.Apache.Hadoop.Mapreduce.V2.App.Job;
using Org.Apache.Hadoop.Mapreduce.V2.Util;
using Org.Apache.Hadoop.Yarn.Webapp;
using Org.Codehaus.Jettison.Json;
using Org.W3c.Dom;
using Org.Xml.Sax;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App.Webapp
{
	/// <summary>
	/// Test the app master web service Rest API for getting tasks, a specific task,
	/// and task counters.
	/// </summary>
	/// <remarks>
	/// Test the app master web service Rest API for getting tasks, a specific task,
	/// and task counters.
	/// /ws/v1/mapreduce/jobs/{jobid}/tasks
	/// /ws/v1/mapreduce/jobs/{jobid}/tasks/{taskid}
	/// /ws/v1/mapreduce/jobs/{jobid}/tasks/{taskid}/counters
	/// </remarks>
	public class TestAMWebServicesTasks : JerseyTest
	{
		private static Configuration conf = new Configuration();

		private static AppContext appContext;

		private sealed class _ServletModule_79 : ServletModule
		{
			public _ServletModule_79()
			{
			}

			protected override void ConfigureServlets()
			{
				Org.Apache.Hadoop.Mapreduce.V2.App.Webapp.TestAMWebServicesTasks.appContext = new 
					MockAppContext(0, 1, 2, 1);
				this.Bind<JAXBContextResolver>();
				this.Bind<AMWebServices>();
				this.Bind<GenericExceptionHandler>();
				this.Bind<AppContext>().ToInstance(Org.Apache.Hadoop.Mapreduce.V2.App.Webapp.TestAMWebServicesTasks
					.appContext);
				this.Bind<Configuration>().ToInstance(Org.Apache.Hadoop.Mapreduce.V2.App.Webapp.TestAMWebServicesTasks
					.conf);
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

			internal GuiceServletConfig(TestAMWebServicesTasks _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly TestAMWebServicesTasks _enclosing;
		}

		/// <exception cref="System.Exception"/>
		[SetUp]
		public override void SetUp()
		{
			base.SetUp();
		}

		public TestAMWebServicesTasks()
			: base(new WebAppDescriptor.Builder("org.apache.hadoop.mapreduce.v2.app.webapp").
				ContextListenerClass(typeof(TestAMWebServicesTasks.GuiceServletConfig)).FilterClass
				(typeof(GuiceFilter)).ContextPath("jersey-guice-filter").ServletPath("/").Build(
				))
		{
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestTasks()
		{
			WebResource r = Resource();
			IDictionary<JobId, Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job> jobsMap = appContext
				.GetAllJobs();
			foreach (JobId id in jobsMap.Keys)
			{
				string jobId = MRApps.ToString(id);
				ClientResponse response = r.Path("ws").Path("v1").Path("mapreduce").Path("jobs").
					Path(jobId).Path("tasks").Accept(MediaType.ApplicationJson).Get<ClientResponse>(
					);
				NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
					);
				JSONObject json = response.GetEntity<JSONObject>();
				NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, json.Length());
				JSONObject tasks = json.GetJSONObject("tasks");
				JSONArray arr = tasks.GetJSONArray("task");
				NUnit.Framework.Assert.AreEqual("incorrect number of elements", 2, arr.Length());
				VerifyAMTask(arr, jobsMap[id], null);
			}
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestTasksDefault()
		{
			WebResource r = Resource();
			IDictionary<JobId, Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job> jobsMap = appContext
				.GetAllJobs();
			foreach (JobId id in jobsMap.Keys)
			{
				string jobId = MRApps.ToString(id);
				ClientResponse response = r.Path("ws").Path("v1").Path("mapreduce").Path("jobs").
					Path(jobId).Path("tasks").Get<ClientResponse>();
				NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
					);
				JSONObject json = response.GetEntity<JSONObject>();
				NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, json.Length());
				JSONObject tasks = json.GetJSONObject("tasks");
				JSONArray arr = tasks.GetJSONArray("task");
				NUnit.Framework.Assert.AreEqual("incorrect number of elements", 2, arr.Length());
				VerifyAMTask(arr, jobsMap[id], null);
			}
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestTasksSlash()
		{
			WebResource r = Resource();
			IDictionary<JobId, Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job> jobsMap = appContext
				.GetAllJobs();
			foreach (JobId id in jobsMap.Keys)
			{
				string jobId = MRApps.ToString(id);
				ClientResponse response = r.Path("ws").Path("v1").Path("mapreduce").Path("jobs").
					Path(jobId).Path("tasks/").Accept(MediaType.ApplicationJson).Get<ClientResponse>
					();
				NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
					);
				JSONObject json = response.GetEntity<JSONObject>();
				NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, json.Length());
				JSONObject tasks = json.GetJSONObject("tasks");
				JSONArray arr = tasks.GetJSONArray("task");
				NUnit.Framework.Assert.AreEqual("incorrect number of elements", 2, arr.Length());
				VerifyAMTask(arr, jobsMap[id], null);
			}
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestTasksXML()
		{
			WebResource r = Resource();
			IDictionary<JobId, Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job> jobsMap = appContext
				.GetAllJobs();
			foreach (JobId id in jobsMap.Keys)
			{
				string jobId = MRApps.ToString(id);
				ClientResponse response = r.Path("ws").Path("v1").Path("mapreduce").Path("jobs").
					Path(jobId).Path("tasks").Accept(MediaType.ApplicationXml).Get<ClientResponse>();
				NUnit.Framework.Assert.AreEqual(MediaType.ApplicationXmlType, response.GetType());
				string xml = response.GetEntity<string>();
				DocumentBuilderFactory dbf = DocumentBuilderFactory.NewInstance();
				DocumentBuilder db = dbf.NewDocumentBuilder();
				InputSource @is = new InputSource();
				@is.SetCharacterStream(new StringReader(xml));
				Document dom = db.Parse(@is);
				NodeList tasks = dom.GetElementsByTagName("tasks");
				NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, tasks.GetLength
					());
				NodeList task = dom.GetElementsByTagName("task");
				VerifyAMTaskXML(task, jobsMap[id]);
			}
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestTasksQueryMap()
		{
			WebResource r = Resource();
			IDictionary<JobId, Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job> jobsMap = appContext
				.GetAllJobs();
			foreach (JobId id in jobsMap.Keys)
			{
				string jobId = MRApps.ToString(id);
				string type = "m";
				ClientResponse response = r.Path("ws").Path("v1").Path("mapreduce").Path("jobs").
					Path(jobId).Path("tasks").QueryParam("type", type).Accept(MediaType.ApplicationJson
					).Get<ClientResponse>();
				NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
					);
				JSONObject json = response.GetEntity<JSONObject>();
				NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, json.Length());
				JSONObject tasks = json.GetJSONObject("tasks");
				JSONArray arr = tasks.GetJSONArray("task");
				NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, arr.Length());
				VerifyAMTask(arr, jobsMap[id], type);
			}
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestTasksQueryReduce()
		{
			WebResource r = Resource();
			IDictionary<JobId, Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job> jobsMap = appContext
				.GetAllJobs();
			foreach (JobId id in jobsMap.Keys)
			{
				string jobId = MRApps.ToString(id);
				string type = "r";
				ClientResponse response = r.Path("ws").Path("v1").Path("mapreduce").Path("jobs").
					Path(jobId).Path("tasks").QueryParam("type", type).Accept(MediaType.ApplicationJson
					).Get<ClientResponse>();
				NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
					);
				JSONObject json = response.GetEntity<JSONObject>();
				NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, json.Length());
				JSONObject tasks = json.GetJSONObject("tasks");
				JSONArray arr = tasks.GetJSONArray("task");
				NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, arr.Length());
				VerifyAMTask(arr, jobsMap[id], type);
			}
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestTasksQueryInvalid()
		{
			WebResource r = Resource();
			IDictionary<JobId, Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job> jobsMap = appContext
				.GetAllJobs();
			foreach (JobId id in jobsMap.Keys)
			{
				string jobId = MRApps.ToString(id);
				// tasktype must be exactly either "m" or "r"
				string tasktype = "reduce";
				try
				{
					r.Path("ws").Path("v1").Path("mapreduce").Path("jobs").Path(jobId).Path("tasks").
						QueryParam("type", tasktype).Accept(MediaType.ApplicationJson).Get<JSONObject>();
					NUnit.Framework.Assert.Fail("should have thrown exception on invalid uri");
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
					WebServicesTestUtils.CheckStringMatch("exception message", "java.lang.Exception: tasktype must be either m or r"
						, message);
					WebServicesTestUtils.CheckStringMatch("exception type", "BadRequestException", type
						);
					WebServicesTestUtils.CheckStringMatch("exception classname", "org.apache.hadoop.yarn.webapp.BadRequestException"
						, classname);
				}
			}
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestTaskId()
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
						Path(jobId).Path("tasks").Path(tid).Accept(MediaType.ApplicationJson).Get<ClientResponse
						>();
					NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
						);
					JSONObject json = response.GetEntity<JSONObject>();
					NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, json.Length());
					JSONObject info = json.GetJSONObject("task");
					VerifyAMSingleTask(info, task);
				}
			}
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestTaskIdSlash()
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
						Path(jobId).Path("tasks").Path(tid + "/").Accept(MediaType.ApplicationJson).Get<
						ClientResponse>();
					NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
						);
					JSONObject json = response.GetEntity<JSONObject>();
					NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, json.Length());
					JSONObject info = json.GetJSONObject("task");
					VerifyAMSingleTask(info, task);
				}
			}
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestTaskIdDefault()
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
						Path(jobId).Path("tasks").Path(tid).Get<ClientResponse>();
					NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
						);
					JSONObject json = response.GetEntity<JSONObject>();
					NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, json.Length());
					JSONObject info = json.GetJSONObject("task");
					VerifyAMSingleTask(info, task);
				}
			}
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestTaskIdBogus()
		{
			WebResource r = Resource();
			IDictionary<JobId, Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job> jobsMap = appContext
				.GetAllJobs();
			foreach (JobId id in jobsMap.Keys)
			{
				string jobId = MRApps.ToString(id);
				string tid = "bogustaskid";
				try
				{
					r.Path("ws").Path("v1").Path("mapreduce").Path("jobs").Path(jobId).Path("tasks").
						Path(tid).Get<JSONObject>();
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
					WebServicesTestUtils.CheckStringMatch("exception message", "java.lang.Exception: TaskId string : "
						 + "bogustaskid is not properly formed", message);
					WebServicesTestUtils.CheckStringMatch("exception type", "NotFoundException", type
						);
					WebServicesTestUtils.CheckStringMatch("exception classname", "org.apache.hadoop.yarn.webapp.NotFoundException"
						, classname);
				}
			}
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestTaskIdNonExist()
		{
			WebResource r = Resource();
			IDictionary<JobId, Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job> jobsMap = appContext
				.GetAllJobs();
			foreach (JobId id in jobsMap.Keys)
			{
				string jobId = MRApps.ToString(id);
				string tid = "task_0_0000_m_000000";
				try
				{
					r.Path("ws").Path("v1").Path("mapreduce").Path("jobs").Path(jobId).Path("tasks").
						Path(tid).Get<JSONObject>();
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
					WebServicesTestUtils.CheckStringMatch("exception message", "java.lang.Exception: task not found with id task_0_0000_m_000000"
						, message);
					WebServicesTestUtils.CheckStringMatch("exception type", "NotFoundException", type
						);
					WebServicesTestUtils.CheckStringMatch("exception classname", "org.apache.hadoop.yarn.webapp.NotFoundException"
						, classname);
				}
			}
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestTaskIdInvalid()
		{
			WebResource r = Resource();
			IDictionary<JobId, Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job> jobsMap = appContext
				.GetAllJobs();
			foreach (JobId id in jobsMap.Keys)
			{
				string jobId = MRApps.ToString(id);
				string tid = "task_0_0000_d_000000";
				try
				{
					r.Path("ws").Path("v1").Path("mapreduce").Path("jobs").Path(jobId).Path("tasks").
						Path(tid).Get<JSONObject>();
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
					WebServicesTestUtils.CheckStringMatch("exception message", "java.lang.Exception: Bad TaskType identifier. TaskId string : "
						 + "task_0_0000_d_000000 is not properly formed.", message);
					WebServicesTestUtils.CheckStringMatch("exception type", "NotFoundException", type
						);
					WebServicesTestUtils.CheckStringMatch("exception classname", "org.apache.hadoop.yarn.webapp.NotFoundException"
						, classname);
				}
			}
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestTaskIdInvalid2()
		{
			WebResource r = Resource();
			IDictionary<JobId, Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job> jobsMap = appContext
				.GetAllJobs();
			foreach (JobId id in jobsMap.Keys)
			{
				string jobId = MRApps.ToString(id);
				string tid = "task_0_m_000000";
				try
				{
					r.Path("ws").Path("v1").Path("mapreduce").Path("jobs").Path(jobId).Path("tasks").
						Path(tid).Get<JSONObject>();
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
					WebServicesTestUtils.CheckStringMatch("exception message", "java.lang.Exception: TaskId string : "
						 + "task_0_m_000000 is not properly formed", message);
					WebServicesTestUtils.CheckStringMatch("exception type", "NotFoundException", type
						);
					WebServicesTestUtils.CheckStringMatch("exception classname", "org.apache.hadoop.yarn.webapp.NotFoundException"
						, classname);
				}
			}
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestTaskIdInvalid3()
		{
			WebResource r = Resource();
			IDictionary<JobId, Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job> jobsMap = appContext
				.GetAllJobs();
			foreach (JobId id in jobsMap.Keys)
			{
				string jobId = MRApps.ToString(id);
				string tid = "task_0_0000_m";
				try
				{
					r.Path("ws").Path("v1").Path("mapreduce").Path("jobs").Path(jobId).Path("tasks").
						Path(tid).Get<JSONObject>();
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
					WebServicesTestUtils.CheckStringMatch("exception message", "java.lang.Exception: TaskId string : "
						 + "task_0_0000_m is not properly formed", message);
					WebServicesTestUtils.CheckStringMatch("exception type", "NotFoundException", type
						);
					WebServicesTestUtils.CheckStringMatch("exception classname", "org.apache.hadoop.yarn.webapp.NotFoundException"
						, classname);
				}
			}
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestTaskIdXML()
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
						Path(jobId).Path("tasks").Path(tid).Accept(MediaType.ApplicationXml).Get<ClientResponse
						>();
					NUnit.Framework.Assert.AreEqual(MediaType.ApplicationXmlType, response.GetType());
					string xml = response.GetEntity<string>();
					DocumentBuilderFactory dbf = DocumentBuilderFactory.NewInstance();
					DocumentBuilder db = dbf.NewDocumentBuilder();
					InputSource @is = new InputSource();
					@is.SetCharacterStream(new StringReader(xml));
					Document dom = db.Parse(@is);
					NodeList nodes = dom.GetElementsByTagName("task");
					for (int i = 0; i < nodes.GetLength(); i++)
					{
						Element element = (Element)nodes.Item(i);
						VerifyAMSingleTaskXML(element, task);
					}
				}
			}
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		public virtual void VerifyAMSingleTask(JSONObject info, Task task)
		{
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 9, info.Length());
			VerifyTaskGeneric(task, info.GetString("id"), info.GetString("state"), info.GetString
				("type"), info.GetString("successfulAttempt"), info.GetLong("startTime"), info.GetLong
				("finishTime"), info.GetLong("elapsedTime"), (float)info.GetDouble("progress"), 
				info.GetString("status"));
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		public virtual void VerifyAMTask(JSONArray arr, Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job
			 job, string type)
		{
			foreach (Task task in job.GetTasks().Values)
			{
				TaskId id = task.GetID();
				string tid = MRApps.ToString(id);
				bool found = false;
				if (type != null && task.GetType() == MRApps.TaskType(type))
				{
					for (int i = 0; i < arr.Length(); i++)
					{
						JSONObject info = arr.GetJSONObject(i);
						if (tid.Matches(info.GetString("id")))
						{
							found = true;
							VerifyAMSingleTask(info, task);
						}
					}
					NUnit.Framework.Assert.IsTrue("task with id: " + tid + " not in web service output"
						, found);
				}
			}
		}

		public virtual void VerifyTaskGeneric(Task task, string id, string state, string 
			type, string successfulAttempt, long startTime, long finishTime, long elapsedTime
			, float progress, string status)
		{
			TaskId taskid = task.GetID();
			string tid = MRApps.ToString(taskid);
			TaskReport report = task.GetReport();
			WebServicesTestUtils.CheckStringMatch("id", tid, id);
			WebServicesTestUtils.CheckStringMatch("type", task.GetType().ToString(), type);
			WebServicesTestUtils.CheckStringMatch("state", report.GetTaskState().ToString(), 
				state);
			// not easily checked without duplicating logic, just make sure its here
			NUnit.Framework.Assert.IsNotNull("successfulAttempt null", successfulAttempt);
			NUnit.Framework.Assert.AreEqual("startTime wrong", report.GetStartTime(), startTime
				);
			NUnit.Framework.Assert.AreEqual("finishTime wrong", report.GetFinishTime(), finishTime
				);
			NUnit.Framework.Assert.AreEqual("elapsedTime wrong", finishTime - startTime, elapsedTime
				);
			NUnit.Framework.Assert.AreEqual("progress wrong", report.GetProgress() * 100, progress
				, 1e-3f);
			NUnit.Framework.Assert.AreEqual("status wrong", report.GetStatus(), status);
		}

		public virtual void VerifyAMSingleTaskXML(Element element, Task task)
		{
			VerifyTaskGeneric(task, WebServicesTestUtils.GetXmlString(element, "id"), WebServicesTestUtils
				.GetXmlString(element, "state"), WebServicesTestUtils.GetXmlString(element, "type"
				), WebServicesTestUtils.GetXmlString(element, "successfulAttempt"), WebServicesTestUtils
				.GetXmlLong(element, "startTime"), WebServicesTestUtils.GetXmlLong(element, "finishTime"
				), WebServicesTestUtils.GetXmlLong(element, "elapsedTime"), WebServicesTestUtils
				.GetXmlFloat(element, "progress"), WebServicesTestUtils.GetXmlString(element, "status"
				));
		}

		public virtual void VerifyAMTaskXML(NodeList nodes, Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job
			 job)
		{
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 2, nodes.GetLength
				());
			foreach (Task task in job.GetTasks().Values)
			{
				TaskId id = task.GetID();
				string tid = MRApps.ToString(id);
				bool found = false;
				for (int i = 0; i < nodes.GetLength(); i++)
				{
					Element element = (Element)nodes.Item(i);
					if (tid.Matches(WebServicesTestUtils.GetXmlString(element, "id")))
					{
						found = true;
						VerifyAMSingleTaskXML(element, task);
					}
				}
				NUnit.Framework.Assert.IsTrue("task with id: " + tid + " not in web service output"
					, found);
			}
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestTaskIdCounters()
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
						Path(jobId).Path("tasks").Path(tid).Path("counters").Accept(MediaType.ApplicationJson
						).Get<ClientResponse>();
					NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
						);
					JSONObject json = response.GetEntity<JSONObject>();
					NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, json.Length());
					JSONObject info = json.GetJSONObject("jobTaskCounters");
					VerifyAMJobTaskCounters(info, task);
				}
			}
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestTaskIdCountersSlash()
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
						Path(jobId).Path("tasks").Path(tid).Path("counters/").Accept(MediaType.ApplicationJson
						).Get<ClientResponse>();
					NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
						);
					JSONObject json = response.GetEntity<JSONObject>();
					NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, json.Length());
					JSONObject info = json.GetJSONObject("jobTaskCounters");
					VerifyAMJobTaskCounters(info, task);
				}
			}
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestTaskIdCountersDefault()
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
						Path(jobId).Path("tasks").Path(tid).Path("counters").Get<ClientResponse>();
					NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
						);
					JSONObject json = response.GetEntity<JSONObject>();
					NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, json.Length());
					JSONObject info = json.GetJSONObject("jobTaskCounters");
					VerifyAMJobTaskCounters(info, task);
				}
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestJobTaskCountersXML()
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
						Path(jobId).Path("tasks").Path(tid).Path("counters").Accept(MediaType.ApplicationXml
						).Get<ClientResponse>();
					NUnit.Framework.Assert.AreEqual(MediaType.ApplicationXmlType, response.GetType());
					string xml = response.GetEntity<string>();
					DocumentBuilderFactory dbf = DocumentBuilderFactory.NewInstance();
					DocumentBuilder db = dbf.NewDocumentBuilder();
					InputSource @is = new InputSource();
					@is.SetCharacterStream(new StringReader(xml));
					Document dom = db.Parse(@is);
					NodeList info = dom.GetElementsByTagName("jobTaskCounters");
					VerifyAMTaskCountersXML(info, task);
				}
			}
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		public virtual void VerifyAMJobTaskCounters(JSONObject info, Task task)
		{
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 2, info.Length());
			WebServicesTestUtils.CheckStringMatch("id", MRApps.ToString(task.GetID()), info.GetString
				("id"));
			// just do simple verification of fields - not data is correct
			// in the fields
			JSONArray counterGroups = info.GetJSONArray("taskCounterGroup");
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

		public virtual void VerifyAMTaskCountersXML(NodeList nodes, Task task)
		{
			for (int i = 0; i < nodes.GetLength(); i++)
			{
				Element element = (Element)nodes.Item(i);
				WebServicesTestUtils.CheckStringMatch("id", MRApps.ToString(task.GetID()), WebServicesTestUtils
					.GetXmlString(element, "id"));
				// just do simple verification of fields - not data is correct
				// in the fields
				NodeList groups = element.GetElementsByTagName("taskCounterGroup");
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
