using System.Collections.Generic;
using Com.Google.Inject;
using Com.Google.Inject.Servlet;
using Com.Sun.Jersey.Api.Client;
using Com.Sun.Jersey.Guice.Spi.Container.Servlet;
using Com.Sun.Jersey.Test.Framework;
using Javax.WS.RS.Core;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Mapreduce.V2.App;
using Org.Apache.Hadoop.Mapreduce.V2.HS;
using Org.Apache.Hadoop.Mapreduce.V2.Util;
using Org.Apache.Hadoop.Yarn.Webapp;
using Org.Codehaus.Jettison.Json;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.HS.Webapp
{
	/// <summary>
	/// Test the history server Rest API for getting jobs with various query
	/// parameters.
	/// </summary>
	/// <remarks>
	/// Test the history server Rest API for getting jobs with various query
	/// parameters.
	/// /ws/v1/history/mapreduce/jobs?{query=value}
	/// </remarks>
	public class TestHsWebServicesJobsQuery : JerseyTest
	{
		private static Configuration conf = new Configuration();

		private static MockHistoryContext appContext;

		private static HsWebApp webApp;

		private sealed class _ServletModule_73 : ServletModule
		{
			public _ServletModule_73()
			{
			}

			protected override void ConfigureServlets()
			{
				Org.Apache.Hadoop.Mapreduce.V2.HS.Webapp.TestHsWebServicesJobsQuery.appContext = 
					new MockHistoryContext(3, 2, 1);
				Org.Apache.Hadoop.Mapreduce.V2.HS.Webapp.TestHsWebServicesJobsQuery.webApp = Org.Mockito.Mockito
					.Mock<HsWebApp>();
				Org.Mockito.Mockito.When(Org.Apache.Hadoop.Mapreduce.V2.HS.Webapp.TestHsWebServicesJobsQuery
					.webApp.Name()).ThenReturn("hsmockwebapp");
				this.Bind<JAXBContextResolver>();
				this.Bind<HsWebServices>();
				this.Bind<GenericExceptionHandler>();
				this.Bind<WebApp>().ToInstance(Org.Apache.Hadoop.Mapreduce.V2.HS.Webapp.TestHsWebServicesJobsQuery
					.webApp);
				this.Bind<AppContext>().ToInstance(Org.Apache.Hadoop.Mapreduce.V2.HS.Webapp.TestHsWebServicesJobsQuery
					.appContext);
				this.Bind<HistoryContext>().ToInstance(Org.Apache.Hadoop.Mapreduce.V2.HS.Webapp.TestHsWebServicesJobsQuery
					.appContext);
				this.Bind<Configuration>().ToInstance(Org.Apache.Hadoop.Mapreduce.V2.HS.Webapp.TestHsWebServicesJobsQuery
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

			internal GuiceServletConfig(TestHsWebServicesJobsQuery _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly TestHsWebServicesJobsQuery _enclosing;
		}

		/// <exception cref="System.Exception"/>
		[SetUp]
		public override void SetUp()
		{
			base.SetUp();
		}

		public TestHsWebServicesJobsQuery()
			: base(new WebAppDescriptor.Builder("org.apache.hadoop.mapreduce.v2.hs.webapp").ContextListenerClass
				(typeof(TestHsWebServicesJobsQuery.GuiceServletConfig)).FilterClass(typeof(GuiceFilter
				)).ContextPath("jersey-guice-filter").ServletPath("/").Build())
		{
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestJobsQueryStateNone()
		{
			WebResource r = Resource();
			AList<JobState> JobStates = new AList<JobState>(Arrays.AsList(JobState.Values()));
			// find a state that isn't in use
			IDictionary<JobId, Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job> jobsMap = appContext
				.GetAllJobs();
			foreach (KeyValuePair<JobId, Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job> entry in 
				jobsMap)
			{
				JobStates.Remove(entry.Value.GetState());
			}
			NUnit.Framework.Assert.IsTrue("No unused job states", JobStates.Count > 0);
			JobState notInUse = JobStates[0];
			ClientResponse response = r.Path("ws").Path("v1").Path("history").Path("mapreduce"
				).Path("jobs").QueryParam("state", notInUse.ToString()).Accept(MediaType.ApplicationJson
				).Get<ClientResponse>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			JSONObject json = response.GetEntity<JSONObject>();
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, json.Length());
			NUnit.Framework.Assert.AreEqual("jobs is not null", JSONObject.Null, json.Get("jobs"
				));
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestJobsQueryState()
		{
			WebResource r = Resource();
			// we only create 3 jobs and it cycles through states so we should have 3 unique states
			IDictionary<JobId, Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job> jobsMap = appContext
				.GetAllJobs();
			string queryState = "BOGUS";
			JobId jid = null;
			foreach (KeyValuePair<JobId, Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job> entry in 
				jobsMap)
			{
				jid = entry.Value.GetID();
				queryState = entry.Value.GetState().ToString();
				break;
			}
			ClientResponse response = r.Path("ws").Path("v1").Path("history").Path("mapreduce"
				).Path("jobs").QueryParam("state", queryState).Accept(MediaType.ApplicationJson)
				.Get<ClientResponse>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			JSONObject json = response.GetEntity<JSONObject>();
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, json.Length());
			JSONObject jobs = json.GetJSONObject("jobs");
			JSONArray arr = jobs.GetJSONArray("job");
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, arr.Length());
			JSONObject info = arr.GetJSONObject(0);
			Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job = appContext.GetPartialJob(jid);
			VerifyJobsUtils.VerifyHsJobPartial(info, job);
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestJobsQueryStateInvalid()
		{
			WebResource r = Resource();
			ClientResponse response = r.Path("ws").Path("v1").Path("history").Path("mapreduce"
				).Path("jobs").QueryParam("state", "InvalidState").Accept(MediaType.ApplicationJson
				).Get<ClientResponse>();
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
			WebServicesTestUtils.CheckStringContains("exception message", "org.apache.hadoop.mapreduce.v2.api.records.JobState.InvalidState"
				, message);
			WebServicesTestUtils.CheckStringMatch("exception type", "IllegalArgumentException"
				, type);
			WebServicesTestUtils.CheckStringMatch("exception classname", "java.lang.IllegalArgumentException"
				, classname);
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestJobsQueryUserNone()
		{
			WebResource r = Resource();
			ClientResponse response = r.Path("ws").Path("v1").Path("history").Path("mapreduce"
				).Path("jobs").QueryParam("user", "bogus").Accept(MediaType.ApplicationJson).Get
				<ClientResponse>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			JSONObject json = response.GetEntity<JSONObject>();
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, json.Length());
			NUnit.Framework.Assert.AreEqual("jobs is not null", JSONObject.Null, json.Get("jobs"
				));
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestJobsQueryUser()
		{
			WebResource r = Resource();
			ClientResponse response = r.Path("ws").Path("v1").Path("history").Path("mapreduce"
				).Path("jobs").QueryParam("user", "mock").Accept(MediaType.ApplicationJson).Get<
				ClientResponse>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			JSONObject json = response.GetEntity<JSONObject>();
			System.Console.Out.WriteLine(json.ToString());
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, json.Length());
			JSONObject jobs = json.GetJSONObject("jobs");
			JSONArray arr = jobs.GetJSONArray("job");
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 3, arr.Length());
			// just verify one of them.
			JSONObject info = arr.GetJSONObject(0);
			Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job = appContext.GetPartialJob(MRApps.
				ToJobID(info.GetString("id")));
			VerifyJobsUtils.VerifyHsJobPartial(info, job);
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestJobsQueryLimit()
		{
			WebResource r = Resource();
			ClientResponse response = r.Path("ws").Path("v1").Path("history").Path("mapreduce"
				).Path("jobs").QueryParam("limit", "2").Accept(MediaType.ApplicationJson).Get<ClientResponse
				>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			JSONObject json = response.GetEntity<JSONObject>();
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, json.Length());
			JSONObject jobs = json.GetJSONObject("jobs");
			JSONArray arr = jobs.GetJSONArray("job");
			// make sure we get 2 back
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 2, arr.Length());
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestJobsQueryLimitInvalid()
		{
			WebResource r = Resource();
			ClientResponse response = r.Path("ws").Path("v1").Path("history").Path("mapreduce"
				).Path("jobs").QueryParam("limit", "-1").Accept(MediaType.ApplicationJson).Get<ClientResponse
				>();
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
			WebServicesTestUtils.CheckStringMatch("exception message", "java.lang.Exception: limit value must be greater then 0"
				, message);
			WebServicesTestUtils.CheckStringMatch("exception type", "BadRequestException", type
				);
			WebServicesTestUtils.CheckStringMatch("exception classname", "org.apache.hadoop.yarn.webapp.BadRequestException"
				, classname);
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestJobsQueryQueue()
		{
			WebResource r = Resource();
			ClientResponse response = r.Path("ws").Path("v1").Path("history").Path("mapreduce"
				).Path("jobs").QueryParam("queue", "mockqueue").Accept(MediaType.ApplicationJson
				).Get<ClientResponse>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			JSONObject json = response.GetEntity<JSONObject>();
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, json.Length());
			JSONObject jobs = json.GetJSONObject("jobs");
			JSONArray arr = jobs.GetJSONArray("job");
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 3, arr.Length());
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestJobsQueryQueueNonExist()
		{
			WebResource r = Resource();
			ClientResponse response = r.Path("ws").Path("v1").Path("history").Path("mapreduce"
				).Path("jobs").QueryParam("queue", "bogus").Accept(MediaType.ApplicationJson).Get
				<ClientResponse>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			JSONObject json = response.GetEntity<JSONObject>();
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, json.Length());
			NUnit.Framework.Assert.AreEqual("jobs is not null", JSONObject.Null, json.Get("jobs"
				));
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestJobsQueryStartTimeEnd()
		{
			WebResource r = Resource();
			// the mockJobs start time is the current time - some random amount
			long now = Runtime.CurrentTimeMillis();
			ClientResponse response = r.Path("ws").Path("v1").Path("history").Path("mapreduce"
				).Path("jobs").QueryParam("startedTimeEnd", now.ToString()).Accept(MediaType.ApplicationJson
				).Get<ClientResponse>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			JSONObject json = response.GetEntity<JSONObject>();
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, json.Length());
			JSONObject jobs = json.GetJSONObject("jobs");
			JSONArray arr = jobs.GetJSONArray("job");
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 3, arr.Length());
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestJobsQueryStartTimeBegin()
		{
			WebResource r = Resource();
			// the mockJobs start time is the current time - some random amount
			long now = Runtime.CurrentTimeMillis();
			ClientResponse response = r.Path("ws").Path("v1").Path("history").Path("mapreduce"
				).Path("jobs").QueryParam("startedTimeBegin", now.ToString()).Accept(MediaType.ApplicationJson
				).Get<ClientResponse>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			JSONObject json = response.GetEntity<JSONObject>();
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, json.Length());
			NUnit.Framework.Assert.AreEqual("jobs is not null", JSONObject.Null, json.Get("jobs"
				));
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestJobsQueryStartTimeBeginEnd()
		{
			WebResource r = Resource();
			IDictionary<JobId, Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job> jobsMap = appContext
				.GetAllJobs();
			int size = jobsMap.Count;
			AList<long> startTime = new AList<long>(size);
			// figure out the middle start Time
			foreach (KeyValuePair<JobId, Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job> entry in 
				jobsMap)
			{
				startTime.AddItem(entry.Value.GetReport().GetStartTime());
			}
			startTime.Sort();
			NUnit.Framework.Assert.IsTrue("Error we must have atleast 3 jobs", size >= 3);
			long midStartTime = startTime[size - 2];
			ClientResponse response = r.Path("ws").Path("v1").Path("history").Path("mapreduce"
				).Path("jobs").QueryParam("startedTimeBegin", 40000.ToString()).QueryParam("startedTimeEnd"
				, midStartTime.ToString()).Accept(MediaType.ApplicationJson).Get<ClientResponse>
				();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			JSONObject json = response.GetEntity<JSONObject>();
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, json.Length());
			JSONObject jobs = json.GetJSONObject("jobs");
			JSONArray arr = jobs.GetJSONArray("job");
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", size - 1, arr.Length
				());
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestJobsQueryStartTimeBeginEndInvalid()
		{
			WebResource r = Resource();
			long now = Runtime.CurrentTimeMillis();
			ClientResponse response = r.Path("ws").Path("v1").Path("history").Path("mapreduce"
				).Path("jobs").QueryParam("startedTimeBegin", now.ToString()).QueryParam("startedTimeEnd"
				, 40000.ToString()).Accept(MediaType.ApplicationJson).Get<ClientResponse>();
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
			WebServicesTestUtils.CheckStringMatch("exception message", "java.lang.Exception: startedTimeEnd must be greater than startTimeBegin"
				, message);
			WebServicesTestUtils.CheckStringMatch("exception type", "BadRequestException", type
				);
			WebServicesTestUtils.CheckStringMatch("exception classname", "org.apache.hadoop.yarn.webapp.BadRequestException"
				, classname);
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestJobsQueryStartTimeInvalidformat()
		{
			WebResource r = Resource();
			ClientResponse response = r.Path("ws").Path("v1").Path("history").Path("mapreduce"
				).Path("jobs").QueryParam("startedTimeBegin", "efsd").Accept(MediaType.ApplicationJson
				).Get<ClientResponse>();
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
			WebServicesTestUtils.CheckStringMatch("exception message", "java.lang.Exception: Invalid number format: For input string: \"efsd\""
				, message);
			WebServicesTestUtils.CheckStringMatch("exception type", "BadRequestException", type
				);
			WebServicesTestUtils.CheckStringMatch("exception classname", "org.apache.hadoop.yarn.webapp.BadRequestException"
				, classname);
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestJobsQueryStartTimeEndInvalidformat()
		{
			WebResource r = Resource();
			ClientResponse response = r.Path("ws").Path("v1").Path("history").Path("mapreduce"
				).Path("jobs").QueryParam("startedTimeEnd", "efsd").Accept(MediaType.ApplicationJson
				).Get<ClientResponse>();
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
			WebServicesTestUtils.CheckStringMatch("exception message", "java.lang.Exception: Invalid number format: For input string: \"efsd\""
				, message);
			WebServicesTestUtils.CheckStringMatch("exception type", "BadRequestException", type
				);
			WebServicesTestUtils.CheckStringMatch("exception classname", "org.apache.hadoop.yarn.webapp.BadRequestException"
				, classname);
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestJobsQueryStartTimeNegative()
		{
			WebResource r = Resource();
			ClientResponse response = r.Path("ws").Path("v1").Path("history").Path("mapreduce"
				).Path("jobs").QueryParam("startedTimeBegin", (-1000).ToString()).Accept(MediaType
				.ApplicationJson).Get<ClientResponse>();
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
			WebServicesTestUtils.CheckStringMatch("exception message", "java.lang.Exception: startedTimeBegin must be greater than 0"
				, message);
			WebServicesTestUtils.CheckStringMatch("exception type", "BadRequestException", type
				);
			WebServicesTestUtils.CheckStringMatch("exception classname", "org.apache.hadoop.yarn.webapp.BadRequestException"
				, classname);
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestJobsQueryStartTimeEndNegative()
		{
			WebResource r = Resource();
			ClientResponse response = r.Path("ws").Path("v1").Path("history").Path("mapreduce"
				).Path("jobs").QueryParam("startedTimeEnd", (-1000).ToString()).Accept(MediaType
				.ApplicationJson).Get<ClientResponse>();
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
			WebServicesTestUtils.CheckStringMatch("exception message", "java.lang.Exception: startedTimeEnd must be greater than 0"
				, message);
			WebServicesTestUtils.CheckStringMatch("exception type", "BadRequestException", type
				);
			WebServicesTestUtils.CheckStringMatch("exception classname", "org.apache.hadoop.yarn.webapp.BadRequestException"
				, classname);
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestJobsQueryFinishTimeEndNegative()
		{
			WebResource r = Resource();
			ClientResponse response = r.Path("ws").Path("v1").Path("history").Path("mapreduce"
				).Path("jobs").QueryParam("finishedTimeEnd", (-1000).ToString()).Accept(MediaType
				.ApplicationJson).Get<ClientResponse>();
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
			WebServicesTestUtils.CheckStringMatch("exception message", "java.lang.Exception: finishedTimeEnd must be greater than 0"
				, message);
			WebServicesTestUtils.CheckStringMatch("exception type", "BadRequestException", type
				);
			WebServicesTestUtils.CheckStringMatch("exception classname", "org.apache.hadoop.yarn.webapp.BadRequestException"
				, classname);
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestJobsQueryFinishTimeBeginNegative()
		{
			WebResource r = Resource();
			ClientResponse response = r.Path("ws").Path("v1").Path("history").Path("mapreduce"
				).Path("jobs").QueryParam("finishedTimeBegin", (-1000).ToString()).Accept(MediaType
				.ApplicationJson).Get<ClientResponse>();
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
			WebServicesTestUtils.CheckStringMatch("exception message", "java.lang.Exception: finishedTimeBegin must be greater than 0"
				, message);
			WebServicesTestUtils.CheckStringMatch("exception type", "BadRequestException", type
				);
			WebServicesTestUtils.CheckStringMatch("exception classname", "org.apache.hadoop.yarn.webapp.BadRequestException"
				, classname);
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestJobsQueryFinishTimeBeginEndInvalid()
		{
			WebResource r = Resource();
			long now = Runtime.CurrentTimeMillis();
			ClientResponse response = r.Path("ws").Path("v1").Path("history").Path("mapreduce"
				).Path("jobs").QueryParam("finishedTimeBegin", now.ToString()).QueryParam("finishedTimeEnd"
				, 40000.ToString()).Accept(MediaType.ApplicationJson).Get<ClientResponse>();
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
			WebServicesTestUtils.CheckStringMatch("exception message", "java.lang.Exception: finishedTimeEnd must be greater than finishedTimeBegin"
				, message);
			WebServicesTestUtils.CheckStringMatch("exception type", "BadRequestException", type
				);
			WebServicesTestUtils.CheckStringMatch("exception classname", "org.apache.hadoop.yarn.webapp.BadRequestException"
				, classname);
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestJobsQueryFinishTimeInvalidformat()
		{
			WebResource r = Resource();
			ClientResponse response = r.Path("ws").Path("v1").Path("history").Path("mapreduce"
				).Path("jobs").QueryParam("finishedTimeBegin", "efsd").Accept(MediaType.ApplicationJson
				).Get<ClientResponse>();
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
			WebServicesTestUtils.CheckStringMatch("exception message", "java.lang.Exception: Invalid number format: For input string: \"efsd\""
				, message);
			WebServicesTestUtils.CheckStringMatch("exception type", "BadRequestException", type
				);
			WebServicesTestUtils.CheckStringMatch("exception classname", "org.apache.hadoop.yarn.webapp.BadRequestException"
				, classname);
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestJobsQueryFinishTimeEndInvalidformat()
		{
			WebResource r = Resource();
			ClientResponse response = r.Path("ws").Path("v1").Path("history").Path("mapreduce"
				).Path("jobs").QueryParam("finishedTimeEnd", "efsd").Accept(MediaType.ApplicationJson
				).Get<ClientResponse>();
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
			WebServicesTestUtils.CheckStringMatch("exception message", "java.lang.Exception: Invalid number format: For input string: \"efsd\""
				, message);
			WebServicesTestUtils.CheckStringMatch("exception type", "BadRequestException", type
				);
			WebServicesTestUtils.CheckStringMatch("exception classname", "org.apache.hadoop.yarn.webapp.BadRequestException"
				, classname);
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestJobsQueryFinishTimeBegin()
		{
			WebResource r = Resource();
			// the mockJobs finish time is the current time + some random amount
			long now = Runtime.CurrentTimeMillis();
			ClientResponse response = r.Path("ws").Path("v1").Path("history").Path("mapreduce"
				).Path("jobs").QueryParam("finishedTimeBegin", now.ToString()).Accept(MediaType.
				ApplicationJson).Get<ClientResponse>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			JSONObject json = response.GetEntity<JSONObject>();
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, json.Length());
			JSONObject jobs = json.GetJSONObject("jobs");
			JSONArray arr = jobs.GetJSONArray("job");
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 3, arr.Length());
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestJobsQueryFinishTimeEnd()
		{
			WebResource r = Resource();
			// the mockJobs finish time is the current time + some random amount
			long now = Runtime.CurrentTimeMillis();
			ClientResponse response = r.Path("ws").Path("v1").Path("history").Path("mapreduce"
				).Path("jobs").QueryParam("finishedTimeEnd", now.ToString()).Accept(MediaType.ApplicationJson
				).Get<ClientResponse>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			JSONObject json = response.GetEntity<JSONObject>();
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, json.Length());
			NUnit.Framework.Assert.AreEqual("jobs is not null", JSONObject.Null, json.Get("jobs"
				));
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestJobsQueryFinishTimeBeginEnd()
		{
			WebResource r = Resource();
			IDictionary<JobId, Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job> jobsMap = appContext
				.GetAllJobs();
			int size = jobsMap.Count;
			// figure out the mid end time - we expect atleast 3 jobs
			AList<long> finishTime = new AList<long>(size);
			foreach (KeyValuePair<JobId, Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job> entry in 
				jobsMap)
			{
				finishTime.AddItem(entry.Value.GetReport().GetFinishTime());
			}
			finishTime.Sort();
			NUnit.Framework.Assert.IsTrue("Error we must have atleast 3 jobs", size >= 3);
			long midFinishTime = finishTime[size - 2];
			ClientResponse response = r.Path("ws").Path("v1").Path("history").Path("mapreduce"
				).Path("jobs").QueryParam("finishedTimeBegin", 40000.ToString()).QueryParam("finishedTimeEnd"
				, midFinishTime.ToString()).Accept(MediaType.ApplicationJson).Get<ClientResponse
				>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			JSONObject json = response.GetEntity<JSONObject>();
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, json.Length());
			JSONObject jobs = json.GetJSONObject("jobs");
			JSONArray arr = jobs.GetJSONArray("job");
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", size - 1, arr.Length
				());
		}
	}
}
