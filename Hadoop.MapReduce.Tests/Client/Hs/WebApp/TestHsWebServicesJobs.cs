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
using Org.Apache.Hadoop.Mapreduce.V2.HS;
using Org.Apache.Hadoop.Mapreduce.V2.Util;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Util;
using Org.Apache.Hadoop.Yarn.Webapp;
using Org.Codehaus.Jettison.Json;
using Org.W3c.Dom;
using Org.Xml.Sax;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.HS.Webapp
{
	/// <summary>
	/// Test the history server Rest API for getting jobs, a specific job, job
	/// counters, and job attempts.
	/// </summary>
	/// <remarks>
	/// Test the history server Rest API for getting jobs, a specific job, job
	/// counters, and job attempts.
	/// /ws/v1/history/mapreduce/jobs /ws/v1/history/mapreduce/jobs/{jobid}
	/// /ws/v1/history/mapreduce/jobs/{jobid}/counters
	/// /ws/v1/history/mapreduce/jobs/{jobid}/jobattempts
	/// </remarks>
	public class TestHsWebServicesJobs : JerseyTest
	{
		private static Configuration conf = new Configuration();

		private static MockHistoryContext appContext;

		private static HsWebApp webApp;

		private sealed class _ServletModule_85 : ServletModule
		{
			public _ServletModule_85()
			{
			}

			protected override void ConfigureServlets()
			{
				Org.Apache.Hadoop.Mapreduce.V2.HS.Webapp.TestHsWebServicesJobs.appContext = new MockHistoryContext
					(0, 1, 2, 1, false);
				Org.Apache.Hadoop.Mapreduce.V2.HS.Webapp.TestHsWebServicesJobs.webApp = Org.Mockito.Mockito
					.Mock<HsWebApp>();
				Org.Mockito.Mockito.When(Org.Apache.Hadoop.Mapreduce.V2.HS.Webapp.TestHsWebServicesJobs
					.webApp.Name()).ThenReturn("hsmockwebapp");
				this.Bind<JAXBContextResolver>();
				this.Bind<HsWebServices>();
				this.Bind<GenericExceptionHandler>();
				this.Bind<WebApp>().ToInstance(Org.Apache.Hadoop.Mapreduce.V2.HS.Webapp.TestHsWebServicesJobs
					.webApp);
				this.Bind<AppContext>().ToInstance(Org.Apache.Hadoop.Mapreduce.V2.HS.Webapp.TestHsWebServicesJobs
					.appContext);
				this.Bind<HistoryContext>().ToInstance(Org.Apache.Hadoop.Mapreduce.V2.HS.Webapp.TestHsWebServicesJobs
					.appContext);
				this.Bind<Configuration>().ToInstance(Org.Apache.Hadoop.Mapreduce.V2.HS.Webapp.TestHsWebServicesJobs
					.conf);
				this.Serve("/*").With(typeof(GuiceContainer));
			}
		}

		private Injector injector = Guice.CreateInjector(new _ServletModule_85());

		public class GuiceServletConfig : GuiceServletContextListener
		{
			protected override Injector GetInjector()
			{
				return this._enclosing.injector;
			}

			internal GuiceServletConfig(TestHsWebServicesJobs _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly TestHsWebServicesJobs _enclosing;
		}

		/// <exception cref="System.Exception"/>
		[SetUp]
		public override void SetUp()
		{
			base.SetUp();
		}

		public TestHsWebServicesJobs()
			: base(new WebAppDescriptor.Builder("org.apache.hadoop.mapreduce.v2.hs.webapp").ContextListenerClass
				(typeof(TestHsWebServicesJobs.GuiceServletConfig)).FilterClass(typeof(GuiceFilter
				)).ContextPath("jersey-guice-filter").ServletPath("/").Build())
		{
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestJobs()
		{
			WebResource r = Resource();
			ClientResponse response = r.Path("ws").Path("v1").Path("history").Path("mapreduce"
				).Path("jobs").Accept(MediaType.ApplicationJson).Get<ClientResponse>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			JSONObject json = response.GetEntity<JSONObject>();
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, json.Length());
			JSONObject jobs = json.GetJSONObject("jobs");
			JSONArray arr = jobs.GetJSONArray("job");
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, arr.Length());
			JSONObject info = arr.GetJSONObject(0);
			Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job = appContext.GetPartialJob(MRApps.
				ToJobID(info.GetString("id")));
			VerifyJobsUtils.VerifyHsJobPartial(info, job);
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestJobsSlash()
		{
			WebResource r = Resource();
			ClientResponse response = r.Path("ws").Path("v1").Path("history").Path("mapreduce"
				).Path("jobs/").Accept(MediaType.ApplicationJson).Get<ClientResponse>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			JSONObject json = response.GetEntity<JSONObject>();
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, json.Length());
			JSONObject jobs = json.GetJSONObject("jobs");
			JSONArray arr = jobs.GetJSONArray("job");
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, arr.Length());
			JSONObject info = arr.GetJSONObject(0);
			Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job = appContext.GetPartialJob(MRApps.
				ToJobID(info.GetString("id")));
			VerifyJobsUtils.VerifyHsJobPartial(info, job);
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestJobsDefault()
		{
			WebResource r = Resource();
			ClientResponse response = r.Path("ws").Path("v1").Path("history").Path("mapreduce"
				).Path("jobs").Get<ClientResponse>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			JSONObject json = response.GetEntity<JSONObject>();
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, json.Length());
			JSONObject jobs = json.GetJSONObject("jobs");
			JSONArray arr = jobs.GetJSONArray("job");
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, arr.Length());
			JSONObject info = arr.GetJSONObject(0);
			Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job = appContext.GetPartialJob(MRApps.
				ToJobID(info.GetString("id")));
			VerifyJobsUtils.VerifyHsJobPartial(info, job);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestJobsXML()
		{
			WebResource r = Resource();
			ClientResponse response = r.Path("ws").Path("v1").Path("history").Path("mapreduce"
				).Path("jobs").Accept(MediaType.ApplicationXml).Get<ClientResponse>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationXmlType, response.GetType());
			string xml = response.GetEntity<string>();
			DocumentBuilderFactory dbf = DocumentBuilderFactory.NewInstance();
			DocumentBuilder db = dbf.NewDocumentBuilder();
			InputSource @is = new InputSource();
			@is.SetCharacterStream(new StringReader(xml));
			Document dom = db.Parse(@is);
			NodeList jobs = dom.GetElementsByTagName("jobs");
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, jobs.GetLength
				());
			NodeList job = dom.GetElementsByTagName("job");
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, job.GetLength(
				));
			VerifyHsJobPartialXML(job, appContext);
		}

		public virtual void VerifyHsJobPartialXML(NodeList nodes, MockHistoryContext appContext
			)
		{
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, nodes.GetLength
				());
			for (int i = 0; i < nodes.GetLength(); i++)
			{
				Element element = (Element)nodes.Item(i);
				Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job = appContext.GetPartialJob(MRApps.
					ToJobID(WebServicesTestUtils.GetXmlString(element, "id")));
				NUnit.Framework.Assert.IsNotNull("Job not found - output incorrect", job);
				VerifyJobsUtils.VerifyHsJobGeneric(job, WebServicesTestUtils.GetXmlString(element
					, "id"), WebServicesTestUtils.GetXmlString(element, "user"), WebServicesTestUtils
					.GetXmlString(element, "name"), WebServicesTestUtils.GetXmlString(element, "state"
					), WebServicesTestUtils.GetXmlString(element, "queue"), WebServicesTestUtils.GetXmlLong
					(element, "startTime"), WebServicesTestUtils.GetXmlLong(element, "finishTime"), 
					WebServicesTestUtils.GetXmlInt(element, "mapsTotal"), WebServicesTestUtils.GetXmlInt
					(element, "mapsCompleted"), WebServicesTestUtils.GetXmlInt(element, "reducesTotal"
					), WebServicesTestUtils.GetXmlInt(element, "reducesCompleted"));
			}
		}

		public virtual void VerifyHsJobXML(NodeList nodes, AppContext appContext)
		{
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, nodes.GetLength
				());
			for (int i = 0; i < nodes.GetLength(); i++)
			{
				Element element = (Element)nodes.Item(i);
				Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job = appContext.GetJob(MRApps.ToJobID
					(WebServicesTestUtils.GetXmlString(element, "id")));
				NUnit.Framework.Assert.IsNotNull("Job not found - output incorrect", job);
				VerifyJobsUtils.VerifyHsJobGeneric(job, WebServicesTestUtils.GetXmlString(element
					, "id"), WebServicesTestUtils.GetXmlString(element, "user"), WebServicesTestUtils
					.GetXmlString(element, "name"), WebServicesTestUtils.GetXmlString(element, "state"
					), WebServicesTestUtils.GetXmlString(element, "queue"), WebServicesTestUtils.GetXmlLong
					(element, "startTime"), WebServicesTestUtils.GetXmlLong(element, "finishTime"), 
					WebServicesTestUtils.GetXmlInt(element, "mapsTotal"), WebServicesTestUtils.GetXmlInt
					(element, "mapsCompleted"), WebServicesTestUtils.GetXmlInt(element, "reducesTotal"
					), WebServicesTestUtils.GetXmlInt(element, "reducesCompleted"));
				// restricted access fields - if security and acls set
				VerifyJobsUtils.VerifyHsJobGenericSecure(job, WebServicesTestUtils.GetXmlBoolean(
					element, "uberized"), WebServicesTestUtils.GetXmlString(element, "diagnostics"), 
					WebServicesTestUtils.GetXmlLong(element, "avgMapTime"), WebServicesTestUtils.GetXmlLong
					(element, "avgReduceTime"), WebServicesTestUtils.GetXmlLong(element, "avgShuffleTime"
					), WebServicesTestUtils.GetXmlLong(element, "avgMergeTime"), WebServicesTestUtils
					.GetXmlInt(element, "failedReduceAttempts"), WebServicesTestUtils.GetXmlInt(element
					, "killedReduceAttempts"), WebServicesTestUtils.GetXmlInt(element, "successfulReduceAttempts"
					), WebServicesTestUtils.GetXmlInt(element, "failedMapAttempts"), WebServicesTestUtils
					.GetXmlInt(element, "killedMapAttempts"), WebServicesTestUtils.GetXmlInt(element
					, "successfulMapAttempts"));
			}
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestJobId()
		{
			WebResource r = Resource();
			IDictionary<JobId, Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job> jobsMap = appContext
				.GetAllJobs();
			foreach (JobId id in jobsMap.Keys)
			{
				string jobId = MRApps.ToString(id);
				ClientResponse response = r.Path("ws").Path("v1").Path("history").Path("mapreduce"
					).Path("jobs").Path(jobId).Accept(MediaType.ApplicationJson).Get<ClientResponse>
					();
				NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
					);
				JSONObject json = response.GetEntity<JSONObject>();
				NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, json.Length());
				JSONObject info = json.GetJSONObject("job");
				VerifyJobsUtils.VerifyHsJob(info, appContext.GetJob(id));
			}
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestJobIdSlash()
		{
			WebResource r = Resource();
			IDictionary<JobId, Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job> jobsMap = appContext
				.GetAllJobs();
			foreach (JobId id in jobsMap.Keys)
			{
				string jobId = MRApps.ToString(id);
				ClientResponse response = r.Path("ws").Path("v1").Path("history").Path("mapreduce"
					).Path("jobs").Path(jobId + "/").Accept(MediaType.ApplicationJson).Get<ClientResponse
					>();
				NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
					);
				JSONObject json = response.GetEntity<JSONObject>();
				NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, json.Length());
				JSONObject info = json.GetJSONObject("job");
				VerifyJobsUtils.VerifyHsJob(info, appContext.GetJob(id));
			}
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestJobIdDefault()
		{
			WebResource r = Resource();
			IDictionary<JobId, Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job> jobsMap = appContext
				.GetAllJobs();
			foreach (JobId id in jobsMap.Keys)
			{
				string jobId = MRApps.ToString(id);
				ClientResponse response = r.Path("ws").Path("v1").Path("history").Path("mapreduce"
					).Path("jobs").Path(jobId).Get<ClientResponse>();
				NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
					);
				JSONObject json = response.GetEntity<JSONObject>();
				NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, json.Length());
				JSONObject info = json.GetJSONObject("job");
				VerifyJobsUtils.VerifyHsJob(info, appContext.GetJob(id));
			}
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestJobIdNonExist()
		{
			WebResource r = Resource();
			try
			{
				r.Path("ws").Path("v1").Path("history").Path("mapreduce").Path("jobs").Path("job_0_1234"
					).Get<JSONObject>();
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
				WebServicesTestUtils.CheckStringMatch("exception message", "java.lang.Exception: job, job_0_1234, is not found"
					, message);
				WebServicesTestUtils.CheckStringMatch("exception type", "NotFoundException", type
					);
				WebServicesTestUtils.CheckStringMatch("exception classname", "org.apache.hadoop.yarn.webapp.NotFoundException"
					, classname);
			}
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestJobIdInvalid()
		{
			WebResource r = Resource();
			try
			{
				r.Path("ws").Path("v1").Path("history").Path("mapreduce").Path("jobs").Path("job_foo"
					).Accept(MediaType.ApplicationJson).Get<JSONObject>();
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
				VerifyJobIdInvalid(message, type, classname);
			}
		}

		// verify the exception output default is JSON
		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestJobIdInvalidDefault()
		{
			WebResource r = Resource();
			try
			{
				r.Path("ws").Path("v1").Path("history").Path("mapreduce").Path("jobs").Path("job_foo"
					).Get<JSONObject>();
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
				VerifyJobIdInvalid(message, type, classname);
			}
		}

		// test that the exception output works in XML
		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestJobIdInvalidXML()
		{
			WebResource r = Resource();
			try
			{
				r.Path("ws").Path("v1").Path("history").Path("mapreduce").Path("jobs").Path("job_foo"
					).Accept(MediaType.ApplicationXml).Get<JSONObject>();
				NUnit.Framework.Assert.Fail("should have thrown exception on invalid uri");
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
				VerifyJobIdInvalid(message, type, classname);
			}
		}

		private void VerifyJobIdInvalid(string message, string type, string classname)
		{
			WebServicesTestUtils.CheckStringMatch("exception message", "java.lang.Exception: JobId string : job_foo is not properly formed"
				, message);
			WebServicesTestUtils.CheckStringMatch("exception type", "NotFoundException", type
				);
			WebServicesTestUtils.CheckStringMatch("exception classname", "org.apache.hadoop.yarn.webapp.NotFoundException"
				, classname);
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestJobIdInvalidBogus()
		{
			WebResource r = Resource();
			try
			{
				r.Path("ws").Path("v1").Path("history").Path("mapreduce").Path("jobs").Path("bogusfoo"
					).Get<JSONObject>();
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
				WebServicesTestUtils.CheckStringMatch("exception message", "java.lang.Exception: JobId string : "
					 + "bogusfoo is not properly formed", message);
				WebServicesTestUtils.CheckStringMatch("exception type", "NotFoundException", type
					);
				WebServicesTestUtils.CheckStringMatch("exception classname", "org.apache.hadoop.yarn.webapp.NotFoundException"
					, classname);
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestJobIdXML()
		{
			WebResource r = Resource();
			IDictionary<JobId, Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job> jobsMap = appContext
				.GetAllJobs();
			foreach (JobId id in jobsMap.Keys)
			{
				string jobId = MRApps.ToString(id);
				ClientResponse response = r.Path("ws").Path("v1").Path("history").Path("mapreduce"
					).Path("jobs").Path(jobId).Accept(MediaType.ApplicationXml).Get<ClientResponse>(
					);
				NUnit.Framework.Assert.AreEqual(MediaType.ApplicationXmlType, response.GetType());
				string xml = response.GetEntity<string>();
				DocumentBuilderFactory dbf = DocumentBuilderFactory.NewInstance();
				DocumentBuilder db = dbf.NewDocumentBuilder();
				InputSource @is = new InputSource();
				@is.SetCharacterStream(new StringReader(xml));
				Document dom = db.Parse(@is);
				NodeList job = dom.GetElementsByTagName("job");
				VerifyHsJobXML(job, appContext);
			}
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestJobCounters()
		{
			WebResource r = Resource();
			IDictionary<JobId, Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job> jobsMap = appContext
				.GetAllJobs();
			foreach (JobId id in jobsMap.Keys)
			{
				string jobId = MRApps.ToString(id);
				ClientResponse response = r.Path("ws").Path("v1").Path("history").Path("mapreduce"
					).Path("jobs").Path(jobId).Path("counters").Accept(MediaType.ApplicationJson).Get
					<ClientResponse>();
				NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
					);
				JSONObject json = response.GetEntity<JSONObject>();
				NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, json.Length());
				JSONObject info = json.GetJSONObject("jobCounters");
				VerifyHsJobCounters(info, appContext.GetJob(id));
			}
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestJobCountersSlash()
		{
			WebResource r = Resource();
			IDictionary<JobId, Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job> jobsMap = appContext
				.GetAllJobs();
			foreach (JobId id in jobsMap.Keys)
			{
				string jobId = MRApps.ToString(id);
				ClientResponse response = r.Path("ws").Path("v1").Path("history").Path("mapreduce"
					).Path("jobs").Path(jobId).Path("counters/").Accept(MediaType.ApplicationJson).Get
					<ClientResponse>();
				NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
					);
				JSONObject json = response.GetEntity<JSONObject>();
				NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, json.Length());
				JSONObject info = json.GetJSONObject("jobCounters");
				VerifyHsJobCounters(info, appContext.GetJob(id));
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestJobCountersForKilledJob()
		{
			WebResource r = Resource();
			appContext = new MockHistoryContext(0, 1, 1, 1, true);
			injector = Com.Google.Inject.Guice.CreateInjector(new _ServletModule_530());
			IDictionary<JobId, Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job> jobsMap = appContext
				.GetAllJobs();
			foreach (JobId id in jobsMap.Keys)
			{
				string jobId = MRApps.ToString(id);
				ClientResponse response = r.Path("ws").Path("v1").Path("history").Path("mapreduce"
					).Path("jobs").Path(jobId).Path("counters/").Accept(MediaType.ApplicationJson).Get
					<ClientResponse>();
				NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
					);
				JSONObject json = response.GetEntity<JSONObject>();
				NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, json.Length());
				JSONObject info = json.GetJSONObject("jobCounters");
				WebServicesTestUtils.CheckStringMatch("id", MRApps.ToString(id), info.GetString("id"
					));
				NUnit.Framework.Assert.IsTrue("Job shouldn't contain any counters", info.Length()
					 == 1);
			}
		}

		private sealed class _ServletModule_530 : ServletModule
		{
			public _ServletModule_530()
			{
			}

			protected override void ConfigureServlets()
			{
				TestHsWebServicesJobs.webApp = Org.Mockito.Mockito.Mock<HsWebApp>();
				Org.Mockito.Mockito.When(TestHsWebServicesJobs.webApp.Name()).ThenReturn("hsmockwebapp"
					);
				this.Bind<JAXBContextResolver>();
				this.Bind<HsWebServices>();
				this.Bind<GenericExceptionHandler>();
				this.Bind<WebApp>().ToInstance(TestHsWebServicesJobs.webApp);
				this.Bind<AppContext>().ToInstance(TestHsWebServicesJobs.appContext);
				this.Bind<HistoryContext>().ToInstance(TestHsWebServicesJobs.appContext);
				this.Bind<Configuration>().ToInstance(TestHsWebServicesJobs.conf);
				this.Serve("/*").With(typeof(GuiceContainer));
			}
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestJobCountersDefault()
		{
			WebResource r = Resource();
			IDictionary<JobId, Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job> jobsMap = appContext
				.GetAllJobs();
			foreach (JobId id in jobsMap.Keys)
			{
				string jobId = MRApps.ToString(id);
				ClientResponse response = r.Path("ws").Path("v1").Path("history").Path("mapreduce"
					).Path("jobs").Path(jobId).Path("counters/").Get<ClientResponse>();
				NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
					);
				JSONObject json = response.GetEntity<JSONObject>();
				NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, json.Length());
				JSONObject info = json.GetJSONObject("jobCounters");
				VerifyHsJobCounters(info, appContext.GetJob(id));
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestJobCountersXML()
		{
			WebResource r = Resource();
			IDictionary<JobId, Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job> jobsMap = appContext
				.GetAllJobs();
			foreach (JobId id in jobsMap.Keys)
			{
				string jobId = MRApps.ToString(id);
				ClientResponse response = r.Path("ws").Path("v1").Path("history").Path("mapreduce"
					).Path("jobs").Path(jobId).Path("counters").Accept(MediaType.ApplicationXml).Get
					<ClientResponse>();
				NUnit.Framework.Assert.AreEqual(MediaType.ApplicationXmlType, response.GetType());
				string xml = response.GetEntity<string>();
				DocumentBuilderFactory dbf = DocumentBuilderFactory.NewInstance();
				DocumentBuilder db = dbf.NewDocumentBuilder();
				InputSource @is = new InputSource();
				@is.SetCharacterStream(new StringReader(xml));
				Document dom = db.Parse(@is);
				NodeList info = dom.GetElementsByTagName("jobCounters");
				VerifyHsJobCountersXML(info, appContext.GetJob(id));
			}
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		public virtual void VerifyHsJobCounters(JSONObject info, Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job
			 job)
		{
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 2, info.Length());
			WebServicesTestUtils.CheckStringMatch("id", MRApps.ToString(job.GetID()), info.GetString
				("id"));
			// just do simple verification of fields - not data is correct
			// in the fields
			JSONArray counterGroups = info.GetJSONArray("counterGroup");
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
					NUnit.Framework.Assert.IsTrue("counter name not set", (counterName != null && !counterName
						.IsEmpty()));
					long mapValue = counter.GetLong("mapCounterValue");
					NUnit.Framework.Assert.IsTrue("mapCounterValue  >= 0", mapValue >= 0);
					long reduceValue = counter.GetLong("reduceCounterValue");
					NUnit.Framework.Assert.IsTrue("reduceCounterValue  >= 0", reduceValue >= 0);
					long totalValue = counter.GetLong("totalCounterValue");
					NUnit.Framework.Assert.IsTrue("totalCounterValue  >= 0", totalValue >= 0);
				}
			}
		}

		public virtual void VerifyHsJobCountersXML(NodeList nodes, Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job
			 job)
		{
			for (int i = 0; i < nodes.GetLength(); i++)
			{
				Element element = (Element)nodes.Item(i);
				NUnit.Framework.Assert.IsNotNull("Job not found - output incorrect", job);
				WebServicesTestUtils.CheckStringMatch("id", MRApps.ToString(job.GetID()), WebServicesTestUtils
					.GetXmlString(element, "id"));
				// just do simple verification of fields - not data is correct
				// in the fields
				NodeList groups = element.GetElementsByTagName("counterGroup");
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
						long mapValue = WebServicesTestUtils.GetXmlLong(counter, "mapCounterValue");
						NUnit.Framework.Assert.IsTrue("mapCounterValue not >= 0", mapValue >= 0);
						long reduceValue = WebServicesTestUtils.GetXmlLong(counter, "reduceCounterValue");
						NUnit.Framework.Assert.IsTrue("reduceCounterValue  >= 0", reduceValue >= 0);
						long totalValue = WebServicesTestUtils.GetXmlLong(counter, "totalCounterValue");
						NUnit.Framework.Assert.IsTrue("totalCounterValue  >= 0", totalValue >= 0);
					}
				}
			}
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestJobAttempts()
		{
			WebResource r = Resource();
			IDictionary<JobId, Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job> jobsMap = appContext
				.GetAllJobs();
			foreach (JobId id in jobsMap.Keys)
			{
				string jobId = MRApps.ToString(id);
				ClientResponse response = r.Path("ws").Path("v1").Path("history").Path("mapreduce"
					).Path("jobs").Path(jobId).Path("jobattempts").Accept(MediaType.ApplicationJson)
					.Get<ClientResponse>();
				NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
					);
				JSONObject json = response.GetEntity<JSONObject>();
				NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, json.Length());
				JSONObject info = json.GetJSONObject("jobAttempts");
				VerifyHsJobAttempts(info, appContext.GetJob(id));
			}
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestJobAttemptsSlash()
		{
			WebResource r = Resource();
			IDictionary<JobId, Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job> jobsMap = appContext
				.GetAllJobs();
			foreach (JobId id in jobsMap.Keys)
			{
				string jobId = MRApps.ToString(id);
				ClientResponse response = r.Path("ws").Path("v1").Path("history").Path("mapreduce"
					).Path("jobs").Path(jobId).Path("jobattempts/").Accept(MediaType.ApplicationJson
					).Get<ClientResponse>();
				NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
					);
				JSONObject json = response.GetEntity<JSONObject>();
				NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, json.Length());
				JSONObject info = json.GetJSONObject("jobAttempts");
				VerifyHsJobAttempts(info, appContext.GetJob(id));
			}
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestJobAttemptsDefault()
		{
			WebResource r = Resource();
			IDictionary<JobId, Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job> jobsMap = appContext
				.GetAllJobs();
			foreach (JobId id in jobsMap.Keys)
			{
				string jobId = MRApps.ToString(id);
				ClientResponse response = r.Path("ws").Path("v1").Path("history").Path("mapreduce"
					).Path("jobs").Path(jobId).Path("jobattempts").Get<ClientResponse>();
				NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
					);
				JSONObject json = response.GetEntity<JSONObject>();
				NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, json.Length());
				JSONObject info = json.GetJSONObject("jobAttempts");
				VerifyHsJobAttempts(info, appContext.GetJob(id));
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestJobAttemptsXML()
		{
			WebResource r = Resource();
			IDictionary<JobId, Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job> jobsMap = appContext
				.GetAllJobs();
			foreach (JobId id in jobsMap.Keys)
			{
				string jobId = MRApps.ToString(id);
				ClientResponse response = r.Path("ws").Path("v1").Path("history").Path("mapreduce"
					).Path("jobs").Path(jobId).Path("jobattempts").Accept(MediaType.ApplicationXml).
					Get<ClientResponse>();
				NUnit.Framework.Assert.AreEqual(MediaType.ApplicationXmlType, response.GetType());
				string xml = response.GetEntity<string>();
				DocumentBuilderFactory dbf = DocumentBuilderFactory.NewInstance();
				DocumentBuilder db = dbf.NewDocumentBuilder();
				InputSource @is = new InputSource();
				@is.SetCharacterStream(new StringReader(xml));
				Document dom = db.Parse(@is);
				NodeList attempts = dom.GetElementsByTagName("jobAttempts");
				NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, attempts.GetLength
					());
				NodeList info = dom.GetElementsByTagName("jobAttempt");
				VerifyHsJobAttemptsXML(info, appContext.GetJob(id));
			}
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		public virtual void VerifyHsJobAttempts(JSONObject info, Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job
			 job)
		{
			JSONArray attempts = info.GetJSONArray("jobAttempt");
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 2, attempts.Length
				());
			for (int i = 0; i < attempts.Length(); i++)
			{
				JSONObject attempt = attempts.GetJSONObject(i);
				VerifyHsJobAttemptsGeneric(job, attempt.GetString("nodeHttpAddress"), attempt.GetString
					("nodeId"), attempt.GetInt("id"), attempt.GetLong("startTime"), attempt.GetString
					("containerId"), attempt.GetString("logsLink"));
			}
		}

		public virtual void VerifyHsJobAttemptsXML(NodeList nodes, Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job
			 job)
		{
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 2, nodes.GetLength
				());
			for (int i = 0; i < nodes.GetLength(); i++)
			{
				Element element = (Element)nodes.Item(i);
				VerifyHsJobAttemptsGeneric(job, WebServicesTestUtils.GetXmlString(element, "nodeHttpAddress"
					), WebServicesTestUtils.GetXmlString(element, "nodeId"), WebServicesTestUtils.GetXmlInt
					(element, "id"), WebServicesTestUtils.GetXmlLong(element, "startTime"), WebServicesTestUtils
					.GetXmlString(element, "containerId"), WebServicesTestUtils.GetXmlString(element
					, "logsLink"));
			}
		}

		public virtual void VerifyHsJobAttemptsGeneric(Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job
			 job, string nodeHttpAddress, string nodeId, int id, long startTime, string containerId
			, string logsLink)
		{
			bool attemptFound = false;
			foreach (AMInfo amInfo in job.GetAMInfos())
			{
				if (amInfo.GetAppAttemptId().GetAttemptId() == id)
				{
					attemptFound = true;
					string nmHost = amInfo.GetNodeManagerHost();
					int nmHttpPort = amInfo.GetNodeManagerHttpPort();
					int nmPort = amInfo.GetNodeManagerPort();
					WebServicesTestUtils.CheckStringMatch("nodeHttpAddress", nmHost + ":" + nmHttpPort
						, nodeHttpAddress);
					WebServicesTestUtils.CheckStringMatch("nodeId", NodeId.NewInstance(nmHost, nmPort
						).ToString(), nodeId);
					NUnit.Framework.Assert.IsTrue("startime not greater than 0", startTime > 0);
					WebServicesTestUtils.CheckStringMatch("containerId", amInfo.GetContainerId().ToString
						(), containerId);
					string localLogsLink = StringHelper.Join("hsmockwebapp", StringHelper.Ujoin("logs"
						, nodeId, containerId, MRApps.ToString(job.GetID()), job.GetUserName()));
					NUnit.Framework.Assert.IsTrue("logsLink", logsLink.Contains(localLogsLink));
				}
			}
			NUnit.Framework.Assert.IsTrue("attempt: " + id + " was not found", attemptFound);
		}
	}
}
