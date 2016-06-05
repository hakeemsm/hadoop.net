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
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Mapreduce.V2.App;
using Org.Apache.Hadoop.Mapreduce.V2.Util;
using Org.Apache.Hadoop.Security.Authorize;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Util;
using Org.Apache.Hadoop.Yarn.Webapp;
using Org.Codehaus.Jettison.Json;
using Org.W3c.Dom;
using Org.Xml.Sax;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App.Webapp
{
	/// <summary>
	/// Test the app master web service Rest API for getting jobs, a specific job,
	/// and job counters.
	/// </summary>
	/// <remarks>
	/// Test the app master web service Rest API for getting jobs, a specific job,
	/// and job counters.
	/// /ws/v1/mapreduce/jobs
	/// /ws/v1/mapreduce/jobs/{jobid}
	/// /ws/v1/mapreduce/jobs/{jobid}/counters
	/// /ws/v1/mapreduce/jobs/{jobid}/jobattempts
	/// </remarks>
	public class TestAMWebServicesJobs : JerseyTest
	{
		private static Configuration conf = new Configuration();

		private static AppContext appContext;

		private sealed class _ServletModule_85 : ServletModule
		{
			public _ServletModule_85()
			{
			}

			protected override void ConfigureServlets()
			{
				Org.Apache.Hadoop.Mapreduce.V2.App.Webapp.TestAMWebServicesJobs.appContext = new 
					MockAppContext(0, 1, 2, 1);
				this.Bind<JAXBContextResolver>();
				this.Bind<AMWebServices>();
				this.Bind<GenericExceptionHandler>();
				this.Bind<AppContext>().ToInstance(Org.Apache.Hadoop.Mapreduce.V2.App.Webapp.TestAMWebServicesJobs
					.appContext);
				this.Bind<Configuration>().ToInstance(Org.Apache.Hadoop.Mapreduce.V2.App.Webapp.TestAMWebServicesJobs
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

			internal GuiceServletConfig(TestAMWebServicesJobs _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly TestAMWebServicesJobs _enclosing;
		}

		/// <exception cref="System.Exception"/>
		[SetUp]
		public override void SetUp()
		{
			base.SetUp();
		}

		public TestAMWebServicesJobs()
			: base(new WebAppDescriptor.Builder("org.apache.hadoop.mapreduce.v2.app.webapp").
				ContextListenerClass(typeof(TestAMWebServicesJobs.GuiceServletConfig)).FilterClass
				(typeof(GuiceFilter)).ContextPath("jersey-guice-filter").ServletPath("/").Build(
				))
		{
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestJobs()
		{
			WebResource r = Resource();
			ClientResponse response = r.Path("ws").Path("v1").Path("mapreduce").Path("jobs").
				Accept(MediaType.ApplicationJson).Get<ClientResponse>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			JSONObject json = response.GetEntity<JSONObject>();
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, json.Length());
			JSONObject jobs = json.GetJSONObject("jobs");
			JSONArray arr = jobs.GetJSONArray("job");
			JSONObject info = arr.GetJSONObject(0);
			Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job = appContext.GetJob(MRApps.ToJobID
				(info.GetString("id")));
			VerifyAMJob(info, job);
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestJobsSlash()
		{
			WebResource r = Resource();
			ClientResponse response = r.Path("ws").Path("v1").Path("mapreduce").Path("jobs/")
				.Accept(MediaType.ApplicationJson).Get<ClientResponse>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			JSONObject json = response.GetEntity<JSONObject>();
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, json.Length());
			JSONObject jobs = json.GetJSONObject("jobs");
			JSONArray arr = jobs.GetJSONArray("job");
			JSONObject info = arr.GetJSONObject(0);
			Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job = appContext.GetJob(MRApps.ToJobID
				(info.GetString("id")));
			VerifyAMJob(info, job);
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestJobsDefault()
		{
			WebResource r = Resource();
			ClientResponse response = r.Path("ws").Path("v1").Path("mapreduce").Path("jobs").
				Get<ClientResponse>();
			NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
				);
			JSONObject json = response.GetEntity<JSONObject>();
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, json.Length());
			JSONObject jobs = json.GetJSONObject("jobs");
			JSONArray arr = jobs.GetJSONArray("job");
			JSONObject info = arr.GetJSONObject(0);
			Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job = appContext.GetJob(MRApps.ToJobID
				(info.GetString("id")));
			VerifyAMJob(info, job);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestJobsXML()
		{
			WebResource r = Resource();
			ClientResponse response = r.Path("ws").Path("v1").Path("mapreduce").Path("jobs").
				Accept(MediaType.ApplicationXml).Get<ClientResponse>();
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
			VerifyAMJobXML(job, appContext);
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
				ClientResponse response = r.Path("ws").Path("v1").Path("mapreduce").Path("jobs").
					Path(jobId).Accept(MediaType.ApplicationJson).Get<ClientResponse>();
				NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
					);
				JSONObject json = response.GetEntity<JSONObject>();
				NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, json.Length());
				JSONObject info = json.GetJSONObject("job");
				VerifyAMJob(info, jobsMap[id]);
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
				ClientResponse response = r.Path("ws").Path("v1").Path("mapreduce").Path("jobs").
					Path(jobId + "/").Accept(MediaType.ApplicationJson).Get<ClientResponse>();
				NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
					);
				JSONObject json = response.GetEntity<JSONObject>();
				NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, json.Length());
				JSONObject info = json.GetJSONObject("job");
				VerifyAMJob(info, jobsMap[id]);
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
				ClientResponse response = r.Path("ws").Path("v1").Path("mapreduce").Path("jobs").
					Path(jobId).Get<ClientResponse>();
				NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
					);
				JSONObject json = response.GetEntity<JSONObject>();
				NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, json.Length());
				JSONObject info = json.GetJSONObject("job");
				VerifyAMJob(info, jobsMap[id]);
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
				r.Path("ws").Path("v1").Path("mapreduce").Path("jobs").Path("job_0_1234").Get<JSONObject
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
				r.Path("ws").Path("v1").Path("mapreduce").Path("jobs").Path("job_foo").Accept(MediaType
					.ApplicationJson).Get<JSONObject>();
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
				r.Path("ws").Path("v1").Path("mapreduce").Path("jobs").Path("job_foo").Get<JSONObject
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
				r.Path("ws").Path("v1").Path("mapreduce").Path("jobs").Path("job_foo").Accept(MediaType
					.ApplicationXml).Get<JSONObject>();
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
				r.Path("ws").Path("v1").Path("mapreduce").Path("jobs").Path("bogusfoo").Get<JSONObject
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
				WebServicesTestUtils.CheckStringMatch("exception message", "java.lang.Exception: JobId string : bogusfoo is not properly formed"
					, message);
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
				ClientResponse response = r.Path("ws").Path("v1").Path("mapreduce").Path("jobs").
					Path(jobId).Accept(MediaType.ApplicationXml).Get<ClientResponse>();
				NUnit.Framework.Assert.AreEqual(MediaType.ApplicationXmlType, response.GetType());
				string xml = response.GetEntity<string>();
				DocumentBuilderFactory dbf = DocumentBuilderFactory.NewInstance();
				DocumentBuilder db = dbf.NewDocumentBuilder();
				InputSource @is = new InputSource();
				@is.SetCharacterStream(new StringReader(xml));
				Document dom = db.Parse(@is);
				NodeList job = dom.GetElementsByTagName("job");
				VerifyAMJobXML(job, appContext);
			}
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		public virtual void VerifyAMJob(JSONObject info, Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job
			 job)
		{
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 30, info.Length()
				);
			// everyone access fields
			VerifyAMJobGeneric(job, info.GetString("id"), info.GetString("user"), info.GetString
				("name"), info.GetString("state"), info.GetLong("startTime"), info.GetLong("finishTime"
				), info.GetLong("elapsedTime"), info.GetInt("mapsTotal"), info.GetInt("mapsCompleted"
				), info.GetInt("reducesTotal"), info.GetInt("reducesCompleted"), (float)info.GetDouble
				("reduceProgress"), (float)info.GetDouble("mapProgress"));
			string diagnostics = string.Empty;
			if (info.Has("diagnostics"))
			{
				diagnostics = info.GetString("diagnostics");
			}
			// restricted access fields - if security and acls set
			VerifyAMJobGenericSecure(job, info.GetInt("mapsPending"), info.GetInt("mapsRunning"
				), info.GetInt("reducesPending"), info.GetInt("reducesRunning"), info.GetBoolean
				("uberized"), diagnostics, info.GetInt("newReduceAttempts"), info.GetInt("runningReduceAttempts"
				), info.GetInt("failedReduceAttempts"), info.GetInt("killedReduceAttempts"), info
				.GetInt("successfulReduceAttempts"), info.GetInt("newMapAttempts"), info.GetInt(
				"runningMapAttempts"), info.GetInt("failedMapAttempts"), info.GetInt("killedMapAttempts"
				), info.GetInt("successfulMapAttempts"));
			IDictionary<JobACL, AccessControlList> allacls = job.GetJobACLs();
			if (allacls != null)
			{
				foreach (KeyValuePair<JobACL, AccessControlList> entry in allacls)
				{
					string expectName = entry.Key.GetAclName();
					string expectValue = entry.Value.GetAclString();
					bool found = false;
					// make sure ws includes it
					if (info.Has("acls"))
					{
						JSONArray arr = info.GetJSONArray("acls");
						for (int i = 0; i < arr.Length(); i++)
						{
							JSONObject aclInfo = arr.GetJSONObject(i);
							if (expectName.Matches(aclInfo.GetString("name")))
							{
								found = true;
								WebServicesTestUtils.CheckStringMatch("value", expectValue, aclInfo.GetString("value"
									));
							}
						}
					}
					else
					{
						NUnit.Framework.Assert.Fail("should have acls in the web service info");
					}
					NUnit.Framework.Assert.IsTrue("acl: " + expectName + " not found in webservice output"
						, found);
				}
			}
		}

		public virtual void VerifyAMJobXML(NodeList nodes, AppContext appContext)
		{
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, nodes.GetLength
				());
			for (int i = 0; i < nodes.GetLength(); i++)
			{
				Element element = (Element)nodes.Item(i);
				Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job = appContext.GetJob(MRApps.ToJobID
					(WebServicesTestUtils.GetXmlString(element, "id")));
				NUnit.Framework.Assert.IsNotNull("Job not found - output incorrect", job);
				VerifyAMJobGeneric(job, WebServicesTestUtils.GetXmlString(element, "id"), WebServicesTestUtils
					.GetXmlString(element, "user"), WebServicesTestUtils.GetXmlString(element, "name"
					), WebServicesTestUtils.GetXmlString(element, "state"), WebServicesTestUtils.GetXmlLong
					(element, "startTime"), WebServicesTestUtils.GetXmlLong(element, "finishTime"), 
					WebServicesTestUtils.GetXmlLong(element, "elapsedTime"), WebServicesTestUtils.GetXmlInt
					(element, "mapsTotal"), WebServicesTestUtils.GetXmlInt(element, "mapsCompleted")
					, WebServicesTestUtils.GetXmlInt(element, "reducesTotal"), WebServicesTestUtils.
					GetXmlInt(element, "reducesCompleted"), WebServicesTestUtils.GetXmlFloat(element
					, "reduceProgress"), WebServicesTestUtils.GetXmlFloat(element, "mapProgress"));
				// restricted access fields - if security and acls set
				VerifyAMJobGenericSecure(job, WebServicesTestUtils.GetXmlInt(element, "mapsPending"
					), WebServicesTestUtils.GetXmlInt(element, "mapsRunning"), WebServicesTestUtils.
					GetXmlInt(element, "reducesPending"), WebServicesTestUtils.GetXmlInt(element, "reducesRunning"
					), WebServicesTestUtils.GetXmlBoolean(element, "uberized"), WebServicesTestUtils
					.GetXmlString(element, "diagnostics"), WebServicesTestUtils.GetXmlInt(element, "newReduceAttempts"
					), WebServicesTestUtils.GetXmlInt(element, "runningReduceAttempts"), WebServicesTestUtils
					.GetXmlInt(element, "failedReduceAttempts"), WebServicesTestUtils.GetXmlInt(element
					, "killedReduceAttempts"), WebServicesTestUtils.GetXmlInt(element, "successfulReduceAttempts"
					), WebServicesTestUtils.GetXmlInt(element, "newMapAttempts"), WebServicesTestUtils
					.GetXmlInt(element, "runningMapAttempts"), WebServicesTestUtils.GetXmlInt(element
					, "failedMapAttempts"), WebServicesTestUtils.GetXmlInt(element, "killedMapAttempts"
					), WebServicesTestUtils.GetXmlInt(element, "successfulMapAttempts"));
				IDictionary<JobACL, AccessControlList> allacls = job.GetJobACLs();
				if (allacls != null)
				{
					foreach (KeyValuePair<JobACL, AccessControlList> entry in allacls)
					{
						string expectName = entry.Key.GetAclName();
						string expectValue = entry.Value.GetAclString();
						bool found = false;
						// make sure ws includes it
						NodeList id = element.GetElementsByTagName("acls");
						if (id != null)
						{
							for (int j = 0; j < id.GetLength(); j++)
							{
								Element aclElem = (Element)id.Item(j);
								if (aclElem == null)
								{
									NUnit.Framework.Assert.Fail("should have acls in the web service info");
								}
								if (expectName.Matches(WebServicesTestUtils.GetXmlString(aclElem, "name")))
								{
									found = true;
									WebServicesTestUtils.CheckStringMatch("value", expectValue, WebServicesTestUtils.
										GetXmlString(aclElem, "value"));
								}
							}
						}
						else
						{
							NUnit.Framework.Assert.Fail("should have acls in the web service info");
						}
						NUnit.Framework.Assert.IsTrue("acl: " + expectName + " not found in webservice output"
							, found);
					}
				}
			}
		}

		public virtual void VerifyAMJobGeneric(Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job
			 job, string id, string user, string name, string state, long startTime, long finishTime
			, long elapsedTime, int mapsTotal, int mapsCompleted, int reducesTotal, int reducesCompleted
			, float reduceProgress, float mapProgress)
		{
			JobReport report = job.GetReport();
			WebServicesTestUtils.CheckStringMatch("id", MRApps.ToString(job.GetID()), id);
			WebServicesTestUtils.CheckStringMatch("user", job.GetUserName().ToString(), user);
			WebServicesTestUtils.CheckStringMatch("name", job.GetName(), name);
			WebServicesTestUtils.CheckStringMatch("state", job.GetState().ToString(), state);
			NUnit.Framework.Assert.AreEqual("startTime incorrect", report.GetStartTime(), startTime
				);
			NUnit.Framework.Assert.AreEqual("finishTime incorrect", report.GetFinishTime(), finishTime
				);
			NUnit.Framework.Assert.AreEqual("elapsedTime incorrect", Times.Elapsed(report.GetStartTime
				(), report.GetFinishTime()), elapsedTime);
			NUnit.Framework.Assert.AreEqual("mapsTotal incorrect", job.GetTotalMaps(), mapsTotal
				);
			NUnit.Framework.Assert.AreEqual("mapsCompleted incorrect", job.GetCompletedMaps()
				, mapsCompleted);
			NUnit.Framework.Assert.AreEqual("reducesTotal incorrect", job.GetTotalReduces(), 
				reducesTotal);
			NUnit.Framework.Assert.AreEqual("reducesCompleted incorrect", job.GetCompletedReduces
				(), reducesCompleted);
			NUnit.Framework.Assert.AreEqual("mapProgress incorrect", report.GetMapProgress() 
				* 100, mapProgress, 0);
			NUnit.Framework.Assert.AreEqual("reduceProgress incorrect", report.GetReduceProgress
				() * 100, reduceProgress, 0);
		}

		public virtual void VerifyAMJobGenericSecure(Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job
			 job, int mapsPending, int mapsRunning, int reducesPending, int reducesRunning, 
			bool uberized, string diagnostics, int newReduceAttempts, int runningReduceAttempts
			, int failedReduceAttempts, int killedReduceAttempts, int successfulReduceAttempts
			, int newMapAttempts, int runningMapAttempts, int failedMapAttempts, int killedMapAttempts
			, int successfulMapAttempts)
		{
			string diagString = string.Empty;
			IList<string> diagList = job.GetDiagnostics();
			if (diagList != null && !diagList.IsEmpty())
			{
				StringBuilder b = new StringBuilder();
				foreach (string diag in diagList)
				{
					b.Append(diag);
				}
				diagString = b.ToString();
			}
			WebServicesTestUtils.CheckStringMatch("diagnostics", diagString, diagnostics);
			NUnit.Framework.Assert.AreEqual("isUber incorrect", job.IsUber(), uberized);
			// unfortunately the following fields are all calculated in JobInfo
			// so not easily accessible without doing all the calculations again.
			// For now just make sure they are present.
			NUnit.Framework.Assert.IsTrue("mapsPending not >= 0", mapsPending >= 0);
			NUnit.Framework.Assert.IsTrue("mapsRunning not >= 0", mapsRunning >= 0);
			NUnit.Framework.Assert.IsTrue("reducesPending not >= 0", reducesPending >= 0);
			NUnit.Framework.Assert.IsTrue("reducesRunning not >= 0", reducesRunning >= 0);
			NUnit.Framework.Assert.IsTrue("newReduceAttempts not >= 0", newReduceAttempts >= 
				0);
			NUnit.Framework.Assert.IsTrue("runningReduceAttempts not >= 0", runningReduceAttempts
				 >= 0);
			NUnit.Framework.Assert.IsTrue("failedReduceAttempts not >= 0", failedReduceAttempts
				 >= 0);
			NUnit.Framework.Assert.IsTrue("killedReduceAttempts not >= 0", killedReduceAttempts
				 >= 0);
			NUnit.Framework.Assert.IsTrue("successfulReduceAttempts not >= 0", successfulReduceAttempts
				 >= 0);
			NUnit.Framework.Assert.IsTrue("newMapAttempts not >= 0", newMapAttempts >= 0);
			NUnit.Framework.Assert.IsTrue("runningMapAttempts not >= 0", runningMapAttempts >=
				 0);
			NUnit.Framework.Assert.IsTrue("failedMapAttempts not >= 0", failedMapAttempts >= 
				0);
			NUnit.Framework.Assert.IsTrue("killedMapAttempts not >= 0", killedMapAttempts >= 
				0);
			NUnit.Framework.Assert.IsTrue("successfulMapAttempts not >= 0", successfulMapAttempts
				 >= 0);
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
				ClientResponse response = r.Path("ws").Path("v1").Path("mapreduce").Path("jobs").
					Path(jobId).Path("counters").Accept(MediaType.ApplicationJson).Get<ClientResponse
					>();
				NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
					);
				JSONObject json = response.GetEntity<JSONObject>();
				NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, json.Length());
				JSONObject info = json.GetJSONObject("jobCounters");
				VerifyAMJobCounters(info, jobsMap[id]);
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
				ClientResponse response = r.Path("ws").Path("v1").Path("mapreduce").Path("jobs").
					Path(jobId).Path("counters/").Accept(MediaType.ApplicationJson).Get<ClientResponse
					>();
				NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
					);
				JSONObject json = response.GetEntity<JSONObject>();
				NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, json.Length());
				JSONObject info = json.GetJSONObject("jobCounters");
				VerifyAMJobCounters(info, jobsMap[id]);
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
				ClientResponse response = r.Path("ws").Path("v1").Path("mapreduce").Path("jobs").
					Path(jobId).Path("counters/").Get<ClientResponse>();
				NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
					);
				JSONObject json = response.GetEntity<JSONObject>();
				NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, json.Length());
				JSONObject info = json.GetJSONObject("jobCounters");
				VerifyAMJobCounters(info, jobsMap[id]);
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
				ClientResponse response = r.Path("ws").Path("v1").Path("mapreduce").Path("jobs").
					Path(jobId).Path("counters").Accept(MediaType.ApplicationXml).Get<ClientResponse
					>();
				NUnit.Framework.Assert.AreEqual(MediaType.ApplicationXmlType, response.GetType());
				string xml = response.GetEntity<string>();
				DocumentBuilderFactory dbf = DocumentBuilderFactory.NewInstance();
				DocumentBuilder db = dbf.NewDocumentBuilder();
				InputSource @is = new InputSource();
				@is.SetCharacterStream(new StringReader(xml));
				Document dom = db.Parse(@is);
				NodeList info = dom.GetElementsByTagName("jobCounters");
				VerifyAMJobCountersXML(info, jobsMap[id]);
			}
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		public virtual void VerifyAMJobCounters(JSONObject info, Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job
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

		public virtual void VerifyAMJobCountersXML(NodeList nodes, Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job
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
				ClientResponse response = r.Path("ws").Path("v1").Path("mapreduce").Path("jobs").
					Path(jobId).Path("jobattempts").Accept(MediaType.ApplicationJson).Get<ClientResponse
					>();
				NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
					);
				JSONObject json = response.GetEntity<JSONObject>();
				NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, json.Length());
				JSONObject info = json.GetJSONObject("jobAttempts");
				VerifyJobAttempts(info, jobsMap[id]);
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
				ClientResponse response = r.Path("ws").Path("v1").Path("mapreduce").Path("jobs").
					Path(jobId).Path("jobattempts/").Accept(MediaType.ApplicationJson).Get<ClientResponse
					>();
				NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
					);
				JSONObject json = response.GetEntity<JSONObject>();
				NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, json.Length());
				JSONObject info = json.GetJSONObject("jobAttempts");
				VerifyJobAttempts(info, jobsMap[id]);
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
				ClientResponse response = r.Path("ws").Path("v1").Path("mapreduce").Path("jobs").
					Path(jobId).Path("jobattempts").Get<ClientResponse>();
				NUnit.Framework.Assert.AreEqual(MediaType.ApplicationJsonType, response.GetType()
					);
				JSONObject json = response.GetEntity<JSONObject>();
				NUnit.Framework.Assert.AreEqual("incorrect number of elements", 1, json.Length());
				JSONObject info = json.GetJSONObject("jobAttempts");
				VerifyJobAttempts(info, jobsMap[id]);
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
				ClientResponse response = r.Path("ws").Path("v1").Path("mapreduce").Path("jobs").
					Path(jobId).Path("jobattempts").Accept(MediaType.ApplicationXml).Get<ClientResponse
					>();
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
				VerifyJobAttemptsXML(info, jobsMap[id]);
			}
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		public virtual void VerifyJobAttempts(JSONObject info, Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job
			 job)
		{
			JSONArray attempts = info.GetJSONArray("jobAttempt");
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 2, attempts.Length
				());
			for (int i = 0; i < attempts.Length(); i++)
			{
				JSONObject attempt = attempts.GetJSONObject(i);
				VerifyJobAttemptsGeneric(job, attempt.GetString("nodeHttpAddress"), attempt.GetString
					("nodeId"), attempt.GetInt("id"), attempt.GetLong("startTime"), attempt.GetString
					("containerId"), attempt.GetString("logsLink"));
			}
		}

		public virtual void VerifyJobAttemptsXML(NodeList nodes, Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job
			 job)
		{
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 2, nodes.GetLength
				());
			for (int i = 0; i < nodes.GetLength(); i++)
			{
				Element element = (Element)nodes.Item(i);
				VerifyJobAttemptsGeneric(job, WebServicesTestUtils.GetXmlString(element, "nodeHttpAddress"
					), WebServicesTestUtils.GetXmlString(element, "nodeId"), WebServicesTestUtils.GetXmlInt
					(element, "id"), WebServicesTestUtils.GetXmlLong(element, "startTime"), WebServicesTestUtils
					.GetXmlString(element, "containerId"), WebServicesTestUtils.GetXmlString(element
					, "logsLink"));
			}
		}

		public virtual void VerifyJobAttemptsGeneric(Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job
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
					string localLogsLink = StringHelper.Ujoin("node", "containerlogs", containerId, job
						.GetUserName());
					NUnit.Framework.Assert.IsTrue("logsLink", logsLink.Contains(localLogsLink));
				}
			}
			NUnit.Framework.Assert.IsTrue("attempt: " + id + " was not found", attemptFound);
		}
	}
}
