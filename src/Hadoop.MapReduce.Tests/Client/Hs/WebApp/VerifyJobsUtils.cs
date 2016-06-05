using System.Collections.Generic;
using System.Text;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Mapreduce.V2.Util;
using Org.Apache.Hadoop.Yarn.Webapp;
using Org.Codehaus.Jettison.Json;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.HS.Webapp
{
	public class VerifyJobsUtils
	{
		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		public static void VerifyHsJobPartial(JSONObject info, Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job
			 job)
		{
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 12, info.Length()
				);
			// everyone access fields
			VerifyHsJobGeneric(job, info.GetString("id"), info.GetString("user"), info.GetString
				("name"), info.GetString("state"), info.GetString("queue"), info.GetLong("startTime"
				), info.GetLong("finishTime"), info.GetInt("mapsTotal"), info.GetInt("mapsCompleted"
				), info.GetInt("reducesTotal"), info.GetInt("reducesCompleted"));
		}

		/// <exception cref="Org.Codehaus.Jettison.Json.JSONException"/>
		public static void VerifyHsJob(JSONObject info, Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job
			 job)
		{
			NUnit.Framework.Assert.AreEqual("incorrect number of elements", 25, info.Length()
				);
			// everyone access fields
			VerifyHsJobGeneric(job, info.GetString("id"), info.GetString("user"), info.GetString
				("name"), info.GetString("state"), info.GetString("queue"), info.GetLong("startTime"
				), info.GetLong("finishTime"), info.GetInt("mapsTotal"), info.GetInt("mapsCompleted"
				), info.GetInt("reducesTotal"), info.GetInt("reducesCompleted"));
			string diagnostics = string.Empty;
			if (info.Has("diagnostics"))
			{
				diagnostics = info.GetString("diagnostics");
			}
			// restricted access fields - if security and acls set
			VerifyHsJobGenericSecure(job, info.GetBoolean("uberized"), diagnostics, info.GetLong
				("avgMapTime"), info.GetLong("avgReduceTime"), info.GetLong("avgShuffleTime"), info
				.GetLong("avgMergeTime"), info.GetInt("failedReduceAttempts"), info.GetInt("killedReduceAttempts"
				), info.GetInt("successfulReduceAttempts"), info.GetInt("failedMapAttempts"), info
				.GetInt("killedMapAttempts"), info.GetInt("successfulMapAttempts"));
		}

		// acls not being checked since
		// we are using mock job instead of CompletedJob
		public static void VerifyHsJobGeneric(Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job 
			job, string id, string user, string name, string state, string queue, long startTime
			, long finishTime, int mapsTotal, int mapsCompleted, int reducesTotal, int reducesCompleted
			)
		{
			JobReport report = job.GetReport();
			WebServicesTestUtils.CheckStringMatch("id", MRApps.ToString(job.GetID()), id);
			WebServicesTestUtils.CheckStringMatch("user", job.GetUserName().ToString(), user);
			WebServicesTestUtils.CheckStringMatch("name", job.GetName(), name);
			WebServicesTestUtils.CheckStringMatch("state", job.GetState().ToString(), state);
			WebServicesTestUtils.CheckStringMatch("queue", job.GetQueueName(), queue);
			NUnit.Framework.Assert.AreEqual("startTime incorrect", report.GetStartTime(), startTime
				);
			NUnit.Framework.Assert.AreEqual("finishTime incorrect", report.GetFinishTime(), finishTime
				);
			NUnit.Framework.Assert.AreEqual("mapsTotal incorrect", job.GetTotalMaps(), mapsTotal
				);
			NUnit.Framework.Assert.AreEqual("mapsCompleted incorrect", job.GetCompletedMaps()
				, mapsCompleted);
			NUnit.Framework.Assert.AreEqual("reducesTotal incorrect", job.GetTotalReduces(), 
				reducesTotal);
			NUnit.Framework.Assert.AreEqual("reducesCompleted incorrect", job.GetCompletedReduces
				(), reducesCompleted);
		}

		public static void VerifyHsJobGenericSecure(Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job
			 job, bool uberized, string diagnostics, long avgMapTime, long avgReduceTime, long
			 avgShuffleTime, long avgMergeTime, int failedReduceAttempts, int killedReduceAttempts
			, int successfulReduceAttempts, int failedMapAttempts, int killedMapAttempts, int
			 successfulMapAttempts)
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
			NUnit.Framework.Assert.IsTrue("failedReduceAttempts not >= 0", failedReduceAttempts
				 >= 0);
			NUnit.Framework.Assert.IsTrue("killedReduceAttempts not >= 0", killedReduceAttempts
				 >= 0);
			NUnit.Framework.Assert.IsTrue("successfulReduceAttempts not >= 0", successfulReduceAttempts
				 >= 0);
			NUnit.Framework.Assert.IsTrue("failedMapAttempts not >= 0", failedMapAttempts >= 
				0);
			NUnit.Framework.Assert.IsTrue("killedMapAttempts not >= 0", killedMapAttempts >= 
				0);
			NUnit.Framework.Assert.IsTrue("successfulMapAttempts not >= 0", successfulMapAttempts
				 >= 0);
			NUnit.Framework.Assert.IsTrue("avgMapTime not >= 0", avgMapTime >= 0);
			NUnit.Framework.Assert.IsTrue("avgReduceTime not >= 0", avgReduceTime >= 0);
			NUnit.Framework.Assert.IsTrue("avgShuffleTime not >= 0", avgShuffleTime >= 0);
			NUnit.Framework.Assert.IsTrue("avgMergeTime not >= 0", avgMergeTime >= 0);
		}
	}
}
