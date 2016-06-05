using System;
using System.IO;
using Org.Apache.Hadoop.Mapreduce;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	/// <summary>Test deprecated methods</summary>
	public class TestOldMethodsJobID
	{
		/// <summary>test deprecated methods of TaskID</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestDepricatedMethods()
		{
			JobID jid = new JobID();
			TaskID test = new TaskID(jid, true, 1);
			NUnit.Framework.Assert.AreEqual(test.GetTaskType(), TaskType.Map);
			test = new TaskID(jid, false, 1);
			NUnit.Framework.Assert.AreEqual(test.GetTaskType(), TaskType.Reduce);
			test = new TaskID("001", 1, false, 1);
			NUnit.Framework.Assert.AreEqual(test.GetTaskType(), TaskType.Reduce);
			test = new TaskID("001", 1, true, 1);
			NUnit.Framework.Assert.AreEqual(test.GetTaskType(), TaskType.Map);
			ByteArrayOutputStream @out = new ByteArrayOutputStream();
			test.Write(new DataOutputStream(@out));
			TaskID ti = TaskID.Read(new DataInputStream(new ByteArrayInputStream(@out.ToByteArray
				())));
			NUnit.Framework.Assert.AreEqual(ti.ToString(), test.ToString());
			NUnit.Framework.Assert.AreEqual("task_001_0001_m_000002", TaskID.GetTaskIDsPattern
				("001", 1, true, 2));
			NUnit.Framework.Assert.AreEqual("task_003_0001_m_000004", TaskID.GetTaskIDsPattern
				("003", 1, TaskType.Map, 4));
			NUnit.Framework.Assert.AreEqual("003_0001_m_000004", TaskID.GetTaskIDsPatternWOPrefix
				("003", 1, TaskType.Map, 4).ToString());
		}

		/// <summary>test JobID</summary>
		/// <exception cref="System.IO.IOException"></exception>
		public virtual void TestJobID()
		{
			JobID jid = new JobID("001", 2);
			ByteArrayOutputStream @out = new ByteArrayOutputStream();
			jid.Write(new DataOutputStream(@out));
			NUnit.Framework.Assert.AreEqual(jid, JobID.Read(new DataInputStream(new ByteArrayInputStream
				(@out.ToByteArray()))));
			NUnit.Framework.Assert.AreEqual("job_001_0001", JobID.GetJobIDsPattern("001", 1));
		}

		/// <summary>test deprecated methods of TaskCompletionEvent</summary>
		public virtual void TestTaskCompletionEvent()
		{
			TaskAttemptID taid = new TaskAttemptID("001", 1, TaskType.Reduce, 2, 3);
			TaskCompletionEvent template = new TaskCompletionEvent(12, taid, 13, true, TaskCompletionEvent.Status
				.Succeeded, "httptracker");
			TaskCompletionEvent testEl = TaskCompletionEvent.Downgrade(template);
			testEl.SetTaskAttemptId(taid);
			testEl.SetTaskTrackerHttp("httpTracker");
			testEl.SetTaskId("attempt_001_0001_m_000002_04");
			NUnit.Framework.Assert.AreEqual("attempt_001_0001_m_000002_4", testEl.GetTaskId()
				);
			testEl.SetTaskStatus(TaskCompletionEvent.Status.Obsolete);
			NUnit.Framework.Assert.AreEqual(TaskCompletionEvent.Status.Obsolete.ToString(), testEl
				.GetStatus().ToString());
			testEl.SetTaskRunTime(20);
			NUnit.Framework.Assert.AreEqual(testEl.GetTaskRunTime(), 20);
			testEl.SetEventId(16);
			NUnit.Framework.Assert.AreEqual(testEl.GetEventId(), 16);
		}

		/// <summary>test depricated methods of JobProfile</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestJobProfile()
		{
			JobProfile profile = new JobProfile("user", "job_001_03", "jobFile", "uri", "name"
				);
			NUnit.Framework.Assert.AreEqual("job_001_0003", profile.GetJobId());
			NUnit.Framework.Assert.AreEqual("default", profile.GetQueueName());
			// serialization test
			ByteArrayOutputStream @out = new ByteArrayOutputStream();
			profile.Write(new DataOutputStream(@out));
			JobProfile profile2 = new JobProfile();
			profile2.ReadFields(new DataInputStream(new ByteArrayInputStream(@out.ToByteArray
				())));
			NUnit.Framework.Assert.AreEqual(profile2.name, profile.name);
			NUnit.Framework.Assert.AreEqual(profile2.jobFile, profile.jobFile);
			NUnit.Framework.Assert.AreEqual(profile2.queueName, profile.queueName);
			NUnit.Framework.Assert.AreEqual(profile2.url, profile.url);
			NUnit.Framework.Assert.AreEqual(profile2.user, profile.user);
		}

		/// <summary>test TaskAttemptID</summary>
		public virtual void TestTaskAttemptID()
		{
			TaskAttemptID task = new TaskAttemptID("001", 2, true, 3, 4);
			NUnit.Framework.Assert.AreEqual("attempt_001_0002_m_000003_4", TaskAttemptID.GetTaskAttemptIDsPattern
				("001", 2, true, 3, 4));
			NUnit.Framework.Assert.AreEqual("task_001_0002_m_000003", ((TaskID)task.GetTaskID
				()).ToString());
			NUnit.Framework.Assert.AreEqual("attempt_001_0001_r_000002_3", TaskAttemptID.GetTaskAttemptIDsPattern
				("001", 1, TaskType.Reduce, 2, 3));
			NUnit.Framework.Assert.AreEqual("001_0001_m_000001_2", TaskAttemptID.GetTaskAttemptIDsPatternWOPrefix
				("001", 1, TaskType.Map, 1, 2).ToString());
		}

		/// <summary>test Reporter.NULL</summary>
		public virtual void TestReporter()
		{
			Reporter nullReporter = Reporter.Null;
			NUnit.Framework.Assert.IsNull(nullReporter.GetCounter(null));
			NUnit.Framework.Assert.IsNull(nullReporter.GetCounter("group", "name"));
			// getInputSplit method removed
			try
			{
				NUnit.Framework.Assert.IsNull(nullReporter.GetInputSplit());
			}
			catch (NotSupportedException e)
			{
				NUnit.Framework.Assert.AreEqual("NULL reporter has no input", e.Message);
			}
			NUnit.Framework.Assert.AreEqual(0, nullReporter.GetProgress(), 0.01);
		}
	}
}
