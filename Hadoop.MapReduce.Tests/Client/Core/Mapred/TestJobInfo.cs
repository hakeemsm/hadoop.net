using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapreduce;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	/// <summary>test class JobInfo</summary>
	public class TestJobInfo
	{
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestJobInfo()
		{
			JobID jid = new JobID("001", 1);
			Text user = new Text("User");
			Path path = new Path("/tmp/test");
			JobInfo info = new JobInfo(jid, user, path);
			ByteArrayOutputStream @out = new ByteArrayOutputStream();
			info.Write(new DataOutputStream(@out));
			JobInfo copyinfo = new JobInfo();
			copyinfo.ReadFields(new DataInputStream(new ByteArrayInputStream(@out.ToByteArray
				())));
			NUnit.Framework.Assert.AreEqual(info.GetJobID().ToString(), copyinfo.GetJobID().ToString
				());
			NUnit.Framework.Assert.AreEqual(info.GetJobSubmitDir().GetName(), copyinfo.GetJobSubmitDir
				().GetName());
			NUnit.Framework.Assert.AreEqual(info.GetUser().ToString(), copyinfo.GetUser().ToString
				());
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual void TestTaskID()
		{
			JobID jobid = new JobID("1014873536921", 6);
			TaskID tid = new TaskID(jobid, TaskType.Map, 0);
			TaskID tid1 = TaskID.Downgrade(tid);
			TaskReport treport = new TaskReport(tid1, 0.0f, JobStatus.State.Failed.ToString()
				, null, TIPStatus.Failed, 100, 100, new Counters());
			NUnit.Framework.Assert.AreEqual(treport.GetTaskId(), "task_1014873536921_0006_m_000000"
				);
			NUnit.Framework.Assert.AreEqual(treport.GetTaskID().ToString(), "task_1014873536921_0006_m_000000"
				);
		}
	}
}
