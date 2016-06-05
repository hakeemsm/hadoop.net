using Org.Apache.Hadoop.Mapreduce.V2.Util;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.Api.Records
{
	public class TestIds
	{
		[NUnit.Framework.Test]
		public virtual void TestJobId()
		{
			long ts1 = 1315890136000l;
			long ts2 = 1315890136001l;
			JobId j1 = CreateJobId(ts1, 2);
			JobId j2 = CreateJobId(ts1, 1);
			JobId j3 = CreateJobId(ts2, 1);
			JobId j4 = CreateJobId(ts1, 2);
			NUnit.Framework.Assert.IsTrue(j1.Equals(j4));
			NUnit.Framework.Assert.IsFalse(j1.Equals(j2));
			NUnit.Framework.Assert.IsFalse(j1.Equals(j3));
			NUnit.Framework.Assert.IsTrue(j1.CompareTo(j4) == 0);
			NUnit.Framework.Assert.IsTrue(j1.CompareTo(j2) > 0);
			NUnit.Framework.Assert.IsTrue(j1.CompareTo(j3) < 0);
			NUnit.Framework.Assert.IsTrue(j1.GetHashCode() == j4.GetHashCode());
			NUnit.Framework.Assert.IsFalse(j1.GetHashCode() == j2.GetHashCode());
			NUnit.Framework.Assert.IsFalse(j1.GetHashCode() == j3.GetHashCode());
			JobId j5 = CreateJobId(ts1, 231415);
			NUnit.Framework.Assert.AreEqual("job_" + ts1 + "_0002", j1.ToString());
			NUnit.Framework.Assert.AreEqual("job_" + ts1 + "_231415", j5.ToString());
		}

		[NUnit.Framework.Test]
		public virtual void TestTaskId()
		{
			long ts1 = 1315890136000l;
			long ts2 = 1315890136001l;
			TaskId t1 = CreateTaskId(ts1, 1, 2, TaskType.Map);
			TaskId t2 = CreateTaskId(ts1, 1, 2, TaskType.Reduce);
			TaskId t3 = CreateTaskId(ts1, 1, 1, TaskType.Map);
			TaskId t4 = CreateTaskId(ts1, 1, 2, TaskType.Map);
			TaskId t5 = CreateTaskId(ts2, 1, 1, TaskType.Map);
			NUnit.Framework.Assert.IsTrue(t1.Equals(t4));
			NUnit.Framework.Assert.IsFalse(t1.Equals(t2));
			NUnit.Framework.Assert.IsFalse(t1.Equals(t3));
			NUnit.Framework.Assert.IsFalse(t1.Equals(t5));
			NUnit.Framework.Assert.IsTrue(t1.CompareTo(t4) == 0);
			NUnit.Framework.Assert.IsTrue(t1.CompareTo(t2) < 0);
			NUnit.Framework.Assert.IsTrue(t1.CompareTo(t3) > 0);
			NUnit.Framework.Assert.IsTrue(t1.CompareTo(t5) < 0);
			NUnit.Framework.Assert.IsTrue(t1.GetHashCode() == t4.GetHashCode());
			NUnit.Framework.Assert.IsFalse(t1.GetHashCode() == t2.GetHashCode());
			NUnit.Framework.Assert.IsFalse(t1.GetHashCode() == t3.GetHashCode());
			NUnit.Framework.Assert.IsFalse(t1.GetHashCode() == t5.GetHashCode());
			TaskId t6 = CreateTaskId(ts1, 324151, 54643747, TaskType.Reduce);
			NUnit.Framework.Assert.AreEqual("task_" + ts1 + "_0001_m_000002", t1.ToString());
			NUnit.Framework.Assert.AreEqual("task_" + ts1 + "_324151_r_54643747", t6.ToString
				());
		}

		[NUnit.Framework.Test]
		public virtual void TestTaskAttemptId()
		{
			long ts1 = 1315890136000l;
			long ts2 = 1315890136001l;
			TaskAttemptId t1 = CreateTaskAttemptId(ts1, 2, 2, TaskType.Map, 2);
			TaskAttemptId t2 = CreateTaskAttemptId(ts1, 2, 2, TaskType.Reduce, 2);
			TaskAttemptId t3 = CreateTaskAttemptId(ts1, 2, 2, TaskType.Map, 3);
			TaskAttemptId t4 = CreateTaskAttemptId(ts1, 2, 2, TaskType.Map, 1);
			TaskAttemptId t5 = CreateTaskAttemptId(ts1, 2, 1, TaskType.Map, 3);
			TaskAttemptId t6 = CreateTaskAttemptId(ts1, 2, 2, TaskType.Map, 2);
			NUnit.Framework.Assert.IsTrue(t1.Equals(t6));
			NUnit.Framework.Assert.IsFalse(t1.Equals(t2));
			NUnit.Framework.Assert.IsFalse(t1.Equals(t3));
			NUnit.Framework.Assert.IsFalse(t1.Equals(t5));
			NUnit.Framework.Assert.IsTrue(t1.CompareTo(t6) == 0);
			NUnit.Framework.Assert.IsTrue(t1.CompareTo(t2) < 0);
			NUnit.Framework.Assert.IsTrue(t1.CompareTo(t3) < 0);
			NUnit.Framework.Assert.IsTrue(t1.CompareTo(t4) > 0);
			NUnit.Framework.Assert.IsTrue(t1.CompareTo(t5) > 0);
			NUnit.Framework.Assert.IsTrue(t1.GetHashCode() == t6.GetHashCode());
			NUnit.Framework.Assert.IsFalse(t1.GetHashCode() == t2.GetHashCode());
			NUnit.Framework.Assert.IsFalse(t1.GetHashCode() == t3.GetHashCode());
			NUnit.Framework.Assert.IsFalse(t1.GetHashCode() == t5.GetHashCode());
			TaskAttemptId t7 = CreateTaskAttemptId(ts2, 5463346, 4326575, TaskType.Reduce, 54375
				);
			NUnit.Framework.Assert.AreEqual("attempt_" + ts1 + "_0002_m_000002_2", t1.ToString
				());
			NUnit.Framework.Assert.AreEqual("attempt_" + ts2 + "_5463346_r_4326575_54375", t7
				.ToString());
		}

		private JobId CreateJobId(long clusterTimestamp, int idInt)
		{
			return MRBuilderUtils.NewJobId(ApplicationId.NewInstance(clusterTimestamp, idInt)
				, idInt);
		}

		private TaskId CreateTaskId(long clusterTimestamp, int jobIdInt, int taskIdInt, TaskType
			 taskType)
		{
			return MRBuilderUtils.NewTaskId(CreateJobId(clusterTimestamp, jobIdInt), taskIdInt
				, taskType);
		}

		private TaskAttemptId CreateTaskAttemptId(long clusterTimestamp, int jobIdInt, int
			 taskIdInt, TaskType taskType, int taskAttemptIdInt)
		{
			return MRBuilderUtils.NewTaskAttemptId(CreateTaskId(clusterTimestamp, jobIdInt, taskIdInt
				, taskType), taskAttemptIdInt);
		}
	}
}
