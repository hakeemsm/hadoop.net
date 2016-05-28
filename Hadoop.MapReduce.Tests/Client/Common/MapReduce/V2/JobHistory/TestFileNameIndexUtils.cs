using NUnit.Framework;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.Jobhistory
{
	public class TestFileNameIndexUtils
	{
		private const string OldJobHistoryFileFormatter = "%s" + FileNameIndexUtils.Delimiter
			 + "%s" + FileNameIndexUtils.Delimiter + "%s" + FileNameIndexUtils.Delimiter + "%s"
			 + FileNameIndexUtils.Delimiter + "%s" + FileNameIndexUtils.Delimiter + "%s" + FileNameIndexUtils
			.Delimiter + "%s" + FileNameIndexUtils.Delimiter + "%s" + JobHistoryUtils.JobHistoryFileExtension;

		private const string OldFormatBeforeAddStartTime = "%s" + FileNameIndexUtils.Delimiter
			 + "%s" + FileNameIndexUtils.Delimiter + "%s" + FileNameIndexUtils.Delimiter + "%s"
			 + FileNameIndexUtils.Delimiter + "%s" + FileNameIndexUtils.Delimiter + "%s" + FileNameIndexUtils
			.Delimiter + "%s" + FileNameIndexUtils.Delimiter + "%s" + FileNameIndexUtils.Delimiter
			 + "%s" + JobHistoryUtils.JobHistoryFileExtension;

		private const string JobHistoryFileFormatter = "%s" + FileNameIndexUtils.Delimiter
			 + "%s" + FileNameIndexUtils.Delimiter + "%s" + FileNameIndexUtils.Delimiter + "%s"
			 + FileNameIndexUtils.Delimiter + "%s" + FileNameIndexUtils.Delimiter + "%s" + FileNameIndexUtils
			.Delimiter + "%s" + FileNameIndexUtils.Delimiter + "%s" + FileNameIndexUtils.Delimiter
			 + "%s" + FileNameIndexUtils.Delimiter + "%s" + JobHistoryUtils.JobHistoryFileExtension;

		private const string JobId = "job_1317928501754_0001";

		private const string SubmitTime = "1317928742025";

		private const string UserName = "username";

		private const string UserNameWithDelimiter = "user" + FileNameIndexUtils.Delimiter
			 + "name";

		private const string UserNameWithDelimiterEscape = "user" + FileNameIndexUtils.DelimiterEscape
			 + "name";

		private const string JobName = "mapreduce";

		private const string JobNameWithDelimiter = "map" + FileNameIndexUtils.Delimiter 
			+ "reduce";

		private const string JobNameWithDelimiterEscape = "map" + FileNameIndexUtils.DelimiterEscape
			 + "reduce";

		private const string FinishTime = "1317928754958";

		private const string NumMaps = "1";

		private const string NumReduces = "1";

		private const string JobStatus = "SUCCEEDED";

		private const string QueueName = "default";

		private const string QueueNameWithDelimiter = "test" + FileNameIndexUtils.Delimiter
			 + "queue";

		private const string QueueNameWithDelimiterEscape = "test" + FileNameIndexUtils.DelimiterEscape
			 + "queue";

		private const string JobStartTime = "1317928742060";

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestEncodingDecodingEquivalence()
		{
			JobIndexInfo info = new JobIndexInfo();
			JobID oldJobId = JobID.ForName(JobId);
			JobId jobId = TypeConverter.ToYarn(oldJobId);
			info.SetJobId(jobId);
			info.SetSubmitTime(long.Parse(SubmitTime));
			info.SetUser(UserName);
			info.SetJobName(JobName);
			info.SetFinishTime(long.Parse(FinishTime));
			info.SetNumMaps(System.Convert.ToInt32(NumMaps));
			info.SetNumReduces(System.Convert.ToInt32(NumReduces));
			info.SetJobStatus(JobStatus);
			info.SetQueueName(QueueName);
			info.SetJobStartTime(long.Parse(JobStartTime));
			string jobHistoryFile = FileNameIndexUtils.GetDoneFileName(info);
			JobIndexInfo parsedInfo = FileNameIndexUtils.GetIndexInfo(jobHistoryFile);
			NUnit.Framework.Assert.AreEqual("Job id different after encoding and decoding", info
				.GetJobId(), parsedInfo.GetJobId());
			NUnit.Framework.Assert.AreEqual("Submit time different after encoding and decoding"
				, info.GetSubmitTime(), parsedInfo.GetSubmitTime());
			NUnit.Framework.Assert.AreEqual("User different after encoding and decoding", info
				.GetUser(), parsedInfo.GetUser());
			NUnit.Framework.Assert.AreEqual("Job name different after encoding and decoding", 
				info.GetJobName(), parsedInfo.GetJobName());
			NUnit.Framework.Assert.AreEqual("Finish time different after encoding and decoding"
				, info.GetFinishTime(), parsedInfo.GetFinishTime());
			NUnit.Framework.Assert.AreEqual("Num maps different after encoding and decoding", 
				info.GetNumMaps(), parsedInfo.GetNumMaps());
			NUnit.Framework.Assert.AreEqual("Num reduces different after encoding and decoding"
				, info.GetNumReduces(), parsedInfo.GetNumReduces());
			NUnit.Framework.Assert.AreEqual("Job status different after encoding and decoding"
				, info.GetJobStatus(), parsedInfo.GetJobStatus());
			NUnit.Framework.Assert.AreEqual("Queue name different after encoding and decoding"
				, info.GetQueueName(), parsedInfo.GetQueueName());
			NUnit.Framework.Assert.AreEqual("Job start time different after encoding and decoding"
				, info.GetJobStartTime(), parsedInfo.GetJobStartTime());
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestUserNamePercentEncoding()
		{
			JobIndexInfo info = new JobIndexInfo();
			JobID oldJobId = JobID.ForName(JobId);
			JobId jobId = TypeConverter.ToYarn(oldJobId);
			info.SetJobId(jobId);
			info.SetSubmitTime(long.Parse(SubmitTime));
			info.SetUser(UserNameWithDelimiter);
			info.SetJobName(JobName);
			info.SetFinishTime(long.Parse(FinishTime));
			info.SetNumMaps(System.Convert.ToInt32(NumMaps));
			info.SetNumReduces(System.Convert.ToInt32(NumReduces));
			info.SetJobStatus(JobStatus);
			info.SetQueueName(QueueName);
			info.SetJobStartTime(long.Parse(JobStartTime));
			string jobHistoryFile = FileNameIndexUtils.GetDoneFileName(info);
			NUnit.Framework.Assert.IsTrue("User name not encoded correctly into job history file"
				, jobHistoryFile.Contains(UserNameWithDelimiterEscape));
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestUserNamePercentDecoding()
		{
			string jobHistoryFile = string.Format(JobHistoryFileFormatter, JobId, SubmitTime, 
				UserNameWithDelimiterEscape, JobName, FinishTime, NumMaps, NumReduces, JobStatus
				, QueueName, JobStartTime);
			JobIndexInfo info = FileNameIndexUtils.GetIndexInfo(jobHistoryFile);
			NUnit.Framework.Assert.AreEqual("User name doesn't match", UserNameWithDelimiter, 
				info.GetUser());
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestJobNamePercentEncoding()
		{
			JobIndexInfo info = new JobIndexInfo();
			JobID oldJobId = JobID.ForName(JobId);
			JobId jobId = TypeConverter.ToYarn(oldJobId);
			info.SetJobId(jobId);
			info.SetSubmitTime(long.Parse(SubmitTime));
			info.SetUser(UserName);
			info.SetJobName(JobNameWithDelimiter);
			info.SetFinishTime(long.Parse(FinishTime));
			info.SetNumMaps(System.Convert.ToInt32(NumMaps));
			info.SetNumReduces(System.Convert.ToInt32(NumReduces));
			info.SetJobStatus(JobStatus);
			info.SetQueueName(QueueName);
			info.SetJobStartTime(long.Parse(JobStartTime));
			string jobHistoryFile = FileNameIndexUtils.GetDoneFileName(info);
			NUnit.Framework.Assert.IsTrue("Job name not encoded correctly into job history file"
				, jobHistoryFile.Contains(JobNameWithDelimiterEscape));
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestJobNamePercentDecoding()
		{
			string jobHistoryFile = string.Format(JobHistoryFileFormatter, JobId, SubmitTime, 
				UserName, JobNameWithDelimiterEscape, FinishTime, NumMaps, NumReduces, JobStatus
				, QueueName, JobStartTime);
			JobIndexInfo info = FileNameIndexUtils.GetIndexInfo(jobHistoryFile);
			NUnit.Framework.Assert.AreEqual("Job name doesn't match", JobNameWithDelimiter, info
				.GetJobName());
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestQueueNamePercentEncoding()
		{
			JobIndexInfo info = new JobIndexInfo();
			JobID oldJobId = JobID.ForName(JobId);
			JobId jobId = TypeConverter.ToYarn(oldJobId);
			info.SetJobId(jobId);
			info.SetSubmitTime(long.Parse(SubmitTime));
			info.SetUser(UserName);
			info.SetJobName(JobName);
			info.SetFinishTime(long.Parse(FinishTime));
			info.SetNumMaps(System.Convert.ToInt32(NumMaps));
			info.SetNumReduces(System.Convert.ToInt32(NumReduces));
			info.SetJobStatus(JobStatus);
			info.SetQueueName(QueueNameWithDelimiter);
			info.SetJobStartTime(long.Parse(JobStartTime));
			string jobHistoryFile = FileNameIndexUtils.GetDoneFileName(info);
			NUnit.Framework.Assert.IsTrue("Queue name not encoded correctly into job history file"
				, jobHistoryFile.Contains(QueueNameWithDelimiterEscape));
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestQueueNamePercentDecoding()
		{
			string jobHistoryFile = string.Format(JobHistoryFileFormatter, JobId, SubmitTime, 
				UserName, JobName, FinishTime, NumMaps, NumReduces, JobStatus, QueueNameWithDelimiterEscape
				, JobStartTime);
			JobIndexInfo info = FileNameIndexUtils.GetIndexInfo(jobHistoryFile);
			NUnit.Framework.Assert.AreEqual("Queue name doesn't match", QueueNameWithDelimiter
				, info.GetQueueName());
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestJobStartTimeBackwardsCompatible()
		{
			string jobHistoryFile = string.Format(OldFormatBeforeAddStartTime, JobId, SubmitTime
				, UserName, JobNameWithDelimiterEscape, FinishTime, NumMaps, NumReduces, JobStatus
				, QueueName);
			JobIndexInfo info = FileNameIndexUtils.GetIndexInfo(jobHistoryFile);
			NUnit.Framework.Assert.AreEqual(info.GetJobStartTime(), info.GetSubmitTime());
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestJobHistoryFileNameBackwardsCompatible()
		{
			JobID oldJobId = JobID.ForName(JobId);
			JobId jobId = TypeConverter.ToYarn(oldJobId);
			long submitTime = long.Parse(SubmitTime);
			long finishTime = long.Parse(FinishTime);
			int numMaps = System.Convert.ToInt32(NumMaps);
			int numReduces = System.Convert.ToInt32(NumReduces);
			string jobHistoryFile = string.Format(OldJobHistoryFileFormatter, JobId, SubmitTime
				, UserName, JobName, FinishTime, NumMaps, NumReduces, JobStatus);
			JobIndexInfo info = FileNameIndexUtils.GetIndexInfo(jobHistoryFile);
			NUnit.Framework.Assert.AreEqual("Job id incorrect after decoding old history file"
				, jobId, info.GetJobId());
			NUnit.Framework.Assert.AreEqual("Submit time incorrect after decoding old history file"
				, submitTime, info.GetSubmitTime());
			NUnit.Framework.Assert.AreEqual("User incorrect after decoding old history file", 
				UserName, info.GetUser());
			NUnit.Framework.Assert.AreEqual("Job name incorrect after decoding old history file"
				, JobName, info.GetJobName());
			NUnit.Framework.Assert.AreEqual("Finish time incorrect after decoding old history file"
				, finishTime, info.GetFinishTime());
			NUnit.Framework.Assert.AreEqual("Num maps incorrect after decoding old history file"
				, numMaps, info.GetNumMaps());
			NUnit.Framework.Assert.AreEqual("Num reduces incorrect after decoding old history file"
				, numReduces, info.GetNumReduces());
			NUnit.Framework.Assert.AreEqual("Job status incorrect after decoding old history file"
				, JobStatus, info.GetJobStatus());
			NUnit.Framework.Assert.IsNull("Queue name incorrect after decoding old history file"
				, info.GetQueueName());
		}
	}
}
