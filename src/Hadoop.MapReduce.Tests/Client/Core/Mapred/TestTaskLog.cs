using System.IO;
using System.Text;
using Org.Apache.Commons.IO;
using Org.Apache.Hadoop.Yarn.Conf;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	/// <summary>TestCounters checks the sanity and recoverability of Queue</summary>
	public class TestTaskLog
	{
		/// <summary>test TaskAttemptID</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestTaskLog()
		{
			// test TaskLog
			Runtime.SetProperty(YarnConfiguration.YarnAppContainerLogDir, "testString");
			NUnit.Framework.Assert.AreEqual(TaskLog.GetMRv2LogDir(), "testString");
			TaskAttemptID taid = Org.Mockito.Mockito.Mock<TaskAttemptID>();
			JobID jid = new JobID("job", 1);
			Org.Mockito.Mockito.When(((JobID)taid.GetJobID())).ThenReturn(jid);
			Org.Mockito.Mockito.When(taid.ToString()).ThenReturn("JobId");
			FilePath f = TaskLog.GetTaskLogFile(taid, true, TaskLog.LogName.Stdout);
			NUnit.Framework.Assert.IsTrue(f.GetAbsolutePath().EndsWith("testString" + FilePath
				.separatorChar + "stdout"));
			// test getRealTaskLogFileLocation
			FilePath indexFile = TaskLog.GetIndexFile(taid, true);
			if (!indexFile.GetParentFile().Exists())
			{
				indexFile.GetParentFile().Mkdirs();
			}
			indexFile.Delete();
			indexFile.CreateNewFile();
			TaskLog.SyncLogs("location", taid, true);
			NUnit.Framework.Assert.IsTrue(indexFile.GetAbsolutePath().EndsWith("userlogs" + FilePath
				.separatorChar + "job_job_0001" + FilePath.separatorChar + "JobId.cleanup" + FilePath
				.separatorChar + "log.index"));
			f = TaskLog.GetRealTaskLogFileLocation(taid, true, TaskLog.LogName.Debugout);
			if (f != null)
			{
				NUnit.Framework.Assert.IsTrue(f.GetAbsolutePath().EndsWith("location" + FilePath.
					separatorChar + "debugout"));
				FileUtils.CopyFile(indexFile, f);
			}
			// test obtainLogDirOwner
			NUnit.Framework.Assert.IsTrue(TaskLog.ObtainLogDirOwner(taid).Length > 0);
			// test TaskLog.Reader
			NUnit.Framework.Assert.IsTrue(ReadTaskLog(TaskLog.LogName.Debugout, taid, true).Length
				 > 0);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual string ReadTaskLog(TaskLog.LogName filter, TaskAttemptID taskId, bool
			 isCleanup)
		{
			// string buffer to store task log
			StringBuilder result = new StringBuilder();
			int res;
			// reads the whole tasklog into inputstream
			InputStream taskLogReader = new TaskLog.Reader(taskId, filter, 0, -1, isCleanup);
			// construct string log from inputstream.
			byte[] b = new byte[65536];
			while (true)
			{
				res = taskLogReader.Read(b);
				if (res > 0)
				{
					result.Append(Sharpen.Runtime.GetStringForBytes(b));
				}
				else
				{
					break;
				}
			}
			taskLogReader.Close();
			// trim the string and return it
			string str = result.ToString();
			str = str.Trim();
			return str;
		}

		/// <summary>test without TASK_LOG_DIR</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestTaskLogWithoutTaskLogDir()
		{
			// TaskLog tasklog= new TaskLog();
			Runtime.ClearProperty(YarnConfiguration.YarnAppContainerLogDir);
			// test TaskLog
			NUnit.Framework.Assert.AreEqual(TaskLog.GetMRv2LogDir(), null);
			TaskAttemptID taid = Org.Mockito.Mockito.Mock<TaskAttemptID>();
			JobID jid = new JobID("job", 1);
			Org.Mockito.Mockito.When(((JobID)taid.GetJobID())).ThenReturn(jid);
			Org.Mockito.Mockito.When(taid.ToString()).ThenReturn("JobId");
			FilePath f = TaskLog.GetTaskLogFile(taid, true, TaskLog.LogName.Stdout);
			NUnit.Framework.Assert.IsTrue(f.GetAbsolutePath().EndsWith("stdout"));
		}
	}
}
