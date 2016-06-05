using System;
using System.IO;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce.Lib.Output;
using Org.Apache.Hadoop.Mapreduce.Tools;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce
{
	/// <summary>test CLI class.</summary>
	/// <remarks>
	/// test CLI class. CLI class implemented  the Tool interface.
	/// Here test that CLI sends correct command with options and parameters.
	/// </remarks>
	public class TestMRJobClient : ClusterMapReduceTestCase
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(TestMRJobClient));

		/// <exception cref="System.Exception"/>
		private Job RunJob(Configuration conf)
		{
			string input = "hello1\nhello2\nhello3\n";
			Job job = MapReduceTestUtil.CreateJob(conf, GetInputDir(), GetOutputDir(), 1, 1, 
				input);
			job.SetJobName("mr");
			job.SetPriority(JobPriority.Normal);
			job.WaitForCompletion(true);
			return job;
		}

		/// <exception cref="System.Exception"/>
		private Job RunJobInBackGround(Configuration conf)
		{
			string input = "hello1\nhello2\nhello3\n";
			Job job = MapReduceTestUtil.CreateJob(conf, GetInputDir(), GetOutputDir(), 1, 1, 
				input);
			job.SetJobName("mr");
			job.SetPriority(JobPriority.Normal);
			job.Submit();
			int i = 0;
			while (i++ < 200 && job.GetJobID() == null)
			{
				Log.Info("waiting for jobId...");
				Sharpen.Thread.Sleep(100);
			}
			return job;
		}

		/// <exception cref="System.Exception"/>
		public static int RunTool(Configuration conf, Tool tool, string[] args, OutputStream
			 @out)
		{
			TextWriter oldOut = System.Console.Out;
			TextWriter newOut = new TextWriter(@out, true);
			try
			{
				Runtime.SetOut(newOut);
				return ToolRunner.Run(conf, tool, args);
			}
			finally
			{
				Runtime.SetOut(oldOut);
			}
		}

		private class BadOutputFormat : TextOutputFormat<object, object>
		{
			/// <exception cref="System.IO.IOException"/>
			public override void CheckOutputSpecs(JobContext job)
			{
				throw new IOException();
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestJobSubmissionSpecsAndFiles()
		{
			Configuration conf = CreateJobConf();
			Job job = MapReduceTestUtil.CreateJob(conf, GetInputDir(), GetOutputDir(), 1, 1);
			job.SetOutputFormatClass(typeof(TestMRJobClient.BadOutputFormat));
			try
			{
				job.Submit();
				Fail("Should've thrown an exception while checking output specs.");
			}
			catch (Exception e)
			{
				NUnit.Framework.Assert.IsTrue(e is IOException);
			}
			Cluster cluster = new Cluster(conf);
			Path jobStagingArea = JobSubmissionFiles.GetStagingDir(cluster, job.GetConfiguration
				());
			Path submitJobDir = new Path(jobStagingArea, "JobId");
			Path submitJobFile = JobSubmissionFiles.GetJobConfPath(submitJobDir);
			NUnit.Framework.Assert.IsFalse("Shouldn't have created a job file if job specs failed."
				, FileSystem.Get(conf).Exists(submitJobFile));
		}

		/// <summary>main test method</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestJobClient()
		{
			Configuration conf = CreateJobConf();
			Job job = RunJob(conf);
			string jobId = job.GetJobID().ToString();
			// test all jobs list
			TestAllJobList(jobId, conf);
			// test only submitted jobs list
			TestSubmittedJobList(conf);
			// test job counter
			TestGetCounter(jobId, conf);
			// status
			TestJobStatus(jobId, conf);
			// test list of events
			TestJobEvents(jobId, conf);
			// test job history
			TestJobHistory(conf);
			// test tracker list
			TestListTrackers(conf);
			// attempts list
			TestListAttemptIds(jobId, conf);
			// black list
			TestListBlackList(conf);
			// test method main and help screen
			StartStop();
			// test a change job priority .
			TestChangingJobPriority(jobId, conf);
			// submit job from file
			TestSubmit(conf);
			// kill a task
			TestKillTask(conf);
			// fail a task
			TestfailTask(conf);
			// kill job
			TestKillJob(conf);
		}

		/// <summary>test fail task</summary>
		/// <exception cref="System.Exception"/>
		private void TestfailTask(Configuration conf)
		{
			Job job = RunJobInBackGround(conf);
			CLI jc = CreateJobClient();
			TaskID tid = new TaskID(job.GetJobID(), TaskType.Map, 0);
			TaskAttemptID taid = new TaskAttemptID(tid, 1);
			ByteArrayOutputStream @out = new ByteArrayOutputStream();
			// TaskAttemptId is not set
			int exitCode = RunTool(conf, jc, new string[] { "-fail-task" }, @out);
			NUnit.Framework.Assert.AreEqual("Exit code", -1, exitCode);
			RunTool(conf, jc, new string[] { "-fail-task", taid.ToString() }, @out);
			string answer = Sharpen.Runtime.GetStringForBytes(@out.ToByteArray(), "UTF-8");
			NUnit.Framework.Assert.IsTrue(answer.Contains("Killed task " + taid + " by failing it"
				));
		}

		/// <summary>test a kill task</summary>
		/// <exception cref="System.Exception"/>
		private void TestKillTask(Configuration conf)
		{
			Job job = RunJobInBackGround(conf);
			CLI jc = CreateJobClient();
			TaskID tid = new TaskID(job.GetJobID(), TaskType.Map, 0);
			TaskAttemptID taid = new TaskAttemptID(tid, 1);
			ByteArrayOutputStream @out = new ByteArrayOutputStream();
			// bad parameters
			int exitCode = RunTool(conf, jc, new string[] { "-kill-task" }, @out);
			NUnit.Framework.Assert.AreEqual("Exit code", -1, exitCode);
			RunTool(conf, jc, new string[] { "-kill-task", taid.ToString() }, @out);
			string answer = Sharpen.Runtime.GetStringForBytes(@out.ToByteArray(), "UTF-8");
			NUnit.Framework.Assert.IsTrue(answer.Contains("Killed task " + taid));
		}

		/// <summary>test a kill job</summary>
		/// <exception cref="System.Exception"/>
		private void TestKillJob(Configuration conf)
		{
			Job job = RunJobInBackGround(conf);
			string jobId = job.GetJobID().ToString();
			CLI jc = CreateJobClient();
			ByteArrayOutputStream @out = new ByteArrayOutputStream();
			// without jobId
			int exitCode = RunTool(conf, jc, new string[] { "-kill" }, @out);
			NUnit.Framework.Assert.AreEqual("Exit code", -1, exitCode);
			// good parameters
			exitCode = RunTool(conf, jc, new string[] { "-kill", jobId }, @out);
			NUnit.Framework.Assert.AreEqual("Exit code", 0, exitCode);
			string answer = Sharpen.Runtime.GetStringForBytes(@out.ToByteArray(), "UTF-8");
			NUnit.Framework.Assert.IsTrue(answer.Contains("Killed job " + jobId));
		}

		/// <summary>test submit task from file</summary>
		/// <exception cref="System.Exception"/>
		private void TestSubmit(Configuration conf)
		{
			CLI jc = CreateJobClient();
			Job job = MapReduceTestUtil.CreateJob(conf, GetInputDir(), GetOutputDir(), 1, 1, 
				"ping");
			job.SetJobName("mr");
			job.SetPriority(JobPriority.Normal);
			FilePath fcon = FilePath.CreateTempFile("config", ".xml");
			FileSystem localFs = FileSystem.GetLocal(conf);
			string fconUri = new Path(fcon.GetAbsolutePath()).MakeQualified(localFs.GetUri(), 
				localFs.GetWorkingDirectory()).ToUri().ToString();
			job.GetConfiguration().WriteXml(new FileOutputStream(fcon));
			ByteArrayOutputStream @out = new ByteArrayOutputStream();
			// bad parameters
			int exitCode = RunTool(conf, jc, new string[] { "-submit" }, @out);
			NUnit.Framework.Assert.AreEqual("Exit code", -1, exitCode);
			exitCode = RunTool(conf, jc, new string[] { "-submit", fconUri }, @out);
			NUnit.Framework.Assert.AreEqual("Exit code", 0, exitCode);
			string answer = Sharpen.Runtime.GetStringForBytes(@out.ToByteArray());
			// in console was written
			NUnit.Framework.Assert.IsTrue(answer.Contains("Created job "));
		}

		/// <summary>test start form console command without options</summary>
		private void StartStop()
		{
			ByteArrayOutputStream data = new ByteArrayOutputStream();
			TextWriter error = System.Console.Error;
			Runtime.SetErr(new TextWriter(data));
			ExitUtil.DisableSystemExit();
			try
			{
				CLI.Main(new string[0]);
				Fail(" CLI.main should call System.exit");
			}
			catch (ExitUtil.ExitException e)
			{
				ExitUtil.ResetFirstExitException();
				NUnit.Framework.Assert.AreEqual(-1, e.status);
			}
			catch (Exception)
			{
			}
			finally
			{
				Runtime.SetErr(error);
			}
			// in console should be written help text 
			string s = Sharpen.Runtime.GetStringForBytes(data.ToByteArray());
			NUnit.Framework.Assert.IsTrue(s.Contains("-submit"));
			NUnit.Framework.Assert.IsTrue(s.Contains("-status"));
			NUnit.Framework.Assert.IsTrue(s.Contains("-kill"));
			NUnit.Framework.Assert.IsTrue(s.Contains("-set-priority"));
			NUnit.Framework.Assert.IsTrue(s.Contains("-events"));
			NUnit.Framework.Assert.IsTrue(s.Contains("-history"));
			NUnit.Framework.Assert.IsTrue(s.Contains("-list"));
			NUnit.Framework.Assert.IsTrue(s.Contains("-list-active-trackers"));
			NUnit.Framework.Assert.IsTrue(s.Contains("-list-blacklisted-trackers"));
			NUnit.Framework.Assert.IsTrue(s.Contains("-list-attempt-ids"));
			NUnit.Framework.Assert.IsTrue(s.Contains("-kill-task"));
			NUnit.Framework.Assert.IsTrue(s.Contains("-fail-task"));
			NUnit.Framework.Assert.IsTrue(s.Contains("-logs"));
		}

		/// <summary>black list</summary>
		/// <exception cref="System.Exception"/>
		private void TestListBlackList(Configuration conf)
		{
			CLI jc = CreateJobClient();
			ByteArrayOutputStream @out = new ByteArrayOutputStream();
			int exitCode = RunTool(conf, jc, new string[] { "-list-blacklisted-trackers", "second in"
				 }, @out);
			NUnit.Framework.Assert.AreEqual("Exit code", -1, exitCode);
			exitCode = RunTool(conf, jc, new string[] { "-list-blacklisted-trackers" }, @out);
			NUnit.Framework.Assert.AreEqual("Exit code", 0, exitCode);
			string line;
			BufferedReader br = new BufferedReader(new InputStreamReader(new ByteArrayInputStream
				(@out.ToByteArray())));
			int counter = 0;
			while ((line = br.ReadLine()) != null)
			{
				Log.Info("line = " + line);
				counter++;
			}
			NUnit.Framework.Assert.AreEqual(0, counter);
		}

		/// <summary>print AttemptIds list</summary>
		/// <exception cref="System.Exception"/>
		private void TestListAttemptIds(string jobId, Configuration conf)
		{
			CLI jc = CreateJobClient();
			ByteArrayOutputStream @out = new ByteArrayOutputStream();
			int exitCode = RunTool(conf, jc, new string[] { "-list-attempt-ids" }, @out);
			NUnit.Framework.Assert.AreEqual("Exit code", -1, exitCode);
			exitCode = RunTool(conf, jc, new string[] { "-list-attempt-ids", jobId, "MAP", "completed"
				 }, @out);
			NUnit.Framework.Assert.AreEqual("Exit code", 0, exitCode);
			string line;
			BufferedReader br = new BufferedReader(new InputStreamReader(new ByteArrayInputStream
				(@out.ToByteArray())));
			int counter = 0;
			while ((line = br.ReadLine()) != null)
			{
				Log.Info("line = " + line);
				counter++;
			}
			NUnit.Framework.Assert.AreEqual(1, counter);
		}

		/// <summary>print tracker list</summary>
		/// <exception cref="System.Exception"/>
		private void TestListTrackers(Configuration conf)
		{
			CLI jc = CreateJobClient();
			ByteArrayOutputStream @out = new ByteArrayOutputStream();
			int exitCode = RunTool(conf, jc, new string[] { "-list-active-trackers", "second parameter"
				 }, @out);
			NUnit.Framework.Assert.AreEqual("Exit code", -1, exitCode);
			exitCode = RunTool(conf, jc, new string[] { "-list-active-trackers" }, @out);
			NUnit.Framework.Assert.AreEqual("Exit code", 0, exitCode);
			string line;
			BufferedReader br = new BufferedReader(new InputStreamReader(new ByteArrayInputStream
				(@out.ToByteArray())));
			int counter = 0;
			while ((line = br.ReadLine()) != null)
			{
				Log.Info("line = " + line);
				counter++;
			}
			NUnit.Framework.Assert.AreEqual(2, counter);
		}

		/// <summary>print job history from file</summary>
		/// <exception cref="System.Exception"/>
		private void TestJobHistory(Configuration conf)
		{
			CLI jc = CreateJobClient();
			ByteArrayOutputStream @out = new ByteArrayOutputStream();
			FilePath f = new FilePath("src/test/resources/job_1329348432655_0001-10.jhist");
			FileSystem localFs = FileSystem.GetLocal(conf);
			string historyFileUri = new Path(f.GetAbsolutePath()).MakeQualified(localFs.GetUri
				(), localFs.GetWorkingDirectory()).ToUri().ToString();
			// bad command
			int exitCode = RunTool(conf, jc, new string[] { "-history", "pul", historyFileUri
				 }, @out);
			NUnit.Framework.Assert.AreEqual("Exit code", -1, exitCode);
			exitCode = RunTool(conf, jc, new string[] { "-history", "all", historyFileUri }, 
				@out);
			NUnit.Framework.Assert.AreEqual("Exit code", 0, exitCode);
			string line;
			BufferedReader br = new BufferedReader(new InputStreamReader(new ByteArrayInputStream
				(@out.ToByteArray())));
			int counter = 0;
			while ((line = br.ReadLine()) != null)
			{
				Log.Info("line = " + line);
				if (line.StartsWith("task_"))
				{
					counter++;
				}
			}
			NUnit.Framework.Assert.AreEqual(23, counter);
		}

		/// <summary>print job events list</summary>
		/// <exception cref="System.Exception"/>
		private void TestJobEvents(string jobId, Configuration conf)
		{
			CLI jc = CreateJobClient();
			ByteArrayOutputStream @out = new ByteArrayOutputStream();
			int exitCode = RunTool(conf, jc, new string[] { "-events" }, @out);
			NUnit.Framework.Assert.AreEqual("Exit code", -1, exitCode);
			exitCode = RunTool(conf, jc, new string[] { "-events", jobId, "0", "100" }, @out);
			NUnit.Framework.Assert.AreEqual("Exit code", 0, exitCode);
			string line;
			BufferedReader br = new BufferedReader(new InputStreamReader(new ByteArrayInputStream
				(@out.ToByteArray())));
			int counter = 0;
			string attemptId = ("attempt" + Sharpen.Runtime.Substring(jobId, 3));
			while ((line = br.ReadLine()) != null)
			{
				Log.Info("line = " + line);
				if (line.Contains(attemptId))
				{
					counter++;
				}
			}
			NUnit.Framework.Assert.AreEqual(2, counter);
		}

		/// <summary>print job status</summary>
		/// <exception cref="System.Exception"/>
		private void TestJobStatus(string jobId, Configuration conf)
		{
			CLI jc = CreateJobClient();
			ByteArrayOutputStream @out = new ByteArrayOutputStream();
			// bad options
			int exitCode = RunTool(conf, jc, new string[] { "-status" }, @out);
			NUnit.Framework.Assert.AreEqual("Exit code", -1, exitCode);
			exitCode = RunTool(conf, jc, new string[] { "-status", jobId }, @out);
			NUnit.Framework.Assert.AreEqual("Exit code", 0, exitCode);
			string line;
			BufferedReader br = new BufferedReader(new InputStreamReader(new ByteArrayInputStream
				(@out.ToByteArray())));
			while ((line = br.ReadLine()) != null)
			{
				Log.Info("line = " + line);
				if (!line.Contains("Job state:"))
				{
					continue;
				}
				break;
			}
			NUnit.Framework.Assert.IsNotNull(line);
			NUnit.Framework.Assert.IsTrue(line.Contains("SUCCEEDED"));
		}

		/// <summary>print counters</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestGetCounter(string jobId, Configuration conf)
		{
			ByteArrayOutputStream @out = new ByteArrayOutputStream();
			// bad command 
			int exitCode = RunTool(conf, CreateJobClient(), new string[] { "-counter" }, @out
				);
			NUnit.Framework.Assert.AreEqual("Exit code", -1, exitCode);
			exitCode = RunTool(conf, CreateJobClient(), new string[] { "-counter", jobId, "org.apache.hadoop.mapreduce.TaskCounter"
				, "MAP_INPUT_RECORDS" }, @out);
			NUnit.Framework.Assert.AreEqual("Exit code", 0, exitCode);
			NUnit.Framework.Assert.AreEqual("Counter", "3", @out.ToString().Trim());
		}

		/// <summary>print a job list</summary>
		/// <exception cref="System.Exception"/>
		protected internal virtual void TestAllJobList(string jobId, Configuration conf)
		{
			ByteArrayOutputStream @out = new ByteArrayOutputStream();
			// bad options
			int exitCode = RunTool(conf, CreateJobClient(), new string[] { "-list", "alldata"
				 }, @out);
			NUnit.Framework.Assert.AreEqual("Exit code", -1, exitCode);
			exitCode = RunTool(conf, CreateJobClient(), new string[] { "-list", "all" }, @out
				);
			// all jobs
			NUnit.Framework.Assert.AreEqual("Exit code", 0, exitCode);
			BufferedReader br = new BufferedReader(new InputStreamReader(new ByteArrayInputStream
				(@out.ToByteArray())));
			string line;
			int counter = 0;
			while ((line = br.ReadLine()) != null)
			{
				Log.Info("line = " + line);
				if (line.Contains(jobId))
				{
					counter++;
				}
			}
			NUnit.Framework.Assert.AreEqual(1, counter);
			@out.Reset();
		}

		/// <exception cref="System.Exception"/>
		protected internal virtual void TestSubmittedJobList(Configuration conf)
		{
			Job job = RunJobInBackGround(conf);
			ByteArrayOutputStream @out = new ByteArrayOutputStream();
			string line;
			int counter = 0;
			// only submitted
			int exitCode = RunTool(conf, CreateJobClient(), new string[] { "-list" }, @out);
			NUnit.Framework.Assert.AreEqual("Exit code", 0, exitCode);
			BufferedReader br = new BufferedReader(new InputStreamReader(new ByteArrayInputStream
				(@out.ToByteArray())));
			counter = 0;
			while ((line = br.ReadLine()) != null)
			{
				Log.Info("line = " + line);
				if (line.Contains(job.GetJobID().ToString()))
				{
					counter++;
				}
			}
			// all jobs submitted! no current
			NUnit.Framework.Assert.AreEqual(1, counter);
		}

		/// <exception cref="System.Exception"/>
		protected internal virtual void VerifyJobPriority(string jobId, string priority, 
			Configuration conf, CLI jc)
		{
			PipedInputStream pis = new PipedInputStream();
			PipedOutputStream pos = new PipedOutputStream(pis);
			int exitCode = RunTool(conf, jc, new string[] { "-list", "all" }, pos);
			NUnit.Framework.Assert.AreEqual("Exit code", 0, exitCode);
			BufferedReader br = new BufferedReader(new InputStreamReader(pis));
			string line;
			while ((line = br.ReadLine()) != null)
			{
				Log.Info("line = " + line);
				if (!line.Contains(jobId))
				{
					continue;
				}
				NUnit.Framework.Assert.IsTrue(line.Contains(priority));
				break;
			}
			pis.Close();
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestChangingJobPriority(string jobId, Configuration conf)
		{
			int exitCode = RunTool(conf, CreateJobClient(), new string[] { "-set-priority" }, 
				new ByteArrayOutputStream());
			NUnit.Framework.Assert.AreEqual("Exit code", -1, exitCode);
			exitCode = RunTool(conf, CreateJobClient(), new string[] { "-set-priority", jobId
				, "VERY_LOW" }, new ByteArrayOutputStream());
			NUnit.Framework.Assert.AreEqual("Exit code", 0, exitCode);
			// because this method does not implemented still.
			VerifyJobPriority(jobId, "NORMAL", conf, CreateJobClient());
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual CLI CreateJobClient()
		{
			return new CLI();
		}
	}
}
