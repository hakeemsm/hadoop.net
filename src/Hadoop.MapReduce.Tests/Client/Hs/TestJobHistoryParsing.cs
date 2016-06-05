using System;
using System.Collections.Generic;
using System.IO;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Jobhistory;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records.Impl.PB;
using Org.Apache.Hadoop.Mapreduce.V2.App;
using Org.Apache.Hadoop.Mapreduce.V2.App.Job;
using Org.Apache.Hadoop.Mapreduce.V2.App.Job.Event;
using Org.Apache.Hadoop.Mapreduce.V2.App.Job.Impl;
using Org.Apache.Hadoop.Mapreduce.V2.HS.Webapp.Dao;
using Org.Apache.Hadoop.Mapreduce.V2.Jobhistory;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Service;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Util;
using Org.Mockito.Invocation;
using Org.Mockito.Stubbing;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.HS
{
	public class TestJobHistoryParsing
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(TestJobHistoryParsing)
			);

		private const string RackName = "/MyRackName";

		private ByteArrayOutputStream outContent = new ByteArrayOutputStream();

		public class MyResolver : DNSToSwitchMapping
		{
			public virtual IList<string> Resolve(IList<string> names)
			{
				return Arrays.AsList(new string[] { RackName });
			}

			public virtual void ReloadCachedMappings()
			{
			}

			public virtual void ReloadCachedMappings(IList<string> names)
			{
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestJobInfo()
		{
			JobHistoryParser.JobInfo info = new JobHistoryParser.JobInfo();
			NUnit.Framework.Assert.AreEqual("NORMAL", info.GetPriority());
			info.PrintAll();
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestHistoryParsing()
		{
			Log.Info("STARTING testHistoryParsing()");
			try
			{
				CheckHistoryParsing(2, 1, 2);
			}
			finally
			{
				Log.Info("FINISHED testHistoryParsing()");
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestHistoryParsingWithParseErrors()
		{
			Log.Info("STARTING testHistoryParsingWithParseErrors()");
			try
			{
				CheckHistoryParsing(3, 0, 2);
			}
			finally
			{
				Log.Info("FINISHED testHistoryParsingWithParseErrors()");
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private static string GetJobSummary(FileContext fc, Path path)
		{
			Path qPath = fc.MakeQualified(path);
			FSDataInputStream @in = fc.Open(qPath);
			string jobSummaryString = @in.ReadUTF();
			@in.Close();
			return jobSummaryString;
		}

		/// <exception cref="System.Exception"/>
		private void CheckHistoryParsing(int numMaps, int numReduces, int numSuccessfulMaps
			)
		{
			Configuration conf = new Configuration();
			conf.Set(MRJobConfig.UserName, Runtime.GetProperty("user.name"));
			long amStartTimeEst = Runtime.CurrentTimeMillis();
			conf.SetClass(CommonConfigurationKeysPublic.NetTopologyNodeSwitchMappingImplKey, 
				typeof(TestJobHistoryParsing.MyResolver), typeof(DNSToSwitchMapping));
			RackResolver.Init(conf);
			MRApp app = new TestJobHistoryEvents.MRAppWithHistory(numMaps, numReduces, true, 
				this.GetType().FullName, true);
			app.Submit(conf);
			Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job = app.GetContext().GetAllJobs().Values
				.GetEnumerator().Next();
			JobId jobId = job.GetID();
			Log.Info("JOBID is " + TypeConverter.FromYarn(jobId).ToString());
			app.WaitForState(job, JobState.Succeeded);
			// make sure all events are flushed
			app.WaitForState(Service.STATE.Stopped);
			string jobhistoryDir = JobHistoryUtils.GetHistoryIntermediateDoneDirForUser(conf);
			FileContext fc = null;
			try
			{
				fc = FileContext.GetFileContext(conf);
			}
			catch (IOException ioe)
			{
				Log.Info("Can not get FileContext", ioe);
				throw (new Exception("Can not get File Context"));
			}
			if (numMaps == numSuccessfulMaps)
			{
				string summaryFileName = JobHistoryUtils.GetIntermediateSummaryFileName(jobId);
				Path summaryFile = new Path(jobhistoryDir, summaryFileName);
				string jobSummaryString = GetJobSummary(fc, summaryFile);
				NUnit.Framework.Assert.IsNotNull(jobSummaryString);
				NUnit.Framework.Assert.IsTrue(jobSummaryString.Contains("resourcesPerMap=100"));
				NUnit.Framework.Assert.IsTrue(jobSummaryString.Contains("resourcesPerReduce=100")
					);
				IDictionary<string, string> jobSummaryElements = new Dictionary<string, string>();
				StringTokenizer strToken = new StringTokenizer(jobSummaryString, ",");
				while (strToken.HasMoreTokens())
				{
					string keypair = strToken.NextToken();
					jobSummaryElements[keypair.Split("=")[0]] = keypair.Split("=")[1];
				}
				NUnit.Framework.Assert.AreEqual("JobId does not match", jobId.ToString(), jobSummaryElements
					["jobId"]);
				NUnit.Framework.Assert.AreEqual("JobName does not match", "test", jobSummaryElements
					["jobName"]);
				NUnit.Framework.Assert.IsTrue("submitTime should not be 0", long.Parse(jobSummaryElements
					["submitTime"]) != 0);
				NUnit.Framework.Assert.IsTrue("launchTime should not be 0", long.Parse(jobSummaryElements
					["launchTime"]) != 0);
				NUnit.Framework.Assert.IsTrue("firstMapTaskLaunchTime should not be 0", long.Parse
					(jobSummaryElements["firstMapTaskLaunchTime"]) != 0);
				NUnit.Framework.Assert.IsTrue("firstReduceTaskLaunchTime should not be 0", long.Parse
					(jobSummaryElements["firstReduceTaskLaunchTime"]) != 0);
				NUnit.Framework.Assert.IsTrue("finishTime should not be 0", long.Parse(jobSummaryElements
					["finishTime"]) != 0);
				NUnit.Framework.Assert.AreEqual("Mismatch in num map slots", numSuccessfulMaps, System.Convert.ToInt32
					(jobSummaryElements["numMaps"]));
				NUnit.Framework.Assert.AreEqual("Mismatch in num reduce slots", numReduces, System.Convert.ToInt32
					(jobSummaryElements["numReduces"]));
				NUnit.Framework.Assert.AreEqual("User does not match", Runtime.GetProperty("user.name"
					), jobSummaryElements["user"]);
				NUnit.Framework.Assert.AreEqual("Queue does not match", "default", jobSummaryElements
					["queue"]);
				NUnit.Framework.Assert.AreEqual("Status does not match", "SUCCEEDED", jobSummaryElements
					["status"]);
			}
			JobHistory jobHistory = new JobHistory();
			jobHistory.Init(conf);
			HistoryFileManager.HistoryFileInfo fileInfo = jobHistory.GetJobFileInfo(jobId);
			JobHistoryParser.JobInfo jobInfo;
			long numFinishedMaps;
			lock (fileInfo)
			{
				Path historyFilePath = fileInfo.GetHistoryFile();
				FSDataInputStream @in = null;
				Log.Info("JobHistoryFile is: " + historyFilePath);
				try
				{
					@in = fc.Open(fc.MakeQualified(historyFilePath));
				}
				catch (IOException ioe)
				{
					Log.Info("Can not open history file: " + historyFilePath, ioe);
					throw (new Exception("Can not open History File"));
				}
				JobHistoryParser parser = new JobHistoryParser(@in);
				EventReader realReader = new EventReader(@in);
				EventReader reader = Org.Mockito.Mockito.Mock<EventReader>();
				if (numMaps == numSuccessfulMaps)
				{
					reader = realReader;
				}
				else
				{
					AtomicInteger numFinishedEvents = new AtomicInteger(0);
					// Hack!
					Org.Mockito.Mockito.When(reader.GetNextEvent()).ThenAnswer(new _Answer_257(realReader
						, numFinishedEvents, numSuccessfulMaps));
				}
				jobInfo = parser.Parse(reader);
				numFinishedMaps = ComputeFinishedMaps(jobInfo, numMaps, numSuccessfulMaps);
				if (numFinishedMaps != numMaps)
				{
					Exception parseException = parser.GetParseException();
					NUnit.Framework.Assert.IsNotNull("Didn't get expected parse exception", parseException
						);
				}
			}
			NUnit.Framework.Assert.AreEqual("Incorrect username ", Runtime.GetProperty("user.name"
				), jobInfo.GetUsername());
			NUnit.Framework.Assert.AreEqual("Incorrect jobName ", "test", jobInfo.GetJobname(
				));
			NUnit.Framework.Assert.AreEqual("Incorrect queuename ", "default", jobInfo.GetJobQueueName
				());
			NUnit.Framework.Assert.AreEqual("incorrect conf path", "test", jobInfo.GetJobConfPath
				());
			NUnit.Framework.Assert.AreEqual("incorrect finishedMap ", numSuccessfulMaps, numFinishedMaps
				);
			NUnit.Framework.Assert.AreEqual("incorrect finishedReduces ", numReduces, jobInfo
				.GetFinishedReduces());
			NUnit.Framework.Assert.AreEqual("incorrect uberized ", job.IsUber(), jobInfo.GetUberized
				());
			IDictionary<TaskID, JobHistoryParser.TaskInfo> allTasks = jobInfo.GetAllTasks();
			int totalTasks = allTasks.Count;
			NUnit.Framework.Assert.AreEqual("total number of tasks is incorrect  ", (numMaps 
				+ numReduces), totalTasks);
			// Verify aminfo
			NUnit.Framework.Assert.AreEqual(1, jobInfo.GetAMInfos().Count);
			NUnit.Framework.Assert.AreEqual(MRApp.NmHost, jobInfo.GetAMInfos()[0].GetNodeManagerHost
				());
			JobHistoryParser.AMInfo amInfo = jobInfo.GetAMInfos()[0];
			NUnit.Framework.Assert.AreEqual(MRApp.NmPort, amInfo.GetNodeManagerPort());
			NUnit.Framework.Assert.AreEqual(MRApp.NmHttpPort, amInfo.GetNodeManagerHttpPort()
				);
			NUnit.Framework.Assert.AreEqual(1, amInfo.GetAppAttemptId().GetAttemptId());
			NUnit.Framework.Assert.AreEqual(amInfo.GetAppAttemptId(), amInfo.GetContainerId()
				.GetApplicationAttemptId());
			NUnit.Framework.Assert.IsTrue(amInfo.GetStartTime() <= Runtime.CurrentTimeMillis(
				) && amInfo.GetStartTime() >= amStartTimeEst);
			ContainerId fakeCid = MRApp.NewContainerId(-1, -1, -1, -1);
			// Assert at taskAttempt level
			foreach (JobHistoryParser.TaskInfo taskInfo in allTasks.Values)
			{
				int taskAttemptCount = taskInfo.GetAllTaskAttempts().Count;
				NUnit.Framework.Assert.AreEqual("total number of task attempts ", 1, taskAttemptCount
					);
				JobHistoryParser.TaskAttemptInfo taInfo = taskInfo.GetAllTaskAttempts().Values.GetEnumerator
					().Next();
				NUnit.Framework.Assert.IsNotNull(taInfo.GetContainerId());
				// Verify the wrong ctor is not being used. Remove after mrv1 is removed.
				NUnit.Framework.Assert.IsFalse(taInfo.GetContainerId().Equals(fakeCid));
			}
			// Deep compare Job and JobInfo
			foreach (Task task in job.GetTasks().Values)
			{
				JobHistoryParser.TaskInfo taskInfo_1 = allTasks[TypeConverter.FromYarn(task.GetID
					())];
				NUnit.Framework.Assert.IsNotNull("TaskInfo not found", taskInfo_1);
				foreach (TaskAttempt taskAttempt in task.GetAttempts().Values)
				{
					JobHistoryParser.TaskAttemptInfo taskAttemptInfo = taskInfo_1.GetAllTaskAttempts(
						)[TypeConverter.FromYarn((taskAttempt.GetID()))];
					NUnit.Framework.Assert.IsNotNull("TaskAttemptInfo not found", taskAttemptInfo);
					NUnit.Framework.Assert.AreEqual("Incorrect shuffle port for task attempt", taskAttempt
						.GetShufflePort(), taskAttemptInfo.GetShufflePort());
					if (numMaps == numSuccessfulMaps)
					{
						NUnit.Framework.Assert.AreEqual(MRApp.NmHost, taskAttemptInfo.GetHostname());
						NUnit.Framework.Assert.AreEqual(MRApp.NmPort, taskAttemptInfo.GetPort());
						// Verify rack-name
						NUnit.Framework.Assert.AreEqual("rack-name is incorrect", taskAttemptInfo.GetRackname
							(), RackName);
					}
				}
			}
			// test output for HistoryViewer
			TextWriter stdps = System.Console.Out;
			try
			{
				Runtime.SetOut(new TextWriter(outContent));
				HistoryViewer viewer;
				lock (fileInfo)
				{
					viewer = new HistoryViewer(fc.MakeQualified(fileInfo.GetHistoryFile()).ToString()
						, conf, true);
				}
				viewer.Print();
				foreach (JobHistoryParser.TaskInfo taskInfo_1 in allTasks.Values)
				{
					string test = (taskInfo_1.GetTaskStatus() == null ? string.Empty : taskInfo_1.GetTaskStatus
						()) + " " + taskInfo_1.GetTaskType() + " task list for " + taskInfo_1.GetTaskId(
						).GetJobID();
					NUnit.Framework.Assert.IsTrue(outContent.ToString().IndexOf(test) > 0);
					NUnit.Framework.Assert.IsTrue(outContent.ToString().IndexOf(taskInfo_1.GetTaskId(
						).ToString()) > 0);
				}
			}
			finally
			{
				Runtime.SetOut(stdps);
			}
		}

		private sealed class _Answer_257 : Answer<HistoryEvent>
		{
			public _Answer_257(EventReader realReader, AtomicInteger numFinishedEvents, int numSuccessfulMaps
				)
			{
				this.realReader = realReader;
				this.numFinishedEvents = numFinishedEvents;
				this.numSuccessfulMaps = numSuccessfulMaps;
			}

			/// <exception cref="System.IO.IOException"/>
			public HistoryEvent Answer(InvocationOnMock invocation)
			{
				HistoryEvent @event = realReader.GetNextEvent();
				if (@event is TaskFinishedEvent)
				{
					numFinishedEvents.IncrementAndGet();
				}
				if (numFinishedEvents.Get() <= numSuccessfulMaps)
				{
					return @event;
				}
				else
				{
					throw new IOException("test");
				}
			}

			private readonly EventReader realReader;

			private readonly AtomicInteger numFinishedEvents;

			private readonly int numSuccessfulMaps;
		}

		// Computes finished maps similar to RecoveryService...
		private long ComputeFinishedMaps(JobHistoryParser.JobInfo jobInfo, int numMaps, int
			 numSuccessfulMaps)
		{
			if (numMaps == numSuccessfulMaps)
			{
				return jobInfo.GetFinishedMaps();
			}
			long numFinishedMaps = 0;
			IDictionary<TaskID, JobHistoryParser.TaskInfo> taskInfos = jobInfo.GetAllTasks();
			foreach (JobHistoryParser.TaskInfo taskInfo in taskInfos.Values)
			{
				if (TaskState.Succeeded.ToString().Equals(taskInfo.GetTaskStatus()))
				{
					++numFinishedMaps;
				}
			}
			return numFinishedMaps;
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestHistoryParsingForFailedAttempts()
		{
			Log.Info("STARTING testHistoryParsingForFailedAttempts");
			try
			{
				Configuration conf = new Configuration();
				conf.SetClass(CommonConfigurationKeysPublic.NetTopologyNodeSwitchMappingImplKey, 
					typeof(TestJobHistoryParsing.MyResolver), typeof(DNSToSwitchMapping));
				RackResolver.Init(conf);
				MRApp app = new TestJobHistoryParsing.MRAppWithHistoryWithFailedAttempt(2, 1, true
					, this.GetType().FullName, true);
				app.Submit(conf);
				Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job = app.GetContext().GetAllJobs().Values
					.GetEnumerator().Next();
				JobId jobId = job.GetID();
				app.WaitForState(job, JobState.Succeeded);
				// make sure all events are flushed
				app.WaitForState(Service.STATE.Stopped);
				JobHistory jobHistory = new JobHistory();
				jobHistory.Init(conf);
				HistoryFileManager.HistoryFileInfo fileInfo = jobHistory.GetJobFileInfo(jobId);
				JobHistoryParser parser;
				JobHistoryParser.JobInfo jobInfo;
				lock (fileInfo)
				{
					Path historyFilePath = fileInfo.GetHistoryFile();
					FSDataInputStream @in = null;
					FileContext fc = null;
					try
					{
						fc = FileContext.GetFileContext(conf);
						@in = fc.Open(fc.MakeQualified(historyFilePath));
					}
					catch (IOException ioe)
					{
						Log.Info("Can not open history file: " + historyFilePath, ioe);
						throw (new Exception("Can not open History File"));
					}
					parser = new JobHistoryParser(@in);
					jobInfo = parser.Parse();
				}
				Exception parseException = parser.GetParseException();
				NUnit.Framework.Assert.IsNull("Caught an expected exception " + parseException, parseException
					);
				int noOffailedAttempts = 0;
				IDictionary<TaskID, JobHistoryParser.TaskInfo> allTasks = jobInfo.GetAllTasks();
				foreach (Task task in job.GetTasks().Values)
				{
					JobHistoryParser.TaskInfo taskInfo = allTasks[TypeConverter.FromYarn(task.GetID()
						)];
					foreach (TaskAttempt taskAttempt in task.GetAttempts().Values)
					{
						JobHistoryParser.TaskAttemptInfo taskAttemptInfo = taskInfo.GetAllTaskAttempts()[
							TypeConverter.FromYarn((taskAttempt.GetID()))];
						// Verify rack-name for all task attempts
						NUnit.Framework.Assert.AreEqual("rack-name is incorrect", taskAttemptInfo.GetRackname
							(), RackName);
						if (taskAttemptInfo.GetTaskStatus().Equals("FAILED"))
						{
							noOffailedAttempts++;
						}
					}
				}
				NUnit.Framework.Assert.AreEqual("No of Failed tasks doesn't match.", 2, noOffailedAttempts
					);
			}
			finally
			{
				Log.Info("FINISHED testHistoryParsingForFailedAttempts");
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestCountersForFailedTask()
		{
			Log.Info("STARTING testCountersForFailedTask");
			try
			{
				Configuration conf = new Configuration();
				conf.SetClass(CommonConfigurationKeysPublic.NetTopologyNodeSwitchMappingImplKey, 
					typeof(TestJobHistoryParsing.MyResolver), typeof(DNSToSwitchMapping));
				RackResolver.Init(conf);
				MRApp app = new TestJobHistoryParsing.MRAppWithHistoryWithFailedTask(2, 1, true, 
					this.GetType().FullName, true);
				app.Submit(conf);
				Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job = app.GetContext().GetAllJobs().Values
					.GetEnumerator().Next();
				JobId jobId = job.GetID();
				app.WaitForState(job, JobState.Failed);
				// make sure all events are flushed
				app.WaitForState(Service.STATE.Stopped);
				JobHistory jobHistory = new JobHistory();
				jobHistory.Init(conf);
				HistoryFileManager.HistoryFileInfo fileInfo = jobHistory.GetJobFileInfo(jobId);
				JobHistoryParser parser;
				JobHistoryParser.JobInfo jobInfo;
				lock (fileInfo)
				{
					Path historyFilePath = fileInfo.GetHistoryFile();
					FSDataInputStream @in = null;
					FileContext fc = null;
					try
					{
						fc = FileContext.GetFileContext(conf);
						@in = fc.Open(fc.MakeQualified(historyFilePath));
					}
					catch (IOException ioe)
					{
						Log.Info("Can not open history file: " + historyFilePath, ioe);
						throw (new Exception("Can not open History File"));
					}
					parser = new JobHistoryParser(@in);
					jobInfo = parser.Parse();
				}
				Exception parseException = parser.GetParseException();
				NUnit.Framework.Assert.IsNull("Caught an expected exception " + parseException, parseException
					);
				foreach (KeyValuePair<TaskID, JobHistoryParser.TaskInfo> entry in jobInfo.GetAllTasks
					())
				{
					TaskId yarnTaskID = TypeConverter.ToYarn(entry.Key);
					CompletedTask ct = new CompletedTask(yarnTaskID, entry.Value);
					NUnit.Framework.Assert.IsNotNull("completed task report has null counters", ct.GetReport
						().GetCounters());
				}
				IList<string> originalDiagnostics = job.GetDiagnostics();
				string historyError = jobInfo.GetErrorInfo();
				NUnit.Framework.Assert.IsTrue("No original diagnostics for a failed job", originalDiagnostics
					 != null && !originalDiagnostics.IsEmpty());
				NUnit.Framework.Assert.IsNotNull("No history error info for a failed job ", historyError
					);
				foreach (string diagString in originalDiagnostics)
				{
					NUnit.Framework.Assert.IsTrue(historyError.Contains(diagString));
				}
			}
			finally
			{
				Log.Info("FINISHED testCountersForFailedTask");
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestDiagnosticsForKilledJob()
		{
			Log.Info("STARTING testDiagnosticsForKilledJob");
			try
			{
				Configuration conf = new Configuration();
				conf.SetClass(CommonConfigurationKeysPublic.NetTopologyNodeSwitchMappingImplKey, 
					typeof(TestJobHistoryParsing.MyResolver), typeof(DNSToSwitchMapping));
				RackResolver.Init(conf);
				MRApp app = new TestJobHistoryParsing.MRAppWithHistoryWithJobKilled(2, 1, true, this
					.GetType().FullName, true);
				app.Submit(conf);
				Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job = app.GetContext().GetAllJobs().Values
					.GetEnumerator().Next();
				JobId jobId = job.GetID();
				app.WaitForState(job, JobState.Killed);
				// make sure all events are flushed
				app.WaitForState(Service.STATE.Stopped);
				JobHistory jobHistory = new JobHistory();
				jobHistory.Init(conf);
				HistoryFileManager.HistoryFileInfo fileInfo = jobHistory.GetJobFileInfo(jobId);
				JobHistoryParser parser;
				JobHistoryParser.JobInfo jobInfo;
				lock (fileInfo)
				{
					Path historyFilePath = fileInfo.GetHistoryFile();
					FSDataInputStream @in = null;
					FileContext fc = null;
					try
					{
						fc = FileContext.GetFileContext(conf);
						@in = fc.Open(fc.MakeQualified(historyFilePath));
					}
					catch (IOException ioe)
					{
						Log.Info("Can not open history file: " + historyFilePath, ioe);
						throw (new Exception("Can not open History File"));
					}
					parser = new JobHistoryParser(@in);
					jobInfo = parser.Parse();
				}
				Exception parseException = parser.GetParseException();
				NUnit.Framework.Assert.IsNull("Caught an expected exception " + parseException, parseException
					);
				IList<string> originalDiagnostics = job.GetDiagnostics();
				string historyError = jobInfo.GetErrorInfo();
				NUnit.Framework.Assert.IsTrue("No original diagnostics for a failed job", originalDiagnostics
					 != null && !originalDiagnostics.IsEmpty());
				NUnit.Framework.Assert.IsNotNull("No history error info for a failed job ", historyError
					);
				foreach (string diagString in originalDiagnostics)
				{
					NUnit.Framework.Assert.IsTrue(historyError.Contains(diagString));
				}
				NUnit.Framework.Assert.IsTrue("No killed message in diagnostics", historyError.Contains
					(JobImpl.JobKilledDiag));
			}
			finally
			{
				Log.Info("FINISHED testDiagnosticsForKilledJob");
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestScanningOldDirs()
		{
			Log.Info("STARTING testScanningOldDirs");
			try
			{
				Configuration conf = new Configuration();
				conf.SetClass(CommonConfigurationKeysPublic.NetTopologyNodeSwitchMappingImplKey, 
					typeof(TestJobHistoryParsing.MyResolver), typeof(DNSToSwitchMapping));
				RackResolver.Init(conf);
				MRApp app = new TestJobHistoryEvents.MRAppWithHistory(1, 1, true, this.GetType().
					FullName, true);
				app.Submit(conf);
				Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job = app.GetContext().GetAllJobs().Values
					.GetEnumerator().Next();
				JobId jobId = job.GetID();
				Log.Info("JOBID is " + TypeConverter.FromYarn(jobId).ToString());
				app.WaitForState(job, JobState.Succeeded);
				// make sure all events are flushed
				app.WaitForState(Service.STATE.Stopped);
				TestJobHistoryParsing.HistoryFileManagerForTest hfm = new TestJobHistoryParsing.HistoryFileManagerForTest
					();
				hfm.Init(conf);
				HistoryFileManager.HistoryFileInfo fileInfo = hfm.GetFileInfo(jobId);
				NUnit.Framework.Assert.IsNotNull("Unable to locate job history", fileInfo);
				// force the manager to "forget" the job
				hfm.DeleteJobFromJobListCache(fileInfo);
				int msecPerSleep = 10;
				int msecToSleep = 10 * 1000;
				while (fileInfo.IsMovePending() && msecToSleep > 0)
				{
					NUnit.Framework.Assert.IsTrue(!fileInfo.DidMoveFail());
					msecToSleep -= msecPerSleep;
					Sharpen.Thread.Sleep(msecPerSleep);
				}
				NUnit.Framework.Assert.IsTrue("Timeout waiting for history move", msecToSleep > 0
					);
				fileInfo = hfm.GetFileInfo(jobId);
				hfm.Stop();
				NUnit.Framework.Assert.IsNotNull("Unable to locate old job history", fileInfo);
				NUnit.Framework.Assert.IsTrue("HistoryFileManager not shutdown properly", hfm.moveToDoneExecutor
					.IsTerminated());
			}
			finally
			{
				Log.Info("FINISHED testScanningOldDirs");
			}
		}

		internal class MRAppWithHistoryWithFailedAttempt : TestJobHistoryEvents.MRAppWithHistory
		{
			public MRAppWithHistoryWithFailedAttempt(int maps, int reduces, bool autoComplete
				, string testName, bool cleanOnStart)
				: base(maps, reduces, autoComplete, testName, cleanOnStart)
			{
			}

			protected override void AttemptLaunched(TaskAttemptId attemptID)
			{
				if (attemptID.GetTaskId().GetId() == 0 && attemptID.GetId() == 0)
				{
					GetContext().GetEventHandler().Handle(new TaskAttemptEvent(attemptID, TaskAttemptEventType
						.TaFailmsg));
				}
				else
				{
					GetContext().GetEventHandler().Handle(new TaskAttemptEvent(attemptID, TaskAttemptEventType
						.TaDone));
				}
			}
		}

		internal class MRAppWithHistoryWithFailedTask : TestJobHistoryEvents.MRAppWithHistory
		{
			public MRAppWithHistoryWithFailedTask(int maps, int reduces, bool autoComplete, string
				 testName, bool cleanOnStart)
				: base(maps, reduces, autoComplete, testName, cleanOnStart)
			{
			}

			protected override void AttemptLaunched(TaskAttemptId attemptID)
			{
				if (attemptID.GetTaskId().GetId() == 0)
				{
					GetContext().GetEventHandler().Handle(new TaskAttemptEvent(attemptID, TaskAttemptEventType
						.TaFailmsg));
				}
				else
				{
					GetContext().GetEventHandler().Handle(new TaskAttemptEvent(attemptID, TaskAttemptEventType
						.TaDone));
				}
			}
		}

		internal class MRAppWithHistoryWithJobKilled : TestJobHistoryEvents.MRAppWithHistory
		{
			public MRAppWithHistoryWithJobKilled(int maps, int reduces, bool autoComplete, string
				 testName, bool cleanOnStart)
				: base(maps, reduces, autoComplete, testName, cleanOnStart)
			{
			}

			protected override void AttemptLaunched(TaskAttemptId attemptID)
			{
				if (attemptID.GetTaskId().GetId() == 0)
				{
					GetContext().GetEventHandler().Handle(new JobEvent(attemptID.GetTaskId().GetJobId
						(), JobEventType.JobKill));
				}
				else
				{
					GetContext().GetEventHandler().Handle(new TaskAttemptEvent(attemptID, TaskAttemptEventType
						.TaDone));
				}
			}
		}

		internal class HistoryFileManagerForTest : HistoryFileManager
		{
			internal virtual void DeleteJobFromJobListCache(HistoryFileManager.HistoryFileInfo
				 fileInfo)
			{
				jobListCache.Delete(fileInfo);
			}
		}

		/// <exception cref="System.Exception"/>
		public static void Main(string[] args)
		{
			TestJobHistoryParsing t = new TestJobHistoryParsing();
			t.TestHistoryParsing();
			t.TestHistoryParsingForFailedAttempts();
		}

		/// <summary>test clean old history files.</summary>
		/// <remarks>
		/// test clean old history files. Files should be deleted after 1 week by
		/// default.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		public virtual void TestDeleteFileInfo()
		{
			Log.Info("STARTING testDeleteFileInfo");
			try
			{
				Configuration conf = new Configuration();
				conf.SetClass(CommonConfigurationKeysPublic.NetTopologyNodeSwitchMappingImplKey, 
					typeof(TestJobHistoryParsing.MyResolver), typeof(DNSToSwitchMapping));
				RackResolver.Init(conf);
				MRApp app = new TestJobHistoryEvents.MRAppWithHistory(1, 1, true, this.GetType().
					FullName, true);
				app.Submit(conf);
				Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job = app.GetContext().GetAllJobs().Values
					.GetEnumerator().Next();
				JobId jobId = job.GetID();
				app.WaitForState(job, JobState.Succeeded);
				// make sure all events are flushed
				app.WaitForState(Service.STATE.Stopped);
				HistoryFileManager hfm = new HistoryFileManager();
				hfm.Init(conf);
				HistoryFileManager.HistoryFileInfo fileInfo = hfm.GetFileInfo(jobId);
				hfm.InitExisting();
				// wait for move files form the done_intermediate directory to the gone
				// directory
				while (fileInfo.IsMovePending())
				{
					Sharpen.Thread.Sleep(300);
				}
				NUnit.Framework.Assert.IsNotNull(hfm.jobListCache.Values());
				// try to remove fileInfo
				hfm.Clean();
				// check that fileInfo does not deleted
				NUnit.Framework.Assert.IsFalse(fileInfo.IsDeleted());
				// correct live time
				hfm.SetMaxHistoryAge(-1);
				hfm.Clean();
				hfm.Stop();
				NUnit.Framework.Assert.IsTrue("Thread pool shutdown", hfm.moveToDoneExecutor.IsTerminated
					());
				// should be deleted !
				NUnit.Framework.Assert.IsTrue("file should be deleted ", fileInfo.IsDeleted());
			}
			finally
			{
				Log.Info("FINISHED testDeleteFileInfo");
			}
		}

		/// <summary>Simple test some methods of JobHistory</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestJobHistoryMethods()
		{
			Log.Info("STARTING testJobHistoryMethods");
			try
			{
				Configuration configuration = new Configuration();
				configuration.SetClass(CommonConfigurationKeysPublic.NetTopologyNodeSwitchMappingImplKey
					, typeof(TestJobHistoryParsing.MyResolver), typeof(DNSToSwitchMapping));
				RackResolver.Init(configuration);
				MRApp app = new TestJobHistoryEvents.MRAppWithHistory(1, 1, true, this.GetType().
					FullName, true);
				app.Submit(configuration);
				Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job = app.GetContext().GetAllJobs().Values
					.GetEnumerator().Next();
				app.WaitForState(job, JobState.Succeeded);
				JobHistory jobHistory = new JobHistory();
				jobHistory.Init(configuration);
				// Method getAllJobs
				NUnit.Framework.Assert.AreEqual(1, jobHistory.GetAllJobs().Count);
				// and with ApplicationId
				NUnit.Framework.Assert.AreEqual(1, jobHistory.GetAllJobs(app.GetAppID()).Count);
				JobsInfo jobsinfo = jobHistory.GetPartialJobs(0L, 10L, null, "default", 0L, Runtime
					.CurrentTimeMillis() + 1, 0L, Runtime.CurrentTimeMillis() + 1, JobState.Succeeded
					);
				NUnit.Framework.Assert.AreEqual(1, jobsinfo.GetJobs().Count);
				NUnit.Framework.Assert.IsNotNull(jobHistory.GetApplicationAttemptId());
				// test Application Id
				NUnit.Framework.Assert.AreEqual("application_0_0000", jobHistory.GetApplicationID
					().ToString());
				NUnit.Framework.Assert.AreEqual("Job History Server", jobHistory.GetApplicationName
					());
				// method does not work
				NUnit.Framework.Assert.IsNull(jobHistory.GetEventHandler());
				// method does not work
				NUnit.Framework.Assert.IsNull(jobHistory.GetClock());
				// method does not work
				NUnit.Framework.Assert.IsNull(jobHistory.GetClusterInfo());
			}
			finally
			{
				Log.Info("FINISHED testJobHistoryMethods");
			}
		}

		/// <summary>Simple test PartialJob</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestPartialJob()
		{
			JobId jobId = new JobIdPBImpl();
			jobId.SetId(0);
			JobIndexInfo jii = new JobIndexInfo(0L, Runtime.CurrentTimeMillis(), "user", "jobName"
				, jobId, 3, 2, "JobStatus");
			PartialJob test = new PartialJob(jii, jobId);
			NUnit.Framework.Assert.AreEqual(1.0f, test.GetProgress(), 0.001);
			NUnit.Framework.Assert.IsNull(test.GetAllCounters());
			NUnit.Framework.Assert.IsNull(test.GetTasks());
			NUnit.Framework.Assert.IsNull(test.GetTasks(TaskType.Map));
			NUnit.Framework.Assert.IsNull(test.GetTask(new TaskIdPBImpl()));
			NUnit.Framework.Assert.IsNull(test.GetTaskAttemptCompletionEvents(0, 100));
			NUnit.Framework.Assert.IsNull(test.GetMapAttemptCompletionEvents(0, 100));
			NUnit.Framework.Assert.IsTrue(test.CheckAccess(UserGroupInformation.GetCurrentUser
				(), null));
			NUnit.Framework.Assert.IsNull(test.GetAMInfos());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestMultipleFailedTasks()
		{
			JobHistoryParser parser = new JobHistoryParser(Org.Mockito.Mockito.Mock<FSDataInputStream
				>());
			EventReader reader = Org.Mockito.Mockito.Mock<EventReader>();
			AtomicInteger numEventsRead = new AtomicInteger(0);
			// Hack!
			TaskType taskType = TaskType.Map;
			TaskID[] tids = new TaskID[2];
			JobID jid = new JobID("1", 1);
			tids[0] = new TaskID(jid, taskType, 0);
			tids[1] = new TaskID(jid, taskType, 1);
			Org.Mockito.Mockito.When(reader.GetNextEvent()).ThenAnswer(new _Answer_842(numEventsRead
				, tids, taskType, jid));
			// send two task start and two task fail events for tasks 0 and 1
			JobHistoryParser.JobInfo info = parser.Parse(reader);
			NUnit.Framework.Assert.IsTrue("Task 0 not implicated", info.GetErrorInfo().Contains
				(tids[0].ToString()));
		}

		private sealed class _Answer_842 : Answer<HistoryEvent>
		{
			public _Answer_842(AtomicInteger numEventsRead, TaskID[] tids, TaskType taskType, 
				JobID jid)
			{
				this.numEventsRead = numEventsRead;
				this.tids = tids;
				this.taskType = taskType;
				this.jid = jid;
			}

			/// <exception cref="System.IO.IOException"/>
			public HistoryEvent Answer(InvocationOnMock invocation)
			{
				int eventId = numEventsRead.GetAndIncrement();
				TaskID tid = tids[eventId & unchecked((int)(0x1))];
				if (eventId < 2)
				{
					return new TaskStartedEvent(tid, 0, taskType, string.Empty);
				}
				if (eventId < 4)
				{
					TaskFailedEvent tfe = new TaskFailedEvent(tid, 0, taskType, "failed", "FAILED", null
						, new Counters());
					tfe.SetDatum(tfe.GetDatum());
					return tfe;
				}
				if (eventId < 5)
				{
					JobUnsuccessfulCompletionEvent juce = new JobUnsuccessfulCompletionEvent(jid, 100L
						, 2, 0, "JOB_FAILED", Sharpen.Collections.SingletonList("Task failed: " + tids[0
						].ToString()));
					return juce;
				}
				return null;
			}

			private readonly AtomicInteger numEventsRead;

			private readonly TaskID[] tids;

			private readonly TaskType taskType;

			private readonly JobID jid;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestFailedJobHistoryWithoutDiagnostics()
		{
			Path histPath = new Path(GetType().GetClassLoader().GetResource("job_1393307629410_0001-1393307687476-user-Sleep+job-1393307723835-0-0-FAILED-default-1393307693920.jhist"
				).GetFile());
			FileSystem lfs = FileSystem.GetLocal(new Configuration());
			FSDataInputStream fsdis = lfs.Open(histPath);
			try
			{
				JobHistoryParser parser = new JobHistoryParser(fsdis);
				JobHistoryParser.JobInfo info = parser.Parse();
				NUnit.Framework.Assert.AreEqual("History parsed jobId incorrectly", info.GetJobId
					(), JobID.ForName("job_1393307629410_0001"));
				NUnit.Framework.Assert.AreEqual("Default diagnostics incorrect ", string.Empty, info
					.GetErrorInfo());
			}
			finally
			{
				fsdis.Close();
			}
		}

		/// <summary>Test compatibility of JobHistoryParser with 2.0.3-alpha history files</summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestTaskAttemptUnsuccessfulCompletionWithoutCounters203()
		{
			Path histPath = new Path(GetType().GetClassLoader().GetResource("job_2.0.3-alpha-FAILED.jhist"
				).GetFile());
			JobHistoryParser parser = new JobHistoryParser(FileSystem.GetLocal(new Configuration
				()), histPath);
			JobHistoryParser.JobInfo jobInfo = parser.Parse();
			Log.Info(" job info: " + jobInfo.GetJobname() + " " + jobInfo.GetFinishedMaps() +
				 " " + jobInfo.GetTotalMaps() + " " + jobInfo.GetJobId());
		}

		/// <summary>Test compatibility of JobHistoryParser with 2.4.0 history files</summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestTaskAttemptUnsuccessfulCompletionWithoutCounters240()
		{
			Path histPath = new Path(GetType().GetClassLoader().GetResource("job_2.4.0-FAILED.jhist"
				).GetFile());
			JobHistoryParser parser = new JobHistoryParser(FileSystem.GetLocal(new Configuration
				()), histPath);
			JobHistoryParser.JobInfo jobInfo = parser.Parse();
			Log.Info(" job info: " + jobInfo.GetJobname() + " " + jobInfo.GetFinishedMaps() +
				 " " + jobInfo.GetTotalMaps() + " " + jobInfo.GetJobId());
		}

		/// <summary>Test compatibility of JobHistoryParser with 0.23.9 history files</summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestTaskAttemptUnsuccessfulCompletionWithoutCounters0239()
		{
			Path histPath = new Path(GetType().GetClassLoader().GetResource("job_0.23.9-FAILED.jhist"
				).GetFile());
			JobHistoryParser parser = new JobHistoryParser(FileSystem.GetLocal(new Configuration
				()), histPath);
			JobHistoryParser.JobInfo jobInfo = parser.Parse();
			Log.Info(" job info: " + jobInfo.GetJobname() + " " + jobInfo.GetFinishedMaps() +
				 " " + jobInfo.GetTotalMaps() + " " + jobInfo.GetJobId());
		}
	}
}
