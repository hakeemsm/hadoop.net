using System.Collections.Generic;
using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.V2.Api;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Protocolrecords;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Mapreduce.V2.App;
using Org.Apache.Hadoop.Mapreduce.V2.App.Job;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Service;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn.Factories;
using Org.Apache.Hadoop.Yarn.Factory.Providers;
using Org.Apache.Hadoop.Yarn.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.HS
{
	public class TestJobHistoryServer
	{
		private static RecordFactory recordFactory = RecordFactoryProvider.GetRecordFactory
			(null);

		internal JobHistoryServer historyServer = null;

		/*
		test JobHistoryServer protocols....
		*/
		// simple test init/start/stop   JobHistoryServer. Status should change.
		/// <exception cref="System.Exception"/>
		public virtual void TestStartStopServer()
		{
			historyServer = new JobHistoryServer();
			Configuration config = new Configuration();
			historyServer.Init(config);
			NUnit.Framework.Assert.AreEqual(Service.STATE.Inited, historyServer.GetServiceState
				());
			NUnit.Framework.Assert.AreEqual(6, historyServer.GetServices().Count);
			HistoryClientService historyService = historyServer.GetClientService();
			NUnit.Framework.Assert.IsNotNull(historyServer.GetClientService());
			NUnit.Framework.Assert.AreEqual(Service.STATE.Inited, historyService.GetServiceState
				());
			historyServer.Start();
			NUnit.Framework.Assert.AreEqual(Service.STATE.Started, historyServer.GetServiceState
				());
			NUnit.Framework.Assert.AreEqual(Service.STATE.Started, historyService.GetServiceState
				());
			historyServer.Stop();
			NUnit.Framework.Assert.AreEqual(Service.STATE.Stopped, historyServer.GetServiceState
				());
			NUnit.Framework.Assert.IsNotNull(historyService.GetClientHandler().GetConnectAddress
				());
		}

		//Test reports of  JobHistoryServer. History server should get log files from  MRApp and read them
		/// <exception cref="System.Exception"/>
		public virtual void TestReports()
		{
			Configuration config = new Configuration();
			config.SetClass(CommonConfigurationKeysPublic.NetTopologyNodeSwitchMappingImplKey
				, typeof(TestJobHistoryParsing.MyResolver), typeof(DNSToSwitchMapping));
			RackResolver.Init(config);
			MRApp app = new TestJobHistoryEvents.MRAppWithHistory(1, 1, true, this.GetType().
				FullName, true);
			app.Submit(config);
			Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job = app.GetContext().GetAllJobs().Values
				.GetEnumerator().Next();
			app.WaitForState(job, JobState.Succeeded);
			historyServer = new JobHistoryServer();
			historyServer.Init(config);
			historyServer.Start();
			// search JobHistory  service
			JobHistory jobHistory = null;
			foreach (Org.Apache.Hadoop.Service.Service service in historyServer.GetServices())
			{
				if (service is JobHistory)
				{
					jobHistory = (JobHistory)service;
				}
			}
			IDictionary<JobId, Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job> jobs = jobHistory.
				GetAllJobs();
			NUnit.Framework.Assert.AreEqual(1, jobs.Count);
			NUnit.Framework.Assert.AreEqual("job_0_0000", jobs.Keys.GetEnumerator().Next().ToString
				());
			Task task = job.GetTasks().Values.GetEnumerator().Next();
			TaskAttempt attempt = task.GetAttempts().Values.GetEnumerator().Next();
			HistoryClientService historyService = historyServer.GetClientService();
			MRClientProtocol protocol = historyService.GetClientHandler();
			GetTaskAttemptReportRequest gtarRequest = recordFactory.NewRecordInstance<GetTaskAttemptReportRequest
				>();
			// test getTaskAttemptReport
			TaskAttemptId taId = attempt.GetID();
			taId.SetTaskId(task.GetID());
			taId.GetTaskId().SetJobId(job.GetID());
			gtarRequest.SetTaskAttemptId(taId);
			GetTaskAttemptReportResponse response = protocol.GetTaskAttemptReport(gtarRequest
				);
			NUnit.Framework.Assert.AreEqual("container_0_0000_01_000000", response.GetTaskAttemptReport
				().GetContainerId().ToString());
			NUnit.Framework.Assert.IsTrue(response.GetTaskAttemptReport().GetDiagnosticInfo()
				.IsEmpty());
			// counters
			NUnit.Framework.Assert.IsNotNull(response.GetTaskAttemptReport().GetCounters().GetCounter
				(TaskCounter.PhysicalMemoryBytes));
			NUnit.Framework.Assert.AreEqual(taId.ToString(), response.GetTaskAttemptReport().
				GetTaskAttemptId().ToString());
			// test getTaskReport
			GetTaskReportRequest request = recordFactory.NewRecordInstance<GetTaskReportRequest
				>();
			TaskId taskId = task.GetID();
			taskId.SetJobId(job.GetID());
			request.SetTaskId(taskId);
			GetTaskReportResponse reportResponse = protocol.GetTaskReport(request);
			NUnit.Framework.Assert.AreEqual(string.Empty, reportResponse.GetTaskReport().GetDiagnosticsList
				().GetEnumerator().Next());
			// progress
			NUnit.Framework.Assert.AreEqual(1.0f, reportResponse.GetTaskReport().GetProgress(
				), 0.01);
			// report has corrected taskId
			NUnit.Framework.Assert.AreEqual(taskId.ToString(), reportResponse.GetTaskReport()
				.GetTaskId().ToString());
			// Task state should be SUCCEEDED
			NUnit.Framework.Assert.AreEqual(TaskState.Succeeded, reportResponse.GetTaskReport
				().GetTaskState());
			// For invalid jobid, throw IOException
			GetTaskReportsRequest gtreportsRequest = recordFactory.NewRecordInstance<GetTaskReportsRequest
				>();
			gtreportsRequest.SetJobId(TypeConverter.ToYarn(JobID.ForName("job_1415730144495_0001"
				)));
			gtreportsRequest.SetTaskType(TaskType.Reduce);
			try
			{
				protocol.GetTaskReports(gtreportsRequest);
				NUnit.Framework.Assert.Fail("IOException not thrown for invalid job id");
			}
			catch (IOException)
			{
			}
			// Expected
			// test getTaskAttemptCompletionEvents
			GetTaskAttemptCompletionEventsRequest taskAttemptRequest = recordFactory.NewRecordInstance
				<GetTaskAttemptCompletionEventsRequest>();
			taskAttemptRequest.SetJobId(job.GetID());
			GetTaskAttemptCompletionEventsResponse taskAttemptCompletionEventsResponse = protocol
				.GetTaskAttemptCompletionEvents(taskAttemptRequest);
			NUnit.Framework.Assert.AreEqual(0, taskAttemptCompletionEventsResponse.GetCompletionEventCount
				());
			// test getDiagnostics
			GetDiagnosticsRequest diagnosticRequest = recordFactory.NewRecordInstance<GetDiagnosticsRequest
				>();
			diagnosticRequest.SetTaskAttemptId(taId);
			GetDiagnosticsResponse diagnosticResponse = protocol.GetDiagnostics(diagnosticRequest
				);
			// it is strange : why one empty string ?
			NUnit.Framework.Assert.AreEqual(1, diagnosticResponse.GetDiagnosticsCount());
			NUnit.Framework.Assert.AreEqual(string.Empty, diagnosticResponse.GetDiagnostics(0
				));
		}

		// test launch method
		/// <exception cref="System.Exception"/>
		public virtual void TestLaunch()
		{
			ExitUtil.DisableSystemExit();
			try
			{
				historyServer = JobHistoryServer.LaunchJobHistoryServer(new string[0]);
			}
			catch (ExitUtil.ExitException e)
			{
				NUnit.Framework.Assert.AreEqual(0, e.status);
				ExitUtil.ResetFirstExitException();
				NUnit.Framework.Assert.Fail();
			}
		}

		[TearDown]
		public virtual void Stop()
		{
			if (historyServer != null)
			{
				historyServer.Stop();
			}
		}
	}
}
