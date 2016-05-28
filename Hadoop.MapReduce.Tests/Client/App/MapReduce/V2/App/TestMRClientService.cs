using System.Collections.Generic;
using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.V2.Api;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Protocolrecords;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Mapreduce.V2.App.Client;
using Org.Apache.Hadoop.Mapreduce.V2.App.Job;
using Org.Apache.Hadoop.Mapreduce.V2.App.Job.Event;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Yarn.Factories;
using Org.Apache.Hadoop.Yarn.Factory.Providers;
using Org.Apache.Hadoop.Yarn.Ipc;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App
{
	public class TestMRClientService
	{
		private static RecordFactory recordFactory = RecordFactoryProvider.GetRecordFactory
			(null);

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void Test()
		{
			TestMRClientService.MRAppWithClientService app = new TestMRClientService.MRAppWithClientService
				(this, 1, 0, false);
			Configuration conf = new Configuration();
			Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job = app.Submit(conf);
			app.WaitForState(job, JobState.Running);
			NUnit.Framework.Assert.AreEqual("Num tasks not correct", 1, job.GetTasks().Count);
			IEnumerator<Task> it = job.GetTasks().Values.GetEnumerator();
			Task task = it.Next();
			app.WaitForState(task, TaskState.Running);
			TaskAttempt attempt = task.GetAttempts().Values.GetEnumerator().Next();
			app.WaitForState(attempt, TaskAttemptState.Running);
			// send the diagnostic
			string diagnostic1 = "Diagnostic1";
			string diagnostic2 = "Diagnostic2";
			app.GetContext().GetEventHandler().Handle(new TaskAttemptDiagnosticsUpdateEvent(attempt
				.GetID(), diagnostic1));
			// send the status update
			TaskAttemptStatusUpdateEvent.TaskAttemptStatus taskAttemptStatus = new TaskAttemptStatusUpdateEvent.TaskAttemptStatus
				();
			taskAttemptStatus.id = attempt.GetID();
			taskAttemptStatus.progress = 0.5f;
			taskAttemptStatus.stateString = "RUNNING";
			taskAttemptStatus.taskState = TaskAttemptState.Running;
			taskAttemptStatus.phase = Phase.Map;
			// send the status update
			app.GetContext().GetEventHandler().Handle(new TaskAttemptStatusUpdateEvent(attempt
				.GetID(), taskAttemptStatus));
			//verify that all object are fully populated by invoking RPCs.
			YarnRPC rpc = YarnRPC.Create(conf);
			MRClientProtocol proxy = (MRClientProtocol)rpc.GetProxy(typeof(MRClientProtocol), 
				app.clientService.GetBindAddress(), conf);
			GetCountersRequest gcRequest = recordFactory.NewRecordInstance<GetCountersRequest
				>();
			gcRequest.SetJobId(job.GetID());
			NUnit.Framework.Assert.IsNotNull("Counters is null", proxy.GetCounters(gcRequest)
				.GetCounters());
			GetJobReportRequest gjrRequest = recordFactory.NewRecordInstance<GetJobReportRequest
				>();
			gjrRequest.SetJobId(job.GetID());
			JobReport jr = proxy.GetJobReport(gjrRequest).GetJobReport();
			VerifyJobReport(jr);
			GetTaskAttemptCompletionEventsRequest gtaceRequest = recordFactory.NewRecordInstance
				<GetTaskAttemptCompletionEventsRequest>();
			gtaceRequest.SetJobId(job.GetID());
			gtaceRequest.SetFromEventId(0);
			gtaceRequest.SetMaxEvents(10);
			NUnit.Framework.Assert.IsNotNull("TaskCompletionEvents is null", proxy.GetTaskAttemptCompletionEvents
				(gtaceRequest).GetCompletionEventList());
			GetDiagnosticsRequest gdRequest = recordFactory.NewRecordInstance<GetDiagnosticsRequest
				>();
			gdRequest.SetTaskAttemptId(attempt.GetID());
			NUnit.Framework.Assert.IsNotNull("Diagnostics is null", proxy.GetDiagnostics(gdRequest
				).GetDiagnosticsList());
			GetTaskAttemptReportRequest gtarRequest = recordFactory.NewRecordInstance<GetTaskAttemptReportRequest
				>();
			gtarRequest.SetTaskAttemptId(attempt.GetID());
			TaskAttemptReport tar = proxy.GetTaskAttemptReport(gtarRequest).GetTaskAttemptReport
				();
			VerifyTaskAttemptReport(tar);
			GetTaskReportRequest gtrRequest = recordFactory.NewRecordInstance<GetTaskReportRequest
				>();
			gtrRequest.SetTaskId(task.GetID());
			NUnit.Framework.Assert.IsNotNull("TaskReport is null", proxy.GetTaskReport(gtrRequest
				).GetTaskReport());
			GetTaskReportsRequest gtreportsRequest = recordFactory.NewRecordInstance<GetTaskReportsRequest
				>();
			gtreportsRequest.SetJobId(job.GetID());
			gtreportsRequest.SetTaskType(TaskType.Map);
			NUnit.Framework.Assert.IsNotNull("TaskReports for map is null", proxy.GetTaskReports
				(gtreportsRequest).GetTaskReportList());
			gtreportsRequest = recordFactory.NewRecordInstance<GetTaskReportsRequest>();
			gtreportsRequest.SetJobId(job.GetID());
			gtreportsRequest.SetTaskType(TaskType.Reduce);
			NUnit.Framework.Assert.IsNotNull("TaskReports for reduce is null", proxy.GetTaskReports
				(gtreportsRequest).GetTaskReportList());
			IList<string> diag = proxy.GetDiagnostics(gdRequest).GetDiagnosticsList();
			NUnit.Framework.Assert.AreEqual("Num diagnostics not correct", 1, diag.Count);
			NUnit.Framework.Assert.AreEqual("Diag 1 not correct", diagnostic1, diag[0].ToString
				());
			TaskReport taskReport = proxy.GetTaskReport(gtrRequest).GetTaskReport();
			NUnit.Framework.Assert.AreEqual("Num diagnostics not correct", 1, taskReport.GetDiagnosticsCount
				());
			//send the done signal to the task
			app.GetContext().GetEventHandler().Handle(new TaskAttemptEvent(task.GetAttempts()
				.Values.GetEnumerator().Next().GetID(), TaskAttemptEventType.TaDone));
			app.WaitForState(job, JobState.Succeeded);
			// For invalid jobid, throw IOException
			gtreportsRequest = recordFactory.NewRecordInstance<GetTaskReportsRequest>();
			gtreportsRequest.SetJobId(TypeConverter.ToYarn(JobID.ForName("job_1415730144495_0001"
				)));
			gtreportsRequest.SetTaskType(TaskType.Reduce);
			try
			{
				proxy.GetTaskReports(gtreportsRequest);
				NUnit.Framework.Assert.Fail("IOException not thrown for invalid job id");
			}
			catch (IOException)
			{
			}
		}

		// Expected
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestViewAclOnlyCannotModify()
		{
			TestMRClientService.MRAppWithClientService app = new TestMRClientService.MRAppWithClientService
				(this, 1, 0, false);
			Configuration conf = new Configuration();
			conf.SetBoolean(MRConfig.MrAclsEnabled, true);
			conf.Set(MRJobConfig.JobAclViewJob, "viewonlyuser");
			Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job = app.Submit(conf);
			app.WaitForState(job, JobState.Running);
			NUnit.Framework.Assert.AreEqual("Num tasks not correct", 1, job.GetTasks().Count);
			IEnumerator<Task> it = job.GetTasks().Values.GetEnumerator();
			Task task = it.Next();
			app.WaitForState(task, TaskState.Running);
			TaskAttempt attempt = task.GetAttempts().Values.GetEnumerator().Next();
			app.WaitForState(attempt, TaskAttemptState.Running);
			UserGroupInformation viewOnlyUser = UserGroupInformation.CreateUserForTesting("viewonlyuser"
				, new string[] {  });
			NUnit.Framework.Assert.IsTrue("viewonlyuser cannot view job", job.CheckAccess(viewOnlyUser
				, JobACL.ViewJob));
			NUnit.Framework.Assert.IsFalse("viewonlyuser can modify job", job.CheckAccess(viewOnlyUser
				, JobACL.ModifyJob));
			MRClientProtocol client = viewOnlyUser.DoAs(new _PrivilegedExceptionAction_223(conf
				, app));
			KillJobRequest killJobRequest = recordFactory.NewRecordInstance<KillJobRequest>();
			killJobRequest.SetJobId(app.GetJobId());
			try
			{
				client.KillJob(killJobRequest);
				NUnit.Framework.Assert.Fail("viewonlyuser killed job");
			}
			catch (AccessControlException)
			{
			}
			// pass
			KillTaskRequest killTaskRequest = recordFactory.NewRecordInstance<KillTaskRequest
				>();
			killTaskRequest.SetTaskId(task.GetID());
			try
			{
				client.KillTask(killTaskRequest);
				NUnit.Framework.Assert.Fail("viewonlyuser killed task");
			}
			catch (AccessControlException)
			{
			}
			// pass
			KillTaskAttemptRequest killTaskAttemptRequest = recordFactory.NewRecordInstance<KillTaskAttemptRequest
				>();
			killTaskAttemptRequest.SetTaskAttemptId(attempt.GetID());
			try
			{
				client.KillTaskAttempt(killTaskAttemptRequest);
				NUnit.Framework.Assert.Fail("viewonlyuser killed task attempt");
			}
			catch (AccessControlException)
			{
			}
			// pass
			FailTaskAttemptRequest failTaskAttemptRequest = recordFactory.NewRecordInstance<FailTaskAttemptRequest
				>();
			failTaskAttemptRequest.SetTaskAttemptId(attempt.GetID());
			try
			{
				client.FailTaskAttempt(failTaskAttemptRequest);
				NUnit.Framework.Assert.Fail("viewonlyuser killed task attempt");
			}
			catch (AccessControlException)
			{
			}
		}

		private sealed class _PrivilegedExceptionAction_223 : PrivilegedExceptionAction<MRClientProtocol
			>
		{
			public _PrivilegedExceptionAction_223(Configuration conf, TestMRClientService.MRAppWithClientService
				 app)
			{
				this.conf = conf;
				this.app = app;
			}

			/// <exception cref="System.Exception"/>
			public MRClientProtocol Run()
			{
				YarnRPC rpc = YarnRPC.Create(conf);
				return (MRClientProtocol)rpc.GetProxy(typeof(MRClientProtocol), app.clientService
					.GetBindAddress(), conf);
			}

			private readonly Configuration conf;

			private readonly TestMRClientService.MRAppWithClientService app;
		}

		// pass
		private void VerifyJobReport(JobReport jr)
		{
			NUnit.Framework.Assert.IsNotNull("JobReport is null", jr);
			IList<AMInfo> amInfos = jr.GetAMInfos();
			NUnit.Framework.Assert.AreEqual(1, amInfos.Count);
			NUnit.Framework.Assert.AreEqual(JobState.Running, jr.GetJobState());
			AMInfo amInfo = amInfos[0];
			NUnit.Framework.Assert.AreEqual(MRApp.NmHost, amInfo.GetNodeManagerHost());
			NUnit.Framework.Assert.AreEqual(MRApp.NmPort, amInfo.GetNodeManagerPort());
			NUnit.Framework.Assert.AreEqual(MRApp.NmHttpPort, amInfo.GetNodeManagerHttpPort()
				);
			NUnit.Framework.Assert.AreEqual(1, amInfo.GetAppAttemptId().GetAttemptId());
			NUnit.Framework.Assert.AreEqual(1, amInfo.GetContainerId().GetApplicationAttemptId
				().GetAttemptId());
			NUnit.Framework.Assert.IsTrue(amInfo.GetStartTime() > 0);
			NUnit.Framework.Assert.AreEqual(false, jr.IsUber());
		}

		private void VerifyTaskAttemptReport(TaskAttemptReport tar)
		{
			NUnit.Framework.Assert.AreEqual(TaskAttemptState.Running, tar.GetTaskAttemptState
				());
			NUnit.Framework.Assert.IsNotNull("TaskAttemptReport is null", tar);
			NUnit.Framework.Assert.AreEqual(MRApp.NmHost, tar.GetNodeManagerHost());
			NUnit.Framework.Assert.AreEqual(MRApp.NmPort, tar.GetNodeManagerPort());
			NUnit.Framework.Assert.AreEqual(MRApp.NmHttpPort, tar.GetNodeManagerHttpPort());
			NUnit.Framework.Assert.AreEqual(1, tar.GetContainerId().GetApplicationAttemptId()
				.GetAttemptId());
		}

		internal class MRAppWithClientService : MRApp
		{
			internal MRClientService clientService = null;

			internal MRAppWithClientService(TestMRClientService _enclosing, int maps, int reduces
				, bool autoComplete)
				: base(maps, reduces, autoComplete, "MRAppWithClientService", true)
			{
				this._enclosing = _enclosing;
			}

			protected internal override ClientService CreateClientService(AppContext context)
			{
				this.clientService = new MRClientService(context);
				return this.clientService;
			}

			private readonly TestMRClientService _enclosing;
		}

		/// <exception cref="System.Exception"/>
		public static void Main(string[] args)
		{
			TestMRClientService t = new TestMRClientService();
			t.Test();
		}
	}
}
