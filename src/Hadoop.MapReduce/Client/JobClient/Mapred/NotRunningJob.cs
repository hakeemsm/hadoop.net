using System.Collections.Generic;
using System.Net;
using Org.Apache.Commons.Lang;
using Org.Apache.Hadoop.Mapreduce.V2.Api;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Protocolrecords;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Factories;
using Org.Apache.Hadoop.Yarn.Factory.Providers;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	public class NotRunningJob : MRClientProtocol
	{
		private RecordFactory recordFactory = RecordFactoryProvider.GetRecordFactory(null
			);

		private readonly JobState jobState;

		private readonly ApplicationReport applicationReport;

		private ApplicationReport GetUnknownApplicationReport()
		{
			ApplicationId unknownAppId = recordFactory.NewRecordInstance<ApplicationId>();
			ApplicationAttemptId unknownAttemptId = recordFactory.NewRecordInstance<ApplicationAttemptId
				>();
			// Setting AppState to NEW and finalStatus to UNDEFINED as they are never
			// used for a non running job
			return ApplicationReport.NewInstance(unknownAppId, unknownAttemptId, "N/A", "N/A"
				, "N/A", "N/A", 0, null, YarnApplicationState.New, "N/A", "N/A", 0, 0, FinalApplicationStatus
				.Undefined, null, "N/A", 0.0f, YarnConfiguration.DefaultApplicationType, null);
		}

		internal NotRunningJob(ApplicationReport applicationReport, JobState jobState)
		{
			this.applicationReport = (applicationReport == null) ? GetUnknownApplicationReport
				() : applicationReport;
			this.jobState = jobState;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual FailTaskAttemptResponse FailTaskAttempt(FailTaskAttemptRequest request
			)
		{
			FailTaskAttemptResponse resp = recordFactory.NewRecordInstance<FailTaskAttemptResponse
				>();
			return resp;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual GetCountersResponse GetCounters(GetCountersRequest request)
		{
			GetCountersResponse resp = recordFactory.NewRecordInstance<GetCountersResponse>();
			Counters counters = recordFactory.NewRecordInstance<Counters>();
			counters.AddAllCounterGroups(new Dictionary<string, CounterGroup>());
			resp.SetCounters(counters);
			return resp;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual GetDiagnosticsResponse GetDiagnostics(GetDiagnosticsRequest request
			)
		{
			GetDiagnosticsResponse resp = recordFactory.NewRecordInstance<GetDiagnosticsResponse
				>();
			resp.AddDiagnostics(string.Empty);
			return resp;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual GetJobReportResponse GetJobReport(GetJobReportRequest request)
		{
			JobReport jobReport = recordFactory.NewRecordInstance<JobReport>();
			jobReport.SetJobId(request.GetJobId());
			jobReport.SetJobState(jobState);
			jobReport.SetUser(applicationReport.GetUser());
			jobReport.SetStartTime(applicationReport.GetStartTime());
			jobReport.SetDiagnostics(applicationReport.GetDiagnostics());
			jobReport.SetJobName(applicationReport.GetName());
			jobReport.SetTrackingUrl(applicationReport.GetTrackingUrl());
			jobReport.SetFinishTime(applicationReport.GetFinishTime());
			GetJobReportResponse resp = recordFactory.NewRecordInstance<GetJobReportResponse>
				();
			resp.SetJobReport(jobReport);
			return resp;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual GetTaskAttemptCompletionEventsResponse GetTaskAttemptCompletionEvents
			(GetTaskAttemptCompletionEventsRequest request)
		{
			GetTaskAttemptCompletionEventsResponse resp = recordFactory.NewRecordInstance<GetTaskAttemptCompletionEventsResponse
				>();
			resp.AddAllCompletionEvents(new AList<TaskAttemptCompletionEvent>());
			return resp;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual GetTaskAttemptReportResponse GetTaskAttemptReport(GetTaskAttemptReportRequest
			 request)
		{
			//not invoked by anybody
			throw new NotImplementedException();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual GetTaskReportResponse GetTaskReport(GetTaskReportRequest request)
		{
			GetTaskReportResponse resp = recordFactory.NewRecordInstance<GetTaskReportResponse
				>();
			TaskReport report = recordFactory.NewRecordInstance<TaskReport>();
			report.SetTaskId(request.GetTaskId());
			report.SetTaskState(TaskState.New);
			Counters counters = recordFactory.NewRecordInstance<Counters>();
			counters.AddAllCounterGroups(new Dictionary<string, CounterGroup>());
			report.SetCounters(counters);
			report.AddAllRunningAttempts(new AList<TaskAttemptId>());
			return resp;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual GetTaskReportsResponse GetTaskReports(GetTaskReportsRequest request
			)
		{
			GetTaskReportsResponse resp = recordFactory.NewRecordInstance<GetTaskReportsResponse
				>();
			resp.AddAllTaskReports(new AList<TaskReport>());
			return resp;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual KillJobResponse KillJob(KillJobRequest request)
		{
			KillJobResponse resp = recordFactory.NewRecordInstance<KillJobResponse>();
			return resp;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual KillTaskResponse KillTask(KillTaskRequest request)
		{
			KillTaskResponse resp = recordFactory.NewRecordInstance<KillTaskResponse>();
			return resp;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual KillTaskAttemptResponse KillTaskAttempt(KillTaskAttemptRequest request
			)
		{
			KillTaskAttemptResponse resp = recordFactory.NewRecordInstance<KillTaskAttemptResponse
				>();
			return resp;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual GetDelegationTokenResponse GetDelegationToken(GetDelegationTokenRequest
			 request)
		{
			/* Should not be invoked by anyone. */
			throw new NotImplementedException();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual RenewDelegationTokenResponse RenewDelegationToken(RenewDelegationTokenRequest
			 request)
		{
			/* Should not be invoked by anyone. */
			throw new NotImplementedException();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual CancelDelegationTokenResponse CancelDelegationToken(CancelDelegationTokenRequest
			 request)
		{
			/* Should not be invoked by anyone. */
			throw new NotImplementedException();
		}

		public virtual IPEndPoint GetConnectAddress()
		{
			/* Should not be invoked by anyone.  Normally used to set token service */
			throw new NotImplementedException();
		}
	}
}
