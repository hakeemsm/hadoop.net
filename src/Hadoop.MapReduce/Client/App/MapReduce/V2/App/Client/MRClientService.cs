using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Http;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.V2.Api;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Protocolrecords;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Mapreduce.V2.App;
using Org.Apache.Hadoop.Mapreduce.V2.App.Job;
using Org.Apache.Hadoop.Mapreduce.V2.App.Job.Event;
using Org.Apache.Hadoop.Mapreduce.V2.App.Security.Authorize;
using Org.Apache.Hadoop.Mapreduce.V2.App.Webapp;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Authorize;
using Org.Apache.Hadoop.Service;
using Org.Apache.Hadoop.Yarn.Factories;
using Org.Apache.Hadoop.Yarn.Factory.Providers;
using Org.Apache.Hadoop.Yarn.Ipc;
using Org.Apache.Hadoop.Yarn.Webapp;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App.Client
{
	/// <summary>
	/// This module is responsible for talking to the
	/// jobclient (user facing).
	/// </summary>
	public class MRClientService : AbstractService, ClientService
	{
		internal static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Mapreduce.V2.App.Client.MRClientService
			));

		private MRClientProtocol protocolHandler;

		private Server server;

		private WebApp webApp;

		private IPEndPoint bindAddress;

		private AppContext appContext;

		public MRClientService(AppContext appContext)
			: base(typeof(Org.Apache.Hadoop.Mapreduce.V2.App.Client.MRClientService).FullName
				)
		{
			this.appContext = appContext;
			this.protocolHandler = new MRClientService.MRClientProtocolHandler(this);
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceStart()
		{
			Configuration conf = GetConfig();
			YarnRPC rpc = YarnRPC.Create(conf);
			IPEndPoint address = new IPEndPoint(0);
			server = rpc.GetServer(typeof(MRClientProtocol), protocolHandler, address, conf, 
				appContext.GetClientToAMTokenSecretManager(), conf.GetInt(MRJobConfig.MrAmJobClientThreadCount
				, MRJobConfig.DefaultMrAmJobClientThreadCount), MRJobConfig.MrAmJobClientPortRange
				);
			// Enable service authorization?
			if (conf.GetBoolean(CommonConfigurationKeysPublic.HadoopSecurityAuthorization, false
				))
			{
				RefreshServiceAcls(conf, new MRAMPolicyProvider());
			}
			server.Start();
			this.bindAddress = NetUtils.CreateSocketAddrForHost(appContext.GetNMHostname(), server
				.GetListenerAddress().Port);
			Log.Info("Instantiated MRClientService at " + this.bindAddress);
			try
			{
				// Explicitly disabling SSL for map reduce task as we can't allow MR users
				// to gain access to keystore file for opening SSL listener. We can trust
				// RM/NM to issue SSL certificates but definitely not MR-AM as it is
				// running in user-land.
				webApp = WebApps.$for<AppContext>("mapreduce", appContext, "ws").WithHttpPolicy(conf
					, HttpConfig.Policy.HttpOnly).Start(new AMWebApp());
			}
			catch (Exception e)
			{
				Log.Error("Webapps failed to start. Ignoring for now:", e);
			}
			base.ServiceStart();
		}

		internal virtual void RefreshServiceAcls(Configuration configuration, PolicyProvider
			 policyProvider)
		{
			this.server.RefreshServiceAcl(configuration, policyProvider);
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceStop()
		{
			if (server != null)
			{
				server.Stop();
			}
			if (webApp != null)
			{
				webApp.Stop();
			}
			base.ServiceStop();
		}

		public virtual IPEndPoint GetBindAddress()
		{
			return bindAddress;
		}

		public virtual int GetHttpPort()
		{
			return webApp.Port();
		}

		internal class MRClientProtocolHandler : MRClientProtocol
		{
			private RecordFactory recordFactory = RecordFactoryProvider.GetRecordFactory(null
				);

			public virtual IPEndPoint GetConnectAddress()
			{
				return this._enclosing.GetBindAddress();
			}

			/// <exception cref="System.IO.IOException"/>
			private Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job VerifyAndGetJob(JobId jobID, JobACL
				 accessType, bool exceptionThrow)
			{
				Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job = this._enclosing.appContext.GetJob
					(jobID);
				if (job == null && exceptionThrow)
				{
					throw new IOException("Unknown Job " + jobID);
				}
				UserGroupInformation ugi = UserGroupInformation.GetCurrentUser();
				if (job != null && !job.CheckAccess(ugi, accessType))
				{
					throw new AccessControlException("User " + ugi.GetShortUserName() + " cannot perform operation "
						 + accessType.ToString() + " on " + jobID);
				}
				return job;
			}

			/// <exception cref="System.IO.IOException"/>
			private Task VerifyAndGetTask(TaskId taskID, JobACL accessType)
			{
				Task task = this.VerifyAndGetJob(taskID.GetJobId(), accessType, true).GetTask(taskID
					);
				if (task == null)
				{
					throw new IOException("Unknown Task " + taskID);
				}
				return task;
			}

			/// <exception cref="System.IO.IOException"/>
			private TaskAttempt VerifyAndGetAttempt(TaskAttemptId attemptID, JobACL accessType
				)
			{
				TaskAttempt attempt = this.VerifyAndGetTask(attemptID.GetTaskId(), accessType).GetAttempt
					(attemptID);
				if (attempt == null)
				{
					throw new IOException("Unknown TaskAttempt " + attemptID);
				}
				return attempt;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual GetCountersResponse GetCounters(GetCountersRequest request)
			{
				JobId jobId = request.GetJobId();
				Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job = this.VerifyAndGetJob(jobId, JobACL
					.ViewJob, true);
				GetCountersResponse response = this.recordFactory.NewRecordInstance<GetCountersResponse
					>();
				response.SetCounters(TypeConverter.ToYarn(job.GetAllCounters()));
				return response;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual GetJobReportResponse GetJobReport(GetJobReportRequest request)
			{
				JobId jobId = request.GetJobId();
				// false is for retain compatibility
				Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job = this.VerifyAndGetJob(jobId, JobACL
					.ViewJob, false);
				GetJobReportResponse response = this.recordFactory.NewRecordInstance<GetJobReportResponse
					>();
				if (job != null)
				{
					response.SetJobReport(job.GetReport());
				}
				else
				{
					response.SetJobReport(null);
				}
				return response;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual GetTaskAttemptReportResponse GetTaskAttemptReport(GetTaskAttemptReportRequest
				 request)
			{
				TaskAttemptId taskAttemptId = request.GetTaskAttemptId();
				GetTaskAttemptReportResponse response = this.recordFactory.NewRecordInstance<GetTaskAttemptReportResponse
					>();
				response.SetTaskAttemptReport(this.VerifyAndGetAttempt(taskAttemptId, JobACL.ViewJob
					).GetReport());
				return response;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual GetTaskReportResponse GetTaskReport(GetTaskReportRequest request)
			{
				TaskId taskId = request.GetTaskId();
				GetTaskReportResponse response = this.recordFactory.NewRecordInstance<GetTaskReportResponse
					>();
				response.SetTaskReport(this.VerifyAndGetTask(taskId, JobACL.ViewJob).GetReport());
				return response;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual GetTaskAttemptCompletionEventsResponse GetTaskAttemptCompletionEvents
				(GetTaskAttemptCompletionEventsRequest request)
			{
				JobId jobId = request.GetJobId();
				int fromEventId = request.GetFromEventId();
				int maxEvents = request.GetMaxEvents();
				Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job = this.VerifyAndGetJob(jobId, JobACL
					.ViewJob, true);
				GetTaskAttemptCompletionEventsResponse response = this.recordFactory.NewRecordInstance
					<GetTaskAttemptCompletionEventsResponse>();
				response.AddAllCompletionEvents(Arrays.AsList(job.GetTaskAttemptCompletionEvents(
					fromEventId, maxEvents)));
				return response;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual KillJobResponse KillJob(KillJobRequest request)
			{
				JobId jobId = request.GetJobId();
				UserGroupInformation callerUGI = UserGroupInformation.GetCurrentUser();
				string message = "Kill job " + jobId + " received from " + callerUGI + " at " + Server
					.GetRemoteAddress();
				MRClientService.Log.Info(message);
				this.VerifyAndGetJob(jobId, JobACL.ModifyJob, false);
				this._enclosing.appContext.GetEventHandler().Handle(new JobDiagnosticsUpdateEvent
					(jobId, message));
				this._enclosing.appContext.GetEventHandler().Handle(new JobEvent(jobId, JobEventType
					.JobKill));
				KillJobResponse response = this.recordFactory.NewRecordInstance<KillJobResponse>(
					);
				return response;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual KillTaskResponse KillTask(KillTaskRequest request)
			{
				TaskId taskId = request.GetTaskId();
				UserGroupInformation callerUGI = UserGroupInformation.GetCurrentUser();
				string message = "Kill task " + taskId + " received from " + callerUGI + " at " +
					 Server.GetRemoteAddress();
				MRClientService.Log.Info(message);
				this.VerifyAndGetTask(taskId, JobACL.ModifyJob);
				this._enclosing.appContext.GetEventHandler().Handle(new TaskEvent(taskId, TaskEventType
					.TKill));
				KillTaskResponse response = this.recordFactory.NewRecordInstance<KillTaskResponse
					>();
				return response;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual KillTaskAttemptResponse KillTaskAttempt(KillTaskAttemptRequest request
				)
			{
				TaskAttemptId taskAttemptId = request.GetTaskAttemptId();
				UserGroupInformation callerUGI = UserGroupInformation.GetCurrentUser();
				string message = "Kill task attempt " + taskAttemptId + " received from " + callerUGI
					 + " at " + Server.GetRemoteAddress();
				MRClientService.Log.Info(message);
				this.VerifyAndGetAttempt(taskAttemptId, JobACL.ModifyJob);
				this._enclosing.appContext.GetEventHandler().Handle(new TaskAttemptDiagnosticsUpdateEvent
					(taskAttemptId, message));
				this._enclosing.appContext.GetEventHandler().Handle(new TaskAttemptEvent(taskAttemptId
					, TaskAttemptEventType.TaKill));
				KillTaskAttemptResponse response = this.recordFactory.NewRecordInstance<KillTaskAttemptResponse
					>();
				return response;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual GetDiagnosticsResponse GetDiagnostics(GetDiagnosticsRequest request
				)
			{
				TaskAttemptId taskAttemptId = request.GetTaskAttemptId();
				GetDiagnosticsResponse response = this.recordFactory.NewRecordInstance<GetDiagnosticsResponse
					>();
				response.AddAllDiagnostics(this.VerifyAndGetAttempt(taskAttemptId, JobACL.ViewJob
					).GetDiagnostics());
				return response;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual FailTaskAttemptResponse FailTaskAttempt(FailTaskAttemptRequest request
				)
			{
				TaskAttemptId taskAttemptId = request.GetTaskAttemptId();
				UserGroupInformation callerUGI = UserGroupInformation.GetCurrentUser();
				string message = "Fail task attempt " + taskAttemptId + " received from " + callerUGI
					 + " at " + Server.GetRemoteAddress();
				MRClientService.Log.Info(message);
				this.VerifyAndGetAttempt(taskAttemptId, JobACL.ModifyJob);
				this._enclosing.appContext.GetEventHandler().Handle(new TaskAttemptDiagnosticsUpdateEvent
					(taskAttemptId, message));
				this._enclosing.appContext.GetEventHandler().Handle(new TaskAttemptEvent(taskAttemptId
					, TaskAttemptEventType.TaFailmsg));
				FailTaskAttemptResponse response = this.recordFactory.NewRecordInstance<FailTaskAttemptResponse
					>();
				return response;
			}

			private readonly object getTaskReportsLock = new object();

			/// <exception cref="System.IO.IOException"/>
			public virtual GetTaskReportsResponse GetTaskReports(GetTaskReportsRequest request
				)
			{
				JobId jobId = request.GetJobId();
				TaskType taskType = request.GetTaskType();
				GetTaskReportsResponse response = this.recordFactory.NewRecordInstance<GetTaskReportsResponse
					>();
				Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job = this.VerifyAndGetJob(jobId, JobACL
					.ViewJob, true);
				ICollection<Task> tasks = job.GetTasks(taskType).Values;
				MRClientService.Log.Info("Getting task report for " + taskType + "   " + jobId + 
					". Report-size will be " + tasks.Count);
				// Take lock to allow only one call, otherwise heap will blow up because
				// of counters in the report when there are multiple callers.
				lock (this.getTaskReportsLock)
				{
					foreach (Task task in tasks)
					{
						response.AddTaskReport(task.GetReport());
					}
				}
				return response;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual GetDelegationTokenResponse GetDelegationToken(GetDelegationTokenRequest
				 request)
			{
				throw new IOException("MR AM not authorized to issue delegation" + " token");
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual RenewDelegationTokenResponse RenewDelegationToken(RenewDelegationTokenRequest
				 request)
			{
				throw new IOException("MR AM not authorized to renew delegation" + " token");
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual CancelDelegationTokenResponse CancelDelegationToken(CancelDelegationTokenRequest
				 request)
			{
				throw new IOException("MR AM not authorized to cancel delegation" + " token");
			}

			internal MRClientProtocolHandler(MRClientService _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly MRClientService _enclosing;
		}

		public virtual WebApp GetWebApp()
		{
			return webApp;
		}
	}
}
