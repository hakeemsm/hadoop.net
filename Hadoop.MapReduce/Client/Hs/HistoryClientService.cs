using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using Com.Google.Common.Annotations;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.V2.Api;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Protocolrecords;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Mapreduce.V2.App.Job;
using Org.Apache.Hadoop.Mapreduce.V2.App.Security.Authorize;
using Org.Apache.Hadoop.Mapreduce.V2.HS.Webapp;
using Org.Apache.Hadoop.Mapreduce.V2.Jobhistory;
using Org.Apache.Hadoop.Mapreduce.V2.Util;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Service;
using Org.Apache.Hadoop.Yarn.Factories;
using Org.Apache.Hadoop.Yarn.Factory.Providers;
using Org.Apache.Hadoop.Yarn.Ipc;
using Org.Apache.Hadoop.Yarn.Webapp;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.HS
{
	/// <summary>
	/// This module is responsible for talking to the
	/// JobClient (user facing).
	/// </summary>
	public class HistoryClientService : AbstractService
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Mapreduce.V2.HS.HistoryClientService
			));

		private HSClientProtocol protocolHandler;

		private Server server;

		private WebApp webApp;

		private IPEndPoint bindAddress;

		private HistoryContext history;

		private JHSDelegationTokenSecretManager jhsDTSecretManager;

		public HistoryClientService(HistoryContext history, JHSDelegationTokenSecretManager
			 jhsDTSecretManager)
			: base("HistoryClientService")
		{
			this.history = history;
			this.protocolHandler = new HistoryClientService.HSClientProtocolHandler(this);
			this.jhsDTSecretManager = jhsDTSecretManager;
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceStart()
		{
			Configuration conf = GetConfig();
			YarnRPC rpc = YarnRPC.Create(conf);
			InitializeWebApp(conf);
			IPEndPoint address = conf.GetSocketAddr(JHAdminConfig.MrHistoryBindHost, JHAdminConfig
				.MrHistoryAddress, JHAdminConfig.DefaultMrHistoryAddress, JHAdminConfig.DefaultMrHistoryPort
				);
			server = rpc.GetServer(typeof(HSClientProtocol), protocolHandler, address, conf, 
				jhsDTSecretManager, conf.GetInt(JHAdminConfig.MrHistoryClientThreadCount, JHAdminConfig
				.DefaultMrHistoryClientThreadCount));
			// Enable service authorization?
			if (conf.GetBoolean(CommonConfigurationKeysPublic.HadoopSecurityAuthorization, false
				))
			{
				server.RefreshServiceAcl(conf, new ClientHSPolicyProvider());
			}
			server.Start();
			this.bindAddress = conf.UpdateConnectAddr(JHAdminConfig.MrHistoryBindHost, JHAdminConfig
				.MrHistoryAddress, JHAdminConfig.DefaultMrHistoryAddress, server.GetListenerAddress
				());
			Log.Info("Instantiated HistoryClientService at " + this.bindAddress);
			base.ServiceStart();
		}

		[VisibleForTesting]
		protected internal virtual void InitializeWebApp(Configuration conf)
		{
			webApp = new HsWebApp(history);
			IPEndPoint bindAddress = MRWebAppUtil.GetJHSWebBindAddress(conf);
			// NOTE: there should be a .at(InetSocketAddress)
			WebApps.$for<Org.Apache.Hadoop.Mapreduce.V2.HS.HistoryClientService>("jobhistory"
				, this, "ws").With(conf).WithHttpSpnegoKeytabKey(JHAdminConfig.MrWebappSpnegoKeytabFileKey
				).WithHttpSpnegoPrincipalKey(JHAdminConfig.MrWebappSpnegoUserNameKey).At(NetUtils
				.GetHostPortString(bindAddress)).Start(webApp);
			string connectHost = MRWebAppUtil.GetJHSWebappURLWithoutScheme(conf).Split(":")[0
				];
			MRWebAppUtil.SetJHSWebappURLWithoutScheme(conf, connectHost + ":" + webApp.GetListenerAddress
				().Port);
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

		[InterfaceAudience.Private]
		public virtual MRClientProtocol GetClientHandler()
		{
			return this.protocolHandler;
		}

		[InterfaceAudience.Private]
		public virtual IPEndPoint GetBindAddress()
		{
			return this.bindAddress;
		}

		private class HSClientProtocolHandler : HSClientProtocol
		{
			private RecordFactory recordFactory = RecordFactoryProvider.GetRecordFactory(null
				);

			public virtual IPEndPoint GetConnectAddress()
			{
				return this._enclosing.GetBindAddress();
			}

			/// <exception cref="System.IO.IOException"/>
			private Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job VerifyAndGetJob(JobId jobID, bool
				 exceptionThrow)
			{
				UserGroupInformation loginUgi = null;
				Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job = null;
				try
				{
					loginUgi = UserGroupInformation.GetLoginUser();
					job = loginUgi.DoAs(new _PrivilegedExceptionAction_205(this, jobID));
				}
				catch (Exception e)
				{
					throw new IOException(e);
				}
				if (job == null && exceptionThrow)
				{
					throw new IOException("Unknown Job " + jobID);
				}
				if (job != null)
				{
					JobACL operation = JobACL.ViewJob;
					this.CheckAccess(job, operation);
				}
				return job;
			}

			private sealed class _PrivilegedExceptionAction_205 : PrivilegedExceptionAction<Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job
				>
			{
				public _PrivilegedExceptionAction_205(HSClientProtocolHandler _enclosing, JobId jobID
					)
				{
					this._enclosing = _enclosing;
					this.jobID = jobID;
				}

				/// <exception cref="System.Exception"/>
				public Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job Run()
				{
					Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job = this._enclosing._enclosing.history
						.GetJob(jobID);
					return job;
				}

				private readonly HSClientProtocolHandler _enclosing;

				private readonly JobId jobID;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual GetCountersResponse GetCounters(GetCountersRequest request)
			{
				JobId jobId = request.GetJobId();
				Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job = this.VerifyAndGetJob(jobId, true
					);
				GetCountersResponse response = this.recordFactory.NewRecordInstance<GetCountersResponse
					>();
				response.SetCounters(TypeConverter.ToYarn(job.GetAllCounters()));
				return response;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual GetJobReportResponse GetJobReport(GetJobReportRequest request)
			{
				JobId jobId = request.GetJobId();
				Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job = this.VerifyAndGetJob(jobId, false
					);
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
				Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job = this.VerifyAndGetJob(taskAttemptId
					.GetTaskId().GetJobId(), true);
				GetTaskAttemptReportResponse response = this.recordFactory.NewRecordInstance<GetTaskAttemptReportResponse
					>();
				response.SetTaskAttemptReport(job.GetTask(taskAttemptId.GetTaskId()).GetAttempt(taskAttemptId
					).GetReport());
				return response;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual GetTaskReportResponse GetTaskReport(GetTaskReportRequest request)
			{
				TaskId taskId = request.GetTaskId();
				Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job = this.VerifyAndGetJob(taskId.GetJobId
					(), true);
				GetTaskReportResponse response = this.recordFactory.NewRecordInstance<GetTaskReportResponse
					>();
				response.SetTaskReport(job.GetTask(taskId).GetReport());
				return response;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual GetTaskAttemptCompletionEventsResponse GetTaskAttemptCompletionEvents
				(GetTaskAttemptCompletionEventsRequest request)
			{
				JobId jobId = request.GetJobId();
				int fromEventId = request.GetFromEventId();
				int maxEvents = request.GetMaxEvents();
				Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job = this.VerifyAndGetJob(jobId, true
					);
				GetTaskAttemptCompletionEventsResponse response = this.recordFactory.NewRecordInstance
					<GetTaskAttemptCompletionEventsResponse>();
				response.AddAllCompletionEvents(Arrays.AsList(job.GetTaskAttemptCompletionEvents(
					fromEventId, maxEvents)));
				return response;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual KillJobResponse KillJob(KillJobRequest request)
			{
				throw new IOException("Invalid operation on completed job");
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual KillTaskResponse KillTask(KillTaskRequest request)
			{
				throw new IOException("Invalid operation on completed job");
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual KillTaskAttemptResponse KillTaskAttempt(KillTaskAttemptRequest request
				)
			{
				throw new IOException("Invalid operation on completed job");
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual GetDiagnosticsResponse GetDiagnostics(GetDiagnosticsRequest request
				)
			{
				TaskAttemptId taskAttemptId = request.GetTaskAttemptId();
				Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job = this.VerifyAndGetJob(taskAttemptId
					.GetTaskId().GetJobId(), true);
				GetDiagnosticsResponse response = this.recordFactory.NewRecordInstance<GetDiagnosticsResponse
					>();
				response.AddAllDiagnostics(job.GetTask(taskAttemptId.GetTaskId()).GetAttempt(taskAttemptId
					).GetDiagnostics());
				return response;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual FailTaskAttemptResponse FailTaskAttempt(FailTaskAttemptRequest request
				)
			{
				throw new IOException("Invalid operation on completed job");
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual GetTaskReportsResponse GetTaskReports(GetTaskReportsRequest request
				)
			{
				JobId jobId = request.GetJobId();
				TaskType taskType = request.GetTaskType();
				GetTaskReportsResponse response = this.recordFactory.NewRecordInstance<GetTaskReportsResponse
					>();
				Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job = this.VerifyAndGetJob(jobId, true
					);
				ICollection<Task> tasks = job.GetTasks(taskType).Values;
				foreach (Task task in tasks)
				{
					response.AddTaskReport(task.GetReport());
				}
				return response;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual GetDelegationTokenResponse GetDelegationToken(GetDelegationTokenRequest
				 request)
			{
				UserGroupInformation ugi = UserGroupInformation.GetCurrentUser();
				// Verify that the connection is kerberos authenticated
				if (!this.IsAllowedDelegationTokenOp())
				{
					throw new IOException("Delegation Token can be issued only with kerberos authentication"
						);
				}
				GetDelegationTokenResponse response = this.recordFactory.NewRecordInstance<GetDelegationTokenResponse
					>();
				string user = ugi.GetUserName();
				Text owner = new Text(user);
				Text realUser = null;
				if (ugi.GetRealUser() != null)
				{
					realUser = new Text(ugi.GetRealUser().GetUserName());
				}
				MRDelegationTokenIdentifier tokenIdentifier = new MRDelegationTokenIdentifier(owner
					, new Text(request.GetRenewer()), realUser);
				Org.Apache.Hadoop.Security.Token.Token<MRDelegationTokenIdentifier> realJHSToken = 
					new Org.Apache.Hadoop.Security.Token.Token<MRDelegationTokenIdentifier>(tokenIdentifier
					, this._enclosing.jhsDTSecretManager);
				Org.Apache.Hadoop.Yarn.Api.Records.Token mrDToken = Org.Apache.Hadoop.Yarn.Api.Records.Token
					.NewInstance(realJHSToken.GetIdentifier(), realJHSToken.GetKind().ToString(), realJHSToken
					.GetPassword(), realJHSToken.GetService().ToString());
				response.SetDelegationToken(mrDToken);
				return response;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual RenewDelegationTokenResponse RenewDelegationToken(RenewDelegationTokenRequest
				 request)
			{
				if (!this.IsAllowedDelegationTokenOp())
				{
					throw new IOException("Delegation Token can be renewed only with kerberos authentication"
						);
				}
				Org.Apache.Hadoop.Yarn.Api.Records.Token protoToken = request.GetDelegationToken(
					);
				Org.Apache.Hadoop.Security.Token.Token<MRDelegationTokenIdentifier> token = new Org.Apache.Hadoop.Security.Token.Token
					<MRDelegationTokenIdentifier>(((byte[])protoToken.GetIdentifier().Array()), ((byte
					[])protoToken.GetPassword().Array()), new Text(protoToken.GetKind()), new Text(protoToken
					.GetService()));
				string user = UserGroupInformation.GetCurrentUser().GetShortUserName();
				long nextExpTime = this._enclosing.jhsDTSecretManager.RenewToken(token, user);
				RenewDelegationTokenResponse renewResponse = Org.Apache.Hadoop.Yarn.Util.Records.
					NewRecord<RenewDelegationTokenResponse>();
				renewResponse.SetNextExpirationTime(nextExpTime);
				return renewResponse;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual CancelDelegationTokenResponse CancelDelegationToken(CancelDelegationTokenRequest
				 request)
			{
				if (!this.IsAllowedDelegationTokenOp())
				{
					throw new IOException("Delegation Token can be cancelled only with kerberos authentication"
						);
				}
				Org.Apache.Hadoop.Yarn.Api.Records.Token protoToken = request.GetDelegationToken(
					);
				Org.Apache.Hadoop.Security.Token.Token<MRDelegationTokenIdentifier> token = new Org.Apache.Hadoop.Security.Token.Token
					<MRDelegationTokenIdentifier>(((byte[])protoToken.GetIdentifier().Array()), ((byte
					[])protoToken.GetPassword().Array()), new Text(protoToken.GetKind()), new Text(protoToken
					.GetService()));
				string user = UserGroupInformation.GetCurrentUser().GetUserName();
				this._enclosing.jhsDTSecretManager.CancelToken(token, user);
				return Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<CancelDelegationTokenResponse
					>();
			}

			/// <exception cref="System.IO.IOException"/>
			private void CheckAccess(Org.Apache.Hadoop.Mapreduce.V2.App.Job.Job job, JobACL jobOperation
				)
			{
				UserGroupInformation callerUGI;
				callerUGI = UserGroupInformation.GetCurrentUser();
				if (!job.CheckAccess(callerUGI, jobOperation))
				{
					throw new IOException(new AccessControlException("User " + callerUGI.GetShortUserName
						() + " cannot perform operation " + jobOperation.ToString() + " on " + job.GetID
						()));
				}
			}

			/// <exception cref="System.IO.IOException"/>
			private bool IsAllowedDelegationTokenOp()
			{
				if (UserGroupInformation.IsSecurityEnabled())
				{
					return EnumSet.Of(UserGroupInformation.AuthenticationMethod.Kerberos, UserGroupInformation.AuthenticationMethod
						.KerberosSsl, UserGroupInformation.AuthenticationMethod.Certificate).Contains(UserGroupInformation
						.GetCurrentUser().GetRealAuthenticationMethod());
				}
				else
				{
					return true;
				}
			}

			internal HSClientProtocolHandler(HistoryClientService _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly HistoryClientService _enclosing;
		}
	}
}
