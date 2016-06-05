using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Reflection;
using System.Security;
using Com.Google.Common.Annotations;
using Org.Apache.Commons.Lang;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.V2;
using Org.Apache.Hadoop.Mapreduce.V2.Api;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Protocolrecords;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Mapreduce.V2.Util;
using Org.Apache.Hadoop.Net;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Authorize;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Factories;
using Org.Apache.Hadoop.Yarn.Factory.Providers;
using Org.Apache.Hadoop.Yarn.Ipc;
using Org.Apache.Hadoop.Yarn.Security.Client;
using Org.Apache.Hadoop.Yarn.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	public class ClientServiceDelegate
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Mapred.ClientServiceDelegate
			));

		private const string Unavailable = "N/A";

		private Dictionary<JobState, Dictionary<string, NotRunningJob>> notRunningJobs;

		private readonly Configuration conf;

		private readonly JobID jobId;

		private readonly ApplicationId appId;

		private readonly ResourceMgrDelegate rm;

		private readonly MRClientProtocol historyServerProxy;

		private MRClientProtocol realProxy = null;

		private RecordFactory recordFactory = RecordFactoryProvider.GetRecordFactory(null
			);

		private static string UnknownUser = "Unknown User";

		private string trackingUrl;

		private AtomicBoolean usingAMProxy = new AtomicBoolean(false);

		private int maxClientRetry;

		private bool amAclDisabledStatusLogged = false;

		public ClientServiceDelegate(Configuration conf, ResourceMgrDelegate rm, JobID jobId
			, MRClientProtocol historyServerProxy)
		{
			// Caches for per-user NotRunningJobs
			this.conf = new Configuration(conf);
			// Cloning for modifying.
			// For faster redirects from AM to HS.
			this.conf.SetInt(CommonConfigurationKeysPublic.IpcClientConnectMaxRetriesKey, this
				.conf.GetInt(MRJobConfig.MrClientToAmIpcMaxRetries, MRJobConfig.DefaultMrClientToAmIpcMaxRetries
				));
			this.conf.SetInt(CommonConfigurationKeysPublic.IpcClientConnectMaxRetriesOnSocketTimeoutsKey
				, this.conf.GetInt(MRJobConfig.MrClientToAmIpcMaxRetriesOnTimeouts, MRJobConfig.
				DefaultMrClientToAmIpcMaxRetriesOnTimeouts));
			this.rm = rm;
			this.jobId = jobId;
			this.historyServerProxy = historyServerProxy;
			this.appId = TypeConverter.ToYarn(jobId).GetAppId();
			notRunningJobs = new Dictionary<JobState, Dictionary<string, NotRunningJob>>();
		}

		// Get the instance of the NotRunningJob corresponding to the specified
		// user and state
		private NotRunningJob GetNotRunningJob(ApplicationReport applicationReport, JobState
			 state)
		{
			lock (notRunningJobs)
			{
				Dictionary<string, NotRunningJob> map = notRunningJobs[state];
				if (map == null)
				{
					map = new Dictionary<string, NotRunningJob>();
					notRunningJobs[state] = map;
				}
				string user = (applicationReport == null) ? UnknownUser : applicationReport.GetUser
					();
				NotRunningJob notRunningJob = map[user];
				if (notRunningJob == null)
				{
					notRunningJob = new NotRunningJob(applicationReport, state);
					map[user] = notRunningJob;
				}
				return notRunningJob;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private MRClientProtocol GetProxy()
		{
			if (realProxy != null)
			{
				return realProxy;
			}
			// Possibly allow nulls through the PB tunnel, otherwise deal with an exception
			// and redirect to the history server.
			ApplicationReport application = null;
			try
			{
				application = rm.GetApplicationReport(appId);
			}
			catch (ApplicationNotFoundException)
			{
				application = null;
			}
			catch (YarnException e2)
			{
				throw new IOException(e2);
			}
			if (application != null)
			{
				trackingUrl = application.GetTrackingUrl();
			}
			IPEndPoint serviceAddr = null;
			while (application == null || YarnApplicationState.Running == application.GetYarnApplicationState
				())
			{
				if (application == null)
				{
					Log.Info("Could not get Job info from RM for job " + jobId + ". Redirecting to job history server."
						);
					return CheckAndGetHSProxy(null, JobState.New);
				}
				try
				{
					if (application.GetHost() == null || string.Empty.Equals(application.GetHost()))
					{
						Log.Debug("AM not assigned to Job. Waiting to get the AM ...");
						Sharpen.Thread.Sleep(2000);
						Log.Debug("Application state is " + application.GetYarnApplicationState());
						application = rm.GetApplicationReport(appId);
						continue;
					}
					else
					{
						if (Unavailable.Equals(application.GetHost()))
						{
							if (!amAclDisabledStatusLogged)
							{
								Log.Info("Job " + jobId + " is running, but the host is unknown." + " Verify user has VIEW_JOB access."
									);
								amAclDisabledStatusLogged = true;
							}
							return GetNotRunningJob(application, JobState.Running);
						}
					}
					if (!conf.GetBoolean(MRJobConfig.JobAmAccessDisabled, false))
					{
						UserGroupInformation newUgi = UserGroupInformation.CreateRemoteUser(UserGroupInformation
							.GetCurrentUser().GetUserName());
						serviceAddr = NetUtils.CreateSocketAddrForHost(application.GetHost(), application
							.GetRpcPort());
						if (UserGroupInformation.IsSecurityEnabled())
						{
							Token clientToAMToken = application.GetClientToAMToken();
							Org.Apache.Hadoop.Security.Token.Token<ClientToAMTokenIdentifier> token = ConverterUtils
								.ConvertFromYarn(clientToAMToken, serviceAddr);
							newUgi.AddToken(token);
						}
						Log.Debug("Connecting to " + serviceAddr);
						IPEndPoint finalServiceAddr = serviceAddr;
						realProxy = newUgi.DoAs(new _PrivilegedExceptionAction_202(this, finalServiceAddr
							));
					}
					else
					{
						if (!amAclDisabledStatusLogged)
						{
							Log.Info("Network ACL closed to AM for job " + jobId + ". Not going to try to reach the AM."
								);
							amAclDisabledStatusLogged = true;
						}
						return GetNotRunningJob(null, JobState.Running);
					}
					return realProxy;
				}
				catch (IOException)
				{
					//possibly the AM has crashed
					//there may be some time before AM is restarted
					//keep retrying by getting the address from RM
					Log.Info("Could not connect to " + serviceAddr + ". Waiting for getting the latest AM address..."
						);
					try
					{
						Sharpen.Thread.Sleep(2000);
					}
					catch (Exception e1)
					{
						Log.Warn("getProxy() call interruped", e1);
						throw new YarnRuntimeException(e1);
					}
					try
					{
						application = rm.GetApplicationReport(appId);
					}
					catch (YarnException e1)
					{
						throw new IOException(e1);
					}
					if (application == null)
					{
						Log.Info("Could not get Job info from RM for job " + jobId + ". Redirecting to job history server."
							);
						return CheckAndGetHSProxy(null, JobState.Running);
					}
				}
				catch (Exception e)
				{
					Log.Warn("getProxy() call interruped", e);
					throw new YarnRuntimeException(e);
				}
				catch (YarnException e)
				{
					throw new IOException(e);
				}
			}
			string user = application.GetUser();
			if (user == null)
			{
				throw new IOException("User is not set in the application report");
			}
			if (application.GetYarnApplicationState() == YarnApplicationState.New || application
				.GetYarnApplicationState() == YarnApplicationState.NewSaving || application.GetYarnApplicationState
				() == YarnApplicationState.Submitted || application.GetYarnApplicationState() ==
				 YarnApplicationState.Accepted)
			{
				realProxy = null;
				return GetNotRunningJob(application, JobState.New);
			}
			if (application.GetYarnApplicationState() == YarnApplicationState.Failed)
			{
				realProxy = null;
				return GetNotRunningJob(application, JobState.Failed);
			}
			if (application.GetYarnApplicationState() == YarnApplicationState.Killed)
			{
				realProxy = null;
				return GetNotRunningJob(application, JobState.Killed);
			}
			//History server can serve a job only if application
			//succeeded.
			if (application.GetYarnApplicationState() == YarnApplicationState.Finished)
			{
				Log.Info("Application state is completed. FinalApplicationStatus=" + application.
					GetFinalApplicationStatus().ToString() + ". Redirecting to job history server");
				realProxy = CheckAndGetHSProxy(application, JobState.Succeeded);
			}
			return realProxy;
		}

		private sealed class _PrivilegedExceptionAction_202 : PrivilegedExceptionAction<MRClientProtocol
			>
		{
			public _PrivilegedExceptionAction_202(ClientServiceDelegate _enclosing, IPEndPoint
				 finalServiceAddr)
			{
				this._enclosing = _enclosing;
				this.finalServiceAddr = finalServiceAddr;
			}

			/// <exception cref="System.IO.IOException"/>
			public MRClientProtocol Run()
			{
				return this._enclosing.InstantiateAMProxy(finalServiceAddr);
			}

			private readonly ClientServiceDelegate _enclosing;

			private readonly IPEndPoint finalServiceAddr;
		}

		private MRClientProtocol CheckAndGetHSProxy(ApplicationReport applicationReport, 
			JobState state)
		{
			if (null == historyServerProxy)
			{
				Log.Warn("Job History Server is not configured.");
				return GetNotRunningJob(applicationReport, state);
			}
			return historyServerProxy;
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual MRClientProtocol InstantiateAMProxy(IPEndPoint serviceAddr)
		{
			Log.Trace("Connecting to ApplicationMaster at: " + serviceAddr);
			YarnRPC rpc = YarnRPC.Create(conf);
			MRClientProtocol proxy = (MRClientProtocol)rpc.GetProxy(typeof(MRClientProtocol), 
				serviceAddr, conf);
			usingAMProxy.Set(true);
			Log.Trace("Connected to ApplicationMaster at: " + serviceAddr);
			return proxy;
		}

		/// <exception cref="System.IO.IOException"/>
		private object Invoke(string method, Type argClass, object args)
		{
			lock (this)
			{
				MethodInfo methodOb = null;
				try
				{
					methodOb = typeof(MRClientProtocol).GetMethod(method, argClass);
				}
				catch (SecurityException e)
				{
					throw new YarnRuntimeException(e);
				}
				catch (MissingMethodException e)
				{
					throw new YarnRuntimeException("Method name mismatch", e);
				}
				maxClientRetry = this.conf.GetInt(MRJobConfig.MrClientMaxRetries, MRJobConfig.DefaultMrClientMaxRetries
					);
				IOException lastException = null;
				while (maxClientRetry > 0)
				{
					MRClientProtocol MRClientProxy = null;
					try
					{
						MRClientProxy = GetProxy();
						return methodOb.Invoke(MRClientProxy, args);
					}
					catch (TargetInvocationException e)
					{
						// Will not throw out YarnException anymore
						Log.Debug("Failed to contact AM/History for job " + jobId + " retrying..", e.InnerException
							);
						// Force reconnection by setting the proxy to null.
						realProxy = null;
						// HS/AMS shut down
						if (e.InnerException is AuthorizationException)
						{
							throw new IOException(e.InnerException);
						}
						// if it's AM shut down, do not decrement maxClientRetry as we wait for
						// AM to be restarted.
						if (!usingAMProxy.Get())
						{
							maxClientRetry--;
						}
						usingAMProxy.Set(false);
						lastException = new IOException(e.InnerException);
						try
						{
							Sharpen.Thread.Sleep(100);
						}
						catch (Exception ie)
						{
							Log.Warn("ClientServiceDelegate invoke call interrupted", ie);
							throw new YarnRuntimeException(ie);
						}
					}
					catch (Exception e)
					{
						Log.Debug("Failed to contact AM/History for job " + jobId + "  Will retry..", e);
						// Force reconnection by setting the proxy to null.
						realProxy = null;
						// RM shutdown
						maxClientRetry--;
						lastException = new IOException(e.Message);
						try
						{
							Sharpen.Thread.Sleep(100);
						}
						catch (Exception ie)
						{
							Log.Warn("ClientServiceDelegate invoke call interrupted", ie);
							throw new YarnRuntimeException(ie);
						}
					}
				}
				throw lastException;
			}
		}

		// Only for testing
		[VisibleForTesting]
		public virtual int GetMaxClientRetry()
		{
			return this.maxClientRetry;
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual Counters GetJobCounters(JobID arg0)
		{
			JobId jobID = TypeConverter.ToYarn(arg0);
			GetCountersRequest request = recordFactory.NewRecordInstance<GetCountersRequest>(
				);
			request.SetJobId(jobID);
			Counters cnt = ((GetCountersResponse)Invoke("getCounters", typeof(GetCountersRequest
				), request)).GetCounters();
			return TypeConverter.FromYarn(cnt);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual TaskCompletionEvent[] GetTaskCompletionEvents(JobID arg0, int arg1
			, int arg2)
		{
			JobId jobID = TypeConverter.ToYarn(arg0);
			GetTaskAttemptCompletionEventsRequest request = recordFactory.NewRecordInstance<GetTaskAttemptCompletionEventsRequest
				>();
			request.SetJobId(jobID);
			request.SetFromEventId(arg1);
			request.SetMaxEvents(arg2);
			IList<TaskAttemptCompletionEvent> list = ((GetTaskAttemptCompletionEventsResponse
				)Invoke("getTaskAttemptCompletionEvents", typeof(GetTaskAttemptCompletionEventsRequest
				), request)).GetCompletionEventList();
			return TypeConverter.FromYarn(Sharpen.Collections.ToArray(list, new TaskAttemptCompletionEvent
				[0]));
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual string[] GetTaskDiagnostics(TaskAttemptID arg0)
		{
			TaskAttemptId attemptID = TypeConverter.ToYarn(arg0);
			GetDiagnosticsRequest request = recordFactory.NewRecordInstance<GetDiagnosticsRequest
				>();
			request.SetTaskAttemptId(attemptID);
			IList<string> list = ((GetDiagnosticsResponse)Invoke("getDiagnostics", typeof(GetDiagnosticsRequest
				), request)).GetDiagnosticsList();
			string[] result = new string[list.Count];
			int i = 0;
			foreach (string c in list)
			{
				result[i++] = c.ToString();
			}
			return result;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual JobStatus GetJobStatus(JobID oldJobID)
		{
			JobId jobId = TypeConverter.ToYarn(oldJobID);
			GetJobReportRequest request = recordFactory.NewRecordInstance<GetJobReportRequest
				>();
			request.SetJobId(jobId);
			JobReport report = ((GetJobReportResponse)Invoke("getJobReport", typeof(GetJobReportRequest
				), request)).GetJobReport();
			JobStatus jobStatus = null;
			if (report != null)
			{
				if (StringUtils.IsEmpty(report.GetJobFile()))
				{
					string jobFile = MRApps.GetJobFile(conf, report.GetUser(), oldJobID);
					report.SetJobFile(jobFile);
				}
				string historyTrackingUrl = report.GetTrackingUrl();
				string url = StringUtils.IsNotEmpty(historyTrackingUrl) ? historyTrackingUrl : trackingUrl;
				jobStatus = TypeConverter.FromYarn(report, url);
			}
			return jobStatus;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual TaskReport[] GetTaskReports(JobID oldJobID, TaskType taskType)
		{
			JobId jobId = TypeConverter.ToYarn(oldJobID);
			GetTaskReportsRequest request = recordFactory.NewRecordInstance<GetTaskReportsRequest
				>();
			request.SetJobId(jobId);
			request.SetTaskType(TypeConverter.ToYarn(taskType));
			IList<TaskReport> taskReports = ((GetTaskReportsResponse)Invoke("getTaskReports", 
				typeof(GetTaskReportsRequest), request)).GetTaskReportList();
			return Sharpen.Collections.ToArray(TypeConverter.FromYarn(taskReports), new TaskReport
				[0]);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual bool KillTask(TaskAttemptID taskAttemptID, bool fail)
		{
			TaskAttemptId attemptID = TypeConverter.ToYarn(taskAttemptID);
			if (fail)
			{
				FailTaskAttemptRequest failRequest = recordFactory.NewRecordInstance<FailTaskAttemptRequest
					>();
				failRequest.SetTaskAttemptId(attemptID);
				Invoke("failTaskAttempt", typeof(FailTaskAttemptRequest), failRequest);
			}
			else
			{
				KillTaskAttemptRequest killRequest = recordFactory.NewRecordInstance<KillTaskAttemptRequest
					>();
				killRequest.SetTaskAttemptId(attemptID);
				Invoke("killTaskAttempt", typeof(KillTaskAttemptRequest), killRequest);
			}
			return true;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual bool KillJob(JobID oldJobID)
		{
			JobId jobId = TypeConverter.ToYarn(oldJobID);
			KillJobRequest killRequest = recordFactory.NewRecordInstance<KillJobRequest>();
			killRequest.SetJobId(jobId);
			Invoke("killJob", typeof(KillJobRequest), killRequest);
			return true;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual LogParams GetLogFilePath(JobID oldJobID, TaskAttemptID oldTaskAttemptID
			)
		{
			JobId jobId = TypeConverter.ToYarn(oldJobID);
			GetJobReportRequest request = recordFactory.NewRecordInstance<GetJobReportRequest
				>();
			request.SetJobId(jobId);
			JobReport report = ((GetJobReportResponse)Invoke("getJobReport", typeof(GetJobReportRequest
				), request)).GetJobReport();
			if (EnumSet.Of(JobState.Succeeded, JobState.Failed, JobState.Killed, JobState.Error
				).Contains(report.GetJobState()))
			{
				if (oldTaskAttemptID != null)
				{
					GetTaskAttemptReportRequest taRequest = recordFactory.NewRecordInstance<GetTaskAttemptReportRequest
						>();
					taRequest.SetTaskAttemptId(TypeConverter.ToYarn(oldTaskAttemptID));
					TaskAttemptReport taReport = ((GetTaskAttemptReportResponse)Invoke("getTaskAttemptReport"
						, typeof(GetTaskAttemptReportRequest), taRequest)).GetTaskAttemptReport();
					if (taReport.GetContainerId() == null || taReport.GetNodeManagerHost() == null)
					{
						throw new IOException("Unable to get log information for task: " + oldTaskAttemptID
							);
					}
					return new LogParams(taReport.GetContainerId().ToString(), taReport.GetContainerId
						().GetApplicationAttemptId().GetApplicationId().ToString(), NodeId.NewInstance(taReport
						.GetNodeManagerHost(), taReport.GetNodeManagerPort()).ToString(), report.GetUser
						());
				}
				else
				{
					if (report.GetAMInfos() == null || report.GetAMInfos().Count == 0)
					{
						throw new IOException("Unable to get log information for job: " + oldJobID);
					}
					AMInfo amInfo = report.GetAMInfos()[report.GetAMInfos().Count - 1];
					return new LogParams(amInfo.GetContainerId().ToString(), amInfo.GetAppAttemptId()
						.GetApplicationId().ToString(), NodeId.NewInstance(amInfo.GetNodeManagerHost(), 
						amInfo.GetNodeManagerPort()).ToString(), report.GetUser());
				}
			}
			else
			{
				throw new IOException("Cannot get log path for a in-progress job");
			}
		}
	}
}
