using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Com.Google.Common.Annotations;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Protocol;
using Org.Apache.Hadoop.Mapreduce.Security.Token.Delegation;
using Org.Apache.Hadoop.Mapreduce.V2;
using Org.Apache.Hadoop.Mapreduce.V2.Api;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Protocolrecords;
using Org.Apache.Hadoop.Mapreduce.V2.Jobhistory;
using Org.Apache.Hadoop.Mapreduce.V2.Util;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Authorize;
using Org.Apache.Hadoop.Yarn.Api;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Factories;
using Org.Apache.Hadoop.Yarn.Factory.Providers;
using Org.Apache.Hadoop.Yarn.Security.Client;
using Org.Apache.Hadoop.Yarn.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	/// <summary>This class enables the current JobClient (0.22 hadoop) to run on YARN.</summary>
	public class YARNRunner : ClientProtocol
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Mapred.YARNRunner
			));

		private readonly RecordFactory recordFactory = RecordFactoryProvider.GetRecordFactory
			(null);

		private ResourceMgrDelegate resMgrDelegate;

		private ClientCache clientCache;

		private Configuration conf;

		private readonly FileContext defaultFileContext;

		/// <summary>
		/// Yarn runner incapsulates the client interface of
		/// yarn
		/// </summary>
		/// <param name="conf">the configuration object for the client</param>
		public YARNRunner(Configuration conf)
			: this(conf, new ResourceMgrDelegate(new YarnConfiguration(conf)))
		{
		}

		/// <summary>
		/// Similar to
		/// <see cref="YARNRunner(Org.Apache.Hadoop.Conf.Configuration)"/>
		/// but allowing injecting
		/// <see cref="ResourceMgrDelegate"/>
		/// . Enables mocking and testing.
		/// </summary>
		/// <param name="conf">the configuration object for the client</param>
		/// <param name="resMgrDelegate">the resourcemanager client handle.</param>
		public YARNRunner(Configuration conf, ResourceMgrDelegate resMgrDelegate)
			: this(conf, resMgrDelegate, new ClientCache(conf, resMgrDelegate))
		{
		}

		/// <summary>
		/// Similar to
		/// <see cref="YARNRunner(Org.Apache.Hadoop.Conf.Configuration, ResourceMgrDelegate)"
		/// 	/>
		/// but allowing injecting
		/// <see cref="ClientCache"/>
		/// . Enable mocking and testing.
		/// </summary>
		/// <param name="conf">the configuration object</param>
		/// <param name="resMgrDelegate">the resource manager delegate</param>
		/// <param name="clientCache">the client cache object.</param>
		public YARNRunner(Configuration conf, ResourceMgrDelegate resMgrDelegate, ClientCache
			 clientCache)
		{
			this.conf = conf;
			try
			{
				this.resMgrDelegate = resMgrDelegate;
				this.clientCache = clientCache;
				this.defaultFileContext = FileContext.GetFileContext(this.conf);
			}
			catch (UnsupportedFileSystemException ufe)
			{
				throw new RuntimeException("Error in instantiating YarnClient", ufe);
			}
		}

		[InterfaceAudience.Private]
		public virtual void SetResourceMgrDelegate(ResourceMgrDelegate resMgrDelegate)
		{
			this.resMgrDelegate = resMgrDelegate;
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public override void CancelDelegationToken(Org.Apache.Hadoop.Security.Token.Token
			<DelegationTokenIdentifier> arg0)
		{
			throw new NotSupportedException("Use Token.renew instead");
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public override TaskTrackerInfo[] GetActiveTrackers()
		{
			return resMgrDelegate.GetActiveTrackers();
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public override JobStatus[] GetAllJobs()
		{
			return resMgrDelegate.GetAllJobs();
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public override TaskTrackerInfo[] GetBlacklistedTrackers()
		{
			return resMgrDelegate.GetBlacklistedTrackers();
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public override ClusterMetrics GetClusterMetrics()
		{
			return resMgrDelegate.GetClusterMetrics();
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		[VisibleForTesting]
		internal virtual void AddHistoryToken(Credentials ts)
		{
			/* check if we have a hsproxy, if not, no need */
			MRClientProtocol hsProxy = clientCache.GetInitializedHSProxy();
			if (UserGroupInformation.IsSecurityEnabled() && (hsProxy != null))
			{
				/*
				* note that get delegation token was called. Again this is hack for oozie
				* to make sure we add history server delegation tokens to the credentials
				*/
				RMDelegationTokenSelector tokenSelector = new RMDelegationTokenSelector();
				Text service = resMgrDelegate.GetRMDelegationTokenService();
				if (tokenSelector.SelectToken(service, ts.GetAllTokens()) != null)
				{
					Text hsService = SecurityUtil.BuildTokenService(hsProxy.GetConnectAddress());
					if (ts.GetToken(hsService) == null)
					{
						ts.AddToken(hsService, GetDelegationTokenFromHS(hsProxy));
					}
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		[VisibleForTesting]
		internal virtual Org.Apache.Hadoop.Security.Token.Token<object> GetDelegationTokenFromHS
			(MRClientProtocol hsProxy)
		{
			GetDelegationTokenRequest request = recordFactory.NewRecordInstance<GetDelegationTokenRequest
				>();
			request.SetRenewer(Master.GetMasterPrincipal(conf));
			Org.Apache.Hadoop.Yarn.Api.Records.Token mrDelegationToken;
			mrDelegationToken = hsProxy.GetDelegationToken(request).GetDelegationToken();
			return ConverterUtils.ConvertFromYarn(mrDelegationToken, hsProxy.GetConnectAddress
				());
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public override Org.Apache.Hadoop.Security.Token.Token<DelegationTokenIdentifier>
			 GetDelegationToken(Text renewer)
		{
			// The token is only used for serialization. So the type information
			// mismatch should be fine.
			return resMgrDelegate.GetDelegationToken(renewer);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public override string GetFilesystemName()
		{
			return resMgrDelegate.GetFilesystemName();
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public override JobID GetNewJobID()
		{
			return resMgrDelegate.GetNewJobID();
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public override QueueInfo GetQueue(string queueName)
		{
			return resMgrDelegate.GetQueue(queueName);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public override QueueAclsInfo[] GetQueueAclsForCurrentUser()
		{
			return resMgrDelegate.GetQueueAclsForCurrentUser();
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public override QueueInfo[] GetQueues()
		{
			return resMgrDelegate.GetQueues();
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public override QueueInfo[] GetRootQueues()
		{
			return resMgrDelegate.GetRootQueues();
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public override QueueInfo[] GetChildQueues(string parent)
		{
			return resMgrDelegate.GetChildQueues(parent);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public override string GetStagingAreaDir()
		{
			return resMgrDelegate.GetStagingAreaDir();
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public override string GetSystemDir()
		{
			return resMgrDelegate.GetSystemDir();
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public override long GetTaskTrackerExpiryInterval()
		{
			return resMgrDelegate.GetTaskTrackerExpiryInterval();
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public override JobStatus SubmitJob(JobID jobId, string jobSubmitDir, Credentials
			 ts)
		{
			AddHistoryToken(ts);
			// Construct necessary information to start the MR AM
			ApplicationSubmissionContext appContext = CreateApplicationSubmissionContext(conf
				, jobSubmitDir, ts);
			// Submit to ResourceManager
			try
			{
				ApplicationId applicationId = resMgrDelegate.SubmitApplication(appContext);
				ApplicationReport appMaster = resMgrDelegate.GetApplicationReport(applicationId);
				string diagnostics = (appMaster == null ? "application report is null" : appMaster
					.GetDiagnostics());
				if (appMaster == null || appMaster.GetYarnApplicationState() == YarnApplicationState
					.Failed || appMaster.GetYarnApplicationState() == YarnApplicationState.Killed)
				{
					throw new IOException("Failed to run job : " + diagnostics);
				}
				return clientCache.GetClient(jobId).GetJobStatus(jobId);
			}
			catch (YarnException e)
			{
				throw new IOException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private LocalResource CreateApplicationResource(FileContext fs, Path p, LocalResourceType
			 type)
		{
			LocalResource rsrc = recordFactory.NewRecordInstance<LocalResource>();
			FileStatus rsrcStat = fs.GetFileStatus(p);
			rsrc.SetResource(ConverterUtils.GetYarnUrlFromPath(fs.GetDefaultFileSystem().ResolvePath
				(rsrcStat.GetPath())));
			rsrc.SetSize(rsrcStat.GetLen());
			rsrc.SetTimestamp(rsrcStat.GetModificationTime());
			rsrc.SetType(type);
			rsrc.SetVisibility(LocalResourceVisibility.Application);
			return rsrc;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual ApplicationSubmissionContext CreateApplicationSubmissionContext(Configuration
			 jobConf, string jobSubmitDir, Credentials ts)
		{
			ApplicationId applicationId = resMgrDelegate.GetApplicationId();
			// Setup resource requirements
			Resource capability = recordFactory.NewRecordInstance<Resource>();
			capability.SetMemory(conf.GetInt(MRJobConfig.MrAmVmemMb, MRJobConfig.DefaultMrAmVmemMb
				));
			capability.SetVirtualCores(conf.GetInt(MRJobConfig.MrAmCpuVcores, MRJobConfig.DefaultMrAmCpuVcores
				));
			Log.Debug("AppMaster capability = " + capability);
			// Setup LocalResources
			IDictionary<string, LocalResource> localResources = new Dictionary<string, LocalResource
				>();
			Path jobConfPath = new Path(jobSubmitDir, MRJobConfig.JobConfFile);
			URL yarnUrlForJobSubmitDir = ConverterUtils.GetYarnUrlFromPath(defaultFileContext
				.GetDefaultFileSystem().ResolvePath(defaultFileContext.MakeQualified(new Path(jobSubmitDir
				))));
			Log.Debug("Creating setup context, jobSubmitDir url is " + yarnUrlForJobSubmitDir
				);
			localResources[MRJobConfig.JobConfFile] = CreateApplicationResource(defaultFileContext
				, jobConfPath, LocalResourceType.File);
			if (jobConf.Get(MRJobConfig.Jar) != null)
			{
				Path jobJarPath = new Path(jobConf.Get(MRJobConfig.Jar));
				LocalResource rc = CreateApplicationResource(FileContext.GetFileContext(jobJarPath
					.ToUri(), jobConf), jobJarPath, LocalResourceType.Pattern);
				string pattern = conf.GetPattern(JobContext.JarUnpackPattern, JobConf.UnpackJarPatternDefault
					).Pattern();
				rc.SetPattern(pattern);
				localResources[MRJobConfig.JobJar] = rc;
			}
			else
			{
				// Job jar may be null. For e.g, for pipes, the job jar is the hadoop
				// mapreduce jar itself which is already on the classpath.
				Log.Info("Job jar is not present. " + "Not adding any jar to the list of resources."
					);
			}
			// TODO gross hack
			foreach (string s in new string[] { MRJobConfig.JobSplit, MRJobConfig.JobSplitMetainfo
				 })
			{
				localResources[MRJobConfig.JobSubmitDir + "/" + s] = CreateApplicationResource(defaultFileContext
					, new Path(jobSubmitDir, s), LocalResourceType.File);
			}
			// Setup security tokens
			DataOutputBuffer dob = new DataOutputBuffer();
			ts.WriteTokenStorageToStream(dob);
			ByteBuffer securityTokens = ByteBuffer.Wrap(dob.GetData(), 0, dob.GetLength());
			// Setup the command to run the AM
			IList<string> vargs = new AList<string>(8);
			vargs.AddItem(MRApps.CrossPlatformifyMREnv(jobConf, ApplicationConstants.Environment
				.JavaHome) + "/bin/java");
			Path amTmpDir = new Path(MRApps.CrossPlatformifyMREnv(conf, ApplicationConstants.Environment
				.Pwd), YarnConfiguration.DefaultContainerTempDir);
			vargs.AddItem("-Djava.io.tmpdir=" + amTmpDir);
			MRApps.AddLog4jSystemProperties(null, vargs, conf);
			// Check for Java Lib Path usage in MAP and REDUCE configs
			WarnForJavaLibPath(conf.Get(MRJobConfig.MapJavaOpts, string.Empty), "map", MRJobConfig
				.MapJavaOpts, MRJobConfig.MapEnv);
			WarnForJavaLibPath(conf.Get(MRJobConfig.MapredMapAdminJavaOpts, string.Empty), "map"
				, MRJobConfig.MapredMapAdminJavaOpts, MRJobConfig.MapredAdminUserEnv);
			WarnForJavaLibPath(conf.Get(MRJobConfig.ReduceJavaOpts, string.Empty), "reduce", 
				MRJobConfig.ReduceJavaOpts, MRJobConfig.ReduceEnv);
			WarnForJavaLibPath(conf.Get(MRJobConfig.MapredReduceAdminJavaOpts, string.Empty), 
				"reduce", MRJobConfig.MapredReduceAdminJavaOpts, MRJobConfig.MapredAdminUserEnv);
			// Add AM admin command opts before user command opts
			// so that it can be overridden by user
			string mrAppMasterAdminOptions = conf.Get(MRJobConfig.MrAmAdminCommandOpts, MRJobConfig
				.DefaultMrAmAdminCommandOpts);
			WarnForJavaLibPath(mrAppMasterAdminOptions, "app master", MRJobConfig.MrAmAdminCommandOpts
				, MRJobConfig.MrAmAdminUserEnv);
			vargs.AddItem(mrAppMasterAdminOptions);
			// Add AM user command opts
			string mrAppMasterUserOptions = conf.Get(MRJobConfig.MrAmCommandOpts, MRJobConfig
				.DefaultMrAmCommandOpts);
			WarnForJavaLibPath(mrAppMasterUserOptions, "app master", MRJobConfig.MrAmCommandOpts
				, MRJobConfig.MrAmEnv);
			vargs.AddItem(mrAppMasterUserOptions);
			if (jobConf.GetBoolean(MRJobConfig.MrAmProfile, MRJobConfig.DefaultMrAmProfile))
			{
				string profileParams = jobConf.Get(MRJobConfig.MrAmProfileParams, MRJobConfig.DefaultTaskProfileParams
					);
				if (profileParams != null)
				{
					vargs.AddItem(string.Format(profileParams, ApplicationConstants.LogDirExpansionVar
						 + Path.Separator + TaskLog.LogName.Profile));
				}
			}
			vargs.AddItem(MRJobConfig.ApplicationMasterClass);
			vargs.AddItem("1>" + ApplicationConstants.LogDirExpansionVar + Path.Separator + ApplicationConstants
				.Stdout);
			vargs.AddItem("2>" + ApplicationConstants.LogDirExpansionVar + Path.Separator + ApplicationConstants
				.Stderr);
			Vector<string> vargsFinal = new Vector<string>(8);
			// Final command
			StringBuilder mergedCommand = new StringBuilder();
			foreach (CharSequence str in vargs)
			{
				mergedCommand.Append(str).Append(" ");
			}
			vargsFinal.AddItem(mergedCommand.ToString());
			Log.Debug("Command to launch container for ApplicationMaster is : " + mergedCommand
				);
			// Setup the CLASSPATH in environment
			// i.e. add { Hadoop jars, job jar, CWD } to classpath.
			IDictionary<string, string> environment = new Dictionary<string, string>();
			MRApps.SetClasspath(environment, conf);
			// Shell
			environment[ApplicationConstants.Environment.Shell.ToString()] = conf.Get(MRJobConfig
				.MapredAdminUserShell, MRJobConfig.DefaultShell);
			// Add the container working directory at the front of LD_LIBRARY_PATH
			MRApps.AddToEnvironment(environment, ApplicationConstants.Environment.LdLibraryPath
				.ToString(), MRApps.CrossPlatformifyMREnv(conf, ApplicationConstants.Environment
				.Pwd), conf);
			// Setup the environment variables for Admin first
			MRApps.SetEnvFromInputString(environment, conf.Get(MRJobConfig.MrAmAdminUserEnv), 
				conf);
			// Setup the environment variables (LD_LIBRARY_PATH, etc)
			MRApps.SetEnvFromInputString(environment, conf.Get(MRJobConfig.MrAmEnv), conf);
			// Parse distributed cache
			MRApps.SetupDistributedCache(jobConf, localResources);
			IDictionary<ApplicationAccessType, string> acls = new Dictionary<ApplicationAccessType
				, string>(2);
			acls[ApplicationAccessType.ViewApp] = jobConf.Get(MRJobConfig.JobAclViewJob, MRJobConfig
				.DefaultJobAclViewJob);
			acls[ApplicationAccessType.ModifyApp] = jobConf.Get(MRJobConfig.JobAclModifyJob, 
				MRJobConfig.DefaultJobAclModifyJob);
			// Setup ContainerLaunchContext for AM container
			ContainerLaunchContext amContainer = ContainerLaunchContext.NewInstance(localResources
				, environment, vargsFinal, null, securityTokens, acls);
			ICollection<string> tagsFromConf = jobConf.GetTrimmedStringCollection(MRJobConfig
				.JobTags);
			// Set up the ApplicationSubmissionContext
			ApplicationSubmissionContext appContext = recordFactory.NewRecordInstance<ApplicationSubmissionContext
				>();
			appContext.SetApplicationId(applicationId);
			// ApplicationId
			appContext.SetQueue(jobConf.Get(JobContext.QueueName, YarnConfiguration.DefaultQueueName
				));
			// Queue name
			// add reservationID if present
			ReservationId reservationID = null;
			try
			{
				reservationID = ReservationId.ParseReservationId(jobConf.Get(JobContext.ReservationId
					));
			}
			catch (FormatException)
			{
				// throw exception as reservationid as is invalid
				string errMsg = "Invalid reservationId: " + jobConf.Get(JobContext.ReservationId)
					 + " specified for the app: " + applicationId;
				Log.Warn(errMsg);
				throw new IOException(errMsg);
			}
			if (reservationID != null)
			{
				appContext.SetReservationID(reservationID);
				Log.Info("SUBMITTING ApplicationSubmissionContext app:" + applicationId + " to queue:"
					 + appContext.GetQueue() + " with reservationId:" + appContext.GetReservationID(
					));
			}
			appContext.SetApplicationName(jobConf.Get(JobContext.JobName, YarnConfiguration.DefaultApplicationName
				));
			// Job name
			appContext.SetCancelTokensWhenComplete(conf.GetBoolean(MRJobConfig.JobCancelDelegationToken
				, true));
			appContext.SetAMContainerSpec(amContainer);
			// AM Container
			appContext.SetMaxAppAttempts(conf.GetInt(MRJobConfig.MrAmMaxAttempts, MRJobConfig
				.DefaultMrAmMaxAttempts));
			appContext.SetResource(capability);
			appContext.SetApplicationType(MRJobConfig.MrApplicationType);
			if (tagsFromConf != null && !tagsFromConf.IsEmpty())
			{
				appContext.SetApplicationTags(new HashSet<string>(tagsFromConf));
			}
			return appContext;
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public override void SetJobPriority(JobID arg0, string arg1)
		{
			resMgrDelegate.SetJobPriority(arg0, arg1);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual long GetProtocolVersion(string arg0, long arg1)
		{
			return resMgrDelegate.GetProtocolVersion(arg0, arg1);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public override long RenewDelegationToken(Org.Apache.Hadoop.Security.Token.Token<
			DelegationTokenIdentifier> arg0)
		{
			throw new NotSupportedException("Use Token.renew instead");
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public override Counters GetJobCounters(JobID arg0)
		{
			return clientCache.GetClient(arg0).GetJobCounters(arg0);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public override string GetJobHistoryDir()
		{
			return JobHistoryUtils.GetConfiguredHistoryServerDoneDirPrefix(conf);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public override JobStatus GetJobStatus(JobID jobID)
		{
			JobStatus status = clientCache.GetClient(jobID).GetJobStatus(jobID);
			return status;
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public override TaskCompletionEvent[] GetTaskCompletionEvents(JobID arg0, int arg1
			, int arg2)
		{
			return clientCache.GetClient(arg0).GetTaskCompletionEvents(arg0, arg1, arg2);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public override string[] GetTaskDiagnostics(TaskAttemptID arg0)
		{
			return clientCache.GetClient(arg0.GetJobID()).GetTaskDiagnostics(arg0);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public override TaskReport[] GetTaskReports(JobID jobID, TaskType taskType)
		{
			return clientCache.GetClient(jobID).GetTaskReports(jobID, taskType);
		}

		/// <exception cref="System.IO.IOException"/>
		private void KillUnFinishedApplication(ApplicationId appId)
		{
			ApplicationReport application = null;
			try
			{
				application = resMgrDelegate.GetApplicationReport(appId);
			}
			catch (YarnException e)
			{
				throw new IOException(e);
			}
			if (application.GetYarnApplicationState() == YarnApplicationState.Finished || application
				.GetYarnApplicationState() == YarnApplicationState.Failed || application.GetYarnApplicationState
				() == YarnApplicationState.Killed)
			{
				return;
			}
			KillApplication(appId);
		}

		/// <exception cref="System.IO.IOException"/>
		private void KillApplication(ApplicationId appId)
		{
			try
			{
				resMgrDelegate.KillApplication(appId);
			}
			catch (YarnException e)
			{
				throw new IOException(e);
			}
		}

		private bool IsJobInTerminalState(JobStatus status)
		{
			return status.GetState() == JobStatus.State.Killed || status.GetState() == JobStatus.State
				.Failed || status.GetState() == JobStatus.State.Succeeded;
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public override void KillJob(JobID arg0)
		{
			/* check if the status is not running, if not send kill to RM */
			JobStatus status = clientCache.GetClient(arg0).GetJobStatus(arg0);
			ApplicationId appId = TypeConverter.ToYarn(arg0).GetAppId();
			// get status from RM and return
			if (status == null)
			{
				KillUnFinishedApplication(appId);
				return;
			}
			if (status.GetState() != JobStatus.State.Running)
			{
				KillApplication(appId);
				return;
			}
			try
			{
				/* send a kill to the AM */
				clientCache.GetClient(arg0).KillJob(arg0);
				long currentTimeMillis = Runtime.CurrentTimeMillis();
				long timeKillIssued = currentTimeMillis;
				long killTimeOut = conf.GetLong(MRJobConfig.MrAmHardKillTimeoutMs, MRJobConfig.DefaultMrAmHardKillTimeoutMs
					);
				while ((currentTimeMillis < timeKillIssued + killTimeOut) && !IsJobInTerminalState
					(status))
				{
					try
					{
						Sharpen.Thread.Sleep(1000L);
					}
					catch (Exception)
					{
						break;
					}
					currentTimeMillis = Runtime.CurrentTimeMillis();
					status = clientCache.GetClient(arg0).GetJobStatus(arg0);
					if (status == null)
					{
						KillUnFinishedApplication(appId);
						return;
					}
				}
			}
			catch (IOException io)
			{
				Log.Debug("Error when checking for application status", io);
			}
			if (status != null && !IsJobInTerminalState(status))
			{
				KillApplication(appId);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public override bool KillTask(TaskAttemptID arg0, bool arg1)
		{
			return clientCache.GetClient(arg0.GetJobID()).KillTask(arg0, arg1);
		}

		/// <exception cref="System.IO.IOException"/>
		public override AccessControlList GetQueueAdmins(string arg0)
		{
			return new AccessControlList("*");
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public override Cluster.JobTrackerStatus GetJobTrackerStatus()
		{
			return Cluster.JobTrackerStatus.Running;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual ProtocolSignature GetProtocolSignature(string protocol, long clientVersion
			, int clientMethodsHash)
		{
			return ProtocolSignature.GetProtocolSignature(this, protocol, clientVersion, clientMethodsHash
				);
		}

		/// <exception cref="System.IO.IOException"/>
		public override LogParams GetLogFileParams(JobID jobID, TaskAttemptID taskAttemptID
			)
		{
			return clientCache.GetClient(jobID).GetLogFilePath(jobID, taskAttemptID);
		}

		private static void WarnForJavaLibPath(string opts, string component, string javaConf
			, string envConf)
		{
			if (opts != null && opts.Contains("-Djava.library.path"))
			{
				Log.Warn("Usage of -Djava.library.path in " + javaConf + " can cause " + "programs to no longer function if hadoop native libraries "
					 + "are used. These values should be set as part of the " + "LD_LIBRARY_PATH in the "
					 + component + " JVM env using " + envConf + " config settings.");
			}
		}
	}
}
