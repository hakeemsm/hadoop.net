using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.V2.Api;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Protocolrecords;
using Org.Apache.Hadoop.Mapreduce.V2.Util;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Yarn.Api;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Client.Api.Impl;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Factories;
using Org.Apache.Hadoop.Yarn.Factory.Providers;
using Org.Apache.Hadoop.Yarn.Security.Client;
using Org.Apache.Log4j;
using Org.Mockito;
using Org.Mockito.Invocation;
using Org.Mockito.Stubbing;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	/// <summary>
	/// Test YarnRunner and make sure the client side plugin works
	/// fine
	/// </summary>
	public class TestYARNRunner : TestCase
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(TestYARNRunner));

		private static readonly RecordFactory recordFactory = RecordFactoryProvider.GetRecordFactory
			(null);

		private static readonly string ProfileParams = Sharpen.Runtime.Substring(MRJobConfig
			.DefaultTaskProfileParams, 0, MRJobConfig.DefaultTaskProfileParams.LastIndexOf("%"
			));

		private YARNRunner yarnRunner;

		private ResourceMgrDelegate resourceMgrDelegate;

		private YarnConfiguration conf;

		private ClientCache clientCache;

		private ApplicationId appId;

		private JobID jobId;

		private FilePath testWorkDir = new FilePath("target", typeof(TestYARNRunner).FullName
			);

		private ApplicationSubmissionContext submissionContext;

		private ClientServiceDelegate clientDelegate;

		private const string failString = "Rejected job";

		// prefix before <LOG_DIR>/profile.out
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		protected override void SetUp()
		{
			resourceMgrDelegate = Org.Mockito.Mockito.Mock<ResourceMgrDelegate>();
			conf = new YarnConfiguration();
			conf.Set(YarnConfiguration.RmPrincipal, "mapred/host@REALM");
			clientCache = new ClientCache(conf, resourceMgrDelegate);
			clientCache = Org.Mockito.Mockito.Spy(clientCache);
			yarnRunner = new YARNRunner(conf, resourceMgrDelegate, clientCache);
			yarnRunner = Org.Mockito.Mockito.Spy(yarnRunner);
			submissionContext = Org.Mockito.Mockito.Mock<ApplicationSubmissionContext>();
			Org.Mockito.Mockito.DoAnswer(new _Answer_146(this)).When(yarnRunner).CreateApplicationSubmissionContext
				(Matchers.Any<Configuration>(), Matchers.Any<string>(), Matchers.Any<Credentials
				>());
			appId = ApplicationId.NewInstance(Runtime.CurrentTimeMillis(), 1);
			jobId = TypeConverter.FromYarn(appId);
			if (testWorkDir.Exists())
			{
				FileContext.GetLocalFSFileContext().Delete(new Path(testWorkDir.ToString()), true
					);
			}
			testWorkDir.Mkdirs();
		}

		private sealed class _Answer_146 : Answer<ApplicationSubmissionContext>
		{
			public _Answer_146(TestYARNRunner _enclosing)
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="System.Exception"/>
			public ApplicationSubmissionContext Answer(InvocationOnMock invocation)
			{
				return this._enclosing.submissionContext;
			}

			private readonly TestYARNRunner _enclosing;
		}

		[TearDown]
		public virtual void Cleanup()
		{
			FileUtil.FullyDelete(testWorkDir);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestJobKill()
		{
			clientDelegate = Org.Mockito.Mockito.Mock<ClientServiceDelegate>();
			Org.Mockito.Mockito.When(clientDelegate.GetJobStatus(Matchers.Any<JobID>())).ThenReturn
				(new JobStatus(jobId, 0f, 0f, 0f, 0f, JobStatus.State.Prep, JobPriority.High, "tmp"
				, "tmp", "tmp", "tmp"));
			Org.Mockito.Mockito.When(clientDelegate.KillJob(Matchers.Any<JobID>())).ThenReturn
				(true);
			Org.Mockito.Mockito.DoAnswer(new _Answer_177(this)).When(clientCache).GetClient(Matchers.Any
				<JobID>());
			yarnRunner.KillJob(jobId);
			Org.Mockito.Mockito.Verify(resourceMgrDelegate).KillApplication(appId);
			Org.Mockito.Mockito.When(clientDelegate.GetJobStatus(Matchers.Any<JobID>())).ThenReturn
				(new JobStatus(jobId, 0f, 0f, 0f, 0f, JobStatus.State.Running, JobPriority.High, 
				"tmp", "tmp", "tmp", "tmp"));
			yarnRunner.KillJob(jobId);
			Org.Mockito.Mockito.Verify(clientDelegate).KillJob(jobId);
			Org.Mockito.Mockito.When(clientDelegate.GetJobStatus(Matchers.Any<JobID>())).ThenReturn
				(null);
			Org.Mockito.Mockito.When(resourceMgrDelegate.GetApplicationReport(Matchers.Any<ApplicationId
				>())).ThenReturn(ApplicationReport.NewInstance(appId, null, "tmp", "tmp", "tmp", 
				"tmp", 0, null, YarnApplicationState.Finished, "tmp", "tmp", 0l, 0l, FinalApplicationStatus
				.Succeeded, null, null, 0f, "tmp", null));
			yarnRunner.KillJob(jobId);
			Org.Mockito.Mockito.Verify(clientDelegate).KillJob(jobId);
		}

		private sealed class _Answer_177 : Answer<ClientServiceDelegate>
		{
			public _Answer_177(TestYARNRunner _enclosing)
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="System.Exception"/>
			public ClientServiceDelegate Answer(InvocationOnMock invocation)
			{
				return this._enclosing.clientDelegate;
			}

			private readonly TestYARNRunner _enclosing;
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestJobKillTimeout()
		{
			long timeToWaitBeforeHardKill = 10000 + MRJobConfig.DefaultMrAmHardKillTimeoutMs;
			conf.SetLong(MRJobConfig.MrAmHardKillTimeoutMs, timeToWaitBeforeHardKill);
			clientDelegate = Org.Mockito.Mockito.Mock<ClientServiceDelegate>();
			Org.Mockito.Mockito.DoAnswer(new _Answer_212(this)).When(clientCache).GetClient(Matchers.Any
				<JobID>());
			Org.Mockito.Mockito.When(clientDelegate.GetJobStatus(Matchers.Any<JobID>())).ThenReturn
				(new JobStatus(jobId, 0f, 0f, 0f, 0f, JobStatus.State.Running, JobPriority.High, 
				"tmp", "tmp", "tmp", "tmp"));
			long startTimeMillis = Runtime.CurrentTimeMillis();
			yarnRunner.KillJob(jobId);
			NUnit.Framework.Assert.IsTrue("killJob should have waited at least " + timeToWaitBeforeHardKill
				 + " ms.", Runtime.CurrentTimeMillis() - startTimeMillis >= timeToWaitBeforeHardKill
				);
		}

		private sealed class _Answer_212 : Answer<ClientServiceDelegate>
		{
			public _Answer_212(TestYARNRunner _enclosing)
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="System.Exception"/>
			public ClientServiceDelegate Answer(InvocationOnMock invocation)
			{
				return this._enclosing.clientDelegate;
			}

			private readonly TestYARNRunner _enclosing;
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestJobSubmissionFailure()
		{
			Org.Mockito.Mockito.When(resourceMgrDelegate.SubmitApplication(Matchers.Any<ApplicationSubmissionContext
				>())).ThenReturn(appId);
			ApplicationReport report = Org.Mockito.Mockito.Mock<ApplicationReport>();
			Org.Mockito.Mockito.When(report.GetApplicationId()).ThenReturn(appId);
			Org.Mockito.Mockito.When(report.GetDiagnostics()).ThenReturn(failString);
			Org.Mockito.Mockito.When(report.GetYarnApplicationState()).ThenReturn(YarnApplicationState
				.Failed);
			Org.Mockito.Mockito.When(resourceMgrDelegate.GetApplicationReport(appId)).ThenReturn
				(report);
			Credentials credentials = new Credentials();
			FilePath jobxml = new FilePath(testWorkDir, "job.xml");
			OutputStream @out = new FileOutputStream(jobxml);
			conf.WriteXml(@out);
			@out.Close();
			try
			{
				yarnRunner.SubmitJob(jobId, testWorkDir.GetAbsolutePath().ToString(), credentials
					);
			}
			catch (IOException io)
			{
				Log.Info("Logging exception:", io);
				NUnit.Framework.Assert.IsTrue(io.GetLocalizedMessage().Contains(failString));
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestResourceMgrDelegate()
		{
			/* we not want a mock of resource mgr delegate */
			ApplicationClientProtocol clientRMProtocol = Org.Mockito.Mockito.Mock<ApplicationClientProtocol
				>();
			ResourceMgrDelegate delegate_ = new _ResourceMgrDelegate_256(clientRMProtocol, conf
				);
			/* make sure kill calls finish application master */
			Org.Mockito.Mockito.When(clientRMProtocol.ForceKillApplication(Matchers.Any<KillApplicationRequest
				>())).ThenReturn(KillApplicationResponse.NewInstance(true));
			delegate_.KillApplication(appId);
			Org.Mockito.Mockito.Verify(clientRMProtocol).ForceKillApplication(Matchers.Any<KillApplicationRequest
				>());
			/* make sure getalljobs calls get all applications */
			Org.Mockito.Mockito.When(clientRMProtocol.GetApplications(Matchers.Any<GetApplicationsRequest
				>())).ThenReturn(recordFactory.NewRecordInstance<GetApplicationsResponse>());
			delegate_.GetAllJobs();
			Org.Mockito.Mockito.Verify(clientRMProtocol).GetApplications(Matchers.Any<GetApplicationsRequest
				>());
			/* make sure getapplication report is called */
			Org.Mockito.Mockito.When(clientRMProtocol.GetApplicationReport(Matchers.Any<GetApplicationReportRequest
				>())).ThenReturn(recordFactory.NewRecordInstance<GetApplicationReportResponse>()
				);
			delegate_.GetApplicationReport(appId);
			Org.Mockito.Mockito.Verify(clientRMProtocol).GetApplicationReport(Matchers.Any<GetApplicationReportRequest
				>());
			/* make sure metrics is called */
			GetClusterMetricsResponse clusterMetricsResponse = recordFactory.NewRecordInstance
				<GetClusterMetricsResponse>();
			clusterMetricsResponse.SetClusterMetrics(recordFactory.NewRecordInstance<YarnClusterMetrics
				>());
			Org.Mockito.Mockito.When(clientRMProtocol.GetClusterMetrics(Matchers.Any<GetClusterMetricsRequest
				>())).ThenReturn(clusterMetricsResponse);
			delegate_.GetClusterMetrics();
			Org.Mockito.Mockito.Verify(clientRMProtocol).GetClusterMetrics(Matchers.Any<GetClusterMetricsRequest
				>());
			Org.Mockito.Mockito.When(clientRMProtocol.GetClusterNodes(Matchers.Any<GetClusterNodesRequest
				>())).ThenReturn(recordFactory.NewRecordInstance<GetClusterNodesResponse>());
			delegate_.GetActiveTrackers();
			Org.Mockito.Mockito.Verify(clientRMProtocol).GetClusterNodes(Matchers.Any<GetClusterNodesRequest
				>());
			GetNewApplicationResponse newAppResponse = recordFactory.NewRecordInstance<GetNewApplicationResponse
				>();
			newAppResponse.SetApplicationId(appId);
			Org.Mockito.Mockito.When(clientRMProtocol.GetNewApplication(Matchers.Any<GetNewApplicationRequest
				>())).ThenReturn(newAppResponse);
			delegate_.GetNewJobID();
			Org.Mockito.Mockito.Verify(clientRMProtocol).GetNewApplication(Matchers.Any<GetNewApplicationRequest
				>());
			GetQueueInfoResponse queueInfoResponse = recordFactory.NewRecordInstance<GetQueueInfoResponse
				>();
			queueInfoResponse.SetQueueInfo(recordFactory.NewRecordInstance<QueueInfo>());
			Org.Mockito.Mockito.When(clientRMProtocol.GetQueueInfo(Matchers.Any<GetQueueInfoRequest
				>())).ThenReturn(queueInfoResponse);
			delegate_.GetQueues();
			Org.Mockito.Mockito.Verify(clientRMProtocol).GetQueueInfo(Matchers.Any<GetQueueInfoRequest
				>());
			GetQueueUserAclsInfoResponse aclResponse = recordFactory.NewRecordInstance<GetQueueUserAclsInfoResponse
				>();
			Org.Mockito.Mockito.When(clientRMProtocol.GetQueueUserAcls(Matchers.Any<GetQueueUserAclsInfoRequest
				>())).ThenReturn(aclResponse);
			delegate_.GetQueueAclsForCurrentUser();
			Org.Mockito.Mockito.Verify(clientRMProtocol).GetQueueUserAcls(Matchers.Any<GetQueueUserAclsInfoRequest
				>());
		}

		private sealed class _ResourceMgrDelegate_256 : ResourceMgrDelegate
		{
			public _ResourceMgrDelegate_256(ApplicationClientProtocol clientRMProtocol, YarnConfiguration
				 baseArg1)
				: base(baseArg1)
			{
				this.clientRMProtocol = clientRMProtocol;
			}

			/// <exception cref="System.Exception"/>
			protected override void ServiceStart()
			{
				NUnit.Framework.Assert.IsTrue(this.client is YarnClientImpl);
				((YarnClientImpl)this.client).SetRMClient(clientRMProtocol);
			}

			private readonly ApplicationClientProtocol clientRMProtocol;
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestGetHSDelegationToken()
		{
			try
			{
				Configuration conf = new Configuration();
				// Setup mock service
				IPEndPoint mockRmAddress = new IPEndPoint("localhost", 4444);
				Text rmTokenSevice = SecurityUtil.BuildTokenService(mockRmAddress);
				IPEndPoint mockHsAddress = new IPEndPoint("localhost", 9200);
				Text hsTokenSevice = SecurityUtil.BuildTokenService(mockHsAddress);
				// Setup mock rm token
				RMDelegationTokenIdentifier tokenIdentifier = new RMDelegationTokenIdentifier(new 
					Text("owner"), new Text("renewer"), new Text("real"));
				Org.Apache.Hadoop.Security.Token.Token<RMDelegationTokenIdentifier> token = new Org.Apache.Hadoop.Security.Token.Token
					<RMDelegationTokenIdentifier>(new byte[0], new byte[0], tokenIdentifier.GetKind(
					), rmTokenSevice);
				token.SetKind(RMDelegationTokenIdentifier.KindName);
				// Setup mock history token
				Org.Apache.Hadoop.Yarn.Api.Records.Token historyToken = Org.Apache.Hadoop.Yarn.Api.Records.Token
					.NewInstance(new byte[0], MRDelegationTokenIdentifier.KindName.ToString(), new byte
					[0], hsTokenSevice.ToString());
				GetDelegationTokenResponse getDtResponse = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord
					<GetDelegationTokenResponse>();
				getDtResponse.SetDelegationToken(historyToken);
				// mock services
				MRClientProtocol mockHsProxy = Org.Mockito.Mockito.Mock<MRClientProtocol>();
				Org.Mockito.Mockito.DoReturn(mockHsAddress).When(mockHsProxy).GetConnectAddress();
				Org.Mockito.Mockito.DoReturn(getDtResponse).When(mockHsProxy).GetDelegationToken(
					Matchers.Any<GetDelegationTokenRequest>());
				ResourceMgrDelegate rmDelegate = Org.Mockito.Mockito.Mock<ResourceMgrDelegate>();
				Org.Mockito.Mockito.DoReturn(rmTokenSevice).When(rmDelegate).GetRMDelegationTokenService
					();
				ClientCache clientCache = Org.Mockito.Mockito.Mock<ClientCache>();
				Org.Mockito.Mockito.DoReturn(mockHsProxy).When(clientCache).GetInitializedHSProxy
					();
				Credentials creds = new Credentials();
				YARNRunner yarnRunner = new YARNRunner(conf, rmDelegate, clientCache);
				// No HS token if no RM token
				yarnRunner.AddHistoryToken(creds);
				Org.Mockito.Mockito.Verify(mockHsProxy, Org.Mockito.Mockito.Times(0)).GetDelegationToken
					(Matchers.Any<GetDelegationTokenRequest>());
				// No HS token if RM token, but secirity disabled.
				creds.AddToken(new Text("rmdt"), token);
				yarnRunner.AddHistoryToken(creds);
				Org.Mockito.Mockito.Verify(mockHsProxy, Org.Mockito.Mockito.Times(0)).GetDelegationToken
					(Matchers.Any<GetDelegationTokenRequest>());
				conf.Set(CommonConfigurationKeys.HadoopSecurityAuthentication, "kerberos");
				UserGroupInformation.SetConfiguration(conf);
				creds = new Credentials();
				// No HS token if no RM token, security enabled
				yarnRunner.AddHistoryToken(creds);
				Org.Mockito.Mockito.Verify(mockHsProxy, Org.Mockito.Mockito.Times(0)).GetDelegationToken
					(Matchers.Any<GetDelegationTokenRequest>());
				// HS token if RM token present, security enabled
				creds.AddToken(new Text("rmdt"), token);
				yarnRunner.AddHistoryToken(creds);
				Org.Mockito.Mockito.Verify(mockHsProxy, Org.Mockito.Mockito.Times(1)).GetDelegationToken
					(Matchers.Any<GetDelegationTokenRequest>());
				// No additional call to get HS token if RM and HS token present
				yarnRunner.AddHistoryToken(creds);
				Org.Mockito.Mockito.Verify(mockHsProxy, Org.Mockito.Mockito.Times(1)).GetDelegationToken
					(Matchers.Any<GetDelegationTokenRequest>());
			}
			finally
			{
				// Back to defaults.
				UserGroupInformation.SetConfiguration(new Configuration());
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestHistoryServerToken()
		{
			//Set the master principal in the config
			conf.Set(YarnConfiguration.RmPrincipal, "foo@LOCAL");
			string masterPrincipal = Master.GetMasterPrincipal(conf);
			MRClientProtocol hsProxy = Org.Mockito.Mockito.Mock<MRClientProtocol>();
			Org.Mockito.Mockito.When(hsProxy.GetDelegationToken(Matchers.Any<GetDelegationTokenRequest
				>())).ThenAnswer(new _Answer_410(masterPrincipal));
			// check that the renewer matches the cluster's RM principal
			// none of these fields matter for the sake of the test
			UserGroupInformation.CreateRemoteUser("someone").DoAs(new _PrivilegedExceptionAction_432
				(this, hsProxy));
		}

		private sealed class _Answer_410 : Answer<GetDelegationTokenResponse>
		{
			public _Answer_410(string masterPrincipal)
			{
				this.masterPrincipal = masterPrincipal;
			}

			public GetDelegationTokenResponse Answer(InvocationOnMock invocation)
			{
				GetDelegationTokenRequest request = (GetDelegationTokenRequest)invocation.GetArguments
					()[0];
				NUnit.Framework.Assert.AreEqual(masterPrincipal, request.GetRenewer());
				Org.Apache.Hadoop.Yarn.Api.Records.Token token = TestYARNRunner.recordFactory.NewRecordInstance
					<Org.Apache.Hadoop.Yarn.Api.Records.Token>();
				token.SetKind(string.Empty);
				token.SetService(string.Empty);
				token.SetIdentifier(ByteBuffer.Allocate(0));
				token.SetPassword(ByteBuffer.Allocate(0));
				GetDelegationTokenResponse tokenResponse = TestYARNRunner.recordFactory.NewRecordInstance
					<GetDelegationTokenResponse>();
				tokenResponse.SetDelegationToken(token);
				return tokenResponse;
			}

			private readonly string masterPrincipal;
		}

		private sealed class _PrivilegedExceptionAction_432 : PrivilegedExceptionAction<Void
			>
		{
			public _PrivilegedExceptionAction_432(TestYARNRunner _enclosing, MRClientProtocol
				 hsProxy)
			{
				this._enclosing = _enclosing;
				this.hsProxy = hsProxy;
			}

			/// <exception cref="System.Exception"/>
			public Void Run()
			{
				this._enclosing.yarnRunner = new YARNRunner(this._enclosing.conf, null, null);
				this._enclosing.yarnRunner.GetDelegationTokenFromHS(hsProxy);
				Org.Mockito.Mockito.Verify(hsProxy).GetDelegationToken(Matchers.Any<GetDelegationTokenRequest
					>());
				return null;
			}

			private readonly TestYARNRunner _enclosing;

			private readonly MRClientProtocol hsProxy;
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestAMAdminCommandOpts()
		{
			JobConf jobConf = new JobConf();
			jobConf.Set(MRJobConfig.MrAmAdminCommandOpts, "-Djava.net.preferIPv4Stack=true");
			jobConf.Set(MRJobConfig.MrAmCommandOpts, "-Xmx1024m");
			YARNRunner yarnRunner = new YARNRunner(jobConf);
			ApplicationSubmissionContext submissionContext = BuildSubmitContext(yarnRunner, jobConf
				);
			ContainerLaunchContext containerSpec = submissionContext.GetAMContainerSpec();
			IList<string> commands = containerSpec.GetCommands();
			int index = 0;
			int adminIndex = 0;
			int adminPos = -1;
			int userIndex = 0;
			int userPos = -1;
			int tmpDirPos = -1;
			foreach (string command in commands)
			{
				if (command != null)
				{
					NUnit.Framework.Assert.IsFalse("Profiler should be disabled by default", command.
						Contains(ProfileParams));
					adminPos = command.IndexOf("-Djava.net.preferIPv4Stack=true");
					if (adminPos >= 0)
					{
						adminIndex = index;
					}
					userPos = command.IndexOf("-Xmx1024m");
					if (userPos >= 0)
					{
						userIndex = index;
					}
					tmpDirPos = command.IndexOf("-Djava.io.tmpdir=");
				}
				index++;
			}
			// Check java.io.tmpdir opts are set in the commands
			NUnit.Framework.Assert.IsTrue("java.io.tmpdir is not set for AM", tmpDirPos > 0);
			// Check both admin java opts and user java opts are in the commands
			NUnit.Framework.Assert.IsTrue("AM admin command opts not in the commands.", adminPos
				 > 0);
			NUnit.Framework.Assert.IsTrue("AM user command opts not in the commands.", userPos
				 > 0);
			// Check the admin java opts is before user java opts in the commands
			if (adminIndex == userIndex)
			{
				NUnit.Framework.Assert.IsTrue("AM admin command opts is after user command opts."
					, adminPos < userPos);
			}
			else
			{
				NUnit.Framework.Assert.IsTrue("AM admin command opts is after user command opts."
					, adminIndex < userIndex);
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestWarnCommandOpts()
		{
			Logger logger = Logger.GetLogger(typeof(YARNRunner));
			ByteArrayOutputStream bout = new ByteArrayOutputStream();
			Layout layout = new SimpleLayout();
			Appender appender = new WriterAppender(layout, bout);
			logger.AddAppender(appender);
			JobConf jobConf = new JobConf();
			jobConf.Set(MRJobConfig.MrAmAdminCommandOpts, "-Djava.net.preferIPv4Stack=true -Djava.library.path=foo"
				);
			jobConf.Set(MRJobConfig.MrAmCommandOpts, "-Xmx1024m -Djava.library.path=bar");
			YARNRunner yarnRunner = new YARNRunner(jobConf);
			ApplicationSubmissionContext submissionContext = BuildSubmitContext(yarnRunner, jobConf
				);
			string logMsg = bout.ToString();
			NUnit.Framework.Assert.IsTrue(logMsg.Contains("WARN - Usage of -Djava.library.path in "
				 + "yarn.app.mapreduce.am.admin-command-opts can cause programs to no " + "longer function if hadoop native libraries are used. These values "
				 + "should be set as part of the LD_LIBRARY_PATH in the app master JVM " + "env using yarn.app.mapreduce.am.admin.user.env config settings."
				));
			NUnit.Framework.Assert.IsTrue(logMsg.Contains("WARN - Usage of -Djava.library.path in "
				 + "yarn.app.mapreduce.am.command-opts can cause programs to no longer " + "function if hadoop native libraries are used. These values should "
				 + "be set as part of the LD_LIBRARY_PATH in the app master JVM env " + "using yarn.app.mapreduce.am.env config settings."
				));
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestAMProfiler()
		{
			JobConf jobConf = new JobConf();
			jobConf.SetBoolean(MRJobConfig.MrAmProfile, true);
			YARNRunner yarnRunner = new YARNRunner(jobConf);
			ApplicationSubmissionContext submissionContext = BuildSubmitContext(yarnRunner, jobConf
				);
			ContainerLaunchContext containerSpec = submissionContext.GetAMContainerSpec();
			IList<string> commands = containerSpec.GetCommands();
			foreach (string command in commands)
			{
				if (command != null)
				{
					if (command.Contains(ProfileParams))
					{
						return;
					}
				}
			}
			throw new InvalidOperationException("Profiler opts not found!");
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestAMStandardEnv()
		{
			string AdminLibPath = "foo";
			string UserLibPath = "bar";
			string UserShell = "shell";
			JobConf jobConf = new JobConf();
			jobConf.Set(MRJobConfig.MrAmAdminUserEnv, "LD_LIBRARY_PATH=" + AdminLibPath);
			jobConf.Set(MRJobConfig.MrAmEnv, "LD_LIBRARY_PATH=" + UserLibPath);
			jobConf.Set(MRJobConfig.MapredAdminUserShell, UserShell);
			YARNRunner yarnRunner = new YARNRunner(jobConf);
			ApplicationSubmissionContext appSubCtx = BuildSubmitContext(yarnRunner, jobConf);
			// make sure PWD is first in the lib path
			ContainerLaunchContext clc = appSubCtx.GetAMContainerSpec();
			IDictionary<string, string> env = clc.GetEnvironment();
			string libPath = env[ApplicationConstants.Environment.LdLibraryPath.ToString()];
			NUnit.Framework.Assert.IsNotNull("LD_LIBRARY_PATH not set", libPath);
			string cps = jobConf.GetBoolean(MRConfig.MapreduceAppSubmissionCrossPlatform, MRConfig
				.DefaultMapreduceAppSubmissionCrossPlatform) ? ApplicationConstants.ClassPathSeparator
				 : FilePath.pathSeparator;
			NUnit.Framework.Assert.AreEqual("Bad AM LD_LIBRARY_PATH setting", MRApps.CrossPlatformifyMREnv
				(conf, ApplicationConstants.Environment.Pwd) + cps + AdminLibPath + cps + UserLibPath
				, libPath);
			// make sure SHELL is set
			string shell = env[ApplicationConstants.Environment.Shell.ToString()];
			NUnit.Framework.Assert.IsNotNull("SHELL not set", shell);
			NUnit.Framework.Assert.AreEqual("Bad SHELL setting", UserShell, shell);
		}

		/// <exception cref="System.IO.IOException"/>
		private ApplicationSubmissionContext BuildSubmitContext(YARNRunner yarnRunner, JobConf
			 jobConf)
		{
			FilePath jobxml = new FilePath(testWorkDir, MRJobConfig.JobConfFile);
			OutputStream @out = new FileOutputStream(jobxml);
			conf.WriteXml(@out);
			@out.Close();
			FilePath jobsplit = new FilePath(testWorkDir, MRJobConfig.JobSplit);
			@out = new FileOutputStream(jobsplit);
			@out.Close();
			FilePath jobsplitmetainfo = new FilePath(testWorkDir, MRJobConfig.JobSplitMetainfo
				);
			@out = new FileOutputStream(jobsplitmetainfo);
			@out.Close();
			return yarnRunner.CreateApplicationSubmissionContext(jobConf, testWorkDir.ToString
				(), new Credentials());
		}
	}
}
