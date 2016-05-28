using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using NUnit.Framework;
using Org.Apache.Commons.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Jobhistory;
using Org.Apache.Hadoop.Mapreduce.V2.Api.Records;
using Org.Apache.Hadoop.Mapreduce.V2.App.Client;
using Org.Apache.Hadoop.Mapreduce.V2.App.Commit;
using Org.Apache.Hadoop.Mapreduce.V2.App.Job;
using Org.Apache.Hadoop.Mapreduce.V2.App.RM;
using Org.Apache.Hadoop.Mapreduce.V2.Util;
using Org.Apache.Hadoop.Metrics2.Lib;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Token;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Event;
using Org.Apache.Hadoop.Yarn.Security;
using Org.Apache.Hadoop.Yarn.Util;
using Org.Apache.Log4j;
using Org.Mockito;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.V2.App
{
	public class TestMRAppMaster
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(TestMRAppMaster));

		internal static string stagingDir = "staging/";

		private static FileContext localFS = null;

		private static readonly FilePath testDir = new FilePath("target", typeof(TestMRAppMaster
			).FullName + "-tmpDir").GetAbsoluteFile();

		/// <exception cref="Org.Apache.Hadoop.Security.AccessControlException"/>
		/// <exception cref="System.IO.FileNotFoundException"/>
		/// <exception cref="System.ArgumentException"/>
		/// <exception cref="System.IO.IOException"/>
		[BeforeClass]
		public static void Setup()
		{
			//Do not error out if metrics are inited multiple times
			DefaultMetricsSystem.SetMiniClusterMode(true);
			FilePath dir = new FilePath(stagingDir);
			stagingDir = dir.GetAbsolutePath();
			localFS = FileContext.GetLocalFSFileContext();
			localFS.Delete(new Path(testDir.GetAbsolutePath()), true);
			testDir.Mkdir();
		}

		/// <exception cref="System.IO.IOException"/>
		[SetUp]
		public virtual void Cleanup()
		{
			FilePath dir = new FilePath(stagingDir);
			if (dir.Exists())
			{
				FileUtils.DeleteDirectory(dir);
			}
			dir.Mkdirs();
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestMRAppMasterForDifferentUser()
		{
			string applicationAttemptIdStr = "appattempt_1317529182569_0004_000001";
			string containerIdStr = "container_1317529182569_0004_000001_1";
			string userName = "TestAppMasterUser";
			ApplicationAttemptId applicationAttemptId = ConverterUtils.ToApplicationAttemptId
				(applicationAttemptIdStr);
			ContainerId containerId = ConverterUtils.ToContainerId(containerIdStr);
			MRAppMasterTest appMaster = new MRAppMasterTest(applicationAttemptId, containerId
				, "host", -1, -1, Runtime.CurrentTimeMillis());
			JobConf conf = new JobConf();
			conf.Set(MRJobConfig.MrAmStagingDir, stagingDir);
			MRAppMaster.InitAndStartAppMaster(appMaster, conf, userName);
			Path userPath = new Path(stagingDir, userName);
			Path userStagingPath = new Path(userPath, ".staging");
			NUnit.Framework.Assert.AreEqual(userStagingPath.ToString(), appMaster.stagingDirPath
				.ToString());
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestMRAppMasterMidLock()
		{
			string applicationAttemptIdStr = "appattempt_1317529182569_0004_000002";
			string containerIdStr = "container_1317529182569_0004_000002_1";
			string userName = "TestAppMasterUser";
			JobConf conf = new JobConf();
			conf.Set(MRJobConfig.MrAmStagingDir, stagingDir);
			ApplicationAttemptId applicationAttemptId = ConverterUtils.ToApplicationAttemptId
				(applicationAttemptIdStr);
			JobId jobId = TypeConverter.ToYarn(TypeConverter.FromYarn(applicationAttemptId.GetApplicationId
				()));
			Path start = MRApps.GetStartJobCommitFile(conf, userName, jobId);
			FileSystem fs = FileSystem.Get(conf);
			//Create the file, but no end file so we should unregister with an error.
			fs.Create(start).Close();
			ContainerId containerId = ConverterUtils.ToContainerId(containerIdStr);
			MRAppMaster appMaster = new MRAppMasterTest(applicationAttemptId, containerId, "host"
				, -1, -1, Runtime.CurrentTimeMillis(), false, false);
			bool caught = false;
			try
			{
				MRAppMaster.InitAndStartAppMaster(appMaster, conf, userName);
			}
			catch (IOException e)
			{
				//The IO Exception is expected
				Log.Info("Caught expected Exception", e);
				caught = true;
			}
			NUnit.Framework.Assert.IsTrue(caught);
			NUnit.Framework.Assert.IsTrue(appMaster.errorHappenedShutDown);
			NUnit.Framework.Assert.AreEqual(JobStateInternal.Error, appMaster.forcedState);
			appMaster.Stop();
			// verify the final status is FAILED
			VerifyFailedStatus((MRAppMasterTest)appMaster, "FAILED");
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestMRAppMasterSuccessLock()
		{
			string applicationAttemptIdStr = "appattempt_1317529182569_0004_000002";
			string containerIdStr = "container_1317529182569_0004_000002_1";
			string userName = "TestAppMasterUser";
			JobConf conf = new JobConf();
			conf.Set(MRJobConfig.MrAmStagingDir, stagingDir);
			ApplicationAttemptId applicationAttemptId = ConverterUtils.ToApplicationAttemptId
				(applicationAttemptIdStr);
			JobId jobId = TypeConverter.ToYarn(TypeConverter.FromYarn(applicationAttemptId.GetApplicationId
				()));
			Path start = MRApps.GetStartJobCommitFile(conf, userName, jobId);
			Path end = MRApps.GetEndJobCommitSuccessFile(conf, userName, jobId);
			FileSystem fs = FileSystem.Get(conf);
			fs.Create(start).Close();
			fs.Create(end).Close();
			ContainerId containerId = ConverterUtils.ToContainerId(containerIdStr);
			MRAppMaster appMaster = new MRAppMasterTest(applicationAttemptId, containerId, "host"
				, -1, -1, Runtime.CurrentTimeMillis(), false, false);
			bool caught = false;
			try
			{
				MRAppMaster.InitAndStartAppMaster(appMaster, conf, userName);
			}
			catch (IOException e)
			{
				//The IO Exception is expected
				Log.Info("Caught expected Exception", e);
				caught = true;
			}
			NUnit.Framework.Assert.IsTrue(caught);
			NUnit.Framework.Assert.IsTrue(appMaster.errorHappenedShutDown);
			NUnit.Framework.Assert.AreEqual(JobStateInternal.Succeeded, appMaster.forcedState
				);
			appMaster.Stop();
			// verify the final status is SUCCEEDED
			VerifyFailedStatus((MRAppMasterTest)appMaster, "SUCCEEDED");
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestMRAppMasterFailLock()
		{
			string applicationAttemptIdStr = "appattempt_1317529182569_0004_000002";
			string containerIdStr = "container_1317529182569_0004_000002_1";
			string userName = "TestAppMasterUser";
			JobConf conf = new JobConf();
			conf.Set(MRJobConfig.MrAmStagingDir, stagingDir);
			ApplicationAttemptId applicationAttemptId = ConverterUtils.ToApplicationAttemptId
				(applicationAttemptIdStr);
			JobId jobId = TypeConverter.ToYarn(TypeConverter.FromYarn(applicationAttemptId.GetApplicationId
				()));
			Path start = MRApps.GetStartJobCommitFile(conf, userName, jobId);
			Path end = MRApps.GetEndJobCommitFailureFile(conf, userName, jobId);
			FileSystem fs = FileSystem.Get(conf);
			fs.Create(start).Close();
			fs.Create(end).Close();
			ContainerId containerId = ConverterUtils.ToContainerId(containerIdStr);
			MRAppMaster appMaster = new MRAppMasterTest(applicationAttemptId, containerId, "host"
				, -1, -1, Runtime.CurrentTimeMillis(), false, false);
			bool caught = false;
			try
			{
				MRAppMaster.InitAndStartAppMaster(appMaster, conf, userName);
			}
			catch (IOException e)
			{
				//The IO Exception is expected
				Log.Info("Caught expected Exception", e);
				caught = true;
			}
			NUnit.Framework.Assert.IsTrue(caught);
			NUnit.Framework.Assert.IsTrue(appMaster.errorHappenedShutDown);
			NUnit.Framework.Assert.AreEqual(JobStateInternal.Failed, appMaster.forcedState);
			appMaster.Stop();
			// verify the final status is FAILED
			VerifyFailedStatus((MRAppMasterTest)appMaster, "FAILED");
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestMRAppMasterMissingStaging()
		{
			string applicationAttemptIdStr = "appattempt_1317529182569_0004_000002";
			string containerIdStr = "container_1317529182569_0004_000002_1";
			string userName = "TestAppMasterUser";
			JobConf conf = new JobConf();
			conf.Set(MRJobConfig.MrAmStagingDir, stagingDir);
			ApplicationAttemptId applicationAttemptId = ConverterUtils.ToApplicationAttemptId
				(applicationAttemptIdStr);
			//Delete the staging directory
			FilePath dir = new FilePath(stagingDir);
			if (dir.Exists())
			{
				FileUtils.DeleteDirectory(dir);
			}
			ContainerId containerId = ConverterUtils.ToContainerId(containerIdStr);
			MRAppMaster appMaster = new MRAppMasterTest(applicationAttemptId, containerId, "host"
				, -1, -1, Runtime.CurrentTimeMillis(), false, false);
			bool caught = false;
			try
			{
				MRAppMaster.InitAndStartAppMaster(appMaster, conf, userName);
			}
			catch (IOException e)
			{
				//The IO Exception is expected
				Log.Info("Caught expected Exception", e);
				caught = true;
			}
			NUnit.Framework.Assert.IsTrue(caught);
			NUnit.Framework.Assert.IsTrue(appMaster.errorHappenedShutDown);
			//Copying the history file is disabled, but it is not really visible from 
			//here
			NUnit.Framework.Assert.AreEqual(JobStateInternal.Error, appMaster.forcedState);
			appMaster.Stop();
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual void TestMRAppMasterMaxAppAttempts()
		{
			// No matter what's the maxAppAttempt or attempt id, the isLastRetry always
			// equals to false
			bool[] expectedBools = new bool[] { false, false, false };
			string applicationAttemptIdStr = "appattempt_1317529182569_0004_000002";
			string containerIdStr = "container_1317529182569_0004_000002_1";
			string userName = "TestAppMasterUser";
			ApplicationAttemptId applicationAttemptId = ConverterUtils.ToApplicationAttemptId
				(applicationAttemptIdStr);
			ContainerId containerId = ConverterUtils.ToContainerId(containerIdStr);
			JobConf conf = new JobConf();
			conf.Set(MRJobConfig.MrAmStagingDir, stagingDir);
			FilePath stagingDir = new FilePath(MRApps.GetStagingAreaDir(conf, userName).ToString
				());
			stagingDir.Mkdirs();
			for (int i = 0; i < expectedBools.Length; ++i)
			{
				MRAppMasterTest appMaster = new MRAppMasterTest(applicationAttemptId, containerId
					, "host", -1, -1, Runtime.CurrentTimeMillis(), false, true);
				MRAppMaster.InitAndStartAppMaster(appMaster, conf, userName);
				NUnit.Framework.Assert.AreEqual("isLastAMRetry is correctly computed.", expectedBools
					[i], appMaster.IsLastAMRetry());
			}
		}

		// A dirty hack to modify the env of the current JVM itself - Dirty, but
		// should be okay for testing.
		/// <exception cref="System.Exception"/>
		private static void SetNewEnvironmentHack(IDictionary<string, string> newenv)
		{
			try
			{
				Type cl = Sharpen.Runtime.GetType("java.lang.ProcessEnvironment");
				FieldInfo field = Sharpen.Runtime.GetDeclaredField(cl, "theEnvironment");
				IDictionary<string, string> env = (IDictionary<string, string>)field.GetValue(null
					);
				env.Clear();
				env.PutAll(newenv);
				FieldInfo ciField = Sharpen.Runtime.GetDeclaredField(cl, "theCaseInsensitiveEnvironment"
					);
				IDictionary<string, string> cienv = (IDictionary<string, string>)ciField.GetValue
					(null);
				cienv.Clear();
				cienv.PutAll(newenv);
			}
			catch (NoSuchFieldException)
			{
				Type[] classes = typeof(Sharpen.Collections).GetDeclaredClasses();
				IDictionary<string, string> env = Sharpen.Runtime.GetEnv();
				foreach (Type cl in classes)
				{
					if ("java.util.Collections$UnmodifiableMap".Equals(cl.FullName))
					{
						FieldInfo field = Sharpen.Runtime.GetDeclaredField(cl, "m");
						object obj = field.GetValue(env);
						IDictionary<string, string> map = (IDictionary<string, string>)obj;
						map.Clear();
						map.PutAll(newenv);
					}
				}
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestMRAppMasterCredentials()
		{
			Logger rootLogger = LogManager.GetRootLogger();
			rootLogger.SetLevel(Level.Debug);
			// Simulate credentials passed to AM via client->RM->NM
			Credentials credentials = new Credentials();
			byte[] identifier = Sharpen.Runtime.GetBytesForString("MyIdentifier");
			byte[] password = Sharpen.Runtime.GetBytesForString("MyPassword");
			Text kind = new Text("MyTokenKind");
			Text service = new Text("host:port");
			Org.Apache.Hadoop.Security.Token.Token<TokenIdentifier> myToken = new Org.Apache.Hadoop.Security.Token.Token
				<TokenIdentifier>(identifier, password, kind, service);
			Text tokenAlias = new Text("myToken");
			credentials.AddToken(tokenAlias, myToken);
			Text appTokenService = new Text("localhost:0");
			Org.Apache.Hadoop.Security.Token.Token<AMRMTokenIdentifier> appToken = new Org.Apache.Hadoop.Security.Token.Token
				<AMRMTokenIdentifier>(identifier, password, AMRMTokenIdentifier.KindName, appTokenService
				);
			credentials.AddToken(appTokenService, appToken);
			Text keyAlias = new Text("mySecretKeyAlias");
			credentials.AddSecretKey(keyAlias, Sharpen.Runtime.GetBytesForString("mySecretKey"
				));
			Org.Apache.Hadoop.Security.Token.Token<TokenIdentifier> storedToken = credentials
				.GetToken(tokenAlias);
			JobConf conf = new JobConf();
			Path tokenFilePath = new Path(testDir.GetAbsolutePath(), "tokens-file");
			IDictionary<string, string> newEnv = new Dictionary<string, string>();
			newEnv[UserGroupInformation.HadoopTokenFileLocation] = tokenFilePath.ToUri().GetPath
				();
			SetNewEnvironmentHack(newEnv);
			credentials.WriteTokenStorageFile(tokenFilePath, conf);
			ApplicationId appId = ApplicationId.NewInstance(12345, 56);
			ApplicationAttemptId applicationAttemptId = ApplicationAttemptId.NewInstance(appId
				, 1);
			ContainerId containerId = ContainerId.NewContainerId(applicationAttemptId, 546);
			string userName = UserGroupInformation.GetCurrentUser().GetShortUserName();
			// Create staging dir, so MRAppMaster doesn't barf.
			FilePath stagingDir = new FilePath(MRApps.GetStagingAreaDir(conf, userName).ToString
				());
			stagingDir.Mkdirs();
			// Set login-user to null as that is how real world MRApp starts with.
			// This is null is the reason why token-file is read by UGI.
			UserGroupInformation.SetLoginUser(null);
			MRAppMasterTest appMaster = new MRAppMasterTest(applicationAttemptId, containerId
				, "host", -1, -1, Runtime.CurrentTimeMillis(), false, true);
			MRAppMaster.InitAndStartAppMaster(appMaster, conf, userName);
			// Now validate the task credentials
			Credentials appMasterCreds = appMaster.GetCredentials();
			NUnit.Framework.Assert.IsNotNull(appMasterCreds);
			NUnit.Framework.Assert.AreEqual(1, appMasterCreds.NumberOfSecretKeys());
			NUnit.Framework.Assert.AreEqual(1, appMasterCreds.NumberOfTokens());
			// Validate the tokens - app token should not be present
			Org.Apache.Hadoop.Security.Token.Token<TokenIdentifier> usedToken = appMasterCreds
				.GetToken(tokenAlias);
			NUnit.Framework.Assert.IsNotNull(usedToken);
			NUnit.Framework.Assert.AreEqual(storedToken, usedToken);
			// Validate the keys
			byte[] usedKey = appMasterCreds.GetSecretKey(keyAlias);
			NUnit.Framework.Assert.IsNotNull(usedKey);
			NUnit.Framework.Assert.AreEqual("mySecretKey", Sharpen.Runtime.GetStringForBytes(
				usedKey));
			// The credentials should also be added to conf so that OuputCommitter can
			// access it - app token should not be present
			Credentials confCredentials = conf.GetCredentials();
			NUnit.Framework.Assert.AreEqual(1, confCredentials.NumberOfSecretKeys());
			NUnit.Framework.Assert.AreEqual(1, confCredentials.NumberOfTokens());
			NUnit.Framework.Assert.AreEqual(storedToken, confCredentials.GetToken(tokenAlias)
				);
			NUnit.Framework.Assert.AreEqual("mySecretKey", Sharpen.Runtime.GetStringForBytes(
				confCredentials.GetSecretKey(keyAlias)));
			// Verify the AM's ugi - app token should be present
			Credentials ugiCredentials = appMaster.GetUgi().GetCredentials();
			NUnit.Framework.Assert.AreEqual(1, ugiCredentials.NumberOfSecretKeys());
			NUnit.Framework.Assert.AreEqual(2, ugiCredentials.NumberOfTokens());
			NUnit.Framework.Assert.AreEqual(storedToken, ugiCredentials.GetToken(tokenAlias));
			NUnit.Framework.Assert.AreEqual(appToken, ugiCredentials.GetToken(appTokenService
				));
			NUnit.Framework.Assert.AreEqual("mySecretKey", Sharpen.Runtime.GetStringForBytes(
				ugiCredentials.GetSecretKey(keyAlias)));
		}

		private void VerifyFailedStatus(MRAppMasterTest appMaster, string expectedJobState
			)
		{
			ArgumentCaptor<JobHistoryEvent> captor = ArgumentCaptor.ForClass<JobHistoryEvent>
				();
			// handle two events: AMStartedEvent and JobUnsuccessfulCompletionEvent
			Org.Mockito.Mockito.Verify(appMaster.spyHistoryService, Org.Mockito.Mockito.Times
				(2)).HandleEvent(captor.Capture());
			HistoryEvent @event = captor.GetValue().GetHistoryEvent();
			NUnit.Framework.Assert.IsTrue(@event is JobUnsuccessfulCompletionEvent);
			NUnit.Framework.Assert.AreEqual(((JobUnsuccessfulCompletionEvent)@event).GetStatus
				(), expectedJobState);
		}
	}

	internal class MRAppMasterTest : MRAppMaster
	{
		internal Path stagingDirPath;

		private Configuration conf;

		private bool overrideInit;

		private bool overrideStart;

		internal ContainerAllocator mockContainerAllocator;

		internal CommitterEventHandler mockCommitterEventHandler;

		internal RMHeartbeatHandler mockRMHeartbeatHandler;

		internal JobHistoryEventHandler spyHistoryService;

		public MRAppMasterTest(ApplicationAttemptId applicationAttemptId, ContainerId containerId
			, string host, int port, int httpPort, long submitTime)
			: this(applicationAttemptId, containerId, host, port, httpPort, submitTime, true, 
				true)
		{
		}

		public MRAppMasterTest(ApplicationAttemptId applicationAttemptId, ContainerId containerId
			, string host, int port, int httpPort, long submitTime, bool overrideInit, bool 
			overrideStart)
			: base(applicationAttemptId, containerId, host, port, httpPort, submitTime)
		{
			this.overrideInit = overrideInit;
			this.overrideStart = overrideStart;
			mockContainerAllocator = Org.Mockito.Mockito.Mock<ContainerAllocator>();
			mockCommitterEventHandler = Org.Mockito.Mockito.Mock<CommitterEventHandler>();
			mockRMHeartbeatHandler = Org.Mockito.Mockito.Mock<RMHeartbeatHandler>();
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceInit(Configuration conf)
		{
			if (!overrideInit)
			{
				base.ServiceInit(conf);
			}
			this.conf = conf;
		}

		protected internal override ContainerAllocator CreateContainerAllocator(ClientService
			 clientService, AppContext context)
		{
			return mockContainerAllocator;
		}

		protected internal override EventHandler<CommitterEvent> CreateCommitterEventHandler
			(AppContext context, OutputCommitter committer)
		{
			return mockCommitterEventHandler;
		}

		protected internal override RMHeartbeatHandler GetRMHeartbeatHandler()
		{
			return mockRMHeartbeatHandler;
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceStart()
		{
			if (overrideStart)
			{
				try
				{
					UserGroupInformation ugi = UserGroupInformation.GetCurrentUser();
					string user = ugi.GetShortUserName();
					stagingDirPath = MRApps.GetStagingAreaDir(conf, user);
				}
				catch (Exception e)
				{
					NUnit.Framework.Assert.Fail(e.Message);
				}
			}
			else
			{
				base.ServiceStart();
			}
		}

		protected internal override Credentials GetCredentials()
		{
			return base.GetCredentials();
		}

		public virtual UserGroupInformation GetUgi()
		{
			return currentUser;
		}

		protected internal override EventHandler<JobHistoryEvent> CreateJobHistoryHandler
			(AppContext context)
		{
			spyHistoryService = Org.Mockito.Mockito.Spy((JobHistoryEventHandler)base.CreateJobHistoryHandler
				(context));
			spyHistoryService.SetForcejobCompletion(this.isLastAMRetry);
			return spyHistoryService;
		}
	}
}
