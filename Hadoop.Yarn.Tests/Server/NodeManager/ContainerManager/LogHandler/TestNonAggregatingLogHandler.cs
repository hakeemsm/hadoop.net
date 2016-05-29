using System;
using System.Collections.Generic;
using System.IO;
using NUnit.Framework;
using Org.Apache.Commons.IO;
using Org.Apache.Commons.Lang;
using Org.Apache.Commons.Lang.Builder;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Event;
using Org.Apache.Hadoop.Yarn.Logaggregation;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Loghandler.Event;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Recovery;
using Org.Apache.Hadoop.Yarn.Server.Utils;
using Org.Mockito;
using Org.Mockito.Exceptions.Verification;
using Org.Mockito.Internal.Matchers;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Loghandler
{
	public class TestNonAggregatingLogHandler
	{
		internal DeletionService mockDelService;

		internal Configuration conf;

		internal DrainDispatcher dispatcher;

		internal EventHandler<ApplicationEvent> appEventHandler;

		internal string user = "testuser";

		internal ApplicationId appId;

		internal ApplicationAttemptId appAttemptId;

		internal ContainerId container11;

		internal LocalDirsHandlerService dirsHandler;

		[SetUp]
		public virtual void Setup()
		{
			mockDelService = Org.Mockito.Mockito.Mock<DeletionService>();
			conf = new YarnConfiguration();
			dispatcher = CreateDispatcher(conf);
			appEventHandler = Org.Mockito.Mockito.Mock<EventHandler>();
			dispatcher.Register(typeof(ApplicationEventType), appEventHandler);
			appId = BuilderUtils.NewApplicationId(1234, 1);
			appAttemptId = BuilderUtils.NewApplicationAttemptId(appId, 1);
			container11 = BuilderUtils.NewContainerId(appAttemptId, 1);
			dirsHandler = new LocalDirsHandlerService();
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.TearDown]
		public virtual void TearDown()
		{
			dirsHandler.Stop();
			dirsHandler.Close();
			dispatcher.Await();
			dispatcher.Stop();
			dispatcher.Close();
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestLogDeletion()
		{
			FilePath[] localLogDirs = GetLocalLogDirFiles(this.GetType().FullName, 2);
			string localLogDirsString = localLogDirs[0].GetAbsolutePath() + "," + localLogDirs
				[1].GetAbsolutePath();
			conf.Set(YarnConfiguration.NmLogDirs, localLogDirsString);
			conf.SetBoolean(YarnConfiguration.LogAggregationEnabled, false);
			conf.SetLong(YarnConfiguration.NmLogRetainSeconds, 0l);
			dirsHandler.Init(conf);
			NonAggregatingLogHandler rawLogHandler = new NonAggregatingLogHandler(dispatcher, 
				mockDelService, dirsHandler, new NMNullStateStoreService());
			NonAggregatingLogHandler logHandler = Org.Mockito.Mockito.Spy(rawLogHandler);
			AbstractFileSystem spylfs = Org.Mockito.Mockito.Spy(FileContext.GetLocalFSFileContext
				().GetDefaultFileSystem());
			FileContext lfs = FileContext.GetFileContext(spylfs, conf);
			Org.Mockito.Mockito.DoReturn(lfs).When(logHandler).GetLocalFileContext(Matchers.IsA
				<Configuration>());
			FsPermission defaultPermission = FsPermission.GetDirDefault().ApplyUMask(lfs.GetUMask
				());
			FileStatus fs = new FileStatus(0, true, 1, 0, Runtime.CurrentTimeMillis(), 0, defaultPermission
				, string.Empty, string.Empty, new Path(localLogDirs[0].GetAbsolutePath()));
			Org.Mockito.Mockito.DoReturn(fs).When(spylfs).GetFileStatus(Matchers.IsA<Path>());
			logHandler.Init(conf);
			logHandler.Start();
			logHandler.Handle(new LogHandlerAppStartedEvent(appId, user, null, ContainerLogsRetentionPolicy
				.AllContainers, null));
			logHandler.Handle(new LogHandlerContainerFinishedEvent(container11, 0));
			logHandler.Handle(new LogHandlerAppFinishedEvent(appId));
			Path[] localAppLogDirs = new Path[2];
			localAppLogDirs[0] = new Path(localLogDirs[0].GetAbsolutePath(), appId.ToString()
				);
			localAppLogDirs[1] = new Path(localLogDirs[1].GetAbsolutePath(), appId.ToString()
				);
			TestDeletionServiceCall(mockDelService, user, 5000, localAppLogDirs);
			logHandler.Close();
			for (int i = 0; i < localLogDirs.Length; i++)
			{
				FileUtils.DeleteDirectory(localLogDirs[i]);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestDelayedDelete()
		{
			FilePath[] localLogDirs = GetLocalLogDirFiles(this.GetType().FullName, 2);
			string localLogDirsString = localLogDirs[0].GetAbsolutePath() + "," + localLogDirs
				[1].GetAbsolutePath();
			conf.Set(YarnConfiguration.NmLogDirs, localLogDirsString);
			conf.SetBoolean(YarnConfiguration.LogAggregationEnabled, false);
			conf.SetLong(YarnConfiguration.NmLogRetainSeconds, YarnConfiguration.DefaultNmLogRetainSeconds
				);
			dirsHandler.Init(conf);
			NonAggregatingLogHandler logHandler = new TestNonAggregatingLogHandler.NonAggregatingLogHandlerWithMockExecutor
				(this, dispatcher, mockDelService, dirsHandler);
			logHandler.Init(conf);
			logHandler.Start();
			logHandler.Handle(new LogHandlerAppStartedEvent(appId, user, null, ContainerLogsRetentionPolicy
				.AllContainers, null));
			logHandler.Handle(new LogHandlerContainerFinishedEvent(container11, 0));
			logHandler.Handle(new LogHandlerAppFinishedEvent(appId));
			Path[] localAppLogDirs = new Path[2];
			localAppLogDirs[0] = new Path(localLogDirs[0].GetAbsolutePath(), appId.ToString()
				);
			localAppLogDirs[1] = new Path(localLogDirs[1].GetAbsolutePath(), appId.ToString()
				);
			ScheduledThreadPoolExecutor mockSched = ((TestNonAggregatingLogHandler.NonAggregatingLogHandlerWithMockExecutor
				)logHandler).mockSched;
			Org.Mockito.Mockito.Verify(mockSched).Schedule(Matchers.Any<Runnable>(), Matchers.Eq
				(10800l), Matchers.Eq(TimeUnit.Seconds));
			logHandler.Close();
			for (int i = 0; i < localLogDirs.Length; i++)
			{
				FileUtils.DeleteDirectory(localLogDirs[i]);
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestStop()
		{
			NonAggregatingLogHandler aggregatingLogHandler = new NonAggregatingLogHandler(null
				, null, null, new NMNullStateStoreService());
			// It should not throw NullPointerException
			aggregatingLogHandler.Stop();
			TestNonAggregatingLogHandler.NonAggregatingLogHandlerWithMockExecutor logHandler = 
				new TestNonAggregatingLogHandler.NonAggregatingLogHandlerWithMockExecutor(this, 
				null, null, null);
			logHandler.Init(new Configuration());
			logHandler.Stop();
			Org.Mockito.Mockito.Verify(logHandler.mockSched).Shutdown();
			Org.Mockito.Mockito.Verify(logHandler.mockSched).AwaitTermination(Matchers.Eq(10l
				), Matchers.Eq(TimeUnit.Seconds));
			Org.Mockito.Mockito.Verify(logHandler.mockSched).ShutdownNow();
			logHandler.Close();
			aggregatingLogHandler.Close();
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestHandlingApplicationFinishedEvent()
		{
			DeletionService delService = new DeletionService(null);
			NonAggregatingLogHandler aggregatingLogHandler = new NonAggregatingLogHandler(new 
				InlineDispatcher(), delService, dirsHandler, new NMNullStateStoreService());
			dirsHandler.Init(conf);
			dirsHandler.Start();
			delService.Init(conf);
			delService.Start();
			aggregatingLogHandler.Init(conf);
			aggregatingLogHandler.Start();
			// It should NOT throw RejectedExecutionException
			aggregatingLogHandler.Handle(new LogHandlerAppFinishedEvent(appId));
			aggregatingLogHandler.Stop();
			// It should NOT throw RejectedExecutionException after stopping
			// handler service.
			aggregatingLogHandler.Handle(new LogHandlerAppFinishedEvent(appId));
			aggregatingLogHandler.Close();
		}

		private class NonAggregatingLogHandlerWithMockExecutor : NonAggregatingLogHandler
		{
			private ScheduledThreadPoolExecutor mockSched;

			public NonAggregatingLogHandlerWithMockExecutor(TestNonAggregatingLogHandler _enclosing
				, Dispatcher dispatcher, DeletionService delService, LocalDirsHandlerService dirsHandler
				)
				: this(dispatcher, delService, dirsHandler, new NMNullStateStoreService())
			{
				this._enclosing = _enclosing;
			}

			public NonAggregatingLogHandlerWithMockExecutor(TestNonAggregatingLogHandler _enclosing
				, Dispatcher dispatcher, DeletionService delService, LocalDirsHandlerService dirsHandler
				, NMStateStoreService stateStore)
				: base(dispatcher, delService, dirsHandler, stateStore)
			{
				this._enclosing = _enclosing;
			}

			internal override ScheduledThreadPoolExecutor CreateScheduledThreadPoolExecutor(Configuration
				 conf)
			{
				this.mockSched = Org.Mockito.Mockito.Mock<ScheduledThreadPoolExecutor>();
				return this.mockSched;
			}

			private readonly TestNonAggregatingLogHandler _enclosing;
		}

		private DrainDispatcher CreateDispatcher(Configuration conf)
		{
			DrainDispatcher dispatcher = new DrainDispatcher();
			dispatcher.Init(conf);
			dispatcher.Start();
			return dispatcher;
		}

		/*
		* Test to ensure that we handle the cleanup of directories that may not have
		* the application log dirs we're trying to delete or may have other problems.
		* Test creates 7 log dirs, and fails the directory check for 4 of them and
		* then checks to ensure we tried to delete only the ones that passed the
		* check.
		*/
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestFailedDirLogDeletion()
		{
			FilePath[] localLogDirs = GetLocalLogDirFiles(this.GetType().FullName, 7);
			IList<string> localLogDirPaths = new AList<string>(localLogDirs.Length);
			for (int i = 0; i < localLogDirs.Length; i++)
			{
				localLogDirPaths.AddItem(localLogDirs[i].GetAbsolutePath());
			}
			string localLogDirsString = StringUtils.Join(localLogDirPaths, ",");
			conf.Set(YarnConfiguration.NmLogDirs, localLogDirsString);
			conf.SetBoolean(YarnConfiguration.LogAggregationEnabled, false);
			conf.SetLong(YarnConfiguration.NmLogRetainSeconds, 0l);
			LocalDirsHandlerService mockDirsHandler = Org.Mockito.Mockito.Mock<LocalDirsHandlerService
				>();
			NonAggregatingLogHandler rawLogHandler = new NonAggregatingLogHandler(dispatcher, 
				mockDelService, mockDirsHandler, new NMNullStateStoreService());
			NonAggregatingLogHandler logHandler = Org.Mockito.Mockito.Spy(rawLogHandler);
			AbstractFileSystem spylfs = Org.Mockito.Mockito.Spy(FileContext.GetLocalFSFileContext
				().GetDefaultFileSystem());
			FileContext lfs = FileContext.GetFileContext(spylfs, conf);
			Org.Mockito.Mockito.DoReturn(lfs).When(logHandler).GetLocalFileContext(Matchers.IsA
				<Configuration>());
			logHandler.Init(conf);
			logHandler.Start();
			RunMockedFailedDirs(logHandler, appId, user, mockDelService, mockDirsHandler, conf
				, spylfs, lfs, localLogDirs);
			logHandler.Close();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRecovery()
		{
			FilePath[] localLogDirs = GetLocalLogDirFiles(this.GetType().FullName, 2);
			string localLogDirsString = localLogDirs[0].GetAbsolutePath() + "," + localLogDirs
				[1].GetAbsolutePath();
			conf.Set(YarnConfiguration.NmLogDirs, localLogDirsString);
			conf.SetBoolean(YarnConfiguration.LogAggregationEnabled, false);
			conf.SetLong(YarnConfiguration.NmLogRetainSeconds, YarnConfiguration.DefaultNmLogRetainSeconds
				);
			dirsHandler.Init(conf);
			NMStateStoreService stateStore = new NMMemoryStateStoreService();
			stateStore.Init(conf);
			stateStore.Start();
			TestNonAggregatingLogHandler.NonAggregatingLogHandlerWithMockExecutor logHandler = 
				new TestNonAggregatingLogHandler.NonAggregatingLogHandlerWithMockExecutor(this, 
				dispatcher, mockDelService, dirsHandler, stateStore);
			logHandler.Init(conf);
			logHandler.Start();
			logHandler.Handle(new LogHandlerAppStartedEvent(appId, user, null, ContainerLogsRetentionPolicy
				.AllContainers, null));
			logHandler.Handle(new LogHandlerContainerFinishedEvent(container11, 0));
			logHandler.Handle(new LogHandlerAppFinishedEvent(appId));
			// simulate a restart and verify deletion is rescheduled
			logHandler.Close();
			logHandler = new TestNonAggregatingLogHandler.NonAggregatingLogHandlerWithMockExecutor
				(this, dispatcher, mockDelService, dirsHandler, stateStore);
			logHandler.Init(conf);
			logHandler.Start();
			ArgumentCaptor<Runnable> schedArg = ArgumentCaptor.ForClass<Runnable>();
			Org.Mockito.Mockito.Verify(logHandler.mockSched).Schedule(schedArg.Capture(), Matchers.AnyLong
				(), Matchers.Eq(TimeUnit.Milliseconds));
			// execute the runnable and verify another restart has nothing scheduled
			schedArg.GetValue().Run();
			logHandler.Close();
			logHandler = new TestNonAggregatingLogHandler.NonAggregatingLogHandlerWithMockExecutor
				(this, dispatcher, mockDelService, dirsHandler, stateStore);
			logHandler.Init(conf);
			logHandler.Start();
			Org.Mockito.Mockito.Verify(logHandler.mockSched, Org.Mockito.Mockito.Never()).Schedule
				(Matchers.Any<Runnable>(), Matchers.AnyLong(), Matchers.Any<TimeUnit>());
			logHandler.Close();
		}

		/// <summary>
		/// Function to run a log handler with directories failing the getFileStatus
		/// call.
		/// </summary>
		/// <remarks>
		/// Function to run a log handler with directories failing the getFileStatus
		/// call. The function accepts the log handler, setup the mocks to fail with
		/// specific exceptions and ensures the deletion service has the correct calls.
		/// </remarks>
		/// <param name="logHandler">the logHandler implementation to test</param>
		/// <param name="appId">
		/// the application id that we wish when sending events to the log
		/// handler
		/// </param>
		/// <param name="user">the user name to use</param>
		/// <param name="mockDelService">
		/// a mock of the DeletionService which we will verify
		/// the delete calls against
		/// </param>
		/// <param name="dirsHandler">
		/// a spy or mock on the LocalDirsHandler service used to
		/// when creating the logHandler. It needs to be a spy so that we can intercept
		/// the getAllLogDirs() call.
		/// </param>
		/// <param name="conf">the configuration used</param>
		/// <param name="spylfs">a spy on the AbstractFileSystem object used when creating lfs
		/// 	</param>
		/// <param name="lfs">
		/// the FileContext object to be used to mock the getFileStatus()
		/// calls
		/// </param>
		/// <param name="localLogDirs">
		/// list of the log dirs to run the test against, must have
		/// at least 7 entries
		/// </param>
		/// <exception cref="System.Exception"/>
		public static void RunMockedFailedDirs(LogHandler logHandler, ApplicationId appId
			, string user, DeletionService mockDelService, LocalDirsHandlerService dirsHandler
			, Configuration conf, AbstractFileSystem spylfs, FileContext lfs, FilePath[] localLogDirs
			)
		{
			IDictionary<ApplicationAccessType, string> appAcls = new Dictionary<ApplicationAccessType
				, string>();
			if (localLogDirs.Length < 7)
			{
				throw new ArgumentException("Argument localLogDirs must be at least of length 7");
			}
			Path[] localAppLogDirPaths = new Path[localLogDirs.Length];
			for (int i = 0; i < localAppLogDirPaths.Length; i++)
			{
				localAppLogDirPaths[i] = new Path(localLogDirs[i].GetAbsolutePath(), appId.ToString
					());
			}
			IList<string> localLogDirPaths = new AList<string>(localLogDirs.Length);
			for (int i_1 = 0; i_1 < localLogDirs.Length; i_1++)
			{
				localLogDirPaths.AddItem(localLogDirs[i_1].GetAbsolutePath());
			}
			// setup mocks
			FsPermission defaultPermission = FsPermission.GetDirDefault().ApplyUMask(lfs.GetUMask
				());
			FileStatus fs = new FileStatus(0, true, 1, 0, Runtime.CurrentTimeMillis(), 0, defaultPermission
				, string.Empty, string.Empty, new Path(localLogDirs[0].GetAbsolutePath()));
			Org.Mockito.Mockito.DoReturn(fs).When(spylfs).GetFileStatus(Matchers.IsA<Path>());
			Org.Mockito.Mockito.DoReturn(localLogDirPaths).When(dirsHandler).GetLogDirsForCleanup
				();
			logHandler.Handle(new LogHandlerAppStartedEvent(appId, user, null, ContainerLogsRetentionPolicy
				.AllContainers, appAcls));
			// test case where some dirs have the log dir to delete
			// mock some dirs throwing various exceptions
			// verify deletion happens only on the others
			Org.Mockito.Mockito.DoThrow(new FileNotFoundException()).When(spylfs).GetFileStatus
				(Matchers.Eq(localAppLogDirPaths[0]));
			Org.Mockito.Mockito.DoReturn(fs).When(spylfs).GetFileStatus(Matchers.Eq(localAppLogDirPaths
				[1]));
			Org.Mockito.Mockito.DoThrow(new AccessControlException()).When(spylfs).GetFileStatus
				(Matchers.Eq(localAppLogDirPaths[2]));
			Org.Mockito.Mockito.DoReturn(fs).When(spylfs).GetFileStatus(Matchers.Eq(localAppLogDirPaths
				[3]));
			Org.Mockito.Mockito.DoThrow(new IOException()).When(spylfs).GetFileStatus(Matchers.Eq
				(localAppLogDirPaths[4]));
			Org.Mockito.Mockito.DoThrow(new UnsupportedFileSystemException("test")).When(spylfs
				).GetFileStatus(Matchers.Eq(localAppLogDirPaths[5]));
			Org.Mockito.Mockito.DoReturn(fs).When(spylfs).GetFileStatus(Matchers.Eq(localAppLogDirPaths
				[6]));
			logHandler.Handle(new LogHandlerAppFinishedEvent(appId));
			TestDeletionServiceCall(mockDelService, user, 5000, localAppLogDirPaths[1], localAppLogDirPaths
				[3], localAppLogDirPaths[6]);
			return;
		}

		[System.Serializable]
		internal class DeletePathsMatcher : ArgumentMatcher<Path[]>, VarargMatcher
		{
			internal const long serialVersionUID = 0;

			[System.NonSerialized]
			private Path[] matchPaths;

			internal DeletePathsMatcher(params Path[] matchPaths)
			{
				// to get rid of serialization warning
				this.matchPaths = matchPaths;
			}

			public override bool Matches(object varargs)
			{
				return new EqualsBuilder().Append(matchPaths, varargs).IsEquals();
			}

			// function to get rid of FindBugs warning
			/// <exception cref="System.IO.NotSerializableException"/>
			private void ReadObject(ObjectInputStream os)
			{
				throw new NotSerializableException(this.GetType().FullName);
			}
		}

		/// <summary>
		/// Function to verify that the DeletionService object received the right
		/// requests.
		/// </summary>
		/// <param name="delService">the DeletionService mock which we verify against</param>
		/// <param name="user">the user name to use when verifying the deletion</param>
		/// <param name="timeout">
		/// amount in milliseconds to wait before we decide the calls
		/// didn't come through
		/// </param>
		/// <param name="matchPaths">the paths to match in the delete calls</param>
		/// <exception cref="Org.Mockito.Exceptions.Verification.WantedButNotInvoked">if the calls could not be verified
		/// 	</exception>
		internal static void TestDeletionServiceCall(DeletionService delService, string user
			, long timeout, params Path[] matchPaths)
		{
			long verifyStartTime = Runtime.CurrentTimeMillis();
			WantedButNotInvoked notInvokedException = null;
			bool matched = false;
			while (!matched && Runtime.CurrentTimeMillis() < verifyStartTime + timeout)
			{
				try
				{
					Org.Mockito.Mockito.Verify(delService).Delete(Org.Mockito.Matchers.Eq(user), (Path
						)Org.Mockito.Matchers.Eq(null), Org.Mockito.Mockito.ArgThat(new TestNonAggregatingLogHandler.DeletePathsMatcher
						(matchPaths)));
					matched = true;
				}
				catch (WantedButNotInvoked e)
				{
					notInvokedException = e;
					try
					{
						Sharpen.Thread.Sleep(50l);
					}
					catch (Exception)
					{
					}
				}
			}
			if (!matched)
			{
				throw notInvokedException;
			}
			return;
		}

		public static FilePath[] GetLocalLogDirFiles(string name, int number)
		{
			FilePath[] dirs = new FilePath[number];
			for (int i = 0; i < dirs.Length; i++)
			{
				dirs[i] = new FilePath("target", name + "-localLogDir" + i).GetAbsoluteFile();
			}
			return dirs;
		}
	}
}
