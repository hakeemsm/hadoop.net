using System.Collections.Generic;
using System.IO;
using System.Net;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Localizer;
using Org.Mockito;
using Org.Mockito.Invocation;
using Org.Mockito.Stubbing;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager
{
	public class TestDefaultContainerExecutor
	{
		private static Path BaseTmpPath = new Path("target", typeof(TestDefaultContainerExecutor
			).Name);

		/*
		// XXX FileContext cannot be mocked to do this
		static FSDataInputStream getRandomStream(Random r, int len)
		throws IOException {
		byte[] bytes = new byte[len];
		r.nextBytes(bytes);
		DataInputBuffer buf = new DataInputBuffer();
		buf.reset(bytes, 0, bytes.length);
		return new FSDataInputStream(new FakeFSDataInputStream(buf));
		}
		
		class PathEndsWith extends ArgumentMatcher<Path> {
		final String suffix;
		PathEndsWith(String suffix) {
		this.suffix = suffix;
		}
		@Override
		public boolean matches(Object o) {
		return
		suffix.equals(((Path)o).getName());
		}
		}
		
		DataOutputBuffer mockStream(
		AbstractFileSystem spylfs, Path p, Random r, int len)
		throws IOException {
		DataOutputBuffer dob = new DataOutputBuffer();
		doReturn(getRandomStream(r, len)).when(spylfs).open(p);
		doReturn(new FileStatus(len, false, -1, -1L, -1L, p)).when(
		spylfs).getFileStatus(argThat(new PathEndsWith(p.getName())));
		doReturn(new FSDataOutputStream(dob)).when(spylfs).createInternal(
		argThat(new PathEndsWith(p.getName())),
		eq(EnumSet.of(OVERWRITE)),
		Matchers.<FsPermission>anyObject(), anyInt(), anyShort(), anyLong(),
		Matchers.<Progressable>anyObject(), anyInt(), anyBoolean());
		return dob;
		}
		*/
		/// <exception cref="System.IO.IOException"/>
		[AfterClass]
		public static void DeleteTmpFiles()
		{
			FileContext lfs = FileContext.GetLocalFSFileContext();
			try
			{
				lfs.Delete(BaseTmpPath, true);
			}
			catch (FileNotFoundException)
			{
			}
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual byte[] CreateTmpFile(Path dst, Random r, int len)
		{
			// use unmodified local context
			FileContext lfs = FileContext.GetLocalFSFileContext();
			dst = lfs.MakeQualified(dst);
			lfs.Mkdir(dst.GetParent(), null, true);
			byte[] bytes = new byte[len];
			FSDataOutputStream @out = null;
			try
			{
				@out = lfs.Create(dst, EnumSet.Of(CreateFlag.Create, CreateFlag.Overwrite));
				r.NextBytes(bytes);
				@out.Write(bytes);
			}
			finally
			{
				if (@out != null)
				{
					@out.Close();
				}
			}
			return bytes;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestDirPermissions()
		{
			DeleteTmpFiles();
			string user = "somebody";
			string appId = "app_12345_123";
			FsPermission userCachePerm = new FsPermission(DefaultContainerExecutor.UserPerm);
			FsPermission appCachePerm = new FsPermission(DefaultContainerExecutor.AppcachePerm
				);
			FsPermission fileCachePerm = new FsPermission(DefaultContainerExecutor.FilecachePerm
				);
			FsPermission appDirPerm = new FsPermission(DefaultContainerExecutor.AppdirPerm);
			FsPermission logDirPerm = new FsPermission(DefaultContainerExecutor.LogdirPerm);
			IList<string> localDirs = new AList<string>();
			localDirs.AddItem(new Path(BaseTmpPath, "localDirA").ToString());
			localDirs.AddItem(new Path(BaseTmpPath, "localDirB").ToString());
			IList<string> logDirs = new AList<string>();
			logDirs.AddItem(new Path(BaseTmpPath, "logDirA").ToString());
			logDirs.AddItem(new Path(BaseTmpPath, "logDirB").ToString());
			Configuration conf = new Configuration();
			conf.Set(CommonConfigurationKeys.FsPermissionsUmaskKey, "077");
			FileContext lfs = FileContext.GetLocalFSFileContext(conf);
			DefaultContainerExecutor executor = new DefaultContainerExecutor(lfs);
			executor.Init();
			try
			{
				executor.CreateUserLocalDirs(localDirs, user);
				executor.CreateUserCacheDirs(localDirs, user);
				executor.CreateAppDirs(localDirs, user, appId);
				foreach (string dir in localDirs)
				{
					FileStatus stats = lfs.GetFileStatus(new Path(new Path(dir, ContainerLocalizer.Usercache
						), user));
					NUnit.Framework.Assert.AreEqual(userCachePerm, stats.GetPermission());
				}
				foreach (string dir_1 in localDirs)
				{
					Path userCachePath = new Path(new Path(dir_1, ContainerLocalizer.Usercache), user
						);
					Path appCachePath = new Path(userCachePath, ContainerLocalizer.Appcache);
					FileStatus stats = lfs.GetFileStatus(appCachePath);
					NUnit.Framework.Assert.AreEqual(appCachePerm, stats.GetPermission());
					stats = lfs.GetFileStatus(new Path(userCachePath, ContainerLocalizer.Filecache));
					NUnit.Framework.Assert.AreEqual(fileCachePerm, stats.GetPermission());
					stats = lfs.GetFileStatus(new Path(appCachePath, appId));
					NUnit.Framework.Assert.AreEqual(appDirPerm, stats.GetPermission());
				}
				executor.CreateAppLogDirs(appId, logDirs, user);
				foreach (string dir_2 in logDirs)
				{
					FileStatus stats = lfs.GetFileStatus(new Path(dir_2, appId));
					NUnit.Framework.Assert.AreEqual(logDirPerm, stats.GetPermission());
				}
			}
			finally
			{
				DeleteTmpFiles();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestContainerLaunchError()
		{
			if (Shell.Windows)
			{
				BaseTmpPath = new Path(new FilePath("target").GetAbsolutePath(), typeof(TestDefaultContainerExecutor
					).Name);
			}
			Path localDir = new Path(BaseTmpPath, "localDir");
			IList<string> localDirs = new AList<string>();
			localDirs.AddItem(localDir.ToString());
			IList<string> logDirs = new AList<string>();
			Path logDir = new Path(BaseTmpPath, "logDir");
			logDirs.AddItem(logDir.ToString());
			Configuration conf = new Configuration();
			conf.Set(CommonConfigurationKeys.FsPermissionsUmaskKey, "077");
			conf.Set(YarnConfiguration.NmLocalDirs, localDir.ToString());
			conf.Set(YarnConfiguration.NmLogDirs, logDir.ToString());
			FileContext lfs = FileContext.GetLocalFSFileContext(conf);
			DefaultContainerExecutor mockExec = Org.Mockito.Mockito.Spy(new DefaultContainerExecutor
				(lfs));
			mockExec.SetConf(conf);
			Org.Mockito.Mockito.DoAnswer(new _Answer_245()).When(mockExec).LogOutput(Matchers.Any
				<string>());
			string appSubmitter = "nobody";
			string appId = "APP_ID";
			string containerId = "CONTAINER_ID";
			Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container container
				 = Org.Mockito.Mockito.Mock<Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container
				>();
			ContainerId cId = Org.Mockito.Mockito.Mock<ContainerId>();
			ContainerLaunchContext context = Org.Mockito.Mockito.Mock<ContainerLaunchContext>
				();
			Dictionary<string, string> env = new Dictionary<string, string>();
			Org.Mockito.Mockito.When(container.GetContainerId()).ThenReturn(cId);
			Org.Mockito.Mockito.When(container.GetLaunchContext()).ThenReturn(context);
			try
			{
				Org.Mockito.Mockito.DoAnswer(new _Answer_268()).When(container).Handle(Matchers.Any
					<ContainerDiagnosticsUpdateEvent>());
				Org.Mockito.Mockito.When(cId.ToString()).ThenReturn(containerId);
				Org.Mockito.Mockito.When(cId.GetApplicationAttemptId()).ThenReturn(ApplicationAttemptId
					.NewInstance(ApplicationId.NewInstance(0, 1), 0));
				Org.Mockito.Mockito.When(context.GetEnvironment()).ThenReturn(env);
				mockExec.CreateUserLocalDirs(localDirs, appSubmitter);
				mockExec.CreateUserCacheDirs(localDirs, appSubmitter);
				mockExec.CreateAppDirs(localDirs, appSubmitter, appId);
				mockExec.CreateAppLogDirs(appId, logDirs, appSubmitter);
				Path scriptPath = new Path("file:///bin/echo");
				Path tokensPath = new Path("file:///dev/null");
				if (Shell.Windows)
				{
					FilePath tmp = new FilePath(BaseTmpPath.ToString(), "test_echo.cmd");
					BufferedWriter output = new BufferedWriter(new FileWriter(tmp));
					output.Write("Exit 1");
					output.Write("Echo No such file or directory 1>&2");
					output.Close();
					scriptPath = new Path(tmp.GetAbsolutePath());
					tmp = new FilePath(BaseTmpPath.ToString(), "tokens");
					tmp.CreateNewFile();
					tokensPath = new Path(tmp.GetAbsolutePath());
				}
				Path workDir = localDir;
				Path pidFile = new Path(workDir, "pid.txt");
				mockExec.Init();
				mockExec.ActivateContainer(cId, pidFile);
				int ret = mockExec.LaunchContainer(container, scriptPath, tokensPath, appSubmitter
					, appId, workDir, localDirs, localDirs);
				NUnit.Framework.Assert.AreNotSame(0, ret);
			}
			finally
			{
				mockExec.DeleteAsUser(appSubmitter, localDir);
				mockExec.DeleteAsUser(appSubmitter, logDir);
			}
		}

		private sealed class _Answer_245 : Answer
		{
			public _Answer_245()
			{
			}

			/// <exception cref="System.Exception"/>
			public object Answer(InvocationOnMock invocationOnMock)
			{
				string diagnostics = (string)invocationOnMock.GetArguments()[0];
				NUnit.Framework.Assert.IsTrue("Invalid Diagnostics message: " + diagnostics, diagnostics
					.Contains("No such file or directory"));
				return null;
			}
		}

		private sealed class _Answer_268 : Answer
		{
			public _Answer_268()
			{
			}

			/// <exception cref="System.Exception"/>
			public object Answer(InvocationOnMock invocationOnMock)
			{
				ContainerDiagnosticsUpdateEvent @event = (ContainerDiagnosticsUpdateEvent)invocationOnMock
					.GetArguments()[0];
				NUnit.Framework.Assert.IsTrue("Invalid Diagnostics message: " + @event.GetDiagnosticsUpdate
					(), @event.GetDiagnosticsUpdate().Contains("No such file or directory"));
				return null;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual void TestStartLocalizer()
		{
			IPEndPoint localizationServerAddress;
			Path firstDir = new Path(BaseTmpPath, "localDir1");
			IList<string> localDirs = new AList<string>();
			Path secondDir = new Path(BaseTmpPath, "localDir2");
			IList<string> logDirs = new AList<string>();
			Path logDir = new Path(BaseTmpPath, "logDir");
			Path tokenDir = new Path(BaseTmpPath, "tokenDir");
			FsPermission perms = new FsPermission((short)0x1f8);
			Configuration conf = new Configuration();
			localizationServerAddress = conf.GetSocketAddr(YarnConfiguration.NmBindHost, YarnConfiguration
				.NmLocalizerAddress, YarnConfiguration.DefaultNmLocalizerAddress, YarnConfiguration
				.DefaultNmLocalizerPort);
			FileContext mockLfs = Org.Mockito.Mockito.Spy(FileContext.GetLocalFSFileContext(conf
				));
			FileContext.Util mockUtil = Org.Mockito.Mockito.Spy(mockLfs.Util());
			Org.Mockito.Mockito.DoAnswer(new _Answer_344(mockUtil)).When(mockLfs).Util();
			Org.Mockito.Mockito.DoAnswer(new _Answer_351(firstDir, mockLfs)).When(mockUtil).Copy
				(Matchers.Any<Path>(), Matchers.Any<Path>());
			// throw an Exception when copy token to the first local dir
			// to simulate no space on the first drive
			// copy token to the second local dir
			Org.Mockito.Mockito.DoAnswer(new _Answer_378(firstDir)).When(mockLfs).GetFsStatus
				(Matchers.Any<Path>());
			// let second local directory return more free space than
			// first local directory
			DefaultContainerExecutor mockExec = Org.Mockito.Mockito.Spy(new DefaultContainerExecutor
				(mockLfs));
			mockExec.SetConf(conf);
			localDirs.AddItem(mockLfs.MakeQualified(firstDir).ToString());
			localDirs.AddItem(mockLfs.MakeQualified(secondDir).ToString());
			logDirs.AddItem(mockLfs.MakeQualified(logDir).ToString());
			conf.SetStrings(YarnConfiguration.NmLocalDirs, Sharpen.Collections.ToArray(localDirs
				, new string[localDirs.Count]));
			conf.Set(YarnConfiguration.NmLogDirs, logDir.ToString());
			mockLfs.Mkdir(tokenDir, perms, true);
			Path nmPrivateCTokensPath = new Path(tokenDir, "test.tokens");
			string appSubmitter = "nobody";
			string appId = "APP_ID";
			string locId = "LOC_ID";
			LocalDirsHandlerService dirsHandler = Org.Mockito.Mockito.Mock<LocalDirsHandlerService
				>();
			Org.Mockito.Mockito.When(dirsHandler.GetLocalDirs()).ThenReturn(localDirs);
			Org.Mockito.Mockito.When(dirsHandler.GetLogDirs()).ThenReturn(logDirs);
			try
			{
				mockExec.StartLocalizer(nmPrivateCTokensPath, localizationServerAddress, appSubmitter
					, appId, locId, dirsHandler);
			}
			catch (IOException e)
			{
				NUnit.Framework.Assert.Fail("StartLocalizer failed to copy token file " + e);
			}
			finally
			{
				mockExec.DeleteAsUser(appSubmitter, firstDir);
				mockExec.DeleteAsUser(appSubmitter, secondDir);
				mockExec.DeleteAsUser(appSubmitter, logDir);
				DeleteTmpFiles();
			}
		}

		private sealed class _Answer_344 : Answer
		{
			public _Answer_344(FileContext.Util mockUtil)
			{
				this.mockUtil = mockUtil;
			}

			/// <exception cref="System.Exception"/>
			public object Answer(InvocationOnMock invocationOnMock)
			{
				return mockUtil;
			}

			private readonly FileContext.Util mockUtil;
		}

		private sealed class _Answer_351 : Answer
		{
			public _Answer_351(Path firstDir, FileContext mockLfs)
			{
				this.firstDir = firstDir;
				this.mockLfs = mockLfs;
			}

			/// <exception cref="System.Exception"/>
			public object Answer(InvocationOnMock invocationOnMock)
			{
				Path dest = (Path)invocationOnMock.GetArguments()[1];
				if (dest.ToString().Contains(firstDir.ToString()))
				{
					throw new IOException("No space on this drive " + dest.ToString());
				}
				else
				{
					DataOutputStream tokenOut = null;
					try
					{
						Credentials credentials = new Credentials();
						tokenOut = mockLfs.Create(dest, EnumSet.Of(CreateFlag.Create, CreateFlag.Overwrite
							));
						credentials.WriteTokenStorageToStream(tokenOut);
					}
					finally
					{
						if (tokenOut != null)
						{
							tokenOut.Close();
						}
					}
				}
				return null;
			}

			private readonly Path firstDir;

			private readonly FileContext mockLfs;
		}

		private sealed class _Answer_378 : Answer
		{
			public _Answer_378(Path firstDir)
			{
				this.firstDir = firstDir;
			}

			/// <exception cref="System.Exception"/>
			public object Answer(InvocationOnMock invocationOnMock)
			{
				Path p = (Path)invocationOnMock.GetArguments()[0];
				if (p.ToString().Contains(firstDir.ToString()))
				{
					return new FsStatus(2000, 2000, 0);
				}
				else
				{
					return new FsStatus(1000, 0, 1000);
				}
			}

			private readonly Path firstDir;
		}
		//  @Test
		//  public void testInit() throws IOException, InterruptedException {
		//    Configuration conf = new Configuration();
		//    AbstractFileSystem spylfs =
		//      spy(FileContext.getLocalFSFileContext().getDefaultFileSystem());
		//    // don't actually create dirs
		//    //doNothing().when(spylfs).mkdir(Matchers.<Path>anyObject(),
		//    //    Matchers.<FsPermission>anyObject(), anyBoolean());
		//    FileContext lfs = FileContext.getFileContext(spylfs, conf);
		//
		//    Path basedir = new Path("target",
		//        TestDefaultContainerExecutor.class.getSimpleName());
		//    List<String> localDirs = new ArrayList<String>();
		//    List<Path> localPaths = new ArrayList<Path>();
		//    for (int i = 0; i < 4; ++i) {
		//      Path p = new Path(basedir, i + "");
		//      lfs.mkdir(p, null, true);
		//      localPaths.add(p);
		//      localDirs.add(p.toString());
		//    }
		//    final String user = "yak";
		//    final String appId = "app_RM_0";
		//    final Path logDir = new Path(basedir, "logs");
		//    final Path nmLocal = new Path(basedir, "nmPrivate/" + user + "/" + appId);
		//    final InetSocketAddress nmAddr = new InetSocketAddress("foobar", 8040);
		//    System.out.println("NMLOCAL: " + nmLocal);
		//    Random r = new Random();
		//
		//    /*
		//    // XXX FileContext cannot be reasonably mocked to do this
		//    // mock jobFiles copy
		//    long fileSeed = r.nextLong();
		//    r.setSeed(fileSeed);
		//    System.out.println("SEED: " + seed);
		//    Path fileCachePath = new Path(nmLocal, ApplicationLocalizer.FILECACHE_FILE);
		//    DataOutputBuffer fileCacheBytes = mockStream(spylfs, fileCachePath, r, 512);
		//
		//    // mock jobTokens copy
		//    long jobSeed = r.nextLong();
		//    r.setSeed(jobSeed);
		//    System.out.println("SEED: " + seed);
		//    Path jobTokenPath = new Path(nmLocal, ApplicationLocalizer.JOBTOKEN_FILE);
		//    DataOutputBuffer jobTokenBytes = mockStream(spylfs, jobTokenPath, r, 512);
		//    */
		//
		//    // create jobFiles
		//    long fileSeed = r.nextLong();
		//    r.setSeed(fileSeed);
		//    System.out.println("SEED: " + fileSeed);
		//    Path fileCachePath = new Path(nmLocal, ApplicationLocalizer.FILECACHE_FILE);
		//    byte[] fileCacheBytes = createTmpFile(fileCachePath, r, 512);
		//
		//    // create jobTokens
		//    long jobSeed = r.nextLong();
		//    r.setSeed(jobSeed);
		//    System.out.println("SEED: " + jobSeed);
		//    Path jobTokenPath = new Path(nmLocal, ApplicationLocalizer.JOBTOKEN_FILE);
		//    byte[] jobTokenBytes = createTmpFile(jobTokenPath, r, 512);
		//
		//    DefaultContainerExecutor dce = new DefaultContainerExecutor(lfs);
		//    Localization mockLocalization = mock(Localization.class);
		//    ApplicationLocalizer spyLocalizer =
		//      spy(new ApplicationLocalizer(lfs, user, appId, logDir,
		//            localPaths));
		//    // ignore cache localization
		//    doNothing().when(spyLocalizer).localizeFiles(
		//        Matchers.<Localization>anyObject(), Matchers.<Path>anyObject());
		//    Path workingDir = lfs.getWorkingDirectory();
		//    dce.initApplication(spyLocalizer, nmLocal, mockLocalization, localPaths);
		//    lfs.setWorkingDirectory(workingDir);
		//
		//    for (Path localdir : localPaths) {
		//      Path userdir = lfs.makeQualified(new Path(localdir,
		//            new Path(ApplicationLocalizer.USERCACHE, user)));
		//      // $localdir/$user
		//      verify(spylfs).mkdir(userdir,
		//          new FsPermission(ApplicationLocalizer.USER_PERM), true);
		//      // $localdir/$user/appcache
		//      Path jobdir = new Path(userdir, ApplicationLocalizer.appcache);
		//      verify(spylfs).mkdir(jobdir,
		//          new FsPermission(ApplicationLocalizer.appcache_PERM), true);
		//      // $localdir/$user/filecache
		//      Path filedir = new Path(userdir, ApplicationLocalizer.FILECACHE);
		//      verify(spylfs).mkdir(filedir,
		//          new FsPermission(ApplicationLocalizer.FILECACHE_PERM), true);
		//      // $localdir/$user/appcache/$appId
		//      Path appdir = new Path(jobdir, appId);
		//      verify(spylfs).mkdir(appdir,
		//          new FsPermission(ApplicationLocalizer.APPDIR_PERM), true);
		//      // $localdir/$user/appcache/$appId/work
		//      Path workdir = new Path(appdir, ApplicationLocalizer.WORKDIR);
		//      verify(spylfs, atMost(1)).mkdir(workdir, FsPermission.getDefault(), true);
		//    }
		//    // $logdir/$appId
		//    Path logdir = new Path(lfs.makeQualified(logDir), appId);
		//    verify(spylfs).mkdir(logdir,
		//        new FsPermission(ApplicationLocalizer.LOGDIR_PERM), true);
		//  }
	}
}
