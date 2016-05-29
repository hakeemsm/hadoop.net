using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container;
using Org.Mockito;
using Org.Mockito.Invocation;
using Org.Mockito.Stubbing;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager
{
	public class TestLinuxContainerExecutorWithMocks
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(TestLinuxContainerExecutorWithMocks
			));

		private LinuxContainerExecutor mockExec = null;

		private readonly FilePath mockParamFile = new FilePath("./params.txt");

		private LocalDirsHandlerService dirsHandler;

		private void DeleteMockParamFile()
		{
			if (mockParamFile.Exists())
			{
				mockParamFile.Delete();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private IList<string> ReadMockParams()
		{
			List<string> ret = new List<string>();
			LineNumberReader reader = new LineNumberReader(new FileReader(mockParamFile));
			string line;
			while ((line = reader.ReadLine()) != null)
			{
				ret.AddItem(line);
			}
			reader.Close();
			return ret;
		}

		[SetUp]
		public virtual void Setup()
		{
			Assume.AssumeTrue(!Path.Windows);
			FilePath f = new FilePath("./src/test/resources/mock-container-executor");
			if (!FileUtil.CanExecute(f))
			{
				FileUtil.SetExecutable(f, true);
			}
			string executorPath = f.GetAbsolutePath();
			Configuration conf = new Configuration();
			conf.Set(YarnConfiguration.NmLinuxContainerExecutorPath, executorPath);
			mockExec = new LinuxContainerExecutor();
			dirsHandler = new LocalDirsHandlerService();
			dirsHandler.Init(conf);
			mockExec.SetConf(conf);
		}

		[NUnit.Framework.TearDown]
		public virtual void TearDown()
		{
			DeleteMockParamFile();
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestContainerLaunch()
		{
			string appSubmitter = "nobody";
			string cmd = LinuxContainerExecutor.Commands.LaunchContainer.GetValue().ToString(
				);
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
			Org.Mockito.Mockito.When(cId.ToString()).ThenReturn(containerId);
			Org.Mockito.Mockito.When(context.GetEnvironment()).ThenReturn(env);
			Path scriptPath = new Path("file:///bin/echo");
			Path tokensPath = new Path("file:///dev/null");
			Path workDir = new Path("/tmp");
			Path pidFile = new Path(workDir, "pid.txt");
			mockExec.ActivateContainer(cId, pidFile);
			int ret = mockExec.LaunchContainer(container, scriptPath, tokensPath, appSubmitter
				, appId, workDir, dirsHandler.GetLocalDirs(), dirsHandler.GetLogDirs());
			NUnit.Framework.Assert.AreEqual(0, ret);
			NUnit.Framework.Assert.AreEqual(Arrays.AsList(YarnConfiguration.DefaultNmNonsecureModeLocalUser
				, appSubmitter, cmd, appId, containerId, workDir.ToString(), "/bin/echo", "/dev/null"
				, pidFile.ToString(), StringUtils.Join(",", dirsHandler.GetLocalDirs()), StringUtils
				.Join(",", dirsHandler.GetLogDirs()), "cgroups=none"), ReadMockParams());
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestContainerLaunchWithPriority()
		{
			// set the scheduler priority to make sure still works with nice -n prio
			FilePath f = new FilePath("./src/test/resources/mock-container-executor");
			if (!FileUtil.CanExecute(f))
			{
				FileUtil.SetExecutable(f, true);
			}
			string executorPath = f.GetAbsolutePath();
			Configuration conf = new Configuration();
			conf.Set(YarnConfiguration.NmLinuxContainerExecutorPath, executorPath);
			conf.SetInt(YarnConfiguration.NmContainerExecutorSchedPriority, 2);
			mockExec.SetConf(conf);
			IList<string> command = new AList<string>();
			mockExec.AddSchedPriorityCommand(command);
			NUnit.Framework.Assert.AreEqual("first should be nice", "nice", command[0]);
			NUnit.Framework.Assert.AreEqual("second should be -n", "-n", command[1]);
			NUnit.Framework.Assert.AreEqual("third should be the priority", Sharpen.Extensions.ToString
				(2), command[2]);
			TestContainerLaunch();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestLaunchCommandWithoutPriority()
		{
			// make sure the command doesn't contain the nice -n since priority
			// not specified
			IList<string> command = new AList<string>();
			mockExec.AddSchedPriorityCommand(command);
			NUnit.Framework.Assert.AreEqual("addSchedPriority should be empty", 0, command.Count
				);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestStartLocalizer()
		{
			IPEndPoint address = IPEndPoint.CreateUnresolved("localhost", 8040);
			Path nmPrivateCTokensPath = new Path("file:///bin/nmPrivateCTokensPath");
			try
			{
				mockExec.StartLocalizer(nmPrivateCTokensPath, address, "test", "application_0", "12345"
					, dirsHandler);
				IList<string> result = ReadMockParams();
				NUnit.Framework.Assert.AreEqual(result.Count, 17);
				NUnit.Framework.Assert.AreEqual(result[0], YarnConfiguration.DefaultNmNonsecureModeLocalUser
					);
				NUnit.Framework.Assert.AreEqual(result[1], "test");
				NUnit.Framework.Assert.AreEqual(result[2], "0");
				NUnit.Framework.Assert.AreEqual(result[3], "application_0");
				NUnit.Framework.Assert.AreEqual(result[4], "/bin/nmPrivateCTokensPath");
				NUnit.Framework.Assert.AreEqual(result[8], "-classpath");
				NUnit.Framework.Assert.AreEqual(result[11], "org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ContainerLocalizer"
					);
				NUnit.Framework.Assert.AreEqual(result[12], "test");
				NUnit.Framework.Assert.AreEqual(result[13], "application_0");
				NUnit.Framework.Assert.AreEqual(result[14], "12345");
				NUnit.Framework.Assert.AreEqual(result[15], "localhost");
				NUnit.Framework.Assert.AreEqual(result[16], "8040");
			}
			catch (Exception e)
			{
				Log.Error("Error:" + e.Message, e);
				NUnit.Framework.Assert.Fail();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestContainerLaunchError()
		{
			// reinitialize executer
			FilePath f = new FilePath("./src/test/resources/mock-container-executer-with-error"
				);
			if (!FileUtil.CanExecute(f))
			{
				FileUtil.SetExecutable(f, true);
			}
			string executorPath = f.GetAbsolutePath();
			Configuration conf = new Configuration();
			conf.Set(YarnConfiguration.NmLinuxContainerExecutorPath, executorPath);
			conf.Set(YarnConfiguration.NmLocalDirs, "file:///bin/echo");
			conf.Set(YarnConfiguration.NmLogDirs, "file:///dev/null");
			mockExec = Org.Mockito.Mockito.Spy(new LinuxContainerExecutor());
			Org.Mockito.Mockito.DoAnswer(new _Answer_226()).When(mockExec).LogOutput(Matchers.Any
				<string>());
			dirsHandler = new LocalDirsHandlerService();
			dirsHandler.Init(conf);
			mockExec.SetConf(conf);
			string appSubmitter = "nobody";
			string cmd = LinuxContainerExecutor.Commands.LaunchContainer.GetValue().ToString(
				);
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
			Org.Mockito.Mockito.DoAnswer(new _Answer_254()).When(container).Handle(Matchers.Any
				<ContainerDiagnosticsUpdateEvent>());
			Org.Mockito.Mockito.When(cId.ToString()).ThenReturn(containerId);
			Org.Mockito.Mockito.When(context.GetEnvironment()).ThenReturn(env);
			Path scriptPath = new Path("file:///bin/echo");
			Path tokensPath = new Path("file:///dev/null");
			Path workDir = new Path("/tmp");
			Path pidFile = new Path(workDir, "pid.txt");
			mockExec.ActivateContainer(cId, pidFile);
			int ret = mockExec.LaunchContainer(container, scriptPath, tokensPath, appSubmitter
				, appId, workDir, dirsHandler.GetLocalDirs(), dirsHandler.GetLogDirs());
			NUnit.Framework.Assert.AreNotSame(0, ret);
			NUnit.Framework.Assert.AreEqual(Arrays.AsList(YarnConfiguration.DefaultNmNonsecureModeLocalUser
				, appSubmitter, cmd, appId, containerId, workDir.ToString(), "/bin/echo", "/dev/null"
				, pidFile.ToString(), StringUtils.Join(",", dirsHandler.GetLocalDirs()), StringUtils
				.Join(",", dirsHandler.GetLogDirs()), "cgroups=none"), ReadMockParams());
		}

		private sealed class _Answer_226 : Answer
		{
			public _Answer_226()
			{
			}

			/// <exception cref="System.Exception"/>
			public object Answer(InvocationOnMock invocationOnMock)
			{
				string diagnostics = (string)invocationOnMock.GetArguments()[0];
				NUnit.Framework.Assert.IsTrue("Invalid Diagnostics message: " + diagnostics, diagnostics
					.Contains("badcommand"));
				return null;
			}
		}

		private sealed class _Answer_254 : Answer
		{
			public _Answer_254()
			{
			}

			/// <exception cref="System.Exception"/>
			public object Answer(InvocationOnMock invocationOnMock)
			{
				ContainerDiagnosticsUpdateEvent @event = (ContainerDiagnosticsUpdateEvent)invocationOnMock
					.GetArguments()[0];
				NUnit.Framework.Assert.IsTrue("Invalid Diagnostics message: " + @event.GetDiagnosticsUpdate
					(), @event.GetDiagnosticsUpdate().Contains("badcommand"));
				return null;
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestInit()
		{
			mockExec.Init();
			NUnit.Framework.Assert.AreEqual(Arrays.AsList("--checksetup"), ReadMockParams());
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestContainerKill()
		{
			string appSubmitter = "nobody";
			string cmd = LinuxContainerExecutor.Commands.SignalContainer.GetValue().ToString(
				);
			ContainerExecutor.Signal signal = ContainerExecutor.Signal.Quit;
			string sigVal = signal.GetValue().ToString();
			mockExec.SignalContainer(appSubmitter, "1000", signal);
			NUnit.Framework.Assert.AreEqual(Arrays.AsList(YarnConfiguration.DefaultNmNonsecureModeLocalUser
				, appSubmitter, cmd, "1000", sigVal), ReadMockParams());
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestDeleteAsUser()
		{
			string appSubmitter = "nobody";
			string cmd = LinuxContainerExecutor.Commands.DeleteAsUser.GetValue().ToString();
			Path dir = new Path("/tmp/testdir");
			Path testFile = new Path("testfile");
			Path baseDir0 = new Path("/grid/0/BaseDir");
			Path baseDir1 = new Path("/grid/1/BaseDir");
			mockExec.DeleteAsUser(appSubmitter, dir);
			NUnit.Framework.Assert.AreEqual(Arrays.AsList(YarnConfiguration.DefaultNmNonsecureModeLocalUser
				, appSubmitter, cmd, "/tmp/testdir"), ReadMockParams());
			mockExec.DeleteAsUser(appSubmitter, null);
			NUnit.Framework.Assert.AreEqual(Arrays.AsList(YarnConfiguration.DefaultNmNonsecureModeLocalUser
				, appSubmitter, cmd, string.Empty), ReadMockParams());
			mockExec.DeleteAsUser(appSubmitter, testFile, baseDir0, baseDir1);
			NUnit.Framework.Assert.AreEqual(Arrays.AsList(YarnConfiguration.DefaultNmNonsecureModeLocalUser
				, appSubmitter, cmd, testFile.ToString(), baseDir0.ToString(), baseDir1.ToString
				()), ReadMockParams());
			mockExec.DeleteAsUser(appSubmitter, null, baseDir0, baseDir1);
			NUnit.Framework.Assert.AreEqual(Arrays.AsList(YarnConfiguration.DefaultNmNonsecureModeLocalUser
				, appSubmitter, cmd, string.Empty, baseDir0.ToString(), baseDir1.ToString()), ReadMockParams
				());
			FilePath f = new FilePath("./src/test/resources/mock-container-executer-with-error"
				);
			if (!FileUtil.CanExecute(f))
			{
				FileUtil.SetExecutable(f, true);
			}
			string executorPath = f.GetAbsolutePath();
			Configuration conf = new Configuration();
			conf.Set(YarnConfiguration.NmLinuxContainerExecutorPath, executorPath);
			mockExec.SetConf(conf);
			mockExec.DeleteAsUser(appSubmitter, dir);
			NUnit.Framework.Assert.AreEqual(Arrays.AsList(YarnConfiguration.DefaultNmNonsecureModeLocalUser
				, appSubmitter, cmd, "/tmp/testdir"), ReadMockParams());
			mockExec.DeleteAsUser(appSubmitter, null);
			NUnit.Framework.Assert.AreEqual(Arrays.AsList(YarnConfiguration.DefaultNmNonsecureModeLocalUser
				, appSubmitter, cmd, string.Empty), ReadMockParams());
			mockExec.DeleteAsUser(appSubmitter, testFile, baseDir0, baseDir1);
			NUnit.Framework.Assert.AreEqual(Arrays.AsList(YarnConfiguration.DefaultNmNonsecureModeLocalUser
				, appSubmitter, cmd, testFile.ToString(), baseDir0.ToString(), baseDir1.ToString
				()), ReadMockParams());
			mockExec.DeleteAsUser(appSubmitter, null, baseDir0, baseDir1);
			NUnit.Framework.Assert.AreEqual(Arrays.AsList(YarnConfiguration.DefaultNmNonsecureModeLocalUser
				, appSubmitter, cmd, string.Empty, baseDir0.ToString(), baseDir1.ToString()), ReadMockParams
				());
		}
	}
}
