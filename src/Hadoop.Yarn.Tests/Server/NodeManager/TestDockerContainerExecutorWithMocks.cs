using System.Collections.Generic;
using System.IO;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager
{
	/// <summary>Mock tests for docker container executor</summary>
	public class TestDockerContainerExecutorWithMocks
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(TestDockerContainerExecutorWithMocks
			));

		public const string DockerLaunchCommand = "/bin/true";

		private DockerContainerExecutor dockerContainerExecutor = null;

		private LocalDirsHandlerService dirsHandler;

		private Path workDir;

		private FileContext lfs;

		private string yarnImage;

		[SetUp]
		public virtual void Setup()
		{
			Assume.AssumeTrue(Shell.Linux);
			FilePath f = new FilePath("./src/test/resources/mock-container-executor");
			if (!FileUtil.CanExecute(f))
			{
				FileUtil.SetExecutable(f, true);
			}
			string executorPath = f.GetAbsolutePath();
			Configuration conf = new Configuration();
			yarnImage = "yarnImage";
			long time = Runtime.CurrentTimeMillis();
			conf.Set(YarnConfiguration.NmLinuxContainerExecutorPath, executorPath);
			conf.Set(YarnConfiguration.NmLocalDirs, "/tmp/nm-local-dir" + time);
			conf.Set(YarnConfiguration.NmLogDirs, "/tmp/userlogs" + time);
			conf.Set(YarnConfiguration.NmDockerContainerExecutorImageName, yarnImage);
			conf.Set(YarnConfiguration.NmDockerContainerExecutorExecName, DockerLaunchCommand
				);
			dockerContainerExecutor = new DockerContainerExecutor();
			dirsHandler = new LocalDirsHandlerService();
			dirsHandler.Init(conf);
			dockerContainerExecutor.SetConf(conf);
			lfs = null;
			try
			{
				lfs = FileContext.GetLocalFSFileContext();
				workDir = new Path("/tmp/temp-" + Runtime.CurrentTimeMillis());
				lfs.Mkdir(workDir, FsPermission.GetDirDefault(), true);
			}
			catch (IOException e)
			{
				throw new RuntimeException(e);
			}
		}

		[NUnit.Framework.TearDown]
		public virtual void TearDown()
		{
			try
			{
				if (lfs != null)
				{
					lfs.Delete(workDir, true);
				}
			}
			catch (IOException e)
			{
				throw new RuntimeException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestContainerInitSecure()
		{
			dockerContainerExecutor.GetConf().Set(CommonConfigurationKeys.HadoopSecurityAuthentication
				, "kerberos");
			dockerContainerExecutor.Init();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestContainerLaunchNullImage()
		{
			string appSubmitter = "nobody";
			string appId = "APP_ID";
			string containerId = "CONTAINER_ID";
			string testImage = string.Empty;
			Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container container
				 = Org.Mockito.Mockito.Mock<Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container
				>(Org.Mockito.Mockito.ReturnsDeepStubs);
			ContainerId cId = Org.Mockito.Mockito.Mock<ContainerId>(Org.Mockito.Mockito.ReturnsDeepStubs
				);
			ContainerLaunchContext context = Org.Mockito.Mockito.Mock<ContainerLaunchContext>
				();
			Dictionary<string, string> env = new Dictionary<string, string>();
			Org.Mockito.Mockito.When(container.GetContainerId()).ThenReturn(cId);
			Org.Mockito.Mockito.When(container.GetLaunchContext()).ThenReturn(context);
			Org.Mockito.Mockito.When(cId.GetApplicationAttemptId().GetApplicationId().ToString
				()).ThenReturn(appId);
			Org.Mockito.Mockito.When(cId.ToString()).ThenReturn(containerId);
			Org.Mockito.Mockito.When(context.GetEnvironment()).ThenReturn(env);
			env[YarnConfiguration.NmDockerContainerExecutorImageName] = testImage;
			dockerContainerExecutor.GetConf().Set(YarnConfiguration.NmDockerContainerExecutorImageName
				, testImage);
			Path scriptPath = new Path("file:///bin/echo");
			Path tokensPath = new Path("file:///dev/null");
			Path pidFile = new Path(workDir, "pid.txt");
			dockerContainerExecutor.ActivateContainer(cId, pidFile);
			dockerContainerExecutor.LaunchContainer(container, scriptPath, tokensPath, appSubmitter
				, appId, workDir, dirsHandler.GetLocalDirs(), dirsHandler.GetLogDirs());
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestContainerLaunchInvalidImage()
		{
			string appSubmitter = "nobody";
			string appId = "APP_ID";
			string containerId = "CONTAINER_ID";
			string testImage = "testrepo.com/test-image rm -rf $HADOOP_PREFIX/*";
			Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container container
				 = Org.Mockito.Mockito.Mock<Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container
				>(Org.Mockito.Mockito.ReturnsDeepStubs);
			ContainerId cId = Org.Mockito.Mockito.Mock<ContainerId>(Org.Mockito.Mockito.ReturnsDeepStubs
				);
			ContainerLaunchContext context = Org.Mockito.Mockito.Mock<ContainerLaunchContext>
				();
			Dictionary<string, string> env = new Dictionary<string, string>();
			Org.Mockito.Mockito.When(container.GetContainerId()).ThenReturn(cId);
			Org.Mockito.Mockito.When(container.GetLaunchContext()).ThenReturn(context);
			Org.Mockito.Mockito.When(cId.GetApplicationAttemptId().GetApplicationId().ToString
				()).ThenReturn(appId);
			Org.Mockito.Mockito.When(cId.ToString()).ThenReturn(containerId);
			Org.Mockito.Mockito.When(context.GetEnvironment()).ThenReturn(env);
			env[YarnConfiguration.NmDockerContainerExecutorImageName] = testImage;
			dockerContainerExecutor.GetConf().Set(YarnConfiguration.NmDockerContainerExecutorImageName
				, testImage);
			Path scriptPath = new Path("file:///bin/echo");
			Path tokensPath = new Path("file:///dev/null");
			Path pidFile = new Path(workDir, "pid.txt");
			dockerContainerExecutor.ActivateContainer(cId, pidFile);
			dockerContainerExecutor.LaunchContainer(container, scriptPath, tokensPath, appSubmitter
				, appId, workDir, dirsHandler.GetLocalDirs(), dirsHandler.GetLogDirs());
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestContainerLaunch()
		{
			string appSubmitter = "nobody";
			string appId = "APP_ID";
			string containerId = "CONTAINER_ID";
			string testImage = "\"sequenceiq/hadoop-docker:2.4.1\"";
			Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container container
				 = Org.Mockito.Mockito.Mock<Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container
				>(Org.Mockito.Mockito.ReturnsDeepStubs);
			ContainerId cId = Org.Mockito.Mockito.Mock<ContainerId>(Org.Mockito.Mockito.ReturnsDeepStubs
				);
			ContainerLaunchContext context = Org.Mockito.Mockito.Mock<ContainerLaunchContext>
				();
			Dictionary<string, string> env = new Dictionary<string, string>();
			Org.Mockito.Mockito.When(container.GetContainerId()).ThenReturn(cId);
			Org.Mockito.Mockito.When(container.GetLaunchContext()).ThenReturn(context);
			Org.Mockito.Mockito.When(cId.GetApplicationAttemptId().GetApplicationId().ToString
				()).ThenReturn(appId);
			Org.Mockito.Mockito.When(cId.ToString()).ThenReturn(containerId);
			Org.Mockito.Mockito.When(context.GetEnvironment()).ThenReturn(env);
			env[YarnConfiguration.NmDockerContainerExecutorImageName] = testImage;
			Path scriptPath = new Path("file:///bin/echo");
			Path tokensPath = new Path("file:///dev/null");
			Path pidFile = new Path(workDir, "pid");
			dockerContainerExecutor.ActivateContainer(cId, pidFile);
			int ret = dockerContainerExecutor.LaunchContainer(container, scriptPath, tokensPath
				, appSubmitter, appId, workDir, dirsHandler.GetLocalDirs(), dirsHandler.GetLogDirs
				());
			NUnit.Framework.Assert.AreEqual(0, ret);
			//get the script
			Path sessionScriptPath = new Path(workDir, Shell.AppendScriptExtension(DockerContainerExecutor
				.DockerContainerExecutorSessionScript));
			LineNumberReader lnr = new LineNumberReader(new FileReader(sessionScriptPath.ToString
				()));
			bool cmdFound = false;
			IList<string> localDirs = DirsToMount(dirsHandler.GetLocalDirs());
			IList<string> logDirs = DirsToMount(dirsHandler.GetLogDirs());
			IList<string> workDirMount = DirsToMount(Sharpen.Collections.SingletonList(workDir
				.ToUri().GetPath()));
			IList<string> expectedCommands = new AList<string>(Arrays.AsList(DockerLaunchCommand
				, "run", "--rm", "--net=host", "--name", containerId));
			Sharpen.Collections.AddAll(expectedCommands, localDirs);
			Sharpen.Collections.AddAll(expectedCommands, logDirs);
			Sharpen.Collections.AddAll(expectedCommands, workDirMount);
			string shellScript = workDir + "/launch_container.sh";
			Sharpen.Collections.AddAll(expectedCommands, Arrays.AsList(testImage.ReplaceAll("['\"]"
				, string.Empty), "bash", "\"" + shellScript + "\""));
			string expectedPidString = "echo `/bin/true inspect --format {{.State.Pid}} " + containerId
				 + "` > " + pidFile.ToString() + ".tmp";
			bool pidSetterFound = false;
			while (lnr.Ready())
			{
				string line = lnr.ReadLine();
				Log.Debug("line: " + line);
				if (line.StartsWith(DockerLaunchCommand))
				{
					IList<string> command = new AList<string>();
					foreach (string s in line.Split("\\s+"))
					{
						command.AddItem(s.Trim());
					}
					NUnit.Framework.Assert.AreEqual(expectedCommands, command);
					cmdFound = true;
				}
				else
				{
					if (line.StartsWith("echo"))
					{
						NUnit.Framework.Assert.AreEqual(expectedPidString, line);
						pidSetterFound = true;
					}
				}
			}
			NUnit.Framework.Assert.IsTrue(cmdFound);
			NUnit.Framework.Assert.IsTrue(pidSetterFound);
		}

		private IList<string> DirsToMount(IList<string> dirs)
		{
			IList<string> localDirs = new AList<string>();
			foreach (string dir in dirs)
			{
				localDirs.AddItem("-v");
				localDirs.AddItem(dir + ":" + dir);
			}
			return localDirs;
		}
	}
}
