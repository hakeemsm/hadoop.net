using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Base;
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
	/// <summary>
	/// This is intended to test the DockerContainerExecutor code, but it requires docker
	/// to be installed.
	/// </summary>
	/// <remarks>
	/// This is intended to test the DockerContainerExecutor code, but it requires docker
	/// to be installed.
	/// <br /><ol>
	/// <li>Install docker, and Compile the code with docker-service-url set to the host and port
	/// where docker service is running.
	/// <br /><pre><code>
	/// &gt; mvn clean install -Ddocker-service-url=tcp://0.0.0.0:4243
	/// -DskipTests
	/// </code></pre>
	/// </remarks>
	public class TestDockerContainerExecutor
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(TestDockerContainerExecutor
			));

		private static FilePath workSpace = null;

		private DockerContainerExecutor exec = null;

		private LocalDirsHandlerService dirsHandler;

		private Path workDir;

		private FileContext lfs;

		private string yarnImage;

		private int id = 0;

		private string appSubmitter;

		private string dockerUrl;

		private string testImage = "centos";

		private string dockerExec;

		private string containerIdStr;

		private ContainerId GetNextContainerId()
		{
			ContainerId cId = Org.Mockito.Mockito.Mock<ContainerId>(Org.Mockito.Mockito.ReturnsDeepStubs
				);
			string id = "CONTAINER_" + Runtime.CurrentTimeMillis();
			Org.Mockito.Mockito.When(cId.ToString()).ThenReturn(id);
			return cId;
		}

		[SetUp]
		public virtual void Setup()
		{
			try
			{
				lfs = FileContext.GetLocalFSFileContext();
				workDir = new Path("/tmp/temp-" + Runtime.CurrentTimeMillis());
				workSpace = new FilePath(workDir.ToUri().GetPath());
				lfs.Mkdir(workDir, FsPermission.GetDirDefault(), true);
			}
			catch (IOException e)
			{
				throw new RuntimeException(e);
			}
			Configuration conf = new Configuration();
			yarnImage = "yarnImage";
			long time = Runtime.CurrentTimeMillis();
			conf.Set(YarnConfiguration.NmLocalDirs, "/tmp/nm-local-dir" + time);
			conf.Set(YarnConfiguration.NmLogDirs, "/tmp/userlogs" + time);
			dockerUrl = Runtime.GetProperty("docker-service-url");
			Log.Info("dockerUrl: " + dockerUrl);
			if (Strings.IsNullOrEmpty(dockerUrl))
			{
				return;
			}
			dockerUrl = " -H " + dockerUrl;
			dockerExec = "docker " + dockerUrl;
			conf.Set(YarnConfiguration.NmDockerContainerExecutorImageName, yarnImage);
			conf.Set(YarnConfiguration.NmDockerContainerExecutorExecName, dockerExec);
			exec = new DockerContainerExecutor();
			dirsHandler = new LocalDirsHandlerService();
			dirsHandler.Init(conf);
			exec.SetConf(conf);
			appSubmitter = Runtime.GetProperty("application.submitter");
			if (appSubmitter == null || appSubmitter.IsEmpty())
			{
				appSubmitter = "nobody";
			}
			ShellExec(dockerExec + " pull " + testImage);
		}

		private Shell.ShellCommandExecutor ShellExec(string command)
		{
			try
			{
				Shell.ShellCommandExecutor shExec = new Shell.ShellCommandExecutor(command.Split(
					"\\s+"), new FilePath(workDir.ToUri().GetPath()), Sharpen.Runtime.GetEnv());
				shExec.Execute();
				return shExec;
			}
			catch (IOException e)
			{
				throw new RuntimeException(e);
			}
		}

		private bool ShouldRun()
		{
			return exec != null;
		}

		/// <exception cref="System.IO.IOException"/>
		private int RunAndBlock(ContainerId cId, IDictionary<string, string> launchCtxEnv
			, params string[] cmd)
		{
			string appId = "APP_" + Runtime.CurrentTimeMillis();
			Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container container
				 = Org.Mockito.Mockito.Mock<Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container
				>();
			ContainerLaunchContext context = Org.Mockito.Mockito.Mock<ContainerLaunchContext>
				();
			Org.Mockito.Mockito.When(container.GetContainerId()).ThenReturn(cId);
			Org.Mockito.Mockito.When(container.GetLaunchContext()).ThenReturn(context);
			Org.Mockito.Mockito.When(cId.GetApplicationAttemptId().GetApplicationId().ToString
				()).ThenReturn(appId);
			Org.Mockito.Mockito.When(context.GetEnvironment()).ThenReturn(launchCtxEnv);
			string script = WriteScriptFile(launchCtxEnv, cmd);
			Path scriptPath = new Path(script);
			Path tokensPath = new Path("/dev/null");
			Path workDir = new Path(workSpace.GetAbsolutePath());
			Path pidFile = new Path(workDir, "pid.txt");
			exec.ActivateContainer(cId, pidFile);
			return exec.LaunchContainer(container, scriptPath, tokensPath, appSubmitter, appId
				, workDir, dirsHandler.GetLocalDirs(), dirsHandler.GetLogDirs());
		}

		/// <exception cref="System.IO.IOException"/>
		private string WriteScriptFile(IDictionary<string, string> launchCtxEnv, params string
			[] cmd)
		{
			FilePath f = FilePath.CreateTempFile("TestDockerContainerExecutor", ".sh");
			f.DeleteOnExit();
			PrintWriter p = new PrintWriter(new FileOutputStream(f));
			foreach (KeyValuePair<string, string> entry in launchCtxEnv)
			{
				p.WriteLine("export " + entry.Key + "=\"" + entry.Value + "\"");
			}
			foreach (string part in cmd)
			{
				p.Write(part.Replace("\\", "\\\\").Replace("'", "\\'"));
				p.Write(" ");
			}
			p.WriteLine();
			p.Close();
			return f.GetAbsolutePath();
		}

		[NUnit.Framework.TearDown]
		public virtual void TearDown()
		{
			try
			{
				lfs.Delete(workDir, true);
			}
			catch (IOException e)
			{
				throw new RuntimeException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestLaunchContainer()
		{
			if (!ShouldRun())
			{
				Log.Warn("Docker not installed, aborting test.");
				return;
			}
			IDictionary<string, string> env = new Dictionary<string, string>();
			env[YarnConfiguration.NmDockerContainerExecutorImageName] = testImage;
			string touchFileName = "touch-file-" + Runtime.CurrentTimeMillis();
			FilePath touchFile = new FilePath(dirsHandler.GetLocalDirs()[0], touchFileName);
			ContainerId cId = GetNextContainerId();
			int ret = RunAndBlock(cId, env, "touch", touchFile.GetAbsolutePath(), "&&", "cp", 
				touchFile.GetAbsolutePath(), "/");
			NUnit.Framework.Assert.AreEqual(0, ret);
		}
	}
}
