using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Local;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Localizer;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager
{
	/// <summary>
	/// This is intended to test the LinuxContainerExecutor code, but because of
	/// some security restrictions this can only be done with some special setup
	/// first.
	/// </summary>
	/// <remarks>
	/// This is intended to test the LinuxContainerExecutor code, but because of
	/// some security restrictions this can only be done with some special setup
	/// first.
	/// <br /><ol>
	/// <li>Compile the code with container-executor.conf.dir set to the location you
	/// want for testing.
	/// <br /><pre><code>
	/// &gt; mvn clean install -Pnative -Dcontainer-executor.conf.dir=/etc/hadoop
	/// -DskipTests
	/// </code></pre>
	/// <li>Set up <code>${container-executor.conf.dir}/container-executor.cfg</code>
	/// container-executor.cfg needs to be owned by root and have in it the proper
	/// config values.
	/// <br /><pre><code>
	/// &gt; cat /etc/hadoop/container-executor.cfg
	/// yarn.nodemanager.linux-container-executor.group=mapred
	/// #depending on the user id of the application.submitter option
	/// min.user.id=1
	/// &gt; sudo chown root:root /etc/hadoop/container-executor.cfg
	/// &gt; sudo chmod 444 /etc/hadoop/container-executor.cfg
	/// </code></pre>
	/// <li>Move the binary and set proper permissions on it. It needs to be owned
	/// by root, the group needs to be the group configured in container-executor.cfg,
	/// and it needs the setuid bit set. (The build will also overwrite it so you
	/// need to move it to a place that you can support it.
	/// <br /><pre><code>
	/// &gt; cp ./hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/c/container-executor/container-executor /tmp/
	/// &gt; sudo chown root:mapred /tmp/container-executor
	/// &gt; sudo chmod 4550 /tmp/container-executor
	/// </code></pre>
	/// <li>Run the tests with the execution enabled (The user you run the tests as
	/// needs to be part of the group from the config.
	/// <br /><pre><code>
	/// mvn test -Dtest=TestLinuxContainerExecutor -Dapplication.submitter=nobody -Dcontainer-executor.path=/tmp/container-executor
	/// </code></pre>
	/// </ol>
	/// </remarks>
	public class TestLinuxContainerExecutor
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(TestLinuxContainerExecutor
			));

		private static FilePath workSpace = new FilePath("target", typeof(TestLinuxContainerExecutor
			).FullName + "-workSpace");

		private LinuxContainerExecutor exec = null;

		private string appSubmitter = null;

		private LocalDirsHandlerService dirsHandler;

		private Configuration conf;

		private FileContext files;

		/// <exception cref="System.Exception"/>
		[SetUp]
		public virtual void Setup()
		{
			files = FileContext.GetLocalFSFileContext();
			Path workSpacePath = new Path(workSpace.GetAbsolutePath());
			files.Mkdir(workSpacePath, null, true);
			FileUtil.Chmod(workSpace.GetAbsolutePath(), "777");
			FilePath localDir = new FilePath(workSpace.GetAbsoluteFile(), "localDir");
			files.Mkdir(new Path(localDir.GetAbsolutePath()), new FsPermission("777"), false);
			FilePath logDir = new FilePath(workSpace.GetAbsoluteFile(), "logDir");
			files.Mkdir(new Path(logDir.GetAbsolutePath()), new FsPermission("777"), false);
			string exec_path = Runtime.GetProperty("container-executor.path");
			if (exec_path != null && !exec_path.IsEmpty())
			{
				conf = new Configuration(false);
				conf.SetClass("fs.AbstractFileSystem.file.impl", typeof(LocalFs), typeof(AbstractFileSystem
					));
				conf.Set(YarnConfiguration.NmNonsecureModeLocalUserKey, "xuan");
				Log.Info("Setting " + YarnConfiguration.NmLinuxContainerExecutorPath + "=" + exec_path
					);
				conf.Set(YarnConfiguration.NmLinuxContainerExecutorPath, exec_path);
				exec = new LinuxContainerExecutor();
				exec.SetConf(conf);
				conf.Set(YarnConfiguration.NmLocalDirs, localDir.GetAbsolutePath());
				conf.Set(YarnConfiguration.NmLogDirs, logDir.GetAbsolutePath());
				dirsHandler = new LocalDirsHandlerService();
				dirsHandler.Init(conf);
			}
			appSubmitter = Runtime.GetProperty("application.submitter");
			if (appSubmitter == null || appSubmitter.IsEmpty())
			{
				appSubmitter = "nobody";
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.TearDown]
		public virtual void TearDown()
		{
			FileContext.GetLocalFSFileContext().Delete(new Path(workSpace.GetAbsolutePath()), 
				true);
		}

		private bool ShouldRun()
		{
			if (exec == null)
			{
				Log.Warn("Not running test because container-executor.path is not set");
				return false;
			}
			return true;
		}

		/// <exception cref="System.IO.IOException"/>
		private string WriteScriptFile(params string[] cmd)
		{
			FilePath f = FilePath.CreateTempFile("TestLinuxContainerExecutor", ".sh");
			f.DeleteOnExit();
			PrintWriter p = new PrintWriter(new FileOutputStream(f));
			p.WriteLine("#!/bin/sh");
			p.Write("exec");
			foreach (string part in cmd)
			{
				p.Write(" '");
				p.Write(part.Replace("\\", "\\\\").Replace("'", "\\'"));
				p.Write("'");
			}
			p.WriteLine();
			p.Close();
			return f.GetAbsolutePath();
		}

		private int id = 0;

		private int GetNextId()
		{
			lock (this)
			{
				id += 1;
				return id;
			}
		}

		private ContainerId GetNextContainerId()
		{
			ContainerId cId = Org.Mockito.Mockito.Mock<ContainerId>();
			string id = "CONTAINER_" + GetNextId();
			Org.Mockito.Mockito.When(cId.ToString()).ThenReturn(id);
			return cId;
		}

		/// <exception cref="System.IO.IOException"/>
		private int RunAndBlock(params string[] cmd)
		{
			return RunAndBlock(GetNextContainerId(), cmd);
		}

		/// <exception cref="System.IO.IOException"/>
		private int RunAndBlock(ContainerId cId, params string[] cmd)
		{
			string appId = "APP_" + GetNextId();
			Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container container
				 = Org.Mockito.Mockito.Mock<Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container
				>();
			ContainerLaunchContext context = Org.Mockito.Mockito.Mock<ContainerLaunchContext>
				();
			Dictionary<string, string> env = new Dictionary<string, string>();
			Org.Mockito.Mockito.When(container.GetContainerId()).ThenReturn(cId);
			Org.Mockito.Mockito.When(container.GetLaunchContext()).ThenReturn(context);
			Org.Mockito.Mockito.When(context.GetEnvironment()).ThenReturn(env);
			string script = WriteScriptFile(cmd);
			Path scriptPath = new Path(script);
			Path tokensPath = new Path("/dev/null");
			Path workDir = new Path(workSpace.GetAbsolutePath());
			Path pidFile = new Path(workDir, "pid.txt");
			exec.ActivateContainer(cId, pidFile);
			return exec.LaunchContainer(container, scriptPath, tokensPath, appSubmitter, appId
				, workDir, dirsHandler.GetLocalDirs(), dirsHandler.GetLogDirs());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestContainerLocalizer()
		{
			if (!ShouldRun())
			{
				return;
			}
			IList<string> localDirs = dirsHandler.GetLocalDirs();
			IList<string> logDirs = dirsHandler.GetLogDirs();
			foreach (string localDir in localDirs)
			{
				Path userDir = new Path(localDir, ContainerLocalizer.Usercache);
				files.Mkdir(userDir, new FsPermission("777"), false);
				// $local/filecache
				Path fileDir = new Path(localDir, ContainerLocalizer.Filecache);
				files.Mkdir(fileDir, new FsPermission("777"), false);
			}
			string locId = "container_01_01";
			Path nmPrivateContainerTokensPath = dirsHandler.GetLocalPathForWrite(ResourceLocalizationService
				.NmPrivateDir + Path.Separator + string.Format(ContainerLocalizer.TokenFileNameFmt
				, locId));
			files.Create(nmPrivateContainerTokensPath, EnumSet.Of(CreateFlag.Create, CreateFlag
				.Overwrite));
			Configuration config = new YarnConfiguration(conf);
			IPEndPoint nmAddr = config.GetSocketAddr(YarnConfiguration.NmBindHost, YarnConfiguration
				.NmLocalizerAddress, YarnConfiguration.DefaultNmLocalizerAddress, YarnConfiguration
				.DefaultNmLocalizerPort);
			string appId = "application_01_01";
			exec = new _LinuxContainerExecutor_258();
			exec.SetConf(conf);
			exec.StartLocalizer(nmPrivateContainerTokensPath, nmAddr, appSubmitter, appId, locId
				, dirsHandler);
			string locId2 = "container_01_02";
			Path nmPrivateContainerTokensPath2 = dirsHandler.GetLocalPathForWrite(ResourceLocalizationService
				.NmPrivateDir + Path.Separator + string.Format(ContainerLocalizer.TokenFileNameFmt
				, locId2));
			files.Create(nmPrivateContainerTokensPath2, EnumSet.Of(CreateFlag.Create, CreateFlag
				.Overwrite));
			exec.StartLocalizer(nmPrivateContainerTokensPath2, nmAddr, appSubmitter, appId, locId2
				, dirsHandler);
		}

		private sealed class _LinuxContainerExecutor_258 : LinuxContainerExecutor
		{
			public _LinuxContainerExecutor_258()
			{
			}

			public override void BuildMainArgs(IList<string> command, string user, string appId
				, string locId, IPEndPoint nmAddr, IList<string> localDirs)
			{
				MockContainerLocalizer.BuildMainArgs(command, user, appId, locId, nmAddr, localDirs
					);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestContainerLaunch()
		{
			if (!ShouldRun())
			{
				return;
			}
			FilePath touchFile = new FilePath(workSpace, "touch-file");
			int ret = RunAndBlock("touch", touchFile.GetAbsolutePath());
			NUnit.Framework.Assert.AreEqual(0, ret);
			FileStatus fileStatus = FileContext.GetLocalFSFileContext().GetFileStatus(new Path
				(touchFile.GetAbsolutePath()));
			NUnit.Framework.Assert.AreEqual(appSubmitter, fileStatus.GetOwner());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestContainerKill()
		{
			if (!ShouldRun())
			{
				return;
			}
			ContainerId sleepId = GetNextContainerId();
			Sharpen.Thread t = new _Thread_304(this, sleepId);
			t.SetDaemon(true);
			//If it does not exit we shouldn't block the test.
			t.Start();
			NUnit.Framework.Assert.IsTrue(t.IsAlive());
			string pid = null;
			int count = 10;
			while ((pid = exec.GetProcessId(sleepId)) == null && count > 0)
			{
				Log.Info("Sleeping for 200 ms before checking for pid ");
				Sharpen.Thread.Sleep(200);
				count--;
			}
			NUnit.Framework.Assert.IsNotNull(pid);
			Log.Info("Going to killing the process.");
			exec.SignalContainer(appSubmitter, pid, ContainerExecutor.Signal.Term);
			Log.Info("sleeping for 100ms to let the sleep be killed");
			Sharpen.Thread.Sleep(100);
			NUnit.Framework.Assert.IsFalse(t.IsAlive());
		}

		private sealed class _Thread_304 : Sharpen.Thread
		{
			public _Thread_304(TestLinuxContainerExecutor _enclosing, ContainerId sleepId)
			{
				this._enclosing = _enclosing;
				this.sleepId = sleepId;
			}

			public override void Run()
			{
				try
				{
					this._enclosing.RunAndBlock(sleepId, "sleep", "100");
				}
				catch (IOException e)
				{
					TestLinuxContainerExecutor.Log.Warn("Caught exception while running sleep", e);
				}
			}

			private readonly TestLinuxContainerExecutor _enclosing;

			private readonly ContainerId sleepId;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestLocalUser()
		{
			try
			{
				//nonsecure default
				Configuration conf = new YarnConfiguration();
				conf.Set(CommonConfigurationKeysPublic.HadoopSecurityAuthentication, "simple");
				UserGroupInformation.SetConfiguration(conf);
				LinuxContainerExecutor lce = new LinuxContainerExecutor();
				lce.SetConf(conf);
				NUnit.Framework.Assert.AreEqual(YarnConfiguration.DefaultNmNonsecureModeLocalUser
					, lce.GetRunAsUser("foo"));
				//nonsecure custom setting
				conf.Set(YarnConfiguration.NmNonsecureModeLocalUserKey, "bar");
				lce = new LinuxContainerExecutor();
				lce.SetConf(conf);
				NUnit.Framework.Assert.AreEqual("bar", lce.GetRunAsUser("foo"));
				//nonsecure without limits
				conf.Set(YarnConfiguration.NmNonsecureModeLocalUserKey, "bar");
				conf.Set(YarnConfiguration.NmNonsecureModeLimitUsers, "false");
				lce = new LinuxContainerExecutor();
				lce.SetConf(conf);
				NUnit.Framework.Assert.AreEqual("foo", lce.GetRunAsUser("foo"));
				//secure
				conf = new YarnConfiguration();
				conf.Set(CommonConfigurationKeysPublic.HadoopSecurityAuthentication, "kerberos");
				UserGroupInformation.SetConfiguration(conf);
				lce = new LinuxContainerExecutor();
				lce.SetConf(conf);
				NUnit.Framework.Assert.AreEqual("foo", lce.GetRunAsUser("foo"));
			}
			finally
			{
				Configuration conf = new YarnConfiguration();
				conf.Set(CommonConfigurationKeysPublic.HadoopSecurityAuthentication, "simple");
				UserGroupInformation.SetConfiguration(conf);
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNonsecureUsernamePattern()
		{
			try
			{
				//nonsecure default
				Configuration conf = new YarnConfiguration();
				conf.Set(CommonConfigurationKeysPublic.HadoopSecurityAuthentication, "simple");
				UserGroupInformation.SetConfiguration(conf);
				LinuxContainerExecutor lce = new LinuxContainerExecutor();
				lce.SetConf(conf);
				lce.VerifyUsernamePattern("foo");
				try
				{
					lce.VerifyUsernamePattern("foo/x");
					NUnit.Framework.Assert.Fail();
				}
				catch (ArgumentException)
				{
				}
				catch (Exception ex)
				{
					//NOP        
					NUnit.Framework.Assert.Fail(ex.ToString());
				}
				//nonsecure custom setting
				conf.Set(YarnConfiguration.NmNonsecureModeUserPatternKey, "foo");
				lce = new LinuxContainerExecutor();
				lce.SetConf(conf);
				lce.VerifyUsernamePattern("foo");
				try
				{
					lce.VerifyUsernamePattern("bar");
					NUnit.Framework.Assert.Fail();
				}
				catch (ArgumentException)
				{
				}
				catch (Exception ex)
				{
					//NOP        
					NUnit.Framework.Assert.Fail(ex.ToString());
				}
				//secure, pattern matching does not kick in.
				conf = new YarnConfiguration();
				conf.Set(CommonConfigurationKeysPublic.HadoopSecurityAuthentication, "kerberos");
				UserGroupInformation.SetConfiguration(conf);
				lce = new LinuxContainerExecutor();
				lce.SetConf(conf);
				lce.VerifyUsernamePattern("foo");
				lce.VerifyUsernamePattern("foo/w");
			}
			finally
			{
				Configuration conf = new YarnConfiguration();
				conf.Set(CommonConfigurationKeysPublic.HadoopSecurityAuthentication, "simple");
				UserGroupInformation.SetConfiguration(conf);
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestPostExecuteAfterReacquisition()
		{
			// make up some bogus container ID
			ApplicationId appId = ApplicationId.NewInstance(12345, 67890);
			ApplicationAttemptId attemptId = ApplicationAttemptId.NewInstance(appId, 54321);
			ContainerId cid = ContainerId.NewContainerId(attemptId, 9876);
			Configuration conf = new YarnConfiguration();
			conf.SetClass(YarnConfiguration.NmLinuxContainerResourcesHandler, typeof(TestLinuxContainerExecutor.TestResourceHandler
				), typeof(LCEResourcesHandler));
			LinuxContainerExecutor lce = new LinuxContainerExecutor();
			lce.SetConf(conf);
			try
			{
				lce.Init();
			}
			catch (IOException)
			{
			}
			// expected if LCE isn't setup right, but not necessary for this test
			lce.ReacquireContainer("foouser", cid);
			NUnit.Framework.Assert.IsTrue("postExec not called after reacquisition", TestLinuxContainerExecutor.TestResourceHandler
				.postExecContainers.Contains(cid));
		}

		private class TestResourceHandler : LCEResourcesHandler
		{
			internal static ICollection<ContainerId> postExecContainers = new HashSet<ContainerId
				>();

			public virtual void SetConf(Configuration conf)
			{
			}

			public virtual Configuration GetConf()
			{
				return null;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Init(LinuxContainerExecutor lce)
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void PreExecute(ContainerId containerId, Resource containerResource
				)
			{
			}

			public virtual void PostExecute(ContainerId containerId)
			{
				postExecContainers.AddItem(containerId);
			}

			public virtual string GetResourcesOption(ContainerId containerId)
			{
				return null;
			}
		}
	}
}
