using System.Collections.Generic;
using System.IO;
using NUnit.Framework;
using NUnit.Framework.Matchers;
using Org.Apache.Commons.Codec.Binary;
using Org.Apache.Commons.Lang;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn.Api;
using Org.Apache.Hadoop.Yarn.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Event;
using Org.Apache.Hadoop.Yarn.Security;
using Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Localizer;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Recovery;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Security;
using Org.Apache.Hadoop.Yarn.Server.Security;
using Org.Apache.Hadoop.Yarn.Server.Utils;
using Org.Apache.Hadoop.Yarn.Util;
using Sharpen;
using Sharpen.Jar;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Launcher
{
	public class TestContainerLaunch : BaseContainerManagerTest
	{
		private sealed class _NMContext_112 : NodeManager.NMContext
		{
			public _NMContext_112(NMContainerTokenSecretManager baseArg1, NMTokenSecretManagerInNM
				 baseArg2, LocalDirsHandlerService baseArg3, ApplicationACLsManager baseArg4, NMStateStoreService
				 baseArg5)
				: base(baseArg1, baseArg2, baseArg3, baseArg4, baseArg5)
			{
			}

			public override int GetHttpPort()
			{
				return BaseContainerManagerTest.HttpPort;
			}

			public override NodeId GetNodeId()
			{
				return NodeId.NewInstance("ahost", 1234);
			}
		}

		protected internal Context distContext = new _NMContext_112(new NMContainerTokenSecretManager
			(conf), new NMTokenSecretManagerInNM(), null, new ApplicationACLsManager(conf), 
			new NMNullStateStoreService());

		/// <exception cref="Org.Apache.Hadoop.FS.UnsupportedFileSystemException"/>
		public TestContainerLaunch()
			: base()
		{
		}

		/// <exception cref="System.IO.IOException"/>
		[SetUp]
		public override void Setup()
		{
			conf.SetClass(YarnConfiguration.NmContainerMonResourceCalculator, typeof(LinuxResourceCalculatorPlugin
				), typeof(ResourceCalculatorPlugin));
			base.Setup();
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestSpecialCharSymlinks()
		{
			FilePath shellFile = null;
			FilePath tempFile = null;
			string badSymlink = Shell.Windows ? "foo@zz_#!-+bar.cmd" : "foo@zz%_#*&!-+= bar()";
			FilePath symLinkFile = null;
			try
			{
				shellFile = Shell.AppendScriptExtension(tmpDir, "hello");
				tempFile = Shell.AppendScriptExtension(tmpDir, "temp");
				string timeoutCommand = Shell.Windows ? "@echo \"hello\"" : "echo \"hello\"";
				PrintWriter writer = new PrintWriter(new FileOutputStream(shellFile));
				FileUtil.SetExecutable(shellFile, true);
				writer.WriteLine(timeoutCommand);
				writer.Close();
				IDictionary<Path, IList<string>> resources = new Dictionary<Path, IList<string>>(
					);
				Path path = new Path(shellFile.GetAbsolutePath());
				resources[path] = Arrays.AsList(badSymlink);
				FileOutputStream fos = new FileOutputStream(tempFile);
				IDictionary<string, string> env = new Dictionary<string, string>();
				IList<string> commands = new AList<string>();
				if (Shell.Windows)
				{
					commands.AddItem("cmd");
					commands.AddItem("/c");
					commands.AddItem("\"" + badSymlink + "\"");
				}
				else
				{
					commands.AddItem("/bin/sh ./\\\"" + badSymlink + "\\\"");
				}
				new DefaultContainerExecutor().WriteLaunchEnv(fos, env, resources, commands);
				fos.Flush();
				fos.Close();
				FileUtil.SetExecutable(tempFile, true);
				Shell.ShellCommandExecutor shexc = new Shell.ShellCommandExecutor(new string[] { 
					tempFile.GetAbsolutePath() }, tmpDir);
				shexc.Execute();
				NUnit.Framework.Assert.AreEqual(shexc.GetExitCode(), 0);
				System.Diagnostics.Debug.Assert((shexc.GetOutput().Contains("hello")));
				symLinkFile = new FilePath(tmpDir, badSymlink);
			}
			finally
			{
				// cleanup
				if (shellFile != null && shellFile.Exists())
				{
					shellFile.Delete();
				}
				if (tempFile != null && tempFile.Exists())
				{
					tempFile.Delete();
				}
				if (symLinkFile != null && symLinkFile.Exists())
				{
					symLinkFile.Delete();
				}
			}
		}

		// test the diagnostics are generated
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestInvalidSymlinkDiagnostics()
		{
			FilePath shellFile = null;
			FilePath tempFile = null;
			string symLink = Shell.Windows ? "test.cmd" : "test";
			FilePath symLinkFile = null;
			try
			{
				shellFile = Shell.AppendScriptExtension(tmpDir, "hello");
				tempFile = Shell.AppendScriptExtension(tmpDir, "temp");
				string timeoutCommand = Shell.Windows ? "@echo \"hello\"" : "echo \"hello\"";
				PrintWriter writer = new PrintWriter(new FileOutputStream(shellFile));
				FileUtil.SetExecutable(shellFile, true);
				writer.WriteLine(timeoutCommand);
				writer.Close();
				IDictionary<Path, IList<string>> resources = new Dictionary<Path, IList<string>>(
					);
				//This is an invalid path and should throw exception because of No such file.
				Path invalidPath = new Path(shellFile.GetAbsolutePath() + "randomPath");
				resources[invalidPath] = Arrays.AsList(symLink);
				FileOutputStream fos = new FileOutputStream(tempFile);
				IDictionary<string, string> env = new Dictionary<string, string>();
				IList<string> commands = new AList<string>();
				if (Shell.Windows)
				{
					commands.AddItem("cmd");
					commands.AddItem("/c");
					commands.AddItem("\"" + symLink + "\"");
				}
				else
				{
					commands.AddItem("/bin/sh ./\\\"" + symLink + "\\\"");
				}
				new DefaultContainerExecutor().WriteLaunchEnv(fos, env, resources, commands);
				fos.Flush();
				fos.Close();
				FileUtil.SetExecutable(tempFile, true);
				Shell.ShellCommandExecutor shexc = new Shell.ShellCommandExecutor(new string[] { 
					tempFile.GetAbsolutePath() }, tmpDir);
				string diagnostics = null;
				try
				{
					shexc.Execute();
					NUnit.Framework.Assert.Fail("Should catch exception");
				}
				catch (Shell.ExitCodeException e)
				{
					diagnostics = e.Message;
				}
				NUnit.Framework.Assert.IsNotNull(diagnostics);
				NUnit.Framework.Assert.IsTrue(shexc.GetExitCode() != 0);
				symLinkFile = new FilePath(tmpDir, symLink);
			}
			finally
			{
				// cleanup
				if (shellFile != null && shellFile.Exists())
				{
					shellFile.Delete();
				}
				if (tempFile != null && tempFile.Exists())
				{
					tempFile.Delete();
				}
				if (symLinkFile != null && symLinkFile.Exists())
				{
					symLinkFile.Delete();
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestInvalidEnvSyntaxDiagnostics()
		{
			FilePath shellFile = null;
			try
			{
				shellFile = Shell.AppendScriptExtension(tmpDir, "hello");
				IDictionary<Path, IList<string>> resources = new Dictionary<Path, IList<string>>(
					);
				FileOutputStream fos = new FileOutputStream(shellFile);
				FileUtil.SetExecutable(shellFile, true);
				IDictionary<string, string> env = new Dictionary<string, string>();
				// invalid env
				env["APPLICATION_WORKFLOW_CONTEXT"] = "{\"workflowId\":\"609f91c5cd83\"," + "\"workflowName\":\"\n\ninsert table "
					 + "\npartition (cd_education_status)\nselect cd_demo_sk, cd_gender, ";
				IList<string> commands = new AList<string>();
				new DefaultContainerExecutor().WriteLaunchEnv(fos, env, resources, commands);
				fos.Flush();
				fos.Close();
				// It is supposed that LANG is set as C.
				IDictionary<string, string> cmdEnv = new Dictionary<string, string>();
				cmdEnv["LANG"] = "C";
				Shell.ShellCommandExecutor shexc = new Shell.ShellCommandExecutor(new string[] { 
					shellFile.GetAbsolutePath() }, tmpDir, cmdEnv);
				string diagnostics = null;
				try
				{
					shexc.Execute();
					NUnit.Framework.Assert.Fail("Should catch exception");
				}
				catch (Shell.ExitCodeException e)
				{
					diagnostics = e.Message;
				}
				NUnit.Framework.Assert.IsTrue(diagnostics.Contains(Shell.Windows ? "is not recognized as an internal or external command"
					 : "command not found"));
				NUnit.Framework.Assert.IsTrue(shexc.GetExitCode() != 0);
			}
			finally
			{
				// cleanup
				if (shellFile != null && shellFile.Exists())
				{
					shellFile.Delete();
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestEnvExpansion()
		{
			Path logPath = new Path("/nm/container/logs");
			string input = Apps.CrossPlatformify("HADOOP_HOME") + "/share/hadoop/common/*" + 
				ApplicationConstants.ClassPathSeparator + Apps.CrossPlatformify("HADOOP_HOME") +
				 "/share/hadoop/common/lib/*" + ApplicationConstants.ClassPathSeparator + Apps.CrossPlatformify
				("HADOOP_LOG_HOME") + ApplicationConstants.LogDirExpansionVar;
			string res = ContainerLaunch.ExpandEnvironment(input, logPath);
			if (Shell.Windows)
			{
				NUnit.Framework.Assert.AreEqual("%HADOOP_HOME%/share/hadoop/common/*;" + "%HADOOP_HOME%/share/hadoop/common/lib/*;"
					 + "%HADOOP_LOG_HOME%/nm/container/logs", res);
			}
			else
			{
				NUnit.Framework.Assert.AreEqual("$HADOOP_HOME/share/hadoop/common/*:" + "$HADOOP_HOME/share/hadoop/common/lib/*:"
					 + "$HADOOP_LOG_HOME/nm/container/logs", res);
			}
			System.Console.Out.WriteLine(res);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestContainerLaunchStdoutAndStderrDiagnostics()
		{
			FilePath shellFile = null;
			try
			{
				shellFile = Shell.AppendScriptExtension(tmpDir, "hello");
				// echo "hello" to stdout and "error" to stderr and exit code with 2;
				string command = Shell.Windows ? "@echo \"hello\" & @echo \"error\" 1>&2 & exit /b 2"
					 : "echo \"hello\"; echo \"error\" 1>&2; exit 2;";
				PrintWriter writer = new PrintWriter(new FileOutputStream(shellFile));
				FileUtil.SetExecutable(shellFile, true);
				writer.WriteLine(command);
				writer.Close();
				IDictionary<Path, IList<string>> resources = new Dictionary<Path, IList<string>>(
					);
				FileOutputStream fos = new FileOutputStream(shellFile, true);
				IDictionary<string, string> env = new Dictionary<string, string>();
				IList<string> commands = new AList<string>();
				commands.AddItem(command);
				ContainerExecutor exec = new DefaultContainerExecutor();
				exec.WriteLaunchEnv(fos, env, resources, commands);
				fos.Flush();
				fos.Close();
				Shell.ShellCommandExecutor shexc = new Shell.ShellCommandExecutor(new string[] { 
					shellFile.GetAbsolutePath() }, tmpDir);
				string diagnostics = null;
				try
				{
					shexc.Execute();
					NUnit.Framework.Assert.Fail("Should catch exception");
				}
				catch (Shell.ExitCodeException e)
				{
					diagnostics = e.Message;
				}
				// test stderr
				NUnit.Framework.Assert.IsTrue(diagnostics.Contains("error"));
				// test stdout
				NUnit.Framework.Assert.IsTrue(shexc.GetOutput().Contains("hello"));
				NUnit.Framework.Assert.IsTrue(shexc.GetExitCode() == 2);
			}
			finally
			{
				// cleanup
				if (shellFile != null && shellFile.Exists())
				{
					shellFile.Delete();
				}
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestPrependDistcache()
		{
			// Test is only relevant on Windows
			Assume.AssumeTrue(Shell.Windows);
			ContainerLaunchContext containerLaunchContext = recordFactory.NewRecordInstance<ContainerLaunchContext
				>();
			ApplicationId appId = ApplicationId.NewInstance(0, 0);
			ApplicationAttemptId appAttemptId = ApplicationAttemptId.NewInstance(appId, 1);
			ContainerId cId = ContainerId.NewContainerId(appAttemptId, 0);
			IDictionary<string, string> userSetEnv = new Dictionary<string, string>();
			userSetEnv[ApplicationConstants.Environment.ContainerId.ToString()] = "user_set_container_id";
			userSetEnv[ApplicationConstants.Environment.NmHost.ToString()] = "user_set_NM_HOST";
			userSetEnv[ApplicationConstants.Environment.NmPort.ToString()] = "user_set_NM_PORT";
			userSetEnv[ApplicationConstants.Environment.NmHttpPort.ToString()] = "user_set_NM_HTTP_PORT";
			userSetEnv[ApplicationConstants.Environment.LocalDirs.ToString()] = "user_set_LOCAL_DIR";
			userSetEnv[ApplicationConstants.Environment.User.Key()] = "user_set_" + ApplicationConstants.Environment
				.User.Key();
			userSetEnv[ApplicationConstants.Environment.Logname.ToString()] = "user_set_LOGNAME";
			userSetEnv[ApplicationConstants.Environment.Pwd.ToString()] = "user_set_PWD";
			userSetEnv[ApplicationConstants.Environment.Home.ToString()] = "user_set_HOME";
			userSetEnv[ApplicationConstants.Environment.Classpath.ToString()] = "APATH";
			containerLaunchContext.SetEnvironment(userSetEnv);
			Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container container
				 = Org.Mockito.Mockito.Mock<Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container
				>();
			Org.Mockito.Mockito.When(container.GetContainerId()).ThenReturn(cId);
			Org.Mockito.Mockito.When(container.GetLaunchContext()).ThenReturn(containerLaunchContext
				);
			Org.Mockito.Mockito.When(container.GetLocalizedResources()).ThenReturn(null);
			Dispatcher dispatcher = Org.Mockito.Mockito.Mock<Dispatcher>();
			EventHandler eventHandler = new _EventHandler_427();
			Org.Mockito.Mockito.When(dispatcher.GetEventHandler()).ThenReturn(eventHandler);
			Configuration conf = new Configuration();
			ContainerLaunch launch = new ContainerLaunch(distContext, conf, dispatcher, exec, 
				null, container, dirsHandler, containerManager);
			string testDir = Runtime.GetProperty("test.build.data", "target/test-dir");
			Path pwd = new Path(testDir);
			IList<Path> appDirs = new AList<Path>();
			IList<string> containerLogs = new AList<string>();
			IDictionary<Path, IList<string>> resources = new Dictionary<Path, IList<string>>(
				);
			Path userjar = new Path("user.jar");
			IList<string> lpaths = new AList<string>();
			lpaths.AddItem("userjarlink.jar");
			resources[userjar] = lpaths;
			Path nmp = new Path(testDir);
			launch.SanitizeEnv(userSetEnv, pwd, appDirs, containerLogs, resources, nmp);
			IList<string> result = GetJarManifestClasspath(userSetEnv[ApplicationConstants.Environment
				.Classpath.ToString()]);
			NUnit.Framework.Assert.IsTrue(result.Count > 1);
			NUnit.Framework.Assert.IsTrue(result[result.Count - 1].EndsWith("userjarlink.jar"
				));
			//Then, with user classpath first
			userSetEnv[ApplicationConstants.Environment.ClasspathPrependDistcache.ToString()]
				 = "true";
			cId = ContainerId.NewContainerId(appAttemptId, 1);
			Org.Mockito.Mockito.When(container.GetContainerId()).ThenReturn(cId);
			launch = new ContainerLaunch(distContext, conf, dispatcher, exec, null, container
				, dirsHandler, containerManager);
			launch.SanitizeEnv(userSetEnv, pwd, appDirs, containerLogs, resources, nmp);
			result = GetJarManifestClasspath(userSetEnv[ApplicationConstants.Environment.Classpath
				.ToString()]);
			NUnit.Framework.Assert.IsTrue(result.Count > 1);
			NUnit.Framework.Assert.IsTrue(result[0].EndsWith("userjarlink.jar"));
		}

		private sealed class _EventHandler_427 : EventHandler
		{
			public _EventHandler_427()
			{
			}

			public void Handle(Org.Apache.Hadoop.Yarn.Event.Event @event)
			{
				NUnit.Framework.Assert.IsTrue(@event is ContainerExitEvent);
				ContainerExitEvent exitEvent = (ContainerExitEvent)@event;
				NUnit.Framework.Assert.AreEqual(ContainerEventType.ContainerExitedWithFailure, exitEvent
					.GetType());
			}
		}

		/// <exception cref="System.Exception"/>
		private static IList<string> GetJarManifestClasspath(string path)
		{
			IList<string> classpath = new AList<string>();
			JarFile jarFile = new JarFile(path);
			Manifest manifest = jarFile.GetManifest();
			string cps = manifest.GetMainAttributes().GetValue("Class-Path");
			StringTokenizer cptok = new StringTokenizer(cps);
			while (cptok.HasMoreTokens())
			{
				string cpentry = cptok.NextToken();
				classpath.AddItem(cpentry);
			}
			return classpath;
		}

		/// <summary>See if environment variable is forwarded using sanitizeEnv.</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestContainerEnvVariables()
		{
			containerManager.Start();
			ContainerLaunchContext containerLaunchContext = recordFactory.NewRecordInstance<ContainerLaunchContext
				>();
			// ////// Construct the Container-id
			ApplicationId appId = ApplicationId.NewInstance(0, 0);
			ApplicationAttemptId appAttemptId = ApplicationAttemptId.NewInstance(appId, 1);
			ContainerId cId = ContainerId.NewContainerId(appAttemptId, 0);
			IDictionary<string, string> userSetEnv = new Dictionary<string, string>();
			userSetEnv[ApplicationConstants.Environment.ContainerId.ToString()] = "user_set_container_id";
			userSetEnv[ApplicationConstants.Environment.NmHost.ToString()] = "user_set_NM_HOST";
			userSetEnv[ApplicationConstants.Environment.NmPort.ToString()] = "user_set_NM_PORT";
			userSetEnv[ApplicationConstants.Environment.NmHttpPort.ToString()] = "user_set_NM_HTTP_PORT";
			userSetEnv[ApplicationConstants.Environment.LocalDirs.ToString()] = "user_set_LOCAL_DIR";
			userSetEnv[ApplicationConstants.Environment.User.Key()] = "user_set_" + ApplicationConstants.Environment
				.User.Key();
			userSetEnv[ApplicationConstants.Environment.Logname.ToString()] = "user_set_LOGNAME";
			userSetEnv[ApplicationConstants.Environment.Pwd.ToString()] = "user_set_PWD";
			userSetEnv[ApplicationConstants.Environment.Home.ToString()] = "user_set_HOME";
			containerLaunchContext.SetEnvironment(userSetEnv);
			FilePath scriptFile = Shell.AppendScriptExtension(tmpDir, "scriptFile");
			PrintWriter fileWriter = new PrintWriter(scriptFile);
			FilePath processStartFile = new FilePath(tmpDir, "env_vars.txt").GetAbsoluteFile(
				);
			if (Shell.Windows)
			{
				fileWriter.WriteLine("@echo " + ApplicationConstants.Environment.ContainerId.$() 
					+ "> " + processStartFile);
				fileWriter.WriteLine("@echo " + ApplicationConstants.Environment.NmHost.$() + ">> "
					 + processStartFile);
				fileWriter.WriteLine("@echo " + ApplicationConstants.Environment.NmPort.$() + ">> "
					 + processStartFile);
				fileWriter.WriteLine("@echo " + ApplicationConstants.Environment.NmHttpPort.$() +
					 ">> " + processStartFile);
				fileWriter.WriteLine("@echo " + ApplicationConstants.Environment.LocalDirs.$() + 
					">> " + processStartFile);
				fileWriter.WriteLine("@echo " + ApplicationConstants.Environment.User.$() + ">> "
					 + processStartFile);
				fileWriter.WriteLine("@echo " + ApplicationConstants.Environment.Logname.$() + ">> "
					 + processStartFile);
				fileWriter.WriteLine("@echo " + ApplicationConstants.Environment.Pwd.$() + ">> " 
					+ processStartFile);
				fileWriter.WriteLine("@echo " + ApplicationConstants.Environment.Home.$() + ">> "
					 + processStartFile);
				foreach (string serviceName in containerManager.GetAuxServiceMetaData().Keys)
				{
					fileWriter.WriteLine("@echo %" + AuxiliaryServiceHelper.NmAuxService + serviceName
						 + "%>> " + processStartFile);
				}
				fileWriter.WriteLine("@echo " + cId + ">> " + processStartFile);
				fileWriter.WriteLine("@ping -n 100 127.0.0.1 >nul");
			}
			else
			{
				fileWriter.Write("\numask 0");
				// So that start file is readable by the test
				fileWriter.Write("\necho $" + ApplicationConstants.Environment.ContainerId.ToString
					() + " > " + processStartFile);
				fileWriter.Write("\necho $" + ApplicationConstants.Environment.NmHost.ToString() 
					+ " >> " + processStartFile);
				fileWriter.Write("\necho $" + ApplicationConstants.Environment.NmPort.ToString() 
					+ " >> " + processStartFile);
				fileWriter.Write("\necho $" + ApplicationConstants.Environment.NmHttpPort.ToString
					() + " >> " + processStartFile);
				fileWriter.Write("\necho $" + ApplicationConstants.Environment.LocalDirs.ToString
					() + " >> " + processStartFile);
				fileWriter.Write("\necho $" + ApplicationConstants.Environment.User.ToString() + 
					" >> " + processStartFile);
				fileWriter.Write("\necho $" + ApplicationConstants.Environment.Logname.ToString()
					 + " >> " + processStartFile);
				fileWriter.Write("\necho $" + ApplicationConstants.Environment.Pwd.ToString() + " >> "
					 + processStartFile);
				fileWriter.Write("\necho $" + ApplicationConstants.Environment.Home.ToString() + 
					" >> " + processStartFile);
				foreach (string serviceName in containerManager.GetAuxServiceMetaData().Keys)
				{
					fileWriter.Write("\necho $" + AuxiliaryServiceHelper.NmAuxService + serviceName +
						 " >> " + processStartFile);
				}
				fileWriter.Write("\necho $$ >> " + processStartFile);
				fileWriter.Write("\nexec sleep 100");
			}
			fileWriter.Close();
			// upload the script file so that the container can run it
			URL resource_alpha = ConverterUtils.GetYarnUrlFromPath(localFS.MakeQualified(new 
				Path(scriptFile.GetAbsolutePath())));
			LocalResource rsrc_alpha = recordFactory.NewRecordInstance<LocalResource>();
			rsrc_alpha.SetResource(resource_alpha);
			rsrc_alpha.SetSize(-1);
			rsrc_alpha.SetVisibility(LocalResourceVisibility.Application);
			rsrc_alpha.SetType(LocalResourceType.File);
			rsrc_alpha.SetTimestamp(scriptFile.LastModified());
			string destinationFile = "dest_file";
			IDictionary<string, LocalResource> localResources = new Dictionary<string, LocalResource
				>();
			localResources[destinationFile] = rsrc_alpha;
			containerLaunchContext.SetLocalResources(localResources);
			// set up the rest of the container
			IList<string> commands = Arrays.AsList(Shell.GetRunScriptCommand(scriptFile));
			containerLaunchContext.SetCommands(commands);
			StartContainerRequest scRequest = StartContainerRequest.NewInstance(containerLaunchContext
				, CreateContainerToken(cId, Priority.NewInstance(0), 0));
			IList<StartContainerRequest> list = new AList<StartContainerRequest>();
			list.AddItem(scRequest);
			StartContainersRequest allRequests = StartContainersRequest.NewInstance(list);
			containerManager.StartContainers(allRequests);
			int timeoutSecs = 0;
			while (!processStartFile.Exists() && timeoutSecs++ < 20)
			{
				Sharpen.Thread.Sleep(1000);
				Log.Info("Waiting for process start-file to be created");
			}
			NUnit.Framework.Assert.IsTrue("ProcessStartFile doesn't exist!", processStartFile
				.Exists());
			// Now verify the contents of the file
			IList<string> localDirs = dirsHandler.GetLocalDirs();
			IList<string> logDirs = dirsHandler.GetLogDirs();
			IList<Path> appDirs = new AList<Path>(localDirs.Count);
			foreach (string localDir in localDirs)
			{
				Path usersdir = new Path(localDir, ContainerLocalizer.Usercache);
				Path userdir = new Path(usersdir, user);
				Path appsdir = new Path(userdir, ContainerLocalizer.Appcache);
				appDirs.AddItem(new Path(appsdir, appId.ToString()));
			}
			IList<string> containerLogDirs = new AList<string>();
			string relativeContainerLogDir = ContainerLaunch.GetRelativeContainerLogDir(appId
				.ToString(), cId.ToString());
			foreach (string logDir in logDirs)
			{
				containerLogDirs.AddItem(logDir + Path.Separator + relativeContainerLogDir);
			}
			BufferedReader reader = new BufferedReader(new FileReader(processStartFile));
			NUnit.Framework.Assert.AreEqual(cId.ToString(), reader.ReadLine());
			NUnit.Framework.Assert.AreEqual(context.GetNodeId().GetHost(), reader.ReadLine());
			NUnit.Framework.Assert.AreEqual(context.GetNodeId().GetPort().ToString(), reader.
				ReadLine());
			NUnit.Framework.Assert.AreEqual(HttpPort.ToString(), reader.ReadLine());
			NUnit.Framework.Assert.AreEqual(StringUtils.Join(",", appDirs), reader.ReadLine()
				);
			NUnit.Framework.Assert.AreEqual(user, reader.ReadLine());
			NUnit.Framework.Assert.AreEqual(user, reader.ReadLine());
			string obtainedPWD = reader.ReadLine();
			bool found = false;
			foreach (Path localDir_1 in appDirs)
			{
				if (new Path(localDir_1, cId.ToString()).ToString().Equals(obtainedPWD))
				{
					found = true;
					break;
				}
			}
			NUnit.Framework.Assert.IsTrue("Wrong local-dir found : " + obtainedPWD, found);
			NUnit.Framework.Assert.AreEqual(conf.Get(YarnConfiguration.NmUserHomeDir, YarnConfiguration
				.DefaultNmUserHomeDir), reader.ReadLine());
			foreach (string serviceName_1 in containerManager.GetAuxServiceMetaData().Keys)
			{
				NUnit.Framework.Assert.AreEqual(containerManager.GetAuxServiceMetaData()[serviceName_1
					], ByteBuffer.Wrap(Base64.DecodeBase64(Sharpen.Runtime.GetBytesForString(reader.
					ReadLine()))));
			}
			NUnit.Framework.Assert.AreEqual(cId.ToString(), containerLaunchContext.GetEnvironment
				()[ApplicationConstants.Environment.ContainerId.ToString()]);
			NUnit.Framework.Assert.AreEqual(context.GetNodeId().GetHost(), containerLaunchContext
				.GetEnvironment()[ApplicationConstants.Environment.NmHost.ToString()]);
			NUnit.Framework.Assert.AreEqual(context.GetNodeId().GetPort().ToString(), containerLaunchContext
				.GetEnvironment()[ApplicationConstants.Environment.NmPort.ToString()]);
			NUnit.Framework.Assert.AreEqual(HttpPort.ToString(), containerLaunchContext.GetEnvironment
				()[ApplicationConstants.Environment.NmHttpPort.ToString()]);
			NUnit.Framework.Assert.AreEqual(StringUtils.Join(",", appDirs), containerLaunchContext
				.GetEnvironment()[ApplicationConstants.Environment.LocalDirs.ToString()]);
			NUnit.Framework.Assert.AreEqual(StringUtils.Join(",", containerLogDirs), containerLaunchContext
				.GetEnvironment()[ApplicationConstants.Environment.LogDirs.ToString()]);
			NUnit.Framework.Assert.AreEqual(user, containerLaunchContext.GetEnvironment()[ApplicationConstants.Environment
				.User.ToString()]);
			NUnit.Framework.Assert.AreEqual(user, containerLaunchContext.GetEnvironment()[ApplicationConstants.Environment
				.Logname.ToString()]);
			found = false;
			obtainedPWD = containerLaunchContext.GetEnvironment()[ApplicationConstants.Environment
				.Pwd.ToString()];
			foreach (Path localDir_2 in appDirs)
			{
				if (new Path(localDir_2, cId.ToString()).ToString().Equals(obtainedPWD))
				{
					found = true;
					break;
				}
			}
			NUnit.Framework.Assert.IsTrue("Wrong local-dir found : " + obtainedPWD, found);
			NUnit.Framework.Assert.AreEqual(conf.Get(YarnConfiguration.NmUserHomeDir, YarnConfiguration
				.DefaultNmUserHomeDir), containerLaunchContext.GetEnvironment()[ApplicationConstants.Environment
				.Home.ToString()]);
			// Get the pid of the process
			string pid = reader.ReadLine().Trim();
			// No more lines
			NUnit.Framework.Assert.AreEqual(null, reader.ReadLine());
			// Now test the stop functionality.
			// Assert that the process is alive
			NUnit.Framework.Assert.IsTrue("Process is not alive!", DefaultContainerExecutor.ContainerIsAlive
				(pid));
			// Once more
			NUnit.Framework.Assert.IsTrue("Process is not alive!", DefaultContainerExecutor.ContainerIsAlive
				(pid));
			// Now test the stop functionality.
			IList<ContainerId> containerIds = new AList<ContainerId>();
			containerIds.AddItem(cId);
			StopContainersRequest stopRequest = StopContainersRequest.NewInstance(containerIds
				);
			containerManager.StopContainers(stopRequest);
			BaseContainerManagerTest.WaitForContainerState(containerManager, cId, ContainerState
				.Complete);
			GetContainerStatusesRequest gcsRequest = GetContainerStatusesRequest.NewInstance(
				containerIds);
			ContainerStatus containerStatus = containerManager.GetContainerStatuses(gcsRequest
				).GetContainerStatuses()[0];
			int expectedExitCode = ContainerExitStatus.KilledByAppmaster;
			NUnit.Framework.Assert.AreEqual(expectedExitCode, containerStatus.GetExitStatus()
				);
			// Assert that the process is not alive anymore
			NUnit.Framework.Assert.IsFalse("Process is still alive!", DefaultContainerExecutor
				.ContainerIsAlive(pid));
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestAuxiliaryServiceHelper()
		{
			IDictionary<string, string> env = new Dictionary<string, string>();
			string serviceName = "testAuxiliaryService";
			ByteBuffer bb = ByteBuffer.Wrap(Sharpen.Runtime.GetBytesForString("testAuxiliaryService"
				));
			AuxiliaryServiceHelper.SetServiceDataIntoEnv(serviceName, bb, env);
			NUnit.Framework.Assert.AreEqual(bb, AuxiliaryServiceHelper.GetServiceDataFromEnv(
				serviceName, env));
		}

		/// <exception cref="System.Exception"/>
		private void InternalKillTest(bool delayed)
		{
			conf.SetLong(YarnConfiguration.NmSleepDelayBeforeSigkillMs, delayed ? 1000 : 0);
			containerManager.Start();
			// ////// Construct the Container-id
			ApplicationId appId = ApplicationId.NewInstance(1, 1);
			ApplicationAttemptId appAttemptId = ApplicationAttemptId.NewInstance(appId, 1);
			ContainerId cId = ContainerId.NewContainerId(appAttemptId, 0);
			FilePath processStartFile = new FilePath(tmpDir, "pid.txt").GetAbsoluteFile();
			// setup a script that can handle sigterm gracefully
			FilePath scriptFile = Shell.AppendScriptExtension(tmpDir, "testscript");
			PrintWriter writer = new PrintWriter(new FileOutputStream(scriptFile));
			if (Shell.Windows)
			{
				writer.WriteLine("@echo \"Running testscript for delayed kill\"");
				writer.WriteLine("@echo \"Writing pid to start file\"");
				writer.WriteLine("@echo " + cId + "> " + processStartFile);
				writer.WriteLine("@ping -n 100 127.0.0.1 >nul");
			}
			else
			{
				writer.WriteLine("#!/bin/bash\n\n");
				writer.WriteLine("echo \"Running testscript for delayed kill\"");
				writer.WriteLine("hello=\"Got SIGTERM\"");
				writer.WriteLine("umask 0");
				writer.WriteLine("trap \"echo $hello >> " + processStartFile + "\" SIGTERM");
				writer.WriteLine("echo \"Writing pid to start file\"");
				writer.WriteLine("echo $$ >> " + processStartFile);
				writer.WriteLine("while true; do\nsleep 1s;\ndone");
			}
			writer.Close();
			FileUtil.SetExecutable(scriptFile, true);
			ContainerLaunchContext containerLaunchContext = recordFactory.NewRecordInstance<ContainerLaunchContext
				>();
			// upload the script file so that the container can run it
			URL resource_alpha = ConverterUtils.GetYarnUrlFromPath(localFS.MakeQualified(new 
				Path(scriptFile.GetAbsolutePath())));
			LocalResource rsrc_alpha = recordFactory.NewRecordInstance<LocalResource>();
			rsrc_alpha.SetResource(resource_alpha);
			rsrc_alpha.SetSize(-1);
			rsrc_alpha.SetVisibility(LocalResourceVisibility.Application);
			rsrc_alpha.SetType(LocalResourceType.File);
			rsrc_alpha.SetTimestamp(scriptFile.LastModified());
			string destinationFile = "dest_file.sh";
			IDictionary<string, LocalResource> localResources = new Dictionary<string, LocalResource
				>();
			localResources[destinationFile] = rsrc_alpha;
			containerLaunchContext.SetLocalResources(localResources);
			// set up the rest of the container
			IList<string> commands = Arrays.AsList(Shell.GetRunScriptCommand(scriptFile));
			containerLaunchContext.SetCommands(commands);
			Priority priority = Priority.NewInstance(10);
			long createTime = 1234;
			Token containerToken = CreateContainerToken(cId, priority, createTime);
			StartContainerRequest scRequest = StartContainerRequest.NewInstance(containerLaunchContext
				, containerToken);
			IList<StartContainerRequest> list = new AList<StartContainerRequest>();
			list.AddItem(scRequest);
			StartContainersRequest allRequests = StartContainersRequest.NewInstance(list);
			containerManager.StartContainers(allRequests);
			int timeoutSecs = 0;
			while (!processStartFile.Exists() && timeoutSecs++ < 20)
			{
				Sharpen.Thread.Sleep(1000);
				Log.Info("Waiting for process start-file to be created");
			}
			NUnit.Framework.Assert.IsTrue("ProcessStartFile doesn't exist!", processStartFile
				.Exists());
			NMContainerStatus nmContainerStatus = containerManager.GetContext().GetContainers
				()[cId].GetNMContainerStatus();
			NUnit.Framework.Assert.AreEqual(priority, nmContainerStatus.GetPriority());
			// Now test the stop functionality.
			IList<ContainerId> containerIds = new AList<ContainerId>();
			containerIds.AddItem(cId);
			StopContainersRequest stopRequest = StopContainersRequest.NewInstance(containerIds
				);
			containerManager.StopContainers(stopRequest);
			BaseContainerManagerTest.WaitForContainerState(containerManager, cId, ContainerState
				.Complete);
			// if delayed container stop sends a sigterm followed by a sigkill
			// otherwise sigkill is sent immediately 
			GetContainerStatusesRequest gcsRequest = GetContainerStatusesRequest.NewInstance(
				containerIds);
			ContainerStatus containerStatus = containerManager.GetContainerStatuses(gcsRequest
				).GetContainerStatuses()[0];
			NUnit.Framework.Assert.AreEqual(ContainerExitStatus.KilledByAppmaster, containerStatus
				.GetExitStatus());
			// Now verify the contents of the file.  Script generates a message when it
			// receives a sigterm so we look for that.  We cannot perform this check on
			// Windows, because the process is not notified when killed by winutils.
			// There is no way for the process to trap and respond.  Instead, we can
			// verify that the job object with ID matching container ID no longer exists.
			if (Shell.Windows || !delayed)
			{
				NUnit.Framework.Assert.IsFalse("Process is still alive!", DefaultContainerExecutor
					.ContainerIsAlive(cId.ToString()));
			}
			else
			{
				BufferedReader reader = new BufferedReader(new FileReader(processStartFile));
				bool foundSigTermMessage = false;
				while (true)
				{
					string line = reader.ReadLine();
					if (line == null)
					{
						break;
					}
					if (line.Contains("SIGTERM"))
					{
						foundSigTermMessage = true;
						break;
					}
				}
				NUnit.Framework.Assert.IsTrue("Did not find sigterm message", foundSigTermMessage
					);
				reader.Close();
			}
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestDelayedKill()
		{
			InternalKillTest(true);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestImmediateKill()
		{
			InternalKillTest(false);
		}

		public virtual void TestCallFailureWithNullLocalizedResources()
		{
			Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container container
				 = Org.Mockito.Mockito.Mock<Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container
				>();
			Org.Mockito.Mockito.When(container.GetContainerId()).ThenReturn(ContainerId.NewContainerId
				(ApplicationAttemptId.NewInstance(ApplicationId.NewInstance(Runtime.CurrentTimeMillis
				(), 1), 1), 1));
			ContainerLaunchContext clc = Org.Mockito.Mockito.Mock<ContainerLaunchContext>();
			Org.Mockito.Mockito.When(clc.GetCommands()).ThenReturn(Sharpen.Collections.EmptyList
				<string>());
			Org.Mockito.Mockito.When(container.GetLaunchContext()).ThenReturn(clc);
			Org.Mockito.Mockito.When(container.GetLocalizedResources()).ThenReturn(null);
			Dispatcher dispatcher = Org.Mockito.Mockito.Mock<Dispatcher>();
			EventHandler eventHandler = new _EventHandler_911();
			Org.Mockito.Mockito.When(dispatcher.GetEventHandler()).ThenReturn(eventHandler);
			ContainerLaunch launch = new ContainerLaunch(context, new Configuration(), dispatcher
				, exec, null, container, dirsHandler, containerManager);
			launch.Call();
		}

		private sealed class _EventHandler_911 : EventHandler
		{
			public _EventHandler_911()
			{
			}

			public void Handle(Org.Apache.Hadoop.Yarn.Event.Event @event)
			{
				NUnit.Framework.Assert.IsTrue(@event is ContainerExitEvent);
				ContainerExitEvent exitEvent = (ContainerExitEvent)@event;
				NUnit.Framework.Assert.AreEqual(ContainerEventType.ContainerExitedWithFailure, exitEvent
					.GetType());
			}
		}

		/// <exception cref="Org.Apache.Hadoop.Security.Token.SecretManager.InvalidToken"/>
		protected internal virtual Token CreateContainerToken(ContainerId cId, Priority priority
			, long createTime)
		{
			Resource r = BuilderUtils.NewResource(1024, 1);
			ContainerTokenIdentifier containerTokenIdentifier = new ContainerTokenIdentifier(
				cId, context.GetNodeId().ToString(), user, r, Runtime.CurrentTimeMillis() + 10000L
				, 123, DummyRmIdentifier, priority, createTime);
			Token containerToken = BuilderUtils.NewContainerToken(context.GetNodeId(), context
				.GetContainerTokenSecretManager().RetrievePassword(containerTokenIdentifier), containerTokenIdentifier
				);
			return containerToken;
		}

		/// <summary>Test that script exists with non-zero exit code when command fails.</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestShellScriptBuilderNonZeroExitCode()
		{
			ContainerLaunch.ShellScriptBuilder builder = ContainerLaunch.ShellScriptBuilder.Create
				();
			builder.Command(Arrays.AsList(new string[] { "unknownCommand" }));
			FilePath shellFile = Shell.AppendScriptExtension(tmpDir, "testShellScriptBuilderError"
				);
			TextWriter writer = new TextWriter(new FileOutputStream(shellFile));
			builder.Write(writer);
			writer.Close();
			try
			{
				FileUtil.SetExecutable(shellFile, true);
				Shell.ShellCommandExecutor shexc = new Shell.ShellCommandExecutor(new string[] { 
					shellFile.GetAbsolutePath() }, tmpDir);
				try
				{
					shexc.Execute();
					NUnit.Framework.Assert.Fail("builder shell command was expected to throw");
				}
				catch (IOException e)
				{
					// expected
					System.Console.Out.WriteLine("Received an expected exception: " + e.Message);
				}
			}
			finally
			{
				FileUtil.FullyDelete(shellFile);
			}
		}

		private const string expectedMessage = "The command line has a length of";

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestWindowsShellScriptBuilderCommand()
		{
			string callCmd = "@call ";
			// Test is only relevant on Windows
			Assume.AssumeTrue(Shell.Windows);
			// The tests are built on assuming 8191 max command line length
			NUnit.Framework.Assert.AreEqual(8191, Shell.WindowsMaxShellLenght);
			ContainerLaunch.ShellScriptBuilder builder = ContainerLaunch.ShellScriptBuilder.Create
				();
			// Basic tests: less length, exact length, max+1 length 
			builder.Command(Arrays.AsList(StringUtils.Repeat("A", 1024)));
			builder.Command(Arrays.AsList(StringUtils.Repeat("E", Shell.WindowsMaxShellLenght
				 - callCmd.Length)));
			try
			{
				builder.Command(Arrays.AsList(StringUtils.Repeat("X", Shell.WindowsMaxShellLenght
					 - callCmd.Length + 1)));
				NUnit.Framework.Assert.Fail("longCommand was expected to throw");
			}
			catch (IOException e)
			{
				Assert.AssertThat(e.Message, JUnitMatchers.ContainsString(expectedMessage));
			}
			// Composite tests, from parts: less, exact and +
			builder.Command(Arrays.AsList(StringUtils.Repeat("A", 1024), StringUtils.Repeat("A"
				, 1024), StringUtils.Repeat("A", 1024)));
			// buildr.command joins the command parts with an extra space
			builder.Command(Arrays.AsList(StringUtils.Repeat("E", 4095), StringUtils.Repeat("E"
				, 2047), StringUtils.Repeat("E", 2047 - callCmd.Length)));
			try
			{
				builder.Command(Arrays.AsList(StringUtils.Repeat("X", 4095), StringUtils.Repeat("X"
					, 2047), StringUtils.Repeat("X", 2048 - callCmd.Length)));
				NUnit.Framework.Assert.Fail("long commands was expected to throw");
			}
			catch (IOException e)
			{
				Assert.AssertThat(e.Message, JUnitMatchers.ContainsString(expectedMessage));
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestWindowsShellScriptBuilderEnv()
		{
			// Test is only relevant on Windows
			Assume.AssumeTrue(Shell.Windows);
			// The tests are built on assuming 8191 max command line length
			NUnit.Framework.Assert.AreEqual(8191, Shell.WindowsMaxShellLenght);
			ContainerLaunch.ShellScriptBuilder builder = ContainerLaunch.ShellScriptBuilder.Create
				();
			// test env
			builder.Env("somekey", StringUtils.Repeat("A", 1024));
			builder.Env("somekey", StringUtils.Repeat("A", Shell.WindowsMaxShellLenght - ("@set somekey="
				).Length));
			try
			{
				builder.Env("somekey", StringUtils.Repeat("A", Shell.WindowsMaxShellLenght - ("@set somekey="
					).Length) + 1);
				NUnit.Framework.Assert.Fail("long env was expected to throw");
			}
			catch (IOException e)
			{
				Assert.AssertThat(e.Message, JUnitMatchers.ContainsString(expectedMessage));
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestWindowsShellScriptBuilderMkdir()
		{
			string mkDirCmd = "@if not exist \"\" mkdir \"\"";
			// Test is only relevant on Windows
			Assume.AssumeTrue(Shell.Windows);
			// The tests are built on assuming 8191 max command line length
			NUnit.Framework.Assert.AreEqual(8191, Shell.WindowsMaxShellLenght);
			ContainerLaunch.ShellScriptBuilder builder = ContainerLaunch.ShellScriptBuilder.Create
				();
			// test mkdir
			builder.Mkdir(new Path(StringUtils.Repeat("A", 1024)));
			builder.Mkdir(new Path(StringUtils.Repeat("E", (Shell.WindowsMaxShellLenght - mkDirCmd
				.Length) / 2)));
			try
			{
				builder.Mkdir(new Path(StringUtils.Repeat("X", (Shell.WindowsMaxShellLenght - mkDirCmd
					.Length) / 2 + 1)));
				NUnit.Framework.Assert.Fail("long mkdir was expected to throw");
			}
			catch (IOException e)
			{
				Assert.AssertThat(e.Message, JUnitMatchers.ContainsString(expectedMessage));
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestWindowsShellScriptBuilderLink()
		{
			// Test is only relevant on Windows
			Assume.AssumeTrue(Shell.Windows);
			string linkCmd = "@" + Shell.Winutils + " symlink \"\" \"\"";
			// The tests are built on assuming 8191 max command line length
			NUnit.Framework.Assert.AreEqual(8191, Shell.WindowsMaxShellLenght);
			ContainerLaunch.ShellScriptBuilder builder = ContainerLaunch.ShellScriptBuilder.Create
				();
			// test link
			builder.Link(new Path(StringUtils.Repeat("A", 1024)), new Path(StringUtils.Repeat
				("B", 1024)));
			builder.Link(new Path(StringUtils.Repeat("E", (Shell.WindowsMaxShellLenght - linkCmd
				.Length) / 2)), new Path(StringUtils.Repeat("F", (Shell.WindowsMaxShellLenght - 
				linkCmd.Length) / 2)));
			try
			{
				builder.Link(new Path(StringUtils.Repeat("X", (Shell.WindowsMaxShellLenght - linkCmd
					.Length) / 2 + 1)), new Path(StringUtils.Repeat("Y", (Shell.WindowsMaxShellLenght
					 - linkCmd.Length) / 2) + 1));
				NUnit.Framework.Assert.Fail("long link was expected to throw");
			}
			catch (IOException e)
			{
				Assert.AssertThat(e.Message, JUnitMatchers.ContainsString(expectedMessage));
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestKillProcessGroup()
		{
			Assume.AssumeTrue(Shell.isSetsidAvailable);
			containerManager.Start();
			// Construct the Container-id
			ApplicationId appId = ApplicationId.NewInstance(2, 2);
			ApplicationAttemptId appAttemptId = ApplicationAttemptId.NewInstance(appId, 1);
			ContainerId cId = ContainerId.NewContainerId(appAttemptId, 0);
			FilePath processStartFile = new FilePath(tmpDir, "pid.txt").GetAbsoluteFile();
			FilePath childProcessStartFile = new FilePath(tmpDir, "child_pid.txt").GetAbsoluteFile
				();
			// setup a script that can handle sigterm gracefully
			FilePath scriptFile = Shell.AppendScriptExtension(tmpDir, "testscript");
			PrintWriter writer = new PrintWriter(new FileOutputStream(scriptFile));
			writer.WriteLine("#!/bin/bash\n\n");
			writer.WriteLine("echo \"Running testscript for forked process\"");
			writer.WriteLine("umask 0");
			writer.WriteLine("echo $$ >> " + processStartFile);
			writer.WriteLine("while true;\ndo sleep 1s;\ndone > /dev/null 2>&1 &");
			writer.WriteLine("echo $! >> " + childProcessStartFile);
			writer.WriteLine("while true;\ndo sleep 1s;\ndone");
			writer.Close();
			FileUtil.SetExecutable(scriptFile, true);
			ContainerLaunchContext containerLaunchContext = recordFactory.NewRecordInstance<ContainerLaunchContext
				>();
			// upload the script file so that the container can run it
			URL resource_alpha = ConverterUtils.GetYarnUrlFromPath(localFS.MakeQualified(new 
				Path(scriptFile.GetAbsolutePath())));
			LocalResource rsrc_alpha = recordFactory.NewRecordInstance<LocalResource>();
			rsrc_alpha.SetResource(resource_alpha);
			rsrc_alpha.SetSize(-1);
			rsrc_alpha.SetVisibility(LocalResourceVisibility.Application);
			rsrc_alpha.SetType(LocalResourceType.File);
			rsrc_alpha.SetTimestamp(scriptFile.LastModified());
			string destinationFile = "dest_file.sh";
			IDictionary<string, LocalResource> localResources = new Dictionary<string, LocalResource
				>();
			localResources[destinationFile] = rsrc_alpha;
			containerLaunchContext.SetLocalResources(localResources);
			// set up the rest of the container
			IList<string> commands = Arrays.AsList(Shell.GetRunScriptCommand(scriptFile));
			containerLaunchContext.SetCommands(commands);
			Priority priority = Priority.NewInstance(10);
			long createTime = 1234;
			Token containerToken = CreateContainerToken(cId, priority, createTime);
			StartContainerRequest scRequest = StartContainerRequest.NewInstance(containerLaunchContext
				, containerToken);
			IList<StartContainerRequest> list = new AList<StartContainerRequest>();
			list.AddItem(scRequest);
			StartContainersRequest allRequests = StartContainersRequest.NewInstance(list);
			containerManager.StartContainers(allRequests);
			int timeoutSecs = 0;
			while (!processStartFile.Exists() && timeoutSecs++ < 20)
			{
				Sharpen.Thread.Sleep(1000);
				Log.Info("Waiting for process start-file to be created");
			}
			NUnit.Framework.Assert.IsTrue("ProcessStartFile doesn't exist!", processStartFile
				.Exists());
			BufferedReader reader = new BufferedReader(new FileReader(processStartFile));
			// Get the pid of the process
			string pid = reader.ReadLine().Trim();
			// No more lines
			NUnit.Framework.Assert.AreEqual(null, reader.ReadLine());
			reader.Close();
			reader = new BufferedReader(new FileReader(childProcessStartFile));
			// Get the pid of the child process
			string child = reader.ReadLine().Trim();
			// No more lines
			NUnit.Framework.Assert.AreEqual(null, reader.ReadLine());
			reader.Close();
			Log.Info("Manually killing pid " + pid + ", but not child pid " + child);
			Shell.ExecCommand(new string[] { "kill", "-9", pid });
			BaseContainerManagerTest.WaitForContainerState(containerManager, cId, ContainerState
				.Complete);
			NUnit.Framework.Assert.IsFalse("Process is still alive!", DefaultContainerExecutor
				.ContainerIsAlive(pid));
			IList<ContainerId> containerIds = new AList<ContainerId>();
			containerIds.AddItem(cId);
			GetContainerStatusesRequest gcsRequest = GetContainerStatusesRequest.NewInstance(
				containerIds);
			ContainerStatus containerStatus = containerManager.GetContainerStatuses(gcsRequest
				).GetContainerStatuses()[0];
			NUnit.Framework.Assert.AreEqual(ContainerExecutor.ExitCode.ForceKilled.GetExitCode
				(), containerStatus.GetExitStatus());
		}
	}
}
