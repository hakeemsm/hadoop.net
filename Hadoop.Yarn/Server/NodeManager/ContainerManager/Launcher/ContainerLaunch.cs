using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Com.Google.Common.Annotations;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn.Api;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Event;
using Org.Apache.Hadoop.Yarn.Ipc;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Localizer;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Util;
using Org.Apache.Hadoop.Yarn.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Launcher
{
	public class ContainerLaunch : Callable<int>
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Launcher.ContainerLaunch
			));

		public static readonly string ContainerScript = Shell.AppendScriptExtension("launch_container"
			);

		public const string FinalContainerTokensFile = "container_tokens";

		private const string PidFileNameFmt = "%s.pid";

		private const string ExitCodeFileSuffix = ".exitcode";

		protected internal readonly Dispatcher dispatcher;

		protected internal readonly ContainerExecutor exec;

		private readonly Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application.Application
			 app;

		protected internal readonly Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container
			 container;

		private readonly Configuration conf;

		private readonly Context context;

		private readonly ContainerManagerImpl containerManager;

		protected internal AtomicBoolean shouldLaunchContainer = new AtomicBoolean(false);

		protected internal AtomicBoolean completed = new AtomicBoolean(false);

		private long sleepDelayBeforeSigKill = 250;

		private long maxKillWaitTime = 2000;

		protected internal Path pidFilePath = null;

		private readonly LocalDirsHandlerService dirsHandler;

		public ContainerLaunch(Context context, Configuration configuration, Dispatcher dispatcher
			, ContainerExecutor exec, Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Application.Application
			 app, Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container
			 container, LocalDirsHandlerService dirsHandler, ContainerManagerImpl containerManager
			)
		{
			this.context = context;
			this.conf = configuration;
			this.app = app;
			this.exec = exec;
			this.container = container;
			this.dispatcher = dispatcher;
			this.dirsHandler = dirsHandler;
			this.containerManager = containerManager;
			this.sleepDelayBeforeSigKill = conf.GetLong(YarnConfiguration.NmSleepDelayBeforeSigkillMs
				, YarnConfiguration.DefaultNmSleepDelayBeforeSigkillMs);
			this.maxKillWaitTime = conf.GetLong(YarnConfiguration.NmProcessKillWaitMs, YarnConfiguration
				.DefaultNmProcessKillWaitMs);
		}

		[VisibleForTesting]
		public static string ExpandEnvironment(string var, Path containerLogDir)
		{
			var = var.Replace(ApplicationConstants.LogDirExpansionVar, containerLogDir.ToString
				());
			var = var.Replace(ApplicationConstants.ClassPathSeparator, FilePath.pathSeparator
				);
			// replace parameter expansion marker. e.g. {{VAR}} on Windows is replaced
			// as %VAR% and on Linux replaced as "$VAR"
			if (Shell.Windows)
			{
				var = var.ReplaceAll("(\\{\\{)|(\\}\\})", "%");
			}
			else
			{
				var = var.Replace(ApplicationConstants.ParameterExpansionLeft, "$");
				var = var.Replace(ApplicationConstants.ParameterExpansionRight, string.Empty);
			}
			return var;
		}

		public virtual int Call()
		{
			// dispatcher not typed
			ContainerLaunchContext launchContext = container.GetLaunchContext();
			IDictionary<Path, IList<string>> localResources = null;
			ContainerId containerID = container.GetContainerId();
			string containerIdStr = ConverterUtils.ToString(containerID);
			IList<string> command = launchContext.GetCommands();
			int ret = -1;
			// CONTAINER_KILLED_ON_REQUEST should not be missed if the container
			// is already at KILLING
			if (container.GetContainerState() == ContainerState.Killing)
			{
				dispatcher.GetEventHandler().Handle(new ContainerExitEvent(containerID, ContainerEventType
					.ContainerKilledOnRequest, Shell.Windows ? ContainerExecutor.ExitCode.ForceKilled
					.GetExitCode() : ContainerExecutor.ExitCode.Terminated.GetExitCode(), "Container terminated before launch."
					));
				return 0;
			}
			try
			{
				localResources = container.GetLocalizedResources();
				if (localResources == null)
				{
					throw RPCUtil.GetRemoteException("Unable to get local resources when Container " 
						+ containerID + " is at " + container.GetContainerState());
				}
				string user = container.GetUser();
				// /////////////////////////// Variable expansion
				// Before the container script gets written out.
				IList<string> newCmds = new AList<string>(command.Count);
				string appIdStr = app.GetAppId().ToString();
				string relativeContainerLogDir = Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Launcher.ContainerLaunch
					.GetRelativeContainerLogDir(appIdStr, containerIdStr);
				Path containerLogDir = dirsHandler.GetLogPathForWrite(relativeContainerLogDir, false
					);
				foreach (string str in command)
				{
					// TODO: Should we instead work via symlinks without this grammar?
					newCmds.AddItem(ExpandEnvironment(str, containerLogDir));
				}
				launchContext.SetCommands(newCmds);
				IDictionary<string, string> environment = launchContext.GetEnvironment();
				// Make a copy of env to iterate & do variable expansion
				foreach (KeyValuePair<string, string> entry in environment)
				{
					string value = entry.Value;
					value = ExpandEnvironment(value, containerLogDir);
					entry.SetValue(value);
				}
				// /////////////////////////// End of variable expansion
				FileContext lfs = FileContext.GetLocalFSFileContext();
				Path nmPrivateContainerScriptPath = dirsHandler.GetLocalPathForWrite(GetContainerPrivateDir
					(appIdStr, containerIdStr) + Path.Separator + ContainerScript);
				Path nmPrivateTokensPath = dirsHandler.GetLocalPathForWrite(GetContainerPrivateDir
					(appIdStr, containerIdStr) + Path.Separator + string.Format(ContainerLocalizer.TokenFileNameFmt
					, containerIdStr));
				Path nmPrivateClasspathJarDir = dirsHandler.GetLocalPathForWrite(GetContainerPrivateDir
					(appIdStr, containerIdStr));
				DataOutputStream containerScriptOutStream = null;
				DataOutputStream tokensOutStream = null;
				// Select the working directory for the container
				Path containerWorkDir = dirsHandler.GetLocalPathForWrite(ContainerLocalizer.Usercache
					 + Path.Separator + user + Path.Separator + ContainerLocalizer.Appcache + Path.Separator
					 + appIdStr + Path.Separator + containerIdStr, LocalDirAllocator.SizeUnknown, false
					);
				string pidFileSubpath = GetPidFileSubpath(appIdStr, containerIdStr);
				// pid file should be in nm private dir so that it is not 
				// accessible by users
				pidFilePath = dirsHandler.GetLocalPathForWrite(pidFileSubpath);
				IList<string> localDirs = dirsHandler.GetLocalDirs();
				IList<string> logDirs = dirsHandler.GetLogDirs();
				IList<string> containerLogDirs = new AList<string>();
				foreach (string logDir in logDirs)
				{
					containerLogDirs.AddItem(logDir + Path.Separator + relativeContainerLogDir);
				}
				if (!dirsHandler.AreDisksHealthy())
				{
					ret = ContainerExitStatus.DisksFailed;
					throw new IOException("Most of the disks failed. " + dirsHandler.GetDisksHealthReport
						(false));
				}
				try
				{
					// /////////// Write out the container-script in the nmPrivate space.
					IList<Path> appDirs = new AList<Path>(localDirs.Count);
					foreach (string localDir in localDirs)
					{
						Path usersdir = new Path(localDir, ContainerLocalizer.Usercache);
						Path userdir = new Path(usersdir, user);
						Path appsdir = new Path(userdir, ContainerLocalizer.Appcache);
						appDirs.AddItem(new Path(appsdir, appIdStr));
					}
					containerScriptOutStream = lfs.Create(nmPrivateContainerScriptPath, EnumSet.Of(CreateFlag
						.Create, CreateFlag.Overwrite));
					// Set the token location too.
					environment[ApplicationConstants.ContainerTokenFileEnvName] = new Path(containerWorkDir
						, FinalContainerTokensFile).ToUri().GetPath();
					// Sanitize the container's environment
					SanitizeEnv(environment, containerWorkDir, appDirs, containerLogDirs, localResources
						, nmPrivateClasspathJarDir);
					// Write out the environment
					exec.WriteLaunchEnv(containerScriptOutStream, environment, localResources, launchContext
						.GetCommands());
					// /////////// End of writing out container-script
					// /////////// Write out the container-tokens in the nmPrivate space.
					tokensOutStream = lfs.Create(nmPrivateTokensPath, EnumSet.Of(CreateFlag.Create, CreateFlag
						.Overwrite));
					Credentials creds = container.GetCredentials();
					creds.WriteTokenStorageToStream(tokensOutStream);
				}
				finally
				{
					// /////////// End of writing out container-tokens
					IOUtils.Cleanup(Log, containerScriptOutStream, tokensOutStream);
				}
				// LaunchContainer is a blocking call. We are here almost means the
				// container is launched, so send out the event.
				dispatcher.GetEventHandler().Handle(new ContainerEvent(containerID, ContainerEventType
					.ContainerLaunched));
				context.GetNMStateStore().StoreContainerLaunched(containerID);
				// Check if the container is signalled to be killed.
				if (!shouldLaunchContainer.CompareAndSet(false, true))
				{
					Log.Info("Container " + containerIdStr + " not launched as " + "cleanup already called"
						);
					ret = ContainerExecutor.ExitCode.Terminated.GetExitCode();
				}
				else
				{
					exec.ActivateContainer(containerID, pidFilePath);
					ret = exec.LaunchContainer(container, nmPrivateContainerScriptPath, nmPrivateTokensPath
						, user, appIdStr, containerWorkDir, localDirs, logDirs);
				}
			}
			catch (Exception e)
			{
				Log.Warn("Failed to launch container.", e);
				dispatcher.GetEventHandler().Handle(new ContainerExitEvent(containerID, ContainerEventType
					.ContainerExitedWithFailure, ret, e.Message));
				return ret;
			}
			finally
			{
				completed.Set(true);
				exec.DeactivateContainer(containerID);
				try
				{
					context.GetNMStateStore().StoreContainerCompleted(containerID, ret);
				}
				catch (IOException)
				{
					Log.Error("Unable to set exit code for container " + containerID);
				}
			}
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Container " + containerIdStr + " completed with exit code " + ret);
			}
			if (ret == ContainerExecutor.ExitCode.ForceKilled.GetExitCode() || ret == ContainerExecutor.ExitCode
				.Terminated.GetExitCode())
			{
				// If the process was killed, Send container_cleanedup_after_kill and
				// just break out of this method.
				dispatcher.GetEventHandler().Handle(new ContainerExitEvent(containerID, ContainerEventType
					.ContainerKilledOnRequest, ret, "Container exited with a non-zero exit code " + 
					ret));
				return ret;
			}
			if (ret != 0)
			{
				Log.Warn("Container exited with a non-zero exit code " + ret);
				this.dispatcher.GetEventHandler().Handle(new ContainerExitEvent(containerID, ContainerEventType
					.ContainerExitedWithFailure, ret, "Container exited with a non-zero exit code " 
					+ ret));
				return ret;
			}
			Log.Info("Container " + containerIdStr + " succeeded ");
			dispatcher.GetEventHandler().Handle(new ContainerEvent(containerID, ContainerEventType
				.ContainerExitedWithSuccess));
			return 0;
		}

		protected internal virtual string GetPidFileSubpath(string appIdStr, string containerIdStr
			)
		{
			return GetContainerPrivateDir(appIdStr, containerIdStr) + Path.Separator + string
				.Format(Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Launcher.ContainerLaunch
				.PidFileNameFmt, containerIdStr);
		}

		/// <summary>Cleanup the container.</summary>
		/// <remarks>
		/// Cleanup the container.
		/// Cancels the launch if launch has not started yet or signals
		/// the executor to not execute the process if not already done so.
		/// Also, sends a SIGTERM followed by a SIGKILL to the process if
		/// the process id is available.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public virtual void CleanupContainer()
		{
			// dispatcher not typed
			ContainerId containerId = container.GetContainerId();
			string containerIdStr = ConverterUtils.ToString(containerId);
			Log.Info("Cleaning up container " + containerIdStr);
			try
			{
				context.GetNMStateStore().StoreContainerKilled(containerId);
			}
			catch (IOException e)
			{
				Log.Error("Unable to mark container " + containerId + " killed in store", e);
			}
			// launch flag will be set to true if process already launched
			bool alreadyLaunched = !shouldLaunchContainer.CompareAndSet(false, true);
			if (!alreadyLaunched)
			{
				Log.Info("Container " + containerIdStr + " not launched." + " No cleanup needed to be done"
					);
				return;
			}
			Log.Debug("Marking container " + containerIdStr + " as inactive");
			// this should ensure that if the container process has not launched 
			// by this time, it will never be launched
			exec.DeactivateContainer(containerId);
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Getting pid for container " + containerIdStr + " to kill" + " from pid file "
					 + (pidFilePath != null ? pidFilePath.ToString() : "null"));
			}
			// however the container process may have already started
			try
			{
				// get process id from pid file if available
				// else if shell is still active, get it from the shell
				string processId = null;
				if (pidFilePath != null)
				{
					processId = GetContainerPid(pidFilePath);
				}
				// kill process
				if (processId != null)
				{
					string user = container.GetUser();
					Log.Debug("Sending signal to pid " + processId + " as user " + user + " for container "
						 + containerIdStr);
					ContainerExecutor.Signal signal = sleepDelayBeforeSigKill > 0 ? ContainerExecutor.Signal
						.Term : ContainerExecutor.Signal.Kill;
					bool result = exec.SignalContainer(user, processId, signal);
					Log.Debug("Sent signal " + signal + " to pid " + processId + " as user " + user +
						 " for container " + containerIdStr + ", result=" + (result ? "success" : "failed"
						));
					if (sleepDelayBeforeSigKill > 0)
					{
						new ContainerExecutor.DelayedProcessKiller(container, user, processId, sleepDelayBeforeSigKill
							, ContainerExecutor.Signal.Kill, exec).Start();
					}
				}
			}
			catch (Exception e)
			{
				string message = "Exception when trying to cleanup container " + containerIdStr +
					 ": " + StringUtils.StringifyException(e);
				Log.Warn(message);
				dispatcher.GetEventHandler().Handle(new ContainerDiagnosticsUpdateEvent(containerId
					, message));
			}
			finally
			{
				// cleanup pid file if present
				if (pidFilePath != null)
				{
					FileContext lfs = FileContext.GetLocalFSFileContext();
					lfs.Delete(pidFilePath, false);
					lfs.Delete(pidFilePath.Suffix(ExitCodeFileSuffix), false);
				}
			}
		}

		/// <summary>
		/// Loop through for a time-bounded interval waiting to
		/// read the process id from a file generated by a running process.
		/// </summary>
		/// <param name="pidFilePath">File from which to read the process id</param>
		/// <returns>Process ID</returns>
		/// <exception cref="System.Exception"/>
		private string GetContainerPid(Path pidFilePath)
		{
			string containerIdStr = ConverterUtils.ToString(container.GetContainerId());
			string processId = null;
			Log.Debug("Accessing pid for container " + containerIdStr + " from pid file " + pidFilePath
				);
			int sleepCounter = 0;
			int sleepInterval = 100;
			// loop waiting for pid file to show up 
			// until our timer expires in which case we admit defeat
			while (true)
			{
				processId = ProcessIdFileReader.GetProcessId(pidFilePath);
				if (processId != null)
				{
					Log.Debug("Got pid " + processId + " for container " + containerIdStr);
					break;
				}
				else
				{
					if ((sleepCounter * sleepInterval) > maxKillWaitTime)
					{
						Log.Info("Could not get pid for " + containerIdStr + ". Waited for " + maxKillWaitTime
							 + " ms.");
						break;
					}
					else
					{
						++sleepCounter;
						Sharpen.Thread.Sleep(sleepInterval);
					}
				}
			}
			return processId;
		}

		public static string GetRelativeContainerLogDir(string appIdStr, string containerIdStr
			)
		{
			return appIdStr + Path.Separator + containerIdStr;
		}

		private string GetContainerPrivateDir(string appIdStr, string containerIdStr)
		{
			return GetAppPrivateDir(appIdStr) + Path.Separator + containerIdStr + Path.Separator;
		}

		private string GetAppPrivateDir(string appIdStr)
		{
			return ResourceLocalizationService.NmPrivateDir + Path.Separator + appIdStr;
		}

		internal virtual Context GetContext()
		{
			return context;
		}

		public abstract class ShellScriptBuilder
		{
			public static ContainerLaunch.ShellScriptBuilder Create()
			{
				return Shell.Windows ? new ContainerLaunch.WindowsShellScriptBuilder() : new ContainerLaunch.UnixShellScriptBuilder
					();
			}

			private static readonly string LineSeparator = Runtime.GetProperty("line.separator"
				);

			private readonly StringBuilder sb = new StringBuilder();

			/// <exception cref="System.IO.IOException"/>
			public abstract void Command(IList<string> command);

			/// <exception cref="System.IO.IOException"/>
			public abstract void Env(string key, string value);

			/// <exception cref="System.IO.IOException"/>
			public void Symlink(Path src, Path dst)
			{
				if (!src.IsAbsolute())
				{
					throw new IOException("Source must be absolute");
				}
				if (dst.IsAbsolute())
				{
					throw new IOException("Destination must be relative");
				}
				if (dst.ToUri().GetPath().IndexOf('/') != -1)
				{
					Mkdir(dst.GetParent());
				}
				Link(src, dst);
			}

			public override string ToString()
			{
				return sb.ToString();
			}

			/// <exception cref="System.IO.IOException"/>
			public void Write(TextWriter @out)
			{
				@out.Append(sb);
			}

			protected internal void Line(params string[] command)
			{
				foreach (string s in command)
				{
					sb.Append(s);
				}
				sb.Append(LineSeparator);
			}

			/// <exception cref="System.IO.IOException"/>
			protected internal abstract void Link(Path src, Path dst);

			/// <exception cref="System.IO.IOException"/>
			protected internal abstract void Mkdir(Path path);
		}

		private sealed class UnixShellScriptBuilder : ContainerLaunch.ShellScriptBuilder
		{
			private void ErrorCheck()
			{
				Line("hadoop_shell_errorcode=$?");
				Line("if [ $hadoop_shell_errorcode -ne 0 ]");
				Line("then");
				Line("  exit $hadoop_shell_errorcode");
				Line("fi");
			}

			public UnixShellScriptBuilder()
			{
				Line("#!/bin/bash");
				Line();
			}

			public override void Command(IList<string> command)
			{
				Line("exec /bin/bash -c \"", StringUtils.Join(" ", command), "\"");
				ErrorCheck();
			}

			public override void Env(string key, string value)
			{
				Line("export ", key, "=\"", value, "\"");
			}

			/// <exception cref="System.IO.IOException"/>
			protected internal override void Link(Path src, Path dst)
			{
				Line("ln -sf \"", src.ToUri().GetPath(), "\" \"", dst.ToString(), "\"");
				ErrorCheck();
			}

			protected internal override void Mkdir(Path path)
			{
				Line("mkdir -p ", path.ToString());
				ErrorCheck();
			}
		}

		private sealed class WindowsShellScriptBuilder : ContainerLaunch.ShellScriptBuilder
		{
			private void ErrorCheck()
			{
				Line("@if %errorlevel% neq 0 exit /b %errorlevel%");
			}

			/// <exception cref="System.IO.IOException"/>
			private void LineWithLenCheck(params string[] commands)
			{
				Shell.CheckWindowsCommandLineLength(commands);
				Line(commands);
			}

			public WindowsShellScriptBuilder()
			{
				Line("@setlocal");
				Line();
			}

			/// <exception cref="System.IO.IOException"/>
			public override void Command(IList<string> command)
			{
				LineWithLenCheck("@call ", StringUtils.Join(" ", command));
				ErrorCheck();
			}

			/// <exception cref="System.IO.IOException"/>
			public override void Env(string key, string value)
			{
				LineWithLenCheck("@set ", key, "=", value);
				ErrorCheck();
			}

			/// <exception cref="System.IO.IOException"/>
			protected internal override void Link(Path src, Path dst)
			{
				FilePath srcFile = new FilePath(src.ToUri().GetPath());
				string srcFileStr = srcFile.GetPath();
				string dstFileStr = new FilePath(dst.ToString()).GetPath();
				// If not on Java7+ on Windows, then copy file instead of symlinking.
				// See also FileUtil#symLink for full explanation.
				if (!Shell.IsJava7OrAbove() && srcFile.IsFile())
				{
					LineWithLenCheck(string.Format("@copy \"%s\" \"%s\"", srcFileStr, dstFileStr));
					ErrorCheck();
				}
				else
				{
					LineWithLenCheck(string.Format("@%s symlink \"%s\" \"%s\"", Shell.Winutils, dstFileStr
						, srcFileStr));
					ErrorCheck();
				}
			}

			/// <exception cref="System.IO.IOException"/>
			protected internal override void Mkdir(Path path)
			{
				LineWithLenCheck(string.Format("@if not exist \"%s\" mkdir \"%s\"", path.ToString
					(), path.ToString()));
				ErrorCheck();
			}
		}

		private static void PutEnvIfNotNull(IDictionary<string, string> environment, string
			 variable, string value)
		{
			if (value != null)
			{
				environment[variable] = value;
			}
		}

		private static void PutEnvIfAbsent(IDictionary<string, string> environment, string
			 variable)
		{
			if (environment[variable] == null)
			{
				PutEnvIfNotNull(environment, variable, Runtime.Getenv(variable));
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void SanitizeEnv(IDictionary<string, string> environment, Path pwd
			, IList<Path> appDirs, IList<string> containerLogDirs, IDictionary<Path, IList<string
			>> resources, Path nmPrivateClasspathJarDir)
		{
			environment[ApplicationConstants.Environment.ContainerId.ToString()] = container.
				GetContainerId().ToString();
			environment[ApplicationConstants.Environment.NmPort.ToString()] = this.context.GetNodeId
				().GetPort().ToString();
			environment[ApplicationConstants.Environment.NmHost.ToString()] = this.context.GetNodeId
				().GetHost();
			environment[ApplicationConstants.Environment.NmHttpPort.ToString()] = this.context
				.GetHttpPort().ToString();
			environment[ApplicationConstants.Environment.LocalDirs.ToString()] = StringUtils.
				Join(",", appDirs);
			environment[ApplicationConstants.Environment.LogDirs.ToString()] = StringUtils.Join
				(",", containerLogDirs);
			environment[ApplicationConstants.Environment.User.ToString()] = container.GetUser
				();
			environment[ApplicationConstants.Environment.Logname.ToString()] = container.GetUser
				();
			environment[ApplicationConstants.Environment.Home.ToString()] = conf.Get(YarnConfiguration
				.NmUserHomeDir, YarnConfiguration.DefaultNmUserHomeDir);
			environment[ApplicationConstants.Environment.Pwd.ToString()] = pwd.ToString();
			PutEnvIfNotNull(environment, ApplicationConstants.Environment.HadoopConfDir.ToString
				(), Runtime.Getenv(ApplicationConstants.Environment.HadoopConfDir.ToString()));
			if (!Shell.Windows)
			{
				environment["JVM_PID"] = "$$";
			}
			// allow containers to override these variables
			string[] whitelist = conf.Get(YarnConfiguration.NmEnvWhitelist, YarnConfiguration
				.DefaultNmEnvWhitelist).Split(",");
			foreach (string whitelistEnvVariable in whitelist)
			{
				PutEnvIfAbsent(environment, whitelistEnvVariable.Trim());
			}
			// variables here will be forced in, even if the container has specified them.
			Apps.SetEnvFromInputString(environment, conf.Get(YarnConfiguration.NmAdminUserEnv
				, YarnConfiguration.DefaultNmAdminUserEnv), FilePath.pathSeparator);
			// TODO: Remove Windows check and use this approach on all platforms after
			// additional testing.  See YARN-358.
			if (Shell.Windows)
			{
				string inputClassPath = environment[ApplicationConstants.Environment.Classpath.ToString
					()];
				if (inputClassPath != null && !inputClassPath.IsEmpty())
				{
					//On non-windows, localized resources
					//from distcache are available via the classpath as they were placed
					//there but on windows they are not available when the classpath
					//jar is created and so they "are lost" and have to be explicitly
					//added to the classpath instead.  This also means that their position
					//is lost relative to other non-distcache classpath entries which will
					//break things like mapreduce.job.user.classpath.first.  An environment
					//variable can be set to indicate that distcache entries should come
					//first
					bool preferLocalizedJars = Sharpen.Extensions.ValueOf(environment[ApplicationConstants.Environment
						.ClasspathPrependDistcache.ToString()]);
					bool needsSeparator = false;
					StringBuilder newClassPath = new StringBuilder();
					if (!preferLocalizedJars)
					{
						newClassPath.Append(inputClassPath);
						needsSeparator = true;
					}
					// Localized resources do not exist at the desired paths yet, because the
					// container launch script has not run to create symlinks yet.  This
					// means that FileUtil.createJarWithClassPath can't automatically expand
					// wildcards to separate classpath entries for each file in the manifest.
					// To resolve this, append classpath entries explicitly for each
					// resource.
					foreach (KeyValuePair<Path, IList<string>> entry in resources)
					{
						bool targetIsDirectory = new FilePath(entry.Key.ToUri().GetPath()).IsDirectory();
						foreach (string linkName in entry.Value)
						{
							// Append resource.
							if (needsSeparator)
							{
								newClassPath.Append(FilePath.pathSeparator);
							}
							else
							{
								needsSeparator = true;
							}
							newClassPath.Append(pwd.ToString()).Append(Path.Separator).Append(linkName);
							// FileUtil.createJarWithClassPath must use File.toURI to convert
							// each file to a URI to write into the manifest's classpath.  For
							// directories, the classpath must have a trailing '/', but
							// File.toURI only appends the trailing '/' if it is a directory that
							// already exists.  To resolve this, add the classpath entries with
							// explicit trailing '/' here for any localized resource that targets
							// a directory.  Then, FileUtil.createJarWithClassPath will guarantee
							// that the resulting entry in the manifest's classpath will have a
							// trailing '/', and thus refer to a directory instead of a file.
							if (targetIsDirectory)
							{
								newClassPath.Append(Path.Separator);
							}
						}
					}
					if (preferLocalizedJars)
					{
						if (needsSeparator)
						{
							newClassPath.Append(FilePath.pathSeparator);
						}
						newClassPath.Append(inputClassPath);
					}
					// When the container launches, it takes the parent process's environment
					// and then adds/overwrites with the entries from the container launch
					// context.  Do the same thing here for correct substitution of
					// environment variables in the classpath jar manifest.
					IDictionary<string, string> mergedEnv = new Dictionary<string, string>(Sharpen.Runtime.GetEnv
						());
					mergedEnv.PutAll(environment);
					// this is hacky and temporary - it's to preserve the windows secure
					// behavior but enable non-secure windows to properly build the class
					// path for access to job.jar/lib/xyz and friends (see YARN-2803)
					Path jarDir;
					if (exec is WindowsSecureContainerExecutor)
					{
						jarDir = nmPrivateClasspathJarDir;
					}
					else
					{
						jarDir = pwd;
					}
					string[] jarCp = FileUtil.CreateJarWithClassPath(newClassPath.ToString(), jarDir, 
						pwd, mergedEnv);
					// In a secure cluster the classpath jar must be localized to grant access
					Path localizedClassPathJar = exec.LocalizeClasspathJar(new Path(jarCp[0]), pwd, container
						.GetUser());
					string replacementClassPath = localizedClassPathJar.ToString() + jarCp[1];
					environment[ApplicationConstants.Environment.Classpath.ToString()] = replacementClassPath;
				}
			}
			// put AuxiliaryService data to environment
			foreach (KeyValuePair<string, ByteBuffer> meta in containerManager.GetAuxServiceMetaData
				())
			{
				AuxiliaryServiceHelper.SetServiceDataIntoEnv(meta.Key, meta.Value, environment);
			}
		}

		public static string GetExitCodeFile(string pidFile)
		{
			return pidFile + ExitCodeFileSuffix;
		}
	}
}
