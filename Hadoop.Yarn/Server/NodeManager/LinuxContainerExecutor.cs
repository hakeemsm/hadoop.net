using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Text;
using Com.Google.Common.Annotations;
using Com.Google.Common.Base;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn.Api;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Localizer;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Util;
using Org.Apache.Hadoop.Yarn.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager
{
	public class LinuxContainerExecutor : ContainerExecutor
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(LinuxContainerExecutor
			));

		private string nonsecureLocalUser;

		private Sharpen.Pattern nonsecureLocalUserPattern;

		private string containerExecutorExe;

		private LCEResourcesHandler resourcesHandler;

		private bool containerSchedPriorityIsSet = false;

		private int containerSchedPriorityAdjustment = 0;

		private bool containerLimitUsers;

		public override void SetConf(Configuration conf)
		{
			base.SetConf(conf);
			containerExecutorExe = GetContainerExecutorExecutablePath(conf);
			resourcesHandler = ReflectionUtils.NewInstance(conf.GetClass<LCEResourcesHandler>
				(YarnConfiguration.NmLinuxContainerResourcesHandler, typeof(DefaultLCEResourcesHandler
				)), conf);
			resourcesHandler.SetConf(conf);
			if (conf.Get(YarnConfiguration.NmContainerExecutorSchedPriority) != null)
			{
				containerSchedPriorityIsSet = true;
				containerSchedPriorityAdjustment = conf.GetInt(YarnConfiguration.NmContainerExecutorSchedPriority
					, YarnConfiguration.DefaultNmContainerExecutorSchedPriority);
			}
			nonsecureLocalUser = conf.Get(YarnConfiguration.NmNonsecureModeLocalUserKey, YarnConfiguration
				.DefaultNmNonsecureModeLocalUser);
			nonsecureLocalUserPattern = Sharpen.Pattern.Compile(conf.Get(YarnConfiguration.NmNonsecureModeUserPatternKey
				, YarnConfiguration.DefaultNmNonsecureModeUserPattern));
			containerLimitUsers = conf.GetBoolean(YarnConfiguration.NmNonsecureModeLimitUsers
				, YarnConfiguration.DefaultNmNonsecureModeLimitUsers);
			if (!containerLimitUsers)
			{
				Log.Warn(YarnConfiguration.NmNonsecureModeLimitUsers + ": impersonation without authentication enabled"
					);
			}
		}

		internal virtual void VerifyUsernamePattern(string user)
		{
			if (!UserGroupInformation.IsSecurityEnabled() && !nonsecureLocalUserPattern.Matcher
				(user).Matches())
			{
				throw new ArgumentException("Invalid user name '" + user + "'," + " it must match '"
					 + nonsecureLocalUserPattern.Pattern() + "'");
			}
		}

		internal virtual string GetRunAsUser(string user)
		{
			if (UserGroupInformation.IsSecurityEnabled() || !containerLimitUsers)
			{
				return user;
			}
			else
			{
				return nonsecureLocalUser;
			}
		}

		/// <summary>List of commands that the setuid script will execute.</summary>
		[System.Serializable]
		internal sealed class Commands
		{
			public static readonly LinuxContainerExecutor.Commands InitializeContainer = new 
				LinuxContainerExecutor.Commands(0);

			public static readonly LinuxContainerExecutor.Commands LaunchContainer = new LinuxContainerExecutor.Commands
				(1);

			public static readonly LinuxContainerExecutor.Commands SignalContainer = new LinuxContainerExecutor.Commands
				(2);

			public static readonly LinuxContainerExecutor.Commands DeleteAsUser = new LinuxContainerExecutor.Commands
				(3);

			private int value;

			internal Commands(int value)
			{
				this.value = value;
			}

			internal int GetValue()
			{
				return LinuxContainerExecutor.Commands.value;
			}
		}

		/// <summary>Result codes returned from the C container-executor.</summary>
		/// <remarks>
		/// Result codes returned from the C container-executor.
		/// These must match the values in container-executor.h.
		/// </remarks>
		[System.Serializable]
		internal sealed class ResultCode
		{
			public static readonly LinuxContainerExecutor.ResultCode Ok = new LinuxContainerExecutor.ResultCode
				(0);

			public static readonly LinuxContainerExecutor.ResultCode InvalidUserName = new LinuxContainerExecutor.ResultCode
				(2);

			public static readonly LinuxContainerExecutor.ResultCode UnableToExecuteContainerScript
				 = new LinuxContainerExecutor.ResultCode(7);

			public static readonly LinuxContainerExecutor.ResultCode InvalidContainerPid = new 
				LinuxContainerExecutor.ResultCode(9);

			public static readonly LinuxContainerExecutor.ResultCode InvalidContainerExecPermissions
				 = new LinuxContainerExecutor.ResultCode(22);

			public static readonly LinuxContainerExecutor.ResultCode InvalidConfigFile = new 
				LinuxContainerExecutor.ResultCode(24);

			public static readonly LinuxContainerExecutor.ResultCode WriteCgroupFailed = new 
				LinuxContainerExecutor.ResultCode(27);

			private readonly int value;

			internal ResultCode(int value)
			{
				this.value = value;
			}

			internal int GetValue()
			{
				return LinuxContainerExecutor.ResultCode.value;
			}
		}

		protected internal virtual string GetContainerExecutorExecutablePath(Configuration
			 conf)
		{
			string yarnHomeEnvVar = Runtime.Getenv(ApplicationConstants.Environment.HadoopYarnHome
				.Key());
			FilePath hadoopBin = new FilePath(yarnHomeEnvVar, "bin");
			string defaultPath = new FilePath(hadoopBin, "container-executor").GetAbsolutePath
				();
			return null == conf ? defaultPath : conf.Get(YarnConfiguration.NmLinuxContainerExecutorPath
				, defaultPath);
		}

		protected internal virtual void AddSchedPriorityCommand(IList<string> command)
		{
			if (containerSchedPriorityIsSet)
			{
				Sharpen.Collections.AddAll(command, Arrays.AsList("nice", "-n", Sharpen.Extensions.ToString
					(containerSchedPriorityAdjustment)));
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Init()
		{
			// Send command to executor which will just start up, 
			// verify configuration/permissions and exit
			IList<string> command = new AList<string>(Arrays.AsList(containerExecutorExe, "--checksetup"
				));
			string[] commandArray = Sharpen.Collections.ToArray(command, new string[command.Count
				]);
			Shell.ShellCommandExecutor shExec = new Shell.ShellCommandExecutor(commandArray);
			if (Log.IsDebugEnabled())
			{
				Log.Debug("checkLinuxExecutorSetup: " + Arrays.ToString(commandArray));
			}
			try
			{
				shExec.Execute();
			}
			catch (Shell.ExitCodeException e)
			{
				int exitCode = shExec.GetExitCode();
				Log.Warn("Exit code from container executor initialization is : " + exitCode, e);
				LogOutput(shExec.GetOutput());
				throw new IOException("Linux container executor not configured properly" + " (error="
					 + exitCode + ")", e);
			}
			resourcesHandler.Init(this);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public override void StartLocalizer(Path nmPrivateContainerTokensPath, IPEndPoint
			 nmAddr, string user, string appId, string locId, LocalDirsHandlerService dirsHandler
			)
		{
			IList<string> localDirs = dirsHandler.GetLocalDirs();
			IList<string> logDirs = dirsHandler.GetLogDirs();
			VerifyUsernamePattern(user);
			string runAsUser = GetRunAsUser(user);
			IList<string> command = new AList<string>();
			AddSchedPriorityCommand(command);
			Sharpen.Collections.AddAll(command, Arrays.AsList(containerExecutorExe, runAsUser
				, user, Sharpen.Extensions.ToString(LinuxContainerExecutor.Commands.InitializeContainer
				.GetValue()), appId, nmPrivateContainerTokensPath.ToUri().GetPath().ToString(), 
				StringUtils.Join(",", localDirs), StringUtils.Join(",", logDirs)));
			FilePath jvm = new FilePath(new FilePath(Runtime.GetProperty("java.home"), "bin")
				, "java");
			// use same jvm as parent
			command.AddItem(jvm.ToString());
			command.AddItem("-classpath");
			command.AddItem(Runtime.GetProperty("java.class.path"));
			string javaLibPath = Runtime.GetProperty("java.library.path");
			if (javaLibPath != null)
			{
				command.AddItem("-Djava.library.path=" + javaLibPath);
			}
			BuildMainArgs(command, user, appId, locId, nmAddr, localDirs);
			string[] commandArray = Sharpen.Collections.ToArray(command, new string[command.Count
				]);
			Shell.ShellCommandExecutor shExec = new Shell.ShellCommandExecutor(commandArray);
			if (Log.IsDebugEnabled())
			{
				Log.Debug("initApplication: " + Arrays.ToString(commandArray));
			}
			try
			{
				shExec.Execute();
				if (Log.IsDebugEnabled())
				{
					LogOutput(shExec.GetOutput());
				}
			}
			catch (Shell.ExitCodeException e)
			{
				int exitCode = shExec.GetExitCode();
				Log.Warn("Exit code from container " + locId + " startLocalizer is : " + exitCode
					, e);
				LogOutput(shExec.GetOutput());
				throw new IOException("Application " + appId + " initialization failed" + " (exitCode="
					 + exitCode + ") with output: " + shExec.GetOutput(), e);
			}
		}

		[VisibleForTesting]
		public virtual void BuildMainArgs(IList<string> command, string user, string appId
			, string locId, IPEndPoint nmAddr, IList<string> localDirs)
		{
			ContainerLocalizer.BuildMainArgs(command, user, appId, locId, nmAddr, localDirs);
		}

		/// <exception cref="System.IO.IOException"/>
		public override int LaunchContainer(Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container
			 container, Path nmPrivateCotainerScriptPath, Path nmPrivateTokensPath, string user
			, string appId, Path containerWorkDir, IList<string> localDirs, IList<string> logDirs
			)
		{
			VerifyUsernamePattern(user);
			string runAsUser = GetRunAsUser(user);
			ContainerId containerId = container.GetContainerId();
			string containerIdStr = ConverterUtils.ToString(containerId);
			resourcesHandler.PreExecute(containerId, container.GetResource());
			string resourcesOptions = resourcesHandler.GetResourcesOption(containerId);
			Shell.ShellCommandExecutor shExec = null;
			try
			{
				Path pidFilePath = GetPidFilePath(containerId);
				if (pidFilePath != null)
				{
					IList<string> command = new AList<string>();
					AddSchedPriorityCommand(command);
					Sharpen.Collections.AddAll(command, Arrays.AsList(containerExecutorExe, runAsUser
						, user, Sharpen.Extensions.ToString(LinuxContainerExecutor.Commands.LaunchContainer
						.GetValue()), appId, containerIdStr, containerWorkDir.ToString(), nmPrivateCotainerScriptPath
						.ToUri().GetPath().ToString(), nmPrivateTokensPath.ToUri().GetPath().ToString(), 
						pidFilePath.ToString(), StringUtils.Join(",", localDirs), StringUtils.Join(",", 
						logDirs), resourcesOptions));
					string[] commandArray = Sharpen.Collections.ToArray(command, new string[command.Count
						]);
					shExec = new Shell.ShellCommandExecutor(commandArray, null, container.GetLaunchContext
						().GetEnvironment());
					// NM's cwd
					// sanitized env
					if (Log.IsDebugEnabled())
					{
						Log.Debug("launchContainer: " + Arrays.ToString(commandArray));
					}
					shExec.Execute();
					if (Log.IsDebugEnabled())
					{
						LogOutput(shExec.GetOutput());
					}
				}
				else
				{
					Log.Info("Container was marked as inactive. Returning terminated error");
					return ContainerExecutor.ExitCode.Terminated.GetExitCode();
				}
			}
			catch (Shell.ExitCodeException e)
			{
				int exitCode = shExec.GetExitCode();
				Log.Warn("Exit code from container " + containerId + " is : " + exitCode);
				// 143 (SIGTERM) and 137 (SIGKILL) exit codes means the container was
				// terminated/killed forcefully. In all other cases, log the
				// container-executor's output
				if (exitCode != ContainerExecutor.ExitCode.ForceKilled.GetExitCode() && exitCode 
					!= ContainerExecutor.ExitCode.Terminated.GetExitCode())
				{
					Log.Warn("Exception from container-launch with container ID: " + containerId + " and exit code: "
						 + exitCode, e);
					StringBuilder builder = new StringBuilder();
					builder.Append("Exception from container-launch.\n");
					builder.Append("Container id: " + containerId + "\n");
					builder.Append("Exit code: " + exitCode + "\n");
					if (!Optional.FromNullable(e.Message).Or(string.Empty).IsEmpty())
					{
						builder.Append("Exception message: " + e.Message + "\n");
					}
					builder.Append("Stack trace: " + StringUtils.StringifyException(e) + "\n");
					if (!shExec.GetOutput().IsEmpty())
					{
						builder.Append("Shell output: " + shExec.GetOutput() + "\n");
					}
					string diagnostics = builder.ToString();
					LogOutput(diagnostics);
					container.Handle(new ContainerDiagnosticsUpdateEvent(containerId, diagnostics));
				}
				else
				{
					container.Handle(new ContainerDiagnosticsUpdateEvent(containerId, "Container killed on request. Exit code is "
						 + exitCode));
				}
				return exitCode;
			}
			finally
			{
				resourcesHandler.PostExecute(containerId);
			}
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Output from LinuxContainerExecutor's launchContainer follows:");
				LogOutput(shExec.GetOutput());
			}
			return 0;
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public override int ReacquireContainer(string user, ContainerId containerId)
		{
			try
			{
				return base.ReacquireContainer(user, containerId);
			}
			finally
			{
				resourcesHandler.PostExecute(containerId);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override bool SignalContainer(string user, string pid, ContainerExecutor.Signal
			 signal)
		{
			VerifyUsernamePattern(user);
			string runAsUser = GetRunAsUser(user);
			string[] command = new string[] { containerExecutorExe, runAsUser, user, Sharpen.Extensions.ToString
				(LinuxContainerExecutor.Commands.SignalContainer.GetValue()), pid, Sharpen.Extensions.ToString
				(signal.GetValue()) };
			Shell.ShellCommandExecutor shExec = new Shell.ShellCommandExecutor(command);
			if (Log.IsDebugEnabled())
			{
				Log.Debug("signalContainer: " + Arrays.ToString(command));
			}
			try
			{
				shExec.Execute();
			}
			catch (Shell.ExitCodeException e)
			{
				int ret_code = shExec.GetExitCode();
				if (ret_code == LinuxContainerExecutor.ResultCode.InvalidContainerPid.GetValue())
				{
					return false;
				}
				Log.Warn("Error in signalling container " + pid + " with " + signal + "; exit = "
					 + ret_code, e);
				LogOutput(shExec.GetOutput());
				throw new IOException("Problem signalling container " + pid + " with " + signal +
					 "; output: " + shExec.GetOutput() + " and exitCode: " + ret_code, e);
			}
			return true;
		}

		public override void DeleteAsUser(string user, Path dir, params Path[] baseDirs)
		{
			VerifyUsernamePattern(user);
			string runAsUser = GetRunAsUser(user);
			string dirString = dir == null ? string.Empty : dir.ToUri().GetPath();
			IList<string> command = new AList<string>(Arrays.AsList(containerExecutorExe, runAsUser
				, user, Sharpen.Extensions.ToString(LinuxContainerExecutor.Commands.DeleteAsUser
				.GetValue()), dirString));
			IList<string> pathsToDelete = new AList<string>();
			if (baseDirs == null || baseDirs.Length == 0)
			{
				Log.Info("Deleting absolute path : " + dir);
				pathsToDelete.AddItem(dirString);
			}
			else
			{
				foreach (Path baseDir in baseDirs)
				{
					Path del = dir == null ? baseDir : new Path(baseDir, dir);
					Log.Info("Deleting path : " + del);
					pathsToDelete.AddItem(del.ToString());
					command.AddItem(baseDir.ToUri().GetPath());
				}
			}
			string[] commandArray = Sharpen.Collections.ToArray(command, new string[command.Count
				]);
			Shell.ShellCommandExecutor shExec = new Shell.ShellCommandExecutor(commandArray);
			if (Log.IsDebugEnabled())
			{
				Log.Debug("deleteAsUser: " + Arrays.ToString(commandArray));
			}
			try
			{
				shExec.Execute();
				if (Log.IsDebugEnabled())
				{
					LogOutput(shExec.GetOutput());
				}
			}
			catch (IOException e)
			{
				int exitCode = shExec.GetExitCode();
				Log.Error("DeleteAsUser for " + StringUtils.Join(" ", pathsToDelete) + " returned with exit code: "
					 + exitCode, e);
				Log.Error("Output from LinuxContainerExecutor's deleteAsUser follows:");
				LogOutput(shExec.GetOutput());
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override bool IsContainerProcessAlive(string user, string pid)
		{
			// Send a test signal to the process as the user to see if it's alive
			return SignalContainer(user, pid, ContainerExecutor.Signal.Null);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void MountCgroups(IList<string> cgroupKVs, string hierarchy)
		{
			IList<string> command = new AList<string>(Arrays.AsList(containerExecutorExe, "--mount-cgroups"
				, hierarchy));
			Sharpen.Collections.AddAll(command, cgroupKVs);
			string[] commandArray = Sharpen.Collections.ToArray(command, new string[command.Count
				]);
			Shell.ShellCommandExecutor shExec = new Shell.ShellCommandExecutor(commandArray);
			if (Log.IsDebugEnabled())
			{
				Log.Debug("mountCgroups: " + Arrays.ToString(commandArray));
			}
			try
			{
				shExec.Execute();
			}
			catch (IOException e)
			{
				int ret_code = shExec.GetExitCode();
				Log.Warn("Exception in LinuxContainerExecutor mountCgroups ", e);
				LogOutput(shExec.GetOutput());
				throw new IOException("Problem mounting cgroups " + cgroupKVs + "; exit code = " 
					+ ret_code + " and output: " + shExec.GetOutput(), e);
			}
		}
	}
}
