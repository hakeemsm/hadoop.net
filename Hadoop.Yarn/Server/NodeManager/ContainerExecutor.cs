using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using Org.Apache.Commons.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Launcher;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager
{
	public abstract class ContainerExecutor : Configurable
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(ContainerExecutor));

		public static readonly FsPermission TaskLaunchScriptPermission = FsPermission.CreateImmutable
			((short)0x1c0);

		private Configuration conf;

		private ConcurrentMap<ContainerId, Path> pidFiles = new ConcurrentHashMap<ContainerId
			, Path>();

		private ReentrantReadWriteLock Lock = new ReentrantReadWriteLock();

		private readonly ReentrantReadWriteLock.ReadLock readLock = Lock.ReadLock();

		private readonly ReentrantReadWriteLock.WriteLock writeLock = Lock.WriteLock();

		public virtual void SetConf(Configuration conf)
		{
			this.conf = conf;
		}

		public virtual Configuration GetConf()
		{
			return conf;
		}

		/// <summary>Run the executor initialization steps.</summary>
		/// <remarks>
		/// Run the executor initialization steps.
		/// Verify that the necessary configs, permissions are in place.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public abstract void Init();

		/// <summary>
		/// On Windows the ContainerLaunch creates a temporary special jar manifest of
		/// other jars to workaround the CLASSPATH length.
		/// </summary>
		/// <remarks>
		/// On Windows the ContainerLaunch creates a temporary special jar manifest of
		/// other jars to workaround the CLASSPATH length. In a  secure cluster this
		/// jar must be localized so that the container has access to it.
		/// This function localizes on-demand the jar.
		/// </remarks>
		/// <param name="classPathJar"/>
		/// <param name="owner"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual Path LocalizeClasspathJar(Path classPathJar, Path pwd, string owner
			)
		{
			// Non-secure executor simply use the classpath created 
			// in the NM fprivate folder
			return classPathJar;
		}

		/// <summary>Prepare the environment for containers in this application to execute.</summary>
		/// <remarks>
		/// Prepare the environment for containers in this application to execute.
		/// <pre>
		/// For $x in local.dirs
		/// create $x/$user/$appId
		/// Copy $nmLocal/appTokens
		/// <literal>-&gt;</literal>
		/// $N/$user/$appId
		/// For $rsrc in private resources
		/// Copy $rsrc
		/// <literal>-&gt;</literal>
		/// $N/$user/filecache/[idef]
		/// For $rsrc in job resources
		/// Copy $rsrc
		/// <literal>-&gt;</literal>
		/// $N/$user/$appId/filecache/idef
		/// </pre>
		/// </remarks>
		/// <param name="user">user name of application owner</param>
		/// <param name="appId">id of the application</param>
		/// <param name="nmPrivateContainerTokens">path to localized credentials, rsrc by NM</param>
		/// <param name="nmAddr">RPC address to contact NM</param>
		/// <param name="dirsHandler">NM local dirs service, for nm-local-dirs and nm-log-dirs
		/// 	</param>
		/// <exception cref="System.IO.IOException">For most application init failures</exception>
		/// <exception cref="System.Exception">If application init thread is halted by NM</exception>
		public abstract void StartLocalizer(Path nmPrivateContainerTokens, IPEndPoint nmAddr
			, string user, string appId, string locId, LocalDirsHandlerService dirsHandler);

		/// <summary>Launch the container on the node.</summary>
		/// <remarks>
		/// Launch the container on the node. This is a blocking call and returns only
		/// when the container exits.
		/// </remarks>
		/// <param name="container">the container to be launched</param>
		/// <param name="nmPrivateContainerScriptPath">the path for launch script</param>
		/// <param name="nmPrivateTokensPath">the path for tokens for the container</param>
		/// <param name="user">the user of the container</param>
		/// <param name="appId">the appId of the container</param>
		/// <param name="containerWorkDir">the work dir for the container</param>
		/// <param name="localDirs">nm-local-dirs to be used for this container</param>
		/// <param name="logDirs">nm-log-dirs to be used for this container</param>
		/// <returns>the return status of the launch</returns>
		/// <exception cref="System.IO.IOException"/>
		public abstract int LaunchContainer(Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container
			 container, Path nmPrivateContainerScriptPath, Path nmPrivateTokensPath, string 
			user, string appId, Path containerWorkDir, IList<string> localDirs, IList<string
			> logDirs);

		/// <exception cref="System.IO.IOException"/>
		public abstract bool SignalContainer(string user, string pid, ContainerExecutor.Signal
			 signal);

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public abstract void DeleteAsUser(string user, Path subDir, params Path[] basedirs
			);

		/// <exception cref="System.IO.IOException"/>
		public abstract bool IsContainerProcessAlive(string user, string pid);

		/// <summary>Recover an already existing container.</summary>
		/// <remarks>
		/// Recover an already existing container. This is a blocking call and returns
		/// only when the container exits.  Note that the container must have been
		/// activated prior to this call.
		/// </remarks>
		/// <param name="user">the user of the container</param>
		/// <param name="containerId">The ID of the container to reacquire</param>
		/// <returns>The exit code of the pre-existing container</returns>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"></exception>
		public virtual int ReacquireContainer(string user, ContainerId containerId)
		{
			Path pidPath = GetPidFilePath(containerId);
			if (pidPath == null)
			{
				Log.Warn(containerId + " is not active, returning terminated error");
				return ContainerExecutor.ExitCode.Terminated.GetExitCode();
			}
			string pid = null;
			pid = ProcessIdFileReader.GetProcessId(pidPath);
			if (pid == null)
			{
				throw new IOException("Unable to determine pid for " + containerId);
			}
			Log.Info("Reacquiring " + containerId + " with pid " + pid);
			while (IsContainerProcessAlive(user, pid))
			{
				Sharpen.Thread.Sleep(1000);
			}
			// wait for exit code file to appear
			string exitCodeFile = ContainerLaunch.GetExitCodeFile(pidPath.ToString());
			FilePath file = new FilePath(exitCodeFile);
			int sleepMsec = 100;
			int msecLeft = 2000;
			while (!file.Exists() && msecLeft >= 0)
			{
				if (!IsContainerActive(containerId))
				{
					Log.Info(containerId + " was deactivated");
					return ContainerExecutor.ExitCode.Terminated.GetExitCode();
				}
				Sharpen.Thread.Sleep(sleepMsec);
				msecLeft -= sleepMsec;
			}
			if (msecLeft < 0)
			{
				throw new IOException("Timeout while waiting for exit code from " + containerId);
			}
			try
			{
				return System.Convert.ToInt32(FileUtils.ReadFileToString(file).Trim());
			}
			catch (FormatException e)
			{
				throw new IOException("Error parsing exit code from pid " + pid, e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void WriteLaunchEnv(OutputStream @out, IDictionary<string, string>
			 environment, IDictionary<Path, IList<string>> resources, IList<string> command)
		{
			ContainerLaunch.ShellScriptBuilder sb = ContainerLaunch.ShellScriptBuilder.Create
				();
			if (environment != null)
			{
				foreach (KeyValuePair<string, string> env in environment)
				{
					sb.Env(env.Key.ToString(), env.Value.ToString());
				}
			}
			if (resources != null)
			{
				foreach (KeyValuePair<Path, IList<string>> entry in resources)
				{
					foreach (string linkName in entry.Value)
					{
						sb.Symlink(entry.Key, new Path(linkName));
					}
				}
			}
			sb.Command(command);
			TextWriter pout = null;
			try
			{
				pout = new TextWriter(@out, false, "UTF-8");
				sb.Write(pout);
			}
			finally
			{
				if (@out != null)
				{
					@out.Close();
				}
			}
		}

		[System.Serializable]
		public sealed class ExitCode
		{
			public static readonly ContainerExecutor.ExitCode ForceKilled = new ContainerExecutor.ExitCode
				(137);

			public static readonly ContainerExecutor.ExitCode Terminated = new ContainerExecutor.ExitCode
				(143);

			public static readonly ContainerExecutor.ExitCode Lost = new ContainerExecutor.ExitCode
				(154);

			private readonly int code;

			private ExitCode(int exitCode)
			{
				this.code = exitCode;
			}

			public int GetExitCode()
			{
				return ContainerExecutor.ExitCode.code;
			}

			public override string ToString()
			{
				return ContainerExecutor.ExitCode.code.ToString();
			}
		}

		/// <summary>The constants for the signals.</summary>
		[System.Serializable]
		public sealed class Signal
		{
			public static readonly ContainerExecutor.Signal Null = new ContainerExecutor.Signal
				(0, "NULL");

			public static readonly ContainerExecutor.Signal Quit = new ContainerExecutor.Signal
				(3, "SIGQUIT");

			public static readonly ContainerExecutor.Signal Kill = new ContainerExecutor.Signal
				(9, "SIGKILL");

			public static readonly ContainerExecutor.Signal Term = new ContainerExecutor.Signal
				(15, "SIGTERM");

			private readonly int value;

			private readonly string str;

			private Signal(int value, string str)
			{
				this.str = str;
				this.value = value;
			}

			public int GetValue()
			{
				return ContainerExecutor.Signal.value;
			}

			public override string ToString()
			{
				return ContainerExecutor.Signal.str;
			}
		}

		protected internal virtual void LogOutput(string output)
		{
			string shExecOutput = output;
			if (shExecOutput != null)
			{
				foreach (string str in shExecOutput.Split("\n"))
				{
					Log.Info(str);
				}
			}
		}

		/// <summary>Get the pidFile of the container.</summary>
		/// <param name="containerId"/>
		/// <returns>the path of the pid-file for the given containerId.</returns>
		protected internal virtual Path GetPidFilePath(ContainerId containerId)
		{
			try
			{
				readLock.Lock();
				return (this.pidFiles[containerId]);
			}
			finally
			{
				readLock.Unlock();
			}
		}

		protected internal virtual string[] GetRunCommand(string command, string groupId, 
			string userName, Path pidFile, Configuration conf)
		{
			return GetRunCommand(command, groupId, userName, pidFile, conf, null);
		}

		/// <summary>Return a command to execute the given command in OS shell.</summary>
		/// <remarks>
		/// Return a command to execute the given command in OS shell.
		/// On Windows, the passed in groupId can be used to launch
		/// and associate the given groupId in a process group. On
		/// non-Windows, groupId is ignored.
		/// </remarks>
		protected internal virtual string[] GetRunCommand(string command, string groupId, 
			string userName, Path pidFile, Configuration conf, Resource resource)
		{
			bool containerSchedPriorityIsSet = false;
			int containerSchedPriorityAdjustment = YarnConfiguration.DefaultNmContainerExecutorSchedPriority;
			if (conf.Get(YarnConfiguration.NmContainerExecutorSchedPriority) != null)
			{
				containerSchedPriorityIsSet = true;
				containerSchedPriorityAdjustment = conf.GetInt(YarnConfiguration.NmContainerExecutorSchedPriority
					, YarnConfiguration.DefaultNmContainerExecutorSchedPriority);
			}
			if (Shell.Windows)
			{
				int cpuRate = -1;
				int memory = -1;
				if (resource != null)
				{
					if (conf.GetBoolean(YarnConfiguration.NmWindowsContainerMemoryLimitEnabled, YarnConfiguration
						.DefaultNmWindowsContainerMemoryLimitEnabled))
					{
						memory = resource.GetMemory();
					}
					if (conf.GetBoolean(YarnConfiguration.NmWindowsContainerCpuLimitEnabled, YarnConfiguration
						.DefaultNmWindowsContainerCpuLimitEnabled))
					{
						int containerVCores = resource.GetVirtualCores();
						int nodeVCores = conf.GetInt(YarnConfiguration.NmVcores, YarnConfiguration.DefaultNmVcores
							);
						// cap overall usage to the number of cores allocated to YARN
						int nodeCpuPercentage = Math.Min(conf.GetInt(YarnConfiguration.NmResourcePercentagePhysicalCpuLimit
							, YarnConfiguration.DefaultNmResourcePercentagePhysicalCpuLimit), 100);
						nodeCpuPercentage = Math.Max(0, nodeCpuPercentage);
						if (nodeCpuPercentage == 0)
						{
							string message = "Illegal value for " + YarnConfiguration.NmResourcePercentagePhysicalCpuLimit
								 + ". Value cannot be less than or equal to 0.";
							throw new ArgumentException(message);
						}
						float yarnVCores = (nodeCpuPercentage * nodeVCores) / 100.0f;
						// CPU should be set to a percentage * 100, e.g. 20% cpu rate limit
						// should be set as 20 * 100. The following setting is equal to:
						// 100 * (100 * (vcores / Total # of cores allocated to YARN))
						cpuRate = Math.Min(10000, (int)((containerVCores * 10000) / yarnVCores));
					}
				}
				return new string[] { Shell.Winutils, "task", "create", "-m", memory.ToString(), 
					"-c", cpuRate.ToString(), groupId, "cmd /c " + command };
			}
			else
			{
				IList<string> retCommand = new AList<string>();
				if (containerSchedPriorityIsSet)
				{
					Sharpen.Collections.AddAll(retCommand, Arrays.AsList("nice", "-n", Sharpen.Extensions.ToString
						(containerSchedPriorityAdjustment)));
				}
				Sharpen.Collections.AddAll(retCommand, Arrays.AsList("bash", command));
				return Sharpen.Collections.ToArray(retCommand, new string[retCommand.Count]);
			}
		}

		/// <summary>Is the container still active?</summary>
		/// <param name="containerId"/>
		/// <returns>true if the container is active else false.</returns>
		protected internal virtual bool IsContainerActive(ContainerId containerId)
		{
			try
			{
				readLock.Lock();
				return (this.pidFiles.Contains(containerId));
			}
			finally
			{
				readLock.Unlock();
			}
		}

		/// <summary>Mark the container as active</summary>
		/// <param name="containerId">the ContainerId</param>
		/// <param name="pidFilePath">
		/// Path where the executor should write the pid of the launched
		/// process
		/// </param>
		public virtual void ActivateContainer(ContainerId containerId, Path pidFilePath)
		{
			try
			{
				writeLock.Lock();
				this.pidFiles[containerId] = pidFilePath;
			}
			finally
			{
				writeLock.Unlock();
			}
		}

		/// <summary>Mark the container as inactive.</summary>
		/// <remarks>
		/// Mark the container as inactive.
		/// Done iff the container is still active. Else treat it as
		/// a no-op
		/// </remarks>
		public virtual void DeactivateContainer(ContainerId containerId)
		{
			try
			{
				writeLock.Lock();
				Sharpen.Collections.Remove(this.pidFiles, containerId);
			}
			finally
			{
				writeLock.Unlock();
			}
		}

		/// <summary>Get the process-identifier for the container</summary>
		/// <param name="containerID"/>
		/// <returns>
		/// the processid of the container if it has already launched,
		/// otherwise return null
		/// </returns>
		public virtual string GetProcessId(ContainerId containerID)
		{
			string pid = null;
			Path pidFile = pidFiles[containerID];
			if (pidFile == null)
			{
				// This container isn't even launched yet.
				return pid;
			}
			try
			{
				pid = ProcessIdFileReader.GetProcessId(pidFile);
			}
			catch (IOException e)
			{
				Log.Error("Got exception reading pid from pid-file " + pidFile, e);
			}
			return pid;
		}

		public class DelayedProcessKiller : Sharpen.Thread
		{
			private Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container
				 container;

			private readonly string user;

			private readonly string pid;

			private readonly long delay;

			private readonly ContainerExecutor.Signal signal;

			private readonly ContainerExecutor containerExecutor;

			public DelayedProcessKiller(Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container
				 container, string user, string pid, long delay, ContainerExecutor.Signal signal
				, ContainerExecutor containerExecutor)
			{
				this.container = container;
				this.user = user;
				this.pid = pid;
				this.delay = delay;
				this.signal = signal;
				this.containerExecutor = containerExecutor;
				SetName("Task killer for " + pid);
				SetDaemon(false);
			}

			public override void Run()
			{
				try
				{
					Sharpen.Thread.Sleep(delay);
					containerExecutor.SignalContainer(user, pid, signal);
				}
				catch (Exception)
				{
					return;
				}
				catch (IOException e)
				{
					string message = "Exception when user " + user + " killing task " + pid + " in DelayedProcessKiller: "
						 + StringUtils.StringifyException(e);
					Log.Warn(message);
					container.Handle(new ContainerDiagnosticsUpdateEvent(container.GetContainerId(), 
						message));
				}
			}
		}
	}
}
