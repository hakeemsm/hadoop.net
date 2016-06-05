using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Text;
using Com.Google.Common.Annotations;
using Com.Google.Common.Base;
using Org.Apache.Commons.Lang.Math;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn.Api;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Factory.Providers;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Launcher;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Localizer;
using Org.Apache.Hadoop.Yarn.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager
{
	/// <summary>This executor will launch a docker container and run the task inside the container.
	/// 	</summary>
	public class DockerContainerExecutor : ContainerExecutor
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Server.Nodemanager.DockerContainerExecutor
			));

		public const string DockerContainerExecutorScript = "docker_container_executor";

		public const string DockerContainerExecutorSessionScript = "docker_container_executor_session";

		public const string DockerImagePattern = "^(([\\w\\.-]+)(:\\d+)*\\/)?[\\w\\.:-]+$";

		private readonly FileContext lfs;

		private readonly Sharpen.Pattern dockerImagePattern;

		public DockerContainerExecutor()
		{
			// This validates that the image is a proper docker image and would not crash docker.
			try
			{
				this.lfs = FileContext.GetLocalFSFileContext();
				this.dockerImagePattern = Sharpen.Pattern.Compile(DockerImagePattern);
			}
			catch (UnsupportedFileSystemException e)
			{
				throw new RuntimeException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual void CopyFile(Path src, Path dst, string owner)
		{
			lfs.Util().Copy(src, dst);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Init()
		{
			string auth = GetConf().Get(CommonConfigurationKeys.HadoopSecurityAuthentication);
			if (auth != null && !auth.Equals("simple"))
			{
				throw new InvalidOperationException("DockerContainerExecutor only works with simple authentication mode"
					);
			}
			string dockerExecutor = GetConf().Get(YarnConfiguration.NmDockerContainerExecutorExecName
				, YarnConfiguration.NmDefaultDockerContainerExecutorExecName);
			if (!new FilePath(dockerExecutor).Exists())
			{
				throw new InvalidOperationException("Invalid docker exec path: " + dockerExecutor
					);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public override void StartLocalizer(Path nmPrivateContainerTokensPath, IPEndPoint
			 nmAddr, string user, string appId, string locId, LocalDirsHandlerService dirsHandler
			)
		{
			lock (this)
			{
				IList<string> localDirs = dirsHandler.GetLocalDirs();
				IList<string> logDirs = dirsHandler.GetLogDirs();
				ContainerLocalizer localizer = new ContainerLocalizer(lfs, user, appId, locId, GetPaths
					(localDirs), RecordFactoryProvider.GetRecordFactory(GetConf()));
				CreateUserLocalDirs(localDirs, user);
				CreateUserCacheDirs(localDirs, user);
				CreateAppDirs(localDirs, user, appId);
				CreateAppLogDirs(appId, logDirs, user);
				// randomly choose the local directory
				Path appStorageDir = GetWorkingDir(localDirs, user, appId);
				string tokenFn = string.Format(ContainerLocalizer.TokenFileNameFmt, locId);
				Path tokenDst = new Path(appStorageDir, tokenFn);
				CopyFile(nmPrivateContainerTokensPath, tokenDst, user);
				Log.Info("Copying from " + nmPrivateContainerTokensPath + " to " + tokenDst);
				lfs.SetWorkingDirectory(appStorageDir);
				Log.Info("CWD set to " + appStorageDir + " = " + lfs.GetWorkingDirectory());
				// TODO: DO it over RPC for maintaining similarity?
				localizer.RunLocalization(nmAddr);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override int LaunchContainer(Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container
			 container, Path nmPrivateContainerScriptPath, Path nmPrivateTokensPath, string 
			userName, string appId, Path containerWorkDir, IList<string> localDirs, IList<string
			> logDirs)
		{
			string containerImageName = container.GetLaunchContext().GetEnvironment()[YarnConfiguration
				.NmDockerContainerExecutorImageName];
			if (Log.IsDebugEnabled())
			{
				Log.Debug("containerImageName from launchContext: " + containerImageName);
			}
			Preconditions.CheckArgument(!Strings.IsNullOrEmpty(containerImageName), "Container image must not be null"
				);
			containerImageName = containerImageName.ReplaceAll("['\"]", string.Empty);
			Preconditions.CheckArgument(SaneDockerImage(containerImageName), "Image: " + containerImageName
				 + " is not a proper docker image");
			string dockerExecutor = GetConf().Get(YarnConfiguration.NmDockerContainerExecutorExecName
				, YarnConfiguration.NmDefaultDockerContainerExecutorExecName);
			FsPermission dirPerm = new FsPermission(AppdirPerm);
			ContainerId containerId = container.GetContainerId();
			// create container dirs on all disks
			string containerIdStr = ConverterUtils.ToString(containerId);
			string appIdStr = ConverterUtils.ToString(containerId.GetApplicationAttemptId().GetApplicationId
				());
			foreach (string sLocalDir in localDirs)
			{
				Path usersdir = new Path(sLocalDir, ContainerLocalizer.Usercache);
				Path userdir = new Path(usersdir, userName);
				Path appCacheDir = new Path(userdir, ContainerLocalizer.Appcache);
				Path appDir = new Path(appCacheDir, appIdStr);
				Path containerDir = new Path(appDir, containerIdStr);
				CreateDir(containerDir, dirPerm, true, userName);
			}
			// Create the container log-dirs on all disks
			CreateContainerLogDirs(appIdStr, containerIdStr, logDirs, userName);
			Path tmpDir = new Path(containerWorkDir, YarnConfiguration.DefaultContainerTempDir
				);
			CreateDir(tmpDir, dirPerm, false, userName);
			// copy launch script to work dir
			Path launchDst = new Path(containerWorkDir, ContainerLaunch.ContainerScript);
			lfs.Util().Copy(nmPrivateContainerScriptPath, launchDst);
			// copy container tokens to work dir
			Path tokenDst = new Path(containerWorkDir, ContainerLaunch.FinalContainerTokensFile
				);
			lfs.Util().Copy(nmPrivateTokensPath, tokenDst);
			string localDirMount = ToMount(localDirs);
			string logDirMount = ToMount(logDirs);
			string containerWorkDirMount = ToMount(Sharpen.Collections.SingletonList(containerWorkDir
				.ToUri().GetPath()));
			StringBuilder commands = new StringBuilder();
			string commandStr = commands.Append(dockerExecutor).Append(" ").Append("run").Append
				(" ").Append("--rm --net=host").Append(" ").Append(" --name " + containerIdStr).
				Append(localDirMount).Append(logDirMount).Append(containerWorkDirMount).Append(" "
				).Append(containerImageName).ToString();
			string dockerPidScript = "`" + dockerExecutor + " inspect --format {{.State.Pid}} "
				 + containerIdStr + "`";
			// Create new local launch wrapper script
			DockerContainerExecutor.LocalWrapperScriptBuilder sb = new DockerContainerExecutor.UnixLocalWrapperScriptBuilder
				(this, containerWorkDir, commandStr, dockerPidScript);
			Path pidFile = GetPidFilePath(containerId);
			if (pidFile != null)
			{
				sb.WriteLocalWrapperScript(launchDst, pidFile);
			}
			else
			{
				Log.Info("Container " + containerIdStr + " was marked as inactive. Returning terminated error"
					);
				return ContainerExecutor.ExitCode.Terminated.GetExitCode();
			}
			Shell.ShellCommandExecutor shExec = null;
			try
			{
				lfs.SetPermission(launchDst, ContainerExecutor.TaskLaunchScriptPermission);
				lfs.SetPermission(sb.GetWrapperScriptPath(), ContainerExecutor.TaskLaunchScriptPermission
					);
				// Setup command to run
				string[] command = GetRunCommand(sb.GetWrapperScriptPath().ToString(), containerIdStr
					, userName, pidFile, this.GetConf());
				if (Log.IsDebugEnabled())
				{
					Log.Debug("launchContainer: " + commandStr + " " + Joiner.On(" ").Join(command));
				}
				shExec = new Shell.ShellCommandExecutor(command, new FilePath(containerWorkDir.ToUri
					().GetPath()), container.GetLaunchContext().GetEnvironment());
				// sanitized env
				if (IsContainerActive(containerId))
				{
					shExec.Execute();
				}
				else
				{
					Log.Info("Container " + containerIdStr + " was marked as inactive. Returning terminated error"
						);
					return ContainerExecutor.ExitCode.Terminated.GetExitCode();
				}
			}
			catch (IOException e)
			{
				if (null == shExec)
				{
					return -1;
				}
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
					LogOutput(shExec.GetOutput());
					string diagnostics = "Exception from container-launch: \n" + StringUtils.StringifyException
						(e) + "\n" + shExec.GetOutput();
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
				if (shExec != null)
				{
					shExec.Close();
				}
			}
			return 0;
		}

		/// <exception cref="System.IO.IOException"/>
		public override void WriteLaunchEnv(OutputStream @out, IDictionary<string, string
			> environment, IDictionary<Path, IList<string>> resources, IList<string> command
			)
		{
			ContainerLaunch.ShellScriptBuilder sb = ContainerLaunch.ShellScriptBuilder.Create
				();
			ICollection<string> exclusionSet = new HashSet<string>();
			exclusionSet.AddItem(YarnConfiguration.NmDockerContainerExecutorImageName);
			exclusionSet.AddItem(ApplicationConstants.Environment.HadoopYarnHome.ToString());
			exclusionSet.AddItem(ApplicationConstants.Environment.HadoopCommonHome.ToString()
				);
			exclusionSet.AddItem(ApplicationConstants.Environment.HadoopHdfsHome.ToString());
			exclusionSet.AddItem(ApplicationConstants.Environment.HadoopConfDir.ToString());
			exclusionSet.AddItem(ApplicationConstants.Environment.JavaHome.ToString());
			if (environment != null)
			{
				foreach (KeyValuePair<string, string> env in environment)
				{
					if (!exclusionSet.Contains(env.Key))
					{
						sb.Env(env.Key.ToString(), env.Value.ToString());
					}
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
			TextWriter ps = null;
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			try
			{
				pout = new TextWriter(@out, false, "UTF-8");
				if (Log.IsDebugEnabled())
				{
					ps = new TextWriter(baos, false, "UTF-8");
					sb.Write(ps);
				}
				sb.Write(pout);
			}
			finally
			{
				if (@out != null)
				{
					@out.Close();
				}
				if (ps != null)
				{
					ps.Close();
				}
			}
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Script: " + baos.ToString("UTF-8"));
			}
		}

		private bool SaneDockerImage(string containerImageName)
		{
			return dockerImagePattern.Matcher(containerImageName).Matches();
		}

		/// <exception cref="System.IO.IOException"/>
		public override bool SignalContainer(string user, string pid, ContainerExecutor.Signal
			 signal)
		{
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Sending signal " + signal.GetValue() + " to pid " + pid + " as user " 
					+ user);
			}
			if (!ContainerIsAlive(pid))
			{
				return false;
			}
			try
			{
				KillContainer(pid, signal);
			}
			catch (IOException e)
			{
				if (!ContainerIsAlive(pid))
				{
					return false;
				}
				throw;
			}
			return true;
		}

		/// <exception cref="System.IO.IOException"/>
		public override bool IsContainerProcessAlive(string user, string pid)
		{
			return ContainerIsAlive(pid);
		}

		/// <summary>Returns true if the process with the specified pid is alive.</summary>
		/// <param name="pid">String pid</param>
		/// <returns>boolean true if the process is alive</returns>
		/// <exception cref="System.IO.IOException"/>
		[VisibleForTesting]
		public static bool ContainerIsAlive(string pid)
		{
			try
			{
				new Shell.ShellCommandExecutor(Shell.GetCheckProcessIsAliveCommand(pid)).Execute(
					);
				// successful execution means process is alive
				return true;
			}
			catch (Shell.ExitCodeException)
			{
				// failure (non-zero exit code) means process is not alive
				return false;
			}
		}

		/// <summary>Send a specified signal to the specified pid</summary>
		/// <param name="pid">the pid of the process [group] to signal.</param>
		/// <param name="signal">
		/// signal to send
		/// (for logging).
		/// </param>
		/// <exception cref="System.IO.IOException"/>
		protected internal virtual void KillContainer(string pid, ContainerExecutor.Signal
			 signal)
		{
			new Shell.ShellCommandExecutor(Shell.GetSignalKillCommand(signal.GetValue(), pid)
				).Execute();
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public override void DeleteAsUser(string user, Path subDir, params Path[] baseDirs
			)
		{
			if (baseDirs == null || baseDirs.Length == 0)
			{
				Log.Info("Deleting absolute path : " + subDir);
				if (!lfs.Delete(subDir, true))
				{
					//Maybe retry
					Log.Warn("delete returned false for path: [" + subDir + "]");
				}
				return;
			}
			foreach (Path baseDir in baseDirs)
			{
				Path del = subDir == null ? baseDir : new Path(baseDir, subDir);
				Log.Info("Deleting path : " + del);
				try
				{
					if (!lfs.Delete(del, true))
					{
						Log.Warn("delete returned false for path: [" + del + "]");
					}
				}
				catch (FileNotFoundException)
				{
					continue;
				}
			}
		}

		/// <summary>Converts a directory list to a docker mount string</summary>
		/// <param name="dirs"/>
		/// <returns>a string of mounts for docker</returns>
		private string ToMount(IList<string> dirs)
		{
			StringBuilder builder = new StringBuilder();
			foreach (string dir in dirs)
			{
				builder.Append(" -v " + dir + ":" + dir);
			}
			return builder.ToString();
		}

		private abstract class LocalWrapperScriptBuilder
		{
			private readonly Path wrapperScriptPath;

			public virtual Path GetWrapperScriptPath()
			{
				return this.wrapperScriptPath;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void WriteLocalWrapperScript(Path launchDst, Path pidFile)
			{
				DataOutputStream @out = null;
				TextWriter pout = null;
				try
				{
					@out = this._enclosing.lfs.Create(this.wrapperScriptPath, EnumSet.Of(CreateFlag.Create
						, CreateFlag.Overwrite));
					pout = new TextWriter(@out, false, "UTF-8");
					this.WriteLocalWrapperScript(launchDst, pidFile, pout);
				}
				finally
				{
					IOUtils.Cleanup(DockerContainerExecutor.Log, pout, @out);
				}
			}

			protected internal abstract void WriteLocalWrapperScript(Path launchDst, Path pidFile
				, TextWriter pout);

			protected internal LocalWrapperScriptBuilder(DockerContainerExecutor _enclosing, 
				Path containerWorkDir)
			{
				this._enclosing = _enclosing;
				this.wrapperScriptPath = new Path(containerWorkDir, Shell.AppendScriptExtension(DockerContainerExecutor
					.DockerContainerExecutorScript));
			}

			private readonly DockerContainerExecutor _enclosing;
		}

		private sealed class UnixLocalWrapperScriptBuilder : DockerContainerExecutor.LocalWrapperScriptBuilder
		{
			private readonly Path sessionScriptPath;

			private readonly string dockerCommand;

			private readonly string dockerPidScript;

			public UnixLocalWrapperScriptBuilder(DockerContainerExecutor _enclosing, Path containerWorkDir
				, string dockerCommand, string dockerPidScript)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
				this.dockerCommand = dockerCommand;
				this.dockerPidScript = dockerPidScript;
				this.sessionScriptPath = new Path(containerWorkDir, Shell.AppendScriptExtension(DockerContainerExecutor
					.DockerContainerExecutorSessionScript));
			}

			/// <exception cref="System.IO.IOException"/>
			public override void WriteLocalWrapperScript(Path launchDst, Path pidFile)
			{
				this.WriteSessionScript(launchDst, pidFile);
				base.WriteLocalWrapperScript(launchDst, pidFile);
			}

			protected internal override void WriteLocalWrapperScript(Path launchDst, Path pidFile
				, TextWriter pout)
			{
				string exitCodeFile = ContainerLaunch.GetExitCodeFile(pidFile.ToString());
				string tmpFile = exitCodeFile + ".tmp";
				pout.WriteLine("#!/usr/bin/env bash");
				pout.WriteLine("bash \"" + this.sessionScriptPath.ToString() + "\"");
				pout.WriteLine("rc=$?");
				pout.WriteLine("echo $rc > \"" + tmpFile + "\"");
				pout.WriteLine("mv -f \"" + tmpFile + "\" \"" + exitCodeFile + "\"");
				pout.WriteLine("exit $rc");
			}

			/// <exception cref="System.IO.IOException"/>
			private void WriteSessionScript(Path launchDst, Path pidFile)
			{
				DataOutputStream @out = null;
				TextWriter pout = null;
				try
				{
					@out = this._enclosing.lfs.Create(this.sessionScriptPath, EnumSet.Of(CreateFlag.Create
						, CreateFlag.Overwrite));
					pout = new TextWriter(@out, false, "UTF-8");
					// We need to do a move as writing to a file is not atomic
					// Process reading a file being written to may get garbled data
					// hence write pid to tmp file first followed by a mv
					pout.WriteLine("#!/usr/bin/env bash");
					pout.WriteLine();
					pout.WriteLine("echo " + this.dockerPidScript + " > " + pidFile.ToString() + ".tmp"
						);
					pout.WriteLine("/bin/mv -f " + pidFile.ToString() + ".tmp " + pidFile);
					pout.WriteLine(this.dockerCommand + " bash \"" + launchDst.ToUri().GetPath().ToString
						() + "\"");
				}
				finally
				{
					IOUtils.Cleanup(DockerContainerExecutor.Log, pout, @out);
				}
				this._enclosing.lfs.SetPermission(this.sessionScriptPath, ContainerExecutor.TaskLaunchScriptPermission
					);
			}

			private readonly DockerContainerExecutor _enclosing;
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual void CreateDir(Path dirPath, FsPermission perms, bool 
			createParent, string user)
		{
			lfs.Mkdir(dirPath, perms, createParent);
			if (!perms.Equals(perms.ApplyUMask(lfs.GetUMask())))
			{
				lfs.SetPermission(dirPath, perms);
			}
		}

		/// <summary>Initialize the local directories for a particular user.</summary>
		/// <remarks>
		/// Initialize the local directories for a particular user.
		/// <ul>.mkdir
		/// <li>$local.dir/usercache/$user</li>
		/// </ul>
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		internal virtual void CreateUserLocalDirs(IList<string> localDirs, string user)
		{
			bool userDirStatus = false;
			FsPermission userperms = new FsPermission(UserPerm);
			foreach (string localDir in localDirs)
			{
				// create $local.dir/usercache/$user and its immediate parent
				try
				{
					CreateDir(GetUserCacheDir(new Path(localDir), user), userperms, true, user);
				}
				catch (IOException e)
				{
					Log.Warn("Unable to create the user directory : " + localDir, e);
					continue;
				}
				userDirStatus = true;
			}
			if (!userDirStatus)
			{
				throw new IOException("Not able to initialize user directories " + "in any of the configured local directories for user "
					 + user);
			}
		}

		/// <summary>Initialize the local cache directories for a particular user.</summary>
		/// <remarks>
		/// Initialize the local cache directories for a particular user.
		/// <ul>
		/// <li>$local.dir/usercache/$user</li>
		/// <li>$local.dir/usercache/$user/appcache</li>
		/// <li>$local.dir/usercache/$user/filecache</li>
		/// </ul>
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		internal virtual void CreateUserCacheDirs(IList<string> localDirs, string user)
		{
			Log.Info("Initializing user " + user);
			bool appcacheDirStatus = false;
			bool distributedCacheDirStatus = false;
			FsPermission appCachePerms = new FsPermission(AppcachePerm);
			FsPermission fileperms = new FsPermission(FilecachePerm);
			foreach (string localDir in localDirs)
			{
				// create $local.dir/usercache/$user/appcache
				Path localDirPath = new Path(localDir);
				Path appDir = GetAppcacheDir(localDirPath, user);
				try
				{
					CreateDir(appDir, appCachePerms, true, user);
					appcacheDirStatus = true;
				}
				catch (IOException e)
				{
					Log.Warn("Unable to create app cache directory : " + appDir, e);
				}
				// create $local.dir/usercache/$user/filecache
				Path distDir = GetFileCacheDir(localDirPath, user);
				try
				{
					CreateDir(distDir, fileperms, true, user);
					distributedCacheDirStatus = true;
				}
				catch (IOException e)
				{
					Log.Warn("Unable to create file cache directory : " + distDir, e);
				}
			}
			if (!appcacheDirStatus)
			{
				throw new IOException("Not able to initialize app-cache directories " + "in any of the configured local directories for user "
					 + user);
			}
			if (!distributedCacheDirStatus)
			{
				throw new IOException("Not able to initialize distributed-cache directories " + "in any of the configured local directories for user "
					 + user);
			}
		}

		/// <summary>Initialize the local directories for a particular user.</summary>
		/// <remarks>
		/// Initialize the local directories for a particular user.
		/// <ul>
		/// <li>$local.dir/usercache/$user/appcache/$appid</li>
		/// </ul>
		/// </remarks>
		/// <param name="localDirs"/>
		/// <exception cref="System.IO.IOException"/>
		internal virtual void CreateAppDirs(IList<string> localDirs, string user, string 
			appId)
		{
			bool initAppDirStatus = false;
			FsPermission appperms = new FsPermission(AppdirPerm);
			foreach (string localDir in localDirs)
			{
				Path fullAppDir = GetApplicationDir(new Path(localDir), user, appId);
				// create $local.dir/usercache/$user/appcache/$appId
				try
				{
					CreateDir(fullAppDir, appperms, true, user);
					initAppDirStatus = true;
				}
				catch (IOException e)
				{
					Log.Warn("Unable to create app directory " + fullAppDir.ToString(), e);
				}
			}
			if (!initAppDirStatus)
			{
				throw new IOException("Not able to initialize app directories " + "in any of the configured local directories for app "
					 + appId.ToString());
			}
		}

		/// <summary>Create application log directories on all disks.</summary>
		/// <exception cref="System.IO.IOException"/>
		internal virtual void CreateContainerLogDirs(string appId, string containerId, IList
			<string> logDirs, string user)
		{
			bool containerLogDirStatus = false;
			FsPermission containerLogDirPerms = new FsPermission(LogdirPerm);
			foreach (string rootLogDir in logDirs)
			{
				// create $log.dir/$appid/$containerid
				Path appLogDir = new Path(rootLogDir, appId);
				Path containerLogDir = new Path(appLogDir, containerId);
				try
				{
					CreateDir(containerLogDir, containerLogDirPerms, true, user);
				}
				catch (IOException e)
				{
					Log.Warn("Unable to create the container-log directory : " + appLogDir, e);
					continue;
				}
				containerLogDirStatus = true;
			}
			if (!containerLogDirStatus)
			{
				throw new IOException("Not able to initialize container-log directories " + "in any of the configured local directories for container "
					 + containerId);
			}
		}

		/// <summary>Permissions for user dir.</summary>
		/// <remarks>
		/// Permissions for user dir.
		/// $local.dir/usercache/$user
		/// </remarks>
		internal const short UserPerm = (short)0x1e8;

		/// <summary>Permissions for user appcache dir.</summary>
		/// <remarks>
		/// Permissions for user appcache dir.
		/// $local.dir/usercache/$user/appcache
		/// </remarks>
		internal const short AppcachePerm = (short)0x1c8;

		/// <summary>Permissions for user filecache dir.</summary>
		/// <remarks>
		/// Permissions for user filecache dir.
		/// $local.dir/usercache/$user/filecache
		/// </remarks>
		internal const short FilecachePerm = (short)0x1c8;

		/// <summary>Permissions for user app dir.</summary>
		/// <remarks>
		/// Permissions for user app dir.
		/// $local.dir/usercache/$user/appcache/$appId
		/// </remarks>
		internal const short AppdirPerm = (short)0x1c8;

		/// <summary>Permissions for user log dir.</summary>
		/// <remarks>
		/// Permissions for user log dir.
		/// $logdir/$user/$appId
		/// </remarks>
		internal const short LogdirPerm = (short)0x1c8;

		/// <exception cref="System.IO.IOException"/>
		private long GetDiskFreeSpace(Path @base)
		{
			return lfs.GetFsStatus(@base).GetRemaining();
		}

		private Path GetApplicationDir(Path @base, string user, string appId)
		{
			return new Path(GetAppcacheDir(@base, user), appId);
		}

		private Path GetUserCacheDir(Path @base, string user)
		{
			return new Path(new Path(@base, ContainerLocalizer.Usercache), user);
		}

		private Path GetAppcacheDir(Path @base, string user)
		{
			return new Path(GetUserCacheDir(@base, user), ContainerLocalizer.Appcache);
		}

		private Path GetFileCacheDir(Path @base, string user)
		{
			return new Path(GetUserCacheDir(@base, user), ContainerLocalizer.Filecache);
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual Path GetWorkingDir(IList<string> localDirs, string user
			, string appId)
		{
			Path appStorageDir = null;
			long totalAvailable = 0L;
			long[] availableOnDisk = new long[localDirs.Count];
			int i = 0;
			// randomly choose the app directory
			// the chance of picking a directory is proportional to
			// the available space on the directory.
			// firstly calculate the sum of all available space on these directories
			foreach (string localDir in localDirs)
			{
				Path curBase = GetApplicationDir(new Path(localDir), user, appId);
				long space = 0L;
				try
				{
					space = GetDiskFreeSpace(curBase);
				}
				catch (IOException e)
				{
					Log.Warn("Unable to get Free Space for " + curBase.ToString(), e);
				}
				availableOnDisk[i++] = space;
				totalAvailable += space;
			}
			// throw an IOException if totalAvailable is 0.
			if (totalAvailable <= 0L)
			{
				throw new IOException("Not able to find a working directory for " + user);
			}
			// make probability to pick a directory proportional to
			// the available space on the directory.
			long randomPosition = RandomUtils.NextLong() % totalAvailable;
			int dir = 0;
			// skip zero available space directory,
			// because totalAvailable is greater than 0 and randomPosition
			// is less than totalAvailable, we can find a valid directory
			// with nonzero available space.
			while (availableOnDisk[dir] == 0L)
			{
				dir++;
			}
			while (randomPosition > availableOnDisk[dir])
			{
				randomPosition -= availableOnDisk[dir++];
			}
			appStorageDir = GetApplicationDir(new Path(localDirs[dir]), user, appId);
			return appStorageDir;
		}

		/// <summary>Create application log directories on all disks.</summary>
		/// <exception cref="System.IO.IOException"/>
		internal virtual void CreateAppLogDirs(string appId, IList<string> logDirs, string
			 user)
		{
			bool appLogDirStatus = false;
			FsPermission appLogDirPerms = new FsPermission(LogdirPerm);
			foreach (string rootLogDir in logDirs)
			{
				// create $log.dir/$appid
				Path appLogDir = new Path(rootLogDir, appId);
				try
				{
					CreateDir(appLogDir, appLogDirPerms, true, user);
				}
				catch (IOException e)
				{
					Log.Warn("Unable to create the app-log directory : " + appLogDir, e);
					continue;
				}
				appLogDirStatus = true;
			}
			if (!appLogDirStatus)
			{
				throw new IOException("Not able to initialize app-log directories " + "in any of the configured local directories for app "
					 + appId);
			}
		}

		/// <returns>the list of paths of given local directories</returns>
		private static IList<Path> GetPaths(IList<string> dirs)
		{
			IList<Path> paths = new AList<Path>(dirs.Count);
			for (int i = 0; i < dirs.Count; i++)
			{
				paths.AddItem(new Path(dirs[i]));
			}
			return paths;
		}
	}
}
