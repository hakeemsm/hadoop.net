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
	public class DefaultContainerExecutor : ContainerExecutor
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Server.Nodemanager.DefaultContainerExecutor
			));

		private const int WinMaxPath = 260;

		protected internal readonly FileContext lfs;

		public DefaultContainerExecutor()
		{
			try
			{
				this.lfs = FileContext.GetLocalFSFileContext();
			}
			catch (UnsupportedFileSystemException e)
			{
				throw new RuntimeException(e);
			}
		}

		internal DefaultContainerExecutor(FileContext lfs)
		{
			this.lfs = lfs;
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual void CopyFile(Path src, Path dst, string owner)
		{
			lfs.Util().Copy(src, dst);
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual void SetScriptExecutable(Path script, string owner)
		{
			lfs.SetPermission(script, ContainerExecutor.TaskLaunchScriptPermission);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Init()
		{
		}

		// nothing to do or verify here
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public override void StartLocalizer(Path nmPrivateContainerTokensPath, IPEndPoint
			 nmAddr, string user, string appId, string locId, LocalDirsHandlerService dirsHandler
			)
		{
			IList<string> localDirs = dirsHandler.GetLocalDirs();
			IList<string> logDirs = dirsHandler.GetLogDirs();
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
			FileContext localizerFc = FileContext.GetFileContext(lfs.GetDefaultFileSystem(), 
				GetConf());
			localizerFc.SetUMask(lfs.GetUMask());
			localizerFc.SetWorkingDirectory(appStorageDir);
			Log.Info("Localizer CWD set to " + appStorageDir + " = " + localizerFc.GetWorkingDirectory
				());
			ContainerLocalizer localizer = new ContainerLocalizer(localizerFc, user, appId, locId
				, GetPaths(localDirs), RecordFactoryProvider.GetRecordFactory(GetConf()));
			// TODO: DO it over RPC for maintaining similarity?
			localizer.RunLocalization(nmAddr);
		}

		/// <exception cref="System.IO.IOException"/>
		public override int LaunchContainer(Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Container.Container
			 container, Path nmPrivateContainerScriptPath, Path nmPrivateTokensPath, string 
			user, string appId, Path containerWorkDir, IList<string> localDirs, IList<string
			> logDirs)
		{
			FsPermission dirPerm = new FsPermission(AppdirPerm);
			ContainerId containerId = container.GetContainerId();
			// create container dirs on all disks
			string containerIdStr = ConverterUtils.ToString(containerId);
			string appIdStr = ConverterUtils.ToString(containerId.GetApplicationAttemptId().GetApplicationId
				());
			foreach (string sLocalDir in localDirs)
			{
				Path usersdir = new Path(sLocalDir, ContainerLocalizer.Usercache);
				Path userdir = new Path(usersdir, user);
				Path appCacheDir = new Path(userdir, ContainerLocalizer.Appcache);
				Path appDir = new Path(appCacheDir, appIdStr);
				Path containerDir = new Path(appDir, containerIdStr);
				CreateDir(containerDir, dirPerm, true, user);
			}
			// Create the container log-dirs on all disks
			CreateContainerLogDirs(appIdStr, containerIdStr, logDirs, user);
			Path tmpDir = new Path(containerWorkDir, YarnConfiguration.DefaultContainerTempDir
				);
			CreateDir(tmpDir, dirPerm, false, user);
			// copy container tokens to work dir
			Path tokenDst = new Path(containerWorkDir, ContainerLaunch.FinalContainerTokensFile
				);
			CopyFile(nmPrivateTokensPath, tokenDst, user);
			// copy launch script to work dir
			Path launchDst = new Path(containerWorkDir, ContainerLaunch.ContainerScript);
			CopyFile(nmPrivateContainerScriptPath, launchDst, user);
			// Create new local launch wrapper script
			DefaultContainerExecutor.LocalWrapperScriptBuilder sb = GetLocalWrapperScriptBuilder
				(containerIdStr, containerWorkDir);
			// Fail fast if attempting to launch the wrapper script would fail due to
			// Windows path length limitation.
			if (Shell.Windows && sb.GetWrapperScriptPath().ToString().Length > WinMaxPath)
			{
				throw new IOException(string.Format("Cannot launch container using script at path %s, because it exceeds "
					 + "the maximum supported path length of %d characters.  Consider " + "configuring shorter directories in %s."
					, sb.GetWrapperScriptPath(), WinMaxPath, YarnConfiguration.NmLocalDirs));
			}
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
			// create log dir under app
			// fork script
			Shell.CommandExecutor shExec = null;
			try
			{
				SetScriptExecutable(launchDst, user);
				SetScriptExecutable(sb.GetWrapperScriptPath(), user);
				shExec = BuildCommandExecutor(sb.GetWrapperScriptPath().ToString(), containerIdStr
					, user, pidFile, container.GetResource(), new FilePath(containerWorkDir.ToUri().
					GetPath()), container.GetLaunchContext().GetEnvironment());
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
				if (shExec != null)
				{
					shExec.Close();
				}
			}
			return 0;
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual Shell.CommandExecutor BuildCommandExecutor(string wrapperScriptPath
			, string containerIdStr, string user, Path pidFile, Resource resource, FilePath 
			wordDir, IDictionary<string, string> environment)
		{
			string[] command = GetRunCommand(wrapperScriptPath, containerIdStr, user, pidFile
				, this.GetConf(), resource);
			Log.Info("launchContainer: " + Arrays.ToString(command));
			return new Shell.ShellCommandExecutor(command, wordDir, environment);
		}

		protected internal virtual DefaultContainerExecutor.LocalWrapperScriptBuilder GetLocalWrapperScriptBuilder
			(string containerIdStr, Path containerWorkDir)
		{
			return Shell.Windows ? new DefaultContainerExecutor.WindowsLocalWrapperScriptBuilder
				(this, containerIdStr, containerWorkDir) : new DefaultContainerExecutor.UnixLocalWrapperScriptBuilder
				(this, containerWorkDir);
		}

		protected internal abstract class LocalWrapperScriptBuilder
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
					IOUtils.Cleanup(DefaultContainerExecutor.Log, pout, @out);
				}
			}

			protected internal abstract void WriteLocalWrapperScript(Path launchDst, Path pidFile
				, TextWriter pout);

			protected internal LocalWrapperScriptBuilder(DefaultContainerExecutor _enclosing, 
				Path containerWorkDir)
			{
				this._enclosing = _enclosing;
				this.wrapperScriptPath = new Path(containerWorkDir, Shell.AppendScriptExtension("default_container_executor"
					));
			}

			private readonly DefaultContainerExecutor _enclosing;
		}

		private sealed class UnixLocalWrapperScriptBuilder : DefaultContainerExecutor.LocalWrapperScriptBuilder
		{
			private readonly Path sessionScriptPath;

			public UnixLocalWrapperScriptBuilder(DefaultContainerExecutor _enclosing, Path containerWorkDir
				)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
				this.sessionScriptPath = new Path(containerWorkDir, Shell.AppendScriptExtension("default_container_executor_session"
					));
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
				pout.WriteLine("#!/bin/bash");
				pout.WriteLine("/bin/bash \"" + this.sessionScriptPath.ToString() + "\"");
				pout.WriteLine("rc=$?");
				pout.WriteLine("echo $rc > \"" + tmpFile + "\"");
				pout.WriteLine("/bin/mv -f \"" + tmpFile + "\" \"" + exitCodeFile + "\"");
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
					pout.WriteLine("#!/bin/bash");
					pout.WriteLine();
					pout.WriteLine("echo $$ > " + pidFile.ToString() + ".tmp");
					pout.WriteLine("/bin/mv -f " + pidFile.ToString() + ".tmp " + pidFile);
					string exec = Shell.isSetsidAvailable ? "exec setsid" : "exec";
					pout.WriteLine(exec + " /bin/bash \"" + launchDst.ToUri().GetPath().ToString() + 
						"\"");
				}
				finally
				{
					IOUtils.Cleanup(DefaultContainerExecutor.Log, pout, @out);
				}
				this._enclosing.lfs.SetPermission(this.sessionScriptPath, ContainerExecutor.TaskLaunchScriptPermission
					);
			}

			private readonly DefaultContainerExecutor _enclosing;
		}

		private sealed class WindowsLocalWrapperScriptBuilder : DefaultContainerExecutor.LocalWrapperScriptBuilder
		{
			private readonly string containerIdStr;

			public WindowsLocalWrapperScriptBuilder(DefaultContainerExecutor _enclosing, string
				 containerIdStr, Path containerWorkDir)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
				this.containerIdStr = containerIdStr;
			}

			protected internal override void WriteLocalWrapperScript(Path launchDst, Path pidFile
				, TextWriter pout)
			{
				// TODO: exit code script for Windows
				// On Windows, the pid is the container ID, so that it can also serve as
				// the name of the job object created by winutils for task management.
				// Write to temp file followed by atomic move.
				string normalizedPidFile = new FilePath(pidFile.ToString()).GetPath();
				pout.WriteLine("@echo " + this.containerIdStr + " > " + normalizedPidFile + ".tmp"
					);
				pout.WriteLine("@move /Y " + normalizedPidFile + ".tmp " + normalizedPidFile);
				pout.WriteLine("@call " + launchDst.ToString());
			}

			private readonly DefaultContainerExecutor _enclosing;
		}

		/// <exception cref="System.IO.IOException"/>
		public override bool SignalContainer(string user, string pid, ContainerExecutor.Signal
			 signal)
		{
			Log.Debug("Sending signal " + signal.GetValue() + " to pid " + pid + " as user " 
				+ user);
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
		/// <param name="localDirs"></param>
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
