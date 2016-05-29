using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Text;
using Org.Apache.Commons.Lang;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.IO.Nativeio;
using Org.Apache.Hadoop.Util;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Localizer;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager
{
	/// <summary>Windows secure container executor (WSCE).</summary>
	/// <remarks>
	/// Windows secure container executor (WSCE).
	/// This class offers a secure container executor on Windows, similar to the
	/// LinuxContainerExecutor. As the NM does not run on a high privileged context,
	/// this class delegates elevated operations to the helper hadoopwintuilsvc,
	/// implemented by the winutils.exe running as a service.
	/// JNI and LRPC is used to communicate with the privileged service.
	/// </remarks>
	public class WindowsSecureContainerExecutor : DefaultContainerExecutor
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Server.Nodemanager.WindowsSecureContainerExecutor
			));

		public const string LocalizerPidFormat = "STAR_LOCALIZER_%s";

		/// <summary>This class is a container for the JNI Win32 native methods used by WSCE.
		/// 	</summary>
		private class Native
		{
			private static bool nativeLoaded = false;

			static Native()
			{
				if (NativeCodeLoader.IsNativeCodeLoaded())
				{
					try
					{
						InitWsceNative();
						nativeLoaded = true;
					}
					catch (Exception t)
					{
						Log.Info("Unable to initialize WSCE Native libraries", t);
					}
				}
			}

			/// <summary>Initialize the JNI method ID and class ID cache</summary>
			private static void InitWsceNative()
			{
			}

			/// <summary>
			/// This class contains methods used by the WindowsSecureContainerExecutor
			/// file system operations.
			/// </summary>
			public class Elevated
			{
				private const int MoveFile = 1;

				private const int CopyFile = 2;

				/// <exception cref="System.IO.IOException"/>
				public static void Mkdir(Path dirName)
				{
					if (!nativeLoaded)
					{
						throw new IOException("Native WSCE libraries are required for mkdir");
					}
					ElevatedMkDirImpl(dirName.ToString());
				}

				/// <exception cref="System.IO.IOException"/>
				private static void ElevatedMkDirImpl(string dirName)
				{
				}

				/// <exception cref="System.IO.IOException"/>
				public static void Chown(Path fileName, string user, string group)
				{
					if (!nativeLoaded)
					{
						throw new IOException("Native WSCE libraries are required for chown");
					}
					ElevatedChownImpl(fileName.ToString(), user, group);
				}

				/// <exception cref="System.IO.IOException"/>
				private static void ElevatedChownImpl(string fileName, string user, string group)
				{
				}

				/// <exception cref="System.IO.IOException"/>
				public static void Move(Path src, Path dst, bool replaceExisting)
				{
					if (!nativeLoaded)
					{
						throw new IOException("Native WSCE libraries are required for move");
					}
					ElevatedCopyImpl(MoveFile, src.ToString(), dst.ToString(), replaceExisting);
				}

				/// <exception cref="System.IO.IOException"/>
				public static void Copy(Path src, Path dst, bool replaceExisting)
				{
					if (!nativeLoaded)
					{
						throw new IOException("Native WSCE libraries are required for copy");
					}
					ElevatedCopyImpl(CopyFile, src.ToString(), dst.ToString(), replaceExisting);
				}

				/// <exception cref="System.IO.IOException"/>
				private static void ElevatedCopyImpl(int operation, string src, string dst, bool 
					replaceExisting)
				{
				}

				/// <exception cref="System.IO.IOException"/>
				public static void Chmod(Path fileName, int mode)
				{
					if (!nativeLoaded)
					{
						throw new IOException("Native WSCE libraries are required for chmod");
					}
					ElevatedChmodImpl(fileName.ToString(), mode);
				}

				/// <exception cref="System.IO.IOException"/>
				private static void ElevatedChmodImpl(string path, int mode)
				{
				}

				/// <exception cref="System.IO.IOException"/>
				public static void KillTask(string containerName)
				{
					if (!nativeLoaded)
					{
						throw new IOException("Native WSCE libraries are required for killTask");
					}
					ElevatedKillTaskImpl(containerName);
				}

				/// <exception cref="System.IO.IOException"/>
				private static void ElevatedKillTaskImpl(string containerName)
				{
				}

				/// <exception cref="System.IO.IOException"/>
				public static OutputStream Create(Path f, bool append)
				{
					if (!nativeLoaded)
					{
						throw new IOException("Native WSCE libraries are required for create");
					}
					long desiredAccess = NativeIO.Windows.GenericWrite;
					long shareMode = 0L;
					long creationDisposition = append ? NativeIO.Windows.OpenAlways : NativeIO.Windows
						.CreateAlways;
					long flags = NativeIO.Windows.FileAttributeNormal;
					string fileName = f.ToString();
					fileName = fileName.Replace('/', '\\');
					long hFile = ElevatedCreateImpl(fileName, desiredAccess, shareMode, creationDisposition
						, flags);
					return new FileOutputStream(WindowsSecureContainerExecutor.Native.WinutilsProcessStub
						.GetFileDescriptorFromHandle(hFile));
				}

				/// <exception cref="System.IO.IOException"/>
				private static long ElevatedCreateImpl(string path, long desiredAccess, long shareMode
					, long creationDisposition, long flags)
				{
				}

				/// <exception cref="System.IO.IOException"/>
				public static bool DeleteFile(Path path)
				{
					if (!nativeLoaded)
					{
						throw new IOException("Native WSCE libraries are required for deleteFile");
					}
					return ElevatedDeletePathImpl(path.ToString(), false);
				}

				/// <exception cref="System.IO.IOException"/>
				public static bool DeleteDirectory(Path path)
				{
					if (!nativeLoaded)
					{
						throw new IOException("Native WSCE libraries are required for deleteDirectory");
					}
					return ElevatedDeletePathImpl(path.ToString(), true);
				}

				/// <exception cref="System.IO.IOException"/>
				public static bool ElevatedDeletePathImpl(string path, bool isDir)
				{
				}
			}

			/// <summary>Wraps a process started by the winutils service helper.</summary>
			public class WinutilsProcessStub : SystemProcess
			{
				private readonly long hProcess;

				private readonly long hThread;

				private bool disposed = false;

				private readonly InputStream stdErr;

				private readonly InputStream stdOut;

				private readonly OutputStream stdIn;

				public WinutilsProcessStub(long hProcess, long hThread, long hStdIn, long hStdOut
					, long hStdErr)
				{
					this.hProcess = hProcess;
					this.hThread = hThread;
					this.stdIn = new FileOutputStream(GetFileDescriptorFromHandle(hStdIn));
					this.stdOut = new FileInputStream(GetFileDescriptorFromHandle(hStdOut));
					this.stdErr = new FileInputStream(GetFileDescriptorFromHandle(hStdErr));
				}

				public static FileDescriptor GetFileDescriptorFromHandle(long handle)
				{
				}

				public override void Destroy()
				{
				}

				public override int ExitValue()
				{
				}

				public override InputStream GetErrorStream()
				{
					return stdErr;
				}

				public override InputStream GetInputStream()
				{
					return stdOut;
				}

				public override OutputStream GetOutputStream()
				{
					return stdIn;
				}

				/// <exception cref="System.Exception"/>
				public override int WaitFor()
				{
				}

				public virtual void Dispose()
				{
					lock (this)
					{
					}
				}

				/// <exception cref="Org.Apache.Hadoop.IO.Nativeio.NativeIOException"/>
				public virtual void Resume()
				{
				}
			}

			/// <exception cref="System.IO.IOException"/>
			public static WindowsSecureContainerExecutor.Native.WinutilsProcessStub CreateTaskAsUser
				(string cwd, string jobName, string user, string pidFile, string cmdLine)
			{
				lock (typeof(Native))
				{
					if (!nativeLoaded)
					{
						throw new IOException("Native WSCE  libraries are required for createTaskAsUser");
					}
					lock (Shell.WindowsProcessLaunchLock)
					{
						return CreateTaskAsUser0(cwd, jobName, user, pidFile, cmdLine);
					}
				}
			}

			/// <exception cref="Org.Apache.Hadoop.IO.Nativeio.NativeIOException"/>
			private static WindowsSecureContainerExecutor.Native.WinutilsProcessStub CreateTaskAsUser0
				(string cwd, string jobName, string user, string pidFile, string cmdLine)
			{
			}
		}

		/// <summary>A shell script wrapper builder for WSCE.</summary>
		/// <remarks>
		/// A shell script wrapper builder for WSCE.
		/// Overwrites the default behavior to remove the creation of the PID file in
		/// the script wrapper. WSCE creates the pid file as part of launching the
		/// task in winutils.
		/// </remarks>
		private class WindowsSecureWrapperScriptBuilder : DefaultContainerExecutor.LocalWrapperScriptBuilder
		{
			public WindowsSecureWrapperScriptBuilder(WindowsSecureContainerExecutor _enclosing
				, Path containerWorkDir)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
			}

			protected internal override void WriteLocalWrapperScript(Path launchDst, Path pidFile
				, TextWriter pout)
			{
				pout.Format("@call \"%s\"", launchDst);
			}

			private readonly WindowsSecureContainerExecutor _enclosing;
		}

		/// <summary>This is a skeleton file system used to elevate certain operations.</summary>
		/// <remarks>
		/// This is a skeleton file system used to elevate certain operations.
		/// WSCE has to create container dirs under local/userchache/$user but
		/// this dir itself is owned by $user, with chmod 750. As ther NM has no
		/// write access, it must delegate the write operations to the privileged
		/// hadoopwintuilsvc.
		/// </remarks>
		private class ElevatedFileSystem : DelegateToFileSystem
		{
			/// <summary>
			/// This overwrites certain RawLocalSystem operations to be performed by a
			/// privileged process.
			/// </summary>
			private class ElevatedRawLocalFilesystem : RawLocalFileSystem
			{
				/// <exception cref="System.IO.IOException"/>
				protected override bool MkOneDirWithMode(Path path, FilePath p2f, FsPermission permission
					)
				{
					if (Log.IsDebugEnabled())
					{
						Log.Debug(string.Format("EFS:mkOneDirWithMode: %s %s", path, permission));
					}
					bool ret = false;
					// File.mkdir returns false, does not throw. Must mimic it.
					try
					{
						WindowsSecureContainerExecutor.Native.Elevated.Mkdir(path);
						SetPermission(path, permission);
						ret = true;
					}
					catch (Exception e)
					{
						if (Log.IsDebugEnabled())
						{
							Log.Debug(string.Format("EFS:mkOneDirWithMode: %s", StringUtils.StringifyException
								(e)));
						}
					}
					return ret;
				}

				/// <exception cref="System.IO.IOException"/>
				public override void SetPermission(Path p, FsPermission permission)
				{
					if (Log.IsDebugEnabled())
					{
						Log.Debug(string.Format("EFS:setPermission: %s %s", p, permission));
					}
					WindowsSecureContainerExecutor.Native.Elevated.Chmod(p, permission.ToShort());
				}

				/// <exception cref="System.IO.IOException"/>
				public override void SetOwner(Path p, string username, string groupname)
				{
					if (Log.IsDebugEnabled())
					{
						Log.Debug(string.Format("EFS:setOwner: %s %s %s", p, username, groupname));
					}
					WindowsSecureContainerExecutor.Native.Elevated.Chown(p, username, groupname);
				}

				/// <exception cref="System.IO.IOException"/>
				protected override OutputStream CreateOutputStreamWithMode(Path f, bool append, FsPermission
					 permission)
				{
					if (Log.IsDebugEnabled())
					{
						Log.Debug(string.Format("EFS:createOutputStreamWithMode: %s %b %s", f, append, permission
							));
					}
					bool success = false;
					OutputStream os = WindowsSecureContainerExecutor.Native.Elevated.Create(f, append
						);
					try
					{
						SetPermission(f, permission);
						success = true;
						return os;
					}
					finally
					{
						if (!success)
						{
							IOUtils.Cleanup(Log, os);
						}
					}
				}

				/// <exception cref="System.IO.IOException"/>
				public override bool Delete(Path p, bool recursive)
				{
					if (Log.IsDebugEnabled())
					{
						Log.Debug(string.Format("EFS:delete: %s %b", p, recursive));
					}
					// The super delete uses the FileUtil.fullyDelete, 
					// but we cannot rely on that because we need to use the elevated 
					// operations to remove the files
					//
					FilePath f = PathToFile(p);
					if (!f.Exists())
					{
						//no path, return false "nothing to delete"
						return false;
					}
					else
					{
						if (f.IsFile())
						{
							return WindowsSecureContainerExecutor.Native.Elevated.DeleteFile(p);
						}
						else
						{
							if (f.IsDirectory())
							{
								// This is a best-effort attempt. There are race conditions in that
								// child files can be created/deleted after we snapped the list. 
								// No need to protect against that case.
								FilePath[] files = FileUtil.ListFiles(f);
								int childCount = files.Length;
								if (recursive)
								{
									foreach (FilePath child in files)
									{
										if (Delete(new Path(child.GetPath()), recursive))
										{
											--childCount;
										}
									}
								}
								if (childCount == 0)
								{
									return WindowsSecureContainerExecutor.Native.Elevated.DeleteDirectory(p);
								}
								else
								{
									throw new IOException("Directory " + f.ToString() + " is not empty");
								}
							}
							else
							{
								// This can happen under race conditions if an external agent 
								// is messing with the file type between IFs
								throw new IOException("Path " + f.ToString() + " exists, but is neither a file nor a directory"
									);
							}
						}
					}
				}
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="Sharpen.URISyntaxException"/>
			protected internal ElevatedFileSystem()
				: base(FsConstants.LocalFsUri, new WindowsSecureContainerExecutor.ElevatedFileSystem.ElevatedRawLocalFilesystem
					(), new Configuration(), FsConstants.LocalFsUri.GetScheme(), false)
			{
			}
		}

		private class WintuilsProcessStubExecutor : Shell.CommandExecutor
		{
			private WindowsSecureContainerExecutor.Native.WinutilsProcessStub processStub;

			private StringBuilder output = new StringBuilder();

			private int exitCode;

			private enum State
			{
				Init,
				Running,
				Complete
			}

			private WindowsSecureContainerExecutor.WintuilsProcessStubExecutor.State state;

			private readonly string cwd;

			private readonly string jobName;

			private readonly string userName;

			private readonly string pidFile;

			private readonly string cmdLine;

			public WintuilsProcessStubExecutor(string cwd, string jobName, string userName, string
				 pidFile, string cmdLine)
			{
				this.cwd = cwd;
				this.jobName = jobName;
				this.userName = userName;
				this.pidFile = pidFile;
				this.cmdLine = cmdLine;
				this.state = WindowsSecureContainerExecutor.WintuilsProcessStubExecutor.State.Init;
			}

			/// <exception cref="System.IO.IOException"/>
			private void AssertComplete()
			{
				if (state != WindowsSecureContainerExecutor.WintuilsProcessStubExecutor.State.Complete)
				{
					throw new IOException("Process is not complete");
				}
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual string GetOutput()
			{
				AssertComplete();
				return output.ToString();
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual int GetExitCode()
			{
				AssertComplete();
				return exitCode;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void ValidateResult()
			{
				AssertComplete();
				if (0 != exitCode)
				{
					Log.Warn(output.ToString());
					throw new IOException("Processs exit code is:" + exitCode);
				}
			}

			/// <exception cref="System.IO.IOException"/>
			private Sharpen.Thread StartStreamReader(InputStream stream)
			{
				Sharpen.Thread streamReaderThread = new _Thread_499(this, stream);
				streamReaderThread.Start();
				return streamReaderThread;
			}

			private sealed class _Thread_499 : Sharpen.Thread
			{
				public _Thread_499(WintuilsProcessStubExecutor _enclosing, InputStream stream)
				{
					this._enclosing = _enclosing;
					this.stream = stream;
				}

				public override void Run()
				{
					try
					{
						BufferedReader lines = new BufferedReader(new InputStreamReader(stream, Sharpen.Extensions.GetEncoding
							("UTF-8")));
						char[] buf = new char[512];
						int nRead;
						while ((nRead = lines.Read(buf, 0, buf.Length)) > 0)
						{
							this._enclosing.output.Append(buf, 0, nRead);
						}
					}
					catch (Exception t)
					{
						WindowsSecureContainerExecutor.Log.Error("Error occured reading the process stdout"
							, t);
					}
				}

				private readonly WintuilsProcessStubExecutor _enclosing;

				private readonly InputStream stream;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Execute()
			{
				if (state != WindowsSecureContainerExecutor.WintuilsProcessStubExecutor.State.Init)
				{
					throw new IOException("Process is already started");
				}
				processStub = WindowsSecureContainerExecutor.Native.CreateTaskAsUser(cwd, jobName
					, userName, pidFile, cmdLine);
				state = WindowsSecureContainerExecutor.WintuilsProcessStubExecutor.State.Running;
				Sharpen.Thread stdOutReader = StartStreamReader(processStub.GetInputStream());
				Sharpen.Thread stdErrReader = StartStreamReader(processStub.GetErrorStream());
				try
				{
					processStub.Resume();
					processStub.WaitFor();
					stdOutReader.Join();
					stdErrReader.Join();
				}
				catch (Exception ie)
				{
					throw new IOException(ie);
				}
				exitCode = processStub.ExitValue();
				state = WindowsSecureContainerExecutor.WintuilsProcessStubExecutor.State.Complete;
			}

			public virtual void Close()
			{
				if (processStub != null)
				{
					processStub.Dispose();
				}
			}
		}

		private string nodeManagerGroup;

		/// <summary>Permissions for user WSCE dirs.</summary>
		internal const short DirPerm = (short)0x1e8;

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Sharpen.URISyntaxException"/>
		public WindowsSecureContainerExecutor()
			: base(FileContext.GetFileContext(new WindowsSecureContainerExecutor.ElevatedFileSystem
				(), new Configuration()))
		{
		}

		public override void SetConf(Configuration conf)
		{
			base.SetConf(conf);
			nodeManagerGroup = conf.Get(YarnConfiguration.NmWindowsSecureContainerGroup);
		}

		protected internal override string[] GetRunCommand(string command, string groupId
			, string userName, Path pidFile, Configuration conf)
		{
			FilePath f = new FilePath(command);
			if (Log.IsDebugEnabled())
			{
				Log.Debug(string.Format("getRunCommand: %s exists:%b", command, f.Exists()));
			}
			return new string[] { Shell.Winutils, "task", "createAsUser", groupId, userName, 
				pidFile.ToString(), "cmd /c " + command };
		}

		protected internal override DefaultContainerExecutor.LocalWrapperScriptBuilder GetLocalWrapperScriptBuilder
			(string containerIdStr, Path containerWorkDir)
		{
			return new WindowsSecureContainerExecutor.WindowsSecureWrapperScriptBuilder(this, 
				containerWorkDir);
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override void CopyFile(Path src, Path dst, string owner)
		{
			if (Log.IsDebugEnabled())
			{
				Log.Debug(string.Format("copyFile: %s -> %s owner:%s", src.ToString(), dst.ToString
					(), owner));
			}
			WindowsSecureContainerExecutor.Native.Elevated.Copy(src, dst, true);
			WindowsSecureContainerExecutor.Native.Elevated.Chown(dst, owner, nodeManagerGroup
				);
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override void CreateDir(Path dirPath, FsPermission perms, bool
			 createParent, string owner)
		{
			// WSCE requires dirs to be 750, not 710 as DCE.
			// This is similar to how LCE creates dirs
			//
			perms = new FsPermission(DirPerm);
			if (Log.IsDebugEnabled())
			{
				Log.Debug(string.Format("createDir: %s perm:%s owner:%s", dirPath.ToString(), perms
					.ToString(), owner));
			}
			base.CreateDir(dirPath, perms, createParent, owner);
			lfs.SetOwner(dirPath, owner, nodeManagerGroup);
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override void SetScriptExecutable(Path script, string owner)
		{
			if (Log.IsDebugEnabled())
			{
				Log.Debug(string.Format("setScriptExecutable: %s owner:%s", script.ToString(), owner
					));
			}
			base.SetScriptExecutable(script, owner);
			WindowsSecureContainerExecutor.Native.Elevated.Chown(script, owner, nodeManagerGroup
				);
		}

		/// <exception cref="System.IO.IOException"/>
		public override Path LocalizeClasspathJar(Path classPathJar, Path pwd, string owner
			)
		{
			if (Log.IsDebugEnabled())
			{
				Log.Debug(string.Format("localizeClasspathJar: %s %s o:%s", classPathJar, pwd, owner
					));
			}
			CreateDir(pwd, new FsPermission(DirPerm), true, owner);
			string fileName = classPathJar.GetName();
			Path dst = new Path(pwd, fileName);
			WindowsSecureContainerExecutor.Native.Elevated.Move(classPathJar, dst, true);
			WindowsSecureContainerExecutor.Native.Elevated.Chown(dst, owner, nodeManagerGroup
				);
			return dst;
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public override void StartLocalizer(Path nmPrivateContainerTokens, IPEndPoint nmAddr
			, string user, string appId, string locId, LocalDirsHandlerService dirsHandler)
		{
			IList<string> localDirs = dirsHandler.GetLocalDirs();
			IList<string> logDirs = dirsHandler.GetLogDirs();
			Path classpathJarPrivateDir = dirsHandler.GetLocalPathForWrite(ResourceLocalizationService
				.NmPrivateDir);
			CreateUserLocalDirs(localDirs, user);
			CreateUserCacheDirs(localDirs, user);
			CreateAppDirs(localDirs, user, appId);
			CreateAppLogDirs(appId, logDirs, user);
			Path appStorageDir = GetWorkingDir(localDirs, user, appId);
			string tokenFn = string.Format(ContainerLocalizer.TokenFileNameFmt, locId);
			Path tokenDst = new Path(appStorageDir, tokenFn);
			CopyFile(nmPrivateContainerTokens, tokenDst, user);
			FilePath cwdApp = new FilePath(appStorageDir.ToString());
			if (Log.IsDebugEnabled())
			{
				Log.Debug(string.Format("cwdApp: %s", cwdApp));
			}
			IList<string> command;
			command = new AList<string>();
			//use same jvm as parent
			FilePath jvm = new FilePath(new FilePath(Runtime.GetProperty("java.home"), "bin")
				, "java.exe");
			command.AddItem(jvm.ToString());
			Path cwdPath = new Path(cwdApp.GetPath());
			// Build a temp classpath jar. See ContainerLaunch.sanitizeEnv().
			// Passing CLASSPATH explicitly is *way* too long for command line.
			string classPath = Runtime.GetProperty("java.class.path");
			IDictionary<string, string> env = new Dictionary<string, string>(Sharpen.Runtime.GetEnv
				());
			string[] jarCp = FileUtil.CreateJarWithClassPath(classPath, classpathJarPrivateDir
				, cwdPath, env);
			string classPathJar = LocalizeClasspathJar(new Path(jarCp[0]), cwdPath, user).ToString
				();
			command.AddItem("-classpath");
			command.AddItem(classPathJar + jarCp[1]);
			string javaLibPath = Runtime.GetProperty("java.library.path");
			if (javaLibPath != null)
			{
				command.AddItem("-Djava.library.path=" + javaLibPath);
			}
			ContainerLocalizer.BuildMainArgs(command, user, appId, locId, nmAddr, localDirs);
			string cmdLine = StringUtils.Join(command, " ");
			string localizerPid = string.Format(LocalizerPidFormat, locId);
			WindowsSecureContainerExecutor.WintuilsProcessStubExecutor stubExecutor = new WindowsSecureContainerExecutor.WintuilsProcessStubExecutor
				(cwdApp.GetAbsolutePath(), localizerPid, user, "nul:", cmdLine);
			try
			{
				stubExecutor.Execute();
				stubExecutor.ValidateResult();
			}
			finally
			{
				stubExecutor.Close();
				try
				{
					KillContainer(localizerPid, ContainerExecutor.Signal.Kill);
				}
				catch (Exception e)
				{
					Log.Warn(string.Format("An exception occured during the cleanup of localizer job %s:%n%s"
						, localizerPid, StringUtils.StringifyException(e)));
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override Shell.CommandExecutor BuildCommandExecutor(string wrapperScriptPath
			, string containerIdStr, string userName, Path pidFile, Resource resource, FilePath
			 wordDir, IDictionary<string, string> environment)
		{
			return new WindowsSecureContainerExecutor.WintuilsProcessStubExecutor(wordDir.ToString
				(), containerIdStr, userName, pidFile.ToString(), "cmd /c " + wrapperScriptPath);
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override void KillContainer(string pid, ContainerExecutor.Signal
			 signal)
		{
			WindowsSecureContainerExecutor.Native.Elevated.KillTask(pid);
		}
	}
}
