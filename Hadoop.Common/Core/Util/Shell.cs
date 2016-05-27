using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Text;
using Org.Apache.Commons.Logging;
using Sharpen;

namespace Org.Apache.Hadoop.Util
{
	/// <summary>A base class for running a Unix command.</summary>
	/// <remarks>
	/// A base class for running a Unix command.
	/// <code>Shell</code> can be used to run unix commands like <code>du</code> or
	/// <code>df</code>. It also offers facilities to gate commands by
	/// time-intervals.
	/// </remarks>
	public abstract class Shell
	{
		public static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Util.Shell
			));

		private static bool IsJava7OrAbove = string.CompareOrdinal(Sharpen.Runtime.Substring
			(Runtime.GetProperty("java.version"), 0, 3), "1.7") >= 0;

		public static bool IsJava7OrAbove()
		{
			return IsJava7OrAbove;
		}

		/// <summary>
		/// Maximum command line length in Windows
		/// KB830473 documents this as 8191
		/// </summary>
		public const int WindowsMaxShellLenght = 8191;

		/// <summary>
		/// Checks if a given command (String[]) fits in the Windows maximum command line length
		/// Note that the input is expected to already include space delimiters, no extra count
		/// will be added for delimiters.
		/// </summary>
		/// <param name="commands">command parts, including any space delimiters</param>
		/// <exception cref="System.IO.IOException"/>
		public static void CheckWindowsCommandLineLength(params string[] commands)
		{
			int len = 0;
			foreach (string s in commands)
			{
				len += s.Length;
			}
			if (len > WindowsMaxShellLenght)
			{
				throw new IOException(string.Format("The command line has a length of %d exceeds maximum allowed length of %d. "
					 + "Command starts with: %s", len, WindowsMaxShellLenght, Sharpen.Runtime.Substring
					(StringUtils.Join(string.Empty, commands), 0, 100)));
			}
		}

		/// <summary>a Unix command to get the current user's name</summary>
		public const string UserNameCommand = "whoami";

		/// <summary>Windows CreateProcess synchronization object</summary>
		public static readonly object WindowsProcessLaunchLock = new object();

		public enum OSType
		{
			OsTypeLinux,
			OsTypeWin,
			OsTypeSolaris,
			OsTypeMac,
			OsTypeFreebsd,
			OsTypeOther
		}

		public static readonly Shell.OSType osType = GetOSType();

		// OSType detection
		private static Shell.OSType GetOSType()
		{
			string osName = Runtime.GetProperty("os.name");
			if (osName.StartsWith("Windows"))
			{
				return Shell.OSType.OsTypeWin;
			}
			else
			{
				if (osName.Contains("SunOS") || osName.Contains("Solaris"))
				{
					return Shell.OSType.OsTypeSolaris;
				}
				else
				{
					if (osName.Contains("Mac"))
					{
						return Shell.OSType.OsTypeMac;
					}
					else
					{
						if (osName.Contains("FreeBSD"))
						{
							return Shell.OSType.OsTypeFreebsd;
						}
						else
						{
							if (osName.StartsWith("Linux"))
							{
								return Shell.OSType.OsTypeLinux;
							}
							else
							{
								// Some other form of Unix
								return Shell.OSType.OsTypeOther;
							}
						}
					}
				}
			}
		}

		public static readonly bool Windows = (osType == Shell.OSType.OsTypeWin);

		public static readonly bool Solaris = (osType == Shell.OSType.OsTypeSolaris);

		public static readonly bool Mac = (osType == Shell.OSType.OsTypeMac);

		public static readonly bool Freebsd = (osType == Shell.OSType.OsTypeFreebsd);

		public static readonly bool Linux = (osType == Shell.OSType.OsTypeLinux);

		public static readonly bool Other = (osType == Shell.OSType.OsTypeOther);

		public static readonly bool Ppc64 = Runtime.GetProperties().GetProperty("os.arch"
			).Contains("ppc64");

		// Helper static vars for each platform
		/// <summary>a Unix command to get the current user's groups list</summary>
		public static string[] GetGroupsCommand()
		{
			return (Windows) ? new string[] { "cmd", "/c", "groups" } : new string[] { "bash"
				, "-c", "groups" };
		}

		/// <summary>a Unix command to get a given user's groups list.</summary>
		/// <remarks>
		/// a Unix command to get a given user's groups list.
		/// If the OS is not WINDOWS, the command will get the user's primary group
		/// first and finally get the groups list which includes the primary group.
		/// i.e. the user's primary group will be included twice.
		/// </remarks>
		public static string[] GetGroupsForUserCommand(string user)
		{
			//'groups username' command return is non-consistent across different unixes
			return (Windows) ? new string[] { Winutils, "groups", "-F", "\"" + user + "\"" } : 
				new string[] { "bash", "-c", "id -gn " + user + "&& id -Gn " + user };
		}

		/// <summary>a Unix command to get a given netgroup's user list</summary>
		public static string[] GetUsersForNetgroupCommand(string netgroup)
		{
			//'groups username' command return is non-consistent across different unixes
			return (Windows) ? new string[] { "cmd", "/c", "getent netgroup " + netgroup } : 
				new string[] { "bash", "-c", "getent netgroup " + netgroup };
		}

		/// <summary>Return a command to get permission information.</summary>
		public static string[] GetGetPermissionCommand()
		{
			return (Windows) ? new string[] { Winutils, "ls", "-F" } : new string[] { "/bin/ls"
				, "-ld" };
		}

		/// <summary>Return a command to set permission</summary>
		public static string[] GetSetPermissionCommand(string perm, bool recursive)
		{
			if (recursive)
			{
				return (Windows) ? new string[] { Winutils, "chmod", "-R", perm } : new string[] 
					{ "chmod", "-R", perm };
			}
			else
			{
				return (Windows) ? new string[] { Winutils, "chmod", perm } : new string[] { "chmod"
					, perm };
			}
		}

		/// <summary>Return a command to set permission for specific file.</summary>
		/// <param name="perm">String permission to set</param>
		/// <param name="recursive">boolean true to apply to all sub-directories recursively</param>
		/// <param name="file">String file to set</param>
		/// <returns>String[] containing command and arguments</returns>
		public static string[] GetSetPermissionCommand(string perm, bool recursive, string
			 file)
		{
			string[] baseCmd = GetSetPermissionCommand(perm, recursive);
			string[] cmdWithFile = Arrays.CopyOf(baseCmd, baseCmd.Length + 1);
			cmdWithFile[cmdWithFile.Length - 1] = file;
			return cmdWithFile;
		}

		/// <summary>Return a command to set owner</summary>
		public static string[] GetSetOwnerCommand(string owner)
		{
			return (Windows) ? new string[] { Winutils, "chown", "\"" + owner + "\"" } : new 
				string[] { "chown", owner };
		}

		/// <summary>Return a command to create symbolic links</summary>
		public static string[] GetSymlinkCommand(string target, string link)
		{
			return Windows ? new string[] { Winutils, "symlink", link, target } : new string[
				] { "ln", "-s", target, link };
		}

		/// <summary>Return a command to read the target of the a symbolic link</summary>
		public static string[] GetReadlinkCommand(string link)
		{
			return Windows ? new string[] { Winutils, "readlink", link } : new string[] { "readlink"
				, link };
		}

		/// <summary>Return a command for determining if process with specified pid is alive.
		/// 	</summary>
		public static string[] GetCheckProcessIsAliveCommand(string pid)
		{
			return Org.Apache.Hadoop.Util.Shell.Windows ? new string[] { Org.Apache.Hadoop.Util.Shell
				.Winutils, "task", "isAlive", pid } : new string[] { "kill", "-0", isSetsidAvailable
				 ? "-" + pid : pid };
		}

		/// <summary>Return a command to send a signal to a given pid</summary>
		public static string[] GetSignalKillCommand(int code, string pid)
		{
			return Org.Apache.Hadoop.Util.Shell.Windows ? new string[] { Org.Apache.Hadoop.Util.Shell
				.Winutils, "task", "kill", pid } : new string[] { "kill", "-" + code, isSetsidAvailable
				 ? "-" + pid : pid };
		}

		/// <summary>Return a regular expression string that match environment variables</summary>
		public static string GetEnvironmentVariableRegex()
		{
			return (Windows) ? "%([A-Za-z_][A-Za-z0-9_]*?)%" : "\\$([A-Za-z_][A-Za-z0-9_]*)";
		}

		/// <summary>
		/// Returns a File referencing a script with the given basename, inside the
		/// given parent directory.
		/// </summary>
		/// <remarks>
		/// Returns a File referencing a script with the given basename, inside the
		/// given parent directory.  The file extension is inferred by platform: ".cmd"
		/// on Windows, or ".sh" otherwise.
		/// </remarks>
		/// <param name="parent">File parent directory</param>
		/// <param name="basename">String script file basename</param>
		/// <returns>File referencing the script in the directory</returns>
		public static FilePath AppendScriptExtension(FilePath parent, string basename)
		{
			return new FilePath(parent, AppendScriptExtension(basename));
		}

		/// <summary>Returns a script file name with the given basename.</summary>
		/// <remarks>
		/// Returns a script file name with the given basename.  The file extension is
		/// inferred by platform: ".cmd" on Windows, or ".sh" otherwise.
		/// </remarks>
		/// <param name="basename">String script file basename</param>
		/// <returns>String script file name</returns>
		public static string AppendScriptExtension(string basename)
		{
			return basename + (Windows ? ".cmd" : ".sh");
		}

		/// <summary>Returns a command to run the given script.</summary>
		/// <remarks>
		/// Returns a command to run the given script.  The script interpreter is
		/// inferred by platform: cmd on Windows or bash otherwise.
		/// </remarks>
		/// <param name="script">File script to run</param>
		/// <returns>String[] command to run the script</returns>
		public static string[] GetRunScriptCommand(FilePath script)
		{
			string absolutePath = script.GetAbsolutePath();
			return Windows ? new string[] { "cmd", "/c", absolutePath } : new string[] { "/bin/bash"
				, absolutePath };
		}

		/// <summary>a Unix command to set permission</summary>
		public const string SetPermissionCommand = "chmod";

		/// <summary>a Unix command to set owner</summary>
		public const string SetOwnerCommand = "chown";

		/// <summary>a Unix command to set the change user's groups list</summary>
		public const string SetGroupCommand = "chgrp";

		/// <summary>a Unix command to create a link</summary>
		public const string LinkCommand = "ln";

		/// <summary>a Unix command to get a link target</summary>
		public const string ReadLinkCommand = "readlink";

		/// <summary>Time after which the executing script would be timedout</summary>
		protected internal long timeOutInterval = 0L;

		/// <summary>If or not script timed out</summary>
		private AtomicBoolean timedOut;

		/// <summary>
		/// Centralized logic to discover and validate the sanity of the Hadoop
		/// home directory.
		/// </summary>
		/// <remarks>
		/// Centralized logic to discover and validate the sanity of the Hadoop
		/// home directory. Returns either NULL or a directory that exists and
		/// was specified via either -Dhadoop.home.dir or the HADOOP_HOME ENV
		/// variable.  This does a lot of work so it should only be called
		/// privately for initialization once per process.
		/// </remarks>
		private static string CheckHadoopHome()
		{
			// first check the Dflag hadoop.home.dir with JVM scope
			string home = Runtime.GetProperty("hadoop.home.dir");
			// fall back to the system/user-global env variable
			if (home == null)
			{
				home = Runtime.Getenv("HADOOP_HOME");
			}
			try
			{
				// couldn't find either setting for hadoop's home directory
				if (home == null)
				{
					throw new IOException("HADOOP_HOME or hadoop.home.dir are not set.");
				}
				if (home.StartsWith("\"") && home.EndsWith("\""))
				{
					home = Sharpen.Runtime.Substring(home, 1, home.Length - 1);
				}
				// check that the home setting is actually a directory that exists
				FilePath homedir = new FilePath(home);
				if (!homedir.IsAbsolute() || !homedir.Exists() || !homedir.IsDirectory())
				{
					throw new IOException("Hadoop home directory " + homedir + " does not exist, is not a directory, or is not an absolute path."
						);
				}
				home = homedir.GetCanonicalPath();
			}
			catch (IOException ioe)
			{
				if (Log.IsDebugEnabled())
				{
					Log.Debug("Failed to detect a valid hadoop home directory", ioe);
				}
				home = null;
			}
			return home;
		}

		private static string HadoopHomeDir = CheckHadoopHome();

		// Public getter, throws an exception if HADOOP_HOME failed validation
		// checks and is being referenced downstream.
		/// <exception cref="System.IO.IOException"/>
		public static string GetHadoopHome()
		{
			if (HadoopHomeDir == null)
			{
				throw new IOException("Misconfigured HADOOP_HOME cannot be referenced.");
			}
			return HadoopHomeDir;
		}

		/// <summary>
		/// fully qualify the path to a binary that should be in a known hadoop
		/// bin location.
		/// </summary>
		/// <remarks>
		/// fully qualify the path to a binary that should be in a known hadoop
		/// bin location. This is primarily useful for disambiguating call-outs
		/// to executable sub-components of Hadoop to avoid clashes with other
		/// executables that may be in the path.  Caveat:  this call doesn't
		/// just format the path to the bin directory.  It also checks for file
		/// existence of the composed path. The output of this call should be
		/// cached by callers.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public static string GetQualifiedBinPath(string executable)
		{
			// construct hadoop bin path to the specified executable
			string fullExeName = HadoopHomeDir + FilePath.separator + "bin" + FilePath.separator
				 + executable;
			FilePath exeFile = new FilePath(fullExeName);
			if (!exeFile.Exists())
			{
				throw new IOException("Could not locate executable " + fullExeName + " in the Hadoop binaries."
					);
			}
			return exeFile.GetCanonicalPath();
		}

		/// <summary>a Windows utility to emulate Unix commands</summary>
		public static readonly string Winutils = GetWinUtilsPath();

		public static string GetWinUtilsPath()
		{
			string winUtilsPath = null;
			try
			{
				if (Windows)
				{
					winUtilsPath = GetQualifiedBinPath("winutils.exe");
				}
			}
			catch (IOException ioe)
			{
				Log.Error("Failed to locate the winutils binary in the hadoop binary path", ioe);
			}
			return winUtilsPath;
		}

		public static readonly bool isSetsidAvailable = IsSetsidSupported();

		private static bool IsSetsidSupported()
		{
			if (Org.Apache.Hadoop.Util.Shell.Windows)
			{
				return false;
			}
			Shell.ShellCommandExecutor shexec = null;
			bool setsidSupported = true;
			try
			{
				string[] args = new string[] { "setsid", "bash", "-c", "echo $$" };
				shexec = new Shell.ShellCommandExecutor(args);
				shexec.Execute();
			}
			catch (IOException)
			{
				Log.Debug("setsid is not available on this machine. So not using it.");
				setsidSupported = false;
			}
			finally
			{
				// handle the exit code
				if (Log.IsDebugEnabled())
				{
					Log.Debug("setsid exited with exit code " + (shexec != null ? shexec.GetExitCode(
						) : "(null executor)"));
				}
			}
			return setsidSupported;
		}

		/// <summary>Token separator regex used to parse Shell tool outputs</summary>
		public static readonly string TokenSeparatorRegex = Windows ? "[|\n\r]" : "[ \t\n\r\f]";

		private long interval;

		private long lastTime;

		private readonly bool redirectErrorStream;

		private IDictionary<string, string> environment;

		private FilePath dir;

		private SystemProcess process;

		private int exitCode;

		/// <summary>If or not script finished executing</summary>
		private volatile AtomicBoolean completed;

		public Shell()
			: this(0L)
		{
		}

		public Shell(long interval)
			: this(interval, false)
		{
		}

		/// <param name="interval">
		/// the minimum duration to wait before re-executing the
		/// command.
		/// </param>
		public Shell(long interval, bool redirectErrorStream)
		{
			// refresh interval in msec
			// last time the command was performed
			// merge stdout and stderr
			// env for the command execution
			// sub process used to execute the command
			this.interval = interval;
			this.lastTime = (interval < 0) ? 0 : -interval;
			this.redirectErrorStream = redirectErrorStream;
		}

		/// <summary>set the environment for the command</summary>
		/// <param name="env">Mapping of environment variables</param>
		protected internal virtual void SetEnvironment(IDictionary<string, string> env)
		{
			this.environment = env;
		}

		/// <summary>set the working directory</summary>
		/// <param name="dir">The directory where the command would be executed</param>
		protected internal virtual void SetWorkingDirectory(FilePath dir)
		{
			this.dir = dir;
		}

		/// <summary>check to see if a command needs to be executed and execute if needed</summary>
		/// <exception cref="System.IO.IOException"/>
		protected internal virtual void Run()
		{
			if (lastTime + interval > Time.MonotonicNow())
			{
				return;
			}
			exitCode = 0;
			// reset for next run
			RunCommand();
		}

		/// <summary>Run a command</summary>
		/// <exception cref="System.IO.IOException"/>
		private void RunCommand()
		{
			ProcessStartInfo builder = new ProcessStartInfo(GetExecString());
			Timer timeOutTimer = null;
			Shell.ShellTimeoutTimerTask timeoutTimerTask = null;
			timedOut = new AtomicBoolean(false);
			completed = new AtomicBoolean(false);
			if (environment != null)
			{
				builder.EnvironmentVariables.PutAll(this.environment);
			}
			if (dir != null)
			{
				builder.WorkingDirectory = this.dir;
			}
			builder.RedirectErrorStream(redirectErrorStream);
			if (Org.Apache.Hadoop.Util.Shell.Windows)
			{
				lock (WindowsProcessLaunchLock)
				{
					// To workaround the race condition issue with child processes
					// inheriting unintended handles during process launch that can
					// lead to hangs on reading output and error streams, we
					// serialize process creation. More info available at:
					// http://support.microsoft.com/kb/315939
					process = builder.Start();
				}
			}
			else
			{
				process = builder.Start();
			}
			if (timeOutInterval > 0)
			{
				timeOutTimer = new Timer("Shell command timeout");
				timeoutTimerTask = new Shell.ShellTimeoutTimerTask(this);
				//One time scheduling.
				timeOutTimer.Schedule(timeoutTimerTask, timeOutInterval);
			}
			BufferedReader errReader = new BufferedReader(new InputStreamReader(process.GetErrorStream
				(), Encoding.Default));
			BufferedReader inReader = new BufferedReader(new InputStreamReader(process.GetInputStream
				(), Encoding.Default));
			StringBuilder errMsg = new StringBuilder();
			// read error and input streams as this would free up the buffers
			// free the error stream buffer
			Sharpen.Thread errThread = new _Thread_506(errReader, errMsg);
			try
			{
				errThread.Start();
			}
			catch (InvalidOperationException)
			{
			}
			catch (OutOfMemoryException oe)
			{
				Log.Error("Caught " + oe + ". One possible reason is that ulimit" + " setting of 'max user processes' is too low. If so, do"
					 + " 'ulimit -u <largerNum>' and try again.");
				throw;
			}
			try
			{
				ParseExecResult(inReader);
				// parse the output
				// clear the input stream buffer
				string line = inReader.ReadLine();
				while (line != null)
				{
					line = inReader.ReadLine();
				}
				// wait for the process to finish and check the exit code
				exitCode = process.WaitFor();
				// make sure that the error thread exits
				JoinThread(errThread);
				completed.Set(true);
				//the timeout thread handling
				//taken care in finally block
				if (exitCode != 0)
				{
					throw new Shell.ExitCodeException(exitCode, errMsg.ToString());
				}
			}
			catch (Exception ie)
			{
				throw new IOException(ie.ToString());
			}
			finally
			{
				if (timeOutTimer != null)
				{
					timeOutTimer.Cancel();
				}
				// close the input stream
				try
				{
					// JDK 7 tries to automatically drain the input streams for us
					// when the process exits, but since close is not synchronized,
					// it creates a race if we close the stream first and the same
					// fd is recycled.  the stream draining thread will attempt to
					// drain that fd!!  it may block, OOM, or cause bizarre behavior
					// see: https://bugs.openjdk.java.net/browse/JDK-8024521
					//      issue is fixed in build 7u60
					InputStream stdout = process.GetInputStream();
					lock (stdout)
					{
						inReader.Close();
					}
				}
				catch (IOException ioe)
				{
					Log.Warn("Error while closing the input stream", ioe);
				}
				if (!completed.Get())
				{
					errThread.Interrupt();
					JoinThread(errThread);
				}
				try
				{
					InputStream stderr = process.GetErrorStream();
					lock (stderr)
					{
						errReader.Close();
					}
				}
				catch (IOException ioe)
				{
					Log.Warn("Error while closing the error stream", ioe);
				}
				process.Destroy();
				lastTime = Time.MonotonicNow();
			}
		}

		private sealed class _Thread_506 : Sharpen.Thread
		{
			public _Thread_506(BufferedReader errReader, StringBuilder errMsg)
			{
				this.errReader = errReader;
				this.errMsg = errMsg;
			}

			public override void Run()
			{
				try
				{
					string line = errReader.ReadLine();
					while ((line != null) && !this.IsInterrupted())
					{
						errMsg.Append(line);
						errMsg.Append(Runtime.GetProperty("line.separator"));
						line = errReader.ReadLine();
					}
				}
				catch (IOException ioe)
				{
					Org.Apache.Hadoop.Util.Shell.Log.Warn("Error reading the error stream", ioe);
				}
			}

			private readonly BufferedReader errReader;

			private readonly StringBuilder errMsg;
		}

		private static void JoinThread(Sharpen.Thread t)
		{
			while (t.IsAlive())
			{
				try
				{
					t.Join();
				}
				catch (Exception ie)
				{
					if (Log.IsWarnEnabled())
					{
						Log.Warn("Interrupted while joining on: " + t, ie);
					}
					t.Interrupt();
				}
			}
		}

		// propagate interrupt
		/// <summary>return an array containing the command name & its parameters</summary>
		protected internal abstract string[] GetExecString();

		/// <summary>Parse the execution result</summary>
		/// <exception cref="System.IO.IOException"/>
		protected internal abstract void ParseExecResult(BufferedReader lines);

		/// <summary>Get the environment variable</summary>
		public virtual string GetEnvironment(string env)
		{
			return environment[env];
		}

		/// <summary>get the current sub-process executing the given command</summary>
		/// <returns>process executing the command</returns>
		public virtual SystemProcess GetProcess()
		{
			return process;
		}

		/// <summary>get the exit code</summary>
		/// <returns>the exit code of the process</returns>
		public virtual int GetExitCode()
		{
			return exitCode;
		}

		/// <summary>This is an IOException with exit code added.</summary>
		[System.Serializable]
		public class ExitCodeException : IOException
		{
			private readonly int exitCode;

			public ExitCodeException(int exitCode, string message)
				: base(message)
			{
				this.exitCode = exitCode;
			}

			public virtual int GetExitCode()
			{
				return exitCode;
			}

			public override string ToString()
			{
				StringBuilder sb = new StringBuilder("ExitCodeException ");
				sb.Append("exitCode=").Append(exitCode).Append(": ");
				sb.Append(base.Message);
				return sb.ToString();
			}
		}

		public interface CommandExecutor
		{
			/// <exception cref="System.IO.IOException"/>
			void Execute();

			/// <exception cref="System.IO.IOException"/>
			int GetExitCode();

			/// <exception cref="System.IO.IOException"/>
			string GetOutput();

			void Close();
		}

		/// <summary>A simple shell command executor.</summary>
		/// <remarks>
		/// A simple shell command executor.
		/// <code>ShellCommandExecutor</code>should be used in cases where the output
		/// of the command needs no explicit parsing and where the command, working
		/// directory and the environment remains unchanged. The output of the command
		/// is stored as-is and is expected to be small.
		/// </remarks>
		public class ShellCommandExecutor : Shell, Shell.CommandExecutor
		{
			private string[] command;

			private StringBuilder output;

			public ShellCommandExecutor(string[] execString)
				: this(execString, null)
			{
			}

			public ShellCommandExecutor(string[] execString, FilePath dir)
				: this(execString, dir, null)
			{
			}

			public ShellCommandExecutor(string[] execString, FilePath dir, IDictionary<string
				, string> env)
				: this(execString, dir, env, 0L)
			{
			}

			/// <summary>Create a new instance of the ShellCommandExecutor to execute a command.</summary>
			/// <param name="execString">The command to execute with arguments</param>
			/// <param name="dir">
			/// If not-null, specifies the directory which should be set
			/// as the current working directory for the command.
			/// If null, the current working directory is not modified.
			/// </param>
			/// <param name="env">
			/// If not-null, environment of the command will include the
			/// key-value pairs specified in the map. If null, the current
			/// environment is not modified.
			/// </param>
			/// <param name="timeout">
			/// Specifies the time in milliseconds, after which the
			/// command will be killed and the status marked as timedout.
			/// If 0, the command will not be timed out.
			/// </param>
			public ShellCommandExecutor(string[] execString, FilePath dir, IDictionary<string
				, string> env, long timeout)
			{
				command = execString.MemberwiseClone();
				if (dir != null)
				{
					SetWorkingDirectory(dir);
				}
				if (env != null)
				{
					SetEnvironment(env);
				}
				timeOutInterval = timeout;
			}

			/// <summary>Execute the shell command.</summary>
			/// <exception cref="System.IO.IOException"/>
			public virtual void Execute()
			{
				this.Run();
			}

			protected internal override string[] GetExecString()
			{
				return command;
			}

			/// <exception cref="System.IO.IOException"/>
			protected internal override void ParseExecResult(BufferedReader lines)
			{
				output = new StringBuilder();
				char[] buf = new char[512];
				int nRead;
				while ((nRead = lines.Read(buf, 0, buf.Length)) > 0)
				{
					output.Append(buf, 0, nRead);
				}
			}

			/// <summary>Get the output of the shell command.</summary>
			public virtual string GetOutput()
			{
				return (output == null) ? string.Empty : output.ToString();
			}

			/// <summary>Returns the commands of this instance.</summary>
			/// <remarks>
			/// Returns the commands of this instance.
			/// Arguments with spaces in are presented with quotes round; other
			/// arguments are presented raw
			/// </remarks>
			/// <returns>a string representation of the object.</returns>
			public override string ToString()
			{
				StringBuilder builder = new StringBuilder();
				string[] args = GetExecString();
				foreach (string s in args)
				{
					if (s.IndexOf(' ') >= 0)
					{
						builder.Append('"').Append(s).Append('"');
					}
					else
					{
						builder.Append(s);
					}
					builder.Append(' ');
				}
				return builder.ToString();
			}

			public virtual void Close()
			{
			}
		}

		/// <summary>
		/// To check if the passed script to shell command executor timed out or
		/// not.
		/// </summary>
		/// <returns>if the script timed out.</returns>
		public virtual bool IsTimedOut()
		{
			return timedOut.Get();
		}

		/// <summary>Set if the command has timed out.</summary>
		private void SetTimedOut()
		{
			this.timedOut.Set(true);
		}

		/// <summary>Static method to execute a shell command.</summary>
		/// <remarks>
		/// Static method to execute a shell command.
		/// Covers most of the simple cases without requiring the user to implement
		/// the <code>Shell</code> interface.
		/// </remarks>
		/// <param name="cmd">shell command to execute.</param>
		/// <returns>the output of the executed command.</returns>
		/// <exception cref="System.IO.IOException"/>
		public static string ExecCommand(params string[] cmd)
		{
			return ExecCommand(null, cmd, 0L);
		}

		/// <summary>Static method to execute a shell command.</summary>
		/// <remarks>
		/// Static method to execute a shell command.
		/// Covers most of the simple cases without requiring the user to implement
		/// the <code>Shell</code> interface.
		/// </remarks>
		/// <param name="env">the map of environment key=value</param>
		/// <param name="cmd">shell command to execute.</param>
		/// <param name="timeout">time in milliseconds after which script should be marked timeout
		/// 	</param>
		/// <returns>the output of the executed command.o</returns>
		/// <exception cref="System.IO.IOException"/>
		public static string ExecCommand(IDictionary<string, string> env, string[] cmd, long
			 timeout)
		{
			Shell.ShellCommandExecutor exec = new Shell.ShellCommandExecutor(cmd, null, env, 
				timeout);
			exec.Execute();
			return exec.GetOutput();
		}

		/// <summary>Static method to execute a shell command.</summary>
		/// <remarks>
		/// Static method to execute a shell command.
		/// Covers most of the simple cases without requiring the user to implement
		/// the <code>Shell</code> interface.
		/// </remarks>
		/// <param name="env">the map of environment key=value</param>
		/// <param name="cmd">shell command to execute.</param>
		/// <returns>the output of the executed command.</returns>
		/// <exception cref="System.IO.IOException"/>
		public static string ExecCommand(IDictionary<string, string> env, params string[]
			 cmd)
		{
			return ExecCommand(env, cmd, 0L);
		}

		/// <summary>Timer which is used to timeout scripts spawned off by shell.</summary>
		private class ShellTimeoutTimerTask : TimerTask
		{
			private Shell shell;

			public ShellTimeoutTimerTask(Shell shell)
			{
				this.shell = shell;
			}

			public override void Run()
			{
				SystemProcess p = shell.GetProcess();
				try
				{
					p.ExitValue();
				}
				catch (Exception)
				{
					//Process has not terminated.
					//So check if it has completed 
					//if not just destroy it.
					if (p != null && !shell.completed.Get())
					{
						shell.SetTimedOut();
						p.Destroy();
					}
				}
			}
		}
	}
}
