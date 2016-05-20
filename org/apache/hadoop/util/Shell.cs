using Sharpen;

namespace org.apache.hadoop.util
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
		public static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.util.Shell)));

		private static bool IS_JAVA7_OR_ABOVE = string.CompareOrdinal(Sharpen.Runtime.substring
			(Sharpen.Runtime.getProperty("java.version"), 0, 3), "1.7") >= 0;

		public static bool isJava7OrAbove()
		{
			return IS_JAVA7_OR_ABOVE;
		}

		/// <summary>
		/// Maximum command line length in Windows
		/// KB830473 documents this as 8191
		/// </summary>
		public const int WINDOWS_MAX_SHELL_LENGHT = 8191;

		/// <summary>
		/// Checks if a given command (String[]) fits in the Windows maximum command line length
		/// Note that the input is expected to already include space delimiters, no extra count
		/// will be added for delimiters.
		/// </summary>
		/// <param name="commands">command parts, including any space delimiters</param>
		/// <exception cref="System.IO.IOException"/>
		public static void checkWindowsCommandLineLength(params string[] commands)
		{
			int len = 0;
			foreach (string s in commands)
			{
				len += s.Length;
			}
			if (len > WINDOWS_MAX_SHELL_LENGHT)
			{
				throw new System.IO.IOException(string.format("The command line has a length of %d exceeds maximum allowed length of %d. "
					 + "Command starts with: %s", len, WINDOWS_MAX_SHELL_LENGHT, Sharpen.Runtime.substring
					(org.apache.hadoop.util.StringUtils.join(string.Empty, commands), 0, 100)));
			}
		}

		/// <summary>a Unix command to get the current user's name</summary>
		public const string USER_NAME_COMMAND = "whoami";

		/// <summary>Windows CreateProcess synchronization object</summary>
		public static readonly object WindowsProcessLaunchLock = new object();

		public enum OSType
		{
			OS_TYPE_LINUX,
			OS_TYPE_WIN,
			OS_TYPE_SOLARIS,
			OS_TYPE_MAC,
			OS_TYPE_FREEBSD,
			OS_TYPE_OTHER
		}

		public static readonly org.apache.hadoop.util.Shell.OSType osType = getOSType();

		// OSType detection
		private static org.apache.hadoop.util.Shell.OSType getOSType()
		{
			string osName = Sharpen.Runtime.getProperty("os.name");
			if (osName.StartsWith("Windows"))
			{
				return org.apache.hadoop.util.Shell.OSType.OS_TYPE_WIN;
			}
			else
			{
				if (osName.contains("SunOS") || osName.contains("Solaris"))
				{
					return org.apache.hadoop.util.Shell.OSType.OS_TYPE_SOLARIS;
				}
				else
				{
					if (osName.contains("Mac"))
					{
						return org.apache.hadoop.util.Shell.OSType.OS_TYPE_MAC;
					}
					else
					{
						if (osName.contains("FreeBSD"))
						{
							return org.apache.hadoop.util.Shell.OSType.OS_TYPE_FREEBSD;
						}
						else
						{
							if (osName.StartsWith("Linux"))
							{
								return org.apache.hadoop.util.Shell.OSType.OS_TYPE_LINUX;
							}
							else
							{
								// Some other form of Unix
								return org.apache.hadoop.util.Shell.OSType.OS_TYPE_OTHER;
							}
						}
					}
				}
			}
		}

		public static readonly bool WINDOWS = (osType == org.apache.hadoop.util.Shell.OSType
			.OS_TYPE_WIN);

		public static readonly bool SOLARIS = (osType == org.apache.hadoop.util.Shell.OSType
			.OS_TYPE_SOLARIS);

		public static readonly bool MAC = (osType == org.apache.hadoop.util.Shell.OSType.
			OS_TYPE_MAC);

		public static readonly bool FREEBSD = (osType == org.apache.hadoop.util.Shell.OSType
			.OS_TYPE_FREEBSD);

		public static readonly bool LINUX = (osType == org.apache.hadoop.util.Shell.OSType
			.OS_TYPE_LINUX);

		public static readonly bool OTHER = (osType == org.apache.hadoop.util.Shell.OSType
			.OS_TYPE_OTHER);

		public static readonly bool PPC_64 = Sharpen.Runtime.getProperties().getProperty(
			"os.arch").contains("ppc64");

		// Helper static vars for each platform
		/// <summary>a Unix command to get the current user's groups list</summary>
		public static string[] getGroupsCommand()
		{
			return (WINDOWS) ? new string[] { "cmd", "/c", "groups" } : new string[] { "bash"
				, "-c", "groups" };
		}

		/// <summary>a Unix command to get a given user's groups list.</summary>
		/// <remarks>
		/// a Unix command to get a given user's groups list.
		/// If the OS is not WINDOWS, the command will get the user's primary group
		/// first and finally get the groups list which includes the primary group.
		/// i.e. the user's primary group will be included twice.
		/// </remarks>
		public static string[] getGroupsForUserCommand(string user)
		{
			//'groups username' command return is non-consistent across different unixes
			return (WINDOWS) ? new string[] { WINUTILS, "groups", "-F", "\"" + user + "\"" } : 
				new string[] { "bash", "-c", "id -gn " + user + "&& id -Gn " + user };
		}

		/// <summary>a Unix command to get a given netgroup's user list</summary>
		public static string[] getUsersForNetgroupCommand(string netgroup)
		{
			//'groups username' command return is non-consistent across different unixes
			return (WINDOWS) ? new string[] { "cmd", "/c", "getent netgroup " + netgroup } : 
				new string[] { "bash", "-c", "getent netgroup " + netgroup };
		}

		/// <summary>Return a command to get permission information.</summary>
		public static string[] getGetPermissionCommand()
		{
			return (WINDOWS) ? new string[] { WINUTILS, "ls", "-F" } : new string[] { "/bin/ls"
				, "-ld" };
		}

		/// <summary>Return a command to set permission</summary>
		public static string[] getSetPermissionCommand(string perm, bool recursive)
		{
			if (recursive)
			{
				return (WINDOWS) ? new string[] { WINUTILS, "chmod", "-R", perm } : new string[] 
					{ "chmod", "-R", perm };
			}
			else
			{
				return (WINDOWS) ? new string[] { WINUTILS, "chmod", perm } : new string[] { "chmod"
					, perm };
			}
		}

		/// <summary>Return a command to set permission for specific file.</summary>
		/// <param name="perm">String permission to set</param>
		/// <param name="recursive">boolean true to apply to all sub-directories recursively</param>
		/// <param name="file">String file to set</param>
		/// <returns>String[] containing command and arguments</returns>
		public static string[] getSetPermissionCommand(string perm, bool recursive, string
			 file)
		{
			string[] baseCmd = getSetPermissionCommand(perm, recursive);
			string[] cmdWithFile = java.util.Arrays.copyOf(baseCmd, baseCmd.Length + 1);
			cmdWithFile[cmdWithFile.Length - 1] = file;
			return cmdWithFile;
		}

		/// <summary>Return a command to set owner</summary>
		public static string[] getSetOwnerCommand(string owner)
		{
			return (WINDOWS) ? new string[] { WINUTILS, "chown", "\"" + owner + "\"" } : new 
				string[] { "chown", owner };
		}

		/// <summary>Return a command to create symbolic links</summary>
		public static string[] getSymlinkCommand(string target, string link)
		{
			return WINDOWS ? new string[] { WINUTILS, "symlink", link, target } : new string[
				] { "ln", "-s", target, link };
		}

		/// <summary>Return a command to read the target of the a symbolic link</summary>
		public static string[] getReadlinkCommand(string link)
		{
			return WINDOWS ? new string[] { WINUTILS, "readlink", link } : new string[] { "readlink"
				, link };
		}

		/// <summary>Return a command for determining if process with specified pid is alive.
		/// 	</summary>
		public static string[] getCheckProcessIsAliveCommand(string pid)
		{
			return org.apache.hadoop.util.Shell.WINDOWS ? new string[] { org.apache.hadoop.util.Shell
				.WINUTILS, "task", "isAlive", pid } : new string[] { "kill", "-0", isSetsidAvailable
				 ? "-" + pid : pid };
		}

		/// <summary>Return a command to send a signal to a given pid</summary>
		public static string[] getSignalKillCommand(int code, string pid)
		{
			return org.apache.hadoop.util.Shell.WINDOWS ? new string[] { org.apache.hadoop.util.Shell
				.WINUTILS, "task", "kill", pid } : new string[] { "kill", "-" + code, isSetsidAvailable
				 ? "-" + pid : pid };
		}

		/// <summary>Return a regular expression string that match environment variables</summary>
		public static string getEnvironmentVariableRegex()
		{
			return (WINDOWS) ? "%([A-Za-z_][A-Za-z0-9_]*?)%" : "\\$([A-Za-z_][A-Za-z0-9_]*)";
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
		public static java.io.File appendScriptExtension(java.io.File parent, string basename
			)
		{
			return new java.io.File(parent, appendScriptExtension(basename));
		}

		/// <summary>Returns a script file name with the given basename.</summary>
		/// <remarks>
		/// Returns a script file name with the given basename.  The file extension is
		/// inferred by platform: ".cmd" on Windows, or ".sh" otherwise.
		/// </remarks>
		/// <param name="basename">String script file basename</param>
		/// <returns>String script file name</returns>
		public static string appendScriptExtension(string basename)
		{
			return basename + (WINDOWS ? ".cmd" : ".sh");
		}

		/// <summary>Returns a command to run the given script.</summary>
		/// <remarks>
		/// Returns a command to run the given script.  The script interpreter is
		/// inferred by platform: cmd on Windows or bash otherwise.
		/// </remarks>
		/// <param name="script">File script to run</param>
		/// <returns>String[] command to run the script</returns>
		public static string[] getRunScriptCommand(java.io.File script)
		{
			string absolutePath = script.getAbsolutePath();
			return WINDOWS ? new string[] { "cmd", "/c", absolutePath } : new string[] { "/bin/bash"
				, absolutePath };
		}

		/// <summary>a Unix command to set permission</summary>
		public const string SET_PERMISSION_COMMAND = "chmod";

		/// <summary>a Unix command to set owner</summary>
		public const string SET_OWNER_COMMAND = "chown";

		/// <summary>a Unix command to set the change user's groups list</summary>
		public const string SET_GROUP_COMMAND = "chgrp";

		/// <summary>a Unix command to create a link</summary>
		public const string LINK_COMMAND = "ln";

		/// <summary>a Unix command to get a link target</summary>
		public const string READ_LINK_COMMAND = "readlink";

		/// <summary>Time after which the executing script would be timedout</summary>
		protected internal long timeOutInterval = 0L;

		/// <summary>If or not script timed out</summary>
		private java.util.concurrent.atomic.AtomicBoolean timedOut;

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
		private static string checkHadoopHome()
		{
			// first check the Dflag hadoop.home.dir with JVM scope
			string home = Sharpen.Runtime.getProperty("hadoop.home.dir");
			// fall back to the system/user-global env variable
			if (home == null)
			{
				home = Sharpen.Runtime.getenv("HADOOP_HOME");
			}
			try
			{
				// couldn't find either setting for hadoop's home directory
				if (home == null)
				{
					throw new System.IO.IOException("HADOOP_HOME or hadoop.home.dir are not set.");
				}
				if (home.StartsWith("\"") && home.EndsWith("\""))
				{
					home = Sharpen.Runtime.substring(home, 1, home.Length - 1);
				}
				// check that the home setting is actually a directory that exists
				java.io.File homedir = new java.io.File(home);
				if (!homedir.isAbsolute() || !homedir.exists() || !homedir.isDirectory())
				{
					throw new System.IO.IOException("Hadoop home directory " + homedir + " does not exist, is not a directory, or is not an absolute path."
						);
				}
				home = homedir.getCanonicalPath();
			}
			catch (System.IO.IOException ioe)
			{
				if (LOG.isDebugEnabled())
				{
					LOG.debug("Failed to detect a valid hadoop home directory", ioe);
				}
				home = null;
			}
			return home;
		}

		private static string HADOOP_HOME_DIR = checkHadoopHome();

		// Public getter, throws an exception if HADOOP_HOME failed validation
		// checks and is being referenced downstream.
		/// <exception cref="System.IO.IOException"/>
		public static string getHadoopHome()
		{
			if (HADOOP_HOME_DIR == null)
			{
				throw new System.IO.IOException("Misconfigured HADOOP_HOME cannot be referenced."
					);
			}
			return HADOOP_HOME_DIR;
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
		public static string getQualifiedBinPath(string executable)
		{
			// construct hadoop bin path to the specified executable
			string fullExeName = HADOOP_HOME_DIR + java.io.File.separator + "bin" + java.io.File
				.separator + executable;
			java.io.File exeFile = new java.io.File(fullExeName);
			if (!exeFile.exists())
			{
				throw new System.IO.IOException("Could not locate executable " + fullExeName + " in the Hadoop binaries."
					);
			}
			return exeFile.getCanonicalPath();
		}

		/// <summary>a Windows utility to emulate Unix commands</summary>
		public static readonly string WINUTILS = getWinUtilsPath();

		public static string getWinUtilsPath()
		{
			string winUtilsPath = null;
			try
			{
				if (WINDOWS)
				{
					winUtilsPath = getQualifiedBinPath("winutils.exe");
				}
			}
			catch (System.IO.IOException ioe)
			{
				LOG.error("Failed to locate the winutils binary in the hadoop binary path", ioe);
			}
			return winUtilsPath;
		}

		public static readonly bool isSetsidAvailable = isSetsidSupported();

		private static bool isSetsidSupported()
		{
			if (org.apache.hadoop.util.Shell.WINDOWS)
			{
				return false;
			}
			org.apache.hadoop.util.Shell.ShellCommandExecutor shexec = null;
			bool setsidSupported = true;
			try
			{
				string[] args = new string[] { "setsid", "bash", "-c", "echo $$" };
				shexec = new org.apache.hadoop.util.Shell.ShellCommandExecutor(args);
				shexec.execute();
			}
			catch (System.IO.IOException)
			{
				LOG.debug("setsid is not available on this machine. So not using it.");
				setsidSupported = false;
			}
			finally
			{
				// handle the exit code
				if (LOG.isDebugEnabled())
				{
					LOG.debug("setsid exited with exit code " + (shexec != null ? shexec.getExitCode(
						) : "(null executor)"));
				}
			}
			return setsidSupported;
		}

		/// <summary>Token separator regex used to parse Shell tool outputs</summary>
		public static readonly string TOKEN_SEPARATOR_REGEX = WINDOWS ? "[|\n\r]" : "[ \t\n\r\f]";

		private long interval;

		private long lastTime;

		private readonly bool redirectErrorStream;

		private System.Collections.Generic.IDictionary<string, string> environment;

		private java.io.File dir;

		private java.lang.Process process;

		private int exitCode;

		/// <summary>If or not script finished executing</summary>
		private volatile java.util.concurrent.atomic.AtomicBoolean completed;

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
		protected internal virtual void setEnvironment(System.Collections.Generic.IDictionary
			<string, string> env)
		{
			this.environment = env;
		}

		/// <summary>set the working directory</summary>
		/// <param name="dir">The directory where the command would be executed</param>
		protected internal virtual void setWorkingDirectory(java.io.File dir)
		{
			this.dir = dir;
		}

		/// <summary>check to see if a command needs to be executed and execute if needed</summary>
		/// <exception cref="System.IO.IOException"/>
		protected internal virtual void run()
		{
			if (lastTime + interval > org.apache.hadoop.util.Time.monotonicNow())
			{
				return;
			}
			exitCode = 0;
			// reset for next run
			runCommand();
		}

		/// <summary>Run a command</summary>
		/// <exception cref="System.IO.IOException"/>
		private void runCommand()
		{
			java.lang.ProcessBuilder builder = new java.lang.ProcessBuilder(getExecString());
			java.util.Timer timeOutTimer = null;
			org.apache.hadoop.util.Shell.ShellTimeoutTimerTask timeoutTimerTask = null;
			timedOut = new java.util.concurrent.atomic.AtomicBoolean(false);
			completed = new java.util.concurrent.atomic.AtomicBoolean(false);
			if (environment != null)
			{
				builder.environment().putAll(this.environment);
			}
			if (dir != null)
			{
				builder.directory(this.dir);
			}
			builder.redirectErrorStream(redirectErrorStream);
			if (org.apache.hadoop.util.Shell.WINDOWS)
			{
				lock (WindowsProcessLaunchLock)
				{
					// To workaround the race condition issue with child processes
					// inheriting unintended handles during process launch that can
					// lead to hangs on reading output and error streams, we
					// serialize process creation. More info available at:
					// http://support.microsoft.com/kb/315939
					process = builder.start();
				}
			}
			else
			{
				process = builder.start();
			}
			if (timeOutInterval > 0)
			{
				timeOutTimer = new java.util.Timer("Shell command timeout");
				timeoutTimerTask = new org.apache.hadoop.util.Shell.ShellTimeoutTimerTask(this);
				//One time scheduling.
				timeOutTimer.schedule(timeoutTimerTask, timeOutInterval);
			}
			java.io.BufferedReader errReader = new java.io.BufferedReader(new java.io.InputStreamReader
				(process.getErrorStream(), java.nio.charset.Charset.defaultCharset()));
			java.io.BufferedReader inReader = new java.io.BufferedReader(new java.io.InputStreamReader
				(process.getInputStream(), java.nio.charset.Charset.defaultCharset()));
			System.Text.StringBuilder errMsg = new System.Text.StringBuilder();
			// read error and input streams as this would free up the buffers
			// free the error stream buffer
			java.lang.Thread errThread = new _Thread_506(errReader, errMsg);
			try
			{
				errThread.start();
			}
			catch (System.InvalidOperationException)
			{
			}
			catch (System.OutOfMemoryException oe)
			{
				LOG.error("Caught " + oe + ". One possible reason is that ulimit" + " setting of 'max user processes' is too low. If so, do"
					 + " 'ulimit -u <largerNum>' and try again.");
				throw;
			}
			try
			{
				parseExecResult(inReader);
				// parse the output
				// clear the input stream buffer
				string line = inReader.readLine();
				while (line != null)
				{
					line = inReader.readLine();
				}
				// wait for the process to finish and check the exit code
				exitCode = process.waitFor();
				// make sure that the error thread exits
				joinThread(errThread);
				completed.set(true);
				//the timeout thread handling
				//taken care in finally block
				if (exitCode != 0)
				{
					throw new org.apache.hadoop.util.Shell.ExitCodeException(exitCode, errMsg.ToString
						());
				}
			}
			catch (System.Exception ie)
			{
				throw new System.IO.IOException(ie.ToString());
			}
			finally
			{
				if (timeOutTimer != null)
				{
					timeOutTimer.cancel();
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
					java.io.InputStream stdout = process.getInputStream();
					lock (stdout)
					{
						inReader.close();
					}
				}
				catch (System.IO.IOException ioe)
				{
					LOG.warn("Error while closing the input stream", ioe);
				}
				if (!completed.get())
				{
					errThread.interrupt();
					joinThread(errThread);
				}
				try
				{
					java.io.InputStream stderr = process.getErrorStream();
					lock (stderr)
					{
						errReader.close();
					}
				}
				catch (System.IO.IOException ioe)
				{
					LOG.warn("Error while closing the error stream", ioe);
				}
				process.destroy();
				lastTime = org.apache.hadoop.util.Time.monotonicNow();
			}
		}

		private sealed class _Thread_506 : java.lang.Thread
		{
			public _Thread_506(java.io.BufferedReader errReader, System.Text.StringBuilder errMsg
				)
			{
				this.errReader = errReader;
				this.errMsg = errMsg;
			}

			public override void run()
			{
				try
				{
					string line = errReader.readLine();
					while ((line != null) && !this.isInterrupted())
					{
						errMsg.Append(line);
						errMsg.Append(Sharpen.Runtime.getProperty("line.separator"));
						line = errReader.readLine();
					}
				}
				catch (System.IO.IOException ioe)
				{
					org.apache.hadoop.util.Shell.LOG.warn("Error reading the error stream", ioe);
				}
			}

			private readonly java.io.BufferedReader errReader;

			private readonly System.Text.StringBuilder errMsg;
		}

		private static void joinThread(java.lang.Thread t)
		{
			while (t.isAlive())
			{
				try
				{
					t.join();
				}
				catch (System.Exception ie)
				{
					if (LOG.isWarnEnabled())
					{
						LOG.warn("Interrupted while joining on: " + t, ie);
					}
					t.interrupt();
				}
			}
		}

		// propagate interrupt
		/// <summary>return an array containing the command name & its parameters</summary>
		protected internal abstract string[] getExecString();

		/// <summary>Parse the execution result</summary>
		/// <exception cref="System.IO.IOException"/>
		protected internal abstract void parseExecResult(java.io.BufferedReader lines);

		/// <summary>Get the environment variable</summary>
		public virtual string getEnvironment(string env)
		{
			return environment[env];
		}

		/// <summary>get the current sub-process executing the given command</summary>
		/// <returns>process executing the command</returns>
		public virtual java.lang.Process getProcess()
		{
			return process;
		}

		/// <summary>get the exit code</summary>
		/// <returns>the exit code of the process</returns>
		public virtual int getExitCode()
		{
			return exitCode;
		}

		/// <summary>This is an IOException with exit code added.</summary>
		[System.Serializable]
		public class ExitCodeException : System.IO.IOException
		{
			private readonly int exitCode;

			public ExitCodeException(int exitCode, string message)
				: base(message)
			{
				this.exitCode = exitCode;
			}

			public virtual int getExitCode()
			{
				return exitCode;
			}

			public override string ToString()
			{
				java.lang.StringBuilder sb = new java.lang.StringBuilder("ExitCodeException ");
				sb.Append("exitCode=").Append(exitCode).Append(": ");
				sb.Append(base.Message);
				return sb.ToString();
			}
		}

		public interface CommandExecutor
		{
			/// <exception cref="System.IO.IOException"/>
			void execute();

			/// <exception cref="System.IO.IOException"/>
			int getExitCode();

			/// <exception cref="System.IO.IOException"/>
			string getOutput();

			void close();
		}

		/// <summary>A simple shell command executor.</summary>
		/// <remarks>
		/// A simple shell command executor.
		/// <code>ShellCommandExecutor</code>should be used in cases where the output
		/// of the command needs no explicit parsing and where the command, working
		/// directory and the environment remains unchanged. The output of the command
		/// is stored as-is and is expected to be small.
		/// </remarks>
		public class ShellCommandExecutor : org.apache.hadoop.util.Shell, org.apache.hadoop.util.Shell.CommandExecutor
		{
			private string[] command;

			private System.Text.StringBuilder output;

			public ShellCommandExecutor(string[] execString)
				: this(execString, null)
			{
			}

			public ShellCommandExecutor(string[] execString, java.io.File dir)
				: this(execString, dir, null)
			{
			}

			public ShellCommandExecutor(string[] execString, java.io.File dir, System.Collections.Generic.IDictionary
				<string, string> env)
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
			public ShellCommandExecutor(string[] execString, java.io.File dir, System.Collections.Generic.IDictionary
				<string, string> env, long timeout)
			{
				command = execString.MemberwiseClone();
				if (dir != null)
				{
					setWorkingDirectory(dir);
				}
				if (env != null)
				{
					setEnvironment(env);
				}
				timeOutInterval = timeout;
			}

			/// <summary>Execute the shell command.</summary>
			/// <exception cref="System.IO.IOException"/>
			public virtual void execute()
			{
				this.run();
			}

			protected internal override string[] getExecString()
			{
				return command;
			}

			/// <exception cref="System.IO.IOException"/>
			protected internal override void parseExecResult(java.io.BufferedReader lines)
			{
				output = new System.Text.StringBuilder();
				char[] buf = new char[512];
				int nRead;
				while ((nRead = lines.read(buf, 0, buf.Length)) > 0)
				{
					output.Append(buf, 0, nRead);
				}
			}

			/// <summary>Get the output of the shell command.</summary>
			public virtual string getOutput()
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
				java.lang.StringBuilder builder = new java.lang.StringBuilder();
				string[] args = getExecString();
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

			public virtual void close()
			{
			}
		}

		/// <summary>
		/// To check if the passed script to shell command executor timed out or
		/// not.
		/// </summary>
		/// <returns>if the script timed out.</returns>
		public virtual bool isTimedOut()
		{
			return timedOut.get();
		}

		/// <summary>Set if the command has timed out.</summary>
		private void setTimedOut()
		{
			this.timedOut.set(true);
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
		public static string execCommand(params string[] cmd)
		{
			return execCommand(null, cmd, 0L);
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
		public static string execCommand(System.Collections.Generic.IDictionary<string, string
			> env, string[] cmd, long timeout)
		{
			org.apache.hadoop.util.Shell.ShellCommandExecutor exec = new org.apache.hadoop.util.Shell.ShellCommandExecutor
				(cmd, null, env, timeout);
			exec.execute();
			return exec.getOutput();
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
		public static string execCommand(System.Collections.Generic.IDictionary<string, string
			> env, params string[] cmd)
		{
			return execCommand(env, cmd, 0L);
		}

		/// <summary>Timer which is used to timeout scripts spawned off by shell.</summary>
		private class ShellTimeoutTimerTask : java.util.TimerTask
		{
			private org.apache.hadoop.util.Shell shell;

			public ShellTimeoutTimerTask(org.apache.hadoop.util.Shell shell)
			{
				this.shell = shell;
			}

			public override void run()
			{
				java.lang.Process p = shell.getProcess();
				try
				{
					p.exitValue();
				}
				catch (System.Exception)
				{
					//Process has not terminated.
					//So check if it has completed 
					//if not just destroy it.
					if (p != null && !shell.completed.get())
					{
						shell.setTimedOut();
						p.destroy();
					}
				}
			}
		}
	}
}
