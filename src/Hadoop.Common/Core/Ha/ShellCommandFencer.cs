using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Reflection;
using Com.Google.Common.Annotations;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Util;


namespace Org.Apache.Hadoop.HA
{
	/// <summary>Fencing method that runs a shell command.</summary>
	/// <remarks>
	/// Fencing method that runs a shell command. It should be specified
	/// in the fencing configuration like:<br />
	/// <code>
	/// shell(/path/to/my/script.sh arg1 arg2 ...)
	/// </code><br />
	/// The string between '(' and ')' is passed directly to a bash shell
	/// (cmd.exe on Windows) and may not include any closing parentheses.<p>
	/// The shell command will be run with an environment set up to contain
	/// all of the current Hadoop configuration variables, with the '_' character
	/// replacing any '.' characters in the configuration keys.<p>
	/// If the shell command returns an exit code of 0, the fencing is
	/// determined to be successful. If it returns any other exit code, the
	/// fencing was not successful and the next fencing method in the list
	/// will be attempted.<p>
	/// <em>Note:</em> this fencing method does not implement any timeout.
	/// If timeouts are necessary, they should be implemented in the shell
	/// script itself (eg by forking a subshell to kill its parent in
	/// some number of seconds).
	/// </remarks>
	public class ShellCommandFencer : Configured, FenceMethod
	{
		/// <summary>Length at which to abbreviate command in long messages</summary>
		private const int AbbrevLength = 20;

		/// <summary>Prefix for target parameters added to the environment</summary>
		private const string TargetPrefix = "target_";

		[VisibleForTesting]
		internal static Log Log = LogFactory.GetLog(typeof(ShellCommandFencer));

		/// <exception cref="Org.Apache.Hadoop.HA.BadFencingConfigurationException"/>
		public virtual void CheckArgs(string args)
		{
			if (args == null || args.IsEmpty())
			{
				throw new BadFencingConfigurationException("No argument passed to 'shell' fencing method"
					);
			}
		}

		// Nothing else we can really check without actually running the command
		public virtual bool TryFence(HAServiceTarget target, string cmd)
		{
			ProcessStartInfo builder;
			if (!Shell.Windows)
			{
				builder = new ProcessStartInfo("bash", "-e", "-c", cmd);
			}
			else
			{
				builder = new ProcessStartInfo("cmd.exe", "/c", cmd);
			}
			SetConfAsEnvVars(builder.EnvironmentVariables);
			AddTargetInfoAsEnvVars(target, builder.EnvironmentVariables);
			SystemProcess p;
			try
			{
				p = builder.Start();
				p.GetOutputStream().Close();
			}
			catch (IOException e)
			{
				Log.Warn("Unable to execute " + cmd, e);
				return false;
			}
			string pid = TryGetPid(p);
			Log.Info("Launched fencing command '" + cmd + "' with " + ((pid != null) ? ("pid "
				 + pid) : "unknown pid"));
			string logPrefix = Abbreviate(cmd, AbbrevLength);
			if (pid != null)
			{
				logPrefix = "[PID " + pid + "] " + logPrefix;
			}
			// Pump logs to stderr
			StreamPumper errPumper = new StreamPumper(Log, logPrefix, p.GetErrorStream(), StreamPumper.StreamType
				.Stderr);
			errPumper.Start();
			StreamPumper outPumper = new StreamPumper(Log, logPrefix, p.GetInputStream(), StreamPumper.StreamType
				.Stdout);
			outPumper.Start();
			int rc;
			try
			{
				rc = p.WaitFor();
				errPumper.Join();
				outPumper.Join();
			}
			catch (Exception)
			{
				Log.Warn("Interrupted while waiting for fencing command: " + cmd);
				return false;
			}
			return rc == 0;
		}

		/// <summary>
		/// Abbreviate a string by putting '...' in the middle of it,
		/// in an attempt to keep logs from getting too messy.
		/// </summary>
		/// <param name="cmd">the string to abbreviate</param>
		/// <param name="len">maximum length to abbreviate to</param>
		/// <returns>abbreviated string</returns>
		internal static string Abbreviate(string cmd, int len)
		{
			if (cmd.Length > len && len >= 5)
			{
				int firstHalf = (len - 3) / 2;
				int rem = len - firstHalf - 3;
				return Runtime.Substring(cmd, 0, firstHalf) + "..." + Runtime.Substring
					(cmd, cmd.Length - rem);
			}
			else
			{
				return cmd;
			}
		}

		/// <summary>
		/// Attempt to use evil reflection tricks to determine the
		/// pid of a launched process.
		/// </summary>
		/// <remarks>
		/// Attempt to use evil reflection tricks to determine the
		/// pid of a launched process. This is helpful to ops
		/// if debugging a fencing process that might have gone
		/// wrong. If running on a system or JVM where this doesn't
		/// work, it will simply return null.
		/// </remarks>
		private static string TryGetPid(SystemProcess p)
		{
			try
			{
				Type clazz = p.GetType();
				if (clazz.FullName.Equals("java.lang.UNIXProcess"))
				{
					FieldInfo f = Runtime.GetDeclaredField(clazz, "pid");
					return f.GetInt(p).ToString();
				}
				else
				{
					Log.Trace("Unable to determine pid for " + p + " since it is not a UNIXProcess");
					return null;
				}
			}
			catch (Exception t)
			{
				Log.Trace("Unable to determine pid for " + p, t);
				return null;
			}
		}

		/// <summary>
		/// Set the environment of the subprocess to be the Configuration,
		/// with '.'s replaced by '_'s.
		/// </summary>
		private void SetConfAsEnvVars(IDictionary<string, string> env)
		{
			foreach (KeyValuePair<string, string> pair in GetConf())
			{
				env[pair.Key.Replace('.', '_')] = pair.Value;
			}
		}

		/// <summary>
		/// Add information about the target to the the environment of the
		/// subprocess.
		/// </summary>
		/// <param name="target"/>
		/// <param name="environment"/>
		private void AddTargetInfoAsEnvVars(HAServiceTarget target, IDictionary<string, string
			> environment)
		{
			foreach (KeyValuePair<string, string> e in target.GetFencingParameters())
			{
				string key = TargetPrefix + e.Key;
				key = key.Replace('.', '_');
				environment[key] = e.Value;
			}
		}
	}
}
