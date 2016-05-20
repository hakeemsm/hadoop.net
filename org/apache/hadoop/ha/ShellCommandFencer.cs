using Sharpen;

namespace org.apache.hadoop.ha
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
	public class ShellCommandFencer : org.apache.hadoop.conf.Configured, org.apache.hadoop.ha.FenceMethod
	{
		/// <summary>Length at which to abbreviate command in long messages</summary>
		private const int ABBREV_LENGTH = 20;

		/// <summary>Prefix for target parameters added to the environment</summary>
		private const string TARGET_PREFIX = "target_";

		[com.google.common.annotations.VisibleForTesting]
		internal static org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.ha.ShellCommandFencer
			)));

		/// <exception cref="org.apache.hadoop.ha.BadFencingConfigurationException"/>
		public virtual void checkArgs(string args)
		{
			if (args == null || args.isEmpty())
			{
				throw new org.apache.hadoop.ha.BadFencingConfigurationException("No argument passed to 'shell' fencing method"
					);
			}
		}

		// Nothing else we can really check without actually running the command
		public virtual bool tryFence(org.apache.hadoop.ha.HAServiceTarget target, string 
			cmd)
		{
			java.lang.ProcessBuilder builder;
			if (!org.apache.hadoop.util.Shell.WINDOWS)
			{
				builder = new java.lang.ProcessBuilder("bash", "-e", "-c", cmd);
			}
			else
			{
				builder = new java.lang.ProcessBuilder("cmd.exe", "/c", cmd);
			}
			setConfAsEnvVars(builder.environment());
			addTargetInfoAsEnvVars(target, builder.environment());
			java.lang.Process p;
			try
			{
				p = builder.start();
				p.getOutputStream().close();
			}
			catch (System.IO.IOException e)
			{
				LOG.warn("Unable to execute " + cmd, e);
				return false;
			}
			string pid = tryGetPid(p);
			LOG.info("Launched fencing command '" + cmd + "' with " + ((pid != null) ? ("pid "
				 + pid) : "unknown pid"));
			string logPrefix = abbreviate(cmd, ABBREV_LENGTH);
			if (pid != null)
			{
				logPrefix = "[PID " + pid + "] " + logPrefix;
			}
			// Pump logs to stderr
			org.apache.hadoop.ha.StreamPumper errPumper = new org.apache.hadoop.ha.StreamPumper
				(LOG, logPrefix, p.getErrorStream(), org.apache.hadoop.ha.StreamPumper.StreamType
				.STDERR);
			errPumper.start();
			org.apache.hadoop.ha.StreamPumper outPumper = new org.apache.hadoop.ha.StreamPumper
				(LOG, logPrefix, p.getInputStream(), org.apache.hadoop.ha.StreamPumper.StreamType
				.STDOUT);
			outPumper.start();
			int rc;
			try
			{
				rc = p.waitFor();
				errPumper.join();
				outPumper.join();
			}
			catch (System.Exception)
			{
				LOG.warn("Interrupted while waiting for fencing command: " + cmd);
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
		internal static string abbreviate(string cmd, int len)
		{
			if (cmd.Length > len && len >= 5)
			{
				int firstHalf = (len - 3) / 2;
				int rem = len - firstHalf - 3;
				return Sharpen.Runtime.substring(cmd, 0, firstHalf) + "..." + Sharpen.Runtime.substring
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
		private static string tryGetPid(java.lang.Process p)
		{
			try
			{
				java.lang.Class clazz = Sharpen.Runtime.getClassForObject(p);
				if (clazz.getName().Equals("java.lang.UNIXProcess"))
				{
					java.lang.reflect.Field f = clazz.getDeclaredField("pid");
					f.setAccessible(true);
					return Sharpen.Runtime.getStringValueOf(f.getInt(p));
				}
				else
				{
					LOG.trace("Unable to determine pid for " + p + " since it is not a UNIXProcess");
					return null;
				}
			}
			catch (System.Exception t)
			{
				LOG.trace("Unable to determine pid for " + p, t);
				return null;
			}
		}

		/// <summary>
		/// Set the environment of the subprocess to be the Configuration,
		/// with '.'s replaced by '_'s.
		/// </summary>
		private void setConfAsEnvVars(System.Collections.Generic.IDictionary<string, string
			> env)
		{
			foreach (System.Collections.Generic.KeyValuePair<string, string> pair in getConf(
				))
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
		private void addTargetInfoAsEnvVars(org.apache.hadoop.ha.HAServiceTarget target, 
			System.Collections.Generic.IDictionary<string, string> environment)
		{
			foreach (System.Collections.Generic.KeyValuePair<string, string> e in target.getFencingParameters
				())
			{
				string key = TARGET_PREFIX + e.Key;
				key = key.Replace('.', '_');
				environment[key] = e.Value;
			}
		}
	}
}
