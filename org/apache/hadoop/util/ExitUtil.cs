using Sharpen;

namespace org.apache.hadoop.util
{
	/// <summary>Facilitates hooking process termination for tests and debugging.</summary>
	public sealed class ExitUtil
	{
		private static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.util.ExitUtil))
			.getName());

		private static volatile bool systemExitDisabled = false;

		private static volatile bool systemHaltDisabled = false;

		private static volatile org.apache.hadoop.util.ExitUtil.ExitException firstExitException;

		private static volatile org.apache.hadoop.util.ExitUtil.HaltException firstHaltException;

		[System.Serializable]
		public class ExitException : System.Exception
		{
			private const long serialVersionUID = 1L;

			public readonly int status;

			public ExitException(int status, string msg)
				: base(msg)
			{
				this.status = status;
			}
		}

		[System.Serializable]
		public class HaltException : System.Exception
		{
			private const long serialVersionUID = 1L;

			public readonly int status;

			public HaltException(int status, string msg)
				: base(msg)
			{
				this.status = status;
			}
		}

		/// <summary>Disable the use of System.exit for testing.</summary>
		public static void disableSystemExit()
		{
			systemExitDisabled = true;
		}

		/// <summary>
		/// Disable the use of
		/// <c>Runtime.getRuntime().halt()</c>
		/// for testing.
		/// </summary>
		public static void disableSystemHalt()
		{
			systemHaltDisabled = true;
		}

		/// <returns>true if terminate has been called</returns>
		public static bool terminateCalled()
		{
			// Either we set this member or we actually called System#exit
			return firstExitException != null;
		}

		/// <returns>true if halt has been called</returns>
		public static bool haltCalled()
		{
			return firstHaltException != null;
		}

		/// <returns>the first ExitException thrown, null if none thrown yet</returns>
		public static org.apache.hadoop.util.ExitUtil.ExitException getFirstExitException
			()
		{
			return firstExitException;
		}

		/// <returns>
		/// the first
		/// <c>HaltException</c>
		/// thrown, null if none thrown yet
		/// </returns>
		public static org.apache.hadoop.util.ExitUtil.HaltException getFirstHaltException
			()
		{
			return firstHaltException;
		}

		/// <summary>Reset the tracking of process termination.</summary>
		/// <remarks>
		/// Reset the tracking of process termination. This is for use in unit tests
		/// where one test in the suite expects an exit but others do not.
		/// </remarks>
		public static void resetFirstExitException()
		{
			firstExitException = null;
		}

		public static void resetFirstHaltException()
		{
			firstHaltException = null;
		}

		/// <summary>Terminate the current process.</summary>
		/// <remarks>
		/// Terminate the current process. Note that terminate is the *only* method
		/// that should be used to terminate the daemon processes.
		/// </remarks>
		/// <param name="status">exit code</param>
		/// <param name="msg">
		/// message used to create the
		/// <c>ExitException</c>
		/// </param>
		/// <exception cref="ExitException">if System.exit is disabled for test purposes</exception>
		/// <exception cref="org.apache.hadoop.util.ExitUtil.ExitException"/>
		public static void terminate(int status, string msg)
		{
			LOG.info("Exiting with status " + status);
			if (systemExitDisabled)
			{
				org.apache.hadoop.util.ExitUtil.ExitException ee = new org.apache.hadoop.util.ExitUtil.ExitException
					(status, msg);
				LOG.fatal("Terminate called", ee);
				if (null == firstExitException)
				{
					firstExitException = ee;
				}
				throw ee;
			}
			System.Environment.Exit(status);
		}

		/// <summary>Forcibly terminates the currently running Java virtual machine.</summary>
		/// <param name="status">exit code</param>
		/// <param name="msg">
		/// message used to create the
		/// <c>HaltException</c>
		/// </param>
		/// <exception cref="HaltException">if Runtime.getRuntime().halt() is disabled for test purposes
		/// 	</exception>
		/// <exception cref="org.apache.hadoop.util.ExitUtil.HaltException"/>
		public static void halt(int status, string msg)
		{
			LOG.info("Halt with status " + status + " Message: " + msg);
			if (systemHaltDisabled)
			{
				org.apache.hadoop.util.ExitUtil.HaltException ee = new org.apache.hadoop.util.ExitUtil.HaltException
					(status, msg);
				LOG.fatal("Halt called", ee);
				if (null == firstHaltException)
				{
					firstHaltException = ee;
				}
				throw ee;
			}
			java.lang.Runtime.getRuntime().halt(status);
		}

		/// <summary>
		/// Like
		/// <see>terminate(int, String)</see>
		/// but uses the given throwable to
		/// initialize the ExitException.
		/// </summary>
		/// <param name="status"/>
		/// <param name="t">throwable used to create the ExitException</param>
		/// <exception cref="ExitException">if System.exit is disabled for test purposes</exception>
		/// <exception cref="org.apache.hadoop.util.ExitUtil.ExitException"/>
		public static void terminate(int status, System.Exception t)
		{
			terminate(status, org.apache.hadoop.util.StringUtils.stringifyException(t));
		}

		/// <summary>Forcibly terminates the currently running Java virtual machine.</summary>
		/// <param name="status"/>
		/// <param name="t"/>
		/// <exception cref="ExitException"/>
		/// <exception cref="org.apache.hadoop.util.ExitUtil.HaltException"/>
		public static void halt(int status, System.Exception t)
		{
			halt(status, org.apache.hadoop.util.StringUtils.stringifyException(t));
		}

		/// <summary>
		/// Like
		/// <see>terminate(int, String)</see>
		/// without a message.
		/// </summary>
		/// <param name="status"/>
		/// <exception cref="ExitException">if System.exit is disabled for test purposes</exception>
		/// <exception cref="org.apache.hadoop.util.ExitUtil.ExitException"/>
		public static void terminate(int status)
		{
			terminate(status, "ExitException");
		}

		/// <summary>Forcibly terminates the currently running Java virtual machine.</summary>
		/// <param name="status"/>
		/// <exception cref="ExitException"/>
		/// <exception cref="org.apache.hadoop.util.ExitUtil.HaltException"/>
		public static void halt(int status)
		{
			halt(status, "HaltException");
		}
	}
}
