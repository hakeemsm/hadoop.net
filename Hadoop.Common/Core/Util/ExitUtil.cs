using System;
using Org.Apache.Commons.Logging;


namespace Org.Apache.Hadoop.Util
{
	/// <summary>Facilitates hooking process termination for tests and debugging.</summary>
	public sealed class ExitUtil
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(ExitUtil).FullName);

		private static volatile bool systemExitDisabled = false;

		private static volatile bool systemHaltDisabled = false;

		private static volatile ExitUtil.ExitException firstExitException;

		private static volatile ExitUtil.HaltException firstHaltException;

		[System.Serializable]
		public class ExitException : RuntimeException
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
		public class HaltException : RuntimeException
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
		public static void DisableSystemExit()
		{
			systemExitDisabled = true;
		}

		/// <summary>
		/// Disable the use of
		/// <c>Runtime.getRuntime().halt()</c>
		/// for testing.
		/// </summary>
		public static void DisableSystemHalt()
		{
			systemHaltDisabled = true;
		}

		/// <returns>true if terminate has been called</returns>
		public static bool TerminateCalled()
		{
			// Either we set this member or we actually called System#exit
			return firstExitException != null;
		}

		/// <returns>true if halt has been called</returns>
		public static bool HaltCalled()
		{
			return firstHaltException != null;
		}

		/// <returns>the first ExitException thrown, null if none thrown yet</returns>
		public static ExitUtil.ExitException GetFirstExitException()
		{
			return firstExitException;
		}

		/// <returns>
		/// the first
		/// <c>HaltException</c>
		/// thrown, null if none thrown yet
		/// </returns>
		public static ExitUtil.HaltException GetFirstHaltException()
		{
			return firstHaltException;
		}

		/// <summary>Reset the tracking of process termination.</summary>
		/// <remarks>
		/// Reset the tracking of process termination. This is for use in unit tests
		/// where one test in the suite expects an exit but others do not.
		/// </remarks>
		public static void ResetFirstExitException()
		{
			firstExitException = null;
		}

		public static void ResetFirstHaltException()
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
		/// <exception cref="Org.Apache.Hadoop.Util.ExitUtil.ExitException"/>
		public static void Terminate(int status, string msg)
		{
			Log.Info("Exiting with status " + status);
			if (systemExitDisabled)
			{
				ExitUtil.ExitException ee = new ExitUtil.ExitException(status, msg);
				Log.Fatal("Terminate called", ee);
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
		/// <exception cref="Org.Apache.Hadoop.Util.ExitUtil.HaltException"/>
		public static void Halt(int status, string msg)
		{
			Log.Info("Halt with status " + status + " Message: " + msg);
			if (systemHaltDisabled)
			{
				ExitUtil.HaltException ee = new ExitUtil.HaltException(status, msg);
				Log.Fatal("Halt called", ee);
				if (null == firstHaltException)
				{
					firstHaltException = ee;
				}
				throw ee;
			}
			Runtime.GetRuntime().Halt(status);
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
		/// <exception cref="Org.Apache.Hadoop.Util.ExitUtil.ExitException"/>
		public static void Terminate(int status, Exception t)
		{
			Terminate(status, StringUtils.StringifyException(t));
		}

		/// <summary>Forcibly terminates the currently running Java virtual machine.</summary>
		/// <param name="status"/>
		/// <param name="t"/>
		/// <exception cref="ExitException"/>
		/// <exception cref="Org.Apache.Hadoop.Util.ExitUtil.HaltException"/>
		public static void Halt(int status, Exception t)
		{
			Halt(status, StringUtils.StringifyException(t));
		}

		/// <summary>
		/// Like
		/// <see>terminate(int, String)</see>
		/// without a message.
		/// </summary>
		/// <param name="status"/>
		/// <exception cref="ExitException">if System.exit is disabled for test purposes</exception>
		/// <exception cref="Org.Apache.Hadoop.Util.ExitUtil.ExitException"/>
		public static void Terminate(int status)
		{
			Terminate(status, "ExitException");
		}

		/// <summary>Forcibly terminates the currently running Java virtual machine.</summary>
		/// <param name="status"/>
		/// <exception cref="ExitException"/>
		/// <exception cref="Org.Apache.Hadoop.Util.ExitUtil.HaltException"/>
		public static void Halt(int status)
		{
			Halt(status, "HaltException");
		}
	}
}
