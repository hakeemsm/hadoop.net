using Sharpen;

namespace org.apache.hadoop.util
{
	/// <summary>This class logs a message whenever we're about to exit on a UNIX signal.
	/// 	</summary>
	/// <remarks>
	/// This class logs a message whenever we're about to exit on a UNIX signal.
	/// This is helpful for determining the root cause of a process' exit.
	/// For example, if the process exited because the system administrator
	/// ran a standard "kill," you would see 'EXITING ON SIGNAL SIGTERM' in the log.
	/// </remarks>
	[System.Serializable]
	public sealed class SignalLogger
	{
		public static readonly org.apache.hadoop.util.SignalLogger INSTANCE = new org.apache.hadoop.util.SignalLogger
			();

		private bool registered = false;

		/// <summary>Our signal handler.</summary>
		private class Handler : sun.misc.SignalHandler
		{
			private readonly org.apache.hadoop.util.LogAdapter LOG;

			private readonly sun.misc.SignalHandler prevHandler;

			internal Handler(string name, org.apache.hadoop.util.LogAdapter LOG)
			{
				this.LOG = LOG;
				prevHandler = sun.misc.Signal.handle(new sun.misc.Signal(name), this);
			}

			/// <summary>Handle an incoming signal.</summary>
			/// <param name="signal">The incoming signal</param>
			public override void handle(sun.misc.Signal signal)
			{
				LOG.error("RECEIVED SIGNAL " + signal.getNumber() + ": SIG" + signal.getName());
				prevHandler.handle(signal);
			}
		}

		/// <summary>Register some signal handlers.</summary>
		/// <param name="LOG">The log4j logfile to use in the signal handlers.</param>
		public void register(org.apache.commons.logging.Log LOG)
		{
			register(org.apache.hadoop.util.LogAdapter.create(LOG));
		}

		internal void register(org.apache.hadoop.util.LogAdapter LOG)
		{
			if (org.apache.hadoop.util.SignalLogger.registered)
			{
				throw new System.InvalidOperationException("Can't re-install the signal handlers."
					);
			}
			org.apache.hadoop.util.SignalLogger.registered = true;
			java.lang.StringBuilder bld = new java.lang.StringBuilder();
			bld.Append("registered UNIX signal handlers for [");
			string[] SIGNALS = new string[] { "TERM", "HUP", "INT" };
			string separator = string.Empty;
			foreach (string signalName in SIGNALS)
			{
				try
				{
					new org.apache.hadoop.util.SignalLogger.Handler(signalName, LOG);
					bld.Append(separator);
					bld.Append(signalName);
					separator = ", ";
				}
				catch (System.Exception e)
				{
					LOG.debug(e);
				}
			}
			bld.Append("]");
			LOG.info(bld.ToString());
		}
	}
}
