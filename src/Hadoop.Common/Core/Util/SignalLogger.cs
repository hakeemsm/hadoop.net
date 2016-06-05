using System;
using System.Text;
using Org.Apache.Commons.Logging;

using Sun.Misc;

namespace Org.Apache.Hadoop.Util
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
		public static readonly SignalLogger Instance = new SignalLogger();

		private bool registered = false;

		/// <summary>Our signal handler.</summary>
		private class Handler : SignalHandler
		{
			private readonly LogAdapter Log;

			private readonly SignalHandler prevHandler;

			internal Handler(string name, LogAdapter Log)
			{
				this.Log = Log;
				prevHandler = Signal.Handle(new Signal(name), this);
			}

			/// <summary>Handle an incoming signal.</summary>
			/// <param name="signal">The incoming signal</param>
			public override void Handle(Signal signal)
			{
				Log.Error("RECEIVED SIGNAL " + signal.GetNumber() + ": SIG" + signal.GetName());
				prevHandler.Handle(signal);
			}
		}

		/// <summary>Register some signal handlers.</summary>
		/// <param name="Log">The log4j logfile to use in the signal handlers.</param>
		public void Register(Log Log)
		{
			Register(LogAdapter.Create(Log));
		}

		internal void Register(LogAdapter Log)
		{
			if (SignalLogger.registered)
			{
				throw new InvalidOperationException("Can't re-install the signal handlers.");
			}
			SignalLogger.registered = true;
			StringBuilder bld = new StringBuilder();
			bld.Append("registered UNIX signal handlers for [");
			string[] Signals = new string[] { "TERM", "HUP", "INT" };
			string separator = string.Empty;
			foreach (string signalName in Signals)
			{
				try
				{
					new SignalLogger.Handler(signalName, Log);
					bld.Append(separator);
					bld.Append(signalName);
					separator = ", ";
				}
				catch (Exception e)
				{
					Log.Debug(e);
				}
			}
			bld.Append("]");
			Log.Info(bld.ToString());
		}
	}
}
