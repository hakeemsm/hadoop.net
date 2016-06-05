

namespace Org.Apache.Hadoop.IO.File.Tfile
{
	/// <summary>A nano-second timer.</summary>
	public class NanoTimer
	{
		private long last = -1;

		private bool started = false;

		private long cumulate = 0;

		/// <summary>Constructor</summary>
		/// <param name="start">Start the timer upon construction.</param>
		public NanoTimer(bool start)
		{
			if (start)
			{
				this.Start();
			}
		}

		/// <summary>Start the timer.</summary>
		/// <remarks>
		/// Start the timer.
		/// Note: No effect if timer is already started.
		/// </remarks>
		public virtual void Start()
		{
			if (!this.started)
			{
				this.last = Runtime.NanoTime();
				this.started = true;
			}
		}

		/// <summary>Stop the timer.</summary>
		/// <remarks>
		/// Stop the timer.
		/// Note: No effect if timer is already stopped.
		/// </remarks>
		public virtual void Stop()
		{
			if (this.started)
			{
				this.started = false;
				this.cumulate += Runtime.NanoTime() - this.last;
			}
		}

		/// <summary>Read the timer.</summary>
		/// <returns>
		/// the elapsed time in nano-seconds. Note: If the timer is never
		/// started before, -1 is returned.
		/// </returns>
		public virtual long Read()
		{
			if (!Readable())
			{
				return -1;
			}
			return this.cumulate;
		}

		/// <summary>Reset the timer.</summary>
		public virtual void Reset()
		{
			this.last = -1;
			this.started = false;
			this.cumulate = 0;
		}

		/// <summary>Checking whether the timer is started</summary>
		/// <returns>true if timer is started.</returns>
		public virtual bool IsStarted()
		{
			return this.started;
		}

		/// <summary>Format the elapsed time to a human understandable string.</summary>
		/// <remarks>
		/// Format the elapsed time to a human understandable string.
		/// Note: If timer is never started, "ERR" will be returned.
		/// </remarks>
		public override string ToString()
		{
			if (!Readable())
			{
				return "ERR";
			}
			return Org.Apache.Hadoop.IO.File.Tfile.NanoTimer.NanoTimeToString(this.cumulate);
		}

		/// <summary>
		/// A utility method to format a time duration in nano seconds into a human
		/// understandable stirng.
		/// </summary>
		/// <param name="t">Time duration in nano seconds.</param>
		/// <returns>String representation.</returns>
		public static string NanoTimeToString(long t)
		{
			if (t < 0)
			{
				return "ERR";
			}
			if (t == 0)
			{
				return "0";
			}
			if (t < 1000)
			{
				return t + "ns";
			}
			double us = (double)t / 1000;
			if (us < 1000)
			{
				return string.Format("%.2fus", us);
			}
			double ms = us / 1000;
			if (ms < 1000)
			{
				return string.Format("%.2fms", ms);
			}
			double ss = ms / 1000;
			if (ss < 1000)
			{
				return string.Format("%.2fs", ss);
			}
			long mm = (long)ss / 60;
			ss -= mm * 60;
			long hh = mm / 60;
			mm -= hh * 60;
			long dd = hh / 24;
			hh -= dd * 24;
			if (dd > 0)
			{
				return string.Format("%dd%dh", dd, hh);
			}
			if (hh > 0)
			{
				return string.Format("%dh%dm", hh, mm);
			}
			if (mm > 0)
			{
				return string.Format("%dm%.1fs", mm, ss);
			}
			return string.Format("%.2fs", ss);
		}

		private bool Readable()
		{
			return this.last != -1;
		}

		/// <summary>Simple tester.</summary>
		/// <param name="args"/>
		public static void Main(string[] args)
		{
			long i = 7;
			for (int x = 0; x < 20; ++x, i *= 7)
			{
				System.Console.Out.WriteLine(Org.Apache.Hadoop.IO.File.Tfile.NanoTimer.NanoTimeToString
					(i));
			}
		}
	}
}
