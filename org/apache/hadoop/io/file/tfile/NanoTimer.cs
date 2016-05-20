using Sharpen;

namespace org.apache.hadoop.io.file.tfile
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
				this.start();
			}
		}

		/// <summary>Start the timer.</summary>
		/// <remarks>
		/// Start the timer.
		/// Note: No effect if timer is already started.
		/// </remarks>
		public virtual void start()
		{
			if (!this.started)
			{
				this.last = Sharpen.Runtime.nanoTime();
				this.started = true;
			}
		}

		/// <summary>Stop the timer.</summary>
		/// <remarks>
		/// Stop the timer.
		/// Note: No effect if timer is already stopped.
		/// </remarks>
		public virtual void stop()
		{
			if (this.started)
			{
				this.started = false;
				this.cumulate += Sharpen.Runtime.nanoTime() - this.last;
			}
		}

		/// <summary>Read the timer.</summary>
		/// <returns>
		/// the elapsed time in nano-seconds. Note: If the timer is never
		/// started before, -1 is returned.
		/// </returns>
		public virtual long read()
		{
			if (!readable())
			{
				return -1;
			}
			return this.cumulate;
		}

		/// <summary>Reset the timer.</summary>
		public virtual void reset()
		{
			this.last = -1;
			this.started = false;
			this.cumulate = 0;
		}

		/// <summary>Checking whether the timer is started</summary>
		/// <returns>true if timer is started.</returns>
		public virtual bool isStarted()
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
			if (!readable())
			{
				return "ERR";
			}
			return org.apache.hadoop.io.file.tfile.NanoTimer.nanoTimeToString(this.cumulate);
		}

		/// <summary>
		/// A utility method to format a time duration in nano seconds into a human
		/// understandable stirng.
		/// </summary>
		/// <param name="t">Time duration in nano seconds.</param>
		/// <returns>String representation.</returns>
		public static string nanoTimeToString(long t)
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
				return string.format("%.2fus", us);
			}
			double ms = us / 1000;
			if (ms < 1000)
			{
				return string.format("%.2fms", ms);
			}
			double ss = ms / 1000;
			if (ss < 1000)
			{
				return string.format("%.2fs", ss);
			}
			long mm = (long)ss / 60;
			ss -= mm * 60;
			long hh = mm / 60;
			mm -= hh * 60;
			long dd = hh / 24;
			hh -= dd * 24;
			if (dd > 0)
			{
				return string.format("%dd%dh", dd, hh);
			}
			if (hh > 0)
			{
				return string.format("%dh%dm", hh, mm);
			}
			if (mm > 0)
			{
				return string.format("%dm%.1fs", mm, ss);
			}
			return string.format("%.2fs", ss);
		}

		private bool readable()
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
				System.Console.Out.WriteLine(org.apache.hadoop.io.file.tfile.NanoTimer.nanoTimeToString
					(i));
			}
		}
	}
}
