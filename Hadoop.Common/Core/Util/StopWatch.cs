using System;
using Sharpen;

namespace Org.Apache.Hadoop.Util
{
	/// <summary>A simplified StopWatch implementation which can measure times in nanoseconds.
	/// 	</summary>
	public class StopWatch : IDisposable
	{
		private bool isStarted;

		private long startNanos;

		private long currentElapsedNanos;

		public StopWatch()
		{
		}

		/// <summary>The method is used to find out if the StopWatch is started.</summary>
		/// <returns>boolean If the StopWatch is started.</returns>
		public virtual bool IsRunning()
		{
			return isStarted;
		}

		/// <summary>Start to measure times and make the state of stopwatch running.</summary>
		/// <returns>this instance of StopWatch.</returns>
		public virtual Org.Apache.Hadoop.Util.StopWatch Start()
		{
			if (isStarted)
			{
				throw new InvalidOperationException("StopWatch is already running");
			}
			isStarted = true;
			startNanos = Runtime.NanoTime();
			return this;
		}

		/// <summary>Stop elapsed time and make the state of stopwatch stop.</summary>
		/// <returns>this instance of StopWatch.</returns>
		public virtual Org.Apache.Hadoop.Util.StopWatch Stop()
		{
			if (!isStarted)
			{
				throw new InvalidOperationException("StopWatch is already stopped");
			}
			long now = Runtime.NanoTime();
			isStarted = false;
			currentElapsedNanos += now - startNanos;
			return this;
		}

		/// <summary>Reset elapsed time to zero and make the state of stopwatch stop.</summary>
		/// <returns>this instance of StopWatch.</returns>
		public virtual Org.Apache.Hadoop.Util.StopWatch Reset()
		{
			currentElapsedNanos = 0;
			isStarted = false;
			return this;
		}

		/// <returns>current elapsed time in specified timeunit.</returns>
		public virtual long Now(TimeUnit timeUnit)
		{
			return timeUnit.Convert(Now(), TimeUnit.Nanoseconds);
		}

		/// <returns>current elapsed time in nanosecond.</returns>
		public virtual long Now()
		{
			return isStarted ? Runtime.NanoTime() - startNanos + currentElapsedNanos : currentElapsedNanos;
		}

		public override string ToString()
		{
			return Now().ToString();
		}

		public virtual void Close()
		{
			if (isStarted)
			{
				Stop();
			}
		}
	}
}
