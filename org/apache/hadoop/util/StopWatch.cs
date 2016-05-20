using Sharpen;

namespace org.apache.hadoop.util
{
	/// <summary>A simplified StopWatch implementation which can measure times in nanoseconds.
	/// 	</summary>
	public class StopWatch : java.io.Closeable
	{
		private bool isStarted;

		private long startNanos;

		private long currentElapsedNanos;

		public StopWatch()
		{
		}

		/// <summary>The method is used to find out if the StopWatch is started.</summary>
		/// <returns>boolean If the StopWatch is started.</returns>
		public virtual bool isRunning()
		{
			return isStarted;
		}

		/// <summary>Start to measure times and make the state of stopwatch running.</summary>
		/// <returns>this instance of StopWatch.</returns>
		public virtual org.apache.hadoop.util.StopWatch start()
		{
			if (isStarted)
			{
				throw new System.InvalidOperationException("StopWatch is already running");
			}
			isStarted = true;
			startNanos = Sharpen.Runtime.nanoTime();
			return this;
		}

		/// <summary>Stop elapsed time and make the state of stopwatch stop.</summary>
		/// <returns>this instance of StopWatch.</returns>
		public virtual org.apache.hadoop.util.StopWatch stop()
		{
			if (!isStarted)
			{
				throw new System.InvalidOperationException("StopWatch is already stopped");
			}
			long now = Sharpen.Runtime.nanoTime();
			isStarted = false;
			currentElapsedNanos += now - startNanos;
			return this;
		}

		/// <summary>Reset elapsed time to zero and make the state of stopwatch stop.</summary>
		/// <returns>this instance of StopWatch.</returns>
		public virtual org.apache.hadoop.util.StopWatch reset()
		{
			currentElapsedNanos = 0;
			isStarted = false;
			return this;
		}

		/// <returns>current elapsed time in specified timeunit.</returns>
		public virtual long now(java.util.concurrent.TimeUnit timeUnit)
		{
			return timeUnit.convert(now(), java.util.concurrent.TimeUnit.NANOSECONDS);
		}

		/// <returns>current elapsed time in nanosecond.</returns>
		public virtual long now()
		{
			return isStarted ? Sharpen.Runtime.nanoTime() - startNanos + currentElapsedNanos : 
				currentElapsedNanos;
		}

		public override string ToString()
		{
			return Sharpen.Runtime.getStringValueOf(now());
		}

		public virtual void close()
		{
			if (isStarted)
			{
				stop();
			}
		}
	}
}
