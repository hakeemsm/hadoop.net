using Sharpen;

namespace org.apache.hadoop.util
{
	/// <summary>Utility methods for getting the time and computing intervals.</summary>
	/// <remarks>
	/// Utility methods for getting the time and computing intervals.
	/// It has the same behavior as {
	/// <see cref="Time"/>
	/// }, with the exception that its
	/// functions can be overridden for dependency injection purposes.
	/// </remarks>
	public class Timer
	{
		/// <summary>Current system time.</summary>
		/// <remarks>
		/// Current system time.  Do not use this to calculate a duration or interval
		/// to sleep, because it will be broken by settimeofday.  Instead, use
		/// monotonicNow.
		/// </remarks>
		/// <returns>current time in msec.</returns>
		public virtual long now()
		{
			return org.apache.hadoop.util.Time.now();
		}

		/// <summary>
		/// Current time from some arbitrary time base in the past, counting in
		/// milliseconds, and not affected by settimeofday or similar system clock
		/// changes.
		/// </summary>
		/// <remarks>
		/// Current time from some arbitrary time base in the past, counting in
		/// milliseconds, and not affected by settimeofday or similar system clock
		/// changes.  This is appropriate to use when computing how much longer to
		/// wait for an interval to expire.
		/// </remarks>
		/// <returns>a monotonic clock that counts in milliseconds.</returns>
		public virtual long monotonicNow()
		{
			return org.apache.hadoop.util.Time.monotonicNow();
		}
	}
}
