using Sharpen;

namespace Org.Apache.Hadoop.FS.Slive
{
	/// <summary>Simple timer class that abstracts time access</summary>
	internal class Timer
	{
		private Timer()
		{
		}

		// no construction allowed
		/// <summary>The current time in milliseconds</summary>
		/// <returns>long (milliseconds)</returns>
		internal static long Now()
		{
			return Runtime.CurrentTimeMillis();
		}

		/// <summary>
		/// Calculates how much time in milliseconds elapsed from given start time to
		/// the current time in milliseconds
		/// </summary>
		/// <param name="startTime"/>
		/// <returns>elapsed time (milliseconds)</returns>
		internal static long Elapsed(long startTime)
		{
			long elapsedTime = Now() - startTime;
			if (elapsedTime < 0)
			{
				elapsedTime = 0;
			}
			return elapsedTime;
		}
	}
}
