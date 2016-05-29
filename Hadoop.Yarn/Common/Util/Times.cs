using Org.Apache.Commons.Logging;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Util
{
	public class Times
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Times));

		private sealed class _ThreadLocal_34 : ThreadLocal<SimpleDateFormat>
		{
			public _ThreadLocal_34()
			{
			}

			// This format should match the one used in yarn.dt.plugins.js
			protected override SimpleDateFormat InitialValue()
			{
				return new SimpleDateFormat("EEE MMM dd HH:mm:ss Z yyyy");
			}
		}

		internal static readonly ThreadLocal<SimpleDateFormat> dateFormat = new _ThreadLocal_34
			();

		public static long Elapsed(long started, long finished)
		{
			return Times.Elapsed(started, finished, true);
		}

		// A valid elapsed is supposed to be non-negative. If finished/current time
		// is ahead of the started time, return -1 to indicate invalid elapsed time,
		// and record a warning log.
		public static long Elapsed(long started, long finished, bool isRunning)
		{
			if (finished > 0 && started > 0)
			{
				long elapsed = finished - started;
				if (elapsed >= 0)
				{
					return elapsed;
				}
				else
				{
					Log.Warn("Finished time " + finished + " is ahead of started time " + started);
					return -1;
				}
			}
			if (isRunning)
			{
				long current = Runtime.CurrentTimeMillis();
				long elapsed = started > 0 ? current - started : 0;
				if (elapsed >= 0)
				{
					return elapsed;
				}
				else
				{
					Log.Warn("Current time " + current + " is ahead of started time " + started);
					return -1;
				}
			}
			else
			{
				return -1;
			}
		}

		public static string Format(long ts)
		{
			return ts > 0 ? dateFormat.Get().Format(Sharpen.Extensions.CreateDate(ts)).ToString
				() : "N/A";
		}
	}
}
