using Sharpen;

namespace org.apache.hadoop.log.metrics
{
	/// <summary>
	/// A log4J Appender that simply counts logging events in three levels:
	/// fatal, error and warn.
	/// </summary>
	/// <remarks>
	/// A log4J Appender that simply counts logging events in three levels:
	/// fatal, error and warn. The class name is used in log4j.properties
	/// </remarks>
	public class EventCounter : org.apache.log4j.AppenderSkeleton
	{
		private const int FATAL = 0;

		private const int ERROR = 1;

		private const int WARN = 2;

		private const int INFO = 3;

		private class EventCounts
		{
			private readonly long[] counts = new long[] { 0, 0, 0, 0 };

			private void incr(int i)
			{
				lock (this)
				{
					++counts[i];
				}
			}

			private long get(int i)
			{
				lock (this)
				{
					return counts[i];
				}
			}
		}

		private static org.apache.hadoop.log.metrics.EventCounter.EventCounts counts = new 
			org.apache.hadoop.log.metrics.EventCounter.EventCounts();

		[org.apache.hadoop.classification.InterfaceAudience.Private]
		public static long getFatal()
		{
			return counts.get(FATAL);
		}

		[org.apache.hadoop.classification.InterfaceAudience.Private]
		public static long getError()
		{
			return counts.get(ERROR);
		}

		[org.apache.hadoop.classification.InterfaceAudience.Private]
		public static long getWarn()
		{
			return counts.get(WARN);
		}

		[org.apache.hadoop.classification.InterfaceAudience.Private]
		public static long getInfo()
		{
			return counts.get(INFO);
		}

		protected override void append(org.apache.log4j.spi.LoggingEvent @event)
		{
			org.apache.log4j.Level level = @event.getLevel();
			// depends on the api, == might not work
			// see HADOOP-7055 for details
			if (level.Equals(org.apache.log4j.Level.INFO))
			{
				counts.incr(INFO);
			}
			else
			{
				if (level.Equals(org.apache.log4j.Level.WARN))
				{
					counts.incr(WARN);
				}
				else
				{
					if (level.Equals(org.apache.log4j.Level.ERROR))
					{
						counts.incr(ERROR);
					}
					else
					{
						if (level.Equals(org.apache.log4j.Level.FATAL))
						{
							counts.incr(FATAL);
						}
					}
				}
			}
		}

		public override void close()
		{
		}

		public override bool requiresLayout()
		{
			return false;
		}
	}
}
