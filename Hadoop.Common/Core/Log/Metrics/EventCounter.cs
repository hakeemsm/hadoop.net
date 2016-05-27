using Org.Apache.Hadoop.Classification;
using Org.Apache.Log4j;
using Org.Apache.Log4j.Spi;
using Sharpen;

namespace Org.Apache.Hadoop.Log.Metrics
{
	/// <summary>
	/// A log4J Appender that simply counts logging events in three levels:
	/// fatal, error and warn.
	/// </summary>
	/// <remarks>
	/// A log4J Appender that simply counts logging events in three levels:
	/// fatal, error and warn. The class name is used in log4j.properties
	/// </remarks>
	public class EventCounter : AppenderSkeleton
	{
		private const int Fatal = 0;

		private const int Error = 1;

		private const int Warn = 2;

		private const int Info = 3;

		private class EventCounts
		{
			private readonly long[] counts = new long[] { 0, 0, 0, 0 };

			private void Incr(int i)
			{
				lock (this)
				{
					++counts[i];
				}
			}

			private long Get(int i)
			{
				lock (this)
				{
					return counts[i];
				}
			}
		}

		private static EventCounter.EventCounts counts = new EventCounter.EventCounts();

		[InterfaceAudience.Private]
		public static long GetFatal()
		{
			return counts.Get(Fatal);
		}

		[InterfaceAudience.Private]
		public static long GetError()
		{
			return counts.Get(Error);
		}

		[InterfaceAudience.Private]
		public static long GetWarn()
		{
			return counts.Get(Warn);
		}

		[InterfaceAudience.Private]
		public static long GetInfo()
		{
			return counts.Get(Info);
		}

		protected override void Append(LoggingEvent @event)
		{
			Level level = @event.GetLevel();
			// depends on the api, == might not work
			// see HADOOP-7055 for details
			if (level.Equals(Level.Info))
			{
				counts.Incr(Info);
			}
			else
			{
				if (level.Equals(Level.Warn))
				{
					counts.Incr(Warn);
				}
				else
				{
					if (level.Equals(Level.Error))
					{
						counts.Incr(Error);
					}
					else
					{
						if (level.Equals(Level.Fatal))
						{
							counts.Incr(Fatal);
						}
					}
				}
			}
		}

		public override void Close()
		{
		}

		public override bool RequiresLayout()
		{
			return false;
		}
	}
}
