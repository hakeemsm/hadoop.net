using Org.Apache.Hadoop.Log.Metrics;
using Sharpen;

namespace Org.Apache.Hadoop.Metrics.Jvm
{
	/// <summary>
	/// A log4J Appender that simply counts logging events in three levels:
	/// fatal, error and warn.
	/// </summary>
	public class EventCounter : EventCounter
	{
		static EventCounter()
		{
			// The logging system is not started yet.
			System.Console.Error.WriteLine("WARNING: " + typeof(EventCounter).FullName + " is deprecated. Please use "
				 + typeof(EventCounter).FullName + " in all the log4j.properties files.");
		}
	}
}
