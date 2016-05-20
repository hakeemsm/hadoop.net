using Sharpen;

namespace org.apache.hadoop.metrics.jvm
{
	/// <summary>
	/// A log4J Appender that simply counts logging events in three levels:
	/// fatal, error and warn.
	/// </summary>
	public class EventCounter : org.apache.hadoop.log.metrics.EventCounter
	{
		static EventCounter()
		{
			// The logging system is not started yet.
			System.Console.Error.WriteLine("WARNING: " + Sharpen.Runtime.getClassForType(typeof(
				org.apache.hadoop.metrics.jvm.EventCounter)).getName() + " is deprecated. Please use "
				 + Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.log.metrics.EventCounter
				)).getName() + " in all the log4j.properties files.");
		}
	}
}
