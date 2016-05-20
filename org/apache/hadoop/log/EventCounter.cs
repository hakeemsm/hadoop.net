using Sharpen;

namespace org.apache.hadoop.log
{
	/// <summary>
	/// A log4J Appender that simply counts logging events in three levels:
	/// fatal, error and warn.
	/// </summary>
	/// <remarks>
	/// A log4J Appender that simply counts logging events in three levels:
	/// fatal, error and warn. The class name is used in log4j.properties
	/// </remarks>
	[System.ObsoleteAttribute(@"use org.apache.hadoop.log.metrics.EventCounter instead"
		)]
	public class EventCounter : org.apache.hadoop.log.metrics.EventCounter
	{
		static EventCounter()
		{
			// The logging system is not started yet.
			System.Console.Error.WriteLine("WARNING: " + Sharpen.Runtime.getClassForType(typeof(
				org.apache.hadoop.log.EventCounter)).getName() + " is deprecated. Please use " +
				 Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.log.metrics.EventCounter
				)).getName() + " in all the log4j.properties files.");
		}
	}
}
