using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Jobhistory
{
	/// <summary>Interface for event wrapper classes.</summary>
	/// <remarks>
	/// Interface for event wrapper classes.  Implementations each wrap an
	/// Avro-generated class, adding constructors and accessor methods.
	/// </remarks>
	public interface HistoryEvent
	{
		/// <summary>Return this event's type.</summary>
		EventType GetEventType();

		/// <summary>Return the Avro datum wrapped by this.</summary>
		object GetDatum();

		/// <summary>Set the Avro datum wrapped by this.</summary>
		void SetDatum(object datum);
	}
}
