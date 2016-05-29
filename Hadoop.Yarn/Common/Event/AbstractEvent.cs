using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Event
{
	/// <summary>Parent class of all the events.</summary>
	/// <remarks>Parent class of all the events. All events extend this class.</remarks>
	public abstract class AbstractEvent<Type> : Org.Apache.Hadoop.Yarn.Event.Event<TYPE
		>
		where Type : Enum<TYPE>
	{
		private readonly TYPE type;

		private readonly long timestamp;

		public AbstractEvent(TYPE type)
		{
			// use this if you DON'T care about the timestamp
			this.type = type;
			// We're not generating a real timestamp here.  It's too expensive.
			timestamp = -1L;
		}

		public AbstractEvent(TYPE type, long timestamp)
		{
			// use this if you care about the timestamp
			this.type = type;
			this.timestamp = timestamp;
		}

		public virtual long GetTimestamp()
		{
			return timestamp;
		}

		public virtual TYPE GetType()
		{
			return type;
		}

		public override string ToString()
		{
			return "EventType: " + GetType();
		}
	}
}
