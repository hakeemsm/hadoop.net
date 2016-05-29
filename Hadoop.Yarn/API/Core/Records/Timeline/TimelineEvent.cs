using System.Collections;
using System.Collections.Generic;
using Org.Apache.Hadoop.Classification;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Records.Timeline
{
	/// <summary>
	/// The class that contains the information of an event that is related to some
	/// conceptual entity of an application.
	/// </summary>
	/// <remarks>
	/// The class that contains the information of an event that is related to some
	/// conceptual entity of an application. Users are free to define what the event
	/// means, such as starting an application, getting allocated a container and
	/// etc.
	/// </remarks>
	public class TimelineEvent : Comparable<Org.Apache.Hadoop.Yarn.Api.Records.Timeline.TimelineEvent
		>
	{
		private long timestamp;

		private string eventType;

		private Dictionary<string, object> eventInfo = new Dictionary<string, object>();

		public TimelineEvent()
		{
		}

		/// <summary>Get the timestamp of the event</summary>
		/// <returns>the timestamp of the event</returns>
		public virtual long GetTimestamp()
		{
			return timestamp;
		}

		/// <summary>Set the timestamp of the event</summary>
		/// <param name="timestamp">the timestamp of the event</param>
		public virtual void SetTimestamp(long timestamp)
		{
			this.timestamp = timestamp;
		}

		/// <summary>Get the event type</summary>
		/// <returns>the event type</returns>
		public virtual string GetEventType()
		{
			return eventType;
		}

		/// <summary>Set the event type</summary>
		/// <param name="eventType">the event type</param>
		public virtual void SetEventType(string eventType)
		{
			this.eventType = eventType;
		}

		/// <summary>Set the information of the event</summary>
		/// <returns>the information of the event</returns>
		public virtual IDictionary<string, object> GetEventInfo()
		{
			return eventInfo;
		}

		// Required by JAXB
		[InterfaceAudience.Private]
		public virtual Dictionary<string, object> GetEventInfoJAXB()
		{
			return eventInfo;
		}

		/// <summary>
		/// Add one piece of the information of the event to the existing information
		/// map
		/// </summary>
		/// <param name="key">the information key</param>
		/// <param name="value">the information value</param>
		public virtual void AddEventInfo(string key, object value)
		{
			this.eventInfo[key] = value;
		}

		/// <summary>Add a map of the information of the event to the existing information map
		/// 	</summary>
		/// <param name="eventInfo">a map of of the information of the event</param>
		public virtual void AddEventInfo(IDictionary<string, object> eventInfo)
		{
			this.eventInfo.PutAll(eventInfo);
		}

		/// <summary>Set the information map to the given map of the information of the event
		/// 	</summary>
		/// <param name="eventInfo">a map of of the information of the event</param>
		public virtual void SetEventInfo(IDictionary<string, object> eventInfo)
		{
			if (eventInfo != null && !(eventInfo is Hashtable))
			{
				this.eventInfo = new Dictionary<string, object>(eventInfo);
			}
			else
			{
				this.eventInfo = (Dictionary<string, object>)eventInfo;
			}
		}

		public virtual int CompareTo(Org.Apache.Hadoop.Yarn.Api.Records.Timeline.TimelineEvent
			 other)
		{
			if (timestamp > other.timestamp)
			{
				return -1;
			}
			else
			{
				if (timestamp < other.timestamp)
				{
					return 1;
				}
				else
				{
					return string.CompareOrdinal(eventType, other.eventType);
				}
			}
		}

		public override bool Equals(object o)
		{
			if (this == o)
			{
				return true;
			}
			if (o == null || GetType() != o.GetType())
			{
				return false;
			}
			Org.Apache.Hadoop.Yarn.Api.Records.Timeline.TimelineEvent @event = (Org.Apache.Hadoop.Yarn.Api.Records.Timeline.TimelineEvent
				)o;
			if (timestamp != @event.timestamp)
			{
				return false;
			}
			if (!eventType.Equals(@event.eventType))
			{
				return false;
			}
			if (eventInfo != null ? !eventInfo.Equals(@event.eventInfo) : @event.eventInfo !=
				 null)
			{
				return false;
			}
			return true;
		}

		public override int GetHashCode()
		{
			int result = (int)(timestamp ^ ((long)(((ulong)timestamp) >> 32)));
			result = 31 * result + eventType.GetHashCode();
			result = 31 * result + (eventInfo != null ? eventInfo.GetHashCode() : 0);
			return result;
		}
	}
}
