using System.Collections.Generic;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Records.Timeline
{
	/// <summary>
	/// The class that hosts a list of events, which are categorized according to
	/// their related entities.
	/// </summary>
	public class TimelineEvents
	{
		private IList<TimelineEvents.EventsOfOneEntity> allEvents = new AList<TimelineEvents.EventsOfOneEntity
			>();

		public TimelineEvents()
		{
		}

		/// <summary>
		/// Get a list of
		/// <see cref="EventsOfOneEntity"/>
		/// instances
		/// </summary>
		/// <returns>
		/// a list of
		/// <see cref="EventsOfOneEntity"/>
		/// instances
		/// </returns>
		public virtual IList<TimelineEvents.EventsOfOneEntity> GetAllEvents()
		{
			return allEvents;
		}

		/// <summary>
		/// Add a single
		/// <see cref="EventsOfOneEntity"/>
		/// instance into the existing list
		/// </summary>
		/// <param name="eventsOfOneEntity">
		/// a single
		/// <see cref="EventsOfOneEntity"/>
		/// instance
		/// </param>
		public virtual void AddEvent(TimelineEvents.EventsOfOneEntity eventsOfOneEntity)
		{
			allEvents.AddItem(eventsOfOneEntity);
		}

		/// <summary>
		/// Add a list of
		/// <see cref="EventsOfOneEntity"/>
		/// instances into the existing list
		/// </summary>
		/// <param name="allEvents">
		/// a list of
		/// <see cref="EventsOfOneEntity"/>
		/// instances
		/// </param>
		public virtual void AddEvents(IList<TimelineEvents.EventsOfOneEntity> allEvents)
		{
			Sharpen.Collections.AddAll(this.allEvents, allEvents);
		}

		/// <summary>
		/// Set the list to the given list of
		/// <see cref="EventsOfOneEntity"/>
		/// instances
		/// </summary>
		/// <param name="allEvents">
		/// a list of
		/// <see cref="EventsOfOneEntity"/>
		/// instances
		/// </param>
		public virtual void SetEvents(IList<TimelineEvents.EventsOfOneEntity> allEvents)
		{
			this.allEvents.Clear();
			Sharpen.Collections.AddAll(this.allEvents, allEvents);
		}

		/// <summary>The class that hosts a list of events that are only related to one entity.
		/// 	</summary>
		public class EventsOfOneEntity
		{
			private string entityId;

			private string entityType;

			private IList<TimelineEvent> events = new AList<TimelineEvent>();

			public EventsOfOneEntity()
			{
			}

			/// <summary>Get the entity Id</summary>
			/// <returns>the entity Id</returns>
			public virtual string GetEntityId()
			{
				return entityId;
			}

			/// <summary>Set the entity Id</summary>
			/// <param name="entityId">the entity Id</param>
			public virtual void SetEntityId(string entityId)
			{
				this.entityId = entityId;
			}

			/// <summary>Get the entity type</summary>
			/// <returns>the entity type</returns>
			public virtual string GetEntityType()
			{
				return entityType;
			}

			/// <summary>Set the entity type</summary>
			/// <param name="entityType">the entity type</param>
			public virtual void SetEntityType(string entityType)
			{
				this.entityType = entityType;
			}

			/// <summary>Get a list of events</summary>
			/// <returns>a list of events</returns>
			public virtual IList<TimelineEvent> GetEvents()
			{
				return events;
			}

			/// <summary>Add a single event to the existing event list</summary>
			/// <param name="event">a single event</param>
			public virtual void AddEvent(TimelineEvent @event)
			{
				events.AddItem(@event);
			}

			/// <summary>Add a list of event to the existing event list</summary>
			/// <param name="events">a list of events</param>
			public virtual void AddEvents(IList<TimelineEvent> events)
			{
				Sharpen.Collections.AddAll(this.events, events);
			}

			/// <summary>Set the event list to the given list of events</summary>
			/// <param name="events">a list of events</param>
			public virtual void SetEvents(IList<TimelineEvent> events)
			{
				this.events = events;
			}
		}
	}
}
