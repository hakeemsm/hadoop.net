using System.Collections.Generic;
using Org.Apache.Hadoop.Yarn.Api.Records.Timeline;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Timeline
{
	/// <summary>This interface is for retrieving timeline information.</summary>
	public abstract class TimelineReader
	{
		/// <summary>
		/// Possible fields to retrieve for
		/// <see cref="#getEntities"/>
		/// and
		/// <see cref="#getEntity"/>
		/// .
		/// </summary>
		public enum Field
		{
			Events,
			RelatedEntities,
			PrimaryFilters,
			OtherInfo,
			LastEventOnly
		}

		/// <summary>
		/// Default limit for
		/// <see cref="GetEntities(string, long, long, long, string, long, NameValuePair, System.Collections.Generic.ICollection{E}, Sharpen.EnumSet{E}, CheckAcl)
		/// 	"/>
		/// and
		/// <see cref="GetEntityTimelines(string, System.Collections.Generic.ICollection{E}, long, long, long, System.Collections.Generic.ICollection{E})
		/// 	"/>
		/// .
		/// </summary>
		public const long DefaultLimit = 100;

		/// <summary>
		/// This method retrieves a list of entity information,
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.Timeline.TimelineEntity"/>
		/// ,
		/// sorted by the starting timestamp for the entity, descending. The starting
		/// timestamp of an entity is a timestamp specified by the client. If it is not
		/// explicitly specified, it will be chosen by the store to be the earliest
		/// timestamp of the events received in the first put for the entity.
		/// </summary>
		/// <param name="entityType">The type of entities to return (required).</param>
		/// <param name="limit">
		/// A limit on the number of entities to return. If null, defaults to
		/// <see cref="DefaultLimit"/>
		/// .
		/// </param>
		/// <param name="windowStart">
		/// The earliest start timestamp to retrieve (exclusive). If null,
		/// defaults to retrieving all entities until the limit is reached.
		/// </param>
		/// <param name="windowEnd">
		/// The latest start timestamp to retrieve (inclusive). If null,
		/// defaults to
		/// <see cref="long.MaxValue"/>
		/// </param>
		/// <param name="fromId">
		/// If fromId is not null, retrieve entities earlier than and
		/// including the specified ID. If no start time is found for the
		/// specified ID, an empty list of entities will be returned. The
		/// windowEnd parameter will take precedence if the start time of this
		/// entity falls later than windowEnd.
		/// </param>
		/// <param name="fromTs">
		/// If fromTs is not null, ignore entities that were inserted into the
		/// store after the given timestamp. The entity's insert timestamp
		/// used for this comparison is the store's system time when the first
		/// put for the entity was received (not the entity's start time).
		/// </param>
		/// <param name="primaryFilter">
		/// Retrieves only entities that have the specified primary filter. If
		/// null, retrieves all entities. This is an indexed retrieval, and no
		/// entities that do not match the filter are scanned.
		/// </param>
		/// <param name="secondaryFilters">
		/// Retrieves only entities that have exact matches for all the
		/// specified filters in their primary filters or other info. This is
		/// not an indexed retrieval, so all entities are scanned but only
		/// those matching the filters are returned.
		/// </param>
		/// <param name="fieldsToRetrieve">
		/// Specifies which fields of the entity object to retrieve (see
		/// <see cref="Field"/>
		/// ). If the set of fields contains
		/// <see cref="Field.LastEventOnly"/>
		/// and not
		/// <see cref="Field.Events"/>
		/// , the
		/// most recent event for each entity is retrieved. If null, retrieves
		/// all fields.
		/// </param>
		/// <returns>
		/// An
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.Timeline.TimelineEntities"/>
		/// object.
		/// </returns>
		/// <exception cref="System.IO.IOException"/>
		public abstract TimelineEntities GetEntities(string entityType, long limit, long 
			windowStart, long windowEnd, string fromId, long fromTs, NameValuePair primaryFilter
			, ICollection<NameValuePair> secondaryFilters, EnumSet<TimelineReader.Field> fieldsToRetrieve
			, TimelineDataManager.CheckAcl checkAcl);

		/// <summary>This method retrieves the entity information for a given entity.</summary>
		/// <param name="entityId">The entity whose information will be retrieved.</param>
		/// <param name="entityType">The type of the entity.</param>
		/// <param name="fieldsToRetrieve">
		/// Specifies which fields of the entity object to retrieve (see
		/// <see cref="Field"/>
		/// ). If the set of fields contains
		/// <see cref="Field.LastEventOnly"/>
		/// and not
		/// <see cref="Field.Events"/>
		/// , the
		/// most recent event for each entity is retrieved. If null, retrieves
		/// all fields.
		/// </param>
		/// <returns>
		/// An
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.Timeline.TimelineEntity"/>
		/// object.
		/// </returns>
		/// <exception cref="System.IO.IOException"/>
		public abstract TimelineEntity GetEntity(string entityId, string entityType, EnumSet
			<TimelineReader.Field> fieldsToRetrieve);

		/// <summary>
		/// This method retrieves the events for a list of entities all of the same
		/// entity type.
		/// </summary>
		/// <remarks>
		/// This method retrieves the events for a list of entities all of the same
		/// entity type. The events for each entity are sorted in order of their
		/// timestamps, descending.
		/// </remarks>
		/// <param name="entityType">The type of entities to retrieve events for.</param>
		/// <param name="entityIds">The entity IDs to retrieve events for.</param>
		/// <param name="limit">
		/// A limit on the number of events to return for each entity. If
		/// null, defaults to
		/// <see cref="DefaultLimit"/>
		/// events per entity.
		/// </param>
		/// <param name="windowStart">
		/// If not null, retrieves only events later than the given time
		/// (exclusive)
		/// </param>
		/// <param name="windowEnd">
		/// If not null, retrieves only events earlier than the given time
		/// (inclusive)
		/// </param>
		/// <param name="eventTypes">
		/// Restricts the events returned to the given types. If null, events
		/// of all types will be returned.
		/// </param>
		/// <returns>
		/// An
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.Timeline.TimelineEvents"/>
		/// object.
		/// </returns>
		/// <exception cref="System.IO.IOException"/>
		public abstract TimelineEvents GetEntityTimelines(string entityType, ICollection<
			string> entityIds, long limit, long windowStart, long windowEnd, ICollection<string
			> eventTypes);

		/// <summary>This method retrieves the domain information for a given ID.</summary>
		/// <returns>
		/// a
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.Timeline.TimelineDomain"/>
		/// object.
		/// </returns>
		/// <exception cref="System.IO.IOException"/>
		public abstract TimelineDomain GetDomain(string domainId);

		/// <summary>This method retrieves all the domains that belong to a given owner.</summary>
		/// <remarks>
		/// This method retrieves all the domains that belong to a given owner.
		/// The domains are sorted according to the created time firstly and the
		/// modified time secondly in descending order.
		/// </remarks>
		/// <param name="owner">the domain owner</param>
		/// <returns>
		/// an
		/// <see cref="Org.Apache.Hadoop.Yarn.Api.Records.Timeline.TimelineDomains"/>
		/// object.
		/// </returns>
		/// <exception cref="System.IO.IOException"/>
		public abstract TimelineDomains GetDomains(string owner);
	}

	public static class TimelineReaderConstants
	{
	}
}
