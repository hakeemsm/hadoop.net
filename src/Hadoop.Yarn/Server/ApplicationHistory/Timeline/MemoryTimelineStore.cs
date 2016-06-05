using System.Collections.Generic;
using Org.Apache.Hadoop.Service;
using Org.Apache.Hadoop.Yarn.Api.Records.Timeline;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Timeline
{
	/// <summary>
	/// In-memory implementation of
	/// <see cref="TimelineStore"/>
	/// . This
	/// implementation is for test purpose only. If users improperly instantiate it,
	/// they may encounter reading and writing history data in different memory
	/// store.
	/// The methods are synchronized to avoid concurrent modification on the memory.
	/// </summary>
	public class MemoryTimelineStore : AbstractService, TimelineStore
	{
		private IDictionary<EntityIdentifier, TimelineEntity> entities = new Dictionary<EntityIdentifier
			, TimelineEntity>();

		private IDictionary<EntityIdentifier, long> entityInsertTimes = new Dictionary<EntityIdentifier
			, long>();

		private IDictionary<string, TimelineDomain> domainsById = new Dictionary<string, 
			TimelineDomain>();

		private IDictionary<string, ICollection<TimelineDomain>> domainsByOwner = new Dictionary
			<string, ICollection<TimelineDomain>>();

		public MemoryTimelineStore()
			: base(typeof(Org.Apache.Hadoop.Yarn.Server.Timeline.MemoryTimelineStore).FullName
				)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual TimelineEntities GetEntities(string entityType, long limit, long windowStart
			, long windowEnd, string fromId, long fromTs, NameValuePair primaryFilter, ICollection
			<NameValuePair> secondaryFilters, EnumSet<TimelineReader.Field> fields, TimelineDataManager.CheckAcl
			 checkAcl)
		{
			lock (this)
			{
				if (limit == null)
				{
					limit = DefaultLimit;
				}
				if (windowStart == null)
				{
					windowStart = long.MinValue;
				}
				if (windowEnd == null)
				{
					windowEnd = long.MaxValue;
				}
				if (fields == null)
				{
					fields = EnumSet.AllOf<TimelineReader.Field>();
				}
				IEnumerator<TimelineEntity> entityIterator = null;
				if (fromId != null)
				{
					TimelineEntity firstEntity = entities[new EntityIdentifier(fromId, entityType)];
					if (firstEntity == null)
					{
						return new TimelineEntities();
					}
					else
					{
						entityIterator = new TreeSet<TimelineEntity>(entities.Values).TailSet(firstEntity
							, true).GetEnumerator();
					}
				}
				if (entityIterator == null)
				{
					entityIterator = new PriorityQueue<TimelineEntity>(entities.Values).GetEnumerator
						();
				}
				IList<TimelineEntity> entitiesSelected = new AList<TimelineEntity>();
				while (entityIterator.HasNext())
				{
					TimelineEntity entity = entityIterator.Next();
					if (entitiesSelected.Count >= limit)
					{
						break;
					}
					if (!entity.GetEntityType().Equals(entityType))
					{
						continue;
					}
					if (entity.GetStartTime() <= windowStart)
					{
						continue;
					}
					if (entity.GetStartTime() > windowEnd)
					{
						continue;
					}
					if (fromTs != null && entityInsertTimes[new EntityIdentifier(entity.GetEntityId()
						, entity.GetEntityType())] > fromTs)
					{
						continue;
					}
					if (primaryFilter != null && !MatchPrimaryFilter(entity.GetPrimaryFilters(), primaryFilter
						))
					{
						continue;
					}
					if (secondaryFilters != null)
					{
						// AND logic
						bool flag = true;
						foreach (NameValuePair secondaryFilter in secondaryFilters)
						{
							if (secondaryFilter != null && !MatchPrimaryFilter(entity.GetPrimaryFilters(), secondaryFilter
								) && !MatchFilter(entity.GetOtherInfo(), secondaryFilter))
							{
								flag = false;
								break;
							}
						}
						if (!flag)
						{
							continue;
						}
					}
					if (entity.GetDomainId() == null)
					{
						entity.SetDomainId(TimelineDataManager.DefaultDomainId);
					}
					if (checkAcl == null || checkAcl.Check(entity))
					{
						entitiesSelected.AddItem(entity);
					}
				}
				IList<TimelineEntity> entitiesToReturn = new AList<TimelineEntity>();
				foreach (TimelineEntity entitySelected in entitiesSelected)
				{
					entitiesToReturn.AddItem(MaskFields(entitySelected, fields));
				}
				entitiesToReturn.Sort();
				TimelineEntities entitiesWrapper = new TimelineEntities();
				entitiesWrapper.SetEntities(entitiesToReturn);
				return entitiesWrapper;
			}
		}

		public virtual TimelineEntity GetEntity(string entityId, string entityType, EnumSet
			<TimelineReader.Field> fieldsToRetrieve)
		{
			lock (this)
			{
				if (fieldsToRetrieve == null)
				{
					fieldsToRetrieve = EnumSet.AllOf<TimelineReader.Field>();
				}
				TimelineEntity entity = entities[new EntityIdentifier(entityId, entityType)];
				if (entity == null)
				{
					return null;
				}
				else
				{
					return MaskFields(entity, fieldsToRetrieve);
				}
			}
		}

		public virtual TimelineEvents GetEntityTimelines(string entityType, ICollection<string
			> entityIds, long limit, long windowStart, long windowEnd, ICollection<string> eventTypes
			)
		{
			lock (this)
			{
				TimelineEvents allEvents = new TimelineEvents();
				if (entityIds == null)
				{
					return allEvents;
				}
				if (limit == null)
				{
					limit = DefaultLimit;
				}
				if (windowStart == null)
				{
					windowStart = long.MinValue;
				}
				if (windowEnd == null)
				{
					windowEnd = long.MaxValue;
				}
				foreach (string entityId in entityIds)
				{
					EntityIdentifier entityID = new EntityIdentifier(entityId, entityType);
					TimelineEntity entity = entities[entityID];
					if (entity == null)
					{
						continue;
					}
					TimelineEvents.EventsOfOneEntity events = new TimelineEvents.EventsOfOneEntity();
					events.SetEntityId(entityId);
					events.SetEntityType(entityType);
					foreach (TimelineEvent @event in entity.GetEvents())
					{
						if (events.GetEvents().Count >= limit)
						{
							break;
						}
						if (@event.GetTimestamp() <= windowStart)
						{
							continue;
						}
						if (@event.GetTimestamp() > windowEnd)
						{
							continue;
						}
						if (eventTypes != null && !eventTypes.Contains(@event.GetEventType()))
						{
							continue;
						}
						events.AddEvent(@event);
					}
					allEvents.AddEvent(events);
				}
				return allEvents;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual TimelineDomain GetDomain(string domainId)
		{
			TimelineDomain domain = domainsById[domainId];
			if (domain == null)
			{
				return null;
			}
			else
			{
				return CreateTimelineDomain(domain.GetId(), domain.GetDescription(), domain.GetOwner
					(), domain.GetReaders(), domain.GetWriters(), domain.GetCreatedTime(), domain.GetModifiedTime
					());
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual TimelineDomains GetDomains(string owner)
		{
			IList<TimelineDomain> domains = new AList<TimelineDomain>();
			ICollection<TimelineDomain> domainsOfOneOwner = domainsByOwner[owner];
			if (domainsOfOneOwner == null)
			{
				return new TimelineDomains();
			}
			foreach (TimelineDomain domain in domainsByOwner[owner])
			{
				TimelineDomain domainToReturn = CreateTimelineDomain(domain.GetId(), domain.GetDescription
					(), domain.GetOwner(), domain.GetReaders(), domain.GetWriters(), domain.GetCreatedTime
					(), domain.GetModifiedTime());
				domains.AddItem(domainToReturn);
			}
			domains.Sort(new _IComparer_267());
			TimelineDomains domainsToReturn = new TimelineDomains();
			domainsToReturn.AddDomains(domains);
			return domainsToReturn;
		}

		private sealed class _IComparer_267 : IComparer<TimelineDomain>
		{
			public _IComparer_267()
			{
			}

			public int Compare(TimelineDomain domain1, TimelineDomain domain2)
			{
				int result = domain2.GetCreatedTime().CompareTo(domain1.GetCreatedTime());
				if (result == 0)
				{
					return domain2.GetModifiedTime().CompareTo(domain1.GetModifiedTime());
				}
				else
				{
					return result;
				}
			}
		}

		public virtual TimelinePutResponse Put(TimelineEntities data)
		{
			lock (this)
			{
				TimelinePutResponse response = new TimelinePutResponse();
				foreach (TimelineEntity entity in data.GetEntities())
				{
					EntityIdentifier entityId = new EntityIdentifier(entity.GetEntityId(), entity.GetEntityType
						());
					// store entity info in memory
					TimelineEntity existingEntity = entities[entityId];
					if (existingEntity == null)
					{
						existingEntity = new TimelineEntity();
						existingEntity.SetEntityId(entity.GetEntityId());
						existingEntity.SetEntityType(entity.GetEntityType());
						existingEntity.SetStartTime(entity.GetStartTime());
						if (entity.GetDomainId() == null || entity.GetDomainId().Length == 0)
						{
							TimelinePutResponse.TimelinePutError error = new TimelinePutResponse.TimelinePutError
								();
							error.SetEntityId(entityId.GetId());
							error.SetEntityType(entityId.GetType());
							error.SetErrorCode(TimelinePutResponse.TimelinePutError.NoDomain);
							response.AddError(error);
							continue;
						}
						existingEntity.SetDomainId(entity.GetDomainId());
						entities[entityId] = existingEntity;
						entityInsertTimes[entityId] = Runtime.CurrentTimeMillis();
					}
					if (entity.GetEvents() != null)
					{
						if (existingEntity.GetEvents() == null)
						{
							existingEntity.SetEvents(entity.GetEvents());
						}
						else
						{
							existingEntity.AddEvents(entity.GetEvents());
						}
						existingEntity.GetEvents().Sort();
					}
					// check startTime
					if (existingEntity.GetStartTime() == null)
					{
						if (existingEntity.GetEvents() == null || existingEntity.GetEvents().IsEmpty())
						{
							TimelinePutResponse.TimelinePutError error = new TimelinePutResponse.TimelinePutError
								();
							error.SetEntityId(entityId.GetId());
							error.SetEntityType(entityId.GetType());
							error.SetErrorCode(TimelinePutResponse.TimelinePutError.NoStartTime);
							response.AddError(error);
							Sharpen.Collections.Remove(entities, entityId);
							Sharpen.Collections.Remove(entityInsertTimes, entityId);
							continue;
						}
						else
						{
							long min = long.MaxValue;
							foreach (TimelineEvent e in entity.GetEvents())
							{
								if (min > e.GetTimestamp())
								{
									min = e.GetTimestamp();
								}
							}
							existingEntity.SetStartTime(min);
						}
					}
					if (entity.GetPrimaryFilters() != null)
					{
						if (existingEntity.GetPrimaryFilters() == null)
						{
							existingEntity.SetPrimaryFilters(new Dictionary<string, ICollection<object>>());
						}
						foreach (KeyValuePair<string, ICollection<object>> pf in entity.GetPrimaryFilters
							())
						{
							foreach (object pfo in pf.Value)
							{
								existingEntity.AddPrimaryFilter(pf.Key, MaybeConvert(pfo));
							}
						}
					}
					if (entity.GetOtherInfo() != null)
					{
						if (existingEntity.GetOtherInfo() == null)
						{
							existingEntity.SetOtherInfo(new Dictionary<string, object>());
						}
						foreach (KeyValuePair<string, object> info in entity.GetOtherInfo())
						{
							existingEntity.AddOtherInfo(info.Key, MaybeConvert(info.Value));
						}
					}
					// relate it to other entities
					if (entity.GetRelatedEntities() == null)
					{
						continue;
					}
					foreach (KeyValuePair<string, ICollection<string>> partRelatedEntities in entity.
						GetRelatedEntities())
					{
						if (partRelatedEntities == null)
						{
							continue;
						}
						foreach (string idStr in partRelatedEntities.Value)
						{
							EntityIdentifier relatedEntityId = new EntityIdentifier(idStr, partRelatedEntities
								.Key);
							TimelineEntity relatedEntity = entities[relatedEntityId];
							if (relatedEntity != null)
							{
								if (relatedEntity.GetDomainId().Equals(existingEntity.GetDomainId()))
								{
									relatedEntity.AddRelatedEntity(existingEntity.GetEntityType(), existingEntity.GetEntityId
										());
								}
								else
								{
									// in this case the entity will be put, but the relation will be
									// ignored
									TimelinePutResponse.TimelinePutError error = new TimelinePutResponse.TimelinePutError
										();
									error.SetEntityType(existingEntity.GetEntityType());
									error.SetEntityId(existingEntity.GetEntityId());
									error.SetErrorCode(TimelinePutResponse.TimelinePutError.ForbiddenRelation);
									response.AddError(error);
								}
							}
							else
							{
								relatedEntity = new TimelineEntity();
								relatedEntity.SetEntityId(relatedEntityId.GetId());
								relatedEntity.SetEntityType(relatedEntityId.GetType());
								relatedEntity.SetStartTime(existingEntity.GetStartTime());
								relatedEntity.AddRelatedEntity(existingEntity.GetEntityType(), existingEntity.GetEntityId
									());
								relatedEntity.SetDomainId(existingEntity.GetDomainId());
								entities[relatedEntityId] = relatedEntity;
								entityInsertTimes[relatedEntityId] = Runtime.CurrentTimeMillis();
							}
						}
					}
				}
				return response;
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void Put(TimelineDomain domain)
		{
			TimelineDomain domainToReplace = domainsById[domain.GetId()];
			long currentTimestamp = Runtime.CurrentTimeMillis();
			TimelineDomain domainToStore = CreateTimelineDomain(domain.GetId(), domain.GetDescription
				(), domain.GetOwner(), domain.GetReaders(), domain.GetWriters(), (domainToReplace
				 == null ? currentTimestamp : domainToReplace.GetCreatedTime()), currentTimestamp
				);
			domainsById[domainToStore.GetId()] = domainToStore;
			ICollection<TimelineDomain> domainsByOneOwner = domainsByOwner[domainToStore.GetOwner
				()];
			if (domainsByOneOwner == null)
			{
				domainsByOneOwner = new HashSet<TimelineDomain>();
				domainsByOwner[domainToStore.GetOwner()] = domainsByOneOwner;
			}
			if (domainToReplace != null)
			{
				domainsByOneOwner.Remove(domainToReplace);
			}
			domainsByOneOwner.AddItem(domainToStore);
		}

		private static TimelineDomain CreateTimelineDomain(string id, string description, 
			string owner, string readers, string writers, long createdTime, long modifiedTime
			)
		{
			TimelineDomain domainToStore = new TimelineDomain();
			domainToStore.SetId(id);
			domainToStore.SetDescription(description);
			domainToStore.SetOwner(owner);
			domainToStore.SetReaders(readers);
			domainToStore.SetWriters(writers);
			domainToStore.SetCreatedTime(createdTime);
			domainToStore.SetModifiedTime(modifiedTime);
			return domainToStore;
		}

		private static TimelineEntity MaskFields(TimelineEntity entity, EnumSet<TimelineReader.Field
			> fields)
		{
			// Conceal the fields that are not going to be exposed
			TimelineEntity entityToReturn = new TimelineEntity();
			entityToReturn.SetEntityId(entity.GetEntityId());
			entityToReturn.SetEntityType(entity.GetEntityType());
			entityToReturn.SetStartTime(entity.GetStartTime());
			entityToReturn.SetDomainId(entity.GetDomainId());
			// Deep copy
			if (fields.Contains(TimelineReader.Field.Events))
			{
				entityToReturn.AddEvents(entity.GetEvents());
			}
			else
			{
				if (fields.Contains(TimelineReader.Field.LastEventOnly))
				{
					entityToReturn.AddEvent(entity.GetEvents()[0]);
				}
				else
				{
					entityToReturn.SetEvents(null);
				}
			}
			if (fields.Contains(TimelineReader.Field.RelatedEntities))
			{
				entityToReturn.AddRelatedEntities(entity.GetRelatedEntities());
			}
			else
			{
				entityToReturn.SetRelatedEntities(null);
			}
			if (fields.Contains(TimelineReader.Field.PrimaryFilters))
			{
				entityToReturn.AddPrimaryFilters(entity.GetPrimaryFilters());
			}
			else
			{
				entityToReturn.SetPrimaryFilters(null);
			}
			if (fields.Contains(TimelineReader.Field.OtherInfo))
			{
				entityToReturn.AddOtherInfo(entity.GetOtherInfo());
			}
			else
			{
				entityToReturn.SetOtherInfo(null);
			}
			return entityToReturn;
		}

		private static bool MatchFilter(IDictionary<string, object> tags, NameValuePair filter
			)
		{
			object value = tags[filter.GetName()];
			if (value == null)
			{
				// doesn't have the filter
				return false;
			}
			else
			{
				if (!value.Equals(filter.GetValue()))
				{
					// doesn't match the filter
					return false;
				}
			}
			return true;
		}

		private static bool MatchPrimaryFilter(IDictionary<string, ICollection<object>> tags
			, NameValuePair filter)
		{
			ICollection<object> value = tags[filter.GetName()];
			if (value == null)
			{
				// doesn't have the filter
				return false;
			}
			else
			{
				return value.Contains(filter.GetValue());
			}
		}

		private static object MaybeConvert(object o)
		{
			if (o is long)
			{
				long l = (long)o;
				if (l >= int.MinValue && l <= int.MaxValue)
				{
					return l;
				}
			}
			return o;
		}
	}
}
