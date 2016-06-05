using System.Collections;
using System.Collections.Generic;
using Org.Apache.Hadoop.Classification;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Records.Timeline
{
	/// <summary>
	/// <p>
	/// The class that contains the the meta information of some conceptual entity
	/// and its related events.
	/// </summary>
	/// <remarks>
	/// <p>
	/// The class that contains the the meta information of some conceptual entity
	/// and its related events. The entity can be an application, an application
	/// attempt, a container or whatever the user-defined object.
	/// </p>
	/// <p>
	/// Primary filters will be used to index the entities in
	/// <code>TimelineStore</code>, such that users should carefully choose the
	/// information they want to store as the primary filters. The remaining can be
	/// stored as other information.
	/// </p>
	/// </remarks>
	public class TimelineEntity : Comparable<Org.Apache.Hadoop.Yarn.Api.Records.Timeline.TimelineEntity
		>
	{
		private string entityType;

		private string entityId;

		private long startTime;

		private IList<TimelineEvent> events = new AList<TimelineEvent>();

		private Dictionary<string, ICollection<string>> relatedEntities = new Dictionary<
			string, ICollection<string>>();

		private Dictionary<string, ICollection<object>> primaryFilters = new Dictionary<string
			, ICollection<object>>();

		private Dictionary<string, object> otherInfo = new Dictionary<string, object>();

		private string domainId;

		public TimelineEntity()
		{
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

		/// <summary>Get the start time of the entity</summary>
		/// <returns>the start time of the entity</returns>
		public virtual long GetStartTime()
		{
			return startTime;
		}

		/// <summary>Set the start time of the entity</summary>
		/// <param name="startTime">the start time of the entity</param>
		public virtual void SetStartTime(long startTime)
		{
			this.startTime = startTime;
		}

		/// <summary>Get a list of events related to the entity</summary>
		/// <returns>a list of events related to the entity</returns>
		public virtual IList<TimelineEvent> GetEvents()
		{
			return events;
		}

		/// <summary>Add a single event related to the entity to the existing event list</summary>
		/// <param name="event">a single event related to the entity</param>
		public virtual void AddEvent(TimelineEvent @event)
		{
			events.AddItem(@event);
		}

		/// <summary>Add a list of events related to the entity to the existing event list</summary>
		/// <param name="events">a list of events related to the entity</param>
		public virtual void AddEvents(IList<TimelineEvent> events)
		{
			Sharpen.Collections.AddAll(this.events, events);
		}

		/// <summary>Set the event list to the given list of events related to the entity</summary>
		/// <param name="events">events a list of events related to the entity</param>
		public virtual void SetEvents(IList<TimelineEvent> events)
		{
			this.events = events;
		}

		/// <summary>Get the related entities</summary>
		/// <returns>the related entities</returns>
		public virtual IDictionary<string, ICollection<string>> GetRelatedEntities()
		{
			return relatedEntities;
		}

		// Required by JAXB
		[InterfaceAudience.Private]
		public virtual Dictionary<string, ICollection<string>> GetRelatedEntitiesJAXB()
		{
			return relatedEntities;
		}

		/// <summary>Add an entity to the existing related entity map</summary>
		/// <param name="entityType">the entity type</param>
		/// <param name="entityId">the entity Id</param>
		public virtual void AddRelatedEntity(string entityType, string entityId)
		{
			ICollection<string> thisRelatedEntity = relatedEntities[entityType];
			if (thisRelatedEntity == null)
			{
				thisRelatedEntity = new HashSet<string>();
				relatedEntities[entityType] = thisRelatedEntity;
			}
			thisRelatedEntity.AddItem(entityId);
		}

		/// <summary>Add a map of related entities to the existing related entity map</summary>
		/// <param name="relatedEntities">a map of related entities</param>
		public virtual void AddRelatedEntities(IDictionary<string, ICollection<string>> relatedEntities
			)
		{
			foreach (KeyValuePair<string, ICollection<string>> relatedEntity in relatedEntities)
			{
				ICollection<string> thisRelatedEntity = this.relatedEntities[relatedEntity.Key];
				if (thisRelatedEntity == null)
				{
					this.relatedEntities[relatedEntity.Key] = relatedEntity.Value;
				}
				else
				{
					Sharpen.Collections.AddAll(thisRelatedEntity, relatedEntity.Value);
				}
			}
		}

		/// <summary>Set the related entity map to the given map of related entities</summary>
		/// <param name="relatedEntities">a map of related entities</param>
		public virtual void SetRelatedEntities(IDictionary<string, ICollection<string>> relatedEntities
			)
		{
			if (relatedEntities != null && !(relatedEntities is Hashtable))
			{
				this.relatedEntities = new Dictionary<string, ICollection<string>>(relatedEntities
					);
			}
			else
			{
				this.relatedEntities = (Dictionary<string, ICollection<string>>)relatedEntities;
			}
		}

		/// <summary>Get the primary filters</summary>
		/// <returns>the primary filters</returns>
		public virtual IDictionary<string, ICollection<object>> GetPrimaryFilters()
		{
			return primaryFilters;
		}

		// Required by JAXB
		[InterfaceAudience.Private]
		public virtual Dictionary<string, ICollection<object>> GetPrimaryFiltersJAXB()
		{
			return primaryFilters;
		}

		/// <summary>Add a single piece of primary filter to the existing primary filter map</summary>
		/// <param name="key">the primary filter key</param>
		/// <param name="value">the primary filter value</param>
		public virtual void AddPrimaryFilter(string key, object value)
		{
			ICollection<object> thisPrimaryFilter = primaryFilters[key];
			if (thisPrimaryFilter == null)
			{
				thisPrimaryFilter = new HashSet<object>();
				primaryFilters[key] = thisPrimaryFilter;
			}
			thisPrimaryFilter.AddItem(value);
		}

		/// <summary>Add a map of primary filters to the existing primary filter map</summary>
		/// <param name="primaryFilters">a map of primary filters</param>
		public virtual void AddPrimaryFilters(IDictionary<string, ICollection<object>> primaryFilters
			)
		{
			foreach (KeyValuePair<string, ICollection<object>> primaryFilter in primaryFilters)
			{
				ICollection<object> thisPrimaryFilter = this.primaryFilters[primaryFilter.Key];
				if (thisPrimaryFilter == null)
				{
					this.primaryFilters[primaryFilter.Key] = primaryFilter.Value;
				}
				else
				{
					Sharpen.Collections.AddAll(thisPrimaryFilter, primaryFilter.Value);
				}
			}
		}

		/// <summary>Set the primary filter map to the given map of primary filters</summary>
		/// <param name="primaryFilters">a map of primary filters</param>
		public virtual void SetPrimaryFilters(IDictionary<string, ICollection<object>> primaryFilters
			)
		{
			if (primaryFilters != null && !(primaryFilters is Hashtable))
			{
				this.primaryFilters = new Dictionary<string, ICollection<object>>(primaryFilters);
			}
			else
			{
				this.primaryFilters = (Dictionary<string, ICollection<object>>)primaryFilters;
			}
		}

		/// <summary>Get the other information of the entity</summary>
		/// <returns>the other information of the entity</returns>
		public virtual IDictionary<string, object> GetOtherInfo()
		{
			return otherInfo;
		}

		// Required by JAXB
		[InterfaceAudience.Private]
		public virtual Dictionary<string, object> GetOtherInfoJAXB()
		{
			return otherInfo;
		}

		/// <summary>
		/// Add one piece of other information of the entity to the existing other info
		/// map
		/// </summary>
		/// <param name="key">the other information key</param>
		/// <param name="value">the other information value</param>
		public virtual void AddOtherInfo(string key, object value)
		{
			this.otherInfo[key] = value;
		}

		/// <summary>Add a map of other information of the entity to the existing other info map
		/// 	</summary>
		/// <param name="otherInfo">a map of other information</param>
		public virtual void AddOtherInfo(IDictionary<string, object> otherInfo)
		{
			this.otherInfo.PutAll(otherInfo);
		}

		/// <summary>Set the other info map to the given map of other information</summary>
		/// <param name="otherInfo">a map of other information</param>
		public virtual void SetOtherInfo(IDictionary<string, object> otherInfo)
		{
			if (otherInfo != null && !(otherInfo is Hashtable))
			{
				this.otherInfo = new Dictionary<string, object>(otherInfo);
			}
			else
			{
				this.otherInfo = (Dictionary<string, object>)otherInfo;
			}
		}

		/// <summary>Get the ID of the domain that the entity is to be put</summary>
		/// <returns>the domain ID</returns>
		public virtual string GetDomainId()
		{
			return domainId;
		}

		/// <summary>Set the ID of the domain that the entity is to be put</summary>
		/// <param name="domainId">the name space ID</param>
		public virtual void SetDomainId(string domainId)
		{
			this.domainId = domainId;
		}

		public override int GetHashCode()
		{
			// generated by eclipse
			int prime = 31;
			int result = 1;
			result = prime * result + ((entityId == null) ? 0 : entityId.GetHashCode());
			result = prime * result + ((entityType == null) ? 0 : entityType.GetHashCode());
			result = prime * result + ((events == null) ? 0 : events.GetHashCode());
			result = prime * result + ((otherInfo == null) ? 0 : otherInfo.GetHashCode());
			result = prime * result + ((primaryFilters == null) ? 0 : primaryFilters.GetHashCode
				());
			result = prime * result + ((relatedEntities == null) ? 0 : relatedEntities.GetHashCode
				());
			result = prime * result + ((startTime == null) ? 0 : startTime.GetHashCode());
			return result;
		}

		public override bool Equals(object obj)
		{
			// generated by eclipse
			if (this == obj)
			{
				return true;
			}
			if (obj == null)
			{
				return false;
			}
			if (GetType() != obj.GetType())
			{
				return false;
			}
			Org.Apache.Hadoop.Yarn.Api.Records.Timeline.TimelineEntity other = (Org.Apache.Hadoop.Yarn.Api.Records.Timeline.TimelineEntity
				)obj;
			if (entityId == null)
			{
				if (other.entityId != null)
				{
					return false;
				}
			}
			else
			{
				if (!entityId.Equals(other.entityId))
				{
					return false;
				}
			}
			if (entityType == null)
			{
				if (other.entityType != null)
				{
					return false;
				}
			}
			else
			{
				if (!entityType.Equals(other.entityType))
				{
					return false;
				}
			}
			if (events == null)
			{
				if (other.events != null)
				{
					return false;
				}
			}
			else
			{
				if (!events.Equals(other.events))
				{
					return false;
				}
			}
			if (otherInfo == null)
			{
				if (other.otherInfo != null)
				{
					return false;
				}
			}
			else
			{
				if (!otherInfo.Equals(other.otherInfo))
				{
					return false;
				}
			}
			if (primaryFilters == null)
			{
				if (other.primaryFilters != null)
				{
					return false;
				}
			}
			else
			{
				if (!primaryFilters.Equals(other.primaryFilters))
				{
					return false;
				}
			}
			if (relatedEntities == null)
			{
				if (other.relatedEntities != null)
				{
					return false;
				}
			}
			else
			{
				if (!relatedEntities.Equals(other.relatedEntities))
				{
					return false;
				}
			}
			if (startTime == null)
			{
				if (other.startTime != null)
				{
					return false;
				}
			}
			else
			{
				if (!startTime.Equals(other.startTime))
				{
					return false;
				}
			}
			return true;
		}

		public virtual int CompareTo(Org.Apache.Hadoop.Yarn.Api.Records.Timeline.TimelineEntity
			 other)
		{
			int comparison = string.CompareOrdinal(entityType, other.entityType);
			if (comparison == 0)
			{
				long thisStartTime = startTime == null ? long.MinValue : startTime;
				long otherStartTime = other.startTime == null ? long.MinValue : other.startTime;
				if (thisStartTime > otherStartTime)
				{
					return -1;
				}
				else
				{
					if (thisStartTime < otherStartTime)
					{
						return 1;
					}
					else
					{
						return string.CompareOrdinal(entityId, other.entityId);
					}
				}
			}
			else
			{
				return comparison;
			}
		}
	}
}
