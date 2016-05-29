using System;
using System.Collections.Generic;
using Com.Google.Common.Annotations;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Service;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Api.Records.Timeline;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Server.Timeline.Security;
using Org.Apache.Hadoop.Yarn.Util;
using Org.Apache.Hadoop.Yarn.Util.Timeline;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Timeline
{
	/// <summary>The class wrap over the timeline store and the ACLs manager.</summary>
	/// <remarks>
	/// The class wrap over the timeline store and the ACLs manager. It does some non
	/// trivial manipulation of the timeline data before putting or after getting it
	/// from the timeline store, and checks the user's access to it.
	/// </remarks>
	public class TimelineDataManager : AbstractService
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Yarn.Server.Timeline.TimelineDataManager
			));

		[VisibleForTesting]
		public const string DefaultDomainId = "DEFAULT";

		private TimelineStore store;

		private TimelineACLsManager timelineACLsManager;

		public TimelineDataManager(TimelineStore store, TimelineACLsManager timelineACLsManager
			)
			: base(typeof(Org.Apache.Hadoop.Yarn.Server.Timeline.TimelineDataManager).FullName
				)
		{
			this.store = store;
			this.timelineACLsManager = timelineACLsManager;
			timelineACLsManager.SetTimelineStore(store);
		}

		/// <exception cref="System.Exception"/>
		protected override void ServiceInit(Configuration conf)
		{
			TimelineDomain domain = store.GetDomain("DEFAULT");
			// it is okay to reuse an existing domain even if it was created by another
			// user of the timeline server before, because it allows everybody to access.
			if (domain == null)
			{
				// create a default domain, which allows everybody to access and modify
				// the entities in it.
				domain = new TimelineDomain();
				domain.SetId(DefaultDomainId);
				domain.SetDescription("System Default Domain");
				domain.SetOwner(UserGroupInformation.GetCurrentUser().GetShortUserName());
				domain.SetReaders("*");
				domain.SetWriters("*");
				store.Put(domain);
			}
			base.ServiceInit(conf);
		}

		public interface CheckAcl
		{
			/// <exception cref="System.IO.IOException"/>
			bool Check(TimelineEntity entity);
		}

		internal class CheckAclImpl : TimelineDataManager.CheckAcl
		{
			internal readonly UserGroupInformation ugi;

			public CheckAclImpl(TimelineDataManager _enclosing, UserGroupInformation callerUGI
				)
			{
				this._enclosing = _enclosing;
				this.ugi = callerUGI;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual bool Check(TimelineEntity entity)
			{
				try
				{
					return this._enclosing.timelineACLsManager.CheckAccess(this.ugi, ApplicationAccessType
						.ViewApp, entity);
				}
				catch (YarnException e)
				{
					TimelineDataManager.Log.Info("Error when verifying access for user " + this.ugi +
						 " on the events of the timeline entity " + new EntityIdentifier(entity.GetEntityId
						(), entity.GetEntityType()), e);
					return false;
				}
			}

			private readonly TimelineDataManager _enclosing;
		}

		/// <summary>Get the timeline entities that the given user have access to.</summary>
		/// <remarks>
		/// Get the timeline entities that the given user have access to. The meaning
		/// of each argument has been documented with
		/// <see cref="TimelineReader.GetEntities(string, long, long, long, string, long, NameValuePair, System.Collections.Generic.ICollection{E}, Sharpen.EnumSet{E}, CheckAcl)
		/// 	"/>
		/// .
		/// </remarks>
		/// <seealso cref="TimelineReader.GetEntities(string, long, long, long, string, long, NameValuePair, System.Collections.Generic.ICollection{E}, Sharpen.EnumSet{E}, CheckAcl)
		/// 	"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual TimelineEntities GetEntities(string entityType, NameValuePair primaryFilter
			, ICollection<NameValuePair> secondaryFilter, long windowStart, long windowEnd, 
			string fromId, long fromTs, long limit, EnumSet<TimelineReader.Field> fields, UserGroupInformation
			 callerUGI)
		{
			TimelineEntities entities = null;
			entities = store.GetEntities(entityType, limit, windowStart, windowEnd, fromId, fromTs
				, primaryFilter, secondaryFilter, fields, new TimelineDataManager.CheckAclImpl(this
				, callerUGI));
			if (entities == null)
			{
				return new TimelineEntities();
			}
			return entities;
		}

		/// <summary>Get the single timeline entity that the given user has access to.</summary>
		/// <remarks>
		/// Get the single timeline entity that the given user has access to. The
		/// meaning of each argument has been documented with
		/// <see cref="TimelineReader.GetEntity(string, string, Sharpen.EnumSet{E})"/>
		/// .
		/// </remarks>
		/// <seealso cref="TimelineReader.GetEntity(string, string, Sharpen.EnumSet{E})"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual TimelineEntity GetEntity(string entityType, string entityId, EnumSet
			<TimelineReader.Field> fields, UserGroupInformation callerUGI)
		{
			TimelineEntity entity = null;
			entity = store.GetEntity(entityId, entityType, fields);
			if (entity != null)
			{
				AddDefaultDomainIdIfAbsent(entity);
				// check ACLs
				if (!timelineACLsManager.CheckAccess(callerUGI, ApplicationAccessType.ViewApp, entity
					))
				{
					entity = null;
				}
			}
			return entity;
		}

		/// <summary>Get the events whose entities the given user has access to.</summary>
		/// <remarks>
		/// Get the events whose entities the given user has access to. The meaning of
		/// each argument has been documented with
		/// <see cref="TimelineReader.GetEntityTimelines(string, System.Collections.Generic.ICollection{E}, long, long, long, System.Collections.Generic.ICollection{E})
		/// 	"/>
		/// .
		/// </remarks>
		/// <seealso cref="TimelineReader.GetEntityTimelines(string, System.Collections.Generic.ICollection{E}, long, long, long, System.Collections.Generic.ICollection{E})
		/// 	"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual TimelineEvents GetEvents(string entityType, ICollection<string> entityIds
			, ICollection<string> eventTypes, long windowStart, long windowEnd, long limit, 
			UserGroupInformation callerUGI)
		{
			TimelineEvents events = null;
			events = store.GetEntityTimelines(entityType, entityIds, limit, windowStart, windowEnd
				, eventTypes);
			if (events != null)
			{
				IEnumerator<TimelineEvents.EventsOfOneEntity> eventsItr = events.GetAllEvents().GetEnumerator
					();
				while (eventsItr.HasNext())
				{
					TimelineEvents.EventsOfOneEntity eventsOfOneEntity = eventsItr.Next();
					try
					{
						TimelineEntity entity = store.GetEntity(eventsOfOneEntity.GetEntityId(), eventsOfOneEntity
							.GetEntityType(), EnumSet.Of(TimelineReader.Field.PrimaryFilters));
						AddDefaultDomainIdIfAbsent(entity);
						// check ACLs
						if (!timelineACLsManager.CheckAccess(callerUGI, ApplicationAccessType.ViewApp, entity
							))
						{
							eventsItr.Remove();
						}
					}
					catch (Exception e)
					{
						Log.Error("Error when verifying access for user " + callerUGI + " on the events of the timeline entity "
							 + new EntityIdentifier(eventsOfOneEntity.GetEntityId(), eventsOfOneEntity.GetEntityType
							()), e);
						eventsItr.Remove();
					}
				}
			}
			if (events == null)
			{
				return new TimelineEvents();
			}
			return events;
		}

		/// <summary>
		/// Store the timeline entities into the store and set the owner of them to the
		/// given user.
		/// </summary>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual TimelinePutResponse PostEntities(TimelineEntities entities, UserGroupInformation
			 callerUGI)
		{
			if (entities == null)
			{
				return new TimelinePutResponse();
			}
			IList<EntityIdentifier> entityIDs = new AList<EntityIdentifier>();
			TimelineEntities entitiesToPut = new TimelineEntities();
			IList<TimelinePutResponse.TimelinePutError> errors = new AList<TimelinePutResponse.TimelinePutError
				>();
			foreach (TimelineEntity entity in entities.GetEntities())
			{
				EntityIdentifier entityID = new EntityIdentifier(entity.GetEntityId(), entity.GetEntityType
					());
				// if the domain id is not specified, the entity will be put into
				// the default domain
				if (entity.GetDomainId() == null || entity.GetDomainId().Length == 0)
				{
					entity.SetDomainId(DefaultDomainId);
				}
				// check if there is existing entity
				TimelineEntity existingEntity = null;
				try
				{
					existingEntity = store.GetEntity(entityID.GetId(), entityID.GetType(), EnumSet.Of
						(TimelineReader.Field.PrimaryFilters));
					if (existingEntity != null)
					{
						AddDefaultDomainIdIfAbsent(existingEntity);
						if (!existingEntity.GetDomainId().Equals(entity.GetDomainId()))
						{
							throw new YarnException("The domain of the timeline entity " + entityID + " is not allowed to be changed."
								);
						}
					}
					if (!timelineACLsManager.CheckAccess(callerUGI, ApplicationAccessType.ModifyApp, 
						entity))
					{
						throw new YarnException(callerUGI + " is not allowed to put the timeline entity "
							 + entityID + " into the domain " + entity.GetDomainId() + ".");
					}
				}
				catch (Exception e)
				{
					// Skip the entity which already exists and was put by others
					Log.Error("Skip the timeline entity: " + entityID, e);
					TimelinePutResponse.TimelinePutError error = new TimelinePutResponse.TimelinePutError
						();
					error.SetEntityId(entityID.GetId());
					error.SetEntityType(entityID.GetType());
					error.SetErrorCode(TimelinePutResponse.TimelinePutError.AccessDenied);
					errors.AddItem(error);
					continue;
				}
				entityIDs.AddItem(entityID);
				entitiesToPut.AddEntity(entity);
				if (Log.IsDebugEnabled())
				{
					Log.Debug("Storing the entity " + entityID + ", JSON-style content: " + TimelineUtils
						.DumpTimelineRecordtoJSON(entity));
				}
			}
			if (Log.IsDebugEnabled())
			{
				Log.Debug("Storing entities: " + StringHelper.CsvJoiner.Join(entityIDs));
			}
			TimelinePutResponse response = store.Put(entitiesToPut);
			// add the errors of timeline system filter key conflict
			response.AddErrors(errors);
			return response;
		}

		/// <summary>Add or update an domain.</summary>
		/// <remarks>
		/// Add or update an domain. If the domain already exists, only the owner
		/// and the admin can update it.
		/// </remarks>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual void PutDomain(TimelineDomain domain, UserGroupInformation callerUGI
			)
		{
			TimelineDomain existingDomain = store.GetDomain(domain.GetId());
			if (existingDomain != null)
			{
				if (!timelineACLsManager.CheckAccess(callerUGI, existingDomain))
				{
					throw new YarnException(callerUGI.GetShortUserName() + " is not allowed to override an existing domain "
						 + existingDomain.GetId());
				}
				// Set it again in case ACLs are not enabled: The domain can be
				// modified by every body, but the owner is not changed.
				domain.SetOwner(existingDomain.GetOwner());
			}
			store.Put(domain);
			// If the domain exists already, it is likely to be in the cache.
			// We need to invalidate it.
			if (existingDomain != null)
			{
				timelineACLsManager.ReplaceIfExist(domain);
			}
		}

		/// <summary>Get a single domain of the particular ID.</summary>
		/// <remarks>
		/// Get a single domain of the particular ID. If callerUGI is not the owner
		/// or the admin of the domain, null will be returned.
		/// </remarks>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual TimelineDomain GetDomain(string domainId, UserGroupInformation callerUGI
			)
		{
			TimelineDomain domain = store.GetDomain(domainId);
			if (domain != null)
			{
				if (timelineACLsManager.CheckAccess(callerUGI, domain))
				{
					return domain;
				}
			}
			return null;
		}

		/// <summary>Get all the domains that belong to the given owner.</summary>
		/// <remarks>
		/// Get all the domains that belong to the given owner. If callerUGI is not
		/// the owner or the admin of the domain, empty list is going to be returned.
		/// </remarks>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual TimelineDomains GetDomains(string owner, UserGroupInformation callerUGI
			)
		{
			TimelineDomains domains = store.GetDomains(owner);
			bool hasAccess = true;
			if (domains.GetDomains().Count > 0)
			{
				// The owner for each domain is the same, just need to check one
				hasAccess = timelineACLsManager.CheckAccess(callerUGI, domains.GetDomains()[0]);
			}
			if (hasAccess)
			{
				return domains;
			}
			else
			{
				return new TimelineDomains();
			}
		}

		private static void AddDefaultDomainIdIfAbsent(TimelineEntity entity)
		{
			// be compatible with the timeline data created before 2.6
			if (entity.GetDomainId() == null)
			{
				entity.SetDomainId(DefaultDomainId);
			}
		}
	}
}
