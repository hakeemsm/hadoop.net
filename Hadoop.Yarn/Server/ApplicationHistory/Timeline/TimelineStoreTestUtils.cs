using System;
using System.Collections.Generic;
using System.IO;
using Org.Apache.Hadoop.Yarn.Api.Records.Timeline;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Timeline
{
	public class TimelineStoreTestUtils
	{
		protected internal static readonly IList<TimelineEvent> EmptyEvents = Sharpen.Collections
			.EmptyList();

		protected internal static readonly IDictionary<string, object> EmptyMap = Sharpen.Collections
			.EmptyMap();

		protected internal static readonly IDictionary<string, ICollection<object>> EmptyPrimaryFilters
			 = Sharpen.Collections.EmptyMap();

		protected internal static readonly IDictionary<string, ICollection<string>> EmptyRelEntities
			 = Sharpen.Collections.EmptyMap();

		protected internal TimelineStore store;

		protected internal string entityId1;

		protected internal string entityType1;

		protected internal string entityId1b;

		protected internal string entityId2;

		protected internal string entityType2;

		protected internal string entityId4;

		protected internal string entityType4;

		protected internal string entityId5;

		protected internal string entityType5;

		protected internal string entityId6;

		protected internal string entityId7;

		protected internal string entityType7;

		protected internal IDictionary<string, ICollection<object>> primaryFilters;

		protected internal IDictionary<string, object> secondaryFilters;

		protected internal IDictionary<string, object> allFilters;

		protected internal IDictionary<string, object> otherInfo;

		protected internal IDictionary<string, ICollection<string>> relEntityMap;

		protected internal IDictionary<string, ICollection<string>> relEntityMap2;

		protected internal NameValuePair userFilter;

		protected internal NameValuePair numericFilter1;

		protected internal NameValuePair numericFilter2;

		protected internal NameValuePair numericFilter3;

		protected internal ICollection<NameValuePair> goodTestingFilters;

		protected internal ICollection<NameValuePair> badTestingFilters;

		protected internal TimelineEvent ev1;

		protected internal TimelineEvent ev2;

		protected internal TimelineEvent ev3;

		protected internal TimelineEvent ev4;

		protected internal IDictionary<string, object> eventInfo;

		protected internal IList<TimelineEvent> events1;

		protected internal IList<TimelineEvent> events2;

		protected internal long beforeTs;

		protected internal string domainId1;

		protected internal string domainId2;

		/// <summary>Load test entity data into the given store</summary>
		/// <exception cref="System.IO.IOException"/>
		protected internal virtual void LoadTestEntityData()
		{
			beforeTs = Runtime.CurrentTimeMillis() - 1;
			TimelineEntities entities = new TimelineEntities();
			IDictionary<string, ICollection<object>> primaryFilters = new Dictionary<string, 
				ICollection<object>>();
			ICollection<object> l1 = new HashSet<object>();
			l1.AddItem("username");
			ICollection<object> l2 = new HashSet<object>();
			l2.AddItem((long)int.MaxValue);
			ICollection<object> l3 = new HashSet<object>();
			l3.AddItem("123abc");
			ICollection<object> l4 = new HashSet<object>();
			l4.AddItem((long)int.MaxValue + 1l);
			primaryFilters["user"] = l1;
			primaryFilters["appname"] = l2;
			primaryFilters["other"] = l3;
			primaryFilters["long"] = l4;
			IDictionary<string, object> secondaryFilters = new Dictionary<string, object>();
			secondaryFilters["startTime"] = 123456l;
			secondaryFilters["status"] = "RUNNING";
			IDictionary<string, object> otherInfo1 = new Dictionary<string, object>();
			otherInfo1["info1"] = "val1";
			otherInfo1.PutAll(secondaryFilters);
			string entityId1 = "id_1";
			string entityType1 = "type_1";
			string entityId1b = "id_2";
			string entityId2 = "id_2";
			string entityType2 = "type_2";
			string entityId4 = "id_4";
			string entityType4 = "type_4";
			string entityId5 = "id_5";
			string entityType5 = "type_5";
			string entityId6 = "id_6";
			string entityId7 = "id_7";
			string entityType7 = "type_7";
			IDictionary<string, ICollection<string>> relatedEntities = new Dictionary<string, 
				ICollection<string>>();
			relatedEntities[entityType2] = Sharpen.Collections.Singleton(entityId2);
			TimelineEvent ev3 = CreateEvent(789l, "launch_event", null);
			TimelineEvent ev4 = CreateEvent(-123l, "init_event", null);
			IList<TimelineEvent> events = new AList<TimelineEvent>();
			events.AddItem(ev3);
			events.AddItem(ev4);
			entities.SetEntities(Sharpen.Collections.SingletonList(CreateEntity(entityId2, entityType2
				, null, events, null, null, null, "domain_id_1")));
			TimelinePutResponse response = store.Put(entities);
			NUnit.Framework.Assert.AreEqual(0, response.GetErrors().Count);
			TimelineEvent ev1 = CreateEvent(123l, "start_event", null);
			entities.SetEntities(Sharpen.Collections.SingletonList(CreateEntity(entityId1, entityType1
				, 123l, Sharpen.Collections.SingletonList(ev1), relatedEntities, primaryFilters, 
				otherInfo1, "domain_id_1")));
			response = store.Put(entities);
			NUnit.Framework.Assert.AreEqual(0, response.GetErrors().Count);
			entities.SetEntities(Sharpen.Collections.SingletonList(CreateEntity(entityId1b, entityType1
				, null, Sharpen.Collections.SingletonList(ev1), relatedEntities, primaryFilters, 
				otherInfo1, "domain_id_1")));
			response = store.Put(entities);
			NUnit.Framework.Assert.AreEqual(0, response.GetErrors().Count);
			IDictionary<string, object> eventInfo = new Dictionary<string, object>();
			eventInfo["event info 1"] = "val1";
			TimelineEvent ev2 = CreateEvent(456l, "end_event", eventInfo);
			IDictionary<string, object> otherInfo2 = new Dictionary<string, object>();
			otherInfo2["info2"] = "val2";
			entities.SetEntities(Sharpen.Collections.SingletonList(CreateEntity(entityId1, entityType1
				, null, Sharpen.Collections.SingletonList(ev2), null, primaryFilters, otherInfo2
				, "domain_id_1")));
			response = store.Put(entities);
			NUnit.Framework.Assert.AreEqual(0, response.GetErrors().Count);
			entities.SetEntities(Sharpen.Collections.SingletonList(CreateEntity(entityId1b, entityType1
				, 789l, Sharpen.Collections.SingletonList(ev2), null, primaryFilters, otherInfo2
				, "domain_id_1")));
			response = store.Put(entities);
			NUnit.Framework.Assert.AreEqual(0, response.GetErrors().Count);
			entities.SetEntities(Sharpen.Collections.SingletonList(CreateEntity("badentityid"
				, "badentity", null, null, null, null, otherInfo1, "domain_id_1")));
			response = store.Put(entities);
			NUnit.Framework.Assert.AreEqual(1, response.GetErrors().Count);
			TimelinePutResponse.TimelinePutError error = response.GetErrors()[0];
			NUnit.Framework.Assert.AreEqual("badentityid", error.GetEntityId());
			NUnit.Framework.Assert.AreEqual("badentity", error.GetEntityType());
			NUnit.Framework.Assert.AreEqual(TimelinePutResponse.TimelinePutError.NoStartTime, 
				error.GetErrorCode());
			relatedEntities.Clear();
			relatedEntities[entityType5] = Sharpen.Collections.Singleton(entityId5);
			entities.SetEntities(Sharpen.Collections.SingletonList(CreateEntity(entityId4, entityType4
				, 42l, null, relatedEntities, null, null, "domain_id_1")));
			response = store.Put(entities);
			relatedEntities.Clear();
			otherInfo1["info2"] = "val2";
			entities.SetEntities(Sharpen.Collections.SingletonList(CreateEntity(entityId6, entityType1
				, 61l, null, relatedEntities, primaryFilters, otherInfo1, "domain_id_2")));
			response = store.Put(entities);
			relatedEntities.Clear();
			relatedEntities[entityType1] = Sharpen.Collections.Singleton(entityId1);
			entities.SetEntities(Sharpen.Collections.SingletonList(CreateEntity(entityId7, entityType7
				, 62l, null, relatedEntities, null, null, "domain_id_2")));
			response = store.Put(entities);
			NUnit.Framework.Assert.AreEqual(1, response.GetErrors().Count);
			NUnit.Framework.Assert.AreEqual(entityType7, response.GetErrors()[0].GetEntityType
				());
			NUnit.Framework.Assert.AreEqual(entityId7, response.GetErrors()[0].GetEntityId());
			NUnit.Framework.Assert.AreEqual(TimelinePutResponse.TimelinePutError.ForbiddenRelation
				, response.GetErrors()[0].GetErrorCode());
			if (store is LeveldbTimelineStore)
			{
				LeveldbTimelineStore leveldb = (LeveldbTimelineStore)store;
				entities.SetEntities(Sharpen.Collections.SingletonList(CreateEntity("OLD_ENTITY_ID_1"
					, "OLD_ENTITY_TYPE_1", 63l, null, null, null, null, null)));
				leveldb.PutWithNoDomainId(entities);
				entities.SetEntities(Sharpen.Collections.SingletonList(CreateEntity("OLD_ENTITY_ID_2"
					, "OLD_ENTITY_TYPE_1", 64l, null, null, null, null, null)));
				leveldb.PutWithNoDomainId(entities);
			}
		}

		/// <summary>Load verification entity data</summary>
		/// <exception cref="System.Exception"/>
		protected internal virtual void LoadVerificationEntityData()
		{
			userFilter = new NameValuePair("user", "username");
			numericFilter1 = new NameValuePair("appname", int.MaxValue);
			numericFilter2 = new NameValuePair("long", (long)int.MaxValue + 1l);
			numericFilter3 = new NameValuePair("other", "123abc");
			goodTestingFilters = new AList<NameValuePair>();
			goodTestingFilters.AddItem(new NameValuePair("appname", int.MaxValue));
			goodTestingFilters.AddItem(new NameValuePair("status", "RUNNING"));
			badTestingFilters = new AList<NameValuePair>();
			badTestingFilters.AddItem(new NameValuePair("appname", int.MaxValue));
			badTestingFilters.AddItem(new NameValuePair("status", "FINISHED"));
			primaryFilters = new Dictionary<string, ICollection<object>>();
			ICollection<object> l1 = new HashSet<object>();
			l1.AddItem("username");
			ICollection<object> l2 = new HashSet<object>();
			l2.AddItem(int.MaxValue);
			ICollection<object> l3 = new HashSet<object>();
			l3.AddItem("123abc");
			ICollection<object> l4 = new HashSet<object>();
			l4.AddItem((long)int.MaxValue + 1l);
			primaryFilters["user"] = l1;
			primaryFilters["appname"] = l2;
			primaryFilters["other"] = l3;
			primaryFilters["long"] = l4;
			secondaryFilters = new Dictionary<string, object>();
			secondaryFilters["startTime"] = 123456;
			secondaryFilters["status"] = "RUNNING";
			allFilters = new Dictionary<string, object>();
			allFilters.PutAll(secondaryFilters);
			foreach (KeyValuePair<string, ICollection<object>> pf in primaryFilters)
			{
				foreach (object o in pf.Value)
				{
					allFilters[pf.Key] = o;
				}
			}
			otherInfo = new Dictionary<string, object>();
			otherInfo["info1"] = "val1";
			otherInfo["info2"] = "val2";
			otherInfo.PutAll(secondaryFilters);
			entityId1 = "id_1";
			entityType1 = "type_1";
			entityId1b = "id_2";
			entityId2 = "id_2";
			entityType2 = "type_2";
			entityId4 = "id_4";
			entityType4 = "type_4";
			entityId5 = "id_5";
			entityType5 = "type_5";
			entityId6 = "id_6";
			entityId7 = "id_7";
			entityType7 = "type_7";
			ev1 = CreateEvent(123l, "start_event", null);
			eventInfo = new Dictionary<string, object>();
			eventInfo["event info 1"] = "val1";
			ev2 = CreateEvent(456l, "end_event", eventInfo);
			events1 = new AList<TimelineEvent>();
			events1.AddItem(ev2);
			events1.AddItem(ev1);
			relEntityMap = new Dictionary<string, ICollection<string>>();
			ICollection<string> ids = new HashSet<string>();
			ids.AddItem(entityId1);
			ids.AddItem(entityId1b);
			relEntityMap[entityType1] = ids;
			relEntityMap2 = new Dictionary<string, ICollection<string>>();
			relEntityMap2[entityType4] = Sharpen.Collections.Singleton(entityId4);
			ev3 = CreateEvent(789l, "launch_event", null);
			ev4 = CreateEvent(-123l, "init_event", null);
			events2 = new AList<TimelineEvent>();
			events2.AddItem(ev3);
			events2.AddItem(ev4);
			domainId1 = "domain_id_1";
			domainId2 = "domain_id_2";
		}

		private TimelineDomain domain1;

		private TimelineDomain domain2;

		private TimelineDomain domain3;

		private long elapsedTime;

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual void LoadTestDomainData()
		{
			domain1 = new TimelineDomain();
			domain1.SetId("domain_id_1");
			domain1.SetDescription("description_1");
			domain1.SetOwner("owner_1");
			domain1.SetReaders("reader_user_1 reader_group_1");
			domain1.SetWriters("writer_user_1 writer_group_1");
			store.Put(domain1);
			domain2 = new TimelineDomain();
			domain2.SetId("domain_id_2");
			domain2.SetDescription("description_2");
			domain2.SetOwner("owner_2");
			domain2.SetReaders("reader_user_2 reader_group_2");
			domain2.SetWriters("writer_user_2 writer_group_2");
			store.Put(domain2);
			// Wait a second before updating the domain information
			elapsedTime = 1000;
			try
			{
				Sharpen.Thread.Sleep(elapsedTime);
			}
			catch (Exception e)
			{
				throw new IOException(e);
			}
			domain2.SetDescription("description_3");
			domain2.SetOwner("owner_3");
			domain2.SetReaders("reader_user_3 reader_group_3");
			domain2.SetWriters("writer_user_3 writer_group_3");
			store.Put(domain2);
			domain3 = new TimelineDomain();
			domain3.SetId("domain_id_4");
			domain3.SetDescription("description_4");
			domain3.SetOwner("owner_1");
			domain3.SetReaders("reader_user_4 reader_group_4");
			domain3.SetWriters("writer_user_4 writer_group_4");
			store.Put(domain3);
			TimelineEntities entities = new TimelineEntities();
			if (store is LeveldbTimelineStore)
			{
				LeveldbTimelineStore leveldb = (LeveldbTimelineStore)store;
				entities.SetEntities(Sharpen.Collections.SingletonList(CreateEntity("ACL_ENTITY_ID_11"
					, "ACL_ENTITY_TYPE_1", 63l, null, null, null, null, "domain_id_4")));
				leveldb.Put(entities);
				entities.SetEntities(Sharpen.Collections.SingletonList(CreateEntity("ACL_ENTITY_ID_22"
					, "ACL_ENTITY_TYPE_1", 64l, null, null, null, null, "domain_id_2")));
				leveldb.Put(entities);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestGetSingleEntity()
		{
			// test getting entity info
			VerifyEntityInfo(null, null, null, null, null, null, store.GetEntity("id_1", "type_2"
				, EnumSet.AllOf<TimelineReader.Field>()), domainId1);
			VerifyEntityInfo(entityId1, entityType1, events1, EmptyRelEntities, primaryFilters
				, otherInfo, 123l, store.GetEntity(entityId1, entityType1, EnumSet.AllOf<TimelineReader.Field
				>()), domainId1);
			VerifyEntityInfo(entityId1b, entityType1, events1, EmptyRelEntities, primaryFilters
				, otherInfo, 123l, store.GetEntity(entityId1b, entityType1, EnumSet.AllOf<TimelineReader.Field
				>()), domainId1);
			VerifyEntityInfo(entityId2, entityType2, events2, relEntityMap, EmptyPrimaryFilters
				, EmptyMap, -123l, store.GetEntity(entityId2, entityType2, EnumSet.AllOf<TimelineReader.Field
				>()), domainId1);
			VerifyEntityInfo(entityId4, entityType4, EmptyEvents, EmptyRelEntities, EmptyPrimaryFilters
				, EmptyMap, 42l, store.GetEntity(entityId4, entityType4, EnumSet.AllOf<TimelineReader.Field
				>()), domainId1);
			VerifyEntityInfo(entityId5, entityType5, EmptyEvents, relEntityMap2, EmptyPrimaryFilters
				, EmptyMap, 42l, store.GetEntity(entityId5, entityType5, EnumSet.AllOf<TimelineReader.Field
				>()), domainId1);
			// test getting single fields
			VerifyEntityInfo(entityId1, entityType1, events1, null, null, null, store.GetEntity
				(entityId1, entityType1, EnumSet.Of(TimelineReader.Field.Events)), domainId1);
			VerifyEntityInfo(entityId1, entityType1, Sharpen.Collections.SingletonList(ev2), 
				null, null, null, store.GetEntity(entityId1, entityType1, EnumSet.Of(TimelineReader.Field
				.LastEventOnly)), domainId1);
			VerifyEntityInfo(entityId1b, entityType1, events1, EmptyRelEntities, primaryFilters
				, otherInfo, store.GetEntity(entityId1b, entityType1, null), domainId1);
			VerifyEntityInfo(entityId1, entityType1, null, null, primaryFilters, null, store.
				GetEntity(entityId1, entityType1, EnumSet.Of(TimelineReader.Field.PrimaryFilters
				)), domainId1);
			VerifyEntityInfo(entityId1, entityType1, null, null, null, otherInfo, store.GetEntity
				(entityId1, entityType1, EnumSet.Of(TimelineReader.Field.OtherInfo)), domainId1);
			VerifyEntityInfo(entityId2, entityType2, null, relEntityMap, null, null, store.GetEntity
				(entityId2, entityType2, EnumSet.Of(TimelineReader.Field.RelatedEntities)), domainId1
				);
			VerifyEntityInfo(entityId6, entityType1, EmptyEvents, EmptyRelEntities, primaryFilters
				, otherInfo, store.GetEntity(entityId6, entityType1, EnumSet.AllOf<TimelineReader.Field
				>()), domainId2);
			// entity is created, but it doesn't relate to <entityType1, entityId1>
			VerifyEntityInfo(entityId7, entityType7, EmptyEvents, EmptyRelEntities, EmptyPrimaryFilters
				, EmptyMap, store.GetEntity(entityId7, entityType7, EnumSet.AllOf<TimelineReader.Field
				>()), domainId2);
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual IList<TimelineEntity> GetEntities(string entityType)
		{
			return store.GetEntities(entityType, null, null, null, null, null, null, null, null
				, null).GetEntities();
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual IList<TimelineEntity> GetEntitiesWithPrimaryFilter(string
			 entityType, NameValuePair primaryFilter)
		{
			return store.GetEntities(entityType, null, null, null, null, null, primaryFilter, 
				null, null, null).GetEntities();
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual IList<TimelineEntity> GetEntitiesFromId(string entityType
			, string fromId)
		{
			return store.GetEntities(entityType, null, null, null, fromId, null, null, null, 
				null, null).GetEntities();
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual IList<TimelineEntity> GetEntitiesFromTs(string entityType
			, long fromTs)
		{
			return store.GetEntities(entityType, null, null, null, null, fromTs, null, null, 
				null, null).GetEntities();
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual IList<TimelineEntity> GetEntitiesFromIdWithPrimaryFilter
			(string entityType, NameValuePair primaryFilter, string fromId)
		{
			return store.GetEntities(entityType, null, null, null, fromId, null, primaryFilter
				, null, null, null).GetEntities();
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual IList<TimelineEntity> GetEntitiesFromTsWithPrimaryFilter
			(string entityType, NameValuePair primaryFilter, long fromTs)
		{
			return store.GetEntities(entityType, null, null, null, null, fromTs, primaryFilter
				, null, null, null).GetEntities();
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual IList<TimelineEntity> GetEntitiesFromIdWithWindow(string
			 entityType, long windowEnd, string fromId)
		{
			return store.GetEntities(entityType, null, null, windowEnd, fromId, null, null, null
				, null, null).GetEntities();
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual IList<TimelineEntity> GetEntitiesFromIdWithPrimaryFilterAndWindow
			(string entityType, long windowEnd, string fromId, NameValuePair primaryFilter)
		{
			return store.GetEntities(entityType, null, null, windowEnd, fromId, null, primaryFilter
				, null, null, null).GetEntities();
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual IList<TimelineEntity> GetEntitiesWithFilters(string entityType
			, NameValuePair primaryFilter, ICollection<NameValuePair> secondaryFilters)
		{
			return store.GetEntities(entityType, null, null, null, null, null, primaryFilter, 
				secondaryFilters, null, null).GetEntities();
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual IList<TimelineEntity> GetEntities(string entityType, long
			 limit, long windowStart, long windowEnd, NameValuePair primaryFilter, EnumSet<TimelineReader.Field
			> fields)
		{
			return store.GetEntities(entityType, limit, windowStart, windowEnd, null, null, primaryFilter
				, null, fields, null).GetEntities();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestGetEntities()
		{
			// test getting entities
			NUnit.Framework.Assert.AreEqual("nonzero entities size for nonexistent type", 0, 
				GetEntities("type_0").Count);
			NUnit.Framework.Assert.AreEqual("nonzero entities size for nonexistent type", 0, 
				GetEntities("type_3").Count);
			NUnit.Framework.Assert.AreEqual("nonzero entities size for nonexistent type", 0, 
				GetEntities("type_6").Count);
			NUnit.Framework.Assert.AreEqual("nonzero entities size for nonexistent type", 0, 
				GetEntitiesWithPrimaryFilter("type_0", userFilter).Count);
			NUnit.Framework.Assert.AreEqual("nonzero entities size for nonexistent type", 0, 
				GetEntitiesWithPrimaryFilter("type_3", userFilter).Count);
			NUnit.Framework.Assert.AreEqual("nonzero entities size for nonexistent type", 0, 
				GetEntitiesWithPrimaryFilter("type_6", userFilter).Count);
			IList<TimelineEntity> entities = GetEntities("type_1");
			NUnit.Framework.Assert.AreEqual(3, entities.Count);
			VerifyEntityInfo(entityId1, entityType1, events1, EmptyRelEntities, primaryFilters
				, otherInfo, entities[0], domainId1);
			VerifyEntityInfo(entityId1b, entityType1, events1, EmptyRelEntities, primaryFilters
				, otherInfo, entities[1], domainId1);
			VerifyEntityInfo(entityId6, entityType1, EmptyEvents, EmptyRelEntities, primaryFilters
				, otherInfo, entities[2], domainId2);
			entities = GetEntities("type_2");
			NUnit.Framework.Assert.AreEqual(1, entities.Count);
			VerifyEntityInfo(entityId2, entityType2, events2, relEntityMap, EmptyPrimaryFilters
				, EmptyMap, entities[0], domainId1);
			entities = GetEntities("type_1", 1l, null, null, null, EnumSet.AllOf<TimelineReader.Field
				>());
			NUnit.Framework.Assert.AreEqual(1, entities.Count);
			VerifyEntityInfo(entityId1, entityType1, events1, EmptyRelEntities, primaryFilters
				, otherInfo, entities[0], domainId1);
			entities = GetEntities("type_1", 1l, 0l, null, null, EnumSet.AllOf<TimelineReader.Field
				>());
			NUnit.Framework.Assert.AreEqual(1, entities.Count);
			VerifyEntityInfo(entityId1, entityType1, events1, EmptyRelEntities, primaryFilters
				, otherInfo, entities[0], domainId1);
			entities = GetEntities("type_1", null, 234l, null, null, EnumSet.AllOf<TimelineReader.Field
				>());
			NUnit.Framework.Assert.AreEqual(0, entities.Count);
			entities = GetEntities("type_1", null, 123l, null, null, EnumSet.AllOf<TimelineReader.Field
				>());
			NUnit.Framework.Assert.AreEqual(0, entities.Count);
			entities = GetEntities("type_1", null, 234l, 345l, null, EnumSet.AllOf<TimelineReader.Field
				>());
			NUnit.Framework.Assert.AreEqual(0, entities.Count);
			entities = GetEntities("type_1", null, null, 345l, null, EnumSet.AllOf<TimelineReader.Field
				>());
			NUnit.Framework.Assert.AreEqual(3, entities.Count);
			VerifyEntityInfo(entityId1, entityType1, events1, EmptyRelEntities, primaryFilters
				, otherInfo, entities[0], domainId1);
			VerifyEntityInfo(entityId1b, entityType1, events1, EmptyRelEntities, primaryFilters
				, otherInfo, entities[1], domainId1);
			VerifyEntityInfo(entityId6, entityType1, EmptyEvents, EmptyRelEntities, primaryFilters
				, otherInfo, entities[2], domainId2);
			entities = GetEntities("type_1", null, null, 123l, null, EnumSet.AllOf<TimelineReader.Field
				>());
			NUnit.Framework.Assert.AreEqual(3, entities.Count);
			VerifyEntityInfo(entityId1, entityType1, events1, EmptyRelEntities, primaryFilters
				, otherInfo, entities[0], domainId1);
			VerifyEntityInfo(entityId1b, entityType1, events1, EmptyRelEntities, primaryFilters
				, otherInfo, entities[1], domainId1);
			VerifyEntityInfo(entityId6, entityType1, EmptyEvents, EmptyRelEntities, primaryFilters
				, otherInfo, entities[2], domainId2);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestGetEntitiesWithFromId()
		{
			IList<TimelineEntity> entities = GetEntitiesFromId("type_1", entityId1);
			NUnit.Framework.Assert.AreEqual(3, entities.Count);
			VerifyEntityInfo(entityId1, entityType1, events1, EmptyRelEntities, primaryFilters
				, otherInfo, entities[0], domainId1);
			VerifyEntityInfo(entityId1b, entityType1, events1, EmptyRelEntities, primaryFilters
				, otherInfo, entities[1], domainId1);
			VerifyEntityInfo(entityId6, entityType1, EmptyEvents, EmptyRelEntities, primaryFilters
				, otherInfo, entities[2], domainId2);
			entities = GetEntitiesFromId("type_1", entityId1b);
			NUnit.Framework.Assert.AreEqual(2, entities.Count);
			VerifyEntityInfo(entityId1b, entityType1, events1, EmptyRelEntities, primaryFilters
				, otherInfo, entities[0], domainId1);
			VerifyEntityInfo(entityId6, entityType1, EmptyEvents, EmptyRelEntities, primaryFilters
				, otherInfo, entities[1], domainId2);
			entities = GetEntitiesFromId("type_1", entityId6);
			NUnit.Framework.Assert.AreEqual(1, entities.Count);
			VerifyEntityInfo(entityId6, entityType1, EmptyEvents, EmptyRelEntities, primaryFilters
				, otherInfo, entities[0], domainId2);
			entities = GetEntitiesFromIdWithWindow("type_1", 0l, entityId6);
			NUnit.Framework.Assert.AreEqual(0, entities.Count);
			entities = GetEntitiesFromId("type_2", "a");
			NUnit.Framework.Assert.AreEqual(0, entities.Count);
			entities = GetEntitiesFromId("type_2", entityId2);
			NUnit.Framework.Assert.AreEqual(1, entities.Count);
			VerifyEntityInfo(entityId2, entityType2, events2, relEntityMap, EmptyPrimaryFilters
				, EmptyMap, entities[0], domainId1);
			entities = GetEntitiesFromIdWithWindow("type_2", -456l, null);
			NUnit.Framework.Assert.AreEqual(0, entities.Count);
			entities = GetEntitiesFromIdWithWindow("type_2", -456l, "a");
			NUnit.Framework.Assert.AreEqual(0, entities.Count);
			entities = GetEntitiesFromIdWithWindow("type_2", 0l, null);
			NUnit.Framework.Assert.AreEqual(1, entities.Count);
			entities = GetEntitiesFromIdWithWindow("type_2", 0l, entityId2);
			NUnit.Framework.Assert.AreEqual(1, entities.Count);
			// same tests with primary filters
			entities = GetEntitiesFromIdWithPrimaryFilter("type_1", userFilter, entityId1);
			NUnit.Framework.Assert.AreEqual(3, entities.Count);
			VerifyEntityInfo(entityId1, entityType1, events1, EmptyRelEntities, primaryFilters
				, otherInfo, entities[0], domainId1);
			VerifyEntityInfo(entityId1b, entityType1, events1, EmptyRelEntities, primaryFilters
				, otherInfo, entities[1], domainId1);
			VerifyEntityInfo(entityId6, entityType1, EmptyEvents, EmptyRelEntities, primaryFilters
				, otherInfo, entities[2], domainId2);
			entities = GetEntitiesFromIdWithPrimaryFilter("type_1", userFilter, entityId1b);
			NUnit.Framework.Assert.AreEqual(2, entities.Count);
			VerifyEntityInfo(entityId1b, entityType1, events1, EmptyRelEntities, primaryFilters
				, otherInfo, entities[0], domainId1);
			VerifyEntityInfo(entityId6, entityType1, EmptyEvents, EmptyRelEntities, primaryFilters
				, otherInfo, entities[1], domainId2);
			entities = GetEntitiesFromIdWithPrimaryFilter("type_1", userFilter, entityId6);
			NUnit.Framework.Assert.AreEqual(1, entities.Count);
			VerifyEntityInfo(entityId6, entityType1, EmptyEvents, EmptyRelEntities, primaryFilters
				, otherInfo, entities[0], domainId2);
			entities = GetEntitiesFromIdWithPrimaryFilterAndWindow("type_1", 0l, entityId6, userFilter
				);
			NUnit.Framework.Assert.AreEqual(0, entities.Count);
			entities = GetEntitiesFromIdWithPrimaryFilter("type_2", userFilter, "a");
			NUnit.Framework.Assert.AreEqual(0, entities.Count);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestGetEntitiesWithFromTs()
		{
			NUnit.Framework.Assert.AreEqual(0, GetEntitiesFromTs("type_1", beforeTs).Count);
			NUnit.Framework.Assert.AreEqual(0, GetEntitiesFromTs("type_2", beforeTs).Count);
			NUnit.Framework.Assert.AreEqual(0, GetEntitiesFromTsWithPrimaryFilter("type_1", userFilter
				, beforeTs).Count);
			long afterTs = Runtime.CurrentTimeMillis();
			NUnit.Framework.Assert.AreEqual(3, GetEntitiesFromTs("type_1", afterTs).Count);
			NUnit.Framework.Assert.AreEqual(1, GetEntitiesFromTs("type_2", afterTs).Count);
			NUnit.Framework.Assert.AreEqual(3, GetEntitiesFromTsWithPrimaryFilter("type_1", userFilter
				, afterTs).Count);
			NUnit.Framework.Assert.AreEqual(3, GetEntities("type_1").Count);
			NUnit.Framework.Assert.AreEqual(1, GetEntities("type_2").Count);
			NUnit.Framework.Assert.AreEqual(3, GetEntitiesWithPrimaryFilter("type_1", userFilter
				).Count);
			// check insert time is not overwritten
			long beforeTs = this.beforeTs;
			LoadTestEntityData();
			NUnit.Framework.Assert.AreEqual(0, GetEntitiesFromTs("type_1", beforeTs).Count);
			NUnit.Framework.Assert.AreEqual(0, GetEntitiesFromTs("type_2", beforeTs).Count);
			NUnit.Framework.Assert.AreEqual(0, GetEntitiesFromTsWithPrimaryFilter("type_1", userFilter
				, beforeTs).Count);
			NUnit.Framework.Assert.AreEqual(3, GetEntitiesFromTs("type_1", afterTs).Count);
			NUnit.Framework.Assert.AreEqual(1, GetEntitiesFromTs("type_2", afterTs).Count);
			NUnit.Framework.Assert.AreEqual(3, GetEntitiesFromTsWithPrimaryFilter("type_1", userFilter
				, afterTs).Count);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestGetEntitiesWithPrimaryFilters()
		{
			// test using primary filter
			NUnit.Framework.Assert.AreEqual("nonzero entities size for primary filter", 0, GetEntitiesWithPrimaryFilter
				("type_1", new NameValuePair("none", "none")).Count);
			NUnit.Framework.Assert.AreEqual("nonzero entities size for primary filter", 0, GetEntitiesWithPrimaryFilter
				("type_2", new NameValuePair("none", "none")).Count);
			NUnit.Framework.Assert.AreEqual("nonzero entities size for primary filter", 0, GetEntitiesWithPrimaryFilter
				("type_3", new NameValuePair("none", "none")).Count);
			IList<TimelineEntity> entities = GetEntitiesWithPrimaryFilter("type_1", userFilter
				);
			NUnit.Framework.Assert.AreEqual(3, entities.Count);
			VerifyEntityInfo(entityId1, entityType1, events1, EmptyRelEntities, primaryFilters
				, otherInfo, entities[0], domainId1);
			VerifyEntityInfo(entityId1b, entityType1, events1, EmptyRelEntities, primaryFilters
				, otherInfo, entities[1], domainId1);
			VerifyEntityInfo(entityId6, entityType1, EmptyEvents, EmptyRelEntities, primaryFilters
				, otherInfo, entities[2], domainId2);
			entities = GetEntitiesWithPrimaryFilter("type_1", numericFilter1);
			NUnit.Framework.Assert.AreEqual(3, entities.Count);
			VerifyEntityInfo(entityId1, entityType1, events1, EmptyRelEntities, primaryFilters
				, otherInfo, entities[0], domainId1);
			VerifyEntityInfo(entityId1b, entityType1, events1, EmptyRelEntities, primaryFilters
				, otherInfo, entities[1], domainId1);
			VerifyEntityInfo(entityId6, entityType1, EmptyEvents, EmptyRelEntities, primaryFilters
				, otherInfo, entities[2], domainId2);
			entities = GetEntitiesWithPrimaryFilter("type_1", numericFilter2);
			NUnit.Framework.Assert.AreEqual(3, entities.Count);
			VerifyEntityInfo(entityId1, entityType1, events1, EmptyRelEntities, primaryFilters
				, otherInfo, entities[0], domainId1);
			VerifyEntityInfo(entityId1b, entityType1, events1, EmptyRelEntities, primaryFilters
				, otherInfo, entities[1], domainId1);
			VerifyEntityInfo(entityId6, entityType1, EmptyEvents, EmptyRelEntities, primaryFilters
				, otherInfo, entities[2], domainId2);
			entities = GetEntitiesWithPrimaryFilter("type_1", numericFilter3);
			NUnit.Framework.Assert.AreEqual(3, entities.Count);
			VerifyEntityInfo(entityId1, entityType1, events1, EmptyRelEntities, primaryFilters
				, otherInfo, entities[0], domainId1);
			VerifyEntityInfo(entityId1b, entityType1, events1, EmptyRelEntities, primaryFilters
				, otherInfo, entities[1], domainId1);
			VerifyEntityInfo(entityId6, entityType1, EmptyEvents, EmptyRelEntities, primaryFilters
				, otherInfo, entities[2], domainId2);
			entities = GetEntitiesWithPrimaryFilter("type_2", userFilter);
			NUnit.Framework.Assert.AreEqual(0, entities.Count);
			entities = GetEntities("type_1", 1l, null, null, userFilter, null);
			NUnit.Framework.Assert.AreEqual(1, entities.Count);
			VerifyEntityInfo(entityId1, entityType1, events1, EmptyRelEntities, primaryFilters
				, otherInfo, entities[0], domainId1);
			entities = GetEntities("type_1", 1l, 0l, null, userFilter, null);
			NUnit.Framework.Assert.AreEqual(1, entities.Count);
			VerifyEntityInfo(entityId1, entityType1, events1, EmptyRelEntities, primaryFilters
				, otherInfo, entities[0], domainId1);
			entities = GetEntities("type_1", null, 234l, null, userFilter, null);
			NUnit.Framework.Assert.AreEqual(0, entities.Count);
			entities = GetEntities("type_1", null, 234l, 345l, userFilter, null);
			NUnit.Framework.Assert.AreEqual(0, entities.Count);
			entities = GetEntities("type_1", null, null, 345l, userFilter, null);
			NUnit.Framework.Assert.AreEqual(3, entities.Count);
			VerifyEntityInfo(entityId1, entityType1, events1, EmptyRelEntities, primaryFilters
				, otherInfo, entities[0], domainId1);
			VerifyEntityInfo(entityId1b, entityType1, events1, EmptyRelEntities, primaryFilters
				, otherInfo, entities[1], domainId1);
			VerifyEntityInfo(entityId6, entityType1, EmptyEvents, EmptyRelEntities, primaryFilters
				, otherInfo, entities[2], domainId2);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestGetEntitiesWithSecondaryFilters()
		{
			// test using secondary filter
			IList<TimelineEntity> entities = GetEntitiesWithFilters("type_1", null, goodTestingFilters
				);
			NUnit.Framework.Assert.AreEqual(3, entities.Count);
			VerifyEntityInfo(entityId1, entityType1, events1, EmptyRelEntities, primaryFilters
				, otherInfo, entities[0], domainId1);
			VerifyEntityInfo(entityId1b, entityType1, events1, EmptyRelEntities, primaryFilters
				, otherInfo, entities[1], domainId1);
			VerifyEntityInfo(entityId6, entityType1, EmptyEvents, EmptyRelEntities, primaryFilters
				, otherInfo, entities[2], domainId2);
			entities = GetEntitiesWithFilters("type_1", userFilter, goodTestingFilters);
			NUnit.Framework.Assert.AreEqual(3, entities.Count);
			VerifyEntityInfo(entityId1, entityType1, events1, EmptyRelEntities, primaryFilters
				, otherInfo, entities[0], domainId1);
			VerifyEntityInfo(entityId1b, entityType1, events1, EmptyRelEntities, primaryFilters
				, otherInfo, entities[1], domainId1);
			VerifyEntityInfo(entityId6, entityType1, EmptyEvents, EmptyRelEntities, primaryFilters
				, otherInfo, entities[2], domainId2);
			entities = GetEntitiesWithFilters("type_1", null, Sharpen.Collections.Singleton(new 
				NameValuePair("user", "none")));
			NUnit.Framework.Assert.AreEqual(0, entities.Count);
			entities = GetEntitiesWithFilters("type_1", null, badTestingFilters);
			NUnit.Framework.Assert.AreEqual(0, entities.Count);
			entities = GetEntitiesWithFilters("type_1", userFilter, badTestingFilters);
			NUnit.Framework.Assert.AreEqual(0, entities.Count);
			entities = GetEntitiesWithFilters("type_5", null, badTestingFilters);
			NUnit.Framework.Assert.AreEqual(0, entities.Count);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestGetEvents()
		{
			// test getting entity timelines
			ICollection<string> sortedSet = new TreeSet<string>();
			sortedSet.AddItem(entityId1);
			IList<TimelineEvents.EventsOfOneEntity> timelines = store.GetEntityTimelines(entityType1
				, sortedSet, null, null, null, null).GetAllEvents();
			NUnit.Framework.Assert.AreEqual(1, timelines.Count);
			VerifyEntityTimeline(timelines[0], entityId1, entityType1, ev2, ev1);
			sortedSet.AddItem(entityId1b);
			timelines = store.GetEntityTimelines(entityType1, sortedSet, null, null, null, null
				).GetAllEvents();
			NUnit.Framework.Assert.AreEqual(2, timelines.Count);
			VerifyEntityTimeline(timelines[0], entityId1, entityType1, ev2, ev1);
			VerifyEntityTimeline(timelines[1], entityId1b, entityType1, ev2, ev1);
			timelines = store.GetEntityTimelines(entityType1, sortedSet, 1l, null, null, null
				).GetAllEvents();
			NUnit.Framework.Assert.AreEqual(2, timelines.Count);
			VerifyEntityTimeline(timelines[0], entityId1, entityType1, ev2);
			VerifyEntityTimeline(timelines[1], entityId1b, entityType1, ev2);
			timelines = store.GetEntityTimelines(entityType1, sortedSet, null, 345l, null, null
				).GetAllEvents();
			NUnit.Framework.Assert.AreEqual(2, timelines.Count);
			VerifyEntityTimeline(timelines[0], entityId1, entityType1, ev2);
			VerifyEntityTimeline(timelines[1], entityId1b, entityType1, ev2);
			timelines = store.GetEntityTimelines(entityType1, sortedSet, null, 123l, null, null
				).GetAllEvents();
			NUnit.Framework.Assert.AreEqual(2, timelines.Count);
			VerifyEntityTimeline(timelines[0], entityId1, entityType1, ev2);
			VerifyEntityTimeline(timelines[1], entityId1b, entityType1, ev2);
			timelines = store.GetEntityTimelines(entityType1, sortedSet, null, null, 345l, null
				).GetAllEvents();
			NUnit.Framework.Assert.AreEqual(2, timelines.Count);
			VerifyEntityTimeline(timelines[0], entityId1, entityType1, ev1);
			VerifyEntityTimeline(timelines[1], entityId1b, entityType1, ev1);
			timelines = store.GetEntityTimelines(entityType1, sortedSet, null, null, 123l, null
				).GetAllEvents();
			NUnit.Framework.Assert.AreEqual(2, timelines.Count);
			VerifyEntityTimeline(timelines[0], entityId1, entityType1, ev1);
			VerifyEntityTimeline(timelines[1], entityId1b, entityType1, ev1);
			timelines = store.GetEntityTimelines(entityType1, sortedSet, null, null, null, Sharpen.Collections
				.Singleton("end_event")).GetAllEvents();
			NUnit.Framework.Assert.AreEqual(2, timelines.Count);
			VerifyEntityTimeline(timelines[0], entityId1, entityType1, ev2);
			VerifyEntityTimeline(timelines[1], entityId1b, entityType1, ev2);
			sortedSet.AddItem(entityId2);
			timelines = store.GetEntityTimelines(entityType2, sortedSet, null, null, null, null
				).GetAllEvents();
			NUnit.Framework.Assert.AreEqual(1, timelines.Count);
			VerifyEntityTimeline(timelines[0], entityId2, entityType2, ev3, ev4);
		}

		/// <summary>Verify a single entity and its start time</summary>
		protected internal static void VerifyEntityInfo(string entityId, string entityType
			, IList<TimelineEvent> events, IDictionary<string, ICollection<string>> relatedEntities
			, IDictionary<string, ICollection<object>> primaryFilters, IDictionary<string, object
			> otherInfo, long startTime, TimelineEntity retrievedEntityInfo, string domainId
			)
		{
			VerifyEntityInfo(entityId, entityType, events, relatedEntities, primaryFilters, otherInfo
				, retrievedEntityInfo, domainId);
			NUnit.Framework.Assert.AreEqual(startTime, retrievedEntityInfo.GetStartTime());
		}

		/// <summary>Verify a single entity</summary>
		protected internal static void VerifyEntityInfo(string entityId, string entityType
			, IList<TimelineEvent> events, IDictionary<string, ICollection<string>> relatedEntities
			, IDictionary<string, ICollection<object>> primaryFilters, IDictionary<string, object
			> otherInfo, TimelineEntity retrievedEntityInfo, string domainId)
		{
			if (entityId == null)
			{
				NUnit.Framework.Assert.IsNull(retrievedEntityInfo);
				return;
			}
			NUnit.Framework.Assert.AreEqual(entityId, retrievedEntityInfo.GetEntityId());
			NUnit.Framework.Assert.AreEqual(entityType, retrievedEntityInfo.GetEntityType());
			NUnit.Framework.Assert.AreEqual(domainId, retrievedEntityInfo.GetDomainId());
			if (events == null)
			{
				NUnit.Framework.Assert.IsNull(retrievedEntityInfo.GetEvents());
			}
			else
			{
				NUnit.Framework.Assert.AreEqual(events, retrievedEntityInfo.GetEvents());
			}
			if (relatedEntities == null)
			{
				NUnit.Framework.Assert.IsNull(retrievedEntityInfo.GetRelatedEntities());
			}
			else
			{
				NUnit.Framework.Assert.AreEqual(relatedEntities, retrievedEntityInfo.GetRelatedEntities
					());
			}
			if (primaryFilters == null)
			{
				NUnit.Framework.Assert.IsNull(retrievedEntityInfo.GetPrimaryFilters());
			}
			else
			{
				NUnit.Framework.Assert.IsTrue(primaryFilters.Equals(retrievedEntityInfo.GetPrimaryFilters
					()));
			}
			if (otherInfo == null)
			{
				NUnit.Framework.Assert.IsNull(retrievedEntityInfo.GetOtherInfo());
			}
			else
			{
				NUnit.Framework.Assert.IsTrue(otherInfo.Equals(retrievedEntityInfo.GetOtherInfo()
					));
			}
		}

		/// <summary>Verify timeline events</summary>
		private static void VerifyEntityTimeline(TimelineEvents.EventsOfOneEntity retrievedEvents
			, string entityId, string entityType, params TimelineEvent[] actualEvents)
		{
			NUnit.Framework.Assert.AreEqual(entityId, retrievedEvents.GetEntityId());
			NUnit.Framework.Assert.AreEqual(entityType, retrievedEvents.GetEntityType());
			NUnit.Framework.Assert.AreEqual(actualEvents.Length, retrievedEvents.GetEvents().
				Count);
			for (int i = 0; i < actualEvents.Length; i++)
			{
				NUnit.Framework.Assert.AreEqual(actualEvents[i], retrievedEvents.GetEvents()[i]);
			}
		}

		/// <summary>Create a test entity</summary>
		protected internal static TimelineEntity CreateEntity(string entityId, string entityType
			, long startTime, IList<TimelineEvent> events, IDictionary<string, ICollection<string
			>> relatedEntities, IDictionary<string, ICollection<object>> primaryFilters, IDictionary
			<string, object> otherInfo, string domainId)
		{
			TimelineEntity entity = new TimelineEntity();
			entity.SetEntityId(entityId);
			entity.SetEntityType(entityType);
			entity.SetStartTime(startTime);
			entity.SetEvents(events);
			if (relatedEntities != null)
			{
				foreach (KeyValuePair<string, ICollection<string>> e in relatedEntities)
				{
					foreach (string v in e.Value)
					{
						entity.AddRelatedEntity(e.Key, v);
					}
				}
			}
			else
			{
				entity.SetRelatedEntities(null);
			}
			entity.SetPrimaryFilters(primaryFilters);
			entity.SetOtherInfo(otherInfo);
			entity.SetDomainId(domainId);
			return entity;
		}

		/// <summary>Create a test event</summary>
		private static TimelineEvent CreateEvent(long timestamp, string type, IDictionary
			<string, object> info)
		{
			TimelineEvent @event = new TimelineEvent();
			@event.SetTimestamp(timestamp);
			@event.SetEventType(type);
			@event.SetEventInfo(info);
			return @event;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestGetDomain()
		{
			TimelineDomain actualDomain1 = store.GetDomain(domain1.GetId());
			VerifyDomainInfo(domain1, actualDomain1);
			NUnit.Framework.Assert.IsTrue(actualDomain1.GetCreatedTime() > 0);
			NUnit.Framework.Assert.IsTrue(actualDomain1.GetModifiedTime() > 0);
			NUnit.Framework.Assert.AreEqual(actualDomain1.GetCreatedTime(), actualDomain1.GetModifiedTime
				());
			TimelineDomain actualDomain2 = store.GetDomain(domain2.GetId());
			VerifyDomainInfo(domain2, actualDomain2);
			NUnit.Framework.Assert.AreEqual("domain_id_2", actualDomain2.GetId());
			NUnit.Framework.Assert.IsTrue(actualDomain2.GetCreatedTime() > 0);
			NUnit.Framework.Assert.IsTrue(actualDomain2.GetModifiedTime() > 0);
			NUnit.Framework.Assert.IsTrue(actualDomain2.GetCreatedTime() < actualDomain2.GetModifiedTime
				());
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestGetDomains()
		{
			TimelineDomains actualDomains = store.GetDomains("owner_1");
			NUnit.Framework.Assert.AreEqual(2, actualDomains.GetDomains().Count);
			VerifyDomainInfo(domain3, actualDomains.GetDomains()[0]);
			VerifyDomainInfo(domain1, actualDomains.GetDomains()[1]);
			// owner without any domain
			actualDomains = store.GetDomains("owner_4");
			NUnit.Framework.Assert.AreEqual(0, actualDomains.GetDomains().Count);
		}

		private static void VerifyDomainInfo(TimelineDomain expected, TimelineDomain actual
			)
		{
			NUnit.Framework.Assert.AreEqual(expected.GetId(), actual.GetId());
			NUnit.Framework.Assert.AreEqual(expected.GetDescription(), actual.GetDescription(
				));
			NUnit.Framework.Assert.AreEqual(expected.GetOwner(), actual.GetOwner());
			NUnit.Framework.Assert.AreEqual(expected.GetReaders(), actual.GetReaders());
			NUnit.Framework.Assert.AreEqual(expected.GetWriters(), actual.GetWriters());
		}
	}
}
