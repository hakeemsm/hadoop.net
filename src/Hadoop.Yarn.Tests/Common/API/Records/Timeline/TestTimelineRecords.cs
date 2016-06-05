using System.Collections;
using System.Collections.Generic;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Yarn.Util.Timeline;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Records.Timeline
{
	public class TestTimelineRecords
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(TestTimelineRecords));

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestEntities()
		{
			TimelineEntities entities = new TimelineEntities();
			for (int j = 0; j < 2; ++j)
			{
				TimelineEntity entity = new TimelineEntity();
				entity.SetEntityId("entity id " + j);
				entity.SetEntityType("entity type " + j);
				entity.SetStartTime(Runtime.CurrentTimeMillis());
				for (int i = 0; i < 2; ++i)
				{
					TimelineEvent @event = new TimelineEvent();
					@event.SetTimestamp(Runtime.CurrentTimeMillis());
					@event.SetEventType("event type " + i);
					@event.AddEventInfo("key1", "val1");
					@event.AddEventInfo("key2", "val2");
					entity.AddEvent(@event);
				}
				entity.AddRelatedEntity("test ref type 1", "test ref id 1");
				entity.AddRelatedEntity("test ref type 2", "test ref id 2");
				entity.AddPrimaryFilter("pkey1", "pval1");
				entity.AddPrimaryFilter("pkey2", "pval2");
				entity.AddOtherInfo("okey1", "oval1");
				entity.AddOtherInfo("okey2", "oval2");
				entity.SetDomainId("domain id " + j);
				entities.AddEntity(entity);
			}
			Log.Info("Entities in JSON:");
			Log.Info(TimelineUtils.DumpTimelineRecordtoJSON(entities, true));
			NUnit.Framework.Assert.AreEqual(2, entities.GetEntities().Count);
			TimelineEntity entity1 = entities.GetEntities()[0];
			NUnit.Framework.Assert.AreEqual("entity id 0", entity1.GetEntityId());
			NUnit.Framework.Assert.AreEqual("entity type 0", entity1.GetEntityType());
			NUnit.Framework.Assert.AreEqual(2, entity1.GetRelatedEntities().Count);
			NUnit.Framework.Assert.AreEqual(2, entity1.GetEvents().Count);
			NUnit.Framework.Assert.AreEqual(2, entity1.GetPrimaryFilters().Count);
			NUnit.Framework.Assert.AreEqual(2, entity1.GetOtherInfo().Count);
			NUnit.Framework.Assert.AreEqual("domain id 0", entity1.GetDomainId());
			TimelineEntity entity2 = entities.GetEntities()[1];
			NUnit.Framework.Assert.AreEqual("entity id 1", entity2.GetEntityId());
			NUnit.Framework.Assert.AreEqual("entity type 1", entity2.GetEntityType());
			NUnit.Framework.Assert.AreEqual(2, entity2.GetRelatedEntities().Count);
			NUnit.Framework.Assert.AreEqual(2, entity2.GetEvents().Count);
			NUnit.Framework.Assert.AreEqual(2, entity2.GetPrimaryFilters().Count);
			NUnit.Framework.Assert.AreEqual(2, entity2.GetOtherInfo().Count);
			NUnit.Framework.Assert.AreEqual("domain id 1", entity2.GetDomainId());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestEvents()
		{
			TimelineEvents events = new TimelineEvents();
			for (int j = 0; j < 2; ++j)
			{
				TimelineEvents.EventsOfOneEntity partEvents = new TimelineEvents.EventsOfOneEntity
					();
				partEvents.SetEntityId("entity id " + j);
				partEvents.SetEntityType("entity type " + j);
				for (int i = 0; i < 2; ++i)
				{
					TimelineEvent @event = new TimelineEvent();
					@event.SetTimestamp(Runtime.CurrentTimeMillis());
					@event.SetEventType("event type " + i);
					@event.AddEventInfo("key1", "val1");
					@event.AddEventInfo("key2", "val2");
					partEvents.AddEvent(@event);
				}
				events.AddEvent(partEvents);
			}
			Log.Info("Events in JSON:");
			Log.Info(TimelineUtils.DumpTimelineRecordtoJSON(events, true));
			NUnit.Framework.Assert.AreEqual(2, events.GetAllEvents().Count);
			TimelineEvents.EventsOfOneEntity partEvents1 = events.GetAllEvents()[0];
			NUnit.Framework.Assert.AreEqual("entity id 0", partEvents1.GetEntityId());
			NUnit.Framework.Assert.AreEqual("entity type 0", partEvents1.GetEntityType());
			NUnit.Framework.Assert.AreEqual(2, partEvents1.GetEvents().Count);
			TimelineEvent event11 = partEvents1.GetEvents()[0];
			NUnit.Framework.Assert.AreEqual("event type 0", event11.GetEventType());
			NUnit.Framework.Assert.AreEqual(2, event11.GetEventInfo().Count);
			TimelineEvent event12 = partEvents1.GetEvents()[1];
			NUnit.Framework.Assert.AreEqual("event type 1", event12.GetEventType());
			NUnit.Framework.Assert.AreEqual(2, event12.GetEventInfo().Count);
			TimelineEvents.EventsOfOneEntity partEvents2 = events.GetAllEvents()[1];
			NUnit.Framework.Assert.AreEqual("entity id 1", partEvents2.GetEntityId());
			NUnit.Framework.Assert.AreEqual("entity type 1", partEvents2.GetEntityType());
			NUnit.Framework.Assert.AreEqual(2, partEvents2.GetEvents().Count);
			TimelineEvent event21 = partEvents2.GetEvents()[0];
			NUnit.Framework.Assert.AreEqual("event type 0", event21.GetEventType());
			NUnit.Framework.Assert.AreEqual(2, event21.GetEventInfo().Count);
			TimelineEvent event22 = partEvents2.GetEvents()[1];
			NUnit.Framework.Assert.AreEqual("event type 1", event22.GetEventType());
			NUnit.Framework.Assert.AreEqual(2, event22.GetEventInfo().Count);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestTimelinePutErrors()
		{
			TimelinePutResponse TimelinePutErrors = new TimelinePutResponse();
			TimelinePutResponse.TimelinePutError error1 = new TimelinePutResponse.TimelinePutError
				();
			error1.SetEntityId("entity id 1");
			error1.SetEntityId("entity type 1");
			error1.SetErrorCode(TimelinePutResponse.TimelinePutError.NoStartTime);
			TimelinePutErrors.AddError(error1);
			IList<TimelinePutResponse.TimelinePutError> response = new AList<TimelinePutResponse.TimelinePutError
				>();
			response.AddItem(error1);
			TimelinePutResponse.TimelinePutError error2 = new TimelinePutResponse.TimelinePutError
				();
			error2.SetEntityId("entity id 2");
			error2.SetEntityId("entity type 2");
			error2.SetErrorCode(TimelinePutResponse.TimelinePutError.IoException);
			response.AddItem(error2);
			TimelinePutErrors.AddErrors(response);
			Log.Info("Errors in JSON:");
			Log.Info(TimelineUtils.DumpTimelineRecordtoJSON(TimelinePutErrors, true));
			NUnit.Framework.Assert.AreEqual(3, TimelinePutErrors.GetErrors().Count);
			TimelinePutResponse.TimelinePutError e = TimelinePutErrors.GetErrors()[0];
			NUnit.Framework.Assert.AreEqual(error1.GetEntityId(), e.GetEntityId());
			NUnit.Framework.Assert.AreEqual(error1.GetEntityType(), e.GetEntityType());
			NUnit.Framework.Assert.AreEqual(error1.GetErrorCode(), e.GetErrorCode());
			e = TimelinePutErrors.GetErrors()[1];
			NUnit.Framework.Assert.AreEqual(error1.GetEntityId(), e.GetEntityId());
			NUnit.Framework.Assert.AreEqual(error1.GetEntityType(), e.GetEntityType());
			NUnit.Framework.Assert.AreEqual(error1.GetErrorCode(), e.GetErrorCode());
			e = TimelinePutErrors.GetErrors()[2];
			NUnit.Framework.Assert.AreEqual(error2.GetEntityId(), e.GetEntityId());
			NUnit.Framework.Assert.AreEqual(error2.GetEntityType(), e.GetEntityType());
			NUnit.Framework.Assert.AreEqual(error2.GetErrorCode(), e.GetErrorCode());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestTimelineDomain()
		{
			TimelineDomains domains = new TimelineDomains();
			TimelineDomain domain = null;
			for (int i = 0; i < 2; ++i)
			{
				domain = new TimelineDomain();
				domain.SetId("test id " + (i + 1));
				domain.SetDescription("test description " + (i + 1));
				domain.SetOwner("test owner " + (i + 1));
				domain.SetReaders("test_reader_user_" + (i + 1) + " test_reader_group+" + (i + 1)
					);
				domain.SetWriters("test_writer_user_" + (i + 1) + " test_writer_group+" + (i + 1)
					);
				domain.SetCreatedTime(0L);
				domain.SetModifiedTime(1L);
				domains.AddDomain(domain);
			}
			Log.Info("Domain in JSON:");
			Log.Info(TimelineUtils.DumpTimelineRecordtoJSON(domains, true));
			NUnit.Framework.Assert.AreEqual(2, domains.GetDomains().Count);
			for (int i_1 = 0; i_1 < domains.GetDomains().Count; ++i_1)
			{
				domain = domains.GetDomains()[i_1];
				NUnit.Framework.Assert.AreEqual("test id " + (i_1 + 1), domain.GetId());
				NUnit.Framework.Assert.AreEqual("test description " + (i_1 + 1), domain.GetDescription
					());
				NUnit.Framework.Assert.AreEqual("test owner " + (i_1 + 1), domain.GetOwner());
				NUnit.Framework.Assert.AreEqual("test_reader_user_" + (i_1 + 1) + " test_reader_group+"
					 + (i_1 + 1), domain.GetReaders());
				NUnit.Framework.Assert.AreEqual("test_writer_user_" + (i_1 + 1) + " test_writer_group+"
					 + (i_1 + 1), domain.GetWriters());
				NUnit.Framework.Assert.AreEqual(0L, domain.GetCreatedTime());
				NUnit.Framework.Assert.AreEqual(1L, domain.GetModifiedTime());
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestMapInterfaceOrTimelineRecords()
		{
			TimelineEntity entity = new TimelineEntity();
			IList<IDictionary<string, ICollection<object>>> primaryFiltersList = new AList<IDictionary
				<string, ICollection<object>>>();
			primaryFiltersList.AddItem(Sharpen.Collections.SingletonMap("pkey", Sharpen.Collections
				.Singleton((object)"pval")));
			IDictionary<string, ICollection<object>> primaryFilters = new SortedDictionary<string
				, ICollection<object>>();
			primaryFilters["pkey1"] = Sharpen.Collections.Singleton((object)"pval1");
			primaryFilters["pkey2"] = Sharpen.Collections.Singleton((object)"pval2");
			primaryFiltersList.AddItem(primaryFilters);
			entity.SetPrimaryFilters(null);
			foreach (IDictionary<string, ICollection<object>> primaryFiltersToSet in primaryFiltersList)
			{
				entity.SetPrimaryFilters(primaryFiltersToSet);
				AssertPrimaryFilters(entity);
				IDictionary<string, ICollection<object>> primaryFiltersToAdd = new WeakHashMap<string
					, ICollection<object>>();
				primaryFiltersToAdd["pkey3"] = Sharpen.Collections.Singleton((object)"pval3");
				entity.AddPrimaryFilters(primaryFiltersToAdd);
				AssertPrimaryFilters(entity);
			}
			IList<IDictionary<string, ICollection<string>>> relatedEntitiesList = new AList<IDictionary
				<string, ICollection<string>>>();
			relatedEntitiesList.AddItem(Sharpen.Collections.SingletonMap("rkey", Sharpen.Collections
				.Singleton("rval")));
			IDictionary<string, ICollection<string>> relatedEntities = new SortedDictionary<string
				, ICollection<string>>();
			relatedEntities["rkey1"] = Sharpen.Collections.Singleton("rval1");
			relatedEntities["rkey2"] = Sharpen.Collections.Singleton("rval2");
			relatedEntitiesList.AddItem(relatedEntities);
			entity.SetRelatedEntities(null);
			foreach (IDictionary<string, ICollection<string>> relatedEntitiesToSet in relatedEntitiesList)
			{
				entity.SetRelatedEntities(relatedEntitiesToSet);
				AssertRelatedEntities(entity);
				IDictionary<string, ICollection<string>> relatedEntitiesToAdd = new WeakHashMap<string
					, ICollection<string>>();
				relatedEntitiesToAdd["rkey3"] = Sharpen.Collections.Singleton("rval3");
				entity.AddRelatedEntities(relatedEntitiesToAdd);
				AssertRelatedEntities(entity);
			}
			IList<IDictionary<string, object>> otherInfoList = new AList<IDictionary<string, 
				object>>();
			otherInfoList.AddItem(Sharpen.Collections.SingletonMap("okey", (object)"oval"));
			IDictionary<string, object> otherInfo = new SortedDictionary<string, object>();
			otherInfo["okey1"] = "oval1";
			otherInfo["okey2"] = "oval2";
			otherInfoList.AddItem(otherInfo);
			entity.SetOtherInfo(null);
			foreach (IDictionary<string, object> otherInfoToSet in otherInfoList)
			{
				entity.SetOtherInfo(otherInfoToSet);
				AssertOtherInfo(entity);
				IDictionary<string, object> otherInfoToAdd = new WeakHashMap<string, object>();
				otherInfoToAdd["okey3"] = "oval3";
				entity.AddOtherInfo(otherInfoToAdd);
				AssertOtherInfo(entity);
			}
			TimelineEvent @event = new TimelineEvent();
			IList<IDictionary<string, object>> eventInfoList = new AList<IDictionary<string, 
				object>>();
			eventInfoList.AddItem(Sharpen.Collections.SingletonMap("ekey", (object)"eval"));
			IDictionary<string, object> eventInfo = new SortedDictionary<string, object>();
			eventInfo["ekey1"] = "eval1";
			eventInfo["ekey2"] = "eval2";
			eventInfoList.AddItem(eventInfo);
			@event.SetEventInfo(null);
			foreach (IDictionary<string, object> eventInfoToSet in eventInfoList)
			{
				@event.SetEventInfo(eventInfoToSet);
				AssertEventInfo(@event);
				IDictionary<string, object> eventInfoToAdd = new WeakHashMap<string, object>();
				eventInfoToAdd["ekey3"] = "eval3";
				@event.AddEventInfo(eventInfoToAdd);
				AssertEventInfo(@event);
			}
		}

		private static void AssertPrimaryFilters(TimelineEntity entity)
		{
			NUnit.Framework.Assert.IsNotNull(entity.GetPrimaryFilters());
			NUnit.Framework.Assert.IsNotNull(entity.GetPrimaryFiltersJAXB());
			NUnit.Framework.Assert.IsTrue(entity.GetPrimaryFilters() is Hashtable);
			NUnit.Framework.Assert.IsTrue(entity.GetPrimaryFiltersJAXB() is Hashtable);
			NUnit.Framework.Assert.AreEqual(entity.GetPrimaryFilters(), entity.GetPrimaryFiltersJAXB
				());
		}

		private static void AssertRelatedEntities(TimelineEntity entity)
		{
			NUnit.Framework.Assert.IsNotNull(entity.GetRelatedEntities());
			NUnit.Framework.Assert.IsNotNull(entity.GetRelatedEntitiesJAXB());
			NUnit.Framework.Assert.IsTrue(entity.GetRelatedEntities() is Hashtable);
			NUnit.Framework.Assert.IsTrue(entity.GetRelatedEntitiesJAXB() is Hashtable);
			NUnit.Framework.Assert.AreEqual(entity.GetRelatedEntities(), entity.GetRelatedEntitiesJAXB
				());
		}

		private static void AssertOtherInfo(TimelineEntity entity)
		{
			NUnit.Framework.Assert.IsNotNull(entity.GetOtherInfo());
			NUnit.Framework.Assert.IsNotNull(entity.GetOtherInfoJAXB());
			NUnit.Framework.Assert.IsTrue(entity.GetOtherInfo() is Hashtable);
			NUnit.Framework.Assert.IsTrue(entity.GetOtherInfoJAXB() is Hashtable);
			NUnit.Framework.Assert.AreEqual(entity.GetOtherInfo(), entity.GetOtherInfoJAXB());
		}

		private static void AssertEventInfo(TimelineEvent @event)
		{
			NUnit.Framework.Assert.IsNotNull(@event);
			NUnit.Framework.Assert.IsNotNull(@event.GetEventInfoJAXB());
			NUnit.Framework.Assert.IsTrue(@event.GetEventInfo() is Hashtable);
			NUnit.Framework.Assert.IsTrue(@event.GetEventInfoJAXB() is Hashtable);
			NUnit.Framework.Assert.AreEqual(@event.GetEventInfo(), @event.GetEventInfoJAXB());
		}
	}
}
