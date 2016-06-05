using System;
using System.Collections.Generic;
using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Service;
using Org.Apache.Hadoop.Yarn.Api.Records.Timeline;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Server.Records;
using Org.Apache.Hadoop.Yarn.Server.Utils;
using Org.Iq80.Leveldb;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Timeline
{
	public class TestLeveldbTimelineStore : TimelineStoreTestUtils
	{
		private FileContext fsContext;

		private FilePath fsPath;

		private Configuration config = new YarnConfiguration();

		/// <exception cref="System.Exception"/>
		[SetUp]
		public virtual void Setup()
		{
			fsContext = FileContext.GetLocalFSFileContext();
			fsPath = new FilePath("target", this.GetType().Name + "-tmpDir").GetAbsoluteFile(
				);
			fsContext.Delete(new Path(fsPath.GetAbsolutePath()), true);
			config.Set(YarnConfiguration.TimelineServiceLeveldbPath, fsPath.GetAbsolutePath()
				);
			config.SetBoolean(YarnConfiguration.TimelineServiceTtlEnable, false);
			store = new LeveldbTimelineStore();
			store.Init(config);
			store.Start();
			LoadTestEntityData();
			LoadVerificationEntityData();
			LoadTestDomainData();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.TearDown]
		public virtual void TearDown()
		{
			store.Stop();
			fsContext.Delete(new Path(fsPath.GetAbsolutePath()), true);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestRootDirPermission()
		{
			FileSystem fs = FileSystem.GetLocal(new YarnConfiguration());
			FileStatus file = fs.GetFileStatus(new Path(fsPath.GetAbsolutePath(), LeveldbTimelineStore
				.Filename));
			NUnit.Framework.Assert.IsNotNull(file);
			NUnit.Framework.Assert.AreEqual(LeveldbTimelineStore.LeveldbDirUmask, file.GetPermission
				());
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public override void TestGetSingleEntity()
		{
			base.TestGetSingleEntity();
			((LeveldbTimelineStore)store).ClearStartTimeCache();
			base.TestGetSingleEntity();
			LoadTestEntityData();
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public override void TestGetEntities()
		{
			base.TestGetEntities();
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public override void TestGetEntitiesWithFromId()
		{
			base.TestGetEntitiesWithFromId();
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public override void TestGetEntitiesWithFromTs()
		{
			base.TestGetEntitiesWithFromTs();
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public override void TestGetEntitiesWithPrimaryFilters()
		{
			base.TestGetEntitiesWithPrimaryFilters();
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public override void TestGetEntitiesWithSecondaryFilters()
		{
			base.TestGetEntitiesWithSecondaryFilters();
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public override void TestGetEvents()
		{
			base.TestGetEvents();
		}

		[NUnit.Framework.Test]
		public virtual void TestCacheSizes()
		{
			Configuration conf = new Configuration();
			NUnit.Framework.Assert.AreEqual(10000, LeveldbTimelineStore.GetStartTimeReadCacheSize
				(conf));
			NUnit.Framework.Assert.AreEqual(10000, LeveldbTimelineStore.GetStartTimeWriteCacheSize
				(conf));
			conf.SetInt(YarnConfiguration.TimelineServiceLeveldbStartTimeReadCacheSize, 10001
				);
			NUnit.Framework.Assert.AreEqual(10001, LeveldbTimelineStore.GetStartTimeReadCacheSize
				(conf));
			conf = new Configuration();
			conf.SetInt(YarnConfiguration.TimelineServiceLeveldbStartTimeWriteCacheSize, 10002
				);
			NUnit.Framework.Assert.AreEqual(10002, LeveldbTimelineStore.GetStartTimeWriteCacheSize
				(conf));
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		private bool DeleteNextEntity(string entityType, byte[] ts)
		{
			LeveldbIterator iterator = null;
			LeveldbIterator pfIterator = null;
			try
			{
				iterator = ((LeveldbTimelineStore)store).GetDbIterator(false);
				pfIterator = ((LeveldbTimelineStore)store).GetDbIterator(false);
				return ((LeveldbTimelineStore)store).DeleteNextEntity(entityType, ts, iterator, pfIterator
					, false);
			}
			catch (DBException e)
			{
				throw new IOException(e);
			}
			finally
			{
				IOUtils.Cleanup(null, iterator, pfIterator);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestGetEntityTypes()
		{
			IList<string> entityTypes = ((LeveldbTimelineStore)store).GetEntityTypes();
			NUnit.Framework.Assert.AreEqual(7, entityTypes.Count);
			NUnit.Framework.Assert.AreEqual("ACL_ENTITY_TYPE_1", entityTypes[0]);
			NUnit.Framework.Assert.AreEqual("OLD_ENTITY_TYPE_1", entityTypes[1]);
			NUnit.Framework.Assert.AreEqual(entityType1, entityTypes[2]);
			NUnit.Framework.Assert.AreEqual(entityType2, entityTypes[3]);
			NUnit.Framework.Assert.AreEqual(entityType4, entityTypes[4]);
			NUnit.Framework.Assert.AreEqual(entityType5, entityTypes[5]);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestDeleteEntities()
		{
			NUnit.Framework.Assert.AreEqual(3, GetEntities("type_1").Count);
			NUnit.Framework.Assert.AreEqual(1, GetEntities("type_2").Count);
			NUnit.Framework.Assert.AreEqual(false, DeleteNextEntity(entityType1, GenericObjectMapper.WriteReverseOrderedLong
				(60l)));
			NUnit.Framework.Assert.AreEqual(3, GetEntities("type_1").Count);
			NUnit.Framework.Assert.AreEqual(1, GetEntities("type_2").Count);
			NUnit.Framework.Assert.AreEqual(true, DeleteNextEntity(entityType1, GenericObjectMapper.WriteReverseOrderedLong
				(123l)));
			IList<TimelineEntity> entities = GetEntities("type_2");
			NUnit.Framework.Assert.AreEqual(1, entities.Count);
			VerifyEntityInfo(entityId2, entityType2, events2, Sharpen.Collections.SingletonMap
				(entityType1, Sharpen.Collections.Singleton(entityId1b)), EmptyPrimaryFilters, EmptyMap
				, entities[0], domainId1);
			entities = GetEntitiesWithPrimaryFilter("type_1", userFilter);
			NUnit.Framework.Assert.AreEqual(2, entities.Count);
			VerifyEntityInfo(entityId1b, entityType1, events1, EmptyRelEntities, primaryFilters
				, otherInfo, entities[0], domainId1);
			// can retrieve entities across domains
			VerifyEntityInfo(entityId6, entityType1, EmptyEvents, EmptyRelEntities, primaryFilters
				, otherInfo, entities[1], domainId2);
			((LeveldbTimelineStore)store).DiscardOldEntities(-123l);
			NUnit.Framework.Assert.AreEqual(2, GetEntities("type_1").Count);
			NUnit.Framework.Assert.AreEqual(0, GetEntities("type_2").Count);
			NUnit.Framework.Assert.AreEqual(6, ((LeveldbTimelineStore)store).GetEntityTypes()
				.Count);
			((LeveldbTimelineStore)store).DiscardOldEntities(123l);
			NUnit.Framework.Assert.AreEqual(0, GetEntities("type_1").Count);
			NUnit.Framework.Assert.AreEqual(0, GetEntities("type_2").Count);
			NUnit.Framework.Assert.AreEqual(0, ((LeveldbTimelineStore)store).GetEntityTypes()
				.Count);
			NUnit.Framework.Assert.AreEqual(0, GetEntitiesWithPrimaryFilter("type_1", userFilter
				).Count);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestDeleteEntitiesPrimaryFilters()
		{
			IDictionary<string, ICollection<object>> primaryFilter = Sharpen.Collections.SingletonMap
				("user", Sharpen.Collections.Singleton((object)"otheruser"));
			TimelineEntities atsEntities = new TimelineEntities();
			atsEntities.SetEntities(Sharpen.Collections.SingletonList(CreateEntity(entityId1b
				, entityType1, 789l, Sharpen.Collections.SingletonList(ev2), null, primaryFilter
				, null, domainId1)));
			TimelinePutResponse response = store.Put(atsEntities);
			NUnit.Framework.Assert.AreEqual(0, response.GetErrors().Count);
			NameValuePair pfPair = new NameValuePair("user", "otheruser");
			IList<TimelineEntity> entities = GetEntitiesWithPrimaryFilter("type_1", pfPair);
			NUnit.Framework.Assert.AreEqual(1, entities.Count);
			VerifyEntityInfo(entityId1b, entityType1, Sharpen.Collections.SingletonList(ev2), 
				EmptyRelEntities, primaryFilter, EmptyMap, entities[0], domainId1);
			entities = GetEntitiesWithPrimaryFilter("type_1", userFilter);
			NUnit.Framework.Assert.AreEqual(3, entities.Count);
			VerifyEntityInfo(entityId1, entityType1, events1, EmptyRelEntities, primaryFilters
				, otherInfo, entities[0], domainId1);
			VerifyEntityInfo(entityId1b, entityType1, events1, EmptyRelEntities, primaryFilters
				, otherInfo, entities[1], domainId1);
			VerifyEntityInfo(entityId6, entityType1, EmptyEvents, EmptyRelEntities, primaryFilters
				, otherInfo, entities[2], domainId2);
			((LeveldbTimelineStore)store).DiscardOldEntities(-123l);
			NUnit.Framework.Assert.AreEqual(1, GetEntitiesWithPrimaryFilter("type_1", pfPair)
				.Count);
			NUnit.Framework.Assert.AreEqual(3, GetEntitiesWithPrimaryFilter("type_1", userFilter
				).Count);
			((LeveldbTimelineStore)store).DiscardOldEntities(123l);
			NUnit.Framework.Assert.AreEqual(0, GetEntities("type_1").Count);
			NUnit.Framework.Assert.AreEqual(0, GetEntities("type_2").Count);
			NUnit.Framework.Assert.AreEqual(0, ((LeveldbTimelineStore)store).GetEntityTypes()
				.Count);
			NUnit.Framework.Assert.AreEqual(0, GetEntitiesWithPrimaryFilter("type_1", pfPair)
				.Count);
			NUnit.Framework.Assert.AreEqual(0, GetEntitiesWithPrimaryFilter("type_1", userFilter
				).Count);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestFromTsWithDeletion()
		{
			long l = Runtime.CurrentTimeMillis();
			NUnit.Framework.Assert.AreEqual(3, GetEntitiesFromTs("type_1", l).Count);
			NUnit.Framework.Assert.AreEqual(1, GetEntitiesFromTs("type_2", l).Count);
			NUnit.Framework.Assert.AreEqual(3, GetEntitiesFromTsWithPrimaryFilter("type_1", userFilter
				, l).Count);
			((LeveldbTimelineStore)store).DiscardOldEntities(123l);
			NUnit.Framework.Assert.AreEqual(0, GetEntitiesFromTs("type_1", l).Count);
			NUnit.Framework.Assert.AreEqual(0, GetEntitiesFromTs("type_2", l).Count);
			NUnit.Framework.Assert.AreEqual(0, GetEntitiesFromTsWithPrimaryFilter("type_1", userFilter
				, l).Count);
			NUnit.Framework.Assert.AreEqual(0, GetEntities("type_1").Count);
			NUnit.Framework.Assert.AreEqual(0, GetEntities("type_2").Count);
			NUnit.Framework.Assert.AreEqual(0, GetEntitiesFromTsWithPrimaryFilter("type_1", userFilter
				, l).Count);
			LoadTestEntityData();
			NUnit.Framework.Assert.AreEqual(0, GetEntitiesFromTs("type_1", l).Count);
			NUnit.Framework.Assert.AreEqual(0, GetEntitiesFromTs("type_2", l).Count);
			NUnit.Framework.Assert.AreEqual(0, GetEntitiesFromTsWithPrimaryFilter("type_1", userFilter
				, l).Count);
			NUnit.Framework.Assert.AreEqual(3, GetEntities("type_1").Count);
			NUnit.Framework.Assert.AreEqual(1, GetEntities("type_2").Count);
			NUnit.Framework.Assert.AreEqual(3, GetEntitiesWithPrimaryFilter("type_1", userFilter
				).Count);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestCheckVersion()
		{
			LeveldbTimelineStore dbStore = (LeveldbTimelineStore)store;
			// default version
			Version defaultVersion = dbStore.GetCurrentVersion();
			NUnit.Framework.Assert.AreEqual(defaultVersion, dbStore.LoadVersion());
			// compatible version
			Version compatibleVersion = Version.NewInstance(defaultVersion.GetMajorVersion(), 
				defaultVersion.GetMinorVersion() + 2);
			dbStore.StoreVersion(compatibleVersion);
			NUnit.Framework.Assert.AreEqual(compatibleVersion, dbStore.LoadVersion());
			RestartTimelineStore();
			dbStore = (LeveldbTimelineStore)store;
			// overwrite the compatible version
			NUnit.Framework.Assert.AreEqual(defaultVersion, dbStore.LoadVersion());
			// incompatible version
			Version incompatibleVersion = Version.NewInstance(defaultVersion.GetMajorVersion(
				) + 1, defaultVersion.GetMinorVersion());
			dbStore.StoreVersion(incompatibleVersion);
			try
			{
				RestartTimelineStore();
				NUnit.Framework.Assert.Fail("Incompatible version, should expect fail here.");
			}
			catch (ServiceStateException e)
			{
				NUnit.Framework.Assert.IsTrue("Exception message mismatch", e.Message.Contains("Incompatible version for timeline store"
					));
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestValidateConfig()
		{
			Configuration copyConfig = new YarnConfiguration(config);
			try
			{
				Configuration newConfig = new YarnConfiguration(copyConfig);
				newConfig.SetLong(YarnConfiguration.TimelineServiceTtlMs, 0);
				config = newConfig;
				RestartTimelineStore();
				NUnit.Framework.Assert.Fail();
			}
			catch (ArgumentException e)
			{
				NUnit.Framework.Assert.IsTrue(e.Message.Contains(YarnConfiguration.TimelineServiceTtlMs
					));
			}
			try
			{
				Configuration newConfig = new YarnConfiguration(copyConfig);
				newConfig.SetLong(YarnConfiguration.TimelineServiceLeveldbTtlIntervalMs, 0);
				config = newConfig;
				RestartTimelineStore();
				NUnit.Framework.Assert.Fail();
			}
			catch (ArgumentException e)
			{
				NUnit.Framework.Assert.IsTrue(e.Message.Contains(YarnConfiguration.TimelineServiceLeveldbTtlIntervalMs
					));
			}
			try
			{
				Configuration newConfig = new YarnConfiguration(copyConfig);
				newConfig.SetLong(YarnConfiguration.TimelineServiceLeveldbReadCacheSize, -1);
				config = newConfig;
				RestartTimelineStore();
				NUnit.Framework.Assert.Fail();
			}
			catch (ArgumentException e)
			{
				NUnit.Framework.Assert.IsTrue(e.Message.Contains(YarnConfiguration.TimelineServiceLeveldbReadCacheSize
					));
			}
			try
			{
				Configuration newConfig = new YarnConfiguration(copyConfig);
				newConfig.SetLong(YarnConfiguration.TimelineServiceLeveldbStartTimeReadCacheSize, 
					0);
				config = newConfig;
				RestartTimelineStore();
				NUnit.Framework.Assert.Fail();
			}
			catch (ArgumentException e)
			{
				NUnit.Framework.Assert.IsTrue(e.Message.Contains(YarnConfiguration.TimelineServiceLeveldbStartTimeReadCacheSize
					));
			}
			try
			{
				Configuration newConfig = new YarnConfiguration(copyConfig);
				newConfig.SetLong(YarnConfiguration.TimelineServiceLeveldbStartTimeWriteCacheSize
					, 0);
				config = newConfig;
				RestartTimelineStore();
				NUnit.Framework.Assert.Fail();
			}
			catch (ArgumentException e)
			{
				NUnit.Framework.Assert.IsTrue(e.Message.Contains(YarnConfiguration.TimelineServiceLeveldbStartTimeWriteCacheSize
					));
			}
			config = copyConfig;
			RestartTimelineStore();
		}

		/// <exception cref="System.IO.IOException"/>
		private void RestartTimelineStore()
		{
			// need to close so leveldb releases database lock
			if (store != null)
			{
				store.Close();
			}
			store = new LeveldbTimelineStore();
			store.Init(config);
			store.Start();
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public override void TestGetDomain()
		{
			base.TestGetDomain();
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public override void TestGetDomains()
		{
			base.TestGetDomains();
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestRelatingToNonExistingEntity()
		{
			TimelineEntity entityToStore = new TimelineEntity();
			entityToStore.SetEntityType("TEST_ENTITY_TYPE_1");
			entityToStore.SetEntityId("TEST_ENTITY_ID_1");
			entityToStore.SetDomainId(TimelineDataManager.DefaultDomainId);
			entityToStore.AddRelatedEntity("TEST_ENTITY_TYPE_2", "TEST_ENTITY_ID_2");
			TimelineEntities entities = new TimelineEntities();
			entities.AddEntity(entityToStore);
			store.Put(entities);
			TimelineEntity entityToGet = store.GetEntity("TEST_ENTITY_ID_2", "TEST_ENTITY_TYPE_2"
				, null);
			NUnit.Framework.Assert.IsNotNull(entityToGet);
			NUnit.Framework.Assert.AreEqual("DEFAULT", entityToGet.GetDomainId());
			NUnit.Framework.Assert.AreEqual("TEST_ENTITY_TYPE_1", entityToGet.GetRelatedEntities
				().Keys.GetEnumerator().Next());
			NUnit.Framework.Assert.AreEqual("TEST_ENTITY_ID_1", entityToGet.GetRelatedEntities
				().Values.GetEnumerator().Next().GetEnumerator().Next());
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestRelatingToOldEntityWithoutDomainId()
		{
			// New entity is put in the default domain
			TimelineEntity entityToStore = new TimelineEntity();
			entityToStore.SetEntityType("NEW_ENTITY_TYPE_1");
			entityToStore.SetEntityId("NEW_ENTITY_ID_1");
			entityToStore.SetDomainId(TimelineDataManager.DefaultDomainId);
			entityToStore.AddRelatedEntity("OLD_ENTITY_TYPE_1", "OLD_ENTITY_ID_1");
			TimelineEntities entities = new TimelineEntities();
			entities.AddEntity(entityToStore);
			store.Put(entities);
			TimelineEntity entityToGet = store.GetEntity("OLD_ENTITY_ID_1", "OLD_ENTITY_TYPE_1"
				, null);
			NUnit.Framework.Assert.IsNotNull(entityToGet);
			NUnit.Framework.Assert.IsNull(entityToGet.GetDomainId());
			NUnit.Framework.Assert.AreEqual("NEW_ENTITY_TYPE_1", entityToGet.GetRelatedEntities
				().Keys.GetEnumerator().Next());
			NUnit.Framework.Assert.AreEqual("NEW_ENTITY_ID_1", entityToGet.GetRelatedEntities
				().Values.GetEnumerator().Next().GetEnumerator().Next());
			// New entity is not put in the default domain
			entityToStore = new TimelineEntity();
			entityToStore.SetEntityType("NEW_ENTITY_TYPE_2");
			entityToStore.SetEntityId("NEW_ENTITY_ID_2");
			entityToStore.SetDomainId("NON_DEFAULT");
			entityToStore.AddRelatedEntity("OLD_ENTITY_TYPE_1", "OLD_ENTITY_ID_1");
			entities = new TimelineEntities();
			entities.AddEntity(entityToStore);
			TimelinePutResponse response = store.Put(entities);
			NUnit.Framework.Assert.AreEqual(1, response.GetErrors().Count);
			NUnit.Framework.Assert.AreEqual(TimelinePutResponse.TimelinePutError.ForbiddenRelation
				, response.GetErrors()[0].GetErrorCode());
			entityToGet = store.GetEntity("OLD_ENTITY_ID_1", "OLD_ENTITY_TYPE_1", null);
			NUnit.Framework.Assert.IsNotNull(entityToGet);
			NUnit.Framework.Assert.IsNull(entityToGet.GetDomainId());
			// Still have one related entity
			NUnit.Framework.Assert.AreEqual(1, entityToGet.GetRelatedEntities().Keys.Count);
			NUnit.Framework.Assert.AreEqual(1, entityToGet.GetRelatedEntities().Values.GetEnumerator
				().Next().Count);
		}
	}
}
