using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Yarn.Api.Records.Timeline;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Security;
using Org.Apache.Hadoop.Yarn.Server.Timeline.Security;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Timeline
{
	public class TestTimelineDataManager : TimelineStoreTestUtils
	{
		private FileContext fsContext;

		private FilePath fsPath;

		private TimelineDataManager dataManaer;

		private static TimelineACLsManager aclsManager;

		private static AdminACLsManager adminACLsManager;

		/// <exception cref="System.Exception"/>
		[SetUp]
		public virtual void Setup()
		{
			fsPath = new FilePath("target", this.GetType().Name + "-tmpDir").GetAbsoluteFile(
				);
			fsContext = FileContext.GetLocalFSFileContext();
			fsContext.Delete(new Path(fsPath.GetAbsolutePath()), true);
			Configuration conf = new YarnConfiguration();
			conf.Set(YarnConfiguration.TimelineServiceLeveldbPath, fsPath.GetAbsolutePath());
			conf.SetBoolean(YarnConfiguration.TimelineServiceTtlEnable, false);
			store = new LeveldbTimelineStore();
			store.Init(conf);
			store.Start();
			LoadTestEntityData();
			LoadVerificationEntityData();
			LoadTestDomainData();
			conf.SetBoolean(YarnConfiguration.YarnAclEnable, false);
			aclsManager = new TimelineACLsManager(conf);
			dataManaer = new TimelineDataManager(store, aclsManager);
			conf.SetBoolean(YarnConfiguration.YarnAclEnable, true);
			conf.Set(YarnConfiguration.YarnAdminAcl, "admin");
			adminACLsManager = new AdminACLsManager(conf);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.TearDown]
		public virtual void TearDown()
		{
			if (store != null)
			{
				store.Stop();
			}
			if (fsContext != null)
			{
				fsContext.Delete(new Path(fsPath.GetAbsolutePath()), true);
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGetOldEntityWithOutDomainId()
		{
			TimelineEntity entity = dataManaer.GetEntity("OLD_ENTITY_TYPE_1", "OLD_ENTITY_ID_1"
				, null, UserGroupInformation.GetCurrentUser());
			NUnit.Framework.Assert.IsNotNull(entity);
			NUnit.Framework.Assert.AreEqual("OLD_ENTITY_ID_1", entity.GetEntityId());
			NUnit.Framework.Assert.AreEqual("OLD_ENTITY_TYPE_1", entity.GetEntityType());
			NUnit.Framework.Assert.AreEqual(TimelineDataManager.DefaultDomainId, entity.GetDomainId
				());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGetEntitiesAclEnabled()
		{
			AdminACLsManager oldAdminACLsManager = aclsManager.SetAdminACLsManager(adminACLsManager
				);
			try
			{
				TimelineEntities entities = dataManaer.GetEntities("ACL_ENTITY_TYPE_1", null, null
					, null, null, null, null, 1l, null, UserGroupInformation.CreateUserForTesting("owner_1"
					, new string[] { "group1" }));
				NUnit.Framework.Assert.AreEqual(1, entities.GetEntities().Count);
				NUnit.Framework.Assert.AreEqual("ACL_ENTITY_ID_11", entities.GetEntities()[0].GetEntityId
					());
			}
			finally
			{
				aclsManager.SetAdminACLsManager(oldAdminACLsManager);
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestGetOldEntitiesWithOutDomainId()
		{
			TimelineEntities entities = dataManaer.GetEntities("OLD_ENTITY_TYPE_1", null, null
				, null, null, null, null, null, null, UserGroupInformation.GetCurrentUser());
			NUnit.Framework.Assert.AreEqual(2, entities.GetEntities().Count);
			NUnit.Framework.Assert.AreEqual("OLD_ENTITY_ID_2", entities.GetEntities()[0].GetEntityId
				());
			NUnit.Framework.Assert.AreEqual("OLD_ENTITY_TYPE_1", entities.GetEntities()[0].GetEntityType
				());
			NUnit.Framework.Assert.AreEqual(TimelineDataManager.DefaultDomainId, entities.GetEntities
				()[0].GetDomainId());
			NUnit.Framework.Assert.AreEqual("OLD_ENTITY_ID_1", entities.GetEntities()[1].GetEntityId
				());
			NUnit.Framework.Assert.AreEqual("OLD_ENTITY_TYPE_1", entities.GetEntities()[1].GetEntityType
				());
			NUnit.Framework.Assert.AreEqual(TimelineDataManager.DefaultDomainId, entities.GetEntities
				()[1].GetDomainId());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestUpdatingOldEntityWithoutDomainId()
		{
			// Set the domain to the default domain when updating
			TimelineEntity entity = new TimelineEntity();
			entity.SetEntityType("OLD_ENTITY_TYPE_1");
			entity.SetEntityId("OLD_ENTITY_ID_1");
			entity.SetDomainId(TimelineDataManager.DefaultDomainId);
			entity.AddOtherInfo("NEW_OTHER_INFO_KEY", "NEW_OTHER_INFO_VALUE");
			TimelineEntities entities = new TimelineEntities();
			entities.AddEntity(entity);
			TimelinePutResponse response = dataManaer.PostEntities(entities, UserGroupInformation
				.GetCurrentUser());
			NUnit.Framework.Assert.AreEqual(0, response.GetErrors().Count);
			entity = store.GetEntity("OLD_ENTITY_ID_1", "OLD_ENTITY_TYPE_1", null);
			NUnit.Framework.Assert.IsNotNull(entity);
			// Even in leveldb, the domain is updated to the default domain Id
			NUnit.Framework.Assert.AreEqual(TimelineDataManager.DefaultDomainId, entity.GetDomainId
				());
			NUnit.Framework.Assert.AreEqual(1, entity.GetOtherInfo().Count);
			NUnit.Framework.Assert.AreEqual("NEW_OTHER_INFO_KEY", entity.GetOtherInfo().Keys.
				GetEnumerator().Next());
			NUnit.Framework.Assert.AreEqual("NEW_OTHER_INFO_VALUE", entity.GetOtherInfo().Values
				.GetEnumerator().Next());
			// Set the domain to the non-default domain when updating
			entity = new TimelineEntity();
			entity.SetEntityType("OLD_ENTITY_TYPE_1");
			entity.SetEntityId("OLD_ENTITY_ID_2");
			entity.SetDomainId("NON_DEFAULT");
			entity.AddOtherInfo("NEW_OTHER_INFO_KEY", "NEW_OTHER_INFO_VALUE");
			entities = new TimelineEntities();
			entities.AddEntity(entity);
			response = dataManaer.PostEntities(entities, UserGroupInformation.GetCurrentUser(
				));
			NUnit.Framework.Assert.AreEqual(1, response.GetErrors().Count);
			NUnit.Framework.Assert.AreEqual(TimelinePutResponse.TimelinePutError.AccessDenied
				, response.GetErrors()[0].GetErrorCode());
			entity = store.GetEntity("OLD_ENTITY_ID_2", "OLD_ENTITY_TYPE_1", null);
			NUnit.Framework.Assert.IsNotNull(entity);
			// In leveldb, the domain Id is still null
			NUnit.Framework.Assert.IsNull(entity.GetDomainId());
			// Updating is not executed
			NUnit.Framework.Assert.AreEqual(0, entity.GetOtherInfo().Count);
		}
	}
}
