using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Api.Records.Timeline;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Server.Timeline;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Timeline.Security
{
	public class TestTimelineACLsManager
	{
		private static TimelineDomain domain;

		static TestTimelineACLsManager()
		{
			domain = new TimelineDomain();
			domain.SetId("domain_id_1");
			domain.SetOwner("owner");
			domain.SetReaders("reader");
			domain.SetWriters("writer");
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestYarnACLsNotEnabledForEntity()
		{
			Configuration conf = new YarnConfiguration();
			conf.SetBoolean(YarnConfiguration.YarnAclEnable, false);
			TimelineACLsManager timelineACLsManager = new TimelineACLsManager(conf);
			timelineACLsManager.SetTimelineStore(new TestTimelineACLsManager.TestTimelineStore
				());
			TimelineEntity entity = new TimelineEntity();
			entity.AddPrimaryFilter(TimelineStore.SystemFilter.EntityOwner.ToString(), "owner"
				);
			entity.SetDomainId("domain_id_1");
			NUnit.Framework.Assert.IsTrue("Always true when ACLs are not enabled", timelineACLsManager
				.CheckAccess(UserGroupInformation.CreateRemoteUser("user"), ApplicationAccessType
				.ViewApp, entity));
			NUnit.Framework.Assert.IsTrue("Always true when ACLs are not enabled", timelineACLsManager
				.CheckAccess(UserGroupInformation.CreateRemoteUser("user"), ApplicationAccessType
				.ModifyApp, entity));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestYarnACLsEnabledForEntity()
		{
			Configuration conf = new YarnConfiguration();
			conf.SetBoolean(YarnConfiguration.YarnAclEnable, true);
			conf.Set(YarnConfiguration.YarnAdminAcl, "admin");
			TimelineACLsManager timelineACLsManager = new TimelineACLsManager(conf);
			timelineACLsManager.SetTimelineStore(new TestTimelineACLsManager.TestTimelineStore
				());
			TimelineEntity entity = new TimelineEntity();
			entity.AddPrimaryFilter(TimelineStore.SystemFilter.EntityOwner.ToString(), "owner"
				);
			entity.SetDomainId("domain_id_1");
			NUnit.Framework.Assert.IsTrue("Owner should be allowed to view", timelineACLsManager
				.CheckAccess(UserGroupInformation.CreateRemoteUser("owner"), ApplicationAccessType
				.ViewApp, entity));
			NUnit.Framework.Assert.IsTrue("Reader should be allowed to view", timelineACLsManager
				.CheckAccess(UserGroupInformation.CreateRemoteUser("reader"), ApplicationAccessType
				.ViewApp, entity));
			NUnit.Framework.Assert.IsFalse("Other shouldn't be allowed to view", timelineACLsManager
				.CheckAccess(UserGroupInformation.CreateRemoteUser("other"), ApplicationAccessType
				.ViewApp, entity));
			NUnit.Framework.Assert.IsTrue("Admin should be allowed to view", timelineACLsManager
				.CheckAccess(UserGroupInformation.CreateRemoteUser("admin"), ApplicationAccessType
				.ViewApp, entity));
			NUnit.Framework.Assert.IsTrue("Owner should be allowed to modify", timelineACLsManager
				.CheckAccess(UserGroupInformation.CreateRemoteUser("owner"), ApplicationAccessType
				.ModifyApp, entity));
			NUnit.Framework.Assert.IsTrue("Writer should be allowed to modify", timelineACLsManager
				.CheckAccess(UserGroupInformation.CreateRemoteUser("writer"), ApplicationAccessType
				.ModifyApp, entity));
			NUnit.Framework.Assert.IsFalse("Other shouldn't be allowed to modify", timelineACLsManager
				.CheckAccess(UserGroupInformation.CreateRemoteUser("other"), ApplicationAccessType
				.ModifyApp, entity));
			NUnit.Framework.Assert.IsTrue("Admin should be allowed to modify", timelineACLsManager
				.CheckAccess(UserGroupInformation.CreateRemoteUser("admin"), ApplicationAccessType
				.ModifyApp, entity));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestCorruptedOwnerInfoForEntity()
		{
			Configuration conf = new YarnConfiguration();
			conf.SetBoolean(YarnConfiguration.YarnAclEnable, true);
			conf.Set(YarnConfiguration.YarnAdminAcl, "owner");
			TimelineACLsManager timelineACLsManager = new TimelineACLsManager(conf);
			timelineACLsManager.SetTimelineStore(new TestTimelineACLsManager.TestTimelineStore
				());
			TimelineEntity entity = new TimelineEntity();
			try
			{
				timelineACLsManager.CheckAccess(UserGroupInformation.CreateRemoteUser("owner"), ApplicationAccessType
					.ViewApp, entity);
				NUnit.Framework.Assert.Fail("Exception is expected");
			}
			catch (YarnException e)
			{
				NUnit.Framework.Assert.IsTrue("It's not the exact expected exception", e.Message.
					Contains("doesn't exist."));
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestYarnACLsNotEnabledForDomain()
		{
			Configuration conf = new YarnConfiguration();
			conf.SetBoolean(YarnConfiguration.YarnAclEnable, false);
			TimelineACLsManager timelineACLsManager = new TimelineACLsManager(conf);
			TimelineDomain domain = new TimelineDomain();
			domain.SetOwner("owner");
			NUnit.Framework.Assert.IsTrue("Always true when ACLs are not enabled", timelineACLsManager
				.CheckAccess(UserGroupInformation.CreateRemoteUser("user"), domain));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestYarnACLsEnabledForDomain()
		{
			Configuration conf = new YarnConfiguration();
			conf.SetBoolean(YarnConfiguration.YarnAclEnable, true);
			conf.Set(YarnConfiguration.YarnAdminAcl, "admin");
			TimelineACLsManager timelineACLsManager = new TimelineACLsManager(conf);
			TimelineDomain domain = new TimelineDomain();
			domain.SetOwner("owner");
			NUnit.Framework.Assert.IsTrue("Owner should be allowed to access", timelineACLsManager
				.CheckAccess(UserGroupInformation.CreateRemoteUser("owner"), domain));
			NUnit.Framework.Assert.IsFalse("Other shouldn't be allowed to access", timelineACLsManager
				.CheckAccess(UserGroupInformation.CreateRemoteUser("other"), domain));
			NUnit.Framework.Assert.IsTrue("Admin should be allowed to access", timelineACLsManager
				.CheckAccess(UserGroupInformation.CreateRemoteUser("admin"), domain));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestCorruptedOwnerInfoForDomain()
		{
			Configuration conf = new YarnConfiguration();
			conf.SetBoolean(YarnConfiguration.YarnAclEnable, true);
			conf.Set(YarnConfiguration.YarnAdminAcl, "owner");
			TimelineACLsManager timelineACLsManager = new TimelineACLsManager(conf);
			TimelineDomain domain = new TimelineDomain();
			try
			{
				timelineACLsManager.CheckAccess(UserGroupInformation.CreateRemoteUser("owner"), domain
					);
				NUnit.Framework.Assert.Fail("Exception is expected");
			}
			catch (YarnException e)
			{
				NUnit.Framework.Assert.IsTrue("It's not the exact expected exception", e.Message.
					Contains("is corrupted."));
			}
		}

		private class TestTimelineStore : MemoryTimelineStore
		{
			/// <exception cref="System.IO.IOException"/>
			public override TimelineDomain GetDomain(string domainId)
			{
				if (domainId == null)
				{
					return null;
				}
				else
				{
					return domain;
				}
			}
		}
	}
}
