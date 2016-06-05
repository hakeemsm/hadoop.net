using System.Collections.Generic;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Conf;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Security
{
	public class TestApplicationACLsManager
	{
		private const string AdminUser = "adminuser";

		private const string AppOwner = "appuser";

		private const string Testuser1 = "testuser1";

		private const string Testuser2 = "testuser2";

		private const string Testuser3 = "testuser3";

		[NUnit.Framework.Test]
		public virtual void TestCheckAccess()
		{
			Configuration conf = new Configuration();
			conf.SetBoolean(YarnConfiguration.YarnAclEnable, true);
			conf.Set(YarnConfiguration.YarnAdminAcl, AdminUser);
			ApplicationACLsManager aclManager = new ApplicationACLsManager(conf);
			IDictionary<ApplicationAccessType, string> aclMap = new Dictionary<ApplicationAccessType
				, string>();
			aclMap[ApplicationAccessType.ViewApp] = Testuser1 + "," + Testuser3;
			aclMap[ApplicationAccessType.ModifyApp] = Testuser1;
			ApplicationId appId = ApplicationId.NewInstance(1, 1);
			aclManager.AddApplication(appId, aclMap);
			//User in ACL, should be allowed access
			UserGroupInformation testUser1 = UserGroupInformation.CreateRemoteUser(Testuser1);
			NUnit.Framework.Assert.IsTrue(aclManager.CheckAccess(testUser1, ApplicationAccessType
				.ViewApp, AppOwner, appId));
			NUnit.Framework.Assert.IsTrue(aclManager.CheckAccess(testUser1, ApplicationAccessType
				.ModifyApp, AppOwner, appId));
			//User NOT in ACL, should not be allowed access
			UserGroupInformation testUser2 = UserGroupInformation.CreateRemoteUser(Testuser2);
			NUnit.Framework.Assert.IsFalse(aclManager.CheckAccess(testUser2, ApplicationAccessType
				.ViewApp, AppOwner, appId));
			NUnit.Framework.Assert.IsFalse(aclManager.CheckAccess(testUser2, ApplicationAccessType
				.ModifyApp, AppOwner, appId));
			//User has View access, but not modify access
			UserGroupInformation testUser3 = UserGroupInformation.CreateRemoteUser(Testuser3);
			NUnit.Framework.Assert.IsTrue(aclManager.CheckAccess(testUser3, ApplicationAccessType
				.ViewApp, AppOwner, appId));
			NUnit.Framework.Assert.IsFalse(aclManager.CheckAccess(testUser3, ApplicationAccessType
				.ModifyApp, AppOwner, appId));
			//Application Owner should have all access
			UserGroupInformation appOwner = UserGroupInformation.CreateRemoteUser(AppOwner);
			NUnit.Framework.Assert.IsTrue(aclManager.CheckAccess(appOwner, ApplicationAccessType
				.ViewApp, AppOwner, appId));
			NUnit.Framework.Assert.IsTrue(aclManager.CheckAccess(appOwner, ApplicationAccessType
				.ModifyApp, AppOwner, appId));
			//Admin should have all access
			UserGroupInformation adminUser = UserGroupInformation.CreateRemoteUser(AdminUser);
			NUnit.Framework.Assert.IsTrue(aclManager.CheckAccess(adminUser, ApplicationAccessType
				.ViewApp, AppOwner, appId));
			NUnit.Framework.Assert.IsTrue(aclManager.CheckAccess(adminUser, ApplicationAccessType
				.ModifyApp, AppOwner, appId));
		}

		[NUnit.Framework.Test]
		public virtual void TestCheckAccessWithNullACLS()
		{
			Configuration conf = new Configuration();
			conf.SetBoolean(YarnConfiguration.YarnAclEnable, true);
			conf.Set(YarnConfiguration.YarnAdminAcl, AdminUser);
			ApplicationACLsManager aclManager = new ApplicationACLsManager(conf);
			UserGroupInformation appOwner = UserGroupInformation.CreateRemoteUser(AppOwner);
			ApplicationId appId = ApplicationId.NewInstance(1, 1);
			//Application ACL is not added
			//Application Owner should have all access even if Application ACL is not added
			NUnit.Framework.Assert.IsTrue(aclManager.CheckAccess(appOwner, ApplicationAccessType
				.ModifyApp, AppOwner, appId));
			NUnit.Framework.Assert.IsTrue(aclManager.CheckAccess(appOwner, ApplicationAccessType
				.ViewApp, AppOwner, appId));
			//Admin should have all access
			UserGroupInformation adminUser = UserGroupInformation.CreateRemoteUser(AdminUser);
			NUnit.Framework.Assert.IsTrue(aclManager.CheckAccess(adminUser, ApplicationAccessType
				.ViewApp, AppOwner, appId));
			NUnit.Framework.Assert.IsTrue(aclManager.CheckAccess(adminUser, ApplicationAccessType
				.ModifyApp, AppOwner, appId));
			// A regular user should Not have access
			UserGroupInformation testUser1 = UserGroupInformation.CreateRemoteUser(Testuser1);
			NUnit.Framework.Assert.IsFalse(aclManager.CheckAccess(testUser1, ApplicationAccessType
				.ViewApp, AppOwner, appId));
			NUnit.Framework.Assert.IsFalse(aclManager.CheckAccess(testUser1, ApplicationAccessType
				.ModifyApp, AppOwner, appId));
		}

		[NUnit.Framework.Test]
		public virtual void TestCheckAccessWithPartialACLS()
		{
			Configuration conf = new Configuration();
			conf.SetBoolean(YarnConfiguration.YarnAclEnable, true);
			conf.Set(YarnConfiguration.YarnAdminAcl, AdminUser);
			ApplicationACLsManager aclManager = new ApplicationACLsManager(conf);
			UserGroupInformation appOwner = UserGroupInformation.CreateRemoteUser(AppOwner);
			// Add only the VIEW ACLS
			IDictionary<ApplicationAccessType, string> aclMap = new Dictionary<ApplicationAccessType
				, string>();
			aclMap[ApplicationAccessType.ViewApp] = Testuser1;
			ApplicationId appId = ApplicationId.NewInstance(1, 1);
			aclManager.AddApplication(appId, aclMap);
			//Application Owner should have all access even if Application ACL is not added
			NUnit.Framework.Assert.IsTrue(aclManager.CheckAccess(appOwner, ApplicationAccessType
				.ModifyApp, AppOwner, appId));
			NUnit.Framework.Assert.IsTrue(aclManager.CheckAccess(appOwner, ApplicationAccessType
				.ViewApp, AppOwner, appId));
			//Admin should have all access
			UserGroupInformation adminUser = UserGroupInformation.CreateRemoteUser(AdminUser);
			NUnit.Framework.Assert.IsTrue(aclManager.CheckAccess(adminUser, ApplicationAccessType
				.ViewApp, AppOwner, appId));
			NUnit.Framework.Assert.IsTrue(aclManager.CheckAccess(adminUser, ApplicationAccessType
				.ModifyApp, AppOwner, appId));
			// testuser1 should  have view access only
			UserGroupInformation testUser1 = UserGroupInformation.CreateRemoteUser(Testuser1);
			NUnit.Framework.Assert.IsTrue(aclManager.CheckAccess(testUser1, ApplicationAccessType
				.ViewApp, AppOwner, appId));
			NUnit.Framework.Assert.IsFalse(aclManager.CheckAccess(testUser1, ApplicationAccessType
				.ModifyApp, AppOwner, appId));
			// A testuser2 should Not have access
			UserGroupInformation testUser2 = UserGroupInformation.CreateRemoteUser(Testuser2);
			NUnit.Framework.Assert.IsFalse(aclManager.CheckAccess(testUser2, ApplicationAccessType
				.ViewApp, AppOwner, appId));
			NUnit.Framework.Assert.IsFalse(aclManager.CheckAccess(testUser2, ApplicationAccessType
				.ModifyApp, AppOwner, appId));
		}
	}
}
