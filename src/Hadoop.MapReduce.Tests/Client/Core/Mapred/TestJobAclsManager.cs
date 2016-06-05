using System.Collections.Generic;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Authorize;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	/// <summary>Test the job acls manager</summary>
	public class TestJobAclsManager
	{
		[NUnit.Framework.Test]
		public virtual void TestClusterAdmins()
		{
			IDictionary<JobACL, AccessControlList> tmpJobACLs = new Dictionary<JobACL, AccessControlList
				>();
			Configuration conf = new Configuration();
			string jobOwner = "testuser";
			conf.Set(JobACL.ViewJob.GetAclName(), jobOwner);
			conf.Set(JobACL.ModifyJob.GetAclName(), jobOwner);
			conf.SetBoolean(MRConfig.MrAclsEnabled, true);
			string clusterAdmin = "testuser2";
			conf.Set(MRConfig.MrAdmins, clusterAdmin);
			JobACLsManager aclsManager = new JobACLsManager(conf);
			tmpJobACLs = aclsManager.ConstructJobACLs(conf);
			IDictionary<JobACL, AccessControlList> jobACLs = tmpJobACLs;
			UserGroupInformation callerUGI = UserGroupInformation.CreateUserForTesting(clusterAdmin
				, new string[] {  });
			// cluster admin should have access
			bool val = aclsManager.CheckAccess(callerUGI, JobACL.ViewJob, jobOwner, jobACLs[JobACL
				.ViewJob]);
			NUnit.Framework.Assert.IsTrue("cluster admin should have view access", val);
			val = aclsManager.CheckAccess(callerUGI, JobACL.ModifyJob, jobOwner, jobACLs[JobACL
				.ModifyJob]);
			NUnit.Framework.Assert.IsTrue("cluster admin should have modify access", val);
		}

		[NUnit.Framework.Test]
		public virtual void TestClusterNoAdmins()
		{
			IDictionary<JobACL, AccessControlList> tmpJobACLs = new Dictionary<JobACL, AccessControlList
				>();
			Configuration conf = new Configuration();
			string jobOwner = "testuser";
			conf.Set(JobACL.ViewJob.GetAclName(), string.Empty);
			conf.SetBoolean(MRConfig.MrAclsEnabled, true);
			string noAdminUser = "testuser2";
			JobACLsManager aclsManager = new JobACLsManager(conf);
			tmpJobACLs = aclsManager.ConstructJobACLs(conf);
			IDictionary<JobACL, AccessControlList> jobACLs = tmpJobACLs;
			UserGroupInformation callerUGI = UserGroupInformation.CreateUserForTesting(noAdminUser
				, new string[] {  });
			// random user should not have access
			bool val = aclsManager.CheckAccess(callerUGI, JobACL.ViewJob, jobOwner, jobACLs[JobACL
				.ViewJob]);
			NUnit.Framework.Assert.IsFalse("random user should not have view access", val);
			val = aclsManager.CheckAccess(callerUGI, JobACL.ModifyJob, jobOwner, jobACLs[JobACL
				.ModifyJob]);
			NUnit.Framework.Assert.IsFalse("random user should not have modify access", val);
			callerUGI = UserGroupInformation.CreateUserForTesting(jobOwner, new string[] {  }
				);
			// Owner should have access
			val = aclsManager.CheckAccess(callerUGI, JobACL.ViewJob, jobOwner, jobACLs[JobACL
				.ViewJob]);
			NUnit.Framework.Assert.IsTrue("owner should have view access", val);
			val = aclsManager.CheckAccess(callerUGI, JobACL.ModifyJob, jobOwner, jobACLs[JobACL
				.ModifyJob]);
			NUnit.Framework.Assert.IsTrue("owner should have modify access", val);
		}

		[NUnit.Framework.Test]
		public virtual void TestAclsOff()
		{
			IDictionary<JobACL, AccessControlList> tmpJobACLs = new Dictionary<JobACL, AccessControlList
				>();
			Configuration conf = new Configuration();
			string jobOwner = "testuser";
			conf.Set(JobACL.ViewJob.GetAclName(), jobOwner);
			conf.SetBoolean(MRConfig.MrAclsEnabled, false);
			string noAdminUser = "testuser2";
			JobACLsManager aclsManager = new JobACLsManager(conf);
			tmpJobACLs = aclsManager.ConstructJobACLs(conf);
			IDictionary<JobACL, AccessControlList> jobACLs = tmpJobACLs;
			UserGroupInformation callerUGI = UserGroupInformation.CreateUserForTesting(noAdminUser
				, new string[] {  });
			// acls off so anyone should have access
			bool val = aclsManager.CheckAccess(callerUGI, JobACL.ViewJob, jobOwner, jobACLs[JobACL
				.ViewJob]);
			NUnit.Framework.Assert.IsTrue("acls off so anyone should have access", val);
		}

		[NUnit.Framework.Test]
		public virtual void TestGroups()
		{
			IDictionary<JobACL, AccessControlList> tmpJobACLs = new Dictionary<JobACL, AccessControlList
				>();
			Configuration conf = new Configuration();
			string jobOwner = "testuser";
			conf.Set(JobACL.ViewJob.GetAclName(), jobOwner);
			conf.SetBoolean(MRConfig.MrAclsEnabled, true);
			string user = "testuser2";
			string adminGroup = "adminGroup";
			conf.Set(MRConfig.MrAdmins, " " + adminGroup);
			JobACLsManager aclsManager = new JobACLsManager(conf);
			tmpJobACLs = aclsManager.ConstructJobACLs(conf);
			IDictionary<JobACL, AccessControlList> jobACLs = tmpJobACLs;
			UserGroupInformation callerUGI = UserGroupInformation.CreateUserForTesting(user, 
				new string[] { adminGroup });
			// acls off so anyone should have access
			bool val = aclsManager.CheckAccess(callerUGI, JobACL.ViewJob, jobOwner, jobACLs[JobACL
				.ViewJob]);
			NUnit.Framework.Assert.IsTrue("user in admin group should have access", val);
		}
	}
}
