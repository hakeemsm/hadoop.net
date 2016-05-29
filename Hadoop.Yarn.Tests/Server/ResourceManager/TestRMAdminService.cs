using System;
using System.Collections.Generic;
using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.HA;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Authorize;
using Org.Apache.Hadoop.Yarn.Api;
using Org.Apache.Hadoop.Yarn.Conf;
using Org.Apache.Hadoop.Yarn.Exceptions;
using Org.Apache.Hadoop.Yarn.Server.Api.Protocolrecords;
using Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Scheduler.Capacity;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager
{
	public class TestRMAdminService
	{
		private Configuration configuration;

		private MockRM rm = null;

		private FileSystem fs;

		private Path workingPath;

		private Path tmpDir;

		static TestRMAdminService()
		{
			YarnConfiguration.AddDefaultResource(YarnConfiguration.CsConfigurationFile);
		}

		/// <exception cref="System.IO.IOException"/>
		[SetUp]
		public virtual void Setup()
		{
			configuration = new YarnConfiguration();
			configuration.Set(YarnConfiguration.RmScheduler, typeof(CapacityScheduler).GetCanonicalName
				());
			fs = FileSystem.Get(configuration);
			workingPath = new Path(new FilePath("target", this.GetType().Name + "-remoteDir")
				.GetAbsolutePath());
			configuration.Set(YarnConfiguration.FsBasedRmConfStore, workingPath.ToString());
			tmpDir = new Path(new FilePath("target", this.GetType().Name + "-tmpDir").GetAbsolutePath
				());
			fs.Delete(workingPath, true);
			fs.Delete(tmpDir, true);
			fs.Mkdirs(workingPath);
			fs.Mkdirs(tmpDir);
			// reset the groups to what it default test settings
			TestRMAdminService.MockUnixGroupsMapping.ResetGroups();
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.TearDown]
		public virtual void TearDown()
		{
			if (rm != null)
			{
				rm.Stop();
			}
			fs.Delete(workingPath, true);
			fs.Delete(tmpDir, true);
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		[NUnit.Framework.Test]
		public virtual void TestAdminRefreshQueuesWithLocalConfigurationProvider()
		{
			rm = new MockRM(configuration);
			rm.Init(configuration);
			rm.Start();
			CapacityScheduler cs = (CapacityScheduler)rm.GetRMContext().GetScheduler();
			int maxAppsBefore = cs.GetConfiguration().GetMaximumSystemApplications();
			try
			{
				rm.adminService.RefreshQueues(RefreshQueuesRequest.NewInstance());
				NUnit.Framework.Assert.AreEqual(maxAppsBefore, cs.GetConfiguration().GetMaximumSystemApplications
					());
			}
			catch (Exception)
			{
				NUnit.Framework.Assert.Fail("Using localConfigurationProvider. Should not get any exception."
					);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		[NUnit.Framework.Test]
		public virtual void TestAdminRefreshQueuesWithFileSystemBasedConfigurationProvider
			()
		{
			configuration.Set(YarnConfiguration.RmConfigurationProviderClass, "org.apache.hadoop.yarn.FileSystemBasedConfigurationProvider"
				);
			//upload default configurations
			UploadDefaultConfiguration();
			try
			{
				rm = new MockRM(configuration);
				rm.Init(configuration);
				rm.Start();
			}
			catch (Exception)
			{
				NUnit.Framework.Assert.Fail("Should not get any exceptions");
			}
			CapacityScheduler cs = (CapacityScheduler)rm.GetRMContext().GetScheduler();
			int maxAppsBefore = cs.GetConfiguration().GetMaximumSystemApplications();
			CapacitySchedulerConfiguration csConf = new CapacitySchedulerConfiguration();
			csConf.Set("yarn.scheduler.capacity.maximum-applications", "5000");
			UploadConfiguration(csConf, "capacity-scheduler.xml");
			rm.adminService.RefreshQueues(RefreshQueuesRequest.NewInstance());
			int maxAppsAfter = cs.GetConfiguration().GetMaximumSystemApplications();
			NUnit.Framework.Assert.AreEqual(maxAppsAfter, 5000);
			NUnit.Framework.Assert.IsTrue(maxAppsAfter != maxAppsBefore);
		}

		[NUnit.Framework.Test]
		public virtual void TestAdminAclsWithLocalConfigurationProvider()
		{
			rm = new MockRM(configuration);
			rm.Init(configuration);
			rm.Start();
			try
			{
				rm.adminService.RefreshAdminAcls(RefreshAdminAclsRequest.NewInstance());
			}
			catch (Exception)
			{
				NUnit.Framework.Assert.Fail("Using localConfigurationProvider. Should not get any exception."
					);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		[NUnit.Framework.Test]
		public virtual void TestAdminAclsWithFileSystemBasedConfigurationProvider()
		{
			configuration.Set(YarnConfiguration.RmConfigurationProviderClass, "org.apache.hadoop.yarn.FileSystemBasedConfigurationProvider"
				);
			//upload default configurations
			UploadDefaultConfiguration();
			try
			{
				rm = new MockRM(configuration);
				rm.Init(configuration);
				rm.Start();
			}
			catch (Exception)
			{
				NUnit.Framework.Assert.Fail("Should not get any exceptions");
			}
			string aclStringBefore = rm.adminService.GetAccessControlList().GetAclString().Trim
				();
			YarnConfiguration yarnConf = new YarnConfiguration();
			yarnConf.Set(YarnConfiguration.YarnAdminAcl, "world:anyone:rwcda");
			UploadConfiguration(yarnConf, "yarn-site.xml");
			rm.adminService.RefreshAdminAcls(RefreshAdminAclsRequest.NewInstance());
			string aclStringAfter = rm.adminService.GetAccessControlList().GetAclString().Trim
				();
			NUnit.Framework.Assert.IsTrue(!aclStringAfter.Equals(aclStringBefore));
			NUnit.Framework.Assert.AreEqual(aclStringAfter, "world:anyone:rwcda," + UserGroupInformation
				.GetCurrentUser().GetShortUserName());
		}

		[NUnit.Framework.Test]
		public virtual void TestServiceAclsRefreshWithLocalConfigurationProvider()
		{
			configuration.SetBoolean(CommonConfigurationKeysPublic.HadoopSecurityAuthorization
				, true);
			ResourceManager resourceManager = null;
			try
			{
				resourceManager = new ResourceManager();
				resourceManager.Init(configuration);
				resourceManager.Start();
				resourceManager.adminService.RefreshServiceAcls(RefreshServiceAclsRequest.NewInstance
					());
			}
			catch (Exception)
			{
				NUnit.Framework.Assert.Fail("Using localConfigurationProvider. Should not get any exception."
					);
			}
			finally
			{
				if (resourceManager != null)
				{
					resourceManager.Stop();
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		[NUnit.Framework.Test]
		public virtual void TestServiceAclsRefreshWithFileSystemBasedConfigurationProvider
			()
		{
			configuration.SetBoolean(CommonConfigurationKeysPublic.HadoopSecurityAuthorization
				, true);
			configuration.Set(YarnConfiguration.RmConfigurationProviderClass, "org.apache.hadoop.yarn.FileSystemBasedConfigurationProvider"
				);
			ResourceManager resourceManager = null;
			try
			{
				//upload default configurations
				UploadDefaultConfiguration();
				Configuration conf = new Configuration();
				conf.SetBoolean(CommonConfigurationKeysPublic.HadoopSecurityAuthorization, true);
				UploadConfiguration(conf, "core-site.xml");
				try
				{
					resourceManager = new ResourceManager();
					resourceManager.Init(configuration);
					resourceManager.Start();
				}
				catch (Exception)
				{
					NUnit.Framework.Assert.Fail("Should not get any exceptions");
				}
				string aclsString = "alice,bob users,wheel";
				Configuration newConf = new Configuration();
				newConf.Set("security.applicationclient.protocol.acl", aclsString);
				UploadConfiguration(newConf, "hadoop-policy.xml");
				resourceManager.adminService.RefreshServiceAcls(RefreshServiceAclsRequest.NewInstance
					());
				// verify service Acls refresh for AdminService
				ServiceAuthorizationManager adminServiceServiceManager = resourceManager.adminService
					.GetServer().GetServiceAuthorizationManager();
				VerifyServiceACLsRefresh(adminServiceServiceManager, typeof(ApplicationClientProtocolPB
					), aclsString);
				// verify service ACLs refresh for ClientRMService
				ServiceAuthorizationManager clientRMServiceServiceManager = resourceManager.GetRMContext
					().GetClientRMService().GetServer().GetServiceAuthorizationManager();
				VerifyServiceACLsRefresh(clientRMServiceServiceManager, typeof(ApplicationClientProtocolPB
					), aclsString);
				// verify service ACLs refresh for ApplicationMasterService
				ServiceAuthorizationManager appMasterService = resourceManager.GetRMContext().GetApplicationMasterService
					().GetServer().GetServiceAuthorizationManager();
				VerifyServiceACLsRefresh(appMasterService, typeof(ApplicationClientProtocolPB), aclsString
					);
				// verify service ACLs refresh for ResourceTrackerService
				ServiceAuthorizationManager RTService = resourceManager.GetRMContext().GetResourceTrackerService
					().GetServer().GetServiceAuthorizationManager();
				VerifyServiceACLsRefresh(RTService, typeof(ApplicationClientProtocolPB), aclsString
					);
			}
			finally
			{
				if (resourceManager != null)
				{
					resourceManager.Stop();
				}
			}
		}

		private void VerifyServiceACLsRefresh(ServiceAuthorizationManager manager, Type protocol
			, string aclString)
		{
			foreach (Type protocolClass in manager.GetProtocolsWithAcls())
			{
				AccessControlList accessList = manager.GetProtocolsAcls(protocolClass);
				if (protocolClass == protocol)
				{
					NUnit.Framework.Assert.AreEqual(accessList.GetAclString(), aclString);
				}
				else
				{
					NUnit.Framework.Assert.AreEqual(accessList.GetAclString(), "*");
				}
			}
		}

		[NUnit.Framework.Test]
		public virtual void TestRefreshSuperUserGroupsWithLocalConfigurationProvider()
		{
			rm = new MockRM(configuration);
			rm.Init(configuration);
			rm.Start();
			try
			{
				rm.adminService.RefreshSuperUserGroupsConfiguration(RefreshSuperUserGroupsConfigurationRequest
					.NewInstance());
			}
			catch (Exception)
			{
				NUnit.Framework.Assert.Fail("Using localConfigurationProvider. Should not get any exception."
					);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		[NUnit.Framework.Test]
		public virtual void TestRefreshSuperUserGroupsWithFileSystemBasedConfigurationProvider
			()
		{
			configuration.Set(YarnConfiguration.RmConfigurationProviderClass, "org.apache.hadoop.yarn.FileSystemBasedConfigurationProvider"
				);
			//upload default configurations
			UploadDefaultConfiguration();
			try
			{
				rm = new MockRM(configuration);
				rm.Init(configuration);
				rm.Start();
			}
			catch (Exception)
			{
				NUnit.Framework.Assert.Fail("Should not get any exceptions");
			}
			Configuration coreConf = new Configuration(false);
			coreConf.Set("hadoop.proxyuser.test.groups", "test_groups");
			coreConf.Set("hadoop.proxyuser.test.hosts", "test_hosts");
			UploadConfiguration(coreConf, "core-site.xml");
			rm.adminService.RefreshSuperUserGroupsConfiguration(RefreshSuperUserGroupsConfigurationRequest
				.NewInstance());
			NUnit.Framework.Assert.IsTrue(ProxyUsers.GetDefaultImpersonationProvider().GetProxyGroups
				()["hadoop.proxyuser.test.groups"].Count == 1);
			NUnit.Framework.Assert.IsTrue(ProxyUsers.GetDefaultImpersonationProvider().GetProxyGroups
				()["hadoop.proxyuser.test.groups"].Contains("test_groups"));
			NUnit.Framework.Assert.IsTrue(ProxyUsers.GetDefaultImpersonationProvider().GetProxyHosts
				()["hadoop.proxyuser.test.hosts"].Count == 1);
			NUnit.Framework.Assert.IsTrue(ProxyUsers.GetDefaultImpersonationProvider().GetProxyHosts
				()["hadoop.proxyuser.test.hosts"].Contains("test_hosts"));
			Configuration yarnConf = new Configuration(false);
			yarnConf.Set("yarn.resourcemanager.proxyuser.test.groups", "test_groups_1");
			yarnConf.Set("yarn.resourcemanager.proxyuser.test.hosts", "test_hosts_1");
			UploadConfiguration(yarnConf, "yarn-site.xml");
			// RM specific configs will overwrite the common ones
			rm.adminService.RefreshSuperUserGroupsConfiguration(RefreshSuperUserGroupsConfigurationRequest
				.NewInstance());
			NUnit.Framework.Assert.IsTrue(ProxyUsers.GetDefaultImpersonationProvider().GetProxyGroups
				()["hadoop.proxyuser.test.groups"].Count == 1);
			NUnit.Framework.Assert.IsTrue(ProxyUsers.GetDefaultImpersonationProvider().GetProxyGroups
				()["hadoop.proxyuser.test.groups"].Contains("test_groups_1"));
			NUnit.Framework.Assert.IsTrue(ProxyUsers.GetDefaultImpersonationProvider().GetProxyHosts
				()["hadoop.proxyuser.test.hosts"].Count == 1);
			NUnit.Framework.Assert.IsTrue(ProxyUsers.GetDefaultImpersonationProvider().GetProxyHosts
				()["hadoop.proxyuser.test.hosts"].Contains("test_hosts_1"));
		}

		[NUnit.Framework.Test]
		public virtual void TestRefreshUserToGroupsMappingsWithLocalConfigurationProvider
			()
		{
			rm = new MockRM(configuration);
			rm.Init(configuration);
			rm.Start();
			try
			{
				rm.adminService.RefreshUserToGroupsMappings(RefreshUserToGroupsMappingsRequest.NewInstance
					());
			}
			catch (Exception)
			{
				NUnit.Framework.Assert.Fail("Using localConfigurationProvider. Should not get any exception."
					);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		[NUnit.Framework.Test]
		public virtual void TestRefreshUserToGroupsMappingsWithFileSystemBasedConfigurationProvider
			()
		{
			configuration.Set(YarnConfiguration.RmConfigurationProviderClass, "org.apache.hadoop.yarn.FileSystemBasedConfigurationProvider"
				);
			string[] defaultTestUserGroups = new string[] { "dummy_group1", "dummy_group2" };
			UserGroupInformation ugi = UserGroupInformation.CreateUserForTesting("dummyUser", 
				defaultTestUserGroups);
			string user = ugi.GetUserName();
			IList<string> groupWithInit = new AList<string>(2);
			for (int i = 0; i < ugi.GetGroupNames().Length; i++)
			{
				groupWithInit.AddItem(ugi.GetGroupNames()[i]);
			}
			// upload default configurations
			UploadDefaultConfiguration();
			Configuration conf = new Configuration();
			conf.SetClass(CommonConfigurationKeys.HadoopSecurityGroupMapping, typeof(TestRMAdminService.MockUnixGroupsMapping
				), typeof(GroupMappingServiceProvider));
			UploadConfiguration(conf, "core-site.xml");
			try
			{
				rm = new MockRM(configuration);
				rm.Init(configuration);
				rm.Start();
			}
			catch (Exception)
			{
				NUnit.Framework.Assert.Fail("Should not get any exceptions");
			}
			// Make sure RM will use the updated GroupMappingServiceProvider
			IList<string> groupBefore = new AList<string>(Groups.GetUserToGroupsMappingService
				(configuration).GetGroups(user));
			NUnit.Framework.Assert.IsTrue(groupBefore.Contains("test_group_A") && groupBefore
				.Contains("test_group_B") && groupBefore.Contains("test_group_C") && groupBefore
				.Count == 3);
			NUnit.Framework.Assert.IsTrue(groupWithInit.Count != groupBefore.Count);
			NUnit.Framework.Assert.IsFalse(groupWithInit.Contains("test_group_A") || groupWithInit
				.Contains("test_group_B") || groupWithInit.Contains("test_group_C"));
			// update the groups
			TestRMAdminService.MockUnixGroupsMapping.UpdateGroups();
			rm.adminService.RefreshUserToGroupsMappings(RefreshUserToGroupsMappingsRequest.NewInstance
				());
			IList<string> groupAfter = Groups.GetUserToGroupsMappingService(configuration).GetGroups
				(user);
			// should get the updated groups
			NUnit.Framework.Assert.IsTrue(groupAfter.Contains("test_group_D") && groupAfter.Contains
				("test_group_E") && groupAfter.Contains("test_group_F") && groupAfter.Count == 3
				);
		}

		[NUnit.Framework.Test]
		public virtual void TestRefreshNodesWithLocalConfigurationProvider()
		{
			rm = new MockRM(configuration);
			rm.Init(configuration);
			rm.Start();
			try
			{
				rm.adminService.RefreshNodes(RefreshNodesRequest.NewInstance());
			}
			catch (Exception)
			{
				NUnit.Framework.Assert.Fail("Using localConfigurationProvider. Should not get any exception."
					);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		[NUnit.Framework.Test]
		public virtual void TestRefreshNodesWithFileSystemBasedConfigurationProvider()
		{
			configuration.Set(YarnConfiguration.RmConfigurationProviderClass, "org.apache.hadoop.yarn.FileSystemBasedConfigurationProvider"
				);
			// upload default configurations
			UploadDefaultConfiguration();
			try
			{
				rm = new MockRM(configuration);
				rm.Init(configuration);
				rm.Start();
			}
			catch (Exception)
			{
				NUnit.Framework.Assert.Fail("Should not get any exceptions");
			}
			FilePath excludeHostsFile = new FilePath(tmpDir.ToString(), "excludeHosts");
			if (excludeHostsFile.Exists())
			{
				excludeHostsFile.Delete();
			}
			if (!excludeHostsFile.CreateNewFile())
			{
				NUnit.Framework.Assert.Fail("Can not create " + "excludeHosts");
			}
			PrintWriter fileWriter = new PrintWriter(excludeHostsFile);
			fileWriter.Write("0.0.0.0:123");
			fileWriter.Close();
			UploadToRemoteFileSystem(new Path(excludeHostsFile.GetAbsolutePath()));
			Configuration yarnConf = new YarnConfiguration();
			yarnConf.Set(YarnConfiguration.RmNodesExcludeFilePath, this.workingPath + "/excludeHosts"
				);
			UploadConfiguration(yarnConf, YarnConfiguration.YarnSiteConfigurationFile);
			rm.adminService.RefreshNodes(RefreshNodesRequest.NewInstance());
			ICollection<string> excludeHosts = rm.GetNodesListManager().GetHostsReader().GetExcludedHosts
				();
			NUnit.Framework.Assert.IsTrue(excludeHosts.Count == 1);
			NUnit.Framework.Assert.IsTrue(excludeHosts.Contains("0.0.0.0:123"));
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.Yarn.Exceptions.YarnException"/>
		[NUnit.Framework.Test]
		public virtual void TestRMHAWithFileSystemBasedConfiguration()
		{
			HAServiceProtocol.StateChangeRequestInfo requestInfo = new HAServiceProtocol.StateChangeRequestInfo
				(HAServiceProtocol.RequestSource.RequestByUser);
			configuration.Set(YarnConfiguration.RmConfigurationProviderClass, "org.apache.hadoop.yarn.FileSystemBasedConfigurationProvider"
				);
			configuration.SetBoolean(YarnConfiguration.RmHaEnabled, true);
			configuration.SetBoolean(YarnConfiguration.AutoFailoverEnabled, false);
			configuration.Set(YarnConfiguration.RmHaIds, "rm1,rm2");
			int @base = 100;
			foreach (string confKey in YarnConfiguration.GetServiceAddressConfKeys(configuration
				))
			{
				configuration.Set(HAUtil.AddSuffix(confKey, "rm1"), "0.0.0.0:" + (@base + 20));
				configuration.Set(HAUtil.AddSuffix(confKey, "rm2"), "0.0.0.0:" + (@base + 40));
				@base = @base * 2;
			}
			Configuration conf1 = new Configuration(configuration);
			conf1.Set(YarnConfiguration.RmHaId, "rm1");
			Configuration conf2 = new Configuration(configuration);
			conf2.Set(YarnConfiguration.RmHaId, "rm2");
			// upload default configurations
			UploadDefaultConfiguration();
			MockRM rm1 = null;
			MockRM rm2 = null;
			try
			{
				rm1 = new MockRM(conf1);
				rm1.Init(conf1);
				rm1.Start();
				NUnit.Framework.Assert.IsTrue(rm1.GetRMContext().GetHAServiceState() == HAServiceProtocol.HAServiceState
					.Standby);
				rm2 = new MockRM(conf2);
				rm2.Init(conf1);
				rm2.Start();
				NUnit.Framework.Assert.IsTrue(rm2.GetRMContext().GetHAServiceState() == HAServiceProtocol.HAServiceState
					.Standby);
				rm1.adminService.TransitionToActive(requestInfo);
				NUnit.Framework.Assert.IsTrue(rm1.GetRMContext().GetHAServiceState() == HAServiceProtocol.HAServiceState
					.Active);
				CapacitySchedulerConfiguration csConf = new CapacitySchedulerConfiguration();
				csConf.Set("yarn.scheduler.capacity.maximum-applications", "5000");
				UploadConfiguration(csConf, "capacity-scheduler.xml");
				rm1.adminService.RefreshQueues(RefreshQueuesRequest.NewInstance());
				int maxApps = ((CapacityScheduler)rm1.GetRMContext().GetScheduler()).GetConfiguration
					().GetMaximumSystemApplications();
				NUnit.Framework.Assert.AreEqual(maxApps, 5000);
				// Before failover happens, the maxApps is
				// still the default value on the standby rm : rm2
				int maxAppsBeforeFailOver = ((CapacityScheduler)rm2.GetRMContext().GetScheduler()
					).GetConfiguration().GetMaximumSystemApplications();
				NUnit.Framework.Assert.AreEqual(maxAppsBeforeFailOver, 10000);
				// Do the failover
				rm1.adminService.TransitionToStandby(requestInfo);
				rm2.adminService.TransitionToActive(requestInfo);
				NUnit.Framework.Assert.IsTrue(rm1.GetRMContext().GetHAServiceState() == HAServiceProtocol.HAServiceState
					.Standby);
				NUnit.Framework.Assert.IsTrue(rm2.GetRMContext().GetHAServiceState() == HAServiceProtocol.HAServiceState
					.Active);
				int maxAppsAfter = ((CapacityScheduler)rm2.GetRMContext().GetScheduler()).GetConfiguration
					().GetMaximumSystemApplications();
				NUnit.Framework.Assert.AreEqual(maxAppsAfter, 5000);
			}
			finally
			{
				if (rm1 != null)
				{
					rm1.Stop();
				}
				if (rm2 != null)
				{
					rm2.Stop();
				}
			}
		}

		[NUnit.Framework.Test]
		public virtual void TestRMStartsWithoutConfigurationFilesProvided()
		{
			// enable FileSystemBasedConfigurationProvider without uploading
			// any configuration files into Remote File System.
			configuration.Set(YarnConfiguration.RmConfigurationProviderClass, "org.apache.hadoop.yarn.FileSystemBasedConfigurationProvider"
				);
			// The configurationProvider will return NULL instead of
			// throwing out Exceptions, if there are no configuration files provided.
			// RM will not load the remote Configuration files,
			// and should start successfully.
			try
			{
				rm = new MockRM(configuration);
				rm.Init(configuration);
				rm.Start();
			}
			catch (Exception)
			{
				NUnit.Framework.Assert.Fail("Should not get any exceptions");
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRMInitialsWithFileSystemBasedConfigurationProvider()
		{
			configuration.Set(YarnConfiguration.RmConfigurationProviderClass, "org.apache.hadoop.yarn.FileSystemBasedConfigurationProvider"
				);
			// upload configurations
			FilePath excludeHostsFile = new FilePath(tmpDir.ToString(), "excludeHosts");
			if (excludeHostsFile.Exists())
			{
				excludeHostsFile.Delete();
			}
			if (!excludeHostsFile.CreateNewFile())
			{
				NUnit.Framework.Assert.Fail("Can not create " + "excludeHosts");
			}
			PrintWriter fileWriter = new PrintWriter(excludeHostsFile);
			fileWriter.Write("0.0.0.0:123");
			fileWriter.Close();
			UploadToRemoteFileSystem(new Path(excludeHostsFile.GetAbsolutePath()));
			YarnConfiguration yarnConf = new YarnConfiguration();
			yarnConf.Set(YarnConfiguration.YarnAdminAcl, "world:anyone:rwcda");
			yarnConf.Set(YarnConfiguration.RmNodesExcludeFilePath, this.workingPath + "/excludeHosts"
				);
			UploadConfiguration(yarnConf, "yarn-site.xml");
			CapacitySchedulerConfiguration csConf = new CapacitySchedulerConfiguration();
			csConf.Set("yarn.scheduler.capacity.maximum-applications", "5000");
			UploadConfiguration(csConf, "capacity-scheduler.xml");
			string aclsString = "alice,bob users,wheel";
			Configuration newConf = new Configuration();
			newConf.Set("security.applicationclient.protocol.acl", aclsString);
			UploadConfiguration(newConf, "hadoop-policy.xml");
			Configuration conf = new Configuration();
			conf.SetBoolean(CommonConfigurationKeysPublic.HadoopSecurityAuthorization, true);
			conf.Set("hadoop.proxyuser.test.groups", "test_groups");
			conf.Set("hadoop.proxyuser.test.hosts", "test_hosts");
			conf.SetClass(CommonConfigurationKeys.HadoopSecurityGroupMapping, typeof(TestRMAdminService.MockUnixGroupsMapping
				), typeof(GroupMappingServiceProvider));
			UploadConfiguration(conf, "core-site.xml");
			// update the groups
			TestRMAdminService.MockUnixGroupsMapping.UpdateGroups();
			ResourceManager resourceManager = null;
			try
			{
				try
				{
					resourceManager = new ResourceManager();
					resourceManager.Init(configuration);
					resourceManager.Start();
				}
				catch (Exception)
				{
					NUnit.Framework.Assert.Fail("Should not get any exceptions");
				}
				// validate values for excludeHosts
				ICollection<string> excludeHosts = resourceManager.GetRMContext().GetNodesListManager
					().GetHostsReader().GetExcludedHosts();
				NUnit.Framework.Assert.IsTrue(excludeHosts.Count == 1);
				NUnit.Framework.Assert.IsTrue(excludeHosts.Contains("0.0.0.0:123"));
				// validate values for admin-acls
				string aclStringAfter = resourceManager.adminService.GetAccessControlList().GetAclString
					().Trim();
				NUnit.Framework.Assert.AreEqual(aclStringAfter, "world:anyone:rwcda," + UserGroupInformation
					.GetCurrentUser().GetShortUserName());
				// validate values for queue configuration
				CapacityScheduler cs = (CapacityScheduler)resourceManager.GetRMContext().GetScheduler
					();
				int maxAppsAfter = cs.GetConfiguration().GetMaximumSystemApplications();
				NUnit.Framework.Assert.AreEqual(maxAppsAfter, 5000);
				// verify service Acls for AdminService
				ServiceAuthorizationManager adminServiceServiceManager = resourceManager.adminService
					.GetServer().GetServiceAuthorizationManager();
				VerifyServiceACLsRefresh(adminServiceServiceManager, typeof(ApplicationClientProtocolPB
					), aclsString);
				// verify service ACLs for ClientRMService
				ServiceAuthorizationManager clientRMServiceServiceManager = resourceManager.GetRMContext
					().GetClientRMService().GetServer().GetServiceAuthorizationManager();
				VerifyServiceACLsRefresh(clientRMServiceServiceManager, typeof(ApplicationClientProtocolPB
					), aclsString);
				// verify service ACLs for ApplicationMasterService
				ServiceAuthorizationManager appMasterService = resourceManager.GetRMContext().GetApplicationMasterService
					().GetServer().GetServiceAuthorizationManager();
				VerifyServiceACLsRefresh(appMasterService, typeof(ApplicationClientProtocolPB), aclsString
					);
				// verify service ACLs for ResourceTrackerService
				ServiceAuthorizationManager RTService = resourceManager.GetRMContext().GetResourceTrackerService
					().GetServer().GetServiceAuthorizationManager();
				VerifyServiceACLsRefresh(RTService, typeof(ApplicationClientProtocolPB), aclsString
					);
				// verify ProxyUsers and ProxyHosts
				ProxyUsers.RefreshSuperUserGroupsConfiguration(configuration);
				NUnit.Framework.Assert.IsTrue(ProxyUsers.GetDefaultImpersonationProvider().GetProxyGroups
					()["hadoop.proxyuser.test.groups"].Count == 1);
				NUnit.Framework.Assert.IsTrue(ProxyUsers.GetDefaultImpersonationProvider().GetProxyGroups
					()["hadoop.proxyuser.test.groups"].Contains("test_groups"));
				NUnit.Framework.Assert.IsTrue(ProxyUsers.GetDefaultImpersonationProvider().GetProxyHosts
					()["hadoop.proxyuser.test.hosts"].Count == 1);
				NUnit.Framework.Assert.IsTrue(ProxyUsers.GetDefaultImpersonationProvider().GetProxyHosts
					()["hadoop.proxyuser.test.hosts"].Contains("test_hosts"));
				// verify UserToGroupsMappings
				IList<string> groupAfter = Groups.GetUserToGroupsMappingService(configuration).GetGroups
					(UserGroupInformation.GetCurrentUser().GetUserName());
				NUnit.Framework.Assert.IsTrue(groupAfter.Contains("test_group_D") && groupAfter.Contains
					("test_group_E") && groupAfter.Contains("test_group_F") && groupAfter.Count == 3
					);
			}
			finally
			{
				if (resourceManager != null)
				{
					resourceManager.Stop();
				}
			}
		}

		/* For verifying fix for YARN-3804 */
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRefreshAclWithDaemonUser()
		{
			string daemonUser = UserGroupInformation.GetCurrentUser().GetShortUserName();
			configuration.Set(YarnConfiguration.RmConfigurationProviderClass, "org.apache.hadoop.yarn.FileSystemBasedConfigurationProvider"
				);
			UploadDefaultConfiguration();
			YarnConfiguration yarnConf = new YarnConfiguration();
			yarnConf.Set(YarnConfiguration.YarnAdminAcl, daemonUser + "xyz");
			UploadConfiguration(yarnConf, "yarn-site.xml");
			try
			{
				rm = new MockRM(configuration);
				rm.Init(configuration);
				rm.Start();
			}
			catch (Exception)
			{
				NUnit.Framework.Assert.Fail("Should not get any exceptions");
			}
			NUnit.Framework.Assert.AreEqual(daemonUser + "xyz," + daemonUser, rm.adminService
				.GetAccessControlList().GetAclString().Trim());
			yarnConf = new YarnConfiguration();
			yarnConf.Set(YarnConfiguration.YarnAdminAcl, daemonUser + "abc");
			UploadConfiguration(yarnConf, "yarn-site.xml");
			try
			{
				rm.adminService.RefreshAdminAcls(RefreshAdminAclsRequest.NewInstance());
			}
			catch (YarnException e)
			{
				if (e.InnerException != null && e.InnerException is AccessControlException)
				{
					NUnit.Framework.Assert.Fail("Refresh should not have failed due to incorrect ACL"
						);
				}
				throw;
			}
			NUnit.Framework.Assert.AreEqual(daemonUser + "abc," + daemonUser, rm.adminService
				.GetAccessControlList().GetAclString().Trim());
		}

		/// <exception cref="System.IO.IOException"/>
		private string WriteConfigurationXML(Configuration conf, string confXMLName)
		{
			DataOutputStream output = null;
			try
			{
				FilePath confFile = new FilePath(tmpDir.ToString(), confXMLName);
				if (confFile.Exists())
				{
					confFile.Delete();
				}
				if (!confFile.CreateNewFile())
				{
					NUnit.Framework.Assert.Fail("Can not create " + confXMLName);
				}
				output = new DataOutputStream(new FileOutputStream(confFile));
				conf.WriteXml(output);
				return confFile.GetAbsolutePath();
			}
			finally
			{
				if (output != null)
				{
					output.Close();
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void UploadToRemoteFileSystem(Path filePath)
		{
			fs.CopyFromLocalFile(filePath, workingPath);
		}

		/// <exception cref="System.IO.IOException"/>
		private void UploadConfiguration(Configuration conf, string confFileName)
		{
			string csConfFile = WriteConfigurationXML(conf, confFileName);
			// upload the file into Remote File System
			UploadToRemoteFileSystem(new Path(csConfFile));
		}

		/// <exception cref="System.IO.IOException"/>
		private void UploadDefaultConfiguration()
		{
			Configuration conf = new Configuration();
			UploadConfiguration(conf, "core-site.xml");
			YarnConfiguration yarnConf = new YarnConfiguration();
			yarnConf.Set(YarnConfiguration.RmConfigurationProviderClass, "org.apache.hadoop.yarn.FileSystemBasedConfigurationProvider"
				);
			UploadConfiguration(yarnConf, "yarn-site.xml");
			CapacitySchedulerConfiguration csConf = new CapacitySchedulerConfiguration();
			UploadConfiguration(csConf, "capacity-scheduler.xml");
			Configuration hadoopPolicyConf = new Configuration(false);
			hadoopPolicyConf.AddResource(YarnConfiguration.HadoopPolicyConfigurationFile);
			UploadConfiguration(hadoopPolicyConf, "hadoop-policy.xml");
		}

		private class MockUnixGroupsMapping : GroupMappingServiceProvider
		{
			private static IList<string> group = new AList<string>();

			/// <exception cref="System.IO.IOException"/>
			public override IList<string> GetGroups(string user)
			{
				return group;
			}

			/// <exception cref="System.IO.IOException"/>
			public override void CacheGroupsRefresh()
			{
			}

			// Do nothing
			/// <exception cref="System.IO.IOException"/>
			public override void CacheGroupsAdd(IList<string> groups)
			{
			}

			// Do nothing
			public static void UpdateGroups()
			{
				group.Clear();
				group.AddItem("test_group_D");
				group.AddItem("test_group_E");
				group.AddItem("test_group_F");
			}

			public static void ResetGroups()
			{
				group.Clear();
				group.AddItem("test_group_A");
				group.AddItem("test_group_B");
				group.AddItem("test_group_C");
			}
		}
	}
}
