using System.Net;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Ipc;
using Org.Apache.Hadoop.Security;
using Sharpen;

namespace Org.Apache.Hadoop.Security.Authorize
{
	public class TestServiceAuthorization
	{
		private const string AclConfig = "test.protocol.acl";

		private const string AclConfig1 = "test.protocol1.acl";

		private const string Address = "0.0.0.0";

		private const string HostConfig = "test.protocol.hosts";

		private const string BlockedHostConfig = "test.protocol.hosts.blocked";

		private const string AuthorizedIp = "1.2.3.4";

		private const string UnauthorizedIp = "1.2.3.5";

		private const string IpRange = "10.222.0.0/16,10.113.221.221";

		public interface TestProtocol1 : TestRPC.TestProtocol
		{
		}

		private class TestPolicyProvider : PolicyProvider
		{
			public override Service[] GetServices()
			{
				return new Service[] { new Service(AclConfig, typeof(TestRPC.TestProtocol)), new 
					Service(AclConfig1, typeof(TestServiceAuthorization.TestProtocol1)) };
			}
		}

		[Fact]
		public virtual void TestDefaultAcl()
		{
			ServiceAuthorizationManager serviceAuthorizationManager = new ServiceAuthorizationManager
				();
			Configuration conf = new Configuration();
			// test without setting a default acl
			conf.Set(AclConfig, "user1 group1");
			serviceAuthorizationManager.Refresh(conf, new TestServiceAuthorization.TestPolicyProvider
				());
			AccessControlList acl = serviceAuthorizationManager.GetProtocolsAcls(typeof(TestRPC.TestProtocol
				));
			Assert.Equal("user1 group1", acl.GetAclString());
			acl = serviceAuthorizationManager.GetProtocolsAcls(typeof(TestServiceAuthorization.TestProtocol1
				));
			Assert.Equal(AccessControlList.WildcardAclValue, acl.GetAclString
				());
			// test with a default acl
			conf.Set(CommonConfigurationKeys.HadoopSecurityServiceAuthorizationDefaultAcl, "user2 group2"
				);
			serviceAuthorizationManager.Refresh(conf, new TestServiceAuthorization.TestPolicyProvider
				());
			acl = serviceAuthorizationManager.GetProtocolsAcls(typeof(TestRPC.TestProtocol));
			Assert.Equal("user1 group1", acl.GetAclString());
			acl = serviceAuthorizationManager.GetProtocolsAcls(typeof(TestServiceAuthorization.TestProtocol1
				));
			Assert.Equal("user2 group2", acl.GetAclString());
		}

		/// <exception cref="Sharpen.UnknownHostException"/>
		[Fact]
		public virtual void TestBlockedAcl()
		{
			UserGroupInformation drwho = UserGroupInformation.CreateUserForTesting("drwho@EXAMPLE.COM"
				, new string[] { "group1", "group2" });
			ServiceAuthorizationManager serviceAuthorizationManager = new ServiceAuthorizationManager
				();
			Configuration conf = new Configuration();
			// test without setting a blocked acl
			conf.Set(AclConfig, "user1 group1");
			serviceAuthorizationManager.Refresh(conf, new TestServiceAuthorization.TestPolicyProvider
				());
			try
			{
				serviceAuthorizationManager.Authorize(drwho, typeof(TestRPC.TestProtocol), conf, 
					Sharpen.Extensions.GetAddressByName(Address));
			}
			catch (AuthorizationException)
			{
				NUnit.Framework.Assert.Fail();
			}
			// now set a blocked acl with another user and another group
			conf.Set(AclConfig + ServiceAuthorizationManager.Blocked, "drwho2 group3");
			serviceAuthorizationManager.Refresh(conf, new TestServiceAuthorization.TestPolicyProvider
				());
			try
			{
				serviceAuthorizationManager.Authorize(drwho, typeof(TestRPC.TestProtocol), conf, 
					Sharpen.Extensions.GetAddressByName(Address));
			}
			catch (AuthorizationException)
			{
				NUnit.Framework.Assert.Fail();
			}
			// now set a blocked acl with the user and another group
			conf.Set(AclConfig + ServiceAuthorizationManager.Blocked, "drwho group3");
			serviceAuthorizationManager.Refresh(conf, new TestServiceAuthorization.TestPolicyProvider
				());
			try
			{
				serviceAuthorizationManager.Authorize(drwho, typeof(TestRPC.TestProtocol), conf, 
					Sharpen.Extensions.GetAddressByName(Address));
				NUnit.Framework.Assert.Fail();
			}
			catch (AuthorizationException)
			{
			}
			// now set a blocked acl with another user and another group
			conf.Set(AclConfig + ServiceAuthorizationManager.Blocked, "drwho2 group3");
			serviceAuthorizationManager.Refresh(conf, new TestServiceAuthorization.TestPolicyProvider
				());
			try
			{
				serviceAuthorizationManager.Authorize(drwho, typeof(TestRPC.TestProtocol), conf, 
					Sharpen.Extensions.GetAddressByName(Address));
			}
			catch (AuthorizationException)
			{
				NUnit.Framework.Assert.Fail();
			}
			// now set a blocked acl with another user and group that the user belongs to
			conf.Set(AclConfig + ServiceAuthorizationManager.Blocked, "drwho2 group2");
			serviceAuthorizationManager.Refresh(conf, new TestServiceAuthorization.TestPolicyProvider
				());
			try
			{
				serviceAuthorizationManager.Authorize(drwho, typeof(TestRPC.TestProtocol), conf, 
					Sharpen.Extensions.GetAddressByName(Address));
				NUnit.Framework.Assert.Fail();
			}
			catch (AuthorizationException)
			{
			}
			// expects Exception
			// reset blocked acl so that there is no blocked ACL
			conf.Set(AclConfig + ServiceAuthorizationManager.Blocked, string.Empty);
			serviceAuthorizationManager.Refresh(conf, new TestServiceAuthorization.TestPolicyProvider
				());
			try
			{
				serviceAuthorizationManager.Authorize(drwho, typeof(TestRPC.TestProtocol), conf, 
					Sharpen.Extensions.GetAddressByName(Address));
			}
			catch (AuthorizationException)
			{
				NUnit.Framework.Assert.Fail();
			}
		}

		/// <exception cref="Sharpen.UnknownHostException"/>
		[Fact]
		public virtual void TestDefaultBlockedAcl()
		{
			UserGroupInformation drwho = UserGroupInformation.CreateUserForTesting("drwho@EXAMPLE.COM"
				, new string[] { "group1", "group2" });
			ServiceAuthorizationManager serviceAuthorizationManager = new ServiceAuthorizationManager
				();
			Configuration conf = new Configuration();
			// test without setting a default blocked acl
			serviceAuthorizationManager.Refresh(conf, new TestServiceAuthorization.TestPolicyProvider
				());
			try
			{
				serviceAuthorizationManager.Authorize(drwho, typeof(TestServiceAuthorization.TestProtocol1
					), conf, Sharpen.Extensions.GetAddressByName(Address));
			}
			catch (AuthorizationException)
			{
				NUnit.Framework.Assert.Fail();
			}
			// set a restrictive default blocked acl and an non-restricting blocked acl for TestProtocol
			conf.Set(CommonConfigurationKeys.HadoopSecurityServiceAuthorizationDefaultBlockedAcl
				, "user2 group2");
			conf.Set(AclConfig + ServiceAuthorizationManager.Blocked, "user2");
			serviceAuthorizationManager.Refresh(conf, new TestServiceAuthorization.TestPolicyProvider
				());
			// drwho is authorized to access TestProtocol
			try
			{
				serviceAuthorizationManager.Authorize(drwho, typeof(TestRPC.TestProtocol), conf, 
					Sharpen.Extensions.GetAddressByName(Address));
			}
			catch (AuthorizationException)
			{
				NUnit.Framework.Assert.Fail();
			}
			// drwho is not authorized to access TestProtocol1 because it uses the default blocked acl.
			try
			{
				serviceAuthorizationManager.Authorize(drwho, typeof(TestServiceAuthorization.TestProtocol1
					), conf, Sharpen.Extensions.GetAddressByName(Address));
				NUnit.Framework.Assert.Fail();
			}
			catch (AuthorizationException)
			{
			}
		}

		// expects Exception
		/// <exception cref="Sharpen.UnknownHostException"/>
		[Fact]
		public virtual void TestMachineList()
		{
			UserGroupInformation drwho = UserGroupInformation.CreateUserForTesting("drwho@EXAMPLE.COM"
				, new string[] { "group1", "group2" });
			ServiceAuthorizationManager serviceAuthorizationManager = new ServiceAuthorizationManager
				();
			Configuration conf = new Configuration();
			conf.Set(HostConfig, "1.2.3.4");
			serviceAuthorizationManager.Refresh(conf, new TestServiceAuthorization.TestPolicyProvider
				());
			try
			{
				serviceAuthorizationManager.Authorize(drwho, typeof(TestRPC.TestProtocol), conf, 
					Sharpen.Extensions.GetAddressByName(AuthorizedIp));
			}
			catch (AuthorizationException)
			{
				NUnit.Framework.Assert.Fail();
			}
			try
			{
				serviceAuthorizationManager.Authorize(drwho, typeof(TestRPC.TestProtocol), conf, 
					Sharpen.Extensions.GetAddressByName(UnauthorizedIp));
				NUnit.Framework.Assert.Fail();
			}
			catch (AuthorizationException)
			{
			}
		}

		// expects Exception
		/// <exception cref="Sharpen.UnknownHostException"/>
		[Fact]
		public virtual void TestDefaultMachineList()
		{
			UserGroupInformation drwho = UserGroupInformation.CreateUserForTesting("drwho@EXAMPLE.COM"
				, new string[] { "group1", "group2" });
			ServiceAuthorizationManager serviceAuthorizationManager = new ServiceAuthorizationManager
				();
			Configuration conf = new Configuration();
			// test without setting a default MachineList
			serviceAuthorizationManager.Refresh(conf, new TestServiceAuthorization.TestPolicyProvider
				());
			try
			{
				serviceAuthorizationManager.Authorize(drwho, typeof(TestRPC.TestProtocol), conf, 
					Sharpen.Extensions.GetAddressByName(UnauthorizedIp));
			}
			catch (AuthorizationException)
			{
				NUnit.Framework.Assert.Fail();
			}
			// test with a default MachineList
			conf.Set("security.service.authorization.default.hosts", IpRange);
			serviceAuthorizationManager.Refresh(conf, new TestServiceAuthorization.TestPolicyProvider
				());
			try
			{
				serviceAuthorizationManager.Authorize(drwho, typeof(TestRPC.TestProtocol), conf, 
					Sharpen.Extensions.GetAddressByName(UnauthorizedIp));
				NUnit.Framework.Assert.Fail();
			}
			catch (AuthorizationException)
			{
			}
			// expects Exception
			try
			{
				serviceAuthorizationManager.Authorize(drwho, typeof(TestRPC.TestProtocol), conf, 
					Sharpen.Extensions.GetAddressByName("10.222.0.0"));
			}
			catch (AuthorizationException)
			{
				NUnit.Framework.Assert.Fail();
			}
		}

		/// <exception cref="Sharpen.UnknownHostException"/>
		[Fact]
		public virtual void TestBlockedMachineList()
		{
			UserGroupInformation drwho = UserGroupInformation.CreateUserForTesting("drwho@EXAMPLE.COM"
				, new string[] { "group1", "group2" });
			ServiceAuthorizationManager serviceAuthorizationManager = new ServiceAuthorizationManager
				();
			Configuration conf = new Configuration();
			// test without setting a blocked MachineList
			serviceAuthorizationManager.Refresh(conf, new TestServiceAuthorization.TestPolicyProvider
				());
			try
			{
				serviceAuthorizationManager.Authorize(drwho, typeof(TestRPC.TestProtocol), conf, 
					Sharpen.Extensions.GetAddressByName("10.222.0.0"));
			}
			catch (AuthorizationException)
			{
				NUnit.Framework.Assert.Fail();
			}
			// now set a blocked MachineList
			conf.Set(BlockedHostConfig, IpRange);
			serviceAuthorizationManager.Refresh(conf, new TestServiceAuthorization.TestPolicyProvider
				());
			try
			{
				serviceAuthorizationManager.Authorize(drwho, typeof(TestRPC.TestProtocol), conf, 
					Sharpen.Extensions.GetAddressByName("10.222.0.0"));
				NUnit.Framework.Assert.Fail();
			}
			catch (AuthorizationException)
			{
			}
			// expects Exception
			// reset blocked MachineList
			conf.Set(BlockedHostConfig, string.Empty);
			serviceAuthorizationManager.Refresh(conf, new TestServiceAuthorization.TestPolicyProvider
				());
			try
			{
				serviceAuthorizationManager.Authorize(drwho, typeof(TestRPC.TestProtocol), conf, 
					Sharpen.Extensions.GetAddressByName("10.222.0.0"));
			}
			catch (AuthorizationException)
			{
				NUnit.Framework.Assert.Fail();
			}
		}

		/// <exception cref="Sharpen.UnknownHostException"/>
		[Fact]
		public virtual void TestDefaultBlockedMachineList()
		{
			UserGroupInformation drwho = UserGroupInformation.CreateUserForTesting("drwho@EXAMPLE.COM"
				, new string[] { "group1", "group2" });
			ServiceAuthorizationManager serviceAuthorizationManager = new ServiceAuthorizationManager
				();
			Configuration conf = new Configuration();
			// test without setting a default blocked MachineList
			serviceAuthorizationManager.Refresh(conf, new TestServiceAuthorization.TestPolicyProvider
				());
			try
			{
				serviceAuthorizationManager.Authorize(drwho, typeof(TestServiceAuthorization.TestProtocol1
					), conf, Sharpen.Extensions.GetAddressByName("10.222.0.0"));
			}
			catch (AuthorizationException)
			{
				NUnit.Framework.Assert.Fail();
			}
			// set a  default blocked MachineList and a blocked MachineList for TestProtocol
			conf.Set("security.service.authorization.default.hosts.blocked", IpRange);
			conf.Set(BlockedHostConfig, "1.2.3.4");
			serviceAuthorizationManager.Refresh(conf, new TestServiceAuthorization.TestPolicyProvider
				());
			// TestProtocol can be accessed from "10.222.0.0" because it blocks only "1.2.3.4"
			try
			{
				serviceAuthorizationManager.Authorize(drwho, typeof(TestRPC.TestProtocol), conf, 
					Sharpen.Extensions.GetAddressByName("10.222.0.0"));
			}
			catch (AuthorizationException)
			{
				NUnit.Framework.Assert.Fail();
			}
			// TestProtocol cannot be accessed from  "1.2.3.4"
			try
			{
				serviceAuthorizationManager.Authorize(drwho, typeof(TestRPC.TestProtocol), conf, 
					Sharpen.Extensions.GetAddressByName("1.2.3.4"));
				NUnit.Framework.Assert.Fail();
			}
			catch (AuthorizationException)
			{
			}
			//expects Exception
			// TestProtocol1 can be accessed from "1.2.3.4" because it uses default block list
			try
			{
				serviceAuthorizationManager.Authorize(drwho, typeof(TestServiceAuthorization.TestProtocol1
					), conf, Sharpen.Extensions.GetAddressByName("1.2.3.4"));
			}
			catch (AuthorizationException)
			{
				NUnit.Framework.Assert.Fail();
			}
			// TestProtocol1 cannot be accessed from "10.222.0.0", 
			// because "10.222.0.0" is in default block list
			try
			{
				serviceAuthorizationManager.Authorize(drwho, typeof(TestServiceAuthorization.TestProtocol1
					), conf, Sharpen.Extensions.GetAddressByName("10.222.0.0"));
				NUnit.Framework.Assert.Fail();
			}
			catch (AuthorizationException)
			{
			}
		}
		//expects Exception
	}
}
