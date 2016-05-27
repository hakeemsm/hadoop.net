using System;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Sharpen;

namespace Org.Apache.Hadoop.Security
{
	/// <summary>
	/// Tests kerberos keytab login using a user-specified external KDC
	/// To run, users must specify the following system properties:
	/// externalKdc=true
	/// java.security.krb5.conf
	/// user.principal
	/// user.keytab
	/// </summary>
	public class TestUGIWithExternalKdc
	{
		[SetUp]
		public virtual void TestExternalKdcRunning()
		{
			Assume.AssumeTrue(SecurityUtilTestHelper.IsExternalKdcRunning());
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestLogin()
		{
			string userPrincipal = Runtime.GetProperty("user.principal");
			string userKeyTab = Runtime.GetProperty("user.keytab");
			NUnit.Framework.Assert.IsNotNull("User principal was not specified", userPrincipal
				);
			NUnit.Framework.Assert.IsNotNull("User keytab was not specified", userKeyTab);
			Configuration conf = new Configuration();
			conf.Set(CommonConfigurationKeys.HadoopSecurityAuthentication, "kerberos");
			UserGroupInformation.SetConfiguration(conf);
			UserGroupInformation ugi = UserGroupInformation.LoginUserFromKeytabAndReturnUGI(userPrincipal
				, userKeyTab);
			NUnit.Framework.Assert.AreEqual(UserGroupInformation.AuthenticationMethod.Kerberos
				, ugi.GetAuthenticationMethod());
			try
			{
				UserGroupInformation.LoginUserFromKeytabAndReturnUGI("bogus@EXAMPLE.COM", userKeyTab
					);
				NUnit.Framework.Assert.Fail("Login should have failed");
			}
			catch (Exception ex)
			{
				Sharpen.Runtime.PrintStackTrace(ex);
			}
		}
	}
}
