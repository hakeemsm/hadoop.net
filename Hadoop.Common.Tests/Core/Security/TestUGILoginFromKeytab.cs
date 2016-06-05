using NUnit.Framework;
using NUnit.Framework.Rules;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Minikdc;
using Sharpen;

namespace Org.Apache.Hadoop.Security
{
	/// <summary>Verify UGI login from keytab.</summary>
	/// <remarks>
	/// Verify UGI login from keytab. Check that the UGI is
	/// configured to use keytab to catch regressions like
	/// HADOOP-10786.
	/// </remarks>
	public class TestUGILoginFromKeytab
	{
		private MiniKdc kdc;

		private FilePath workDir;

		[Rule]
		public readonly TemporaryFolder folder = new TemporaryFolder();

		/// <exception cref="System.Exception"/>
		[SetUp]
		public virtual void StartMiniKdc()
		{
			// This setting below is required. If not enabled, UGI will abort
			// any attempt to loginUserFromKeytab.
			Configuration conf = new Configuration();
			conf.Set(CommonConfigurationKeys.HadoopSecurityAuthentication, "kerberos");
			UserGroupInformation.SetConfiguration(conf);
			workDir = folder.GetRoot();
			kdc = new MiniKdc(MiniKdc.CreateConf(), workDir);
			kdc.Start();
		}

		[TearDown]
		public virtual void StopMiniKdc()
		{
			if (kdc != null)
			{
				kdc.Stop();
			}
		}

		/// <summary>
		/// Login from keytab using the MiniKDC and verify the UGI can successfully
		/// relogin from keytab as well.
		/// </summary>
		/// <remarks>
		/// Login from keytab using the MiniKDC and verify the UGI can successfully
		/// relogin from keytab as well. This will catch regressions like HADOOP-10786.
		/// </remarks>
		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestUGILoginFromKeytab()
		{
			UserGroupInformation.SetShouldRenewImmediatelyForTests(true);
			string principal = "foo";
			FilePath keytab = new FilePath(workDir, "foo.keytab");
			kdc.CreatePrincipal(keytab, principal);
			UserGroupInformation.LoginUserFromKeytab(principal, keytab.GetPath());
			UserGroupInformation ugi = UserGroupInformation.GetLoginUser();
			Assert.True("UGI should be configured to login from keytab", ugi
				.IsFromKeytab());
			// Verify relogin from keytab.
			User user = ugi.GetSubject().GetPrincipals<User>().GetEnumerator().Next();
			long firstLogin = user.GetLastLogin();
			ugi.ReloginFromKeytab();
			long secondLogin = user.GetLastLogin();
			Assert.True("User should have been able to relogin from keytab"
				, secondLogin > firstLogin);
		}
	}
}
