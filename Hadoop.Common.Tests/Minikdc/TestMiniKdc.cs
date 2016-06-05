using System.Collections.Generic;
using Javax.Security.Auth;
using Javax.Security.Auth.Kerberos;
using Javax.Security.Auth.Login;
using NUnit.Framework;
using Org.Apache.Directory.Server.Kerberos.Shared.Keytab;
using Sharpen;

namespace Org.Apache.Hadoop.Minikdc
{
	public class TestMiniKdc : KerberosSecurityTestcase
	{
		private static readonly bool IbmJava = Runtime.GetProperty("java.vendor").Contains
			("IBM");

		[Fact]
		public virtual void TestMiniKdcStart()
		{
			MiniKdc kdc = GetKdc();
			NUnit.Framework.Assert.AreNotSame(0, kdc.GetPort());
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestKeytabGen()
		{
			MiniKdc kdc = GetKdc();
			FilePath workDir = GetWorkDir();
			kdc.CreatePrincipal(new FilePath(workDir, "keytab"), "foo/bar", "bar/foo");
			Org.Apache.Directory.Server.Kerberos.Shared.Keytab.Keytab kt = Org.Apache.Directory.Server.Kerberos.Shared.Keytab.Keytab
				.Read(new FilePath(workDir, "keytab"));
			ICollection<string> principals = new HashSet<string>();
			foreach (KeytabEntry entry in kt.GetEntries())
			{
				principals.AddItem(entry.GetPrincipalName());
			}
			//here principals use \ instead of /
			//because org.apache.directory.server.kerberos.shared.keytab.KeytabDecoder
			// .getPrincipalName(IoBuffer buffer) use \\ when generates principal
			Assert.Equal(new HashSet<string>(Arrays.AsList("foo\\bar@" + kdc
				.GetRealm(), "bar\\foo@" + kdc.GetRealm())), principals);
		}

		private class KerberosConfiguration : Configuration
		{
			private string principal;

			private string keytab;

			private bool isInitiator;

			private KerberosConfiguration(string principal, FilePath keytab, bool client)
			{
				this.principal = principal;
				this.keytab = keytab.GetAbsolutePath();
				this.isInitiator = client;
			}

			public static Configuration CreateClientConfig(string principal, FilePath keytab)
			{
				return new TestMiniKdc.KerberosConfiguration(principal, keytab, true);
			}

			public static Configuration CreateServerConfig(string principal, FilePath keytab)
			{
				return new TestMiniKdc.KerberosConfiguration(principal, keytab, false);
			}

			private static string GetKrb5LoginModuleName()
			{
				return Runtime.GetProperty("java.vendor").Contains("IBM") ? "com.ibm.security.auth.module.Krb5LoginModule"
					 : "com.sun.security.auth.module.Krb5LoginModule";
			}

			public override AppConfigurationEntry[] GetAppConfigurationEntry(string name)
			{
				IDictionary<string, string> options = new Dictionary<string, string>();
				options["principal"] = principal;
				options["refreshKrb5Config"] = "true";
				if (IbmJava)
				{
					options["useKeytab"] = keytab;
					options["credsType"] = "both";
				}
				else
				{
					options["keyTab"] = keytab;
					options["useKeyTab"] = "true";
					options["storeKey"] = "true";
					options["doNotPrompt"] = "true";
					options["useTicketCache"] = "true";
					options["renewTGT"] = "true";
					options["isInitiator"] = bool.ToString(isInitiator);
				}
				string ticketCache = Runtime.Getenv("KRB5CCNAME");
				if (ticketCache != null)
				{
					options["ticketCache"] = ticketCache;
				}
				options["debug"] = "true";
				return new AppConfigurationEntry[] { new AppConfigurationEntry(GetKrb5LoginModuleName
					(), AppConfigurationEntry.LoginModuleControlFlag.Required, options) };
			}
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestKerberosLogin()
		{
			MiniKdc kdc = GetKdc();
			FilePath workDir = GetWorkDir();
			LoginContext loginContext = null;
			try
			{
				string principal = "foo";
				FilePath keytab = new FilePath(workDir, "foo.keytab");
				kdc.CreatePrincipal(keytab, principal);
				ICollection<Principal> principals = new HashSet<Principal>();
				principals.AddItem(new KerberosPrincipal(principal));
				//client login
				Subject subject = new Subject(false, principals, new HashSet<object>(), new HashSet
					<object>());
				loginContext = new LoginContext(string.Empty, subject, null, TestMiniKdc.KerberosConfiguration
					.CreateClientConfig(principal, keytab));
				loginContext.Login();
				subject = loginContext.GetSubject();
				Assert.Equal(1, subject.GetPrincipals().Count);
				Assert.Equal(typeof(KerberosPrincipal), subject.GetPrincipals(
					).GetEnumerator().Next().GetType());
				Assert.Equal(principal + "@" + kdc.GetRealm(), subject.GetPrincipals
					().GetEnumerator().Next().GetName());
				loginContext.Logout();
				//server login
				subject = new Subject(false, principals, new HashSet<object>(), new HashSet<object
					>());
				loginContext = new LoginContext(string.Empty, subject, null, TestMiniKdc.KerberosConfiguration
					.CreateServerConfig(principal, keytab));
				loginContext.Login();
				subject = loginContext.GetSubject();
				Assert.Equal(1, subject.GetPrincipals().Count);
				Assert.Equal(typeof(KerberosPrincipal), subject.GetPrincipals(
					).GetEnumerator().Next().GetType());
				Assert.Equal(principal + "@" + kdc.GetRealm(), subject.GetPrincipals
					().GetEnumerator().Next().GetName());
				loginContext.Logout();
			}
			finally
			{
				if (loginContext != null)
				{
					loginContext.Logout();
				}
			}
		}
	}
}
