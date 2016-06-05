using System.Collections.Generic;
using Javax.Security.Auth;
using Javax.Security.Auth.Kerberos;
using Javax.Security.Auth.Login;
using Org.Apache.Hadoop.Security.Authentication.Util;


namespace Org.Apache.Hadoop.Security.Authentication
{
	/// <summary>Test helper class for Java Kerberos setup.</summary>
	public class KerberosTestUtils
	{
		private static string keytabFile = new FilePath(Runtime.GetProperty("test.dir", "target"
			), UUID.RandomUUID().ToString()).ToString();

		public static string GetRealm()
		{
			return "EXAMPLE.COM";
		}

		public static string GetClientPrincipal()
		{
			return "client@EXAMPLE.COM";
		}

		public static string GetServerPrincipal()
		{
			return "HTTP/localhost@EXAMPLE.COM";
		}

		public static string GetKeytabFile()
		{
			return keytabFile;
		}

		private class KerberosConfiguration : Configuration
		{
			private string principal;

			public KerberosConfiguration(string principal)
			{
				this.principal = principal;
			}

			public override AppConfigurationEntry[] GetAppConfigurationEntry(string name)
			{
				IDictionary<string, string> options = new Dictionary<string, string>();
				options["keyTab"] = KerberosTestUtils.GetKeytabFile();
				options["principal"] = principal;
				options["useKeyTab"] = "true";
				options["storeKey"] = "true";
				options["doNotPrompt"] = "true";
				options["useTicketCache"] = "true";
				options["renewTGT"] = "true";
				options["refreshKrb5Config"] = "true";
				options["isInitiator"] = "true";
				string ticketCache = Runtime.Getenv("KRB5CCNAME");
				if (ticketCache != null)
				{
					options["ticketCache"] = ticketCache;
				}
				options["debug"] = "true";
				return new AppConfigurationEntry[] { new AppConfigurationEntry(KerberosUtil.GetKrb5LoginModuleName
					(), AppConfigurationEntry.LoginModuleControlFlag.Required, options) };
			}
		}

		/// <exception cref="System.Exception"/>
		public static T DoAs<T>(string principal, Callable<T> callable)
		{
			LoginContext loginContext = null;
			try
			{
				ICollection<Principal> principals = new HashSet<Principal>();
				principals.AddItem(new KerberosPrincipal(KerberosTestUtils.GetClientPrincipal()));
				Subject subject = new Subject(false, principals, new HashSet<object>(), new HashSet
					<object>());
				loginContext = new LoginContext(string.Empty, subject, null, new KerberosTestUtils.KerberosConfiguration
					(principal));
				loginContext.Login();
				subject = loginContext.GetSubject();
				return Subject.DoAs(subject, new _PrivilegedExceptionAction_99(callable));
			}
			catch (PrivilegedActionException ex)
			{
				throw ex.GetException();
			}
			finally
			{
				if (loginContext != null)
				{
					loginContext.Logout();
				}
			}
		}

		private sealed class _PrivilegedExceptionAction_99 : PrivilegedExceptionAction<T>
		{
			public _PrivilegedExceptionAction_99(Callable<T> callable)
			{
				this.callable = callable;
			}

			/// <exception cref="System.Exception"/>
			public T Run()
			{
				return callable.Call();
			}

			private readonly Callable<T> callable;
		}

		/// <exception cref="System.Exception"/>
		public static T DoAsClient<T>(Callable<T> callable)
		{
			return DoAs(GetClientPrincipal(), callable);
		}

		/// <exception cref="System.Exception"/>
		public static T DoAsServer<T>(Callable<T> callable)
		{
			return DoAs(GetServerPrincipal(), callable);
		}
	}
}
