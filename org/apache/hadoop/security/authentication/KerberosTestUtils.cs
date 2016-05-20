using Sharpen;

namespace org.apache.hadoop.security.authentication
{
	/// <summary>Test helper class for Java Kerberos setup.</summary>
	public class KerberosTestUtils
	{
		private static string keytabFile = new java.io.File(Sharpen.Runtime.getProperty("test.dir"
			, "target"), java.util.UUID.randomUUID().ToString()).ToString();

		public static string getRealm()
		{
			return "EXAMPLE.COM";
		}

		public static string getClientPrincipal()
		{
			return "client@EXAMPLE.COM";
		}

		public static string getServerPrincipal()
		{
			return "HTTP/localhost@EXAMPLE.COM";
		}

		public static string getKeytabFile()
		{
			return keytabFile;
		}

		private class KerberosConfiguration : javax.security.auth.login.Configuration
		{
			private string principal;

			public KerberosConfiguration(string principal)
			{
				this.principal = principal;
			}

			public override javax.security.auth.login.AppConfigurationEntry[] getAppConfigurationEntry
				(string name)
			{
				System.Collections.Generic.IDictionary<string, string> options = new System.Collections.Generic.Dictionary
					<string, string>();
				options["keyTab"] = org.apache.hadoop.security.authentication.KerberosTestUtils.getKeytabFile
					();
				options["principal"] = principal;
				options["useKeyTab"] = "true";
				options["storeKey"] = "true";
				options["doNotPrompt"] = "true";
				options["useTicketCache"] = "true";
				options["renewTGT"] = "true";
				options["refreshKrb5Config"] = "true";
				options["isInitiator"] = "true";
				string ticketCache = Sharpen.Runtime.getenv("KRB5CCNAME");
				if (ticketCache != null)
				{
					options["ticketCache"] = ticketCache;
				}
				options["debug"] = "true";
				return new javax.security.auth.login.AppConfigurationEntry[] { new javax.security.auth.login.AppConfigurationEntry
					(org.apache.hadoop.security.authentication.util.KerberosUtil.getKrb5LoginModuleName
					(), javax.security.auth.login.AppConfigurationEntry.LoginModuleControlFlag.REQUIRED
					, options) };
			}
		}

		/// <exception cref="System.Exception"/>
		public static T doAs<T>(string principal, java.util.concurrent.Callable<T> callable
			)
		{
			javax.security.auth.login.LoginContext loginContext = null;
			try
			{
				System.Collections.Generic.ICollection<java.security.Principal> principals = new 
					java.util.HashSet<java.security.Principal>();
				principals.add(new javax.security.auth.kerberos.KerberosPrincipal(org.apache.hadoop.security.authentication.KerberosTestUtils
					.getClientPrincipal()));
				javax.security.auth.Subject subject = new javax.security.auth.Subject(false, principals
					, new java.util.HashSet<object>(), new java.util.HashSet<object>());
				loginContext = new javax.security.auth.login.LoginContext(string.Empty, subject, 
					null, new org.apache.hadoop.security.authentication.KerberosTestUtils.KerberosConfiguration
					(principal));
				loginContext.login();
				subject = loginContext.getSubject();
				return javax.security.auth.Subject.doAs(subject, new _PrivilegedExceptionAction_99
					(callable));
			}
			catch (java.security.PrivilegedActionException ex)
			{
				throw ex.getException();
			}
			finally
			{
				if (loginContext != null)
				{
					loginContext.logout();
				}
			}
		}

		private sealed class _PrivilegedExceptionAction_99 : java.security.PrivilegedExceptionAction
			<T>
		{
			public _PrivilegedExceptionAction_99(java.util.concurrent.Callable<T> callable)
			{
				this.callable = callable;
			}

			/// <exception cref="System.Exception"/>
			public T run()
			{
				return callable.call();
			}

			private readonly java.util.concurrent.Callable<T> callable;
		}

		/// <exception cref="System.Exception"/>
		public static T doAsClient<T>(java.util.concurrent.Callable<T> callable)
		{
			return doAs(getClientPrincipal(), callable);
		}

		/// <exception cref="System.Exception"/>
		public static T doAsServer<T>(java.util.concurrent.Callable<T> callable)
		{
			return doAs(getServerPrincipal(), callable);
		}
	}
}
