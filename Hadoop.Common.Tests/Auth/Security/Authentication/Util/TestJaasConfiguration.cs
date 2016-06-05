using System.Collections.Generic;
using Javax.Security.Auth.Login;
using NUnit.Framework;


namespace Org.Apache.Hadoop.Security.Authentication.Util
{
	public class TestJaasConfiguration
	{
		// We won't test actually using it to authenticate because that gets messy and
		// may conflict with other tests; but we can test that it otherwise behaves
		// correctly
		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void Test()
		{
			string krb5LoginModuleName;
			if (Runtime.GetProperty("java.vendor").Contains("IBM"))
			{
				krb5LoginModuleName = "com.ibm.security.auth.module.Krb5LoginModule";
			}
			else
			{
				krb5LoginModuleName = "com.sun.security.auth.module.Krb5LoginModule";
			}
			ZKSignerSecretProvider.JaasConfiguration jConf = new ZKSignerSecretProvider.JaasConfiguration
				("foo", "foo/localhost", "/some/location/foo.keytab");
			AppConfigurationEntry[] entries = jConf.GetAppConfigurationEntry("bar");
			NUnit.Framework.Assert.IsNull(entries);
			entries = jConf.GetAppConfigurationEntry("foo");
			Assert.Equal(1, entries.Length);
			AppConfigurationEntry entry = entries[0];
			Assert.Equal(AppConfigurationEntry.LoginModuleControlFlag.Required
				, entry.GetControlFlag());
			Assert.Equal(krb5LoginModuleName, entry.GetLoginModuleName());
			IDictionary<string, object> options = entry.GetOptions();
			Assert.Equal("/some/location/foo.keytab", options["keyTab"]);
			Assert.Equal("foo/localhost", options["principal"]);
			Assert.Equal("true", options["useKeyTab"]);
			Assert.Equal("true", options["storeKey"]);
			Assert.Equal("false", options["useTicketCache"]);
			Assert.Equal("true", options["refreshKrb5Config"]);
			Assert.Equal(6, options.Count);
		}
	}
}
