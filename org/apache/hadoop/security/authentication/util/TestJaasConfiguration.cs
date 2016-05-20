using Sharpen;

namespace org.apache.hadoop.security.authentication.util
{
	public class TestJaasConfiguration
	{
		// We won't test actually using it to authenticate because that gets messy and
		// may conflict with other tests; but we can test that it otherwise behaves
		// correctly
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void test()
		{
			string krb5LoginModuleName;
			if (Sharpen.Runtime.getProperty("java.vendor").contains("IBM"))
			{
				krb5LoginModuleName = "com.ibm.security.auth.module.Krb5LoginModule";
			}
			else
			{
				krb5LoginModuleName = "com.sun.security.auth.module.Krb5LoginModule";
			}
			org.apache.hadoop.security.authentication.util.ZKSignerSecretProvider.JaasConfiguration
				 jConf = new org.apache.hadoop.security.authentication.util.ZKSignerSecretProvider.JaasConfiguration
				("foo", "foo/localhost", "/some/location/foo.keytab");
			javax.security.auth.login.AppConfigurationEntry[] entries = jConf.getAppConfigurationEntry
				("bar");
			NUnit.Framework.Assert.IsNull(entries);
			entries = jConf.getAppConfigurationEntry("foo");
			NUnit.Framework.Assert.AreEqual(1, entries.Length);
			javax.security.auth.login.AppConfigurationEntry entry = entries[0];
			NUnit.Framework.Assert.AreEqual(javax.security.auth.login.AppConfigurationEntry.LoginModuleControlFlag
				.REQUIRED, entry.getControlFlag());
			NUnit.Framework.Assert.AreEqual(krb5LoginModuleName, entry.getLoginModuleName());
			System.Collections.Generic.IDictionary<string, object> options = entry.getOptions
				();
			NUnit.Framework.Assert.AreEqual("/some/location/foo.keytab", options["keyTab"]);
			NUnit.Framework.Assert.AreEqual("foo/localhost", options["principal"]);
			NUnit.Framework.Assert.AreEqual("true", options["useKeyTab"]);
			NUnit.Framework.Assert.AreEqual("true", options["storeKey"]);
			NUnit.Framework.Assert.AreEqual("false", options["useTicketCache"]);
			NUnit.Framework.Assert.AreEqual("true", options["refreshKrb5Config"]);
			NUnit.Framework.Assert.AreEqual(6, options.Count);
		}
	}
}
