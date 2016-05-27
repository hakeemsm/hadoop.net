using System.Collections.Generic;
using Javax.Security.Auth.Login;
using NUnit.Framework;
using Sharpen;

namespace Org.Apache.Hadoop.Security.Authentication.Util
{
	public class TestJaasConfiguration
	{
		// We won't test actually using it to authenticate because that gets messy and
		// may conflict with other tests; but we can test that it otherwise behaves
		// correctly
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
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
			NUnit.Framework.Assert.AreEqual(1, entries.Length);
			AppConfigurationEntry entry = entries[0];
			NUnit.Framework.Assert.AreEqual(AppConfigurationEntry.LoginModuleControlFlag.Required
				, entry.GetControlFlag());
			NUnit.Framework.Assert.AreEqual(krb5LoginModuleName, entry.GetLoginModuleName());
			IDictionary<string, object> options = entry.GetOptions();
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
