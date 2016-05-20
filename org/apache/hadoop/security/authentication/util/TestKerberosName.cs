using Sharpen;

namespace org.apache.hadoop.security.authentication.util
{
	public class TestKerberosName
	{
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public virtual void setUp()
		{
			Sharpen.Runtime.setProperty("java.security.krb5.realm", org.apache.hadoop.security.authentication.KerberosTestUtils
				.getRealm());
			Sharpen.Runtime.setProperty("java.security.krb5.kdc", "localhost:88");
			string rules = "RULE:[1:$1@$0](.*@YAHOO\\.COM)s/@.*//\n" + "RULE:[2:$1](johndoe)s/^.*$/guest/\n"
				 + "RULE:[2:$1;$2](^.*;admin$)s/;admin$//\n" + "RULE:[2:$2](root)\n" + "DEFAULT";
			org.apache.hadoop.security.authentication.util.KerberosName.setRules(rules);
			org.apache.hadoop.security.authentication.util.KerberosName.printRules();
		}

		/// <exception cref="System.Exception"/>
		private void checkTranslation(string from, string to)
		{
			System.Console.Out.WriteLine("Translate " + from);
			org.apache.hadoop.security.authentication.util.KerberosName nm = new org.apache.hadoop.security.authentication.util.KerberosName
				(from);
			string simple = nm.getShortName();
			System.Console.Out.WriteLine("to " + simple);
			NUnit.Framework.Assert.AreEqual("short name incorrect", to, simple);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testRules()
		{
			checkTranslation("omalley@" + org.apache.hadoop.security.authentication.KerberosTestUtils
				.getRealm(), "omalley");
			checkTranslation("hdfs/10.0.0.1@" + org.apache.hadoop.security.authentication.KerberosTestUtils
				.getRealm(), "hdfs");
			checkTranslation("oom@YAHOO.COM", "oom");
			checkTranslation("johndoe/zoo@FOO.COM", "guest");
			checkTranslation("joe/admin@FOO.COM", "joe");
			checkTranslation("joe/root@FOO.COM", "root");
		}

		private void checkBadName(string name)
		{
			System.Console.Out.WriteLine("Checking " + name + " to ensure it is bad.");
			try
			{
				new org.apache.hadoop.security.authentication.util.KerberosName(name);
				NUnit.Framework.Assert.Fail("didn't get exception for " + name);
			}
			catch (System.ArgumentException)
			{
			}
		}

		// PASS
		private void checkBadTranslation(string from)
		{
			System.Console.Out.WriteLine("Checking bad translation for " + from);
			org.apache.hadoop.security.authentication.util.KerberosName nm = new org.apache.hadoop.security.authentication.util.KerberosName
				(from);
			try
			{
				nm.getShortName();
				NUnit.Framework.Assert.Fail("didn't get exception for " + from);
			}
			catch (System.IO.IOException)
			{
			}
		}

		// PASS
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testAntiPatterns()
		{
			checkBadName("owen/owen/owen@FOO.COM");
			checkBadName("owen@foo/bar.com");
			checkBadTranslation("foo@ACME.COM");
			checkBadTranslation("root/joe@FOO.COM");
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testToLowerCase()
		{
			string rules = "RULE:[1:$1]/L\n" + "RULE:[2:$1]/L\n" + "RULE:[2:$1;$2](^.*;admin$)s/;admin$///L\n"
				 + "RULE:[2:$1;$2](^.*;guest$)s/;guest$//g/L\n" + "DEFAULT";
			org.apache.hadoop.security.authentication.util.KerberosName.setRules(rules);
			org.apache.hadoop.security.authentication.util.KerberosName.printRules();
			checkTranslation("Joe@FOO.COM", "joe");
			checkTranslation("Joe/root@FOO.COM", "joe");
			checkTranslation("Joe/admin@FOO.COM", "joe");
			checkTranslation("Joe/guestguest@FOO.COM", "joe");
		}

		[NUnit.Framework.TearDown]
		public virtual void clear()
		{
			Sharpen.Runtime.clearProperty("java.security.krb5.realm");
			Sharpen.Runtime.clearProperty("java.security.krb5.kdc");
		}
	}
}
