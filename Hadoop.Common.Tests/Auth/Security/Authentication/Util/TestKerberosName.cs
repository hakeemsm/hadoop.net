using System;
using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.Security.Authentication;
using Sharpen;

namespace Org.Apache.Hadoop.Security.Authentication.Util
{
	public class TestKerberosName
	{
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public virtual void SetUp()
		{
			Runtime.SetProperty("java.security.krb5.realm", KerberosTestUtils.GetRealm());
			Runtime.SetProperty("java.security.krb5.kdc", "localhost:88");
			string rules = "RULE:[1:$1@$0](.*@YAHOO\\.COM)s/@.*//\n" + "RULE:[2:$1](johndoe)s/^.*$/guest/\n"
				 + "RULE:[2:$1;$2](^.*;admin$)s/;admin$//\n" + "RULE:[2:$2](root)\n" + "DEFAULT";
			KerberosName.SetRules(rules);
			KerberosName.PrintRules();
		}

		/// <exception cref="System.Exception"/>
		private void CheckTranslation(string from, string to)
		{
			System.Console.Out.WriteLine("Translate " + from);
			KerberosName nm = new KerberosName(from);
			string simple = nm.GetShortName();
			System.Console.Out.WriteLine("to " + simple);
			Assert.Equal("short name incorrect", to, simple);
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestRules()
		{
			CheckTranslation("omalley@" + KerberosTestUtils.GetRealm(), "omalley");
			CheckTranslation("hdfs/10.0.0.1@" + KerberosTestUtils.GetRealm(), "hdfs");
			CheckTranslation("oom@YAHOO.COM", "oom");
			CheckTranslation("johndoe/zoo@FOO.COM", "guest");
			CheckTranslation("joe/admin@FOO.COM", "joe");
			CheckTranslation("joe/root@FOO.COM", "root");
		}

		private void CheckBadName(string name)
		{
			System.Console.Out.WriteLine("Checking " + name + " to ensure it is bad.");
			try
			{
				new KerberosName(name);
				NUnit.Framework.Assert.Fail("didn't get exception for " + name);
			}
			catch (ArgumentException)
			{
			}
		}

		// PASS
		private void CheckBadTranslation(string from)
		{
			System.Console.Out.WriteLine("Checking bad translation for " + from);
			KerberosName nm = new KerberosName(from);
			try
			{
				nm.GetShortName();
				NUnit.Framework.Assert.Fail("didn't get exception for " + from);
			}
			catch (IOException)
			{
			}
		}

		// PASS
		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestAntiPatterns()
		{
			CheckBadName("owen/owen/owen@FOO.COM");
			CheckBadName("owen@foo/bar.com");
			CheckBadTranslation("foo@ACME.COM");
			CheckBadTranslation("root/joe@FOO.COM");
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestToLowerCase()
		{
			string rules = "RULE:[1:$1]/L\n" + "RULE:[2:$1]/L\n" + "RULE:[2:$1;$2](^.*;admin$)s/;admin$///L\n"
				 + "RULE:[2:$1;$2](^.*;guest$)s/;guest$//g/L\n" + "DEFAULT";
			KerberosName.SetRules(rules);
			KerberosName.PrintRules();
			CheckTranslation("Joe@FOO.COM", "joe");
			CheckTranslation("Joe/root@FOO.COM", "joe");
			CheckTranslation("Joe/admin@FOO.COM", "joe");
			CheckTranslation("Joe/guestguest@FOO.COM", "joe");
		}

		[TearDown]
		public virtual void Clear()
		{
			Runtime.ClearProperty("java.security.krb5.realm");
			Runtime.ClearProperty("java.security.krb5.kdc");
		}
	}
}
