using System;
using System.Collections.Generic;
using System.IO;
using NUnit.Framework;
using Org.Apache.Directory.Server.Kerberos.Shared.Keytab;
using Org.Apache.Directory.Shared.Kerberos;
using Org.Apache.Directory.Shared.Kerberos.Codec.Types;
using Org.Apache.Directory.Shared.Kerberos.Components;
using Sharpen;

namespace Org.Apache.Hadoop.Security.Authentication.Util
{
	public class TestKerberosUtil
	{
		internal static string testKeytab = "test.keytab";

		internal static string[] testPrincipals = new string[] { "HTTP@testRealm", "test/testhost@testRealm"
			, "HTTP/testhost@testRealm", "HTTP1/testhost@testRealm", "HTTP/testhostanother@testRealm"
			 };

		[TearDown]
		public virtual void DeleteKeytab()
		{
			FilePath keytabFile = new FilePath(testKeytab);
			if (keytabFile.Exists())
			{
				keytabFile.Delete();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestGetServerPrincipal()
		{
			string service = "TestKerberosUtil";
			string localHostname = KerberosUtil.GetLocalHostName();
			string testHost = "FooBar";
			// send null hostname
			NUnit.Framework.Assert.AreEqual("When no hostname is sent", service + "/" + localHostname
				.ToLower(Sharpen.Extensions.GetEnglishCulture()), KerberosUtil.GetServicePrincipal
				(service, null));
			// send empty hostname
			NUnit.Framework.Assert.AreEqual("When empty hostname is sent", service + "/" + localHostname
				.ToLower(Sharpen.Extensions.GetEnglishCulture()), KerberosUtil.GetServicePrincipal
				(service, string.Empty));
			// send 0.0.0.0 hostname
			NUnit.Framework.Assert.AreEqual("When 0.0.0.0 hostname is sent", service + "/" + 
				localHostname.ToLower(Sharpen.Extensions.GetEnglishCulture()), KerberosUtil.GetServicePrincipal
				(service, "0.0.0.0"));
			// send uppercase hostname
			NUnit.Framework.Assert.AreEqual("When uppercase hostname is sent", service + "/" 
				+ testHost.ToLower(Sharpen.Extensions.GetEnglishCulture()), KerberosUtil.GetServicePrincipal
				(service, testHost));
			// send lowercase hostname
			NUnit.Framework.Assert.AreEqual("When lowercase hostname is sent", service + "/" 
				+ testHost.ToLower(Sharpen.Extensions.GetEnglishCulture()), KerberosUtil.GetServicePrincipal
				(service, testHost.ToLower(Sharpen.Extensions.GetEnglishCulture())));
		}

		[NUnit.Framework.Test]
		public virtual void TestGetPrincipalNamesMissingKeytab()
		{
			try
			{
				KerberosUtil.GetPrincipalNames(testKeytab);
				NUnit.Framework.Assert.Fail("Exception should have been thrown");
			}
			catch (IOException)
			{
			}
		}

		//expects exception
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestGetPrincipalNamesMissingPattern()
		{
			CreateKeyTab(testKeytab, new string[] { "test/testhost@testRealm" });
			try
			{
				KerberosUtil.GetPrincipalNames(testKeytab, null);
				NUnit.Framework.Assert.Fail("Exception should have been thrown");
			}
			catch (Exception)
			{
			}
		}

		//expects exception
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestGetPrincipalNamesFromKeytab()
		{
			CreateKeyTab(testKeytab, testPrincipals);
			// read all principals in the keytab file
			string[] principals = KerberosUtil.GetPrincipalNames(testKeytab);
			NUnit.Framework.Assert.IsNotNull("principals cannot be null", principals);
			int expectedSize = 0;
			IList<string> principalList = Arrays.AsList(principals);
			foreach (string principal in testPrincipals)
			{
				NUnit.Framework.Assert.IsTrue("missing principal " + principal, principalList.Contains
					(principal));
				expectedSize++;
			}
			NUnit.Framework.Assert.AreEqual(expectedSize, principals.Length);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestGetPrincipalNamesFromKeytabWithPattern()
		{
			CreateKeyTab(testKeytab, testPrincipals);
			// read the keytab file
			// look for principals with HTTP as the first part
			Sharpen.Pattern httpPattern = Sharpen.Pattern.Compile("HTTP/.*");
			string[] httpPrincipals = KerberosUtil.GetPrincipalNames(testKeytab, httpPattern);
			NUnit.Framework.Assert.IsNotNull("principals cannot be null", httpPrincipals);
			int expectedSize = 0;
			IList<string> httpPrincipalList = Arrays.AsList(httpPrincipals);
			foreach (string principal in testPrincipals)
			{
				if (httpPattern.Matcher(principal).Matches())
				{
					NUnit.Framework.Assert.IsTrue("missing principal " + principal, httpPrincipalList
						.Contains(principal));
					expectedSize++;
				}
			}
			NUnit.Framework.Assert.AreEqual(expectedSize, httpPrincipals.Length);
		}

		/// <exception cref="System.IO.IOException"/>
		private void CreateKeyTab(string fileName, string[] principalNames)
		{
			//create a test keytab file
			IList<KeytabEntry> lstEntries = new AList<KeytabEntry>();
			foreach (string principal in principalNames)
			{
				// create 3 versions of the key to ensure methods don't return
				// duplicate principals
				for (int kvno = 1; kvno <= 3; kvno++)
				{
					EncryptionKey key = new EncryptionKey(EncryptionType.Unknown, Sharpen.Runtime.GetBytesForString
						("samplekey1"), kvno);
					KeytabEntry keytabEntry = new KeytabEntry(principal, 1, new KerberosTime(), unchecked(
						(byte)1), key);
					lstEntries.AddItem(keytabEntry);
				}
			}
			Org.Apache.Directory.Server.Kerberos.Shared.Keytab.Keytab keytab = Org.Apache.Directory.Server.Kerberos.Shared.Keytab.Keytab
				.GetInstance();
			keytab.SetEntries(lstEntries);
			keytab.Write(new FilePath(testKeytab));
		}
	}
}
