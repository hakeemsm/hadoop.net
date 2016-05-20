using Sharpen;

namespace org.apache.hadoop.security.authentication.util
{
	public class TestKerberosUtil
	{
		internal static string testKeytab = "test.keytab";

		internal static string[] testPrincipals = new string[] { "HTTP@testRealm", "test/testhost@testRealm"
			, "HTTP/testhost@testRealm", "HTTP1/testhost@testRealm", "HTTP/testhostanother@testRealm"
			 };

		[NUnit.Framework.TearDown]
		public virtual void deleteKeytab()
		{
			java.io.File keytabFile = new java.io.File(testKeytab);
			if (keytabFile.exists())
			{
				keytabFile.delete();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testGetServerPrincipal()
		{
			string service = "TestKerberosUtil";
			string localHostname = org.apache.hadoop.security.authentication.util.KerberosUtil
				.getLocalHostName();
			string testHost = "FooBar";
			// send null hostname
			NUnit.Framework.Assert.AreEqual("When no hostname is sent", service + "/" + localHostname
				.ToLower(java.util.Locale.ENGLISH), org.apache.hadoop.security.authentication.util.KerberosUtil
				.getServicePrincipal(service, null));
			// send empty hostname
			NUnit.Framework.Assert.AreEqual("When empty hostname is sent", service + "/" + localHostname
				.ToLower(java.util.Locale.ENGLISH), org.apache.hadoop.security.authentication.util.KerberosUtil
				.getServicePrincipal(service, string.Empty));
			// send 0.0.0.0 hostname
			NUnit.Framework.Assert.AreEqual("When 0.0.0.0 hostname is sent", service + "/" + 
				localHostname.ToLower(java.util.Locale.ENGLISH), org.apache.hadoop.security.authentication.util.KerberosUtil
				.getServicePrincipal(service, "0.0.0.0"));
			// send uppercase hostname
			NUnit.Framework.Assert.AreEqual("When uppercase hostname is sent", service + "/" 
				+ testHost.ToLower(java.util.Locale.ENGLISH), org.apache.hadoop.security.authentication.util.KerberosUtil
				.getServicePrincipal(service, testHost));
			// send lowercase hostname
			NUnit.Framework.Assert.AreEqual("When lowercase hostname is sent", service + "/" 
				+ testHost.ToLower(java.util.Locale.ENGLISH), org.apache.hadoop.security.authentication.util.KerberosUtil
				.getServicePrincipal(service, testHost.ToLower(java.util.Locale.ENGLISH)));
		}

		[NUnit.Framework.Test]
		public virtual void testGetPrincipalNamesMissingKeytab()
		{
			try
			{
				org.apache.hadoop.security.authentication.util.KerberosUtil.getPrincipalNames(testKeytab
					);
				NUnit.Framework.Assert.Fail("Exception should have been thrown");
			}
			catch (System.IO.IOException)
			{
			}
		}

		//expects exception
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testGetPrincipalNamesMissingPattern()
		{
			createKeyTab(testKeytab, new string[] { "test/testhost@testRealm" });
			try
			{
				org.apache.hadoop.security.authentication.util.KerberosUtil.getPrincipalNames(testKeytab
					, null);
				NUnit.Framework.Assert.Fail("Exception should have been thrown");
			}
			catch (System.Exception)
			{
			}
		}

		//expects exception
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testGetPrincipalNamesFromKeytab()
		{
			createKeyTab(testKeytab, testPrincipals);
			// read all principals in the keytab file
			string[] principals = org.apache.hadoop.security.authentication.util.KerberosUtil
				.getPrincipalNames(testKeytab);
			NUnit.Framework.Assert.IsNotNull("principals cannot be null", principals);
			int expectedSize = 0;
			System.Collections.Generic.IList<string> principalList = java.util.Arrays.asList(
				principals);
			foreach (string principal in testPrincipals)
			{
				NUnit.Framework.Assert.IsTrue("missing principal " + principal, principalList.contains
					(principal));
				expectedSize++;
			}
			NUnit.Framework.Assert.AreEqual(expectedSize, principals.Length);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void testGetPrincipalNamesFromKeytabWithPattern()
		{
			createKeyTab(testKeytab, testPrincipals);
			// read the keytab file
			// look for principals with HTTP as the first part
			java.util.regex.Pattern httpPattern = java.util.regex.Pattern.compile("HTTP/.*");
			string[] httpPrincipals = org.apache.hadoop.security.authentication.util.KerberosUtil
				.getPrincipalNames(testKeytab, httpPattern);
			NUnit.Framework.Assert.IsNotNull("principals cannot be null", httpPrincipals);
			int expectedSize = 0;
			System.Collections.Generic.IList<string> httpPrincipalList = java.util.Arrays.asList
				(httpPrincipals);
			foreach (string principal in testPrincipals)
			{
				if (httpPattern.matcher(principal).matches())
				{
					NUnit.Framework.Assert.IsTrue("missing principal " + principal, httpPrincipalList
						.contains(principal));
					expectedSize++;
				}
			}
			NUnit.Framework.Assert.AreEqual(expectedSize, httpPrincipals.Length);
		}

		/// <exception cref="System.IO.IOException"/>
		private void createKeyTab(string fileName, string[] principalNames)
		{
			//create a test keytab file
			System.Collections.Generic.IList<org.apache.directory.server.kerberos.shared.keytab.KeytabEntry
				> lstEntries = new System.Collections.Generic.List<org.apache.directory.server.kerberos.shared.keytab.KeytabEntry
				>();
			foreach (string principal in principalNames)
			{
				// create 3 versions of the key to ensure methods don't return
				// duplicate principals
				for (int kvno = 1; kvno <= 3; kvno++)
				{
					org.apache.directory.shared.kerberos.components.EncryptionKey key = new org.apache.directory.shared.kerberos.components.EncryptionKey
						(org.apache.directory.shared.kerberos.codec.types.EncryptionType.UNKNOWN, Sharpen.Runtime.getBytesForString
						("samplekey1"), kvno);
					org.apache.directory.server.kerberos.shared.keytab.KeytabEntry keytabEntry = new 
						org.apache.directory.server.kerberos.shared.keytab.KeytabEntry(principal, 1, new 
						org.apache.directory.shared.kerberos.KerberosTime(), unchecked((byte)1), key);
					lstEntries.add(keytabEntry);
				}
			}
			org.apache.directory.server.kerberos.shared.keytab.Keytab keytab = org.apache.directory.server.kerberos.shared.keytab.Keytab
				.getInstance();
			keytab.setEntries(lstEntries);
			keytab.write(new java.io.File(testKeytab));
		}
	}
}
