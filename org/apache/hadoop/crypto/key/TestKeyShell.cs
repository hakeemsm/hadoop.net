using Sharpen;

namespace org.apache.hadoop.crypto.key
{
	public class TestKeyShell
	{
		private readonly java.io.ByteArrayOutputStream outContent = new java.io.ByteArrayOutputStream
			();

		private readonly java.io.ByteArrayOutputStream errContent = new java.io.ByteArrayOutputStream
			();

		private System.IO.TextWriter initialStdOut;

		private System.IO.TextWriter initialStdErr;

		private string jceksProvider;

		/* The default JCEKS provider - for testing purposes */
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public virtual void setup()
		{
			outContent.reset();
			errContent.reset();
			java.io.File tmpDir = new java.io.File(Sharpen.Runtime.getProperty("test.build.data"
				, "target"), java.util.UUID.randomUUID().ToString());
			if (!tmpDir.mkdirs())
			{
				throw new System.IO.IOException("Unable to create " + tmpDir);
			}
			org.apache.hadoop.fs.Path jksPath = new org.apache.hadoop.fs.Path(tmpDir.ToString
				(), "keystore.jceks");
			jceksProvider = "jceks://file" + jksPath.toUri();
			initialStdOut = System.Console.Out;
			initialStdErr = System.Console.Error;
			Sharpen.Runtime.setOut(new System.IO.TextWriter(outContent));
			Sharpen.Runtime.setErr(new System.IO.TextWriter(errContent));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.TearDown]
		public virtual void cleanUp()
		{
			Sharpen.Runtime.setOut(initialStdOut);
			Sharpen.Runtime.setErr(initialStdErr);
		}

		/// <summary>Delete a key from the default jceksProvider</summary>
		/// <param name="ks">The KeyShell instance</param>
		/// <param name="keyName">The key to delete</param>
		/// <exception cref="System.Exception"/>
		private void deleteKey(org.apache.hadoop.crypto.key.KeyShell ks, string keyName)
		{
			int rc;
			outContent.reset();
			string[] delArgs = new string[] { "delete", keyName, "-f", "-provider", jceksProvider
				 };
			rc = ks.run(delArgs);
			NUnit.Framework.Assert.AreEqual(0, rc);
			NUnit.Framework.Assert.IsTrue(outContent.ToString().contains(keyName + " has been "
				 + "successfully deleted."));
		}

		/// <summary>Lists the keys in the jceksProvider</summary>
		/// <param name="ks">The KeyShell instance</param>
		/// <param name="wantMetadata">True if you want metadata returned with the keys</param>
		/// <returns>The output from the "list" call</returns>
		/// <exception cref="System.Exception"/>
		private string listKeys(org.apache.hadoop.crypto.key.KeyShell ks, bool wantMetadata
			)
		{
			int rc;
			outContent.reset();
			string[] listArgs = new string[] { "list", "-provider", jceksProvider };
			string[] listArgsM = new string[] { "list", "-metadata", "-provider", jceksProvider
				 };
			rc = ks.run(wantMetadata ? listArgsM : listArgs);
			NUnit.Framework.Assert.AreEqual(0, rc);
			return outContent.ToString();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testKeySuccessfulKeyLifecycle()
		{
			int rc = 0;
			string keyName = "key1";
			org.apache.hadoop.crypto.key.KeyShell ks = new org.apache.hadoop.crypto.key.KeyShell
				();
			ks.setConf(new org.apache.hadoop.conf.Configuration());
			outContent.reset();
			string[] args1 = new string[] { "create", keyName, "-provider", jceksProvider };
			rc = ks.run(args1);
			NUnit.Framework.Assert.AreEqual(0, rc);
			NUnit.Framework.Assert.IsTrue(outContent.ToString().contains(keyName + " has been "
				 + "successfully created"));
			string listOut = listKeys(ks, false);
			NUnit.Framework.Assert.IsTrue(listOut.contains(keyName));
			listOut = listKeys(ks, true);
			NUnit.Framework.Assert.IsTrue(listOut.contains(keyName));
			NUnit.Framework.Assert.IsTrue(listOut.contains("description"));
			NUnit.Framework.Assert.IsTrue(listOut.contains("created"));
			outContent.reset();
			string[] args2 = new string[] { "roll", keyName, "-provider", jceksProvider };
			rc = ks.run(args2);
			NUnit.Framework.Assert.AreEqual(0, rc);
			NUnit.Framework.Assert.IsTrue(outContent.ToString().contains("key1 has been successfully "
				 + "rolled."));
			deleteKey(ks, keyName);
			listOut = listKeys(ks, false);
			NUnit.Framework.Assert.IsFalse(listOut, listOut.contains(keyName));
		}

		/* HADOOP-10586 KeyShell didn't allow -description. */
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testKeySuccessfulCreationWithDescription()
		{
			outContent.reset();
			string[] args1 = new string[] { "create", "key1", "-provider", jceksProvider, "-description"
				, "someDescription" };
			int rc = 0;
			org.apache.hadoop.crypto.key.KeyShell ks = new org.apache.hadoop.crypto.key.KeyShell
				();
			ks.setConf(new org.apache.hadoop.conf.Configuration());
			rc = ks.run(args1);
			NUnit.Framework.Assert.AreEqual(0, rc);
			NUnit.Framework.Assert.IsTrue(outContent.ToString().contains("key1 has been successfully "
				 + "created"));
			string listOut = listKeys(ks, true);
			NUnit.Framework.Assert.IsTrue(listOut.contains("description"));
			NUnit.Framework.Assert.IsTrue(listOut.contains("someDescription"));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testInvalidKeySize()
		{
			string[] args1 = new string[] { "create", "key1", "-size", "56", "-provider", jceksProvider
				 };
			int rc = 0;
			org.apache.hadoop.crypto.key.KeyShell ks = new org.apache.hadoop.crypto.key.KeyShell
				();
			ks.setConf(new org.apache.hadoop.conf.Configuration());
			rc = ks.run(args1);
			NUnit.Framework.Assert.AreEqual(1, rc);
			NUnit.Framework.Assert.IsTrue(outContent.ToString().contains("key1 has not been created."
				));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testInvalidCipher()
		{
			string[] args1 = new string[] { "create", "key1", "-cipher", "LJM", "-provider", 
				jceksProvider };
			int rc = 0;
			org.apache.hadoop.crypto.key.KeyShell ks = new org.apache.hadoop.crypto.key.KeyShell
				();
			ks.setConf(new org.apache.hadoop.conf.Configuration());
			rc = ks.run(args1);
			NUnit.Framework.Assert.AreEqual(1, rc);
			NUnit.Framework.Assert.IsTrue(outContent.ToString().contains("key1 has not been created."
				));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testInvalidProvider()
		{
			string[] args1 = new string[] { "create", "key1", "-cipher", "AES", "-provider", 
				"sdff://file/tmp/keystore.jceks" };
			int rc = 0;
			org.apache.hadoop.crypto.key.KeyShell ks = new org.apache.hadoop.crypto.key.KeyShell
				();
			ks.setConf(new org.apache.hadoop.conf.Configuration());
			rc = ks.run(args1);
			NUnit.Framework.Assert.AreEqual(1, rc);
			NUnit.Framework.Assert.IsTrue(outContent.ToString().contains("There are no valid "
				 + "KeyProviders configured."));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testTransientProviderWarning()
		{
			string[] args1 = new string[] { "create", "key1", "-cipher", "AES", "-provider", 
				"user:///" };
			int rc = 0;
			org.apache.hadoop.crypto.key.KeyShell ks = new org.apache.hadoop.crypto.key.KeyShell
				();
			ks.setConf(new org.apache.hadoop.conf.Configuration());
			rc = ks.run(args1);
			NUnit.Framework.Assert.AreEqual(0, rc);
			NUnit.Framework.Assert.IsTrue(outContent.ToString().contains("WARNING: you are modifying a "
				 + "transient provider."));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testTransientProviderOnlyConfig()
		{
			string[] args1 = new string[] { "create", "key1" };
			int rc = 0;
			org.apache.hadoop.crypto.key.KeyShell ks = new org.apache.hadoop.crypto.key.KeyShell
				();
			org.apache.hadoop.conf.Configuration config = new org.apache.hadoop.conf.Configuration
				();
			config.set(org.apache.hadoop.crypto.key.KeyProviderFactory.KEY_PROVIDER_PATH, "user:///"
				);
			ks.setConf(config);
			rc = ks.run(args1);
			NUnit.Framework.Assert.AreEqual(1, rc);
			NUnit.Framework.Assert.IsTrue(outContent.ToString().contains("There are no valid "
				 + "KeyProviders configured."));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testFullCipher()
		{
			string keyName = "key1";
			string[] args1 = new string[] { "create", keyName, "-cipher", "AES/CBC/pkcs5Padding"
				, "-provider", jceksProvider };
			int rc = 0;
			org.apache.hadoop.crypto.key.KeyShell ks = new org.apache.hadoop.crypto.key.KeyShell
				();
			ks.setConf(new org.apache.hadoop.conf.Configuration());
			rc = ks.run(args1);
			NUnit.Framework.Assert.AreEqual(0, rc);
			NUnit.Framework.Assert.IsTrue(outContent.ToString().contains(keyName + " has been "
				 + "successfully created"));
			deleteKey(ks, keyName);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testAttributes()
		{
			int rc;
			org.apache.hadoop.crypto.key.KeyShell ks = new org.apache.hadoop.crypto.key.KeyShell
				();
			ks.setConf(new org.apache.hadoop.conf.Configuration());
			/* Simple creation test */
			string[] args1 = new string[] { "create", "keyattr1", "-provider", jceksProvider, 
				"-attr", "foo=bar" };
			rc = ks.run(args1);
			NUnit.Framework.Assert.AreEqual(0, rc);
			NUnit.Framework.Assert.IsTrue(outContent.ToString().contains("keyattr1 has been "
				 + "successfully created"));
			/* ...and list to see that we have the attr */
			string listOut = listKeys(ks, true);
			NUnit.Framework.Assert.IsTrue(listOut.contains("keyattr1"));
			NUnit.Framework.Assert.IsTrue(listOut.contains("attributes: [foo=bar]"));
			/* Negative tests: no attribute */
			outContent.reset();
			string[] args2 = new string[] { "create", "keyattr2", "-provider", jceksProvider, 
				"-attr", "=bar" };
			rc = ks.run(args2);
			NUnit.Framework.Assert.AreEqual(1, rc);
			/* Not in attribute = value form */
			outContent.reset();
			args2[5] = "foo";
			rc = ks.run(args2);
			NUnit.Framework.Assert.AreEqual(1, rc);
			/* No attribute or value */
			outContent.reset();
			args2[5] = "=";
			rc = ks.run(args2);
			NUnit.Framework.Assert.AreEqual(1, rc);
			/* Legal: attribute is a, value is b=c */
			outContent.reset();
			args2[5] = "a=b=c";
			rc = ks.run(args2);
			NUnit.Framework.Assert.AreEqual(0, rc);
			listOut = listKeys(ks, true);
			NUnit.Framework.Assert.IsTrue(listOut.contains("keyattr2"));
			NUnit.Framework.Assert.IsTrue(listOut.contains("attributes: [a=b=c]"));
			/* Test several attrs together... */
			outContent.reset();
			string[] args3 = new string[] { "create", "keyattr3", "-provider", jceksProvider, 
				"-attr", "foo = bar", "-attr", " glarch =baz  ", "-attr", "abc=def" };
			rc = ks.run(args3);
			NUnit.Framework.Assert.AreEqual(0, rc);
			/* ...and list to ensure they're there. */
			listOut = listKeys(ks, true);
			NUnit.Framework.Assert.IsTrue(listOut.contains("keyattr3"));
			NUnit.Framework.Assert.IsTrue(listOut.contains("[foo=bar]"));
			NUnit.Framework.Assert.IsTrue(listOut.contains("[glarch=baz]"));
			NUnit.Framework.Assert.IsTrue(listOut.contains("[abc=def]"));
			/* Negative test - repeated attributes should fail */
			outContent.reset();
			string[] args4 = new string[] { "create", "keyattr4", "-provider", jceksProvider, 
				"-attr", "foo=bar", "-attr", "foo=glarch" };
			rc = ks.run(args4);
			NUnit.Framework.Assert.AreEqual(1, rc);
			/* Clean up to be a good citizen */
			deleteKey(ks, "keyattr1");
			deleteKey(ks, "keyattr2");
			deleteKey(ks, "keyattr3");
		}
	}
}
