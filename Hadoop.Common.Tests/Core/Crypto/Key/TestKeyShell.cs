using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Sharpen;

namespace Org.Apache.Hadoop.Crypto.Key
{
	public class TestKeyShell
	{
		private readonly ByteArrayOutputStream outContent = new ByteArrayOutputStream();

		private readonly ByteArrayOutputStream errContent = new ByteArrayOutputStream();

		private TextWriter initialStdOut;

		private TextWriter initialStdErr;

		private string jceksProvider;

		/* The default JCEKS provider - for testing purposes */
		/// <exception cref="System.Exception"/>
		[SetUp]
		public virtual void Setup()
		{
			outContent.Reset();
			errContent.Reset();
			FilePath tmpDir = new FilePath(Runtime.GetProperty("test.build.data", "target"), 
				UUID.RandomUUID().ToString());
			if (!tmpDir.Mkdirs())
			{
				throw new IOException("Unable to create " + tmpDir);
			}
			Path jksPath = new Path(tmpDir.ToString(), "keystore.jceks");
			jceksProvider = "jceks://file" + jksPath.ToUri();
			initialStdOut = System.Console.Out;
			initialStdErr = System.Console.Error;
			Runtime.SetOut(new TextWriter(outContent));
			Runtime.SetErr(new TextWriter(errContent));
		}

		/// <exception cref="System.Exception"/>
		[TearDown]
		public virtual void CleanUp()
		{
			Runtime.SetOut(initialStdOut);
			Runtime.SetErr(initialStdErr);
		}

		/// <summary>Delete a key from the default jceksProvider</summary>
		/// <param name="ks">The KeyShell instance</param>
		/// <param name="keyName">The key to delete</param>
		/// <exception cref="System.Exception"/>
		private void DeleteKey(KeyShell ks, string keyName)
		{
			int rc;
			outContent.Reset();
			string[] delArgs = new string[] { "delete", keyName, "-f", "-provider", jceksProvider
				 };
			rc = ks.Run(delArgs);
			Assert.Equal(0, rc);
			Assert.True(outContent.ToString().Contains(keyName + " has been "
				 + "successfully deleted."));
		}

		/// <summary>Lists the keys in the jceksProvider</summary>
		/// <param name="ks">The KeyShell instance</param>
		/// <param name="wantMetadata">True if you want metadata returned with the keys</param>
		/// <returns>The output from the "list" call</returns>
		/// <exception cref="System.Exception"/>
		private string ListKeys(KeyShell ks, bool wantMetadata)
		{
			int rc;
			outContent.Reset();
			string[] listArgs = new string[] { "list", "-provider", jceksProvider };
			string[] listArgsM = new string[] { "list", "-metadata", "-provider", jceksProvider
				 };
			rc = ks.Run(wantMetadata ? listArgsM : listArgs);
			Assert.Equal(0, rc);
			return outContent.ToString();
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestKeySuccessfulKeyLifecycle()
		{
			int rc = 0;
			string keyName = "key1";
			KeyShell ks = new KeyShell();
			ks.SetConf(new Configuration());
			outContent.Reset();
			string[] args1 = new string[] { "create", keyName, "-provider", jceksProvider };
			rc = ks.Run(args1);
			Assert.Equal(0, rc);
			Assert.True(outContent.ToString().Contains(keyName + " has been "
				 + "successfully created"));
			string listOut = ListKeys(ks, false);
			Assert.True(listOut.Contains(keyName));
			listOut = ListKeys(ks, true);
			Assert.True(listOut.Contains(keyName));
			Assert.True(listOut.Contains("description"));
			Assert.True(listOut.Contains("created"));
			outContent.Reset();
			string[] args2 = new string[] { "roll", keyName, "-provider", jceksProvider };
			rc = ks.Run(args2);
			Assert.Equal(0, rc);
			Assert.True(outContent.ToString().Contains("key1 has been successfully "
				 + "rolled."));
			DeleteKey(ks, keyName);
			listOut = ListKeys(ks, false);
			NUnit.Framework.Assert.IsFalse(listOut, listOut.Contains(keyName));
		}

		/* HADOOP-10586 KeyShell didn't allow -description. */
		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestKeySuccessfulCreationWithDescription()
		{
			outContent.Reset();
			string[] args1 = new string[] { "create", "key1", "-provider", jceksProvider, "-description"
				, "someDescription" };
			int rc = 0;
			KeyShell ks = new KeyShell();
			ks.SetConf(new Configuration());
			rc = ks.Run(args1);
			Assert.Equal(0, rc);
			Assert.True(outContent.ToString().Contains("key1 has been successfully "
				 + "created"));
			string listOut = ListKeys(ks, true);
			Assert.True(listOut.Contains("description"));
			Assert.True(listOut.Contains("someDescription"));
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestInvalidKeySize()
		{
			string[] args1 = new string[] { "create", "key1", "-size", "56", "-provider", jceksProvider
				 };
			int rc = 0;
			KeyShell ks = new KeyShell();
			ks.SetConf(new Configuration());
			rc = ks.Run(args1);
			Assert.Equal(1, rc);
			Assert.True(outContent.ToString().Contains("key1 has not been created."
				));
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestInvalidCipher()
		{
			string[] args1 = new string[] { "create", "key1", "-cipher", "LJM", "-provider", 
				jceksProvider };
			int rc = 0;
			KeyShell ks = new KeyShell();
			ks.SetConf(new Configuration());
			rc = ks.Run(args1);
			Assert.Equal(1, rc);
			Assert.True(outContent.ToString().Contains("key1 has not been created."
				));
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestInvalidProvider()
		{
			string[] args1 = new string[] { "create", "key1", "-cipher", "AES", "-provider", 
				"sdff://file/tmp/keystore.jceks" };
			int rc = 0;
			KeyShell ks = new KeyShell();
			ks.SetConf(new Configuration());
			rc = ks.Run(args1);
			Assert.Equal(1, rc);
			Assert.True(outContent.ToString().Contains("There are no valid "
				 + "KeyProviders configured."));
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestTransientProviderWarning()
		{
			string[] args1 = new string[] { "create", "key1", "-cipher", "AES", "-provider", 
				"user:///" };
			int rc = 0;
			KeyShell ks = new KeyShell();
			ks.SetConf(new Configuration());
			rc = ks.Run(args1);
			Assert.Equal(0, rc);
			Assert.True(outContent.ToString().Contains("WARNING: you are modifying a "
				 + "transient provider."));
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestTransientProviderOnlyConfig()
		{
			string[] args1 = new string[] { "create", "key1" };
			int rc = 0;
			KeyShell ks = new KeyShell();
			Configuration config = new Configuration();
			config.Set(KeyProviderFactory.KeyProviderPath, "user:///");
			ks.SetConf(config);
			rc = ks.Run(args1);
			Assert.Equal(1, rc);
			Assert.True(outContent.ToString().Contains("There are no valid "
				 + "KeyProviders configured."));
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestFullCipher()
		{
			string keyName = "key1";
			string[] args1 = new string[] { "create", keyName, "-cipher", "AES/CBC/pkcs5Padding"
				, "-provider", jceksProvider };
			int rc = 0;
			KeyShell ks = new KeyShell();
			ks.SetConf(new Configuration());
			rc = ks.Run(args1);
			Assert.Equal(0, rc);
			Assert.True(outContent.ToString().Contains(keyName + " has been "
				 + "successfully created"));
			DeleteKey(ks, keyName);
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestAttributes()
		{
			int rc;
			KeyShell ks = new KeyShell();
			ks.SetConf(new Configuration());
			/* Simple creation test */
			string[] args1 = new string[] { "create", "keyattr1", "-provider", jceksProvider, 
				"-attr", "foo=bar" };
			rc = ks.Run(args1);
			Assert.Equal(0, rc);
			Assert.True(outContent.ToString().Contains("keyattr1 has been "
				 + "successfully created"));
			/* ...and list to see that we have the attr */
			string listOut = ListKeys(ks, true);
			Assert.True(listOut.Contains("keyattr1"));
			Assert.True(listOut.Contains("attributes: [foo=bar]"));
			/* Negative tests: no attribute */
			outContent.Reset();
			string[] args2 = new string[] { "create", "keyattr2", "-provider", jceksProvider, 
				"-attr", "=bar" };
			rc = ks.Run(args2);
			Assert.Equal(1, rc);
			/* Not in attribute = value form */
			outContent.Reset();
			args2[5] = "foo";
			rc = ks.Run(args2);
			Assert.Equal(1, rc);
			/* No attribute or value */
			outContent.Reset();
			args2[5] = "=";
			rc = ks.Run(args2);
			Assert.Equal(1, rc);
			/* Legal: attribute is a, value is b=c */
			outContent.Reset();
			args2[5] = "a=b=c";
			rc = ks.Run(args2);
			Assert.Equal(0, rc);
			listOut = ListKeys(ks, true);
			Assert.True(listOut.Contains("keyattr2"));
			Assert.True(listOut.Contains("attributes: [a=b=c]"));
			/* Test several attrs together... */
			outContent.Reset();
			string[] args3 = new string[] { "create", "keyattr3", "-provider", jceksProvider, 
				"-attr", "foo = bar", "-attr", " glarch =baz  ", "-attr", "abc=def" };
			rc = ks.Run(args3);
			Assert.Equal(0, rc);
			/* ...and list to ensure they're there. */
			listOut = ListKeys(ks, true);
			Assert.True(listOut.Contains("keyattr3"));
			Assert.True(listOut.Contains("[foo=bar]"));
			Assert.True(listOut.Contains("[glarch=baz]"));
			Assert.True(listOut.Contains("[abc=def]"));
			/* Negative test - repeated attributes should fail */
			outContent.Reset();
			string[] args4 = new string[] { "create", "keyattr4", "-provider", jceksProvider, 
				"-attr", "foo=bar", "-attr", "foo=glarch" };
			rc = ks.Run(args4);
			Assert.Equal(1, rc);
			/* Clean up to be a good citizen */
			DeleteKey(ks, "keyattr1");
			DeleteKey(ks, "keyattr2");
			DeleteKey(ks, "keyattr3");
		}
	}
}
