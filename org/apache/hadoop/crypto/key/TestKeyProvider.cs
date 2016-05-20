using Sharpen;

namespace org.apache.hadoop.crypto.key
{
	public class TestKeyProvider
	{
		private const string CIPHER = "AES";

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testBuildVersionName()
		{
			NUnit.Framework.Assert.AreEqual("/a/b@3", org.apache.hadoop.crypto.key.KeyProvider
				.buildVersionName("/a/b", 3));
			NUnit.Framework.Assert.AreEqual("/aaa@12", org.apache.hadoop.crypto.key.KeyProvider
				.buildVersionName("/aaa", 12));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testParseVersionName()
		{
			NUnit.Framework.Assert.AreEqual("/a/b", org.apache.hadoop.crypto.key.KeyProvider.
				getBaseName("/a/b@3"));
			NUnit.Framework.Assert.AreEqual("/aaa", org.apache.hadoop.crypto.key.KeyProvider.
				getBaseName("/aaa@112"));
			try
			{
				org.apache.hadoop.crypto.key.KeyProvider.getBaseName("no-slashes");
				NUnit.Framework.Assert.IsTrue("should have thrown", false);
			}
			catch (System.IO.IOException)
			{
				NUnit.Framework.Assert.IsTrue(true);
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testKeyMaterial()
		{
			byte[] key1 = new byte[] { 1, 2, 3, 4 };
			org.apache.hadoop.crypto.key.KeyProvider.KeyVersion obj = new org.apache.hadoop.crypto.key.KeyProvider.KeyVersion
				("key1", "key1@1", key1);
			NUnit.Framework.Assert.AreEqual("key1@1", obj.getVersionName());
			NUnit.Framework.Assert.assertArrayEquals(new byte[] { 1, 2, 3, 4 }, obj.getMaterial
				());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testMetadata()
		{
			//Metadata without description
			java.text.DateFormat format = new java.text.SimpleDateFormat("y/m/d");
			System.DateTime date = format.parse("2013/12/25");
			org.apache.hadoop.crypto.key.KeyProvider.Metadata meta = new org.apache.hadoop.crypto.key.KeyProvider.Metadata
				("myCipher", 100, null, null, date, 123);
			NUnit.Framework.Assert.AreEqual("myCipher", meta.getCipher());
			NUnit.Framework.Assert.AreEqual(100, meta.getBitLength());
			NUnit.Framework.Assert.IsNull(meta.getDescription());
			NUnit.Framework.Assert.AreEqual(date, meta.getCreated());
			NUnit.Framework.Assert.AreEqual(123, meta.getVersions());
			org.apache.hadoop.crypto.key.KeyProvider.Metadata second = new org.apache.hadoop.crypto.key.KeyProvider.Metadata
				(meta.serialize());
			NUnit.Framework.Assert.AreEqual(meta.getCipher(), second.getCipher());
			NUnit.Framework.Assert.AreEqual(meta.getBitLength(), second.getBitLength());
			NUnit.Framework.Assert.IsNull(second.getDescription());
			NUnit.Framework.Assert.IsTrue(second.getAttributes().isEmpty());
			NUnit.Framework.Assert.AreEqual(meta.getCreated(), second.getCreated());
			NUnit.Framework.Assert.AreEqual(meta.getVersions(), second.getVersions());
			int newVersion = second.addVersion();
			NUnit.Framework.Assert.AreEqual(123, newVersion);
			NUnit.Framework.Assert.AreEqual(124, second.getVersions());
			NUnit.Framework.Assert.AreEqual(123, meta.getVersions());
			//Metadata with description
			format = new java.text.SimpleDateFormat("y/m/d");
			date = format.parse("2013/12/25");
			System.Collections.Generic.IDictionary<string, string> attributes = new System.Collections.Generic.Dictionary
				<string, string>();
			attributes["a"] = "A";
			meta = new org.apache.hadoop.crypto.key.KeyProvider.Metadata("myCipher", 100, "description"
				, attributes, date, 123);
			NUnit.Framework.Assert.AreEqual("myCipher", meta.getCipher());
			NUnit.Framework.Assert.AreEqual(100, meta.getBitLength());
			NUnit.Framework.Assert.AreEqual("description", meta.getDescription());
			NUnit.Framework.Assert.AreEqual(attributes, meta.getAttributes());
			NUnit.Framework.Assert.AreEqual(date, meta.getCreated());
			NUnit.Framework.Assert.AreEqual(123, meta.getVersions());
			second = new org.apache.hadoop.crypto.key.KeyProvider.Metadata(meta.serialize());
			NUnit.Framework.Assert.AreEqual(meta.getCipher(), second.getCipher());
			NUnit.Framework.Assert.AreEqual(meta.getBitLength(), second.getBitLength());
			NUnit.Framework.Assert.AreEqual(meta.getDescription(), second.getDescription());
			NUnit.Framework.Assert.AreEqual(meta.getAttributes(), second.getAttributes());
			NUnit.Framework.Assert.AreEqual(meta.getCreated(), second.getCreated());
			NUnit.Framework.Assert.AreEqual(meta.getVersions(), second.getVersions());
			newVersion = second.addVersion();
			NUnit.Framework.Assert.AreEqual(123, newVersion);
			NUnit.Framework.Assert.AreEqual(124, second.getVersions());
			NUnit.Framework.Assert.AreEqual(123, meta.getVersions());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testOptions()
		{
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			conf.set(org.apache.hadoop.crypto.key.KeyProvider.DEFAULT_CIPHER_NAME, "myCipher"
				);
			conf.setInt(org.apache.hadoop.crypto.key.KeyProvider.DEFAULT_BITLENGTH_NAME, 512);
			System.Collections.Generic.IDictionary<string, string> attributes = new System.Collections.Generic.Dictionary
				<string, string>();
			attributes["a"] = "A";
			org.apache.hadoop.crypto.key.KeyProvider.Options options = org.apache.hadoop.crypto.key.KeyProvider
				.options(conf);
			NUnit.Framework.Assert.AreEqual("myCipher", options.getCipher());
			NUnit.Framework.Assert.AreEqual(512, options.getBitLength());
			options.setCipher("yourCipher");
			options.setDescription("description");
			options.setAttributes(attributes);
			options.setBitLength(128);
			NUnit.Framework.Assert.AreEqual("yourCipher", options.getCipher());
			NUnit.Framework.Assert.AreEqual(128, options.getBitLength());
			NUnit.Framework.Assert.AreEqual("description", options.getDescription());
			NUnit.Framework.Assert.AreEqual(attributes, options.getAttributes());
			options = org.apache.hadoop.crypto.key.KeyProvider.options(new org.apache.hadoop.conf.Configuration
				());
			NUnit.Framework.Assert.AreEqual(org.apache.hadoop.crypto.key.KeyProvider.DEFAULT_CIPHER
				, options.getCipher());
			NUnit.Framework.Assert.AreEqual(org.apache.hadoop.crypto.key.KeyProvider.DEFAULT_BITLENGTH
				, options.getBitLength());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testUnnestUri()
		{
			NUnit.Framework.Assert.AreEqual(new org.apache.hadoop.fs.Path("hdfs://nn.example.com/my/path"
				), org.apache.hadoop.security.ProviderUtils.unnestUri(new java.net.URI("myscheme://hdfs@nn.example.com/my/path"
				)));
			NUnit.Framework.Assert.AreEqual(new org.apache.hadoop.fs.Path("hdfs://nn/my/path?foo=bar&baz=bat#yyy"
				), org.apache.hadoop.security.ProviderUtils.unnestUri(new java.net.URI("myscheme://hdfs@nn/my/path?foo=bar&baz=bat#yyy"
				)));
			NUnit.Framework.Assert.AreEqual(new org.apache.hadoop.fs.Path("inner://hdfs@nn1.example.com/my/path"
				), org.apache.hadoop.security.ProviderUtils.unnestUri(new java.net.URI("outer://inner@hdfs@nn1.example.com/my/path"
				)));
			NUnit.Framework.Assert.AreEqual(new org.apache.hadoop.fs.Path("user:///"), org.apache.hadoop.security.ProviderUtils
				.unnestUri(new java.net.URI("outer://user/")));
		}

		private class MyKeyProvider : org.apache.hadoop.crypto.key.KeyProvider
		{
			private string algorithm;

			private int size;

			private byte[] material;

			public MyKeyProvider(org.apache.hadoop.conf.Configuration conf)
				: base(conf)
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public override org.apache.hadoop.crypto.key.KeyProvider.KeyVersion getKeyVersion
				(string versionName)
			{
				return null;
			}

			/// <exception cref="System.IO.IOException"/>
			public override System.Collections.Generic.IList<string> getKeys()
			{
				return null;
			}

			/// <exception cref="System.IO.IOException"/>
			public override System.Collections.Generic.IList<org.apache.hadoop.crypto.key.KeyProvider.KeyVersion
				> getKeyVersions(string name)
			{
				return null;
			}

			/// <exception cref="System.IO.IOException"/>
			public override org.apache.hadoop.crypto.key.KeyProvider.Metadata getMetadata(string
				 name)
			{
				return new org.apache.hadoop.crypto.key.KeyProvider.Metadata(CIPHER, 128, "description"
					, null, new System.DateTime(), 0);
			}

			/// <exception cref="System.IO.IOException"/>
			public override org.apache.hadoop.crypto.key.KeyProvider.KeyVersion createKey(string
				 name, byte[] material, org.apache.hadoop.crypto.key.KeyProvider.Options options
				)
			{
				this.material = material;
				return null;
			}

			/// <exception cref="System.IO.IOException"/>
			public override void deleteKey(string name)
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public override org.apache.hadoop.crypto.key.KeyProvider.KeyVersion rollNewVersion
				(string name, byte[] material)
			{
				this.material = material;
				return null;
			}

			/// <exception cref="System.IO.IOException"/>
			public override void flush()
			{
			}

			/// <exception cref="java.security.NoSuchAlgorithmException"/>
			protected internal override byte[] generateKey(int size, string algorithm)
			{
				this.size = size;
				this.algorithm = algorithm;
				return base.generateKey(size, algorithm);
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testMaterialGeneration()
		{
			org.apache.hadoop.crypto.key.TestKeyProvider.MyKeyProvider kp = new org.apache.hadoop.crypto.key.TestKeyProvider.MyKeyProvider
				(new org.apache.hadoop.conf.Configuration());
			org.apache.hadoop.crypto.key.KeyProvider.Options options = new org.apache.hadoop.crypto.key.KeyProvider.Options
				(new org.apache.hadoop.conf.Configuration());
			options.setCipher(CIPHER);
			options.setBitLength(128);
			kp.createKey("hello", options);
			NUnit.Framework.Assert.AreEqual(128, kp.size);
			NUnit.Framework.Assert.AreEqual(CIPHER, kp.algorithm);
			NUnit.Framework.Assert.IsNotNull(kp.material);
			kp = new org.apache.hadoop.crypto.key.TestKeyProvider.MyKeyProvider(new org.apache.hadoop.conf.Configuration
				());
			kp.rollNewVersion("hello");
			NUnit.Framework.Assert.AreEqual(128, kp.size);
			NUnit.Framework.Assert.AreEqual(CIPHER, kp.algorithm);
			NUnit.Framework.Assert.IsNotNull(kp.material);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testConfiguration()
		{
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				(false);
			conf.set("a", "A");
			org.apache.hadoop.crypto.key.TestKeyProvider.MyKeyProvider kp = new org.apache.hadoop.crypto.key.TestKeyProvider.MyKeyProvider
				(conf);
			NUnit.Framework.Assert.AreEqual("A", kp.getConf().get("a"));
		}
	}
}
