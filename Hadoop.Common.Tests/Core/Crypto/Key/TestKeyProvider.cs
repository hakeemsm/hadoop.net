using System;
using System.Collections.Generic;
using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Security;
using Sharpen;

namespace Org.Apache.Hadoop.Crypto.Key
{
	public class TestKeyProvider
	{
		private const string Cipher = "AES";

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestBuildVersionName()
		{
			NUnit.Framework.Assert.AreEqual("/a/b@3", KeyProvider.BuildVersionName("/a/b", 3)
				);
			NUnit.Framework.Assert.AreEqual("/aaa@12", KeyProvider.BuildVersionName("/aaa", 12
				));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestParseVersionName()
		{
			NUnit.Framework.Assert.AreEqual("/a/b", KeyProvider.GetBaseName("/a/b@3"));
			NUnit.Framework.Assert.AreEqual("/aaa", KeyProvider.GetBaseName("/aaa@112"));
			try
			{
				KeyProvider.GetBaseName("no-slashes");
				NUnit.Framework.Assert.IsTrue("should have thrown", false);
			}
			catch (IOException)
			{
				NUnit.Framework.Assert.IsTrue(true);
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestKeyMaterial()
		{
			byte[] key1 = new byte[] { 1, 2, 3, 4 };
			KeyProvider.KeyVersion obj = new KeyProvider.KeyVersion("key1", "key1@1", key1);
			NUnit.Framework.Assert.AreEqual("key1@1", obj.GetVersionName());
			Assert.AssertArrayEquals(new byte[] { 1, 2, 3, 4 }, obj.GetMaterial());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestMetadata()
		{
			//Metadata without description
			DateFormat format = new SimpleDateFormat("y/m/d");
			DateTime date = format.Parse("2013/12/25");
			KeyProvider.Metadata meta = new KeyProvider.Metadata("myCipher", 100, null, null, 
				date, 123);
			NUnit.Framework.Assert.AreEqual("myCipher", meta.GetCipher());
			NUnit.Framework.Assert.AreEqual(100, meta.GetBitLength());
			NUnit.Framework.Assert.IsNull(meta.GetDescription());
			NUnit.Framework.Assert.AreEqual(date, meta.GetCreated());
			NUnit.Framework.Assert.AreEqual(123, meta.GetVersions());
			KeyProvider.Metadata second = new KeyProvider.Metadata(meta.Serialize());
			NUnit.Framework.Assert.AreEqual(meta.GetCipher(), second.GetCipher());
			NUnit.Framework.Assert.AreEqual(meta.GetBitLength(), second.GetBitLength());
			NUnit.Framework.Assert.IsNull(second.GetDescription());
			NUnit.Framework.Assert.IsTrue(second.GetAttributes().IsEmpty());
			NUnit.Framework.Assert.AreEqual(meta.GetCreated(), second.GetCreated());
			NUnit.Framework.Assert.AreEqual(meta.GetVersions(), second.GetVersions());
			int newVersion = second.AddVersion();
			NUnit.Framework.Assert.AreEqual(123, newVersion);
			NUnit.Framework.Assert.AreEqual(124, second.GetVersions());
			NUnit.Framework.Assert.AreEqual(123, meta.GetVersions());
			//Metadata with description
			format = new SimpleDateFormat("y/m/d");
			date = format.Parse("2013/12/25");
			IDictionary<string, string> attributes = new Dictionary<string, string>();
			attributes["a"] = "A";
			meta = new KeyProvider.Metadata("myCipher", 100, "description", attributes, date, 
				123);
			NUnit.Framework.Assert.AreEqual("myCipher", meta.GetCipher());
			NUnit.Framework.Assert.AreEqual(100, meta.GetBitLength());
			NUnit.Framework.Assert.AreEqual("description", meta.GetDescription());
			NUnit.Framework.Assert.AreEqual(attributes, meta.GetAttributes());
			NUnit.Framework.Assert.AreEqual(date, meta.GetCreated());
			NUnit.Framework.Assert.AreEqual(123, meta.GetVersions());
			second = new KeyProvider.Metadata(meta.Serialize());
			NUnit.Framework.Assert.AreEqual(meta.GetCipher(), second.GetCipher());
			NUnit.Framework.Assert.AreEqual(meta.GetBitLength(), second.GetBitLength());
			NUnit.Framework.Assert.AreEqual(meta.GetDescription(), second.GetDescription());
			NUnit.Framework.Assert.AreEqual(meta.GetAttributes(), second.GetAttributes());
			NUnit.Framework.Assert.AreEqual(meta.GetCreated(), second.GetCreated());
			NUnit.Framework.Assert.AreEqual(meta.GetVersions(), second.GetVersions());
			newVersion = second.AddVersion();
			NUnit.Framework.Assert.AreEqual(123, newVersion);
			NUnit.Framework.Assert.AreEqual(124, second.GetVersions());
			NUnit.Framework.Assert.AreEqual(123, meta.GetVersions());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestOptions()
		{
			Configuration conf = new Configuration();
			conf.Set(KeyProvider.DefaultCipherName, "myCipher");
			conf.SetInt(KeyProvider.DefaultBitlengthName, 512);
			IDictionary<string, string> attributes = new Dictionary<string, string>();
			attributes["a"] = "A";
			KeyProvider.Options options = KeyProvider.Options(conf);
			NUnit.Framework.Assert.AreEqual("myCipher", options.GetCipher());
			NUnit.Framework.Assert.AreEqual(512, options.GetBitLength());
			options.SetCipher("yourCipher");
			options.SetDescription("description");
			options.SetAttributes(attributes);
			options.SetBitLength(128);
			NUnit.Framework.Assert.AreEqual("yourCipher", options.GetCipher());
			NUnit.Framework.Assert.AreEqual(128, options.GetBitLength());
			NUnit.Framework.Assert.AreEqual("description", options.GetDescription());
			NUnit.Framework.Assert.AreEqual(attributes, options.GetAttributes());
			options = KeyProvider.Options(new Configuration());
			NUnit.Framework.Assert.AreEqual(KeyProvider.DefaultCipher, options.GetCipher());
			NUnit.Framework.Assert.AreEqual(KeyProvider.DefaultBitlength, options.GetBitLength
				());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestUnnestUri()
		{
			NUnit.Framework.Assert.AreEqual(new Path("hdfs://nn.example.com/my/path"), ProviderUtils
				.UnnestUri(new URI("myscheme://hdfs@nn.example.com/my/path")));
			NUnit.Framework.Assert.AreEqual(new Path("hdfs://nn/my/path?foo=bar&baz=bat#yyy")
				, ProviderUtils.UnnestUri(new URI("myscheme://hdfs@nn/my/path?foo=bar&baz=bat#yyy"
				)));
			NUnit.Framework.Assert.AreEqual(new Path("inner://hdfs@nn1.example.com/my/path"), 
				ProviderUtils.UnnestUri(new URI("outer://inner@hdfs@nn1.example.com/my/path")));
			NUnit.Framework.Assert.AreEqual(new Path("user:///"), ProviderUtils.UnnestUri(new 
				URI("outer://user/")));
		}

		private class MyKeyProvider : KeyProvider
		{
			private string algorithm;

			private int size;

			private byte[] material;

			public MyKeyProvider(Configuration conf)
				: base(conf)
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public override KeyProvider.KeyVersion GetKeyVersion(string versionName)
			{
				return null;
			}

			/// <exception cref="System.IO.IOException"/>
			public override IList<string> GetKeys()
			{
				return null;
			}

			/// <exception cref="System.IO.IOException"/>
			public override IList<KeyProvider.KeyVersion> GetKeyVersions(string name)
			{
				return null;
			}

			/// <exception cref="System.IO.IOException"/>
			public override KeyProvider.Metadata GetMetadata(string name)
			{
				return new KeyProvider.Metadata(Cipher, 128, "description", null, new DateTime(), 
					0);
			}

			/// <exception cref="System.IO.IOException"/>
			public override KeyProvider.KeyVersion CreateKey(string name, byte[] material, KeyProvider.Options
				 options)
			{
				this.material = material;
				return null;
			}

			/// <exception cref="System.IO.IOException"/>
			public override void DeleteKey(string name)
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public override KeyProvider.KeyVersion RollNewVersion(string name, byte[] material
				)
			{
				this.material = material;
				return null;
			}

			/// <exception cref="System.IO.IOException"/>
			public override void Flush()
			{
			}

			/// <exception cref="Sharpen.NoSuchAlgorithmException"/>
			protected internal override byte[] GenerateKey(int size, string algorithm)
			{
				this.size = size;
				this.algorithm = algorithm;
				return base.GenerateKey(size, algorithm);
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestMaterialGeneration()
		{
			TestKeyProvider.MyKeyProvider kp = new TestKeyProvider.MyKeyProvider(new Configuration
				());
			KeyProvider.Options options = new KeyProvider.Options(new Configuration());
			options.SetCipher(Cipher);
			options.SetBitLength(128);
			kp.CreateKey("hello", options);
			NUnit.Framework.Assert.AreEqual(128, kp.size);
			NUnit.Framework.Assert.AreEqual(Cipher, kp.algorithm);
			NUnit.Framework.Assert.IsNotNull(kp.material);
			kp = new TestKeyProvider.MyKeyProvider(new Configuration());
			kp.RollNewVersion("hello");
			NUnit.Framework.Assert.AreEqual(128, kp.size);
			NUnit.Framework.Assert.AreEqual(Cipher, kp.algorithm);
			NUnit.Framework.Assert.IsNotNull(kp.material);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestConfiguration()
		{
			Configuration conf = new Configuration(false);
			conf.Set("a", "A");
			TestKeyProvider.MyKeyProvider kp = new TestKeyProvider.MyKeyProvider(conf);
			NUnit.Framework.Assert.AreEqual("A", kp.GetConf().Get("a"));
		}
	}
}
