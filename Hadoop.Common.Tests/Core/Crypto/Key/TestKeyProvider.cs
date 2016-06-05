using System;
using System.Collections.Generic;
using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Security;


namespace Org.Apache.Hadoop.Crypto.Key
{
	public class TestKeyProvider
	{
		private const string Cipher = "AES";

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestBuildVersionName()
		{
			Assert.Equal("/a/b@3", KeyProvider.BuildVersionName("/a/b", 3)
				);
			Assert.Equal("/aaa@12", KeyProvider.BuildVersionName("/aaa", 12
				));
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestParseVersionName()
		{
			Assert.Equal("/a/b", KeyProvider.GetBaseName("/a/b@3"));
			Assert.Equal("/aaa", KeyProvider.GetBaseName("/aaa@112"));
			try
			{
				KeyProvider.GetBaseName("no-slashes");
				Assert.True("should have thrown", false);
			}
			catch (IOException)
			{
				Assert.True(true);
			}
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestKeyMaterial()
		{
			byte[] key1 = new byte[] { 1, 2, 3, 4 };
			KeyProvider.KeyVersion obj = new KeyProvider.KeyVersion("key1", "key1@1", key1);
			Assert.Equal("key1@1", obj.GetVersionName());
			Assert.AssertArrayEquals(new byte[] { 1, 2, 3, 4 }, obj.GetMaterial());
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestMetadata()
		{
			//Metadata without description
			DateFormat format = new SimpleDateFormat("y/m/d");
			DateTime date = format.Parse("2013/12/25");
			KeyProvider.Metadata meta = new KeyProvider.Metadata("myCipher", 100, null, null, 
				date, 123);
			Assert.Equal("myCipher", meta.GetCipher());
			Assert.Equal(100, meta.GetBitLength());
			NUnit.Framework.Assert.IsNull(meta.GetDescription());
			Assert.Equal(date, meta.GetCreated());
			Assert.Equal(123, meta.GetVersions());
			KeyProvider.Metadata second = new KeyProvider.Metadata(meta.Serialize());
			Assert.Equal(meta.GetCipher(), second.GetCipher());
			Assert.Equal(meta.GetBitLength(), second.GetBitLength());
			NUnit.Framework.Assert.IsNull(second.GetDescription());
			Assert.True(second.GetAttributes().IsEmpty());
			Assert.Equal(meta.GetCreated(), second.GetCreated());
			Assert.Equal(meta.GetVersions(), second.GetVersions());
			int newVersion = second.AddVersion();
			Assert.Equal(123, newVersion);
			Assert.Equal(124, second.GetVersions());
			Assert.Equal(123, meta.GetVersions());
			//Metadata with description
			format = new SimpleDateFormat("y/m/d");
			date = format.Parse("2013/12/25");
			IDictionary<string, string> attributes = new Dictionary<string, string>();
			attributes["a"] = "A";
			meta = new KeyProvider.Metadata("myCipher", 100, "description", attributes, date, 
				123);
			Assert.Equal("myCipher", meta.GetCipher());
			Assert.Equal(100, meta.GetBitLength());
			Assert.Equal("description", meta.GetDescription());
			Assert.Equal(attributes, meta.GetAttributes());
			Assert.Equal(date, meta.GetCreated());
			Assert.Equal(123, meta.GetVersions());
			second = new KeyProvider.Metadata(meta.Serialize());
			Assert.Equal(meta.GetCipher(), second.GetCipher());
			Assert.Equal(meta.GetBitLength(), second.GetBitLength());
			Assert.Equal(meta.GetDescription(), second.GetDescription());
			Assert.Equal(meta.GetAttributes(), second.GetAttributes());
			Assert.Equal(meta.GetCreated(), second.GetCreated());
			Assert.Equal(meta.GetVersions(), second.GetVersions());
			newVersion = second.AddVersion();
			Assert.Equal(123, newVersion);
			Assert.Equal(124, second.GetVersions());
			Assert.Equal(123, meta.GetVersions());
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestOptions()
		{
			Configuration conf = new Configuration();
			conf.Set(KeyProvider.DefaultCipherName, "myCipher");
			conf.SetInt(KeyProvider.DefaultBitlengthName, 512);
			IDictionary<string, string> attributes = new Dictionary<string, string>();
			attributes["a"] = "A";
			KeyProvider.Options options = KeyProvider.Options(conf);
			Assert.Equal("myCipher", options.GetCipher());
			Assert.Equal(512, options.GetBitLength());
			options.SetCipher("yourCipher");
			options.SetDescription("description");
			options.SetAttributes(attributes);
			options.SetBitLength(128);
			Assert.Equal("yourCipher", options.GetCipher());
			Assert.Equal(128, options.GetBitLength());
			Assert.Equal("description", options.GetDescription());
			Assert.Equal(attributes, options.GetAttributes());
			options = KeyProvider.Options(new Configuration());
			Assert.Equal(KeyProvider.DefaultCipher, options.GetCipher());
			Assert.Equal(KeyProvider.DefaultBitlength, options.GetBitLength
				());
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestUnnestUri()
		{
			Assert.Equal(new Path("hdfs://nn.example.com/my/path"), ProviderUtils
				.UnnestUri(new URI("myscheme://hdfs@nn.example.com/my/path")));
			Assert.Equal(new Path("hdfs://nn/my/path?foo=bar&baz=bat#yyy")
				, ProviderUtils.UnnestUri(new URI("myscheme://hdfs@nn/my/path?foo=bar&baz=bat#yyy"
				)));
			Assert.Equal(new Path("inner://hdfs@nn1.example.com/my/path"), 
				ProviderUtils.UnnestUri(new URI("outer://inner@hdfs@nn1.example.com/my/path")));
			Assert.Equal(new Path("user:///"), ProviderUtils.UnnestUri(new 
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

			/// <exception cref="NoSuchAlgorithmException"/>
			protected internal override byte[] GenerateKey(int size, string algorithm)
			{
				this.size = size;
				this.algorithm = algorithm;
				return base.GenerateKey(size, algorithm);
			}
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestMaterialGeneration()
		{
			TestKeyProvider.MyKeyProvider kp = new TestKeyProvider.MyKeyProvider(new Configuration
				());
			KeyProvider.Options options = new KeyProvider.Options(new Configuration());
			options.SetCipher(Cipher);
			options.SetBitLength(128);
			kp.CreateKey("hello", options);
			Assert.Equal(128, kp.size);
			Assert.Equal(Cipher, kp.algorithm);
			NUnit.Framework.Assert.IsNotNull(kp.material);
			kp = new TestKeyProvider.MyKeyProvider(new Configuration());
			kp.RollNewVersion("hello");
			Assert.Equal(128, kp.size);
			Assert.Equal(Cipher, kp.algorithm);
			NUnit.Framework.Assert.IsNotNull(kp.material);
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestConfiguration()
		{
			Configuration conf = new Configuration(false);
			conf.Set("a", "A");
			TestKeyProvider.MyKeyProvider kp = new TestKeyProvider.MyKeyProvider(conf);
			Assert.Equal("A", kp.GetConf().Get("a"));
		}
	}
}
