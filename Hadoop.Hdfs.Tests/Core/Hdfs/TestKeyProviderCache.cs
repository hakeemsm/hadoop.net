using System.Collections.Generic;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Crypto.Key;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	public class TestKeyProviderCache
	{
		public class DummyKeyProvider : KeyProvider
		{
			public DummyKeyProvider(Configuration conf)
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
				return null;
			}

			/// <exception cref="System.IO.IOException"/>
			public override KeyProvider.KeyVersion CreateKey(string name, byte[] material, KeyProvider.Options
				 options)
			{
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
				return null;
			}

			/// <exception cref="System.IO.IOException"/>
			public override void Flush()
			{
			}
		}

		public class Factory : KeyProviderFactory
		{
			/// <exception cref="System.IO.IOException"/>
			public override KeyProvider CreateProvider(URI providerName, Configuration conf)
			{
				if ("dummy".Equals(providerName.GetScheme()))
				{
					return new TestKeyProviderCache.DummyKeyProvider(conf);
				}
				return null;
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestCache()
		{
			KeyProviderCache kpCache = new KeyProviderCache(10000);
			Configuration conf = new Configuration();
			conf.Set(DFSConfigKeys.DfsEncryptionKeyProviderUri, "dummy://foo:bar@test_provider1"
				);
			KeyProvider keyProvider1 = kpCache.Get(conf);
			NUnit.Framework.Assert.IsNotNull("Returned Key Provider is null !!", keyProvider1
				);
			conf.Set(DFSConfigKeys.DfsEncryptionKeyProviderUri, "dummy://foo:bar@test_provider1"
				);
			KeyProvider keyProvider2 = kpCache.Get(conf);
			NUnit.Framework.Assert.IsTrue("Different KeyProviders returned !!", keyProvider1 
				== keyProvider2);
			conf.Set(DFSConfigKeys.DfsEncryptionKeyProviderUri, "dummy://test_provider3");
			KeyProvider keyProvider3 = kpCache.Get(conf);
			NUnit.Framework.Assert.IsFalse("Same KeyProviders returned !!", keyProvider1 == keyProvider3
				);
			conf.Set(DFSConfigKeys.DfsEncryptionKeyProviderUri, "dummy://hello:there@test_provider1"
				);
			KeyProvider keyProvider4 = kpCache.Get(conf);
			NUnit.Framework.Assert.IsFalse("Same KeyProviders returned !!", keyProvider1 == keyProvider4
				);
		}
	}
}
