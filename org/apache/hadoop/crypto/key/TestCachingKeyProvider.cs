using Sharpen;

namespace org.apache.hadoop.crypto.key
{
	public class TestCachingKeyProvider
	{
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testCurrentKey()
		{
			org.apache.hadoop.crypto.key.KeyProvider.KeyVersion mockKey = org.mockito.Mockito
				.mock<org.apache.hadoop.crypto.key.KeyProvider.KeyVersion>();
			org.apache.hadoop.crypto.key.KeyProvider mockProv = org.mockito.Mockito.mock<org.apache.hadoop.crypto.key.KeyProvider
				>();
			org.mockito.Mockito.when(mockProv.getCurrentKey(org.mockito.Mockito.eq("k1"))).thenReturn
				(mockKey);
			org.mockito.Mockito.when(mockProv.getCurrentKey(org.mockito.Mockito.eq("k2"))).thenReturn
				(null);
			org.mockito.Mockito.when(mockProv.getConf()).thenReturn(new org.apache.hadoop.conf.Configuration
				());
			org.apache.hadoop.crypto.key.KeyProvider cache = new org.apache.hadoop.crypto.key.CachingKeyProvider
				(mockProv, 100, 100);
			// asserting caching
			NUnit.Framework.Assert.AreEqual(mockKey, cache.getCurrentKey("k1"));
			org.mockito.Mockito.verify(mockProv, org.mockito.Mockito.times(1)).getCurrentKey(
				org.mockito.Mockito.eq("k1"));
			NUnit.Framework.Assert.AreEqual(mockKey, cache.getCurrentKey("k1"));
			org.mockito.Mockito.verify(mockProv, org.mockito.Mockito.times(1)).getCurrentKey(
				org.mockito.Mockito.eq("k1"));
			java.lang.Thread.sleep(1200);
			NUnit.Framework.Assert.AreEqual(mockKey, cache.getCurrentKey("k1"));
			org.mockito.Mockito.verify(mockProv, org.mockito.Mockito.times(2)).getCurrentKey(
				org.mockito.Mockito.eq("k1"));
			// asserting no caching when key is not known
			cache = new org.apache.hadoop.crypto.key.CachingKeyProvider(mockProv, 100, 100);
			NUnit.Framework.Assert.AreEqual(null, cache.getCurrentKey("k2"));
			org.mockito.Mockito.verify(mockProv, org.mockito.Mockito.times(1)).getCurrentKey(
				org.mockito.Mockito.eq("k2"));
			NUnit.Framework.Assert.AreEqual(null, cache.getCurrentKey("k2"));
			org.mockito.Mockito.verify(mockProv, org.mockito.Mockito.times(2)).getCurrentKey(
				org.mockito.Mockito.eq("k2"));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testKeyVersion()
		{
			org.apache.hadoop.crypto.key.KeyProvider.KeyVersion mockKey = org.mockito.Mockito
				.mock<org.apache.hadoop.crypto.key.KeyProvider.KeyVersion>();
			org.apache.hadoop.crypto.key.KeyProvider mockProv = org.mockito.Mockito.mock<org.apache.hadoop.crypto.key.KeyProvider
				>();
			org.mockito.Mockito.when(mockProv.getKeyVersion(org.mockito.Mockito.eq("k1@0"))).
				thenReturn(mockKey);
			org.mockito.Mockito.when(mockProv.getKeyVersion(org.mockito.Mockito.eq("k2@0"))).
				thenReturn(null);
			org.mockito.Mockito.when(mockProv.getConf()).thenReturn(new org.apache.hadoop.conf.Configuration
				());
			org.apache.hadoop.crypto.key.KeyProvider cache = new org.apache.hadoop.crypto.key.CachingKeyProvider
				(mockProv, 100, 100);
			// asserting caching
			NUnit.Framework.Assert.AreEqual(mockKey, cache.getKeyVersion("k1@0"));
			org.mockito.Mockito.verify(mockProv, org.mockito.Mockito.times(1)).getKeyVersion(
				org.mockito.Mockito.eq("k1@0"));
			NUnit.Framework.Assert.AreEqual(mockKey, cache.getKeyVersion("k1@0"));
			org.mockito.Mockito.verify(mockProv, org.mockito.Mockito.times(1)).getKeyVersion(
				org.mockito.Mockito.eq("k1@0"));
			java.lang.Thread.sleep(200);
			NUnit.Framework.Assert.AreEqual(mockKey, cache.getKeyVersion("k1@0"));
			org.mockito.Mockito.verify(mockProv, org.mockito.Mockito.times(2)).getKeyVersion(
				org.mockito.Mockito.eq("k1@0"));
			// asserting no caching when key is not known
			cache = new org.apache.hadoop.crypto.key.CachingKeyProvider(mockProv, 100, 100);
			NUnit.Framework.Assert.AreEqual(null, cache.getKeyVersion("k2@0"));
			org.mockito.Mockito.verify(mockProv, org.mockito.Mockito.times(1)).getKeyVersion(
				org.mockito.Mockito.eq("k2@0"));
			NUnit.Framework.Assert.AreEqual(null, cache.getKeyVersion("k2@0"));
			org.mockito.Mockito.verify(mockProv, org.mockito.Mockito.times(2)).getKeyVersion(
				org.mockito.Mockito.eq("k2@0"));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testMetadata()
		{
			org.apache.hadoop.crypto.key.KeyProvider.Metadata mockMeta = org.mockito.Mockito.
				mock<org.apache.hadoop.crypto.key.KeyProvider.Metadata>();
			org.apache.hadoop.crypto.key.KeyProvider mockProv = org.mockito.Mockito.mock<org.apache.hadoop.crypto.key.KeyProvider
				>();
			org.mockito.Mockito.when(mockProv.getMetadata(org.mockito.Mockito.eq("k1"))).thenReturn
				(mockMeta);
			org.mockito.Mockito.when(mockProv.getMetadata(org.mockito.Mockito.eq("k2"))).thenReturn
				(null);
			org.mockito.Mockito.when(mockProv.getConf()).thenReturn(new org.apache.hadoop.conf.Configuration
				());
			org.apache.hadoop.crypto.key.KeyProvider cache = new org.apache.hadoop.crypto.key.CachingKeyProvider
				(mockProv, 100, 100);
			// asserting caching
			NUnit.Framework.Assert.AreEqual(mockMeta, cache.getMetadata("k1"));
			org.mockito.Mockito.verify(mockProv, org.mockito.Mockito.times(1)).getMetadata(org.mockito.Mockito
				.eq("k1"));
			NUnit.Framework.Assert.AreEqual(mockMeta, cache.getMetadata("k1"));
			org.mockito.Mockito.verify(mockProv, org.mockito.Mockito.times(1)).getMetadata(org.mockito.Mockito
				.eq("k1"));
			java.lang.Thread.sleep(200);
			NUnit.Framework.Assert.AreEqual(mockMeta, cache.getMetadata("k1"));
			org.mockito.Mockito.verify(mockProv, org.mockito.Mockito.times(2)).getMetadata(org.mockito.Mockito
				.eq("k1"));
			// asserting no caching when key is not known
			cache = new org.apache.hadoop.crypto.key.CachingKeyProvider(mockProv, 100, 100);
			NUnit.Framework.Assert.AreEqual(null, cache.getMetadata("k2"));
			org.mockito.Mockito.verify(mockProv, org.mockito.Mockito.times(1)).getMetadata(org.mockito.Mockito
				.eq("k2"));
			NUnit.Framework.Assert.AreEqual(null, cache.getMetadata("k2"));
			org.mockito.Mockito.verify(mockProv, org.mockito.Mockito.times(2)).getMetadata(org.mockito.Mockito
				.eq("k2"));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testRollNewVersion()
		{
			org.apache.hadoop.crypto.key.KeyProvider.KeyVersion mockKey = org.mockito.Mockito
				.mock<org.apache.hadoop.crypto.key.KeyProvider.KeyVersion>();
			org.apache.hadoop.crypto.key.KeyProvider mockProv = org.mockito.Mockito.mock<org.apache.hadoop.crypto.key.KeyProvider
				>();
			org.mockito.Mockito.when(mockProv.getCurrentKey(org.mockito.Mockito.eq("k1"))).thenReturn
				(mockKey);
			org.mockito.Mockito.when(mockProv.getConf()).thenReturn(new org.apache.hadoop.conf.Configuration
				());
			org.apache.hadoop.crypto.key.KeyProvider cache = new org.apache.hadoop.crypto.key.CachingKeyProvider
				(mockProv, 100, 100);
			NUnit.Framework.Assert.AreEqual(mockKey, cache.getCurrentKey("k1"));
			org.mockito.Mockito.verify(mockProv, org.mockito.Mockito.times(1)).getCurrentKey(
				org.mockito.Mockito.eq("k1"));
			cache.rollNewVersion("k1");
			// asserting the cache is purged
			NUnit.Framework.Assert.AreEqual(mockKey, cache.getCurrentKey("k1"));
			org.mockito.Mockito.verify(mockProv, org.mockito.Mockito.times(2)).getCurrentKey(
				org.mockito.Mockito.eq("k1"));
			cache.rollNewVersion("k1", new byte[0]);
			NUnit.Framework.Assert.AreEqual(mockKey, cache.getCurrentKey("k1"));
			org.mockito.Mockito.verify(mockProv, org.mockito.Mockito.times(3)).getCurrentKey(
				org.mockito.Mockito.eq("k1"));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testDeleteKey()
		{
			org.apache.hadoop.crypto.key.KeyProvider.KeyVersion mockKey = org.mockito.Mockito
				.mock<org.apache.hadoop.crypto.key.KeyProvider.KeyVersion>();
			org.apache.hadoop.crypto.key.KeyProvider mockProv = org.mockito.Mockito.mock<org.apache.hadoop.crypto.key.KeyProvider
				>();
			org.mockito.Mockito.when(mockProv.getCurrentKey(org.mockito.Mockito.eq("k1"))).thenReturn
				(mockKey);
			org.mockito.Mockito.when(mockProv.getKeyVersion(org.mockito.Mockito.eq("k1@0"))).
				thenReturn(mockKey);
			org.mockito.Mockito.when(mockProv.getMetadata(org.mockito.Mockito.eq("k1"))).thenReturn
				(new org.apache.hadoop.crypto.key.kms.KMSClientProvider.KMSMetadata("c", 0, "l", 
				null, new System.DateTime(), 1));
			org.mockito.Mockito.when(mockProv.getConf()).thenReturn(new org.apache.hadoop.conf.Configuration
				());
			org.apache.hadoop.crypto.key.KeyProvider cache = new org.apache.hadoop.crypto.key.CachingKeyProvider
				(mockProv, 100, 100);
			NUnit.Framework.Assert.AreEqual(mockKey, cache.getCurrentKey("k1"));
			org.mockito.Mockito.verify(mockProv, org.mockito.Mockito.times(1)).getCurrentKey(
				org.mockito.Mockito.eq("k1"));
			NUnit.Framework.Assert.AreEqual(mockKey, cache.getKeyVersion("k1@0"));
			org.mockito.Mockito.verify(mockProv, org.mockito.Mockito.times(1)).getKeyVersion(
				org.mockito.Mockito.eq("k1@0"));
			cache.deleteKey("k1");
			// asserting the cache is purged
			NUnit.Framework.Assert.AreEqual(mockKey, cache.getCurrentKey("k1"));
			org.mockito.Mockito.verify(mockProv, org.mockito.Mockito.times(2)).getCurrentKey(
				org.mockito.Mockito.eq("k1"));
			NUnit.Framework.Assert.AreEqual(mockKey, cache.getKeyVersion("k1@0"));
			org.mockito.Mockito.verify(mockProv, org.mockito.Mockito.times(2)).getKeyVersion(
				org.mockito.Mockito.eq("k1@0"));
		}
	}
}
