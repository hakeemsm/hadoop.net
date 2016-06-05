using System;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Crypto.Key.Kms;
using Sharpen;

namespace Org.Apache.Hadoop.Crypto.Key
{
	public class TestCachingKeyProvider
	{
		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestCurrentKey()
		{
			KeyProvider.KeyVersion mockKey = Org.Mockito.Mockito.Mock<KeyProvider.KeyVersion>
				();
			KeyProvider mockProv = Org.Mockito.Mockito.Mock<KeyProvider>();
			Org.Mockito.Mockito.When(mockProv.GetCurrentKey(Org.Mockito.Mockito.Eq("k1"))).ThenReturn
				(mockKey);
			Org.Mockito.Mockito.When(mockProv.GetCurrentKey(Org.Mockito.Mockito.Eq("k2"))).ThenReturn
				(null);
			Org.Mockito.Mockito.When(mockProv.GetConf()).ThenReturn(new Configuration());
			KeyProvider cache = new CachingKeyProvider(mockProv, 100, 100);
			// asserting caching
			Assert.Equal(mockKey, cache.GetCurrentKey("k1"));
			Org.Mockito.Mockito.Verify(mockProv, Org.Mockito.Mockito.Times(1)).GetCurrentKey(
				Org.Mockito.Mockito.Eq("k1"));
			Assert.Equal(mockKey, cache.GetCurrentKey("k1"));
			Org.Mockito.Mockito.Verify(mockProv, Org.Mockito.Mockito.Times(1)).GetCurrentKey(
				Org.Mockito.Mockito.Eq("k1"));
			Sharpen.Thread.Sleep(1200);
			Assert.Equal(mockKey, cache.GetCurrentKey("k1"));
			Org.Mockito.Mockito.Verify(mockProv, Org.Mockito.Mockito.Times(2)).GetCurrentKey(
				Org.Mockito.Mockito.Eq("k1"));
			// asserting no caching when key is not known
			cache = new CachingKeyProvider(mockProv, 100, 100);
			Assert.Equal(null, cache.GetCurrentKey("k2"));
			Org.Mockito.Mockito.Verify(mockProv, Org.Mockito.Mockito.Times(1)).GetCurrentKey(
				Org.Mockito.Mockito.Eq("k2"));
			Assert.Equal(null, cache.GetCurrentKey("k2"));
			Org.Mockito.Mockito.Verify(mockProv, Org.Mockito.Mockito.Times(2)).GetCurrentKey(
				Org.Mockito.Mockito.Eq("k2"));
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestKeyVersion()
		{
			KeyProvider.KeyVersion mockKey = Org.Mockito.Mockito.Mock<KeyProvider.KeyVersion>
				();
			KeyProvider mockProv = Org.Mockito.Mockito.Mock<KeyProvider>();
			Org.Mockito.Mockito.When(mockProv.GetKeyVersion(Org.Mockito.Mockito.Eq("k1@0"))).
				ThenReturn(mockKey);
			Org.Mockito.Mockito.When(mockProv.GetKeyVersion(Org.Mockito.Mockito.Eq("k2@0"))).
				ThenReturn(null);
			Org.Mockito.Mockito.When(mockProv.GetConf()).ThenReturn(new Configuration());
			KeyProvider cache = new CachingKeyProvider(mockProv, 100, 100);
			// asserting caching
			Assert.Equal(mockKey, cache.GetKeyVersion("k1@0"));
			Org.Mockito.Mockito.Verify(mockProv, Org.Mockito.Mockito.Times(1)).GetKeyVersion(
				Org.Mockito.Mockito.Eq("k1@0"));
			Assert.Equal(mockKey, cache.GetKeyVersion("k1@0"));
			Org.Mockito.Mockito.Verify(mockProv, Org.Mockito.Mockito.Times(1)).GetKeyVersion(
				Org.Mockito.Mockito.Eq("k1@0"));
			Sharpen.Thread.Sleep(200);
			Assert.Equal(mockKey, cache.GetKeyVersion("k1@0"));
			Org.Mockito.Mockito.Verify(mockProv, Org.Mockito.Mockito.Times(2)).GetKeyVersion(
				Org.Mockito.Mockito.Eq("k1@0"));
			// asserting no caching when key is not known
			cache = new CachingKeyProvider(mockProv, 100, 100);
			Assert.Equal(null, cache.GetKeyVersion("k2@0"));
			Org.Mockito.Mockito.Verify(mockProv, Org.Mockito.Mockito.Times(1)).GetKeyVersion(
				Org.Mockito.Mockito.Eq("k2@0"));
			Assert.Equal(null, cache.GetKeyVersion("k2@0"));
			Org.Mockito.Mockito.Verify(mockProv, Org.Mockito.Mockito.Times(2)).GetKeyVersion(
				Org.Mockito.Mockito.Eq("k2@0"));
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestMetadata()
		{
			KeyProvider.Metadata mockMeta = Org.Mockito.Mockito.Mock<KeyProvider.Metadata>();
			KeyProvider mockProv = Org.Mockito.Mockito.Mock<KeyProvider>();
			Org.Mockito.Mockito.When(mockProv.GetMetadata(Org.Mockito.Mockito.Eq("k1"))).ThenReturn
				(mockMeta);
			Org.Mockito.Mockito.When(mockProv.GetMetadata(Org.Mockito.Mockito.Eq("k2"))).ThenReturn
				(null);
			Org.Mockito.Mockito.When(mockProv.GetConf()).ThenReturn(new Configuration());
			KeyProvider cache = new CachingKeyProvider(mockProv, 100, 100);
			// asserting caching
			Assert.Equal(mockMeta, cache.GetMetadata("k1"));
			Org.Mockito.Mockito.Verify(mockProv, Org.Mockito.Mockito.Times(1)).GetMetadata(Org.Mockito.Mockito
				.Eq("k1"));
			Assert.Equal(mockMeta, cache.GetMetadata("k1"));
			Org.Mockito.Mockito.Verify(mockProv, Org.Mockito.Mockito.Times(1)).GetMetadata(Org.Mockito.Mockito
				.Eq("k1"));
			Sharpen.Thread.Sleep(200);
			Assert.Equal(mockMeta, cache.GetMetadata("k1"));
			Org.Mockito.Mockito.Verify(mockProv, Org.Mockito.Mockito.Times(2)).GetMetadata(Org.Mockito.Mockito
				.Eq("k1"));
			// asserting no caching when key is not known
			cache = new CachingKeyProvider(mockProv, 100, 100);
			Assert.Equal(null, cache.GetMetadata("k2"));
			Org.Mockito.Mockito.Verify(mockProv, Org.Mockito.Mockito.Times(1)).GetMetadata(Org.Mockito.Mockito
				.Eq("k2"));
			Assert.Equal(null, cache.GetMetadata("k2"));
			Org.Mockito.Mockito.Verify(mockProv, Org.Mockito.Mockito.Times(2)).GetMetadata(Org.Mockito.Mockito
				.Eq("k2"));
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestRollNewVersion()
		{
			KeyProvider.KeyVersion mockKey = Org.Mockito.Mockito.Mock<KeyProvider.KeyVersion>
				();
			KeyProvider mockProv = Org.Mockito.Mockito.Mock<KeyProvider>();
			Org.Mockito.Mockito.When(mockProv.GetCurrentKey(Org.Mockito.Mockito.Eq("k1"))).ThenReturn
				(mockKey);
			Org.Mockito.Mockito.When(mockProv.GetConf()).ThenReturn(new Configuration());
			KeyProvider cache = new CachingKeyProvider(mockProv, 100, 100);
			Assert.Equal(mockKey, cache.GetCurrentKey("k1"));
			Org.Mockito.Mockito.Verify(mockProv, Org.Mockito.Mockito.Times(1)).GetCurrentKey(
				Org.Mockito.Mockito.Eq("k1"));
			cache.RollNewVersion("k1");
			// asserting the cache is purged
			Assert.Equal(mockKey, cache.GetCurrentKey("k1"));
			Org.Mockito.Mockito.Verify(mockProv, Org.Mockito.Mockito.Times(2)).GetCurrentKey(
				Org.Mockito.Mockito.Eq("k1"));
			cache.RollNewVersion("k1", new byte[0]);
			Assert.Equal(mockKey, cache.GetCurrentKey("k1"));
			Org.Mockito.Mockito.Verify(mockProv, Org.Mockito.Mockito.Times(3)).GetCurrentKey(
				Org.Mockito.Mockito.Eq("k1"));
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestDeleteKey()
		{
			KeyProvider.KeyVersion mockKey = Org.Mockito.Mockito.Mock<KeyProvider.KeyVersion>
				();
			KeyProvider mockProv = Org.Mockito.Mockito.Mock<KeyProvider>();
			Org.Mockito.Mockito.When(mockProv.GetCurrentKey(Org.Mockito.Mockito.Eq("k1"))).ThenReturn
				(mockKey);
			Org.Mockito.Mockito.When(mockProv.GetKeyVersion(Org.Mockito.Mockito.Eq("k1@0"))).
				ThenReturn(mockKey);
			Org.Mockito.Mockito.When(mockProv.GetMetadata(Org.Mockito.Mockito.Eq("k1"))).ThenReturn
				(new KMSClientProvider.KMSMetadata("c", 0, "l", null, new DateTime(), 1));
			Org.Mockito.Mockito.When(mockProv.GetConf()).ThenReturn(new Configuration());
			KeyProvider cache = new CachingKeyProvider(mockProv, 100, 100);
			Assert.Equal(mockKey, cache.GetCurrentKey("k1"));
			Org.Mockito.Mockito.Verify(mockProv, Org.Mockito.Mockito.Times(1)).GetCurrentKey(
				Org.Mockito.Mockito.Eq("k1"));
			Assert.Equal(mockKey, cache.GetKeyVersion("k1@0"));
			Org.Mockito.Mockito.Verify(mockProv, Org.Mockito.Mockito.Times(1)).GetKeyVersion(
				Org.Mockito.Mockito.Eq("k1@0"));
			cache.DeleteKey("k1");
			// asserting the cache is purged
			Assert.Equal(mockKey, cache.GetCurrentKey("k1"));
			Org.Mockito.Mockito.Verify(mockProv, Org.Mockito.Mockito.Times(2)).GetCurrentKey(
				Org.Mockito.Mockito.Eq("k1"));
			Assert.Equal(mockKey, cache.GetKeyVersion("k1@0"));
			Org.Mockito.Mockito.Verify(mockProv, Org.Mockito.Mockito.Times(2)).GetKeyVersion(
				Org.Mockito.Mockito.Eq("k1@0"));
		}
	}
}
