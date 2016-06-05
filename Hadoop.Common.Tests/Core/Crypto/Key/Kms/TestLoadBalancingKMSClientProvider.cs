using System;
using System.IO;
using Com.Google.Common.Collect;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.Crypto.Key;
using Sharpen;

namespace Org.Apache.Hadoop.Crypto.Key.Kms
{
	public class TestLoadBalancingKMSClientProvider
	{
		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestCreation()
		{
			Configuration conf = new Configuration();
			KeyProvider kp = new KMSClientProvider.Factory().CreateProvider(new URI("kms://http@host1/kms/foo"
				), conf);
			Assert.True(kp is KMSClientProvider);
			Assert.Equal("http://host1/kms/foo/v1/", ((KMSClientProvider)kp
				).GetKMSUrl());
			kp = new KMSClientProvider.Factory().CreateProvider(new URI("kms://http@host1;host2;host3/kms/foo"
				), conf);
			Assert.True(kp is LoadBalancingKMSClientProvider);
			KMSClientProvider[] providers = ((LoadBalancingKMSClientProvider)kp).GetProviders
				();
			Assert.Equal(3, providers.Length);
			Assert.Equal(Sets.NewHashSet("http://host1/kms/foo/v1/", "http://host2/kms/foo/v1/"
				, "http://host3/kms/foo/v1/"), Sets.NewHashSet(providers[0].GetKMSUrl(), providers
				[1].GetKMSUrl(), providers[2].GetKMSUrl()));
			kp = new KMSClientProvider.Factory().CreateProvider(new URI("kms://http@host1;host2;host3:16000/kms/foo"
				), conf);
			Assert.True(kp is LoadBalancingKMSClientProvider);
			providers = ((LoadBalancingKMSClientProvider)kp).GetProviders();
			Assert.Equal(3, providers.Length);
			Assert.Equal(Sets.NewHashSet("http://host1:16000/kms/foo/v1/", 
				"http://host2:16000/kms/foo/v1/", "http://host3:16000/kms/foo/v1/"), Sets.NewHashSet
				(providers[0].GetKMSUrl(), providers[1].GetKMSUrl(), providers[2].GetKMSUrl()));
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestLoadBalancing()
		{
			Configuration conf = new Configuration();
			KMSClientProvider p1 = Org.Mockito.Mockito.Mock<KMSClientProvider>();
			Org.Mockito.Mockito.When(p1.CreateKey(Org.Mockito.Mockito.AnyString(), Org.Mockito.Mockito
				.Any<KeyProvider.Options>())).ThenReturn(new KMSClientProvider.KMSKeyVersion("p1"
				, "v1", new byte[0]));
			KMSClientProvider p2 = Org.Mockito.Mockito.Mock<KMSClientProvider>();
			Org.Mockito.Mockito.When(p2.CreateKey(Org.Mockito.Mockito.AnyString(), Org.Mockito.Mockito
				.Any<KeyProvider.Options>())).ThenReturn(new KMSClientProvider.KMSKeyVersion("p2"
				, "v2", new byte[0]));
			KMSClientProvider p3 = Org.Mockito.Mockito.Mock<KMSClientProvider>();
			Org.Mockito.Mockito.When(p3.CreateKey(Org.Mockito.Mockito.AnyString(), Org.Mockito.Mockito
				.Any<KeyProvider.Options>())).ThenReturn(new KMSClientProvider.KMSKeyVersion("p3"
				, "v3", new byte[0]));
			KeyProvider kp = new LoadBalancingKMSClientProvider(new KMSClientProvider[] { p1, 
				p2, p3 }, 0, conf);
			Assert.Equal("p1", kp.CreateKey("test1", new KeyProvider.Options
				(conf)).GetName());
			Assert.Equal("p2", kp.CreateKey("test2", new KeyProvider.Options
				(conf)).GetName());
			Assert.Equal("p3", kp.CreateKey("test3", new KeyProvider.Options
				(conf)).GetName());
			Assert.Equal("p1", kp.CreateKey("test4", new KeyProvider.Options
				(conf)).GetName());
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestLoadBalancingWithFailure()
		{
			Configuration conf = new Configuration();
			KMSClientProvider p1 = Org.Mockito.Mockito.Mock<KMSClientProvider>();
			Org.Mockito.Mockito.When(p1.CreateKey(Org.Mockito.Mockito.AnyString(), Org.Mockito.Mockito
				.Any<KeyProvider.Options>())).ThenReturn(new KMSClientProvider.KMSKeyVersion("p1"
				, "v1", new byte[0]));
			Org.Mockito.Mockito.When(p1.GetKMSUrl()).ThenReturn("p1");
			// This should not be retried
			KMSClientProvider p2 = Org.Mockito.Mockito.Mock<KMSClientProvider>();
			Org.Mockito.Mockito.When(p2.CreateKey(Org.Mockito.Mockito.AnyString(), Org.Mockito.Mockito
				.Any<KeyProvider.Options>())).ThenThrow(new NoSuchAlgorithmException("p2"));
			Org.Mockito.Mockito.When(p2.GetKMSUrl()).ThenReturn("p2");
			KMSClientProvider p3 = Org.Mockito.Mockito.Mock<KMSClientProvider>();
			Org.Mockito.Mockito.When(p3.CreateKey(Org.Mockito.Mockito.AnyString(), Org.Mockito.Mockito
				.Any<KeyProvider.Options>())).ThenReturn(new KMSClientProvider.KMSKeyVersion("p3"
				, "v3", new byte[0]));
			Org.Mockito.Mockito.When(p3.GetKMSUrl()).ThenReturn("p3");
			// This should be retried
			KMSClientProvider p4 = Org.Mockito.Mockito.Mock<KMSClientProvider>();
			Org.Mockito.Mockito.When(p4.CreateKey(Org.Mockito.Mockito.AnyString(), Org.Mockito.Mockito
				.Any<KeyProvider.Options>())).ThenThrow(new IOException("p4"));
			Org.Mockito.Mockito.When(p4.GetKMSUrl()).ThenReturn("p4");
			KeyProvider kp = new LoadBalancingKMSClientProvider(new KMSClientProvider[] { p1, 
				p2, p3, p4 }, 0, conf);
			Assert.Equal("p1", kp.CreateKey("test4", new KeyProvider.Options
				(conf)).GetName());
			// Exceptions other than IOExceptions will not be retried
			try
			{
				kp.CreateKey("test1", new KeyProvider.Options(conf)).GetName();
				NUnit.Framework.Assert.Fail("Should fail since its not an IOException");
			}
			catch (Exception e)
			{
				Assert.True(e is NoSuchAlgorithmException);
			}
			Assert.Equal("p3", kp.CreateKey("test2", new KeyProvider.Options
				(conf)).GetName());
			// IOException will trigger retry in next provider
			Assert.Equal("p1", kp.CreateKey("test3", new KeyProvider.Options
				(conf)).GetName());
		}

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestLoadBalancingWithAllBadNodes()
		{
			Configuration conf = new Configuration();
			KMSClientProvider p1 = Org.Mockito.Mockito.Mock<KMSClientProvider>();
			Org.Mockito.Mockito.When(p1.CreateKey(Org.Mockito.Mockito.AnyString(), Org.Mockito.Mockito
				.Any<KeyProvider.Options>())).ThenThrow(new IOException("p1"));
			KMSClientProvider p2 = Org.Mockito.Mockito.Mock<KMSClientProvider>();
			Org.Mockito.Mockito.When(p2.CreateKey(Org.Mockito.Mockito.AnyString(), Org.Mockito.Mockito
				.Any<KeyProvider.Options>())).ThenThrow(new IOException("p2"));
			KMSClientProvider p3 = Org.Mockito.Mockito.Mock<KMSClientProvider>();
			Org.Mockito.Mockito.When(p3.CreateKey(Org.Mockito.Mockito.AnyString(), Org.Mockito.Mockito
				.Any<KeyProvider.Options>())).ThenThrow(new IOException("p3"));
			KMSClientProvider p4 = Org.Mockito.Mockito.Mock<KMSClientProvider>();
			Org.Mockito.Mockito.When(p4.CreateKey(Org.Mockito.Mockito.AnyString(), Org.Mockito.Mockito
				.Any<KeyProvider.Options>())).ThenThrow(new IOException("p4"));
			Org.Mockito.Mockito.When(p1.GetKMSUrl()).ThenReturn("p1");
			Org.Mockito.Mockito.When(p2.GetKMSUrl()).ThenReturn("p2");
			Org.Mockito.Mockito.When(p3.GetKMSUrl()).ThenReturn("p3");
			Org.Mockito.Mockito.When(p4.GetKMSUrl()).ThenReturn("p4");
			KeyProvider kp = new LoadBalancingKMSClientProvider(new KMSClientProvider[] { p1, 
				p2, p3, p4 }, 0, conf);
			try
			{
				kp.CreateKey("test3", new KeyProvider.Options(conf)).GetName();
				NUnit.Framework.Assert.Fail("Should fail since all providers threw an IOException"
					);
			}
			catch (Exception e)
			{
				Assert.True(e is IOException);
			}
		}
	}
}
