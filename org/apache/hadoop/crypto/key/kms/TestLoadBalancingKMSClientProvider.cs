using Sharpen;

namespace org.apache.hadoop.crypto.key.kms
{
	public class TestLoadBalancingKMSClientProvider
	{
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testCreation()
		{
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			org.apache.hadoop.crypto.key.KeyProvider kp = new org.apache.hadoop.crypto.key.kms.KMSClientProvider.Factory
				().createProvider(new java.net.URI("kms://http@host1/kms/foo"), conf);
			NUnit.Framework.Assert.IsTrue(kp is org.apache.hadoop.crypto.key.kms.KMSClientProvider
				);
			NUnit.Framework.Assert.AreEqual("http://host1/kms/foo/v1/", ((org.apache.hadoop.crypto.key.kms.KMSClientProvider
				)kp).getKMSUrl());
			kp = new org.apache.hadoop.crypto.key.kms.KMSClientProvider.Factory().createProvider
				(new java.net.URI("kms://http@host1;host2;host3/kms/foo"), conf);
			NUnit.Framework.Assert.IsTrue(kp is org.apache.hadoop.crypto.key.kms.LoadBalancingKMSClientProvider
				);
			org.apache.hadoop.crypto.key.kms.KMSClientProvider[] providers = ((org.apache.hadoop.crypto.key.kms.LoadBalancingKMSClientProvider
				)kp).getProviders();
			NUnit.Framework.Assert.AreEqual(3, providers.Length);
			NUnit.Framework.Assert.AreEqual(com.google.common.collect.Sets.newHashSet("http://host1/kms/foo/v1/"
				, "http://host2/kms/foo/v1/", "http://host3/kms/foo/v1/"), com.google.common.collect.Sets
				.newHashSet(providers[0].getKMSUrl(), providers[1].getKMSUrl(), providers[2].getKMSUrl
				()));
			kp = new org.apache.hadoop.crypto.key.kms.KMSClientProvider.Factory().createProvider
				(new java.net.URI("kms://http@host1;host2;host3:16000/kms/foo"), conf);
			NUnit.Framework.Assert.IsTrue(kp is org.apache.hadoop.crypto.key.kms.LoadBalancingKMSClientProvider
				);
			providers = ((org.apache.hadoop.crypto.key.kms.LoadBalancingKMSClientProvider)kp)
				.getProviders();
			NUnit.Framework.Assert.AreEqual(3, providers.Length);
			NUnit.Framework.Assert.AreEqual(com.google.common.collect.Sets.newHashSet("http://host1:16000/kms/foo/v1/"
				, "http://host2:16000/kms/foo/v1/", "http://host3:16000/kms/foo/v1/"), com.google.common.collect.Sets
				.newHashSet(providers[0].getKMSUrl(), providers[1].getKMSUrl(), providers[2].getKMSUrl
				()));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testLoadBalancing()
		{
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			org.apache.hadoop.crypto.key.kms.KMSClientProvider p1 = org.mockito.Mockito.mock<
				org.apache.hadoop.crypto.key.kms.KMSClientProvider>();
			org.mockito.Mockito.when(p1.createKey(org.mockito.Mockito.anyString(), org.mockito.Mockito
				.any<org.apache.hadoop.crypto.key.KeyProvider.Options>())).thenReturn(new org.apache.hadoop.crypto.key.kms.KMSClientProvider.KMSKeyVersion
				("p1", "v1", new byte[0]));
			org.apache.hadoop.crypto.key.kms.KMSClientProvider p2 = org.mockito.Mockito.mock<
				org.apache.hadoop.crypto.key.kms.KMSClientProvider>();
			org.mockito.Mockito.when(p2.createKey(org.mockito.Mockito.anyString(), org.mockito.Mockito
				.any<org.apache.hadoop.crypto.key.KeyProvider.Options>())).thenReturn(new org.apache.hadoop.crypto.key.kms.KMSClientProvider.KMSKeyVersion
				("p2", "v2", new byte[0]));
			org.apache.hadoop.crypto.key.kms.KMSClientProvider p3 = org.mockito.Mockito.mock<
				org.apache.hadoop.crypto.key.kms.KMSClientProvider>();
			org.mockito.Mockito.when(p3.createKey(org.mockito.Mockito.anyString(), org.mockito.Mockito
				.any<org.apache.hadoop.crypto.key.KeyProvider.Options>())).thenReturn(new org.apache.hadoop.crypto.key.kms.KMSClientProvider.KMSKeyVersion
				("p3", "v3", new byte[0]));
			org.apache.hadoop.crypto.key.KeyProvider kp = new org.apache.hadoop.crypto.key.kms.LoadBalancingKMSClientProvider
				(new org.apache.hadoop.crypto.key.kms.KMSClientProvider[] { p1, p2, p3 }, 0, conf
				);
			NUnit.Framework.Assert.AreEqual("p1", kp.createKey("test1", new org.apache.hadoop.crypto.key.KeyProvider.Options
				(conf)).getName());
			NUnit.Framework.Assert.AreEqual("p2", kp.createKey("test2", new org.apache.hadoop.crypto.key.KeyProvider.Options
				(conf)).getName());
			NUnit.Framework.Assert.AreEqual("p3", kp.createKey("test3", new org.apache.hadoop.crypto.key.KeyProvider.Options
				(conf)).getName());
			NUnit.Framework.Assert.AreEqual("p1", kp.createKey("test4", new org.apache.hadoop.crypto.key.KeyProvider.Options
				(conf)).getName());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testLoadBalancingWithFailure()
		{
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			org.apache.hadoop.crypto.key.kms.KMSClientProvider p1 = org.mockito.Mockito.mock<
				org.apache.hadoop.crypto.key.kms.KMSClientProvider>();
			org.mockito.Mockito.when(p1.createKey(org.mockito.Mockito.anyString(), org.mockito.Mockito
				.any<org.apache.hadoop.crypto.key.KeyProvider.Options>())).thenReturn(new org.apache.hadoop.crypto.key.kms.KMSClientProvider.KMSKeyVersion
				("p1", "v1", new byte[0]));
			org.mockito.Mockito.when(p1.getKMSUrl()).thenReturn("p1");
			// This should not be retried
			org.apache.hadoop.crypto.key.kms.KMSClientProvider p2 = org.mockito.Mockito.mock<
				org.apache.hadoop.crypto.key.kms.KMSClientProvider>();
			org.mockito.Mockito.when(p2.createKey(org.mockito.Mockito.anyString(), org.mockito.Mockito
				.any<org.apache.hadoop.crypto.key.KeyProvider.Options>())).thenThrow(new java.security.NoSuchAlgorithmException
				("p2"));
			org.mockito.Mockito.when(p2.getKMSUrl()).thenReturn("p2");
			org.apache.hadoop.crypto.key.kms.KMSClientProvider p3 = org.mockito.Mockito.mock<
				org.apache.hadoop.crypto.key.kms.KMSClientProvider>();
			org.mockito.Mockito.when(p3.createKey(org.mockito.Mockito.anyString(), org.mockito.Mockito
				.any<org.apache.hadoop.crypto.key.KeyProvider.Options>())).thenReturn(new org.apache.hadoop.crypto.key.kms.KMSClientProvider.KMSKeyVersion
				("p3", "v3", new byte[0]));
			org.mockito.Mockito.when(p3.getKMSUrl()).thenReturn("p3");
			// This should be retried
			org.apache.hadoop.crypto.key.kms.KMSClientProvider p4 = org.mockito.Mockito.mock<
				org.apache.hadoop.crypto.key.kms.KMSClientProvider>();
			org.mockito.Mockito.when(p4.createKey(org.mockito.Mockito.anyString(), org.mockito.Mockito
				.any<org.apache.hadoop.crypto.key.KeyProvider.Options>())).thenThrow(new System.IO.IOException
				("p4"));
			org.mockito.Mockito.when(p4.getKMSUrl()).thenReturn("p4");
			org.apache.hadoop.crypto.key.KeyProvider kp = new org.apache.hadoop.crypto.key.kms.LoadBalancingKMSClientProvider
				(new org.apache.hadoop.crypto.key.kms.KMSClientProvider[] { p1, p2, p3, p4 }, 0, 
				conf);
			NUnit.Framework.Assert.AreEqual("p1", kp.createKey("test4", new org.apache.hadoop.crypto.key.KeyProvider.Options
				(conf)).getName());
			// Exceptions other than IOExceptions will not be retried
			try
			{
				kp.createKey("test1", new org.apache.hadoop.crypto.key.KeyProvider.Options(conf))
					.getName();
				NUnit.Framework.Assert.Fail("Should fail since its not an IOException");
			}
			catch (System.Exception e)
			{
				NUnit.Framework.Assert.IsTrue(e is java.security.NoSuchAlgorithmException);
			}
			NUnit.Framework.Assert.AreEqual("p3", kp.createKey("test2", new org.apache.hadoop.crypto.key.KeyProvider.Options
				(conf)).getName());
			// IOException will trigger retry in next provider
			NUnit.Framework.Assert.AreEqual("p1", kp.createKey("test3", new org.apache.hadoop.crypto.key.KeyProvider.Options
				(conf)).getName());
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testLoadBalancingWithAllBadNodes()
		{
			org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration
				();
			org.apache.hadoop.crypto.key.kms.KMSClientProvider p1 = org.mockito.Mockito.mock<
				org.apache.hadoop.crypto.key.kms.KMSClientProvider>();
			org.mockito.Mockito.when(p1.createKey(org.mockito.Mockito.anyString(), org.mockito.Mockito
				.any<org.apache.hadoop.crypto.key.KeyProvider.Options>())).thenThrow(new System.IO.IOException
				("p1"));
			org.apache.hadoop.crypto.key.kms.KMSClientProvider p2 = org.mockito.Mockito.mock<
				org.apache.hadoop.crypto.key.kms.KMSClientProvider>();
			org.mockito.Mockito.when(p2.createKey(org.mockito.Mockito.anyString(), org.mockito.Mockito
				.any<org.apache.hadoop.crypto.key.KeyProvider.Options>())).thenThrow(new System.IO.IOException
				("p2"));
			org.apache.hadoop.crypto.key.kms.KMSClientProvider p3 = org.mockito.Mockito.mock<
				org.apache.hadoop.crypto.key.kms.KMSClientProvider>();
			org.mockito.Mockito.when(p3.createKey(org.mockito.Mockito.anyString(), org.mockito.Mockito
				.any<org.apache.hadoop.crypto.key.KeyProvider.Options>())).thenThrow(new System.IO.IOException
				("p3"));
			org.apache.hadoop.crypto.key.kms.KMSClientProvider p4 = org.mockito.Mockito.mock<
				org.apache.hadoop.crypto.key.kms.KMSClientProvider>();
			org.mockito.Mockito.when(p4.createKey(org.mockito.Mockito.anyString(), org.mockito.Mockito
				.any<org.apache.hadoop.crypto.key.KeyProvider.Options>())).thenThrow(new System.IO.IOException
				("p4"));
			org.mockito.Mockito.when(p1.getKMSUrl()).thenReturn("p1");
			org.mockito.Mockito.when(p2.getKMSUrl()).thenReturn("p2");
			org.mockito.Mockito.when(p3.getKMSUrl()).thenReturn("p3");
			org.mockito.Mockito.when(p4.getKMSUrl()).thenReturn("p4");
			org.apache.hadoop.crypto.key.KeyProvider kp = new org.apache.hadoop.crypto.key.kms.LoadBalancingKMSClientProvider
				(new org.apache.hadoop.crypto.key.kms.KMSClientProvider[] { p1, p2, p3, p4 }, 0, 
				conf);
			try
			{
				kp.createKey("test3", new org.apache.hadoop.crypto.key.KeyProvider.Options(conf))
					.getName();
				NUnit.Framework.Assert.Fail("Should fail since all providers threw an IOException"
					);
			}
			catch (System.Exception e)
			{
				NUnit.Framework.Assert.IsTrue(e is System.IO.IOException);
			}
		}
	}
}
