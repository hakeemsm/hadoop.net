using Sharpen;

namespace org.apache.hadoop.security.authentication.util
{
	public class TestZKSignerSecretProvider
	{
		private org.apache.curator.test.TestingServer zkServer;

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public virtual void setup()
		{
			zkServer = new org.apache.curator.test.TestingServer();
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.TearDown]
		public virtual void teardown()
		{
			if (zkServer != null)
			{
				zkServer.stop();
				zkServer.close();
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testOne()
		{
			// Test just one ZKSignerSecretProvider to verify that it works in the
			// simplest case
			long rolloverFrequency = 15 * 1000;
			// rollover every 15 sec
			// use the same seed so we can predict the RNG
			long seed = Sharpen.Runtime.currentTimeMillis();
			java.util.Random rand = new java.util.Random(seed);
			byte[] secret2 = Sharpen.Runtime.getBytesForString(System.Convert.ToString(rand.nextLong
				()));
			byte[] secret1 = Sharpen.Runtime.getBytesForString(System.Convert.ToString(rand.nextLong
				()));
			byte[] secret3 = Sharpen.Runtime.getBytesForString(System.Convert.ToString(rand.nextLong
				()));
			org.apache.hadoop.security.authentication.util.ZKSignerSecretProvider secretProvider
				 = new org.apache.hadoop.security.authentication.util.ZKSignerSecretProvider(seed
				);
			java.util.Properties config = new java.util.Properties();
			config.setProperty(org.apache.hadoop.security.authentication.util.ZKSignerSecretProvider
				.ZOOKEEPER_CONNECTION_STRING, zkServer.getConnectString());
			config.setProperty(org.apache.hadoop.security.authentication.util.ZKSignerSecretProvider
				.ZOOKEEPER_PATH, "/secret");
			try
			{
				secretProvider.init(config, getDummyServletContext(), rolloverFrequency);
				byte[] currentSecret = secretProvider.getCurrentSecret();
				byte[][] allSecrets = secretProvider.getAllSecrets();
				NUnit.Framework.Assert.assertArrayEquals(secret1, currentSecret);
				NUnit.Framework.Assert.AreEqual(2, allSecrets.Length);
				NUnit.Framework.Assert.assertArrayEquals(secret1, allSecrets[0]);
				NUnit.Framework.Assert.IsNull(allSecrets[1]);
				java.lang.Thread.sleep((rolloverFrequency + 2000));
				currentSecret = secretProvider.getCurrentSecret();
				allSecrets = secretProvider.getAllSecrets();
				NUnit.Framework.Assert.assertArrayEquals(secret2, currentSecret);
				NUnit.Framework.Assert.AreEqual(2, allSecrets.Length);
				NUnit.Framework.Assert.assertArrayEquals(secret2, allSecrets[0]);
				NUnit.Framework.Assert.assertArrayEquals(secret1, allSecrets[1]);
				java.lang.Thread.sleep((rolloverFrequency + 2000));
				currentSecret = secretProvider.getCurrentSecret();
				allSecrets = secretProvider.getAllSecrets();
				NUnit.Framework.Assert.assertArrayEquals(secret3, currentSecret);
				NUnit.Framework.Assert.AreEqual(2, allSecrets.Length);
				NUnit.Framework.Assert.assertArrayEquals(secret3, allSecrets[0]);
				NUnit.Framework.Assert.assertArrayEquals(secret2, allSecrets[1]);
				java.lang.Thread.sleep((rolloverFrequency + 2000));
			}
			finally
			{
				secretProvider.destroy();
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testMultipleInit()
		{
			long rolloverFrequency = 15 * 1000;
			// rollover every 15 sec
			// use the same seed so we can predict the RNG
			long seedA = Sharpen.Runtime.currentTimeMillis();
			java.util.Random rand = new java.util.Random(seedA);
			byte[] secretA2 = Sharpen.Runtime.getBytesForString(System.Convert.ToString(rand.
				nextLong()));
			byte[] secretA1 = Sharpen.Runtime.getBytesForString(System.Convert.ToString(rand.
				nextLong()));
			// use the same seed so we can predict the RNG
			long seedB = Sharpen.Runtime.currentTimeMillis() + rand.nextLong();
			rand = new java.util.Random(seedB);
			byte[] secretB2 = Sharpen.Runtime.getBytesForString(System.Convert.ToString(rand.
				nextLong()));
			byte[] secretB1 = Sharpen.Runtime.getBytesForString(System.Convert.ToString(rand.
				nextLong()));
			// use the same seed so we can predict the RNG
			long seedC = Sharpen.Runtime.currentTimeMillis() + rand.nextLong();
			rand = new java.util.Random(seedC);
			byte[] secretC2 = Sharpen.Runtime.getBytesForString(System.Convert.ToString(rand.
				nextLong()));
			byte[] secretC1 = Sharpen.Runtime.getBytesForString(System.Convert.ToString(rand.
				nextLong()));
			org.apache.hadoop.security.authentication.util.ZKSignerSecretProvider secretProviderA
				 = new org.apache.hadoop.security.authentication.util.ZKSignerSecretProvider(seedA
				);
			org.apache.hadoop.security.authentication.util.ZKSignerSecretProvider secretProviderB
				 = new org.apache.hadoop.security.authentication.util.ZKSignerSecretProvider(seedB
				);
			org.apache.hadoop.security.authentication.util.ZKSignerSecretProvider secretProviderC
				 = new org.apache.hadoop.security.authentication.util.ZKSignerSecretProvider(seedC
				);
			java.util.Properties config = new java.util.Properties();
			config.setProperty(org.apache.hadoop.security.authentication.util.ZKSignerSecretProvider
				.ZOOKEEPER_CONNECTION_STRING, zkServer.getConnectString());
			config.setProperty(org.apache.hadoop.security.authentication.util.ZKSignerSecretProvider
				.ZOOKEEPER_PATH, "/secret");
			try
			{
				secretProviderA.init(config, getDummyServletContext(), rolloverFrequency);
				secretProviderB.init(config, getDummyServletContext(), rolloverFrequency);
				secretProviderC.init(config, getDummyServletContext(), rolloverFrequency);
				byte[] currentSecretA = secretProviderA.getCurrentSecret();
				byte[][] allSecretsA = secretProviderA.getAllSecrets();
				byte[] currentSecretB = secretProviderB.getCurrentSecret();
				byte[][] allSecretsB = secretProviderB.getAllSecrets();
				byte[] currentSecretC = secretProviderC.getCurrentSecret();
				byte[][] allSecretsC = secretProviderC.getAllSecrets();
				NUnit.Framework.Assert.assertArrayEquals(currentSecretA, currentSecretB);
				NUnit.Framework.Assert.assertArrayEquals(currentSecretB, currentSecretC);
				NUnit.Framework.Assert.AreEqual(2, allSecretsA.Length);
				NUnit.Framework.Assert.AreEqual(2, allSecretsB.Length);
				NUnit.Framework.Assert.AreEqual(2, allSecretsC.Length);
				NUnit.Framework.Assert.assertArrayEquals(allSecretsA[0], allSecretsB[0]);
				NUnit.Framework.Assert.assertArrayEquals(allSecretsB[0], allSecretsC[0]);
				NUnit.Framework.Assert.IsNull(allSecretsA[1]);
				NUnit.Framework.Assert.IsNull(allSecretsB[1]);
				NUnit.Framework.Assert.IsNull(allSecretsC[1]);
				char secretChosen = 'z';
				if (java.util.Arrays.equals(secretA1, currentSecretA))
				{
					NUnit.Framework.Assert.assertArrayEquals(secretA1, allSecretsA[0]);
					secretChosen = 'A';
				}
				else
				{
					if (java.util.Arrays.equals(secretB1, currentSecretB))
					{
						NUnit.Framework.Assert.assertArrayEquals(secretB1, allSecretsA[0]);
						secretChosen = 'B';
					}
					else
					{
						if (java.util.Arrays.equals(secretC1, currentSecretC))
						{
							NUnit.Framework.Assert.assertArrayEquals(secretC1, allSecretsA[0]);
							secretChosen = 'C';
						}
						else
						{
							NUnit.Framework.Assert.Fail("It appears that they all agreed on the same secret, but "
								 + "not one of the secrets they were supposed to");
						}
					}
				}
				java.lang.Thread.sleep((rolloverFrequency + 2000));
				currentSecretA = secretProviderA.getCurrentSecret();
				allSecretsA = secretProviderA.getAllSecrets();
				currentSecretB = secretProviderB.getCurrentSecret();
				allSecretsB = secretProviderB.getAllSecrets();
				currentSecretC = secretProviderC.getCurrentSecret();
				allSecretsC = secretProviderC.getAllSecrets();
				NUnit.Framework.Assert.assertArrayEquals(currentSecretA, currentSecretB);
				NUnit.Framework.Assert.assertArrayEquals(currentSecretB, currentSecretC);
				NUnit.Framework.Assert.AreEqual(2, allSecretsA.Length);
				NUnit.Framework.Assert.AreEqual(2, allSecretsB.Length);
				NUnit.Framework.Assert.AreEqual(2, allSecretsC.Length);
				NUnit.Framework.Assert.assertArrayEquals(allSecretsA[0], allSecretsB[0]);
				NUnit.Framework.Assert.assertArrayEquals(allSecretsB[0], allSecretsC[0]);
				NUnit.Framework.Assert.assertArrayEquals(allSecretsA[1], allSecretsB[1]);
				NUnit.Framework.Assert.assertArrayEquals(allSecretsB[1], allSecretsC[1]);
				// The second secret used is prechosen by whoever won the init; so it
				// should match with whichever we saw before
				if (secretChosen == 'A')
				{
					NUnit.Framework.Assert.assertArrayEquals(secretA2, currentSecretA);
				}
				else
				{
					if (secretChosen == 'B')
					{
						NUnit.Framework.Assert.assertArrayEquals(secretB2, currentSecretA);
					}
					else
					{
						if (secretChosen == 'C')
						{
							NUnit.Framework.Assert.assertArrayEquals(secretC2, currentSecretA);
						}
					}
				}
			}
			finally
			{
				secretProviderC.destroy();
				secretProviderB.destroy();
				secretProviderA.destroy();
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testMultipleUnsychnronized()
		{
			long rolloverFrequency = 15 * 1000;
			// rollover every 15 sec
			// use the same seed so we can predict the RNG
			long seedA = Sharpen.Runtime.currentTimeMillis();
			java.util.Random rand = new java.util.Random(seedA);
			byte[] secretA2 = Sharpen.Runtime.getBytesForString(System.Convert.ToString(rand.
				nextLong()));
			byte[] secretA1 = Sharpen.Runtime.getBytesForString(System.Convert.ToString(rand.
				nextLong()));
			byte[] secretA3 = Sharpen.Runtime.getBytesForString(System.Convert.ToString(rand.
				nextLong()));
			// use the same seed so we can predict the RNG
			long seedB = Sharpen.Runtime.currentTimeMillis() + rand.nextLong();
			rand = new java.util.Random(seedB);
			byte[] secretB2 = Sharpen.Runtime.getBytesForString(System.Convert.ToString(rand.
				nextLong()));
			byte[] secretB1 = Sharpen.Runtime.getBytesForString(System.Convert.ToString(rand.
				nextLong()));
			byte[] secretB3 = Sharpen.Runtime.getBytesForString(System.Convert.ToString(rand.
				nextLong()));
			org.apache.hadoop.security.authentication.util.ZKSignerSecretProvider secretProviderA
				 = new org.apache.hadoop.security.authentication.util.ZKSignerSecretProvider(seedA
				);
			org.apache.hadoop.security.authentication.util.ZKSignerSecretProvider secretProviderB
				 = new org.apache.hadoop.security.authentication.util.ZKSignerSecretProvider(seedB
				);
			java.util.Properties config = new java.util.Properties();
			config.setProperty(org.apache.hadoop.security.authentication.util.ZKSignerSecretProvider
				.ZOOKEEPER_CONNECTION_STRING, zkServer.getConnectString());
			config.setProperty(org.apache.hadoop.security.authentication.util.ZKSignerSecretProvider
				.ZOOKEEPER_PATH, "/secret");
			try
			{
				secretProviderA.init(config, getDummyServletContext(), rolloverFrequency);
				byte[] currentSecretA = secretProviderA.getCurrentSecret();
				byte[][] allSecretsA = secretProviderA.getAllSecrets();
				NUnit.Framework.Assert.assertArrayEquals(secretA1, currentSecretA);
				NUnit.Framework.Assert.AreEqual(2, allSecretsA.Length);
				NUnit.Framework.Assert.assertArrayEquals(secretA1, allSecretsA[0]);
				NUnit.Framework.Assert.IsNull(allSecretsA[1]);
				java.lang.Thread.sleep((rolloverFrequency + 2000));
				currentSecretA = secretProviderA.getCurrentSecret();
				allSecretsA = secretProviderA.getAllSecrets();
				NUnit.Framework.Assert.assertArrayEquals(secretA2, currentSecretA);
				NUnit.Framework.Assert.AreEqual(2, allSecretsA.Length);
				NUnit.Framework.Assert.assertArrayEquals(secretA2, allSecretsA[0]);
				NUnit.Framework.Assert.assertArrayEquals(secretA1, allSecretsA[1]);
				java.lang.Thread.sleep((rolloverFrequency / 5));
				secretProviderB.init(config, getDummyServletContext(), rolloverFrequency);
				byte[] currentSecretB = secretProviderB.getCurrentSecret();
				byte[][] allSecretsB = secretProviderB.getAllSecrets();
				NUnit.Framework.Assert.assertArrayEquals(secretA2, currentSecretB);
				NUnit.Framework.Assert.AreEqual(2, allSecretsA.Length);
				NUnit.Framework.Assert.assertArrayEquals(secretA2, allSecretsB[0]);
				NUnit.Framework.Assert.assertArrayEquals(secretA1, allSecretsB[1]);
				java.lang.Thread.sleep((rolloverFrequency));
				currentSecretA = secretProviderA.getCurrentSecret();
				allSecretsA = secretProviderA.getAllSecrets();
				currentSecretB = secretProviderB.getCurrentSecret();
				allSecretsB = secretProviderB.getAllSecrets();
				NUnit.Framework.Assert.assertArrayEquals(currentSecretA, currentSecretB);
				NUnit.Framework.Assert.AreEqual(2, allSecretsA.Length);
				NUnit.Framework.Assert.AreEqual(2, allSecretsB.Length);
				NUnit.Framework.Assert.assertArrayEquals(allSecretsA[0], allSecretsB[0]);
				NUnit.Framework.Assert.assertArrayEquals(allSecretsA[1], allSecretsB[1]);
				if (java.util.Arrays.equals(secretA3, currentSecretA))
				{
					NUnit.Framework.Assert.assertArrayEquals(secretA3, allSecretsA[0]);
				}
				else
				{
					if (java.util.Arrays.equals(secretB3, currentSecretB))
					{
						NUnit.Framework.Assert.assertArrayEquals(secretB3, allSecretsA[0]);
					}
					else
					{
						NUnit.Framework.Assert.Fail("It appears that they all agreed on the same secret, but "
							 + "not one of the secrets they were supposed to");
					}
				}
			}
			finally
			{
				secretProviderB.destroy();
				secretProviderA.destroy();
			}
		}

		private javax.servlet.ServletContext getDummyServletContext()
		{
			javax.servlet.ServletContext servletContext = org.mockito.Mockito.mock<javax.servlet.ServletContext
				>();
			org.mockito.Mockito.when(servletContext.getAttribute(org.apache.hadoop.security.authentication.util.ZKSignerSecretProvider
				.ZOOKEEPER_SIGNER_SECRET_PROVIDER_CURATOR_CLIENT_ATTRIBUTE)).thenReturn(null);
			return servletContext;
		}
	}
}
