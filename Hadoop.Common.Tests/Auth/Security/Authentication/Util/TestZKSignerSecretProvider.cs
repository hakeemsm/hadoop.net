using Javax.Servlet;
using NUnit.Framework;
using Org.Apache.Curator.Test;
using Sharpen;

namespace Org.Apache.Hadoop.Security.Authentication.Util
{
	public class TestZKSignerSecretProvider
	{
		private TestingServer zkServer;

		/// <exception cref="System.Exception"/>
		[SetUp]
		public virtual void Setup()
		{
			zkServer = new TestingServer();
		}

		/// <exception cref="System.Exception"/>
		[TearDown]
		public virtual void Teardown()
		{
			if (zkServer != null)
			{
				zkServer.Stop();
				zkServer.Close();
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestOne()
		{
			// Test just one ZKSignerSecretProvider to verify that it works in the
			// simplest case
			long rolloverFrequency = 15 * 1000;
			// rollover every 15 sec
			// use the same seed so we can predict the RNG
			long seed = Runtime.CurrentTimeMillis();
			Random rand = new Random(seed);
			byte[] secret2 = Sharpen.Runtime.GetBytesForString(System.Convert.ToString(rand.NextLong
				()));
			byte[] secret1 = Sharpen.Runtime.GetBytesForString(System.Convert.ToString(rand.NextLong
				()));
			byte[] secret3 = Sharpen.Runtime.GetBytesForString(System.Convert.ToString(rand.NextLong
				()));
			ZKSignerSecretProvider secretProvider = new ZKSignerSecretProvider(seed);
			Properties config = new Properties();
			config.SetProperty(ZKSignerSecretProvider.ZookeeperConnectionString, zkServer.GetConnectString
				());
			config.SetProperty(ZKSignerSecretProvider.ZookeeperPath, "/secret");
			try
			{
				secretProvider.Init(config, GetDummyServletContext(), rolloverFrequency);
				byte[] currentSecret = secretProvider.GetCurrentSecret();
				byte[][] allSecrets = secretProvider.GetAllSecrets();
				Assert.AssertArrayEquals(secret1, currentSecret);
				NUnit.Framework.Assert.AreEqual(2, allSecrets.Length);
				Assert.AssertArrayEquals(secret1, allSecrets[0]);
				NUnit.Framework.Assert.IsNull(allSecrets[1]);
				Sharpen.Thread.Sleep((rolloverFrequency + 2000));
				currentSecret = secretProvider.GetCurrentSecret();
				allSecrets = secretProvider.GetAllSecrets();
				Assert.AssertArrayEquals(secret2, currentSecret);
				NUnit.Framework.Assert.AreEqual(2, allSecrets.Length);
				Assert.AssertArrayEquals(secret2, allSecrets[0]);
				Assert.AssertArrayEquals(secret1, allSecrets[1]);
				Sharpen.Thread.Sleep((rolloverFrequency + 2000));
				currentSecret = secretProvider.GetCurrentSecret();
				allSecrets = secretProvider.GetAllSecrets();
				Assert.AssertArrayEquals(secret3, currentSecret);
				NUnit.Framework.Assert.AreEqual(2, allSecrets.Length);
				Assert.AssertArrayEquals(secret3, allSecrets[0]);
				Assert.AssertArrayEquals(secret2, allSecrets[1]);
				Sharpen.Thread.Sleep((rolloverFrequency + 2000));
			}
			finally
			{
				secretProvider.Destroy();
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestMultipleInit()
		{
			long rolloverFrequency = 15 * 1000;
			// rollover every 15 sec
			// use the same seed so we can predict the RNG
			long seedA = Runtime.CurrentTimeMillis();
			Random rand = new Random(seedA);
			byte[] secretA2 = Sharpen.Runtime.GetBytesForString(System.Convert.ToString(rand.
				NextLong()));
			byte[] secretA1 = Sharpen.Runtime.GetBytesForString(System.Convert.ToString(rand.
				NextLong()));
			// use the same seed so we can predict the RNG
			long seedB = Runtime.CurrentTimeMillis() + rand.NextLong();
			rand = new Random(seedB);
			byte[] secretB2 = Sharpen.Runtime.GetBytesForString(System.Convert.ToString(rand.
				NextLong()));
			byte[] secretB1 = Sharpen.Runtime.GetBytesForString(System.Convert.ToString(rand.
				NextLong()));
			// use the same seed so we can predict the RNG
			long seedC = Runtime.CurrentTimeMillis() + rand.NextLong();
			rand = new Random(seedC);
			byte[] secretC2 = Sharpen.Runtime.GetBytesForString(System.Convert.ToString(rand.
				NextLong()));
			byte[] secretC1 = Sharpen.Runtime.GetBytesForString(System.Convert.ToString(rand.
				NextLong()));
			ZKSignerSecretProvider secretProviderA = new ZKSignerSecretProvider(seedA);
			ZKSignerSecretProvider secretProviderB = new ZKSignerSecretProvider(seedB);
			ZKSignerSecretProvider secretProviderC = new ZKSignerSecretProvider(seedC);
			Properties config = new Properties();
			config.SetProperty(ZKSignerSecretProvider.ZookeeperConnectionString, zkServer.GetConnectString
				());
			config.SetProperty(ZKSignerSecretProvider.ZookeeperPath, "/secret");
			try
			{
				secretProviderA.Init(config, GetDummyServletContext(), rolloverFrequency);
				secretProviderB.Init(config, GetDummyServletContext(), rolloverFrequency);
				secretProviderC.Init(config, GetDummyServletContext(), rolloverFrequency);
				byte[] currentSecretA = secretProviderA.GetCurrentSecret();
				byte[][] allSecretsA = secretProviderA.GetAllSecrets();
				byte[] currentSecretB = secretProviderB.GetCurrentSecret();
				byte[][] allSecretsB = secretProviderB.GetAllSecrets();
				byte[] currentSecretC = secretProviderC.GetCurrentSecret();
				byte[][] allSecretsC = secretProviderC.GetAllSecrets();
				Assert.AssertArrayEquals(currentSecretA, currentSecretB);
				Assert.AssertArrayEquals(currentSecretB, currentSecretC);
				NUnit.Framework.Assert.AreEqual(2, allSecretsA.Length);
				NUnit.Framework.Assert.AreEqual(2, allSecretsB.Length);
				NUnit.Framework.Assert.AreEqual(2, allSecretsC.Length);
				Assert.AssertArrayEquals(allSecretsA[0], allSecretsB[0]);
				Assert.AssertArrayEquals(allSecretsB[0], allSecretsC[0]);
				NUnit.Framework.Assert.IsNull(allSecretsA[1]);
				NUnit.Framework.Assert.IsNull(allSecretsB[1]);
				NUnit.Framework.Assert.IsNull(allSecretsC[1]);
				char secretChosen = 'z';
				if (Arrays.Equals(secretA1, currentSecretA))
				{
					Assert.AssertArrayEquals(secretA1, allSecretsA[0]);
					secretChosen = 'A';
				}
				else
				{
					if (Arrays.Equals(secretB1, currentSecretB))
					{
						Assert.AssertArrayEquals(secretB1, allSecretsA[0]);
						secretChosen = 'B';
					}
					else
					{
						if (Arrays.Equals(secretC1, currentSecretC))
						{
							Assert.AssertArrayEquals(secretC1, allSecretsA[0]);
							secretChosen = 'C';
						}
						else
						{
							NUnit.Framework.Assert.Fail("It appears that they all agreed on the same secret, but "
								 + "not one of the secrets they were supposed to");
						}
					}
				}
				Sharpen.Thread.Sleep((rolloverFrequency + 2000));
				currentSecretA = secretProviderA.GetCurrentSecret();
				allSecretsA = secretProviderA.GetAllSecrets();
				currentSecretB = secretProviderB.GetCurrentSecret();
				allSecretsB = secretProviderB.GetAllSecrets();
				currentSecretC = secretProviderC.GetCurrentSecret();
				allSecretsC = secretProviderC.GetAllSecrets();
				Assert.AssertArrayEquals(currentSecretA, currentSecretB);
				Assert.AssertArrayEquals(currentSecretB, currentSecretC);
				NUnit.Framework.Assert.AreEqual(2, allSecretsA.Length);
				NUnit.Framework.Assert.AreEqual(2, allSecretsB.Length);
				NUnit.Framework.Assert.AreEqual(2, allSecretsC.Length);
				Assert.AssertArrayEquals(allSecretsA[0], allSecretsB[0]);
				Assert.AssertArrayEquals(allSecretsB[0], allSecretsC[0]);
				Assert.AssertArrayEquals(allSecretsA[1], allSecretsB[1]);
				Assert.AssertArrayEquals(allSecretsB[1], allSecretsC[1]);
				// The second secret used is prechosen by whoever won the init; so it
				// should match with whichever we saw before
				if (secretChosen == 'A')
				{
					Assert.AssertArrayEquals(secretA2, currentSecretA);
				}
				else
				{
					if (secretChosen == 'B')
					{
						Assert.AssertArrayEquals(secretB2, currentSecretA);
					}
					else
					{
						if (secretChosen == 'C')
						{
							Assert.AssertArrayEquals(secretC2, currentSecretA);
						}
					}
				}
			}
			finally
			{
				secretProviderC.Destroy();
				secretProviderB.Destroy();
				secretProviderA.Destroy();
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestMultipleUnsychnronized()
		{
			long rolloverFrequency = 15 * 1000;
			// rollover every 15 sec
			// use the same seed so we can predict the RNG
			long seedA = Runtime.CurrentTimeMillis();
			Random rand = new Random(seedA);
			byte[] secretA2 = Sharpen.Runtime.GetBytesForString(System.Convert.ToString(rand.
				NextLong()));
			byte[] secretA1 = Sharpen.Runtime.GetBytesForString(System.Convert.ToString(rand.
				NextLong()));
			byte[] secretA3 = Sharpen.Runtime.GetBytesForString(System.Convert.ToString(rand.
				NextLong()));
			// use the same seed so we can predict the RNG
			long seedB = Runtime.CurrentTimeMillis() + rand.NextLong();
			rand = new Random(seedB);
			byte[] secretB2 = Sharpen.Runtime.GetBytesForString(System.Convert.ToString(rand.
				NextLong()));
			byte[] secretB1 = Sharpen.Runtime.GetBytesForString(System.Convert.ToString(rand.
				NextLong()));
			byte[] secretB3 = Sharpen.Runtime.GetBytesForString(System.Convert.ToString(rand.
				NextLong()));
			ZKSignerSecretProvider secretProviderA = new ZKSignerSecretProvider(seedA);
			ZKSignerSecretProvider secretProviderB = new ZKSignerSecretProvider(seedB);
			Properties config = new Properties();
			config.SetProperty(ZKSignerSecretProvider.ZookeeperConnectionString, zkServer.GetConnectString
				());
			config.SetProperty(ZKSignerSecretProvider.ZookeeperPath, "/secret");
			try
			{
				secretProviderA.Init(config, GetDummyServletContext(), rolloverFrequency);
				byte[] currentSecretA = secretProviderA.GetCurrentSecret();
				byte[][] allSecretsA = secretProviderA.GetAllSecrets();
				Assert.AssertArrayEquals(secretA1, currentSecretA);
				NUnit.Framework.Assert.AreEqual(2, allSecretsA.Length);
				Assert.AssertArrayEquals(secretA1, allSecretsA[0]);
				NUnit.Framework.Assert.IsNull(allSecretsA[1]);
				Sharpen.Thread.Sleep((rolloverFrequency + 2000));
				currentSecretA = secretProviderA.GetCurrentSecret();
				allSecretsA = secretProviderA.GetAllSecrets();
				Assert.AssertArrayEquals(secretA2, currentSecretA);
				NUnit.Framework.Assert.AreEqual(2, allSecretsA.Length);
				Assert.AssertArrayEquals(secretA2, allSecretsA[0]);
				Assert.AssertArrayEquals(secretA1, allSecretsA[1]);
				Sharpen.Thread.Sleep((rolloverFrequency / 5));
				secretProviderB.Init(config, GetDummyServletContext(), rolloverFrequency);
				byte[] currentSecretB = secretProviderB.GetCurrentSecret();
				byte[][] allSecretsB = secretProviderB.GetAllSecrets();
				Assert.AssertArrayEquals(secretA2, currentSecretB);
				NUnit.Framework.Assert.AreEqual(2, allSecretsA.Length);
				Assert.AssertArrayEquals(secretA2, allSecretsB[0]);
				Assert.AssertArrayEquals(secretA1, allSecretsB[1]);
				Sharpen.Thread.Sleep((rolloverFrequency));
				currentSecretA = secretProviderA.GetCurrentSecret();
				allSecretsA = secretProviderA.GetAllSecrets();
				currentSecretB = secretProviderB.GetCurrentSecret();
				allSecretsB = secretProviderB.GetAllSecrets();
				Assert.AssertArrayEquals(currentSecretA, currentSecretB);
				NUnit.Framework.Assert.AreEqual(2, allSecretsA.Length);
				NUnit.Framework.Assert.AreEqual(2, allSecretsB.Length);
				Assert.AssertArrayEquals(allSecretsA[0], allSecretsB[0]);
				Assert.AssertArrayEquals(allSecretsA[1], allSecretsB[1]);
				if (Arrays.Equals(secretA3, currentSecretA))
				{
					Assert.AssertArrayEquals(secretA3, allSecretsA[0]);
				}
				else
				{
					if (Arrays.Equals(secretB3, currentSecretB))
					{
						Assert.AssertArrayEquals(secretB3, allSecretsA[0]);
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
				secretProviderB.Destroy();
				secretProviderA.Destroy();
			}
		}

		private ServletContext GetDummyServletContext()
		{
			ServletContext servletContext = Org.Mockito.Mockito.Mock<ServletContext>();
			Org.Mockito.Mockito.When(servletContext.GetAttribute(ZKSignerSecretProvider.ZookeeperSignerSecretProviderCuratorClientAttribute
				)).ThenReturn(null);
			return servletContext;
		}
	}
}
