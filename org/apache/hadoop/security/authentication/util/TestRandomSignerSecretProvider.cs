using Sharpen;

namespace org.apache.hadoop.security.authentication.util
{
	public class TestRandomSignerSecretProvider
	{
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testGetAndRollSecrets()
		{
			long rolloverFrequency = 15 * 1000;
			// rollover every 15 sec
			// use the same seed so we can predict the RNG
			long seed = Sharpen.Runtime.currentTimeMillis();
			java.util.Random rand = new java.util.Random(seed);
			byte[] secret1 = Sharpen.Runtime.getBytesForString(System.Convert.ToString(rand.nextLong
				()));
			byte[] secret2 = Sharpen.Runtime.getBytesForString(System.Convert.ToString(rand.nextLong
				()));
			byte[] secret3 = Sharpen.Runtime.getBytesForString(System.Convert.ToString(rand.nextLong
				()));
			org.apache.hadoop.security.authentication.util.RandomSignerSecretProvider secretProvider
				 = new org.apache.hadoop.security.authentication.util.RandomSignerSecretProvider
				(seed);
			try
			{
				secretProvider.init(null, null, rolloverFrequency);
				byte[] currentSecret = secretProvider.getCurrentSecret();
				byte[][] allSecrets = secretProvider.getAllSecrets();
				NUnit.Framework.Assert.assertArrayEquals(secret1, currentSecret);
				NUnit.Framework.Assert.AreEqual(2, allSecrets.Length);
				NUnit.Framework.Assert.assertArrayEquals(secret1, allSecrets[0]);
				NUnit.Framework.Assert.IsNull(allSecrets[1]);
				java.lang.Thread.sleep(rolloverFrequency + 2000);
				currentSecret = secretProvider.getCurrentSecret();
				allSecrets = secretProvider.getAllSecrets();
				NUnit.Framework.Assert.assertArrayEquals(secret2, currentSecret);
				NUnit.Framework.Assert.AreEqual(2, allSecrets.Length);
				NUnit.Framework.Assert.assertArrayEquals(secret2, allSecrets[0]);
				NUnit.Framework.Assert.assertArrayEquals(secret1, allSecrets[1]);
				java.lang.Thread.sleep(rolloverFrequency + 2000);
				currentSecret = secretProvider.getCurrentSecret();
				allSecrets = secretProvider.getAllSecrets();
				NUnit.Framework.Assert.assertArrayEquals(secret3, currentSecret);
				NUnit.Framework.Assert.AreEqual(2, allSecrets.Length);
				NUnit.Framework.Assert.assertArrayEquals(secret3, allSecrets[0]);
				NUnit.Framework.Assert.assertArrayEquals(secret2, allSecrets[1]);
				java.lang.Thread.sleep(rolloverFrequency + 2000);
			}
			finally
			{
				secretProvider.destroy();
			}
		}
	}
}
