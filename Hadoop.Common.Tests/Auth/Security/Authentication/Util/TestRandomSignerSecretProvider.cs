using NUnit.Framework;
using Sharpen;

namespace Org.Apache.Hadoop.Security.Authentication.Util
{
	public class TestRandomSignerSecretProvider
	{
		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestGetAndRollSecrets()
		{
			long rolloverFrequency = 15 * 1000;
			// rollover every 15 sec
			// use the same seed so we can predict the RNG
			long seed = Runtime.CurrentTimeMillis();
			Random rand = new Random(seed);
			byte[] secret1 = Sharpen.Runtime.GetBytesForString(System.Convert.ToString(rand.NextLong
				()));
			byte[] secret2 = Sharpen.Runtime.GetBytesForString(System.Convert.ToString(rand.NextLong
				()));
			byte[] secret3 = Sharpen.Runtime.GetBytesForString(System.Convert.ToString(rand.NextLong
				()));
			RandomSignerSecretProvider secretProvider = new RandomSignerSecretProvider(seed);
			try
			{
				secretProvider.Init(null, null, rolloverFrequency);
				byte[] currentSecret = secretProvider.GetCurrentSecret();
				byte[][] allSecrets = secretProvider.GetAllSecrets();
				Assert.AssertArrayEquals(secret1, currentSecret);
				Assert.Equal(2, allSecrets.Length);
				Assert.AssertArrayEquals(secret1, allSecrets[0]);
				NUnit.Framework.Assert.IsNull(allSecrets[1]);
				Sharpen.Thread.Sleep(rolloverFrequency + 2000);
				currentSecret = secretProvider.GetCurrentSecret();
				allSecrets = secretProvider.GetAllSecrets();
				Assert.AssertArrayEquals(secret2, currentSecret);
				Assert.Equal(2, allSecrets.Length);
				Assert.AssertArrayEquals(secret2, allSecrets[0]);
				Assert.AssertArrayEquals(secret1, allSecrets[1]);
				Sharpen.Thread.Sleep(rolloverFrequency + 2000);
				currentSecret = secretProvider.GetCurrentSecret();
				allSecrets = secretProvider.GetAllSecrets();
				Assert.AssertArrayEquals(secret3, currentSecret);
				Assert.Equal(2, allSecrets.Length);
				Assert.AssertArrayEquals(secret3, allSecrets[0]);
				Assert.AssertArrayEquals(secret2, allSecrets[1]);
				Sharpen.Thread.Sleep(rolloverFrequency + 2000);
			}
			finally
			{
				secretProvider.Destroy();
			}
		}
	}
}
