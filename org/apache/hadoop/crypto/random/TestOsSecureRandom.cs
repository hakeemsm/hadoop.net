using Sharpen;

namespace org.apache.hadoop.crypto.random
{
	public class TestOsSecureRandom
	{
		/// <exception cref="System.IO.IOException"/>
		private static org.apache.hadoop.crypto.random.OsSecureRandom getOsSecureRandom()
		{
			NUnit.Framework.Assume.assumeTrue(org.apache.commons.lang.SystemUtils.IS_OS_LINUX
				);
			org.apache.hadoop.crypto.random.OsSecureRandom random = new org.apache.hadoop.crypto.random.OsSecureRandom
				();
			random.setConf(new org.apache.hadoop.conf.Configuration());
			return random;
		}

		/// <exception cref="System.Exception"/>
		public virtual void testRandomBytes()
		{
			org.apache.hadoop.crypto.random.OsSecureRandom random = getOsSecureRandom();
			// len = 16
			checkRandomBytes(random, 16);
			// len = 32
			checkRandomBytes(random, 32);
			// len = 128
			checkRandomBytes(random, 128);
			// len = 256
			checkRandomBytes(random, 256);
			random.close();
		}

		/// <summary>
		/// Test will timeout if secure random implementation always returns a
		/// constant value.
		/// </summary>
		private void checkRandomBytes(org.apache.hadoop.crypto.random.OsSecureRandom random
			, int len)
		{
			byte[] bytes = new byte[len];
			byte[] bytes1 = new byte[len];
			random.nextBytes(bytes);
			random.nextBytes(bytes1);
			while (java.util.Arrays.equals(bytes, bytes1))
			{
				random.nextBytes(bytes1);
			}
		}

		/// <summary>
		/// Test will timeout if secure random implementation always returns a
		/// constant value.
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void testRandomInt()
		{
			org.apache.hadoop.crypto.random.OsSecureRandom random = getOsSecureRandom();
			int rand1 = random.nextInt();
			int rand2 = random.nextInt();
			while (rand1 == rand2)
			{
				rand2 = random.nextInt();
			}
			random.close();
		}

		/// <summary>
		/// Test will timeout if secure random implementation always returns a
		/// constant value.
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void testRandomLong()
		{
			org.apache.hadoop.crypto.random.OsSecureRandom random = getOsSecureRandom();
			long rand1 = random.nextLong();
			long rand2 = random.nextLong();
			while (rand1 == rand2)
			{
				rand2 = random.nextLong();
			}
			random.close();
		}

		/// <summary>
		/// Test will timeout if secure random implementation always returns a
		/// constant value.
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void testRandomFloat()
		{
			org.apache.hadoop.crypto.random.OsSecureRandom random = getOsSecureRandom();
			float rand1 = random.nextFloat();
			float rand2 = random.nextFloat();
			while (rand1 == rand2)
			{
				rand2 = random.nextFloat();
			}
			random.close();
		}

		/// <summary>
		/// Test will timeout if secure random implementation always returns a
		/// constant value.
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void testRandomDouble()
		{
			org.apache.hadoop.crypto.random.OsSecureRandom random = getOsSecureRandom();
			double rand1 = random.nextDouble();
			double rand2 = random.nextDouble();
			while (rand1 == rand2)
			{
				rand2 = random.nextDouble();
			}
			random.close();
		}

		/// <exception cref="System.Exception"/>
		public virtual void testRefillReservoir()
		{
			org.apache.hadoop.crypto.random.OsSecureRandom random = getOsSecureRandom();
			for (int i = 0; i < 8196; i++)
			{
				random.nextLong();
			}
			random.close();
		}
	}
}
