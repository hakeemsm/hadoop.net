using NUnit.Framework;
using Org.Apache.Commons.Lang;
using Org.Apache.Hadoop.Conf;


namespace Org.Apache.Hadoop.Crypto.Random
{
	public class TestOsSecureRandom
	{
		/// <exception cref="System.IO.IOException"/>
		private static OsSecureRandom GetOsSecureRandom()
		{
			Assume.AssumeTrue(SystemUtils.IsOsLinux);
			OsSecureRandom random = new OsSecureRandom();
			random.SetConf(new Configuration());
			return random;
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestRandomBytes()
		{
			OsSecureRandom random = GetOsSecureRandom();
			// len = 16
			CheckRandomBytes(random, 16);
			// len = 32
			CheckRandomBytes(random, 32);
			// len = 128
			CheckRandomBytes(random, 128);
			// len = 256
			CheckRandomBytes(random, 256);
			random.Close();
		}

		/// <summary>
		/// Test will timeout if secure random implementation always returns a
		/// constant value.
		/// </summary>
		private void CheckRandomBytes(OsSecureRandom random, int len)
		{
			byte[] bytes = new byte[len];
			byte[] bytes1 = new byte[len];
			random.NextBytes(bytes);
			random.NextBytes(bytes1);
			while (Arrays.Equals(bytes, bytes1))
			{
				random.NextBytes(bytes1);
			}
		}

		/// <summary>
		/// Test will timeout if secure random implementation always returns a
		/// constant value.
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestRandomInt()
		{
			OsSecureRandom random = GetOsSecureRandom();
			int rand1 = random.Next();
			int rand2 = random.Next();
			while (rand1 == rand2)
			{
				rand2 = random.Next();
			}
			random.Close();
		}

		/// <summary>
		/// Test will timeout if secure random implementation always returns a
		/// constant value.
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestRandomLong()
		{
			OsSecureRandom random = GetOsSecureRandom();
			long rand1 = random.NextLong();
			long rand2 = random.NextLong();
			while (rand1 == rand2)
			{
				rand2 = random.NextLong();
			}
			random.Close();
		}

		/// <summary>
		/// Test will timeout if secure random implementation always returns a
		/// constant value.
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestRandomFloat()
		{
			OsSecureRandom random = GetOsSecureRandom();
			float rand1 = random.NextFloat();
			float rand2 = random.NextFloat();
			while (rand1 == rand2)
			{
				rand2 = random.NextFloat();
			}
			random.Close();
		}

		/// <summary>
		/// Test will timeout if secure random implementation always returns a
		/// constant value.
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestRandomDouble()
		{
			OsSecureRandom random = GetOsSecureRandom();
			double rand1 = random.NextDouble();
			double rand2 = random.NextDouble();
			while (rand1 == rand2)
			{
				rand2 = random.NextDouble();
			}
			random.Close();
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestRefillReservoir()
		{
			OsSecureRandom random = GetOsSecureRandom();
			for (int i = 0; i < 8196; i++)
			{
				random.NextLong();
			}
			random.Close();
		}
	}
}
