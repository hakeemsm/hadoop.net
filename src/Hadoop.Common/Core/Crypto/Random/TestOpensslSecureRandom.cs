

namespace Org.Apache.Hadoop.Crypto.Random
{
	public class TestOpensslSecureRandom
	{
		/// <exception cref="System.Exception"/>
		public virtual void TestRandomBytes()
		{
			OpensslSecureRandom random = new OpensslSecureRandom();
			// len = 16
			CheckRandomBytes(random, 16);
			// len = 32
			CheckRandomBytes(random, 32);
			// len = 128
			CheckRandomBytes(random, 128);
			// len = 256
			CheckRandomBytes(random, 256);
		}

		/// <summary>
		/// Test will timeout if secure random implementation always returns a
		/// constant value.
		/// </summary>
		private void CheckRandomBytes(OpensslSecureRandom random, int len)
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
			OpensslSecureRandom random = new OpensslSecureRandom();
			int rand1 = random.Next();
			int rand2 = random.Next();
			while (rand1 == rand2)
			{
				rand2 = random.Next();
			}
		}

		/// <summary>
		/// Test will timeout if secure random implementation always returns a
		/// constant value.
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestRandomLong()
		{
			OpensslSecureRandom random = new OpensslSecureRandom();
			long rand1 = random.NextLong();
			long rand2 = random.NextLong();
			while (rand1 == rand2)
			{
				rand2 = random.NextLong();
			}
		}

		/// <summary>
		/// Test will timeout if secure random implementation always returns a
		/// constant value.
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestRandomFloat()
		{
			OpensslSecureRandom random = new OpensslSecureRandom();
			float rand1 = random.NextFloat();
			float rand2 = random.NextFloat();
			while (rand1 == rand2)
			{
				rand2 = random.NextFloat();
			}
		}

		/// <summary>
		/// Test will timeout if secure random implementation always returns a
		/// constant value.
		/// </summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestRandomDouble()
		{
			OpensslSecureRandom random = new OpensslSecureRandom();
			double rand1 = random.NextDouble();
			double rand2 = random.NextDouble();
			while (rand1 == rand2)
			{
				rand2 = random.NextDouble();
			}
		}
	}
}
