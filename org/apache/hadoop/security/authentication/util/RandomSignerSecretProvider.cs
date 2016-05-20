using Sharpen;

namespace org.apache.hadoop.security.authentication.util
{
	/// <summary>A SignerSecretProvider that uses a random number as its secret.</summary>
	/// <remarks>
	/// A SignerSecretProvider that uses a random number as its secret.  It rolls
	/// the secret at a regular interval.
	/// </remarks>
	public class RandomSignerSecretProvider : org.apache.hadoop.security.authentication.util.RolloverSignerSecretProvider
	{
		private readonly java.util.Random rand;

		public RandomSignerSecretProvider()
			: base()
		{
			rand = new java.util.Random();
		}

		/// <summary>
		/// This constructor lets you set the seed of the Random Number Generator and
		/// is meant for testing.
		/// </summary>
		/// <param name="seed">the seed for the random number generator</param>
		[com.google.common.annotations.VisibleForTesting]
		public RandomSignerSecretProvider(long seed)
			: base()
		{
			rand = new java.util.Random(seed);
		}

		protected internal override byte[] generateNewSecret()
		{
			return Sharpen.Runtime.getBytesForString(System.Convert.ToString(rand.nextLong())
				, java.nio.charset.Charset.forName("UTF-8"));
		}
	}
}
