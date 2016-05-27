using System.Text;
using Com.Google.Common.Annotations;
using Sharpen;

namespace Org.Apache.Hadoop.Security.Authentication.Util
{
	/// <summary>A SignerSecretProvider that uses a random number as its secret.</summary>
	/// <remarks>
	/// A SignerSecretProvider that uses a random number as its secret.  It rolls
	/// the secret at a regular interval.
	/// </remarks>
	public class RandomSignerSecretProvider : RolloverSignerSecretProvider
	{
		private readonly Random rand;

		public RandomSignerSecretProvider()
			: base()
		{
			rand = new Random();
		}

		/// <summary>
		/// This constructor lets you set the seed of the Random Number Generator and
		/// is meant for testing.
		/// </summary>
		/// <param name="seed">the seed for the random number generator</param>
		[VisibleForTesting]
		public RandomSignerSecretProvider(long seed)
			: base()
		{
			rand = new Random(seed);
		}

		protected internal override byte[] GenerateNewSecret()
		{
			return Sharpen.Runtime.GetBytesForString(System.Convert.ToString(rand.NextLong())
				, Sharpen.Extensions.GetEncoding("UTF-8"));
		}
	}
}
