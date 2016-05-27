using System;
using Sharpen;

namespace Org.Apache.Hadoop.Util.Bloom
{
	/// <summary>Implements a hash object that returns a certain number of hashed values.
	/// 	</summary>
	/// <seealso cref="Key">The general behavior of a key being stored in a filter</seealso>
	/// <seealso cref="Filter">The general behavior of a filter</seealso>
	public sealed class HashFunction
	{
		/// <summary>The number of hashed values.</summary>
		private int nbHash;

		/// <summary>The maximum highest returned value.</summary>
		private int maxValue;

		/// <summary>Hashing algorithm to use.</summary>
		private Org.Apache.Hadoop.Util.Hash.Hash hashFunction;

		/// <summary>Constructor.</summary>
		/// <remarks>
		/// Constructor.
		/// <p>
		/// Builds a hash function that must obey to a given maximum number of returned values and a highest value.
		/// </remarks>
		/// <param name="maxValue">The maximum highest returned value.</param>
		/// <param name="nbHash">The number of resulting hashed values.</param>
		/// <param name="hashType">
		/// type of the hashing function (see
		/// <see cref="Org.Apache.Hadoop.Util.Hash.Hash"/>
		/// ).
		/// </param>
		public HashFunction(int maxValue, int nbHash, int hashType)
		{
			if (maxValue <= 0)
			{
				throw new ArgumentException("maxValue must be > 0");
			}
			if (nbHash <= 0)
			{
				throw new ArgumentException("nbHash must be > 0");
			}
			this.maxValue = maxValue;
			this.nbHash = nbHash;
			this.hashFunction = Org.Apache.Hadoop.Util.Hash.Hash.GetInstance(hashType);
			if (this.hashFunction == null)
			{
				throw new ArgumentException("hashType must be known");
			}
		}

		/// <summary>Clears <i>this</i> hash function.</summary>
		/// <remarks>Clears <i>this</i> hash function. A NOOP</remarks>
		public void Clear()
		{
		}

		/// <summary>Hashes a specified key into several integers.</summary>
		/// <param name="k">The specified key.</param>
		/// <returns>The array of hashed values.</returns>
		public int[] Hash(Key k)
		{
			byte[] b = k.GetBytes();
			if (b == null)
			{
				throw new ArgumentNullException("buffer reference is null");
			}
			if (b.Length == 0)
			{
				throw new ArgumentException("key length must be > 0");
			}
			int[] result = new int[nbHash];
			for (int i = 0; i < nbHash; i++)
			{
				initval = hashFunction.Hash(b, initval);
				result[i] = Math.Abs(initval % maxValue);
			}
			return result;
		}
	}
}
