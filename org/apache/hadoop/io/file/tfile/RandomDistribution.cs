using Sharpen;

namespace org.apache.hadoop.io.file.tfile
{
	/// <summary>A class that generates random numbers that follow some distribution.</summary>
	public class RandomDistribution
	{
		/// <summary>Interface for discrete (integer) random distributions.</summary>
		public interface DiscreteRNG
		{
			/// <summary>Get the next random number</summary>
			/// <returns>the next random number.</returns>
			int nextInt();
		}

		/// <summary>P(i)=1/(max-min)</summary>
		public sealed class Flat : org.apache.hadoop.io.file.tfile.RandomDistribution.DiscreteRNG
		{
			private readonly java.util.Random random;

			private readonly int min;

			private readonly int max;

			/// <summary>
			/// Generate random integers from min (inclusive) to max (exclusive)
			/// following even distribution.
			/// </summary>
			/// <param name="random">The basic random number generator.</param>
			/// <param name="min">Minimum integer</param>
			/// <param name="max">maximum integer (exclusive).</param>
			public Flat(java.util.Random random, int min, int max)
			{
				if (min >= max)
				{
					throw new System.ArgumentException("Invalid range");
				}
				this.random = random;
				this.min = min;
				this.max = max;
			}

			/// <seealso cref="DiscreteRNG.nextInt()"/>
			public int nextInt()
			{
				return random.nextInt(max - min) + min;
			}
		}

		/// <summary>Zipf distribution.</summary>
		/// <remarks>
		/// Zipf distribution. The ratio of the probabilities of integer i and j is
		/// defined as follows:
		/// P(i)/P(j)=((j-min+1)/(i-min+1))^sigma.
		/// </remarks>
		public sealed class Zipf : org.apache.hadoop.io.file.tfile.RandomDistribution.DiscreteRNG
		{
			private const double DEFAULT_EPSILON = 0.001;

			private readonly java.util.Random random;

			private readonly System.Collections.Generic.List<int> k;

			private readonly System.Collections.Generic.List<double> v;

			/// <summary>Constructor</summary>
			/// <param name="r">The random number generator.</param>
			/// <param name="min">minimum integer (inclusvie)</param>
			/// <param name="max">maximum integer (exclusive)</param>
			/// <param name="sigma">parameter sigma. (sigma &gt; 1.0)</param>
			public Zipf(java.util.Random r, int min, int max, double sigma)
				: this(r, min, max, sigma, DEFAULT_EPSILON)
			{
			}

			/// <summary>Constructor.</summary>
			/// <param name="r">The random number generator.</param>
			/// <param name="min">minimum integer (inclusvie)</param>
			/// <param name="max">maximum integer (exclusive)</param>
			/// <param name="sigma">parameter sigma. (sigma &gt; 1.0)</param>
			/// <param name="epsilon">Allowable error percentage (0 &lt; epsilon &lt; 1.0).</param>
			public Zipf(java.util.Random r, int min, int max, double sigma, double epsilon)
			{
				if ((max <= min) || (sigma <= 1) || (epsilon <= 0) || (epsilon >= 0.5))
				{
					throw new System.ArgumentException("Invalid arguments");
				}
				random = r;
				k = new System.Collections.Generic.List<int>();
				v = new System.Collections.Generic.List<double>();
				double sum = 0;
				int last = -1;
				for (int i = min; i < max; ++i)
				{
					sum += System.Math.exp(-sigma * System.Math.log(i - min + 1));
					if ((last == -1) || i * (1 - epsilon) > last)
					{
						k.add(i);
						v.add(sum);
						last = i;
					}
				}
				if (last != max - 1)
				{
					k.add(max - 1);
					v.add(sum);
				}
				v.set(v.Count - 1, 1.0);
				for (int i_1 = v.Count - 2; i_1 >= 0; --i_1)
				{
					v.set(i_1, v[i_1] / sum);
				}
			}

			/// <seealso cref="DiscreteRNG.nextInt()"/>
			public int nextInt()
			{
				double d = random.nextDouble();
				int idx = java.util.Collections.binarySearch(v, d);
				if (idx > 0)
				{
					++idx;
				}
				else
				{
					idx = -(idx + 1);
				}
				if (idx >= v.Count)
				{
					idx = v.Count - 1;
				}
				if (idx == 0)
				{
					return k[0];
				}
				int ceiling = k[idx];
				int lower = k[idx - 1];
				return ceiling - random.nextInt(ceiling - lower);
			}
		}

		/// <summary>Binomial distribution.</summary>
		/// <remarks>
		/// Binomial distribution.
		/// P(k)=select(n, k)*p^k*(1-p)^(n-k) (k = 0, 1, ..., n)
		/// P(k)=select(max-min-1, k-min)*p^(k-min)*(1-p)^(k-min)*(1-p)^(max-k-1)
		/// </remarks>
		public sealed class Binomial : org.apache.hadoop.io.file.tfile.RandomDistribution.DiscreteRNG
		{
			private readonly java.util.Random random;

			private readonly int min;

			private readonly int n;

			private readonly double[] v;

			private static double select(int n, int k)
			{
				double ret = 1.0;
				for (int i = k + 1; i <= n; ++i)
				{
					ret *= (double)i / (i - k);
				}
				return ret;
			}

			private static double power(double p, int k)
			{
				return System.Math.exp(k * System.Math.log(p));
			}

			/// <summary>
			/// Generate random integers from min (inclusive) to max (exclusive)
			/// following Binomial distribution.
			/// </summary>
			/// <param name="random">The basic random number generator.</param>
			/// <param name="min">Minimum integer</param>
			/// <param name="max">maximum integer (exclusive).</param>
			/// <param name="p">parameter.</param>
			public Binomial(java.util.Random random, int min, int max, double p)
			{
				if (min >= max)
				{
					throw new System.ArgumentException("Invalid range");
				}
				this.random = random;
				this.min = min;
				this.n = max - min - 1;
				if (n > 0)
				{
					v = new double[n + 1];
					double sum = 0.0;
					for (int i = 0; i <= n; ++i)
					{
						sum += select(n, i) * power(p, i) * power(1 - p, n - i);
						v[i] = sum;
					}
					for (int i_1 = 0; i_1 <= n; ++i_1)
					{
						v[i_1] /= sum;
					}
				}
				else
				{
					v = null;
				}
			}

			/// <seealso cref="DiscreteRNG.nextInt()"/>
			public int nextInt()
			{
				if (v == null)
				{
					return min;
				}
				double d = random.nextDouble();
				int idx = java.util.Arrays.binarySearch(v, d);
				if (idx > 0)
				{
					++idx;
				}
				else
				{
					idx = -(idx + 1);
				}
				if (idx >= v.Length)
				{
					idx = v.Length - 1;
				}
				return idx + min;
			}
		}
	}
}
