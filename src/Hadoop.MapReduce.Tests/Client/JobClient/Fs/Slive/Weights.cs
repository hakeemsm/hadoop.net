using System;
using Sharpen;

namespace Org.Apache.Hadoop.FS.Slive
{
	/// <summary>Class to isolate the various weight algorithms we use.</summary>
	internal class Weights
	{
		private Weights()
		{
		}

		/// <summary>A weight which always returns the same weight (1/3).</summary>
		/// <remarks>
		/// A weight which always returns the same weight (1/3). Which will have an
		/// overall area of (1/3) unless otherwise provided.
		/// </remarks>
		internal class UniformWeight : WeightSelector.Weightable
		{
			private static double DefaultWeight = (1.0d / 3.0d);

			private double weight;

			internal UniformWeight(double w)
			{
				weight = w;
			}

			internal UniformWeight()
				: this(DefaultWeight)
			{
			}

			public virtual double Weight(int elapsed, int duration)
			{
				// Weightable
				return weight;
			}
		}

		/// <summary>
		/// A weight which normalized the elapsed time and the duration to a value
		/// between 0 and 1 and applies the algorithm to form an output using the
		/// function (-2 * (x-0.5)^2) + 0.5 which initially (close to 0) has a value
		/// close to 0 and near input being 1 has a value close to 0 and near 0.5 has a
		/// value close to 0.5 (with overall area 0.3).
		/// </summary>
		internal class MidWeight : WeightSelector.Weightable
		{
			public virtual double Weight(int elapsed, int duration)
			{
				// Weightable
				double normalized = (double)elapsed / (double)duration;
				double result = (-2.0d * Math.Pow(normalized - 0.5, 2)) + 0.5d;
				if (result < 0)
				{
					result = 0;
				}
				if (result > 1)
				{
					result = 1;
				}
				return result;
			}
		}

		/// <summary>
		/// A weight which normalized the elapsed time and the duration to a value
		/// between 0 and 1 and applies the algorithm to form an output using the
		/// function (x)^2 which initially (close to 0) has a value close to 0 and near
		/// input being 1 has a value close to 1 (with overall area 1/3).
		/// </summary>
		internal class EndWeight : WeightSelector.Weightable
		{
			public virtual double Weight(int elapsed, int duration)
			{
				// Weightable
				double normalized = (double)elapsed / (double)duration;
				double result = Math.Pow(normalized, 2);
				if (result < 0)
				{
					result = 0;
				}
				if (result > 1)
				{
					result = 1;
				}
				return result;
			}
		}

		/// <summary>
		/// A weight which normalized the elapsed time and the duration to a value
		/// between 0 and 1 and applies the algorithm to form an output using the
		/// function (x-1)^2 which initially (close to 0) has a value close to 1 and
		/// near input being 1 has a value close to 0 (with overall area 1/3).
		/// </summary>
		internal class BeginWeight : WeightSelector.Weightable
		{
			public virtual double Weight(int elapsed, int duration)
			{
				// Weightable
				double normalized = (double)elapsed / (double)duration;
				double result = Math.Pow((normalized - 1), 2);
				if (result < 0)
				{
					result = 0;
				}
				if (result > 1)
				{
					result = 1;
				}
				return result;
			}
		}
	}
}
