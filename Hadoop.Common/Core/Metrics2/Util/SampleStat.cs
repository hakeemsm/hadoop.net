using System;
using Sharpen;

namespace Org.Apache.Hadoop.Metrics2.Util
{
	/// <summary>Helper to compute running sample stats</summary>
	public class SampleStat
	{
		private readonly SampleStat.MinMax minmax = new SampleStat.MinMax();

		private long numSamples = 0;

		private double a0;

		private double a1;

		private double s0;

		private double s1;

		/// <summary>Construct a new running sample stat</summary>
		public SampleStat()
		{
			a0 = s0 = 0.0;
		}

		public virtual void Reset()
		{
			numSamples = 0;
			a0 = s0 = 0.0;
			minmax.Reset();
		}

		// We want to reuse the object, sometimes.
		internal virtual void Reset(long numSamples, double a0, double a1, double s0, double
			 s1, SampleStat.MinMax minmax)
		{
			this.numSamples = numSamples;
			this.a0 = a0;
			this.a1 = a1;
			this.s0 = s0;
			this.s1 = s1;
			this.minmax.Reset(minmax);
		}

		/// <summary>Copy the values to other (saves object creation and gc.)</summary>
		/// <param name="other">the destination to hold our values</param>
		public virtual void CopyTo(Org.Apache.Hadoop.Metrics2.Util.SampleStat other)
		{
			other.Reset(numSamples, a0, a1, s0, s1, minmax);
		}

		/// <summary>Add a sample the running stat.</summary>
		/// <param name="x">the sample number</param>
		/// <returns>self</returns>
		public virtual Org.Apache.Hadoop.Metrics2.Util.SampleStat Add(double x)
		{
			minmax.Add(x);
			return Add(1, x);
		}

		/// <summary>Add some sample and a partial sum to the running stat.</summary>
		/// <remarks>
		/// Add some sample and a partial sum to the running stat.
		/// Note, min/max is not evaluated using this method.
		/// </remarks>
		/// <param name="nSamples">number of samples</param>
		/// <param name="x">the partial sum</param>
		/// <returns>self</returns>
		public virtual Org.Apache.Hadoop.Metrics2.Util.SampleStat Add(long nSamples, double
			 x)
		{
			numSamples += nSamples;
			if (numSamples == 1)
			{
				a0 = a1 = x;
				s0 = 0.0;
			}
			else
			{
				// The Welford method for numerical stability
				a1 = a0 + (x - a0) / numSamples;
				s1 = s0 + (x - a0) * (x - a1);
				a0 = a1;
				s0 = s1;
			}
			return this;
		}

		/// <returns>the total number of samples</returns>
		public virtual long NumSamples()
		{
			return numSamples;
		}

		/// <returns>the arithmetic mean of the samples</returns>
		public virtual double Mean()
		{
			return numSamples > 0 ? a1 : 0.0;
		}

		/// <returns>the variance of the samples</returns>
		public virtual double Variance()
		{
			return numSamples > 1 ? s1 / (numSamples - 1) : 0.0;
		}

		/// <returns>the standard deviation of the samples</returns>
		public virtual double Stddev()
		{
			return Math.Sqrt(Variance());
		}

		/// <returns>the minimum value of the samples</returns>
		public virtual double Min()
		{
			return minmax.Min();
		}

		/// <returns>the maximum value of the samples</returns>
		public virtual double Max()
		{
			return minmax.Max();
		}

		/// <summary>Helper to keep running min/max</summary>
		public class MinMax
		{
			internal const double DefaultMinValue = float.MaxValue;

			internal const double DefaultMaxValue = float.MinValue;

			private double min = DefaultMinValue;

			private double max = DefaultMaxValue;

			// Float.MAX_VALUE is used rather than Double.MAX_VALUE, even though the
			// min and max variables are of type double.
			// Float.MAX_VALUE is big enough, and using Double.MAX_VALUE makes 
			// Ganglia core due to buffer overflow.
			// The same reasoning applies to the MIN_VALUE counterparts.
			public virtual void Add(double value)
			{
				if (value > max)
				{
					max = value;
				}
				if (value < min)
				{
					min = value;
				}
			}

			public virtual double Min()
			{
				return min;
			}

			public virtual double Max()
			{
				return max;
			}

			public virtual void Reset()
			{
				min = DefaultMinValue;
				max = DefaultMaxValue;
			}

			public virtual void Reset(SampleStat.MinMax other)
			{
				min = other.Min();
				max = other.Max();
			}
		}
	}
}
