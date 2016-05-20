using Sharpen;

namespace org.apache.hadoop.metrics2.util
{
	/// <summary>Helper to compute running sample stats</summary>
	public class SampleStat
	{
		private readonly org.apache.hadoop.metrics2.util.SampleStat.MinMax minmax = new org.apache.hadoop.metrics2.util.SampleStat.MinMax
			();

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

		public virtual void reset()
		{
			numSamples = 0;
			a0 = s0 = 0.0;
			minmax.reset();
		}

		// We want to reuse the object, sometimes.
		internal virtual void reset(long numSamples, double a0, double a1, double s0, double
			 s1, org.apache.hadoop.metrics2.util.SampleStat.MinMax minmax)
		{
			this.numSamples = numSamples;
			this.a0 = a0;
			this.a1 = a1;
			this.s0 = s0;
			this.s1 = s1;
			this.minmax.reset(minmax);
		}

		/// <summary>Copy the values to other (saves object creation and gc.)</summary>
		/// <param name="other">the destination to hold our values</param>
		public virtual void copyTo(org.apache.hadoop.metrics2.util.SampleStat other)
		{
			other.reset(numSamples, a0, a1, s0, s1, minmax);
		}

		/// <summary>Add a sample the running stat.</summary>
		/// <param name="x">the sample number</param>
		/// <returns>self</returns>
		public virtual org.apache.hadoop.metrics2.util.SampleStat add(double x)
		{
			minmax.add(x);
			return add(1, x);
		}

		/// <summary>Add some sample and a partial sum to the running stat.</summary>
		/// <remarks>
		/// Add some sample and a partial sum to the running stat.
		/// Note, min/max is not evaluated using this method.
		/// </remarks>
		/// <param name="nSamples">number of samples</param>
		/// <param name="x">the partial sum</param>
		/// <returns>self</returns>
		public virtual org.apache.hadoop.metrics2.util.SampleStat add(long nSamples, double
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
		public virtual long numSamples()
		{
			return numSamples;
		}

		/// <returns>the arithmetic mean of the samples</returns>
		public virtual double mean()
		{
			return numSamples > 0 ? a1 : 0.0;
		}

		/// <returns>the variance of the samples</returns>
		public virtual double variance()
		{
			return numSamples > 1 ? s1 / (numSamples - 1) : 0.0;
		}

		/// <returns>the standard deviation of the samples</returns>
		public virtual double stddev()
		{
			return System.Math.sqrt(variance());
		}

		/// <returns>the minimum value of the samples</returns>
		public virtual double min()
		{
			return minmax.min();
		}

		/// <returns>the maximum value of the samples</returns>
		public virtual double max()
		{
			return minmax.max();
		}

		/// <summary>Helper to keep running min/max</summary>
		public class MinMax
		{
			internal const double DEFAULT_MIN_VALUE = float.MaxValue;

			internal const double DEFAULT_MAX_VALUE = float.MinValue;

			private double min = DEFAULT_MIN_VALUE;

			private double max = DEFAULT_MAX_VALUE;

			// Float.MAX_VALUE is used rather than Double.MAX_VALUE, even though the
			// min and max variables are of type double.
			// Float.MAX_VALUE is big enough, and using Double.MAX_VALUE makes 
			// Ganglia core due to buffer overflow.
			// The same reasoning applies to the MIN_VALUE counterparts.
			public virtual void add(double value)
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

			public virtual double min()
			{
				return min;
			}

			public virtual double max()
			{
				return max;
			}

			public virtual void reset()
			{
				min = DEFAULT_MIN_VALUE;
				max = DEFAULT_MAX_VALUE;
			}

			public virtual void reset(org.apache.hadoop.metrics2.util.SampleStat.MinMax other
				)
			{
				min = other.min();
				max = other.max();
			}
		}
	}
}
