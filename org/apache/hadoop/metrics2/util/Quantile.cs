using Sharpen;

namespace org.apache.hadoop.metrics2.util
{
	/// <summary>
	/// Specifies a quantile (with error bounds) to be watched by a
	/// <see cref="SampleQuantiles"/>
	/// object.
	/// </summary>
	public class Quantile : java.lang.Comparable<org.apache.hadoop.metrics2.util.Quantile
		>
	{
		public readonly double quantile;

		public readonly double error;

		public Quantile(double quantile, double error)
		{
			this.quantile = quantile;
			this.error = error;
		}

		public override bool Equals(object aThat)
		{
			if (this == aThat)
			{
				return true;
			}
			if (!(aThat is org.apache.hadoop.metrics2.util.Quantile))
			{
				return false;
			}
			org.apache.hadoop.metrics2.util.Quantile that = (org.apache.hadoop.metrics2.util.Quantile
				)aThat;
			long qbits = double.doubleToLongBits(quantile);
			long ebits = double.doubleToLongBits(error);
			return qbits == double.doubleToLongBits(that.quantile) && ebits == double.doubleToLongBits
				(that.error);
		}

		public override int GetHashCode()
		{
			return (int)(double.doubleToLongBits(quantile) ^ double.doubleToLongBits(error));
		}

		public virtual int compareTo(org.apache.hadoop.metrics2.util.Quantile other)
		{
			return com.google.common.collect.ComparisonChain.start().compare(quantile, other.
				quantile).compare(error, other.error).result();
		}

		public override string ToString()
		{
			return string.format("%.2f %%ile +/- %.2f%%", quantile * 100, error * 100);
		}
	}
}
