using Com.Google.Common.Collect;


namespace Org.Apache.Hadoop.Metrics2.Util
{
	/// <summary>
	/// Specifies a quantile (with error bounds) to be watched by a
	/// <see cref="SampleQuantiles"/>
	/// object.
	/// </summary>
	public class Quantile : Comparable<Org.Apache.Hadoop.Metrics2.Util.Quantile>
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
			if (!(aThat is Org.Apache.Hadoop.Metrics2.Util.Quantile))
			{
				return false;
			}
			Org.Apache.Hadoop.Metrics2.Util.Quantile that = (Org.Apache.Hadoop.Metrics2.Util.Quantile
				)aThat;
			long qbits = double.DoubleToLongBits(quantile);
			long ebits = double.DoubleToLongBits(error);
			return qbits == double.DoubleToLongBits(that.quantile) && ebits == double.DoubleToLongBits
				(that.error);
		}

		public override int GetHashCode()
		{
			return (int)(double.DoubleToLongBits(quantile) ^ double.DoubleToLongBits(error));
		}

		public virtual int CompareTo(Org.Apache.Hadoop.Metrics2.Util.Quantile other)
		{
			return ComparisonChain.Start().Compare(quantile, other.quantile).Compare(error, other
				.error).Result();
		}

		public override string ToString()
		{
			return string.Format("%.2f %%ile +/- %.2f%%", quantile * 100, error * 100);
		}
	}
}
