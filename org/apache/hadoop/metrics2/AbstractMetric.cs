using Sharpen;

namespace org.apache.hadoop.metrics2
{
	/// <summary>The immutable metric</summary>
	public abstract class AbstractMetric : org.apache.hadoop.metrics2.MetricsInfo
	{
		private readonly org.apache.hadoop.metrics2.MetricsInfo info;

		/// <summary>Construct the metric</summary>
		/// <param name="info">about the metric</param>
		protected internal AbstractMetric(org.apache.hadoop.metrics2.MetricsInfo info)
		{
			this.info = com.google.common.@base.Preconditions.checkNotNull(info, "metric info"
				);
		}

		public virtual string name()
		{
			return info.name();
		}

		public virtual string description()
		{
			return info.description();
		}

		protected internal virtual org.apache.hadoop.metrics2.MetricsInfo info()
		{
			return info;
		}

		/// <summary>Get the value of the metric</summary>
		/// <returns>the value of the metric</returns>
		public abstract java.lang.Number value();

		/// <summary>Get the type of the metric</summary>
		/// <returns>the type of the metric</returns>
		public abstract org.apache.hadoop.metrics2.MetricType type();

		/// <summary>Accept a visitor interface</summary>
		/// <param name="visitor">of the metric</param>
		public abstract void visit(org.apache.hadoop.metrics2.MetricsVisitor visitor);

		public override bool Equals(object obj)
		{
			if (obj is org.apache.hadoop.metrics2.AbstractMetric)
			{
				org.apache.hadoop.metrics2.AbstractMetric other = (org.apache.hadoop.metrics2.AbstractMetric
					)obj;
				return com.google.common.@base.Objects.equal(info, other.info()) && com.google.common.@base.Objects
					.equal(value(), other.value());
			}
			return false;
		}

		public override int GetHashCode()
		{
			return com.google.common.@base.Objects.hashCode(info, value());
		}

		public override string ToString()
		{
			return com.google.common.@base.Objects.toStringHelper(this).add("info", info).add
				("value", value()).ToString();
		}
	}
}
