using Sharpen;

namespace org.apache.hadoop.metrics2
{
	/// <summary>Immutable tag for metrics (for grouping on host/queue/username etc.)</summary>
	public class MetricsTag : org.apache.hadoop.metrics2.MetricsInfo
	{
		private readonly org.apache.hadoop.metrics2.MetricsInfo info;

		private readonly string value;

		/// <summary>Construct the tag with name, description and value</summary>
		/// <param name="info">of the tag</param>
		/// <param name="value">of the tag</param>
		public MetricsTag(org.apache.hadoop.metrics2.MetricsInfo info, string value)
		{
			this.info = com.google.common.@base.Preconditions.checkNotNull(info, "tag info");
			this.value = value;
		}

		public virtual string name()
		{
			return info.name();
		}

		public virtual string description()
		{
			return info.description();
		}

		/// <returns>the info object of the tag</returns>
		public virtual org.apache.hadoop.metrics2.MetricsInfo info()
		{
			return info;
		}

		/// <summary>Get the value of the tag</summary>
		/// <returns>the value</returns>
		public virtual string value()
		{
			return value;
		}

		public override bool Equals(object obj)
		{
			if (obj is org.apache.hadoop.metrics2.MetricsTag)
			{
				org.apache.hadoop.metrics2.MetricsTag other = (org.apache.hadoop.metrics2.MetricsTag
					)obj;
				return com.google.common.@base.Objects.equal(info, other.info()) && com.google.common.@base.Objects
					.equal(value, other.value());
			}
			return false;
		}

		public override int GetHashCode()
		{
			return com.google.common.@base.Objects.hashCode(info, value);
		}

		public override string ToString()
		{
			return com.google.common.@base.Objects.toStringHelper(this).add("info", info).add
				("value", value()).ToString();
		}
	}
}
