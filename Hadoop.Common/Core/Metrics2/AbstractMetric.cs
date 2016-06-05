using Com.Google.Common.Base;


namespace Org.Apache.Hadoop.Metrics2
{
	/// <summary>The immutable metric</summary>
	public abstract class AbstractMetric : MetricsInfo
	{
		private readonly MetricsInfo info;

		/// <summary>Construct the metric</summary>
		/// <param name="info">about the metric</param>
		protected internal AbstractMetric(MetricsInfo info)
		{
			this.info = Preconditions.CheckNotNull(info, "metric info");
		}

		public virtual string Name()
		{
			return info.Name();
		}

		public virtual string Description()
		{
			return info.Description();
		}

		protected internal virtual MetricsInfo Info()
		{
			return info;
		}

		/// <summary>Get the value of the metric</summary>
		/// <returns>the value of the metric</returns>
		public abstract Number Value();

		/// <summary>Get the type of the metric</summary>
		/// <returns>the type of the metric</returns>
		public abstract MetricType Type();

		/// <summary>Accept a visitor interface</summary>
		/// <param name="visitor">of the metric</param>
		public abstract void Visit(MetricsVisitor visitor);

		public override bool Equals(object obj)
		{
			if (obj is Org.Apache.Hadoop.Metrics2.AbstractMetric)
			{
				Org.Apache.Hadoop.Metrics2.AbstractMetric other = (Org.Apache.Hadoop.Metrics2.AbstractMetric
					)obj;
				return Objects.Equal(info, other.Info()) && Objects.Equal(Value(), other.Value());
			}
			return false;
		}

		public override int GetHashCode()
		{
			return Objects.HashCode(info, Value());
		}

		public override string ToString()
		{
			return Objects.ToStringHelper(this).Add("info", info).Add("value", Value()).ToString
				();
		}
	}
}
