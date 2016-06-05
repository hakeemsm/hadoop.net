using Org.Apache.Hadoop.Metrics;


namespace Org.Apache.Hadoop.Metrics.Util
{
	/// <summary>This is base class for all metrics</summary>
	public abstract class MetricsBase
	{
		public const string NoDescription = "NoDescription";

		private readonly string name;

		private readonly string description;

		protected internal MetricsBase(string nam)
		{
			name = nam;
			description = NoDescription;
		}

		protected internal MetricsBase(string nam, string desc)
		{
			name = nam;
			description = desc;
		}

		public abstract void PushMetric(MetricsRecord mr);

		public virtual string GetName()
		{
			return name;
		}

		public virtual string GetDescription()
		{
			return description;
		}
	}
}
