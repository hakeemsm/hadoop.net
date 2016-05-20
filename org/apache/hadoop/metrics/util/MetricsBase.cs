using Sharpen;

namespace org.apache.hadoop.metrics.util
{
	/// <summary>This is base class for all metrics</summary>
	public abstract class MetricsBase
	{
		public const string NO_DESCRIPTION = "NoDescription";

		private readonly string name;

		private readonly string description;

		protected internal MetricsBase(string nam)
		{
			name = nam;
			description = NO_DESCRIPTION;
		}

		protected internal MetricsBase(string nam, string desc)
		{
			name = nam;
			description = desc;
		}

		public abstract void pushMetric(org.apache.hadoop.metrics.MetricsRecord mr);

		public virtual string getName()
		{
			return name;
		}

		public virtual string getDescription()
		{
			return description;
		}
	}
}
