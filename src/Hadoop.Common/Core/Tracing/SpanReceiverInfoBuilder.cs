

namespace Org.Apache.Hadoop.Tracing
{
	public class SpanReceiverInfoBuilder
	{
		private SpanReceiverInfo info;

		public SpanReceiverInfoBuilder(string className)
		{
			info = new SpanReceiverInfo(0, className);
		}

		public virtual void AddConfigurationPair(string key, string value)
		{
			info.configPairs.AddItem(new SpanReceiverInfo.ConfigurationPair(key, value));
		}

		public virtual SpanReceiverInfo Build()
		{
			SpanReceiverInfo ret = info;
			info = null;
			return ret;
		}
	}
}
