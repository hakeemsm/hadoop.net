using Sharpen;

namespace org.apache.hadoop.tracing
{
	public class SpanReceiverInfoBuilder
	{
		private org.apache.hadoop.tracing.SpanReceiverInfo info;

		public SpanReceiverInfoBuilder(string className)
		{
			info = new org.apache.hadoop.tracing.SpanReceiverInfo(0, className);
		}

		public virtual void addConfigurationPair(string key, string value)
		{
			info.configPairs.add(new org.apache.hadoop.tracing.SpanReceiverInfo.ConfigurationPair
				(key, value));
		}

		public virtual org.apache.hadoop.tracing.SpanReceiverInfo build()
		{
			org.apache.hadoop.tracing.SpanReceiverInfo ret = info;
			info = null;
			return ret;
		}
	}
}
