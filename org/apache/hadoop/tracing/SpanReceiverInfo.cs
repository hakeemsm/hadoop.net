using Sharpen;

namespace org.apache.hadoop.tracing
{
	public class SpanReceiverInfo
	{
		private readonly long id;

		private readonly string className;

		internal readonly System.Collections.Generic.IList<org.apache.hadoop.tracing.SpanReceiverInfo.ConfigurationPair
			> configPairs = new System.Collections.Generic.LinkedList<org.apache.hadoop.tracing.SpanReceiverInfo.ConfigurationPair
			>();

		internal class ConfigurationPair
		{
			private readonly string key;

			private readonly string value;

			internal ConfigurationPair(string key, string value)
			{
				this.key = key;
				this.value = value;
			}

			public virtual string getKey()
			{
				return key;
			}

			public virtual string getValue()
			{
				return value;
			}
		}

		internal SpanReceiverInfo(long id, string className)
		{
			this.id = id;
			this.className = className;
		}

		public virtual long getId()
		{
			return id;
		}

		public virtual string getClassName()
		{
			return className;
		}
	}
}
