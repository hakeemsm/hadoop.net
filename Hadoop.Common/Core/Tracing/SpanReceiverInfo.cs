using System.Collections.Generic;
using Sharpen;

namespace Org.Apache.Hadoop.Tracing
{
	public class SpanReceiverInfo
	{
		private readonly long id;

		private readonly string className;

		internal readonly IList<SpanReceiverInfo.ConfigurationPair> configPairs = new List
			<SpanReceiverInfo.ConfigurationPair>();

		internal class ConfigurationPair
		{
			private readonly string key;

			private readonly string value;

			internal ConfigurationPair(string key, string value)
			{
				this.key = key;
				this.value = value;
			}

			public virtual string GetKey()
			{
				return key;
			}

			public virtual string GetValue()
			{
				return value;
			}
		}

		internal SpanReceiverInfo(long id, string className)
		{
			this.id = id;
			this.className = className;
		}

		public virtual long GetId()
		{
			return id;
		}

		public virtual string GetClassName()
		{
			return className;
		}
	}
}
