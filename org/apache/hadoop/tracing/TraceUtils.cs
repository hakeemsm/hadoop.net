using Sharpen;

namespace org.apache.hadoop.tracing
{
	/// <summary>This class provides utility functions for tracing.</summary>
	public class TraceUtils
	{
		private static System.Collections.Generic.IList<org.apache.hadoop.tracing.SpanReceiverInfo.ConfigurationPair
			> EMPTY = java.util.Collections.emptyList();

		public static org.apache.htrace.HTraceConfiguration wrapHadoopConf(string prefix, 
			org.apache.hadoop.conf.Configuration conf)
		{
			return wrapHadoopConf(prefix, conf, EMPTY);
		}

		public static org.apache.htrace.HTraceConfiguration wrapHadoopConf(string prefix, 
			org.apache.hadoop.conf.Configuration conf, System.Collections.Generic.IList<org.apache.hadoop.tracing.SpanReceiverInfo.ConfigurationPair
			> extraConfig)
		{
			System.Collections.Generic.Dictionary<string, string> extraMap = new System.Collections.Generic.Dictionary
				<string, string>();
			foreach (org.apache.hadoop.tracing.SpanReceiverInfo.ConfigurationPair pair in extraConfig)
			{
				extraMap[pair.getKey()] = pair.getValue();
			}
			return new _HTraceConfiguration_47(extraMap, conf, prefix);
		}

		private sealed class _HTraceConfiguration_47 : org.apache.htrace.HTraceConfiguration
		{
			public _HTraceConfiguration_47(System.Collections.Generic.Dictionary<string, string
				> extraMap, org.apache.hadoop.conf.Configuration conf, string prefix)
			{
				this.extraMap = extraMap;
				this.conf = conf;
				this.prefix = prefix;
			}

			public override string get(string key)
			{
				if (extraMap.Contains(key))
				{
					return extraMap[key];
				}
				return conf.get(prefix + key, string.Empty);
			}

			public override string get(string key, string defaultValue)
			{
				if (extraMap.Contains(key))
				{
					return extraMap[key];
				}
				return conf.get(prefix + key, defaultValue);
			}

			private readonly System.Collections.Generic.Dictionary<string, string> extraMap;

			private readonly org.apache.hadoop.conf.Configuration conf;

			private readonly string prefix;
		}
	}
}
