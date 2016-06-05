using System.Collections.Generic;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Htrace;


namespace Org.Apache.Hadoop.Tracing
{
	public class TestTraceUtils
	{
		private static string TestPrefix = "test.prefix.htrace.";

		[Fact]
		public virtual void TestWrappedHadoopConf()
		{
			string key = "sampler";
			string value = "ProbabilitySampler";
			Configuration conf = new Configuration();
			conf.Set(TestPrefix + key, value);
			HTraceConfiguration wrapped = TraceUtils.WrapHadoopConf(TestPrefix, conf);
			Assert.Equal(value, wrapped.Get(key));
		}

		[Fact]
		public virtual void TestExtraConfig()
		{
			string key = "test.extra.config";
			string oldValue = "old value";
			string newValue = "new value";
			Configuration conf = new Configuration();
			conf.Set(TestPrefix + key, oldValue);
			List<SpanReceiverInfo.ConfigurationPair> extraConfig = new List<SpanReceiverInfo.ConfigurationPair
				>();
			extraConfig.AddItem(new SpanReceiverInfo.ConfigurationPair(key, newValue));
			HTraceConfiguration wrapped = TraceUtils.WrapHadoopConf(TestPrefix, conf, extraConfig
				);
			Assert.Equal(newValue, wrapped.Get(key));
		}
	}
}
