using System.Collections.Generic;
using Org.Apache.Commons.Logging;
using Sharpen;

namespace Org.Apache.Hadoop.Metrics2.Impl
{
	/// <summary>Test metrics configuration</summary>
	public class TestMetricsConfig
	{
		internal static readonly Log Log = LogFactory.GetLog(typeof(TestMetricsConfig));

		/// <summary>Common use cases</summary>
		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestCommon()
		{
			string filename = GetTestFilename("test-metrics2");
			new ConfigBuilder().Add("*.foo", "default foo").Add("p1.*.bar", "p1 default bar")
				.Add("p1.t1.*.bar", "p1.t1 default bar").Add("p1.t1.i1.name", "p1.t1.i1.name").Add
				("p1.t1.42.bar", "p1.t1.42.bar").Add("p1.t2.i1.foo", "p1.t2.i1.foo").Add("p2.*.foo"
				, "p2 default foo").Save(filename);
			MetricsConfig mc = MetricsConfig.Create("p1", filename);
			Log.Debug("mc:" + mc);
			Org.Apache.Commons.Configuration.Configuration expected = new ConfigBuilder().Add
				("*.bar", "p1 default bar").Add("t1.*.bar", "p1.t1 default bar").Add("t1.i1.name"
				, "p1.t1.i1.name").Add("t1.42.bar", "p1.t1.42.bar").Add("t2.i1.foo", "p1.t2.i1.foo"
				).config;
			ConfigUtil.AssertEq(expected, mc);
			TestInstances(mc);
		}

		/// <exception cref="System.Exception"/>
		private void TestInstances(MetricsConfig c)
		{
			IDictionary<string, MetricsConfig> map = c.GetInstanceConfigs("t1");
			IDictionary<string, MetricsConfig> map2 = c.GetInstanceConfigs("t2");
			Assert.Equal("number of t1 instances", 2, map.Count);
			Assert.Equal("number of t2 instances", 1, map2.Count);
			Assert.True("contains t1 instance i1", map.Contains("i1"));
			Assert.True("contains t1 instance 42", map.Contains("42"));
			Assert.True("contains t2 instance i1", map2.Contains("i1"));
			MetricsConfig t1i1 = map["i1"];
			MetricsConfig t1i42 = map["42"];
			MetricsConfig t2i1 = map2["i1"];
			Log.Debug("--- t1 instance i1:" + t1i1);
			Log.Debug("--- t1 instance 42:" + t1i42);
			Log.Debug("--- t2 instance i1:" + t2i1);
			Org.Apache.Commons.Configuration.Configuration t1expected1 = new ConfigBuilder().
				Add("name", "p1.t1.i1.name").config;
			Org.Apache.Commons.Configuration.Configuration t1expected42 = new ConfigBuilder()
				.Add("bar", "p1.t1.42.bar").config;
			Org.Apache.Commons.Configuration.Configuration t2expected1 = new ConfigBuilder().
				Add("foo", "p1.t2.i1.foo").config;
			ConfigUtil.AssertEq(t1expected1, t1i1);
			ConfigUtil.AssertEq(t1expected42, t1i42);
			ConfigUtil.AssertEq(t2expected1, t2i1);
			Log.Debug("asserting foo == default foo");
			// Check default lookups
			Assert.Equal("value of foo in t1 instance i1", "default foo", 
				t1i1.GetString("foo"));
			Assert.Equal("value of bar in t1 instance i1", "p1.t1 default bar"
				, t1i1.GetString("bar"));
			Assert.Equal("value of foo in t1 instance 42", "default foo", 
				t1i42.GetString("foo"));
			Assert.Equal("value of foo in t2 instance i1", "p1.t2.i1.foo", 
				t2i1.GetString("foo"));
			Assert.Equal("value of bar in t2 instance i1", "p1 default bar"
				, t2i1.GetString("bar"));
		}

		/// <summary>Should not throw if missing config files</summary>
		[Fact]
		public virtual void TestMissingFiles()
		{
			MetricsConfig config = MetricsConfig.Create("JobTracker", "non-existent.properties"
				);
			Assert.True(config.IsEmpty());
		}

		/// <summary>Test the config file load order</summary>
		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestLoadFirst()
		{
			string filename = GetTestFilename("hadoop-metrics2-p1");
			new ConfigBuilder().Add("p1.foo", "p1foo").Save(filename);
			MetricsConfig mc = MetricsConfig.Create("p1");
			MetricsConfig mc2 = MetricsConfig.Create("p1", "na1", "na2", filename);
			Org.Apache.Commons.Configuration.Configuration expected = new ConfigBuilder().Add
				("foo", "p1foo").config;
			ConfigUtil.AssertEq(expected, mc);
			ConfigUtil.AssertEq(expected, mc2);
		}

		/// <summary>Return a test filename in the class path</summary>
		/// <param name="basename"/>
		/// <returns>the filename</returns>
		public static string GetTestFilename(string basename)
		{
			return Runtime.GetProperty("test.build.classes", "target/test-classes") + "/" + basename
				 + ".properties";
		}
	}
}
