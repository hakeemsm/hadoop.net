using System.Text;
using Com.Google.Common.Base;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Metrics2;
using Org.Apache.Hadoop.Metrics2.Lib;
using Org.Apache.Hadoop.Metrics2.Util;
using Org.Hamcrest;
using Org.Mockito;
using Org.Mockito.Internal.Matchers;
using Org.Mockito.Invocation;
using Org.Mockito.Stubbing;
using Sharpen;

namespace Org.Apache.Hadoop.Test
{
	/// <summary>Helpers for metrics source tests</summary>
	public class MetricsAsserts
	{
		internal static readonly Log Log = LogFactory.GetLog(typeof(MetricsAsserts));

		private const double Epsilon = 0.00001;

		public static MetricsSystem MockMetricsSystem()
		{
			MetricsSystem ms = Org.Mockito.Mockito.Mock<MetricsSystem>();
			DefaultMetricsSystem.SetInstance(ms);
			return ms;
		}

		public static MetricsRecordBuilder MockMetricsRecordBuilder()
		{
			MetricsCollector mc = Org.Mockito.Mockito.Mock<MetricsCollector>();
			MetricsRecordBuilder rb = Org.Mockito.Mockito.Mock<MetricsRecordBuilder>(new _Answer_66
				(mc));
			Org.Mockito.Mockito.When(mc.AddRecord(AnyString())).ThenReturn(rb);
			Org.Mockito.Mockito.When(mc.AddRecord(AnyInfo())).ThenReturn(rb);
			return rb;
		}

		private sealed class _Answer_66 : Answer<object>
		{
			public _Answer_66(MetricsCollector mc)
			{
				this.mc = mc;
			}

			public object Answer(InvocationOnMock invocation)
			{
				object[] args = invocation.GetArguments();
				StringBuilder sb = new StringBuilder();
				foreach (object o in args)
				{
					if (sb.Length > 0)
					{
						sb.Append(", ");
					}
					sb.Append(o.ToString());
				}
				string methodName = invocation.GetMethod().Name;
				MetricsAsserts.Log.Debug(methodName + ": " + sb);
				return methodName.Equals("parent") || methodName.Equals("endRecord") ? mc : invocation
					.GetMock();
			}

			private readonly MetricsCollector mc;
		}

		/// <summary>Call getMetrics on source and get a record builder mock to verify</summary>
		/// <param name="source">the metrics source</param>
		/// <param name="all">if true, return all metrics even if not changed</param>
		/// <returns>the record builder mock to verify</returns>
		public static MetricsRecordBuilder GetMetrics(MetricsSource source, bool all)
		{
			MetricsRecordBuilder rb = MockMetricsRecordBuilder();
			MetricsCollector mc = rb.Parent();
			source.GetMetrics(mc, all);
			return rb;
		}

		public static MetricsRecordBuilder GetMetrics(string name)
		{
			return GetMetrics(DefaultMetricsSystem.Instance().GetSource(name));
		}

		public static MetricsRecordBuilder GetMetrics(MetricsSource source)
		{
			return GetMetrics(source, true);
		}

		private class InfoWithSameName : ArgumentMatcher<MetricsInfo>
		{
			private readonly string expected;

			internal InfoWithSameName(MetricsInfo info)
			{
				expected = Preconditions.CheckNotNull(info.Name(), "info name");
			}

			public override bool Matches(object info)
			{
				return expected.Equals(((MetricsInfo)info).Name());
			}

			public override void DescribeTo(Description desc)
			{
				desc.AppendText("Info with name=" + expected);
			}
		}

		/// <summary>MetricInfo with the same name</summary>
		/// <param name="info">to match</param>
		/// <returns><code>null</code></returns>
		public static MetricsInfo EqName(MetricsInfo info)
		{
			return ArgThat(new MetricsAsserts.InfoWithSameName(info));
		}

		private class AnyInfo : ArgumentMatcher<MetricsInfo>
		{
			public override bool Matches(object info)
			{
				return info is MetricsInfo;
			}
			// not null as well
		}

		public static MetricsInfo AnyInfo()
		{
			return ArgThat(new MetricsAsserts.AnyInfo());
		}

		/// <summary>Assert an int gauge metric as expected</summary>
		/// <param name="name">of the metric</param>
		/// <param name="expected">value of the metric</param>
		/// <param name="rb">the record builder mock used to getMetrics</param>
		public static void AssertGauge(string name, int expected, MetricsRecordBuilder rb
			)
		{
			NUnit.Framework.Assert.AreEqual("Bad value for metric " + name, expected, GetIntGauge
				(name, rb));
		}

		public static int GetIntGauge(string name, MetricsRecordBuilder rb)
		{
			ArgumentCaptor<int> captor = ArgumentCaptor.ForClass<int>();
			Org.Mockito.Mockito.Verify(rb, Org.Mockito.Mockito.AtLeast(0)).AddGauge(EqName(Interns.Info
				(name, string.Empty)), captor.Capture());
			CheckCaptured(captor, name);
			return captor.GetValue();
		}

		/// <summary>Assert an int counter metric as expected</summary>
		/// <param name="name">of the metric</param>
		/// <param name="expected">value of the metric</param>
		/// <param name="rb">the record builder mock used to getMetrics</param>
		public static void AssertCounter(string name, int expected, MetricsRecordBuilder 
			rb)
		{
			NUnit.Framework.Assert.AreEqual("Bad value for metric " + name, expected, GetIntCounter
				(name, rb));
		}

		public static int GetIntCounter(string name, MetricsRecordBuilder rb)
		{
			ArgumentCaptor<int> captor = ArgumentCaptor.ForClass<int>();
			Org.Mockito.Mockito.Verify(rb, Org.Mockito.Mockito.AtLeast(0)).AddCounter(EqName(
				Interns.Info(name, string.Empty)), captor.Capture());
			CheckCaptured(captor, name);
			return captor.GetValue();
		}

		/// <summary>Assert a long gauge metric as expected</summary>
		/// <param name="name">of the metric</param>
		/// <param name="expected">value of the metric</param>
		/// <param name="rb">the record builder mock used to getMetrics</param>
		public static void AssertGauge(string name, long expected, MetricsRecordBuilder rb
			)
		{
			NUnit.Framework.Assert.AreEqual("Bad value for metric " + name, expected, GetLongGauge
				(name, rb));
		}

		public static long GetLongGauge(string name, MetricsRecordBuilder rb)
		{
			ArgumentCaptor<long> captor = ArgumentCaptor.ForClass<long>();
			Org.Mockito.Mockito.Verify(rb, Org.Mockito.Mockito.AtLeast(0)).AddGauge(EqName(Interns.Info
				(name, string.Empty)), captor.Capture());
			CheckCaptured(captor, name);
			return captor.GetValue();
		}

		/// <summary>Assert a double gauge metric as expected</summary>
		/// <param name="name">of the metric</param>
		/// <param name="expected">value of the metric</param>
		/// <param name="rb">the record builder mock used to getMetrics</param>
		public static void AssertGauge(string name, double expected, MetricsRecordBuilder
			 rb)
		{
			NUnit.Framework.Assert.AreEqual("Bad value for metric " + name, expected, GetDoubleGauge
				(name, rb), Epsilon);
		}

		public static double GetDoubleGauge(string name, MetricsRecordBuilder rb)
		{
			ArgumentCaptor<double> captor = ArgumentCaptor.ForClass<double>();
			Org.Mockito.Mockito.Verify(rb, Org.Mockito.Mockito.AtLeast(0)).AddGauge(EqName(Interns.Info
				(name, string.Empty)), captor.Capture());
			CheckCaptured(captor, name);
			return captor.GetValue();
		}

		/// <summary>Assert a long counter metric as expected</summary>
		/// <param name="name">of the metric</param>
		/// <param name="expected">value of the metric</param>
		/// <param name="rb">the record builder mock used to getMetrics</param>
		public static void AssertCounter(string name, long expected, MetricsRecordBuilder
			 rb)
		{
			NUnit.Framework.Assert.AreEqual("Bad value for metric " + name, expected, GetLongCounter
				(name, rb));
		}

		public static long GetLongCounter(string name, MetricsRecordBuilder rb)
		{
			ArgumentCaptor<long> captor = ArgumentCaptor.ForClass<long>();
			Org.Mockito.Mockito.Verify(rb, Org.Mockito.Mockito.AtLeast(0)).AddCounter(EqName(
				Interns.Info(name, string.Empty)), captor.Capture());
			CheckCaptured(captor, name);
			return captor.GetValue();
		}

		/// <summary>Assert a float gauge metric as expected</summary>
		/// <param name="name">of the metric</param>
		/// <param name="expected">value of the metric</param>
		/// <param name="rb">the record builder mock used to getMetrics</param>
		public static void AssertGauge(string name, float expected, MetricsRecordBuilder 
			rb)
		{
			NUnit.Framework.Assert.AreEqual("Bad value for metric " + name, expected, GetFloatGauge
				(name, rb), Epsilon);
		}

		public static float GetFloatGauge(string name, MetricsRecordBuilder rb)
		{
			ArgumentCaptor<float> captor = ArgumentCaptor.ForClass<float>();
			Org.Mockito.Mockito.Verify(rb, Org.Mockito.Mockito.AtLeast(0)).AddGauge(EqName(Interns.Info
				(name, string.Empty)), captor.Capture());
			CheckCaptured(captor, name);
			return captor.GetValue();
		}

		/// <summary>Check that this metric was captured exactly once.</summary>
		private static void CheckCaptured<_T0>(ArgumentCaptor<_T0> captor, string name)
		{
			NUnit.Framework.Assert.AreEqual("Expected exactly one metric for name " + name, 1
				, captor.GetAllValues().Count);
		}

		/// <summary>Assert an int gauge metric as expected</summary>
		/// <param name="name">of the metric</param>
		/// <param name="expected">value of the metric</param>
		/// <param name="source">to get metrics from</param>
		public static void AssertGauge(string name, int expected, MetricsSource source)
		{
			AssertGauge(name, expected, GetMetrics(source));
		}

		/// <summary>Assert an int counter metric as expected</summary>
		/// <param name="name">of the metric</param>
		/// <param name="expected">value of the metric</param>
		/// <param name="source">to get metrics from</param>
		public static void AssertCounter(string name, int expected, MetricsSource source)
		{
			AssertCounter(name, expected, GetMetrics(source));
		}

		/// <summary>Assert a long gauge metric as expected</summary>
		/// <param name="name">of the metric</param>
		/// <param name="expected">value of the metric</param>
		/// <param name="source">to get metrics from</param>
		public static void AssertGauge(string name, long expected, MetricsSource source)
		{
			AssertGauge(name, expected, GetMetrics(source));
		}

		/// <summary>Assert a long counter metric as expected</summary>
		/// <param name="name">of the metric</param>
		/// <param name="expected">value of the metric</param>
		/// <param name="source">to get metrics from</param>
		public static void AssertCounter(string name, long expected, MetricsSource source
			)
		{
			AssertCounter(name, expected, GetMetrics(source));
		}

		/// <summary>Assert that a long counter metric is greater than a value</summary>
		/// <param name="name">of the metric</param>
		/// <param name="greater">value of the metric should be greater than this</param>
		/// <param name="rb">the record builder mock used to getMetrics</param>
		public static void AssertCounterGt(string name, long greater, MetricsRecordBuilder
			 rb)
		{
			Assert.AssertThat("Bad value for metric " + name, GetLongCounter(name, rb), new GreaterThan
				<long>(greater));
		}

		/// <summary>Assert that a long counter metric is greater than a value</summary>
		/// <param name="name">of the metric</param>
		/// <param name="greater">value of the metric should be greater than this</param>
		/// <param name="source">the metrics source</param>
		public static void AssertCounterGt(string name, long greater, MetricsSource source
			)
		{
			AssertCounterGt(name, greater, GetMetrics(source));
		}

		/// <summary>Assert that a double gauge metric is greater than a value</summary>
		/// <param name="name">of the metric</param>
		/// <param name="greater">value of the metric should be greater than this</param>
		/// <param name="rb">the record builder mock used to getMetrics</param>
		public static void AssertGaugeGt(string name, double greater, MetricsRecordBuilder
			 rb)
		{
			Assert.AssertThat("Bad value for metric " + name, GetDoubleGauge(name, rb), new GreaterThan
				<double>(greater));
		}

		/// <summary>Assert that a double gauge metric is greater than a value</summary>
		/// <param name="name">of the metric</param>
		/// <param name="greater">value of the metric should be greater than this</param>
		/// <param name="source">the metrics source</param>
		public static void AssertGaugeGt(string name, double greater, MetricsSource source
			)
		{
			AssertGaugeGt(name, greater, GetMetrics(source));
		}

		/// <summary>
		/// Asserts that the NumOps and quantiles for a metric have been changed at
		/// some point to a non-zero value.
		/// </summary>
		/// <param name="prefix">of the metric</param>
		/// <param name="rb">MetricsRecordBuilder with the metric</param>
		public static void AssertQuantileGauges(string prefix, MetricsRecordBuilder rb)
		{
			Org.Mockito.Mockito.Verify(rb).AddGauge(EqName(Interns.Info(prefix + "NumOps", string.Empty
				)), AdditionalMatchers.Geq(0l));
			foreach (Quantile q in MutableQuantiles.quantiles)
			{
				string nameTemplate = prefix + "%dthPercentileLatency";
				int percentile = (int)(100 * q.quantile);
				Org.Mockito.Mockito.Verify(rb).AddGauge(EqName(Interns.Info(string.Format(nameTemplate
					, percentile), string.Empty)), AdditionalMatchers.Geq(0l));
			}
		}
	}
}
