using Sharpen;

namespace Org.Apache.Hadoop.Metrics2.Util
{
	/// <summary>Test the running sample stat computation</summary>
	public class TestSampleStat
	{
		private const double Epsilon = 1e-42;

		/// <summary>Some simple use cases</summary>
		[NUnit.Framework.Test]
		public virtual void TestSimple()
		{
			SampleStat stat = new SampleStat();
			NUnit.Framework.Assert.AreEqual("num samples", 0, stat.NumSamples());
			NUnit.Framework.Assert.AreEqual("mean", 0.0, stat.Mean(), Epsilon);
			NUnit.Framework.Assert.AreEqual("variance", 0.0, stat.Variance(), Epsilon);
			NUnit.Framework.Assert.AreEqual("stddev", 0.0, stat.Stddev(), Epsilon);
			NUnit.Framework.Assert.AreEqual("min", SampleStat.MinMax.DefaultMinValue, stat.Min
				(), Epsilon);
			NUnit.Framework.Assert.AreEqual("max", SampleStat.MinMax.DefaultMaxValue, stat.Max
				(), Epsilon);
			stat.Add(3);
			NUnit.Framework.Assert.AreEqual("num samples", 1L, stat.NumSamples());
			NUnit.Framework.Assert.AreEqual("mean", 3.0, stat.Mean(), Epsilon);
			NUnit.Framework.Assert.AreEqual("variance", 0.0, stat.Variance(), Epsilon);
			NUnit.Framework.Assert.AreEqual("stddev", 0.0, stat.Stddev(), Epsilon);
			NUnit.Framework.Assert.AreEqual("min", 3.0, stat.Min(), Epsilon);
			NUnit.Framework.Assert.AreEqual("max", 3.0, stat.Max(), Epsilon);
			stat.Add(2).Add(1);
			NUnit.Framework.Assert.AreEqual("num samples", 3L, stat.NumSamples());
			NUnit.Framework.Assert.AreEqual("mean", 2.0, stat.Mean(), Epsilon);
			NUnit.Framework.Assert.AreEqual("variance", 1.0, stat.Variance(), Epsilon);
			NUnit.Framework.Assert.AreEqual("stddev", 1.0, stat.Stddev(), Epsilon);
			NUnit.Framework.Assert.AreEqual("min", 1.0, stat.Min(), Epsilon);
			NUnit.Framework.Assert.AreEqual("max", 3.0, stat.Max(), Epsilon);
			stat.Reset();
			NUnit.Framework.Assert.AreEqual("num samples", 0, stat.NumSamples());
			NUnit.Framework.Assert.AreEqual("mean", 0.0, stat.Mean(), Epsilon);
			NUnit.Framework.Assert.AreEqual("variance", 0.0, stat.Variance(), Epsilon);
			NUnit.Framework.Assert.AreEqual("stddev", 0.0, stat.Stddev(), Epsilon);
			NUnit.Framework.Assert.AreEqual("min", SampleStat.MinMax.DefaultMinValue, stat.Min
				(), Epsilon);
			NUnit.Framework.Assert.AreEqual("max", SampleStat.MinMax.DefaultMaxValue, stat.Max
				(), Epsilon);
		}
	}
}
