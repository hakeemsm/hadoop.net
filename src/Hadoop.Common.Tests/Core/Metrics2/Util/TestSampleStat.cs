

namespace Org.Apache.Hadoop.Metrics2.Util
{
	/// <summary>Test the running sample stat computation</summary>
	public class TestSampleStat
	{
		private const double Epsilon = 1e-42;

		/// <summary>Some simple use cases</summary>
		[Fact]
		public virtual void TestSimple()
		{
			SampleStat stat = new SampleStat();
			Assert.Equal("num samples", 0, stat.NumSamples());
			Assert.Equal("mean", 0.0, stat.Mean(), Epsilon);
			Assert.Equal("variance", 0.0, stat.Variance(), Epsilon);
			Assert.Equal("stddev", 0.0, stat.Stddev(), Epsilon);
			Assert.Equal("min", SampleStat.MinMax.DefaultMinValue, stat.Min
				(), Epsilon);
			Assert.Equal("max", SampleStat.MinMax.DefaultMaxValue, stat.Max
				(), Epsilon);
			stat.Add(3);
			Assert.Equal("num samples", 1L, stat.NumSamples());
			Assert.Equal("mean", 3.0, stat.Mean(), Epsilon);
			Assert.Equal("variance", 0.0, stat.Variance(), Epsilon);
			Assert.Equal("stddev", 0.0, stat.Stddev(), Epsilon);
			Assert.Equal("min", 3.0, stat.Min(), Epsilon);
			Assert.Equal("max", 3.0, stat.Max(), Epsilon);
			stat.Add(2).Add(1);
			Assert.Equal("num samples", 3L, stat.NumSamples());
			Assert.Equal("mean", 2.0, stat.Mean(), Epsilon);
			Assert.Equal("variance", 1.0, stat.Variance(), Epsilon);
			Assert.Equal("stddev", 1.0, stat.Stddev(), Epsilon);
			Assert.Equal("min", 1.0, stat.Min(), Epsilon);
			Assert.Equal("max", 3.0, stat.Max(), Epsilon);
			stat.Reset();
			Assert.Equal("num samples", 0, stat.NumSamples());
			Assert.Equal("mean", 0.0, stat.Mean(), Epsilon);
			Assert.Equal("variance", 0.0, stat.Variance(), Epsilon);
			Assert.Equal("stddev", 0.0, stat.Stddev(), Epsilon);
			Assert.Equal("min", SampleStat.MinMax.DefaultMinValue, stat.Min
				(), Epsilon);
			Assert.Equal("max", SampleStat.MinMax.DefaultMaxValue, stat.Max
				(), Epsilon);
		}
	}
}
