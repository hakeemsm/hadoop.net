using System.Collections.Generic;
using NUnit.Framework;


namespace Org.Apache.Hadoop.Metrics2.Util
{
	public class TestSampleQuantiles
	{
		internal static readonly Quantile[] quantiles = new Quantile[] { new Quantile(0.50
			, 0.050), new Quantile(0.75, 0.025), new Quantile(0.90, 0.010), new Quantile(0.95
			, 0.005), new Quantile(0.99, 0.001) };

		internal SampleQuantiles estimator;

		[SetUp]
		public virtual void Init()
		{
			estimator = new SampleQuantiles(quantiles);
		}

		/// <summary>
		/// Check that the counts of the number of items in the window and sample are
		/// incremented correctly as items are added.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		[Fact]
		public virtual void TestCount()
		{
			// Counts start off zero
			Assert.Equal(estimator.GetCount(), 0);
			Assert.Equal(estimator.GetSampleCount(), 0);
			// Snapshot should be null if there are no entries.
			NUnit.Framework.Assert.IsNull(estimator.Snapshot());
			// Count increment correctly by 1
			estimator.Insert(1337);
			Assert.Equal(estimator.GetCount(), 1);
			estimator.Snapshot();
			Assert.Equal(estimator.GetSampleCount(), 1);
			Assert.Equal("50.00 %ile +/- 5.00%: 1337\n" + "75.00 %ile +/- 2.50%: 1337\n"
				 + "90.00 %ile +/- 1.00%: 1337\n" + "95.00 %ile +/- 0.50%: 1337\n" + "99.00 %ile +/- 0.10%: 1337"
				, estimator.ToString());
		}

		/// <summary>
		/// Check that counts and quantile estimates are correctly reset after a call
		/// to
		/// <see cref="SampleQuantiles.Clear()"/>
		/// .
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		[Fact]
		public virtual void TestClear()
		{
			for (int i = 0; i < 1000; i++)
			{
				estimator.Insert(i);
			}
			estimator.Clear();
			Assert.Equal(estimator.GetCount(), 0);
			Assert.Equal(estimator.GetSampleCount(), 0);
			NUnit.Framework.Assert.IsNull(estimator.Snapshot());
		}

		/// <summary>
		/// Correctness test that checks that absolute error of the estimate is within
		/// specified error bounds for some randomly permuted streams of items.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		[Fact]
		public virtual void TestQuantileError()
		{
			int count = 100000;
			Random r = new Random(unchecked((int)(0xDEADDEAD)));
			long[] values = new long[count];
			for (int i = 0; i < count; i++)
			{
				values[i] = (long)(i + 1);
			}
			// Do 10 shuffle/insert/check cycles
			for (int i_1 = 0; i_1 < 10; i_1++)
			{
				System.Console.Out.WriteLine("Starting run " + i_1);
				Collections.Shuffle(Arrays.AsList(values), r);
				estimator.Clear();
				for (int j = 0; j < count; j++)
				{
					estimator.Insert(values[j]);
				}
				IDictionary<Quantile, long> snapshot;
				snapshot = estimator.Snapshot();
				foreach (Quantile q in quantiles)
				{
					long actual = (long)(q.quantile * count);
					long error = (long)(q.error * count);
					long estimate = snapshot[q];
					System.Console.Out.WriteLine(string.Format("Expected %d with error %d, estimated %d"
						, actual, error, estimate));
					Assert.True(estimate <= actual + error);
					Assert.True(estimate >= actual - error);
				}
			}
		}
	}
}
