using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	public class TestTaskPerformanceSplits
	{
		[NUnit.Framework.Test]
		public virtual void TestPeriodStatsets()
		{
			PeriodicStatsAccumulator cumulative = new CumulativePeriodicStats(8);
			PeriodicStatsAccumulator status = new StatePeriodicStats(8);
			cumulative.Extend(0.0D, 0);
			cumulative.Extend(0.4375D, 700);
			// 200 per octant
			cumulative.Extend(0.5625D, 1100);
			// 0.5 = 900
			cumulative.Extend(0.625D, 1300);
			cumulative.Extend(1.0D, 7901);
			int total = 0;
			int[] results = cumulative.GetValues();
			for (int i = 0; i < 8; ++i)
			{
				System.Console.Error.WriteLine("segment i = " + results[i]);
			}
			NUnit.Framework.Assert.AreEqual("Bad interpolation in cumulative segment 0", 200, 
				results[0]);
			NUnit.Framework.Assert.AreEqual("Bad interpolation in cumulative segment 1", 200, 
				results[1]);
			NUnit.Framework.Assert.AreEqual("Bad interpolation in cumulative segment 2", 200, 
				results[2]);
			NUnit.Framework.Assert.AreEqual("Bad interpolation in cumulative segment 3", 300, 
				results[3]);
			NUnit.Framework.Assert.AreEqual("Bad interpolation in cumulative segment 4", 400, 
				results[4]);
			NUnit.Framework.Assert.AreEqual("Bad interpolation in cumulative segment 5", 2200
				, results[5]);
			// these are rounded down
			NUnit.Framework.Assert.AreEqual("Bad interpolation in cumulative segment 6", 2200
				, results[6]);
			NUnit.Framework.Assert.AreEqual("Bad interpolation in cumulative segment 7", 2201
				, results[7]);
			status.Extend(0.0D, 0);
			status.Extend(1.0D / 16.0D, 300);
			// + 75 for bucket 0
			status.Extend(3.0D / 16.0D, 700);
			// + 200 for 0, +300 for 1
			status.Extend(7.0D / 16.0D, 2300);
			// + 450 for 1, + 1500 for 2, + 1050 for 3
			status.Extend(1.0D, 1400);
			// +1125 for 3, +2100 for 4, +1900 for 5,
			// +1700 for 6, +1500 for 7
			results = status.GetValues();
			NUnit.Framework.Assert.AreEqual("Bad interpolation in status segment 0", 275, results
				[0]);
			NUnit.Framework.Assert.AreEqual("Bad interpolation in status segment 1", 750, results
				[1]);
			NUnit.Framework.Assert.AreEqual("Bad interpolation in status segment 2", 1500, results
				[2]);
			NUnit.Framework.Assert.AreEqual("Bad interpolation in status segment 3", 2175, results
				[3]);
			NUnit.Framework.Assert.AreEqual("Bad interpolation in status segment 4", 2100, results
				[4]);
			NUnit.Framework.Assert.AreEqual("Bad interpolation in status segment 5", 1900, results
				[5]);
			NUnit.Framework.Assert.AreEqual("Bad interpolation in status segment 6", 1700, results
				[6]);
			NUnit.Framework.Assert.AreEqual("Bad interpolation in status segment 7", 1500, results
				[7]);
		}
	}
}
