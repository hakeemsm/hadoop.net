using System.Collections.Generic;
using NUnit.Framework;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Util.Resource;
using Org.Slf4j;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Reservation
{
	public class TestRLESparseResourceAllocation
	{
		private static readonly Logger Log = LoggerFactory.GetLogger(typeof(TestRLESparseResourceAllocation
			));

		[NUnit.Framework.Test]
		public virtual void TestBlocks()
		{
			ResourceCalculator resCalc = new DefaultResourceCalculator();
			Org.Apache.Hadoop.Yarn.Api.Records.Resource minAlloc = Org.Apache.Hadoop.Yarn.Api.Records.Resource
				.NewInstance(1, 1);
			RLESparseResourceAllocation rleSparseVector = new RLESparseResourceAllocation(resCalc
				, minAlloc);
			int[] alloc = new int[] { 10, 10, 10, 10, 10, 10 };
			int start = 100;
			ICollection<KeyValuePair<ReservationInterval, ReservationRequest>> inputs = GenerateAllocation
				(start, alloc, false);
			foreach (KeyValuePair<ReservationInterval, ReservationRequest> ip in inputs)
			{
				rleSparseVector.AddInterval(ip.Key, ip.Value);
			}
			Log.Info(rleSparseVector.ToString());
			NUnit.Framework.Assert.IsFalse(rleSparseVector.IsEmpty());
			NUnit.Framework.Assert.AreEqual(Org.Apache.Hadoop.Yarn.Api.Records.Resource.NewInstance
				(0, 0), rleSparseVector.GetCapacityAtTime(99));
			NUnit.Framework.Assert.AreEqual(Org.Apache.Hadoop.Yarn.Api.Records.Resource.NewInstance
				(0, 0), rleSparseVector.GetCapacityAtTime(start + alloc.Length + 1));
			for (int i = 0; i < alloc.Length; i++)
			{
				NUnit.Framework.Assert.AreEqual(Org.Apache.Hadoop.Yarn.Api.Records.Resource.NewInstance
					(1024 * (alloc[i]), (alloc[i])), rleSparseVector.GetCapacityAtTime(start + i));
			}
			NUnit.Framework.Assert.AreEqual(Org.Apache.Hadoop.Yarn.Api.Records.Resource.NewInstance
				(0, 0), rleSparseVector.GetCapacityAtTime(start + alloc.Length + 2));
			foreach (KeyValuePair<ReservationInterval, ReservationRequest> ip_1 in inputs)
			{
				rleSparseVector.RemoveInterval(ip_1.Key, ip_1.Value);
			}
			Log.Info(rleSparseVector.ToString());
			for (int i_1 = 0; i_1 < alloc.Length; i_1++)
			{
				NUnit.Framework.Assert.AreEqual(Org.Apache.Hadoop.Yarn.Api.Records.Resource.NewInstance
					(0, 0), rleSparseVector.GetCapacityAtTime(start + i_1));
			}
			NUnit.Framework.Assert.IsTrue(rleSparseVector.IsEmpty());
		}

		[NUnit.Framework.Test]
		public virtual void TestSteps()
		{
			ResourceCalculator resCalc = new DefaultResourceCalculator();
			Org.Apache.Hadoop.Yarn.Api.Records.Resource minAlloc = Org.Apache.Hadoop.Yarn.Api.Records.Resource
				.NewInstance(1, 1);
			RLESparseResourceAllocation rleSparseVector = new RLESparseResourceAllocation(resCalc
				, minAlloc);
			int[] alloc = new int[] { 10, 10, 10, 10, 10, 10 };
			int start = 100;
			ICollection<KeyValuePair<ReservationInterval, ReservationRequest>> inputs = GenerateAllocation
				(start, alloc, true);
			foreach (KeyValuePair<ReservationInterval, ReservationRequest> ip in inputs)
			{
				rleSparseVector.AddInterval(ip.Key, ip.Value);
			}
			Log.Info(rleSparseVector.ToString());
			NUnit.Framework.Assert.IsFalse(rleSparseVector.IsEmpty());
			NUnit.Framework.Assert.AreEqual(Org.Apache.Hadoop.Yarn.Api.Records.Resource.NewInstance
				(0, 0), rleSparseVector.GetCapacityAtTime(99));
			NUnit.Framework.Assert.AreEqual(Org.Apache.Hadoop.Yarn.Api.Records.Resource.NewInstance
				(0, 0), rleSparseVector.GetCapacityAtTime(start + alloc.Length + 1));
			for (int i = 0; i < alloc.Length; i++)
			{
				NUnit.Framework.Assert.AreEqual(Org.Apache.Hadoop.Yarn.Api.Records.Resource.NewInstance
					(1024 * (alloc[i] + i), (alloc[i] + i)), rleSparseVector.GetCapacityAtTime(start
					 + i));
			}
			NUnit.Framework.Assert.AreEqual(Org.Apache.Hadoop.Yarn.Api.Records.Resource.NewInstance
				(0, 0), rleSparseVector.GetCapacityAtTime(start + alloc.Length + 2));
			foreach (KeyValuePair<ReservationInterval, ReservationRequest> ip_1 in inputs)
			{
				rleSparseVector.RemoveInterval(ip_1.Key, ip_1.Value);
			}
			Log.Info(rleSparseVector.ToString());
			for (int i_1 = 0; i_1 < alloc.Length; i_1++)
			{
				NUnit.Framework.Assert.AreEqual(Org.Apache.Hadoop.Yarn.Api.Records.Resource.NewInstance
					(0, 0), rleSparseVector.GetCapacityAtTime(start + i_1));
			}
			NUnit.Framework.Assert.IsTrue(rleSparseVector.IsEmpty());
		}

		[NUnit.Framework.Test]
		public virtual void TestSkyline()
		{
			ResourceCalculator resCalc = new DefaultResourceCalculator();
			Org.Apache.Hadoop.Yarn.Api.Records.Resource minAlloc = Org.Apache.Hadoop.Yarn.Api.Records.Resource
				.NewInstance(1, 1);
			RLESparseResourceAllocation rleSparseVector = new RLESparseResourceAllocation(resCalc
				, minAlloc);
			int[] alloc = new int[] { 0, 5, 10, 10, 5, 0 };
			int start = 100;
			ICollection<KeyValuePair<ReservationInterval, ReservationRequest>> inputs = GenerateAllocation
				(start, alloc, true);
			foreach (KeyValuePair<ReservationInterval, ReservationRequest> ip in inputs)
			{
				rleSparseVector.AddInterval(ip.Key, ip.Value);
			}
			Log.Info(rleSparseVector.ToString());
			NUnit.Framework.Assert.IsFalse(rleSparseVector.IsEmpty());
			NUnit.Framework.Assert.AreEqual(Org.Apache.Hadoop.Yarn.Api.Records.Resource.NewInstance
				(0, 0), rleSparseVector.GetCapacityAtTime(99));
			NUnit.Framework.Assert.AreEqual(Org.Apache.Hadoop.Yarn.Api.Records.Resource.NewInstance
				(0, 0), rleSparseVector.GetCapacityAtTime(start + alloc.Length + 1));
			for (int i = 0; i < alloc.Length; i++)
			{
				NUnit.Framework.Assert.AreEqual(Org.Apache.Hadoop.Yarn.Api.Records.Resource.NewInstance
					(1024 * (alloc[i] + i), (alloc[i] + i)), rleSparseVector.GetCapacityAtTime(start
					 + i));
			}
			NUnit.Framework.Assert.AreEqual(Org.Apache.Hadoop.Yarn.Api.Records.Resource.NewInstance
				(0, 0), rleSparseVector.GetCapacityAtTime(start + alloc.Length + 2));
			foreach (KeyValuePair<ReservationInterval, ReservationRequest> ip_1 in inputs)
			{
				rleSparseVector.RemoveInterval(ip_1.Key, ip_1.Value);
			}
			Log.Info(rleSparseVector.ToString());
			for (int i_1 = 0; i_1 < alloc.Length; i_1++)
			{
				NUnit.Framework.Assert.AreEqual(Org.Apache.Hadoop.Yarn.Api.Records.Resource.NewInstance
					(0, 0), rleSparseVector.GetCapacityAtTime(start + i_1));
			}
			NUnit.Framework.Assert.IsTrue(rleSparseVector.IsEmpty());
		}

		[NUnit.Framework.Test]
		public virtual void TestZeroAlloaction()
		{
			ResourceCalculator resCalc = new DefaultResourceCalculator();
			Org.Apache.Hadoop.Yarn.Api.Records.Resource minAlloc = Org.Apache.Hadoop.Yarn.Api.Records.Resource
				.NewInstance(1, 1);
			RLESparseResourceAllocation rleSparseVector = new RLESparseResourceAllocation(resCalc
				, minAlloc);
			rleSparseVector.AddInterval(new ReservationInterval(0, long.MaxValue), ReservationRequest
				.NewInstance(Org.Apache.Hadoop.Yarn.Api.Records.Resource.NewInstance(0, 0), (0))
				);
			Log.Info(rleSparseVector.ToString());
			NUnit.Framework.Assert.AreEqual(Org.Apache.Hadoop.Yarn.Api.Records.Resource.NewInstance
				(0, 0), rleSparseVector.GetCapacityAtTime(new Random().NextLong()));
			NUnit.Framework.Assert.IsTrue(rleSparseVector.IsEmpty());
		}

		private IDictionary<ReservationInterval, ReservationRequest> GenerateAllocation(int
			 startTime, int[] alloc, bool isStep)
		{
			IDictionary<ReservationInterval, ReservationRequest> req = new Dictionary<ReservationInterval
				, ReservationRequest>();
			int numContainers = 0;
			for (int i = 0; i < alloc.Length; i++)
			{
				if (isStep)
				{
					numContainers = alloc[i] + i;
				}
				else
				{
					numContainers = alloc[i];
				}
				req[new ReservationInterval(startTime + i, startTime + i + 1)] = ReservationRequest
					.NewInstance(Org.Apache.Hadoop.Yarn.Api.Records.Resource.NewInstance(1024, 1), (
					numContainers));
			}
			return req;
		}
	}
}
