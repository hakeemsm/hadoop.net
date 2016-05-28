using System.Collections.Generic;
using NUnit.Framework;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs.Protocol;
using Org.Apache.Hadoop.Hdfs.Server.Blockmanagement;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	public class TestStoragePolicySummary
	{
		private IDictionary<string, long> ConvertToStringMap(StoragePolicySummary sts)
		{
			LinkedHashMap<string, long> actualOutput = new LinkedHashMap<string, long>();
			foreach (KeyValuePair<StoragePolicySummary.StorageTypeAllocation, long> entry in 
				StoragePolicySummary.SortByComparator(sts.storageComboCounts))
			{
				actualOutput[entry.Key.ToString()] = entry.Value;
			}
			return actualOutput;
		}

		[NUnit.Framework.Test]
		public virtual void TestMultipleHots()
		{
			BlockStoragePolicySuite bsps = BlockStoragePolicySuite.CreateDefaultSuite();
			StoragePolicySummary sts = new StoragePolicySummary(bsps.GetAllPolicies());
			BlockStoragePolicy hot = bsps.GetPolicy("HOT");
			sts.Add(new StorageType[] { StorageType.Disk }, hot);
			sts.Add(new StorageType[] { StorageType.Disk, StorageType.Disk }, hot);
			sts.Add(new StorageType[] { StorageType.Disk, StorageType.Disk, StorageType.Disk }
				, hot);
			sts.Add(new StorageType[] { StorageType.Disk, StorageType.Disk, StorageType.Disk, 
				StorageType.Disk }, hot);
			IDictionary<string, long> actualOutput = ConvertToStringMap(sts);
			NUnit.Framework.Assert.AreEqual(4, actualOutput.Count);
			IDictionary<string, long> expectedOutput = new Dictionary<string, long>();
			expectedOutput["HOT|DISK:1(HOT)"] = 1l;
			expectedOutput["HOT|DISK:2(HOT)"] = 1l;
			expectedOutput["HOT|DISK:3(HOT)"] = 1l;
			expectedOutput["HOT|DISK:4(HOT)"] = 1l;
			NUnit.Framework.Assert.AreEqual(expectedOutput, actualOutput);
		}

		[NUnit.Framework.Test]
		public virtual void TestMultipleHotsWithDifferentCounts()
		{
			BlockStoragePolicySuite bsps = BlockStoragePolicySuite.CreateDefaultSuite();
			StoragePolicySummary sts = new StoragePolicySummary(bsps.GetAllPolicies());
			BlockStoragePolicy hot = bsps.GetPolicy("HOT");
			sts.Add(new StorageType[] { StorageType.Disk }, hot);
			sts.Add(new StorageType[] { StorageType.Disk, StorageType.Disk }, hot);
			sts.Add(new StorageType[] { StorageType.Disk, StorageType.Disk }, hot);
			sts.Add(new StorageType[] { StorageType.Disk, StorageType.Disk, StorageType.Disk }
				, hot);
			sts.Add(new StorageType[] { StorageType.Disk, StorageType.Disk, StorageType.Disk }
				, hot);
			sts.Add(new StorageType[] { StorageType.Disk, StorageType.Disk, StorageType.Disk, 
				StorageType.Disk }, hot);
			IDictionary<string, long> actualOutput = ConvertToStringMap(sts);
			NUnit.Framework.Assert.AreEqual(4, actualOutput.Count);
			IDictionary<string, long> expectedOutput = new Dictionary<string, long>();
			expectedOutput["HOT|DISK:1(HOT)"] = 1l;
			expectedOutput["HOT|DISK:2(HOT)"] = 2l;
			expectedOutput["HOT|DISK:3(HOT)"] = 2l;
			expectedOutput["HOT|DISK:4(HOT)"] = 1l;
			NUnit.Framework.Assert.AreEqual(expectedOutput, actualOutput);
		}

		[NUnit.Framework.Test]
		public virtual void TestMultipleWarmsInDifferentOrder()
		{
			BlockStoragePolicySuite bsps = BlockStoragePolicySuite.CreateDefaultSuite();
			StoragePolicySummary sts = new StoragePolicySummary(bsps.GetAllPolicies());
			BlockStoragePolicy warm = bsps.GetPolicy("WARM");
			//DISK:1,ARCHIVE:1
			sts.Add(new StorageType[] { StorageType.Disk, StorageType.Archive }, warm);
			sts.Add(new StorageType[] { StorageType.Archive, StorageType.Disk }, warm);
			//DISK:2,ARCHIVE:1
			sts.Add(new StorageType[] { StorageType.Archive, StorageType.Disk, StorageType.Disk
				 }, warm);
			sts.Add(new StorageType[] { StorageType.Disk, StorageType.Archive, StorageType.Disk
				 }, warm);
			sts.Add(new StorageType[] { StorageType.Disk, StorageType.Disk, StorageType.Archive
				 }, warm);
			//DISK:1,ARCHIVE:2
			sts.Add(new StorageType[] { StorageType.Disk, StorageType.Archive, StorageType.Archive
				 }, warm);
			sts.Add(new StorageType[] { StorageType.Archive, StorageType.Disk, StorageType.Archive
				 }, warm);
			sts.Add(new StorageType[] { StorageType.Archive, StorageType.Archive, StorageType
				.Disk }, warm);
			//DISK:2,ARCHIVE:2
			sts.Add(new StorageType[] { StorageType.Archive, StorageType.Archive, StorageType
				.Disk, StorageType.Disk }, warm);
			IDictionary<string, long> actualOutput = ConvertToStringMap(sts);
			NUnit.Framework.Assert.AreEqual(4, actualOutput.Count);
			IDictionary<string, long> expectedOutput = new Dictionary<string, long>();
			expectedOutput["WARM|DISK:1,ARCHIVE:1(WARM)"] = 2l;
			expectedOutput["WARM|DISK:2,ARCHIVE:1"] = 3l;
			expectedOutput["WARM|DISK:1,ARCHIVE:2(WARM)"] = 3l;
			expectedOutput["WARM|DISK:2,ARCHIVE:2"] = 1l;
			NUnit.Framework.Assert.AreEqual(expectedOutput, actualOutput);
		}

		[NUnit.Framework.Test]
		public virtual void TestDifferentSpecifiedPolicies()
		{
			BlockStoragePolicySuite bsps = BlockStoragePolicySuite.CreateDefaultSuite();
			StoragePolicySummary sts = new StoragePolicySummary(bsps.GetAllPolicies());
			BlockStoragePolicy hot = bsps.GetPolicy("HOT");
			BlockStoragePolicy warm = bsps.GetPolicy("WARM");
			BlockStoragePolicy cold = bsps.GetPolicy("COLD");
			//DISK:3
			sts.Add(new StorageType[] { StorageType.Disk, StorageType.Disk, StorageType.Disk }
				, hot);
			sts.Add(new StorageType[] { StorageType.Disk, StorageType.Disk, StorageType.Disk }
				, hot);
			sts.Add(new StorageType[] { StorageType.Disk, StorageType.Disk, StorageType.Disk }
				, warm);
			sts.Add(new StorageType[] { StorageType.Disk, StorageType.Disk, StorageType.Disk }
				, cold);
			//DISK:1,ARCHIVE:2
			sts.Add(new StorageType[] { StorageType.Disk, StorageType.Archive, StorageType.Archive
				 }, hot);
			sts.Add(new StorageType[] { StorageType.Archive, StorageType.Disk, StorageType.Archive
				 }, warm);
			sts.Add(new StorageType[] { StorageType.Archive, StorageType.Archive, StorageType
				.Disk }, cold);
			sts.Add(new StorageType[] { StorageType.Archive, StorageType.Archive, StorageType
				.Disk }, cold);
			//ARCHIVE:3
			sts.Add(new StorageType[] { StorageType.Archive, StorageType.Archive, StorageType
				.Archive }, hot);
			sts.Add(new StorageType[] { StorageType.Archive, StorageType.Archive, StorageType
				.Archive }, hot);
			sts.Add(new StorageType[] { StorageType.Archive, StorageType.Archive, StorageType
				.Archive }, warm);
			sts.Add(new StorageType[] { StorageType.Archive, StorageType.Archive, StorageType
				.Archive }, cold);
			IDictionary<string, long> actualOutput = ConvertToStringMap(sts);
			NUnit.Framework.Assert.AreEqual(9, actualOutput.Count);
			IDictionary<string, long> expectedOutput = new Dictionary<string, long>();
			expectedOutput["HOT|DISK:3(HOT)"] = 2l;
			expectedOutput["COLD|DISK:1,ARCHIVE:2(WARM)"] = 2l;
			expectedOutput["HOT|ARCHIVE:3(COLD)"] = 2l;
			expectedOutput["WARM|DISK:3(HOT)"] = 1l;
			expectedOutput["COLD|DISK:3(HOT)"] = 1l;
			expectedOutput["WARM|ARCHIVE:3(COLD)"] = 1l;
			expectedOutput["WARM|DISK:1,ARCHIVE:2(WARM)"] = 1l;
			expectedOutput["COLD|ARCHIVE:3(COLD)"] = 1l;
			expectedOutput["HOT|DISK:1,ARCHIVE:2(WARM)"] = 1l;
			NUnit.Framework.Assert.AreEqual(expectedOutput, actualOutput);
		}

		[NUnit.Framework.Test]
		public virtual void TestSortInDescendingOrder()
		{
			BlockStoragePolicySuite bsps = BlockStoragePolicySuite.CreateDefaultSuite();
			StoragePolicySummary sts = new StoragePolicySummary(bsps.GetAllPolicies());
			BlockStoragePolicy hot = bsps.GetPolicy("HOT");
			BlockStoragePolicy warm = bsps.GetPolicy("WARM");
			BlockStoragePolicy cold = bsps.GetPolicy("COLD");
			//DISK:3
			sts.Add(new StorageType[] { StorageType.Disk, StorageType.Disk, StorageType.Disk }
				, hot);
			sts.Add(new StorageType[] { StorageType.Disk, StorageType.Disk, StorageType.Disk }
				, hot);
			//DISK:1,ARCHIVE:2
			sts.Add(new StorageType[] { StorageType.Disk, StorageType.Archive, StorageType.Archive
				 }, warm);
			sts.Add(new StorageType[] { StorageType.Archive, StorageType.Disk, StorageType.Archive
				 }, warm);
			sts.Add(new StorageType[] { StorageType.Archive, StorageType.Archive, StorageType
				.Disk }, warm);
			//ARCHIVE:3
			sts.Add(new StorageType[] { StorageType.Archive, StorageType.Archive, StorageType
				.Archive }, cold);
			sts.Add(new StorageType[] { StorageType.Archive, StorageType.Archive, StorageType
				.Archive }, cold);
			sts.Add(new StorageType[] { StorageType.Archive, StorageType.Archive, StorageType
				.Archive }, cold);
			sts.Add(new StorageType[] { StorageType.Archive, StorageType.Archive, StorageType
				.Archive }, cold);
			IDictionary<string, long> actualOutput = ConvertToStringMap(sts);
			NUnit.Framework.Assert.AreEqual(3, actualOutput.Count);
			IDictionary<string, long> expectedOutput = new LinkedHashMap<string, long>();
			expectedOutput["COLD|ARCHIVE:3(COLD)"] = 4l;
			expectedOutput["WARM|DISK:1,ARCHIVE:2(WARM)"] = 3l;
			expectedOutput["HOT|DISK:3(HOT)"] = 2l;
			NUnit.Framework.Assert.AreEqual(expectedOutput.ToString(), actualOutput.ToString(
				));
		}
	}
}
