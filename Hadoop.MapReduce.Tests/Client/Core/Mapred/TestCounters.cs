using System;
using System.Collections.Generic;
using System.IO;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Mapreduce.Counters;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	/// <summary>
	/// TestCounters checks the sanity and recoverability of
	/// <c>Counters</c>
	/// </summary>
	public class TestCounters
	{
		internal enum MyCounters
		{
			Test1,
			Test2
		}

		private const long MaxValue = 10;

		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Mapred.TestCounters
			));

		internal static readonly Enum<object> FrameworkCounter = TaskCounter.CpuMilliseconds;

		internal const long FrameworkCounterValue = 8;

		internal const string FsScheme = "HDFS";

		internal static readonly FileSystemCounter FsCounter = FileSystemCounter.BytesRead;

		internal const long FsCounterValue = 10;

		// Generates enum based counters
		private Counters GetEnumCounters(Enum[] keys)
		{
			Counters counters = new Counters();
			foreach (Enum key in keys)
			{
				for (long i = 0; i < MaxValue; ++i)
				{
					counters.IncrCounter(key, i);
				}
			}
			return counters;
		}

		// Generate string based counters
		private Counters GetEnumCounters(string[] gNames, string[] cNames)
		{
			Counters counters = new Counters();
			foreach (string gName in gNames)
			{
				foreach (string cName in cNames)
				{
					for (long i = 0; i < MaxValue; ++i)
					{
						counters.IncrCounter(gName, cName, i);
					}
				}
			}
			return counters;
		}

		/// <summary>Test counter recovery</summary>
		/// <exception cref="Sharpen.ParseException"/>
		private void TestCounter(Counters counter)
		{
			string compactEscapedString = counter.MakeEscapedCompactString();
			Counters recoveredCounter = Counters.FromEscapedCompactString(compactEscapedString
				);
			// Check for recovery from string
			NUnit.Framework.Assert.AreEqual("Recovered counter does not match on content", counter
				, recoveredCounter);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestCounters()
		{
			Enum[] keysWithResource = new Enum[] { TaskCounter.MapInputRecords, TaskCounter.MapOutputBytes
				 };
			Enum[] keysWithoutResource = new Enum[] { TestCounters.MyCounters.Test1, TestCounters.MyCounters
				.Test2 };
			string[] groups = new string[] { "group1", "group2", "group{}()[]" };
			string[] counters = new string[] { "counter1", "counter2", "counter{}()[]" };
			try
			{
				// I. Check enum counters that have resource bundler
				TestCounter(GetEnumCounters(keysWithResource));
				// II. Check enum counters that dont have resource bundler
				TestCounter(GetEnumCounters(keysWithoutResource));
				// III. Check string counters
				TestCounter(GetEnumCounters(groups, counters));
			}
			catch (ParseException pe)
			{
				throw new IOException(pe);
			}
		}

		/// <summary>Verify counter value works</summary>
		[NUnit.Framework.Test]
		public virtual void TestCounterValue()
		{
			Counters counters = new Counters();
			int NumberTests = 100;
			int NumberInc = 10;
			Random rand = new Random();
			for (int i = 0; i < NumberTests; i++)
			{
				long initValue = rand.Next();
				long expectedValue = initValue;
				Counters.Counter counter = counters.FindCounter("foo", "bar");
				counter.SetValue(initValue);
				NUnit.Framework.Assert.AreEqual("Counter value is not initialized correctly", expectedValue
					, counter.GetValue());
				for (int j = 0; j < NumberInc; j++)
				{
					int incValue = rand.Next();
					counter.Increment(incValue);
					expectedValue += incValue;
					NUnit.Framework.Assert.AreEqual("Counter value is not incremented correctly", expectedValue
						, counter.GetValue());
				}
				expectedValue = rand.Next();
				counter.SetValue(expectedValue);
				NUnit.Framework.Assert.AreEqual("Counter value is not set correctly", expectedValue
					, counter.GetValue());
			}
		}

		[NUnit.Framework.Test]
		public virtual void TestReadWithLegacyNames()
		{
			Counters counters = new Counters();
			counters.IncrCounter(TaskCounter.MapInputRecords, 1);
			counters.IncrCounter(JobCounter.DataLocalMaps, 1);
			counters.FindCounter("file", FileSystemCounter.BytesRead).Increment(1);
			CheckLegacyNames(counters);
		}

		[NUnit.Framework.Test]
		public virtual void TestWriteWithLegacyNames()
		{
			Counters counters = new Counters();
			counters.IncrCounter(Task.Counter.MapInputRecords, 1);
			counters.IncrCounter(JobInProgress.Counter.DataLocalMaps, 1);
			counters.FindCounter("FileSystemCounters", "FILE_BYTES_READ").Increment(1);
			CheckLegacyNames(counters);
		}

		private void CheckLegacyNames(Counters counters)
		{
			NUnit.Framework.Assert.AreEqual("New name", 1, counters.FindCounter(typeof(TaskCounter
				).FullName, "MAP_INPUT_RECORDS").GetValue());
			NUnit.Framework.Assert.AreEqual("Legacy name", 1, counters.FindCounter("org.apache.hadoop.mapred.Task$Counter"
				, "MAP_INPUT_RECORDS").GetValue());
			NUnit.Framework.Assert.AreEqual("Legacy enum", 1, counters.FindCounter(Task.Counter
				.MapInputRecords).GetValue());
			NUnit.Framework.Assert.AreEqual("New name", 1, counters.FindCounter(typeof(JobCounter
				).FullName, "DATA_LOCAL_MAPS").GetValue());
			NUnit.Framework.Assert.AreEqual("Legacy name", 1, counters.FindCounter("org.apache.hadoop.mapred.JobInProgress$Counter"
				, "DATA_LOCAL_MAPS").GetValue());
			NUnit.Framework.Assert.AreEqual("Legacy enum", 1, counters.FindCounter(JobInProgress.Counter
				.DataLocalMaps).GetValue());
			NUnit.Framework.Assert.AreEqual("New name", 1, counters.FindCounter(typeof(FileSystemCounter
				).FullName, "FILE_BYTES_READ").GetValue());
			NUnit.Framework.Assert.AreEqual("New name and method", 1, counters.FindCounter("file"
				, FileSystemCounter.BytesRead).GetValue());
			NUnit.Framework.Assert.AreEqual("Legacy name", 1, counters.FindCounter("FileSystemCounters"
				, "FILE_BYTES_READ").GetValue());
		}

		[NUnit.Framework.Test]
		public virtual void TestCounterIteratorConcurrency()
		{
			Counters counters = new Counters();
			counters.IncrCounter("group1", "counter1", 1);
			IEnumerator<Counters.Group> iterator = counters.GetEnumerator();
			counters.IncrCounter("group2", "counter2", 1);
			iterator.Next();
		}

		[NUnit.Framework.Test]
		public virtual void TestGroupIteratorConcurrency()
		{
			Counters counters = new Counters();
			counters.IncrCounter("group1", "counter1", 1);
			Counters.Group group = counters.GetGroup("group1");
			IEnumerator<Counters.Counter> iterator = group.GetEnumerator();
			counters.IncrCounter("group1", "counter2", 1);
			iterator.Next();
		}

		[NUnit.Framework.Test]
		public virtual void TestFileSystemGroupIteratorConcurrency()
		{
			Counters counters = new Counters();
			// create 2 filesystem counter groups
			counters.FindCounter("fs1", FileSystemCounter.BytesRead).Increment(1);
			counters.FindCounter("fs2", FileSystemCounter.BytesRead).Increment(1);
			// Iterate over the counters in this group while updating counters in
			// the group
			Counters.Group group = counters.GetGroup(typeof(FileSystemCounter).FullName);
			IEnumerator<Counters.Counter> iterator = group.GetEnumerator();
			counters.FindCounter("fs3", FileSystemCounter.BytesRead).Increment(1);
			NUnit.Framework.Assert.IsTrue(iterator.HasNext());
			iterator.Next();
			counters.FindCounter("fs3", FileSystemCounter.BytesRead).Increment(1);
			NUnit.Framework.Assert.IsTrue(iterator.HasNext());
			iterator.Next();
		}

		[NUnit.Framework.Test]
		public virtual void TestLegacyGetGroupNames()
		{
			Counters counters = new Counters();
			// create 2 filesystem counter groups
			counters.FindCounter("fs1", FileSystemCounter.BytesRead).Increment(1);
			counters.FindCounter("fs2", FileSystemCounter.BytesRead).Increment(1);
			counters.IncrCounter("group1", "counter1", 1);
			HashSet<string> groups = new HashSet<string>(((ICollection<string>)counters.GetGroupNames
				()));
			HashSet<string> expectedGroups = new HashSet<string>();
			expectedGroups.AddItem("group1");
			expectedGroups.AddItem("FileSystemCounters");
			//Legacy Name
			expectedGroups.AddItem("org.apache.hadoop.mapreduce.FileSystemCounter");
			NUnit.Framework.Assert.AreEqual(expectedGroups, groups);
		}

		[NUnit.Framework.Test]
		public virtual void TestMakeCompactString()
		{
			string Gc1 = "group1.counter1:1";
			string Gc2 = "group2.counter2:3";
			Counters counters = new Counters();
			counters.IncrCounter("group1", "counter1", 1);
			NUnit.Framework.Assert.AreEqual("group1.counter1:1", counters.MakeCompactString()
				);
			counters.IncrCounter("group2", "counter2", 3);
			string cs = counters.MakeCompactString();
			NUnit.Framework.Assert.IsTrue("Bad compact string", cs.Equals(Gc1 + ',' + Gc2) ||
				 cs.Equals(Gc2 + ',' + Gc1));
		}

		[NUnit.Framework.Test]
		public virtual void TestCounterLimits()
		{
			TestMaxCountersLimits(new Counters());
			TestMaxGroupsLimits(new Counters());
		}

		private void TestMaxCountersLimits(Counters counters)
		{
			for (int i = 0; i < Counters.MaxCounterLimit; ++i)
			{
				counters.FindCounter("test", "test" + i);
			}
			SetExpected(counters);
			ShouldThrow(typeof(Counters.CountersExceededException), new _Runnable_281(counters
				));
			CheckExpected(counters);
		}

		private sealed class _Runnable_281 : Runnable
		{
			public _Runnable_281(Counters counters)
			{
				this.counters = counters;
			}

			public void Run()
			{
				counters.FindCounter("test", "bad");
			}

			private readonly Counters counters;
		}

		private void TestMaxGroupsLimits(Counters counters)
		{
			for (int i = 0; i < Counters.MaxGroupLimit; ++i)
			{
				// assuming COUNTERS_MAX > GROUPS_MAX
				counters.FindCounter("test" + i, "test");
			}
			SetExpected(counters);
			ShouldThrow(typeof(Counters.CountersExceededException), new _Runnable_295(counters
				));
			CheckExpected(counters);
		}

		private sealed class _Runnable_295 : Runnable
		{
			public _Runnable_295(Counters counters)
			{
				this.counters = counters;
			}

			public void Run()
			{
				counters.FindCounter("bad", "test");
			}

			private readonly Counters counters;
		}

		private void SetExpected(Counters counters)
		{
			counters.FindCounter(FrameworkCounter).SetValue(FrameworkCounterValue);
			counters.FindCounter(FsScheme, FsCounter).SetValue(FsCounterValue);
		}

		private void CheckExpected(Counters counters)
		{
			NUnit.Framework.Assert.AreEqual(FrameworkCounterValue, counters.FindCounter(FrameworkCounter
				).GetValue());
			NUnit.Framework.Assert.AreEqual(FsCounterValue, counters.FindCounter(FsScheme, FsCounter
				).GetValue());
		}

		private void ShouldThrow(Type ecls, Runnable runnable)
		{
			try
			{
				runnable.Run();
			}
			catch (Counters.CountersExceededException)
			{
				return;
			}
			NUnit.Framework.Assert.Fail("Should've thrown " + ecls.Name);
		}

		/// <exception cref="System.IO.IOException"/>
		public static void Main(string[] args)
		{
			new Org.Apache.Hadoop.Mapred.TestCounters().TestCounters();
		}

		[NUnit.Framework.Test]
		public virtual void TestFrameworkCounter()
		{
			Counters.GroupFactory groupFactory = new GroupFactoryForTest();
			CounterGroupFactory.FrameworkGroupFactory frameworkGroupFactory = groupFactory.NewFrameworkGroupFactory
				<JobCounter>();
			Counters.Group group = (Counters.Group)frameworkGroupFactory.NewGroup("JobCounter"
				);
			FrameworkCounterGroup counterGroup = (FrameworkCounterGroup)group.GetUnderlyingGroup
				();
			Counter count1 = counterGroup.FindCounter(JobCounter.NumFailedMaps.ToString());
			NUnit.Framework.Assert.IsNotNull(count1);
			// Verify no exception get thrown when finding an unknown counter
			Counter count2 = counterGroup.FindCounter("Unknown");
			NUnit.Framework.Assert.IsNull(count2);
		}

		[NUnit.Framework.Test]
		public virtual void TestFilesystemCounter()
		{
			Counters.GroupFactory groupFactory = new GroupFactoryForTest();
			Counters.Group fsGroup = groupFactory.NewFileSystemGroup();
			Counter count1 = fsGroup.FindCounter("ANY_BYTES_READ");
			NUnit.Framework.Assert.IsNotNull(count1);
			// Verify no exception get thrown when finding an unknown counter
			Counter count2 = fsGroup.FindCounter("Unknown");
			NUnit.Framework.Assert.IsNull(count2);
		}
	}

	internal class GroupFactoryForTest : Counters.GroupFactory
	{
		protected internal override CounterGroupFactory.FrameworkGroupFactory<Counters.Group
			> NewFrameworkGroupFactory<T>()
		{
			System.Type cls = typeof(T);
			return base.NewFrameworkGroupFactory(cls);
		}

		protected internal override Counters.Group NewFileSystemGroup()
		{
			return base.NewFileSystemGroup();
		}
	}
}
