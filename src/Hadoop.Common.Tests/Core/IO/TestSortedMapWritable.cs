using System.Collections.Generic;
using Hadoop.Common.Core.IO;


namespace Org.Apache.Hadoop.IO
{
	/// <summary>Tests SortedMapWritable</summary>
	public class TestSortedMapWritable
	{
		/// <summary>the test</summary>
		[Fact]
		public virtual void TestSortedMapWritable()
		{
			Text[] keys = new Text[] { new Text("key1"), new Text("key2"), new Text("key3") };
			BytesWritable[] values = new BytesWritable[] { new BytesWritable(Runtime.GetBytesForString
				("value1")), new BytesWritable(Runtime.GetBytesForString("value2")), new 
				BytesWritable(Runtime.GetBytesForString("value3")) };
			SortedMapWritable inMap = new SortedMapWritable();
			for (int i = 0; i < keys.Length; i++)
			{
				inMap[keys[i]] = values[i];
			}
			Assert.Equal(0, inMap.FirstKey().CompareTo(keys[0]));
			Assert.Equal(0, inMap.LastKey().CompareTo(keys[2]));
			SortedMapWritable outMap = new SortedMapWritable(inMap);
			Assert.Equal(inMap.Count, outMap.Count);
			foreach (KeyValuePair<WritableComparable, Writable> e in inMap)
			{
				Assert.True(outMap.Contains(e.Key));
				Assert.Equal(0, ((WritableComparable)outMap[e.Key]).CompareTo(
					e.Value));
			}
			// Now for something a little harder...
			Text[] maps = new Text[] { new Text("map1"), new Text("map2") };
			SortedMapWritable mapOfMaps = new SortedMapWritable();
			mapOfMaps[maps[0]] = inMap;
			mapOfMaps[maps[1]] = outMap;
			SortedMapWritable copyOfMapOfMaps = new SortedMapWritable(mapOfMaps);
			for (int i_1 = 0; i_1 < maps.Length; i_1++)
			{
				Assert.True(copyOfMapOfMaps.Contains(maps[i_1]));
				SortedMapWritable a = (SortedMapWritable)mapOfMaps[maps[i_1]];
				SortedMapWritable b = (SortedMapWritable)copyOfMapOfMaps[maps[i_1]];
				Assert.Equal(a.Count, b.Count);
				foreach (Writable key in a.Keys)
				{
					Assert.True(b.Contains(key));
					// This will work because we know what we put into each set
					WritableComparable aValue = (WritableComparable)a[key];
					WritableComparable bValue = (WritableComparable)b[key];
					Assert.Equal(0, aValue.CompareTo(bValue));
				}
			}
		}

		/// <summary>Test that number of "unknown" classes is propagated across multiple copies.
		/// 	</summary>
		[Fact]
		public virtual void TestForeignClass()
		{
			SortedMapWritable inMap = new SortedMapWritable();
			inMap[new Text("key")] = new UTF8("value");
			inMap[new Text("key2")] = new UTF8("value2");
			SortedMapWritable outMap = new SortedMapWritable(inMap);
			SortedMapWritable copyOfCopy = new SortedMapWritable(outMap);
			Assert.Equal(1, copyOfCopy.GetNewClasses());
		}

		/// <summary>Tests if equal and hashCode method still hold the contract.</summary>
		[Fact]
		public virtual void TestEqualsAndHashCode()
		{
			string failureReason;
			SortedMapWritable mapA = new SortedMapWritable();
			SortedMapWritable mapB = new SortedMapWritable();
			// Sanity checks
			failureReason = "SortedMapWritable couldn't be initialized. Got null reference";
			NUnit.Framework.Assert.IsNotNull(failureReason, mapA);
			NUnit.Framework.Assert.IsNotNull(failureReason, mapB);
			// Basic null check
			NUnit.Framework.Assert.IsFalse("equals method returns true when passed null", mapA
				.Equals(null));
			// When entry set is empty, they should be equal
			Assert.True("Two empty SortedMapWritables are no longer equal", 
				mapA.Equals(mapB));
			// Setup
			Text[] keys = new Text[] { new Text("key1"), new Text("key2") };
			BytesWritable[] values = new BytesWritable[] { new BytesWritable(Runtime.GetBytesForString
				("value1")), new BytesWritable(Runtime.GetBytesForString("value2")) };
			mapA[keys[0]] = values[0];
			mapB[keys[1]] = values[1];
			// entrySets are different
			failureReason = "Two SortedMapWritables with different data are now equal";
			Assert.True(failureReason, mapA.GetHashCode() != mapB.GetHashCode
				());
			Assert.True(failureReason, !mapA.Equals(mapB));
			Assert.True(failureReason, !mapB.Equals(mapA));
			mapA[keys[1]] = values[1];
			mapB[keys[0]] = values[0];
			// entrySets are now same
			failureReason = "Two SortedMapWritables with same entry sets formed in different order are now different";
			Assert.Equal(failureReason, mapA.GetHashCode(), mapB.GetHashCode
				());
			Assert.True(failureReason, mapA.Equals(mapB));
			Assert.True(failureReason, mapB.Equals(mapA));
			// Let's check if entry sets of same keys but different values
			mapA[keys[0]] = values[1];
			mapA[keys[1]] = values[0];
			failureReason = "Two SortedMapWritables with different content are now equal";
			Assert.True(failureReason, mapA.GetHashCode() != mapB.GetHashCode
				());
			Assert.True(failureReason, !mapA.Equals(mapB));
			Assert.True(failureReason, !mapB.Equals(mapA));
		}

		public virtual void TestPutAll()
		{
			SortedMapWritable map1 = new SortedMapWritable();
			SortedMapWritable map2 = new SortedMapWritable();
			map1[new Text("key")] = new Text("value");
			map2.PutAll(map1);
			Assert.Equal("map1 entries don't match map2 entries", map1, map2
				);
			Assert.True("map2 doesn't have class information from map1", map2
				.classToIdMap.Contains(typeof(Text)) && map2.idToClassMap.ContainsValue(typeof(Text
				)));
		}
	}
}
