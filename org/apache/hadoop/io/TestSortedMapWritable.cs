using Sharpen;

namespace org.apache.hadoop.io
{
	/// <summary>Tests SortedMapWritable</summary>
	public class TestSortedMapWritable
	{
		/// <summary>the test</summary>
		[NUnit.Framework.Test]
		public virtual void testSortedMapWritable()
		{
			org.apache.hadoop.io.Text[] keys = new org.apache.hadoop.io.Text[] { new org.apache.hadoop.io.Text
				("key1"), new org.apache.hadoop.io.Text("key2"), new org.apache.hadoop.io.Text("key3"
				) };
			org.apache.hadoop.io.BytesWritable[] values = new org.apache.hadoop.io.BytesWritable
				[] { new org.apache.hadoop.io.BytesWritable(Sharpen.Runtime.getBytesForString("value1"
				)), new org.apache.hadoop.io.BytesWritable(Sharpen.Runtime.getBytesForString("value2"
				)), new org.apache.hadoop.io.BytesWritable(Sharpen.Runtime.getBytesForString("value3"
				)) };
			org.apache.hadoop.io.SortedMapWritable inMap = new org.apache.hadoop.io.SortedMapWritable
				();
			for (int i = 0; i < keys.Length; i++)
			{
				inMap[keys[i]] = values[i];
			}
			NUnit.Framework.Assert.AreEqual(0, inMap.firstKey().compareTo(keys[0]));
			NUnit.Framework.Assert.AreEqual(0, inMap.lastKey().compareTo(keys[2]));
			org.apache.hadoop.io.SortedMapWritable outMap = new org.apache.hadoop.io.SortedMapWritable
				(inMap);
			NUnit.Framework.Assert.AreEqual(inMap.Count, outMap.Count);
			foreach (System.Collections.Generic.KeyValuePair<org.apache.hadoop.io.WritableComparable
				, org.apache.hadoop.io.Writable> e in inMap)
			{
				NUnit.Framework.Assert.IsTrue(outMap.Contains(e.Key));
				NUnit.Framework.Assert.AreEqual(0, ((org.apache.hadoop.io.WritableComparable)outMap
					[e.Key]).compareTo(e.Value));
			}
			// Now for something a little harder...
			org.apache.hadoop.io.Text[] maps = new org.apache.hadoop.io.Text[] { new org.apache.hadoop.io.Text
				("map1"), new org.apache.hadoop.io.Text("map2") };
			org.apache.hadoop.io.SortedMapWritable mapOfMaps = new org.apache.hadoop.io.SortedMapWritable
				();
			mapOfMaps[maps[0]] = inMap;
			mapOfMaps[maps[1]] = outMap;
			org.apache.hadoop.io.SortedMapWritable copyOfMapOfMaps = new org.apache.hadoop.io.SortedMapWritable
				(mapOfMaps);
			for (int i_1 = 0; i_1 < maps.Length; i_1++)
			{
				NUnit.Framework.Assert.IsTrue(copyOfMapOfMaps.Contains(maps[i_1]));
				org.apache.hadoop.io.SortedMapWritable a = (org.apache.hadoop.io.SortedMapWritable
					)mapOfMaps[maps[i_1]];
				org.apache.hadoop.io.SortedMapWritable b = (org.apache.hadoop.io.SortedMapWritable
					)copyOfMapOfMaps[maps[i_1]];
				NUnit.Framework.Assert.AreEqual(a.Count, b.Count);
				foreach (org.apache.hadoop.io.Writable key in a.Keys)
				{
					NUnit.Framework.Assert.IsTrue(b.Contains(key));
					// This will work because we know what we put into each set
					org.apache.hadoop.io.WritableComparable aValue = (org.apache.hadoop.io.WritableComparable
						)a[key];
					org.apache.hadoop.io.WritableComparable bValue = (org.apache.hadoop.io.WritableComparable
						)b[key];
					NUnit.Framework.Assert.AreEqual(0, aValue.compareTo(bValue));
				}
			}
		}

		/// <summary>Test that number of "unknown" classes is propagated across multiple copies.
		/// 	</summary>
		[NUnit.Framework.Test]
		public virtual void testForeignClass()
		{
			org.apache.hadoop.io.SortedMapWritable inMap = new org.apache.hadoop.io.SortedMapWritable
				();
			inMap[new org.apache.hadoop.io.Text("key")] = new org.apache.hadoop.io.UTF8("value"
				);
			inMap[new org.apache.hadoop.io.Text("key2")] = new org.apache.hadoop.io.UTF8("value2"
				);
			org.apache.hadoop.io.SortedMapWritable outMap = new org.apache.hadoop.io.SortedMapWritable
				(inMap);
			org.apache.hadoop.io.SortedMapWritable copyOfCopy = new org.apache.hadoop.io.SortedMapWritable
				(outMap);
			NUnit.Framework.Assert.AreEqual(1, copyOfCopy.getNewClasses());
		}

		/// <summary>Tests if equal and hashCode method still hold the contract.</summary>
		[NUnit.Framework.Test]
		public virtual void testEqualsAndHashCode()
		{
			string failureReason;
			org.apache.hadoop.io.SortedMapWritable mapA = new org.apache.hadoop.io.SortedMapWritable
				();
			org.apache.hadoop.io.SortedMapWritable mapB = new org.apache.hadoop.io.SortedMapWritable
				();
			// Sanity checks
			failureReason = "SortedMapWritable couldn't be initialized. Got null reference";
			NUnit.Framework.Assert.IsNotNull(failureReason, mapA);
			NUnit.Framework.Assert.IsNotNull(failureReason, mapB);
			// Basic null check
			NUnit.Framework.Assert.IsFalse("equals method returns true when passed null", mapA
				.Equals(null));
			// When entry set is empty, they should be equal
			NUnit.Framework.Assert.IsTrue("Two empty SortedMapWritables are no longer equal", 
				mapA.Equals(mapB));
			// Setup
			org.apache.hadoop.io.Text[] keys = new org.apache.hadoop.io.Text[] { new org.apache.hadoop.io.Text
				("key1"), new org.apache.hadoop.io.Text("key2") };
			org.apache.hadoop.io.BytesWritable[] values = new org.apache.hadoop.io.BytesWritable
				[] { new org.apache.hadoop.io.BytesWritable(Sharpen.Runtime.getBytesForString("value1"
				)), new org.apache.hadoop.io.BytesWritable(Sharpen.Runtime.getBytesForString("value2"
				)) };
			mapA[keys[0]] = values[0];
			mapB[keys[1]] = values[1];
			// entrySets are different
			failureReason = "Two SortedMapWritables with different data are now equal";
			NUnit.Framework.Assert.IsTrue(failureReason, mapA.GetHashCode() != mapB.GetHashCode
				());
			NUnit.Framework.Assert.IsTrue(failureReason, !mapA.Equals(mapB));
			NUnit.Framework.Assert.IsTrue(failureReason, !mapB.Equals(mapA));
			mapA[keys[1]] = values[1];
			mapB[keys[0]] = values[0];
			// entrySets are now same
			failureReason = "Two SortedMapWritables with same entry sets formed in different order are now different";
			NUnit.Framework.Assert.AreEqual(failureReason, mapA.GetHashCode(), mapB.GetHashCode
				());
			NUnit.Framework.Assert.IsTrue(failureReason, mapA.Equals(mapB));
			NUnit.Framework.Assert.IsTrue(failureReason, mapB.Equals(mapA));
			// Let's check if entry sets of same keys but different values
			mapA[keys[0]] = values[1];
			mapA[keys[1]] = values[0];
			failureReason = "Two SortedMapWritables with different content are now equal";
			NUnit.Framework.Assert.IsTrue(failureReason, mapA.GetHashCode() != mapB.GetHashCode
				());
			NUnit.Framework.Assert.IsTrue(failureReason, !mapA.Equals(mapB));
			NUnit.Framework.Assert.IsTrue(failureReason, !mapB.Equals(mapA));
		}

		public virtual void testPutAll()
		{
			org.apache.hadoop.io.SortedMapWritable map1 = new org.apache.hadoop.io.SortedMapWritable
				();
			org.apache.hadoop.io.SortedMapWritable map2 = new org.apache.hadoop.io.SortedMapWritable
				();
			map1[new org.apache.hadoop.io.Text("key")] = new org.apache.hadoop.io.Text("value"
				);
			map2.putAll(map1);
			NUnit.Framework.Assert.AreEqual("map1 entries don't match map2 entries", map1, map2
				);
			NUnit.Framework.Assert.IsTrue("map2 doesn't have class information from map1", map2
				.classToIdMap.Contains(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.Text
				))) && map2.idToClassMap.containsValue(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.Text
				))));
		}
	}
}
