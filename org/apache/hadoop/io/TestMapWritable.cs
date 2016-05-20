using Sharpen;

namespace org.apache.hadoop.io
{
	/// <summary>Tests MapWritable</summary>
	public class TestMapWritable : NUnit.Framework.TestCase
	{
		/// <summary>the test</summary>
		public virtual void testMapWritable()
		{
			org.apache.hadoop.io.Text[] keys = new org.apache.hadoop.io.Text[] { new org.apache.hadoop.io.Text
				("key1"), new org.apache.hadoop.io.Text("key2"), new org.apache.hadoop.io.Text("Key3"
				) };
			org.apache.hadoop.io.BytesWritable[] values = new org.apache.hadoop.io.BytesWritable
				[] { new org.apache.hadoop.io.BytesWritable(Sharpen.Runtime.getBytesForString("value1"
				)), new org.apache.hadoop.io.BytesWritable(Sharpen.Runtime.getBytesForString("value2"
				)), new org.apache.hadoop.io.BytesWritable(Sharpen.Runtime.getBytesForString("value3"
				)) };
			org.apache.hadoop.io.MapWritable inMap = new org.apache.hadoop.io.MapWritable();
			for (int i = 0; i < keys.Length; i++)
			{
				inMap[keys[i]] = values[i];
			}
			org.apache.hadoop.io.MapWritable outMap = new org.apache.hadoop.io.MapWritable(inMap
				);
			NUnit.Framework.Assert.AreEqual(inMap.Count, outMap.Count);
			foreach (System.Collections.Generic.KeyValuePair<org.apache.hadoop.io.Writable, org.apache.hadoop.io.Writable
				> e in inMap)
			{
				NUnit.Framework.Assert.IsTrue(outMap.Contains(e.Key));
				NUnit.Framework.Assert.AreEqual(0, ((org.apache.hadoop.io.WritableComparable)outMap
					[e.Key]).compareTo(e.Value));
			}
			// Now for something a little harder...
			org.apache.hadoop.io.Text[] maps = new org.apache.hadoop.io.Text[] { new org.apache.hadoop.io.Text
				("map1"), new org.apache.hadoop.io.Text("map2") };
			org.apache.hadoop.io.MapWritable mapOfMaps = new org.apache.hadoop.io.MapWritable
				();
			mapOfMaps[maps[0]] = inMap;
			mapOfMaps[maps[1]] = outMap;
			org.apache.hadoop.io.MapWritable copyOfMapOfMaps = new org.apache.hadoop.io.MapWritable
				(mapOfMaps);
			for (int i_1 = 0; i_1 < maps.Length; i_1++)
			{
				NUnit.Framework.Assert.IsTrue(copyOfMapOfMaps.Contains(maps[i_1]));
				org.apache.hadoop.io.MapWritable a = (org.apache.hadoop.io.MapWritable)mapOfMaps[
					maps[i_1]];
				org.apache.hadoop.io.MapWritable b = (org.apache.hadoop.io.MapWritable)copyOfMapOfMaps
					[maps[i_1]];
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
		public virtual void testForeignClass()
		{
			org.apache.hadoop.io.MapWritable inMap = new org.apache.hadoop.io.MapWritable();
			inMap[new org.apache.hadoop.io.Text("key")] = new org.apache.hadoop.io.UTF8("value"
				);
			inMap[new org.apache.hadoop.io.Text("key2")] = new org.apache.hadoop.io.UTF8("value2"
				);
			org.apache.hadoop.io.MapWritable outMap = new org.apache.hadoop.io.MapWritable(inMap
				);
			org.apache.hadoop.io.MapWritable copyOfCopy = new org.apache.hadoop.io.MapWritable
				(outMap);
			NUnit.Framework.Assert.AreEqual(1, copyOfCopy.getNewClasses());
		}

		/// <summary>Assert MapWritable does not grow across calls to readFields.</summary>
		/// <exception cref="System.Exception"/>
		/// <seealso><a href="https://issues.apache.org/jira/browse/HADOOP-2244">HADOOP-2244</a>
		/// 	</seealso>
		public virtual void testMultipleCallsToReadFieldsAreSafe()
		{
			// Create an instance and add a key/value.
			org.apache.hadoop.io.MapWritable m = new org.apache.hadoop.io.MapWritable();
			org.apache.hadoop.io.Text t = new org.apache.hadoop.io.Text(getName());
			m[t] = t;
			// Get current size of map.  Key values are 't'.
			int count = m.Count;
			// Now serialize... save off the bytes.
			java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
			java.io.DataOutputStream dos = new java.io.DataOutputStream(baos);
			m.write(dos);
			dos.close();
			// Now add new values to the MapWritable.
			m[new org.apache.hadoop.io.Text("key1")] = new org.apache.hadoop.io.Text("value1"
				);
			m[new org.apache.hadoop.io.Text("key2")] = new org.apache.hadoop.io.Text("value2"
				);
			// Now deserialize the original MapWritable.  Ensure count and key values
			// match original state.
			java.io.ByteArrayInputStream bais = new java.io.ByteArrayInputStream(baos.toByteArray
				());
			java.io.DataInputStream dis = new java.io.DataInputStream(bais);
			m.readFields(dis);
			NUnit.Framework.Assert.AreEqual(count, m.Count);
			NUnit.Framework.Assert.IsTrue(m[t].Equals(t));
			dis.close();
		}

		public virtual void testEquality()
		{
			org.apache.hadoop.io.MapWritable map1 = new org.apache.hadoop.io.MapWritable();
			org.apache.hadoop.io.MapWritable map2 = new org.apache.hadoop.io.MapWritable();
			org.apache.hadoop.io.MapWritable map3 = new org.apache.hadoop.io.MapWritable();
			org.apache.hadoop.io.IntWritable k1 = new org.apache.hadoop.io.IntWritable(5);
			org.apache.hadoop.io.IntWritable k2 = new org.apache.hadoop.io.IntWritable(10);
			org.apache.hadoop.io.Text value = new org.apache.hadoop.io.Text("value");
			map1[k1] = value;
			// equal
			map2[k1] = value;
			// equal
			map3[k2] = value;
			// not equal
			NUnit.Framework.Assert.IsTrue(map1.Equals(map2));
			NUnit.Framework.Assert.IsTrue(map2.Equals(map1));
			NUnit.Framework.Assert.IsFalse(map1.Equals(map3));
			NUnit.Framework.Assert.AreEqual(map1.GetHashCode(), map2.GetHashCode());
			NUnit.Framework.Assert.IsFalse(map1.GetHashCode() == map3.GetHashCode());
		}
	}
}
