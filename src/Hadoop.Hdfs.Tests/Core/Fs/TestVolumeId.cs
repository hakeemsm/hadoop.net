using Sharpen;

namespace Org.Apache.Hadoop.FS
{
	public class TestVolumeId
	{
		[NUnit.Framework.Test]
		public virtual void TestEquality()
		{
			VolumeId id1 = new HdfsVolumeId(new byte[] { unchecked((byte)0), unchecked((byte)
				0) });
			TestEq(true, id1, id1);
			VolumeId id2 = new HdfsVolumeId(new byte[] { unchecked((byte)0), unchecked((byte)
				1) });
			TestEq(true, id2, id2);
			TestEq(false, id1, id2);
			VolumeId id3 = new HdfsVolumeId(new byte[] { unchecked((byte)1), unchecked((byte)
				0) });
			TestEq(true, id3, id3);
			TestEq(false, id1, id3);
			// same as 2, but "invalid":
			VolumeId id2copy1 = new HdfsVolumeId(new byte[] { unchecked((byte)0), unchecked((
				byte)1) });
			TestEq(true, id2, id2copy1);
			// same as 2copy1: 
			VolumeId id2copy2 = new HdfsVolumeId(new byte[] { unchecked((byte)0), unchecked((
				byte)1) });
			TestEq(true, id2, id2copy2);
			TestEqMany(true, new VolumeId[] { id2, id2copy1, id2copy2 });
			TestEqMany(false, new VolumeId[] { id1, id2, id3 });
		}

		private void TestEq<T>(bool eq, Comparable<T> id1, Comparable<T> id2)
		{
			int h1 = id1.GetHashCode();
			int h2 = id2.GetHashCode();
			// eq reflectivity:
			NUnit.Framework.Assert.IsTrue(id1.Equals(id1));
			NUnit.Framework.Assert.IsTrue(id2.Equals(id2));
			NUnit.Framework.Assert.AreEqual(0, id1.CompareTo((T)id1));
			NUnit.Framework.Assert.AreEqual(0, id2.CompareTo((T)id2));
			// eq symmetry:
			NUnit.Framework.Assert.AreEqual(eq, id1.Equals(id2));
			NUnit.Framework.Assert.AreEqual(eq, id2.Equals(id1));
			// null comparison:
			NUnit.Framework.Assert.IsFalse(id1.Equals(null));
			NUnit.Framework.Assert.IsFalse(id2.Equals(null));
			// compareTo:
			NUnit.Framework.Assert.AreEqual(eq, 0 == id1.CompareTo((T)id2));
			NUnit.Framework.Assert.AreEqual(eq, 0 == id2.CompareTo((T)id1));
			// compareTo must be antisymmetric:
			NUnit.Framework.Assert.AreEqual(Sign(id1.CompareTo((T)id2)), -Sign(id2.CompareTo(
				(T)id1)));
			// compare with null should never return 0 to be consistent with #equals(): 
			NUnit.Framework.Assert.IsTrue(id1.CompareTo(null) != 0);
			NUnit.Framework.Assert.IsTrue(id2.CompareTo(null) != 0);
			// check that hash codes did not change:
			NUnit.Framework.Assert.AreEqual(h1, id1.GetHashCode());
			NUnit.Framework.Assert.AreEqual(h2, id2.GetHashCode());
			if (eq)
			{
				// in this case the hash codes must be the same:
				NUnit.Framework.Assert.AreEqual(h1, h2);
			}
		}

		private static int Sign(int x)
		{
			if (x == 0)
			{
				return 0;
			}
			else
			{
				if (x > 0)
				{
					return 1;
				}
				else
				{
					return -1;
				}
			}
		}

		private void TestEqMany<T>(bool eq, params Comparable<T>[] volumeIds)
		{
			Comparable<T> vidNext;
			int sum = 0;
			for (int i = 0; i < volumeIds.Length; i++)
			{
				if (i == volumeIds.Length - 1)
				{
					vidNext = volumeIds[0];
				}
				else
				{
					vidNext = volumeIds[i + 1];
				}
				TestEq(eq, volumeIds[i], vidNext);
				sum += Sign(volumeIds[i].CompareTo((T)vidNext));
			}
			// the comparison relationship must always be acyclic:
			NUnit.Framework.Assert.IsTrue(sum < volumeIds.Length);
		}

		/*
		* Test HdfsVolumeId(new byte[0]) instances: show that we permit such
		* objects, they are still valid, and obey the same equality
		* rules other objects do.
		*/
		[NUnit.Framework.Test]
		public virtual void TestIdEmptyBytes()
		{
			VolumeId idEmpty1 = new HdfsVolumeId(new byte[0]);
			VolumeId idEmpty2 = new HdfsVolumeId(new byte[0]);
			VolumeId idNotEmpty = new HdfsVolumeId(new byte[] { unchecked((byte)1) });
			TestEq(true, idEmpty1, idEmpty2);
			TestEq(false, idEmpty1, idNotEmpty);
			TestEq(false, idEmpty2, idNotEmpty);
		}

		/*
		* test #toString() for typical VolumeId equality classes
		*/
		[NUnit.Framework.Test]
		public virtual void TestToString()
		{
			string strEmpty = new HdfsVolumeId(new byte[] {  }).ToString();
			NUnit.Framework.Assert.IsNotNull(strEmpty);
			string strNotEmpty = new HdfsVolumeId(new byte[] { unchecked((byte)1) }).ToString
				();
			NUnit.Framework.Assert.IsNotNull(strNotEmpty);
		}
	}
}
