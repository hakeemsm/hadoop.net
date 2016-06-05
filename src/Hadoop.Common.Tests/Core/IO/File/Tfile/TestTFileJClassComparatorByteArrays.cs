using Org.Apache.Hadoop.IO;


namespace Org.Apache.Hadoop.IO.File.Tfile
{
	/// <summary>
	/// Byte arrays test case class using GZ compression codec, base class of none
	/// and LZO compression classes.
	/// </summary>
	public class TestTFileJClassComparatorByteArrays : TestTFileByteArrays
	{
		/// <summary>Test non-compression codec, using the same test cases as in the ByteArrays.
		/// 	</summary>
		/// <exception cref="System.IO.IOException"/>
		public override void SetUp()
		{
			Init(Compression.Algorithm.Gz.GetName(), "jclass: org.apache.hadoop.io.file.tfile.MyComparator"
				);
			base.SetUp();
		}
	}

	[System.Serializable]
	internal class MyComparator : RawComparator<byte[]>
	{
		public virtual int Compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2)
		{
			return WritableComparator.CompareBytes(b1, s1, l1, b2, s2, l2);
		}

		public virtual int Compare(byte[] o1, byte[] o2)
		{
			return WritableComparator.CompareBytes(o1, 0, o1.Length, o2, 0, o2.Length);
		}
	}
}
