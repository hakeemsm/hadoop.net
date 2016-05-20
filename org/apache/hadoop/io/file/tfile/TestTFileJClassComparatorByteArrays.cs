using Sharpen;

namespace org.apache.hadoop.io.file.tfile
{
	/// <summary>
	/// Byte arrays test case class using GZ compression codec, base class of none
	/// and LZO compression classes.
	/// </summary>
	public class TestTFileJClassComparatorByteArrays : org.apache.hadoop.io.file.tfile.TestTFileByteArrays
	{
		/// <summary>Test non-compression codec, using the same test cases as in the ByteArrays.
		/// 	</summary>
		/// <exception cref="System.IO.IOException"/>
		public override void setUp()
		{
			init(org.apache.hadoop.io.file.tfile.Compression.Algorithm.GZ.getName(), "jclass: org.apache.hadoop.io.file.tfile.MyComparator"
				);
			base.setUp();
		}
	}

	[System.Serializable]
	internal class MyComparator : org.apache.hadoop.io.RawComparator<byte[]>
	{
		public virtual int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2)
		{
			return org.apache.hadoop.io.WritableComparator.compareBytes(b1, s1, l1, b2, s2, l2
				);
		}

		public virtual int compare(byte[] o1, byte[] o2)
		{
			return org.apache.hadoop.io.WritableComparator.compareBytes(o1, 0, o1.Length, o2, 
				0, o2.Length);
		}
	}
}
