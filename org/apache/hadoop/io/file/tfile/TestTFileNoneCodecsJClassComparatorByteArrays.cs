using Sharpen;

namespace org.apache.hadoop.io.file.tfile
{
	/// <summary>
	/// Byte arrays test case class using GZ compression codec, base class of none
	/// and LZO compression classes.
	/// </summary>
	public class TestTFileNoneCodecsJClassComparatorByteArrays : org.apache.hadoop.io.file.tfile.TestTFileByteArrays
	{
		/// <summary>Test non-compression codec, using the same test cases as in the ByteArrays.
		/// 	</summary>
		/// <exception cref="System.IO.IOException"/>
		public override void setUp()
		{
			init(org.apache.hadoop.io.file.tfile.Compression.Algorithm.NONE.getName(), "jclass: org.apache.hadoop.io.file.tfile.MyComparator"
				, 24, 24);
			base.setUp();
		}
	}
}
