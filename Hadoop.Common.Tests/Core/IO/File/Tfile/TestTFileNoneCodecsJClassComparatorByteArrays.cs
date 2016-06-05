

namespace Org.Apache.Hadoop.IO.File.Tfile
{
	/// <summary>
	/// Byte arrays test case class using GZ compression codec, base class of none
	/// and LZO compression classes.
	/// </summary>
	public class TestTFileNoneCodecsJClassComparatorByteArrays : TestTFileByteArrays
	{
		/// <summary>Test non-compression codec, using the same test cases as in the ByteArrays.
		/// 	</summary>
		/// <exception cref="System.IO.IOException"/>
		public override void SetUp()
		{
			Init(Compression.Algorithm.None.GetName(), "jclass: org.apache.hadoop.io.file.tfile.MyComparator"
				, 24, 24);
			base.SetUp();
		}
	}
}
