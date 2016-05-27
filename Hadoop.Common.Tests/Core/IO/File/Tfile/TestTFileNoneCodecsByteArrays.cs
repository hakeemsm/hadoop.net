using Sharpen;

namespace Org.Apache.Hadoop.IO.File.Tfile
{
	public class TestTFileNoneCodecsByteArrays : TestTFileByteArrays
	{
		/// <summary>Test non-compression codec, using the same test cases as in the ByteArrays.
		/// 	</summary>
		/// <exception cref="System.IO.IOException"/>
		public override void SetUp()
		{
			Init(Compression.Algorithm.None.GetName(), "memcmp", 24, 24);
			base.SetUp();
		}
	}
}
