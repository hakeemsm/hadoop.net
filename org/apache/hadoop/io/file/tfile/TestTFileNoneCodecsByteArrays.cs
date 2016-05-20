using Sharpen;

namespace org.apache.hadoop.io.file.tfile
{
	public class TestTFileNoneCodecsByteArrays : org.apache.hadoop.io.file.tfile.TestTFileByteArrays
	{
		/// <summary>Test non-compression codec, using the same test cases as in the ByteArrays.
		/// 	</summary>
		/// <exception cref="System.IO.IOException"/>
		public override void setUp()
		{
			init(org.apache.hadoop.io.file.tfile.Compression.Algorithm.NONE.getName(), "memcmp"
				, 24, 24);
			base.setUp();
		}
	}
}
