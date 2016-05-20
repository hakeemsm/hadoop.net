using Sharpen;

namespace org.apache.hadoop.io.file.tfile
{
	public class TestTFileNoneCodecsStreams : org.apache.hadoop.io.file.tfile.TestTFileStreams
	{
		/// <summary>Test non-compression codec, using the same test cases as in the ByteArrays.
		/// 	</summary>
		/// <exception cref="System.IO.IOException"/>
		protected override void setUp()
		{
			init(org.apache.hadoop.io.file.tfile.Compression.Algorithm.NONE.getName(), "memcmp"
				);
			base.setUp();
		}
	}
}
