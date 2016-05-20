using Sharpen;

namespace org.apache.hadoop.io.file.tfile
{
	public class TestTFileLzoCodecsStreams : org.apache.hadoop.io.file.tfile.TestTFileStreams
	{
		/// <summary>Test LZO compression codec, using the same test cases as in the ByteArrays.
		/// 	</summary>
		/// <exception cref="System.IO.IOException"/>
		protected override void setUp()
		{
			skip = !(org.apache.hadoop.io.file.tfile.Compression.Algorithm.LZO.isSupported());
			if (skip)
			{
				System.Console.Out.WriteLine("Skipped");
			}
			init(org.apache.hadoop.io.file.tfile.Compression.Algorithm.LZO.getName(), "memcmp"
				);
			if (!skip)
			{
				base.setUp();
			}
		}
	}
}
