using Sharpen;

namespace org.apache.hadoop.io.file.tfile
{
	public class TestTFileLzoCodecsByteArrays : org.apache.hadoop.io.file.tfile.TestTFileByteArrays
	{
		/// <summary>Test LZO compression codec, using the same test cases as in the ByteArrays.
		/// 	</summary>
		/// <exception cref="System.IO.IOException"/>
		public override void setUp()
		{
			skip = !(org.apache.hadoop.io.file.tfile.Compression.Algorithm.LZO.isSupported());
			if (skip)
			{
				System.Console.Out.WriteLine("Skipped");
			}
			// TODO: sample the generated key/value records, and put the numbers below
			init(org.apache.hadoop.io.file.tfile.Compression.Algorithm.LZO.getName(), "memcmp"
				, 2605, 2558);
			if (!skip)
			{
				base.setUp();
			}
		}
	}
}
