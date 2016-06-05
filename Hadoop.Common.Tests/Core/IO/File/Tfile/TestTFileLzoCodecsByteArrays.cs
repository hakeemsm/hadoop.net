

namespace Org.Apache.Hadoop.IO.File.Tfile
{
	public class TestTFileLzoCodecsByteArrays : TestTFileByteArrays
	{
		/// <summary>Test LZO compression codec, using the same test cases as in the ByteArrays.
		/// 	</summary>
		/// <exception cref="System.IO.IOException"/>
		public override void SetUp()
		{
			skip = !(Compression.Algorithm.Lzo.IsSupported());
			if (skip)
			{
				System.Console.Out.WriteLine("Skipped");
			}
			// TODO: sample the generated key/value records, and put the numbers below
			Init(Compression.Algorithm.Lzo.GetName(), "memcmp", 2605, 2558);
			if (!skip)
			{
				base.SetUp();
			}
		}
	}
}
