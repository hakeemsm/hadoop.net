

namespace Org.Apache.Hadoop.IO.File.Tfile
{
	public class TestTFileLzoCodecsStreams : TestTFileStreams
	{
		/// <summary>Test LZO compression codec, using the same test cases as in the ByteArrays.
		/// 	</summary>
		/// <exception cref="System.IO.IOException"/>
		protected override void SetUp()
		{
			skip = !(Compression.Algorithm.Lzo.IsSupported());
			if (skip)
			{
				System.Console.Out.WriteLine("Skipped");
			}
			Init(Compression.Algorithm.Lzo.GetName(), "memcmp");
			if (!skip)
			{
				base.SetUp();
			}
		}
	}
}
