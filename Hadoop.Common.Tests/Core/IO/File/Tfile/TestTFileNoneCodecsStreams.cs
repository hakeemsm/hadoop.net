using Sharpen;

namespace Org.Apache.Hadoop.IO.File.Tfile
{
	public class TestTFileNoneCodecsStreams : TestTFileStreams
	{
		/// <summary>Test non-compression codec, using the same test cases as in the ByteArrays.
		/// 	</summary>
		/// <exception cref="System.IO.IOException"/>
		protected override void SetUp()
		{
			Init(Compression.Algorithm.None.GetName(), "memcmp");
			base.SetUp();
		}
	}
}
