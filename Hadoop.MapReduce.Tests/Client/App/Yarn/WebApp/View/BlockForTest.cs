using System.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Webapp.View
{
	/// <summary>BlockForTest publishes constructor for test</summary>
	public class BlockForTest : HtmlBlock.Block
	{
		public BlockForTest(HtmlBlock htmlBlock, PrintWriter @out, int level, bool wasInline
			)
			: base(@out, level, wasInline)
		{
		}
	}
}
