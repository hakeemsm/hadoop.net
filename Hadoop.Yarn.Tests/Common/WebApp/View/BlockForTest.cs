using System.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Webapp.View
{
	public class BlockForTest : HtmlBlock.Block
	{
		public BlockForTest(HtmlBlock htmlBlock, PrintWriter @out, int level, bool wasInline
			)
			: base(@out, level, wasInline)
		{
		}
	}
}
