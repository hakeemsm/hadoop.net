using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Tools.OfflineImageViewer
{
	/// <summary>
	/// Utility class for tracking descent into the structure of the
	/// Visitor class (ImageVisitor, EditsVisitor etc.)
	/// </summary>
	public class DepthCounter
	{
		private int depth = 0;

		public virtual void IncLevel()
		{
			depth++;
		}

		public virtual void DecLevel()
		{
			if (depth >= 1)
			{
				depth--;
			}
		}

		public virtual int GetLevel()
		{
			return depth;
		}
	}
}
