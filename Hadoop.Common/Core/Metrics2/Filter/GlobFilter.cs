using Org.Apache.Hadoop.FS;
using Sharpen;

namespace Org.Apache.Hadoop.Metrics2.Filter
{
	/// <summary>A glob pattern filter for metrics.</summary>
	/// <remarks>
	/// A glob pattern filter for metrics.
	/// The class name is used in metrics config files
	/// </remarks>
	public class GlobFilter : AbstractPatternFilter
	{
		protected internal override Sharpen.Pattern Compile(string s)
		{
			return GlobPattern.Compile(s);
		}
	}
}
