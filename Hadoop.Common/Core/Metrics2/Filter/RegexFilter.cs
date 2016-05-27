using Sharpen;

namespace Org.Apache.Hadoop.Metrics2.Filter
{
	/// <summary>A regex pattern filter for metrics</summary>
	public class RegexFilter : AbstractPatternFilter
	{
		protected internal override Sharpen.Pattern Compile(string s)
		{
			return Sharpen.Pattern.Compile(s);
		}
	}
}
