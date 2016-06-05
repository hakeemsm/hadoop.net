

namespace Org.Apache.Hadoop.Metrics2.Filter
{
	/// <summary>A regex pattern filter for metrics</summary>
	public class RegexFilter : AbstractPatternFilter
	{
		protected internal override Pattern Compile(string s)
		{
			return Pattern.Compile(s);
		}
	}
}
