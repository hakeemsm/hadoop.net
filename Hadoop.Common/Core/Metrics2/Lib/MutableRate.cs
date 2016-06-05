

namespace Org.Apache.Hadoop.Metrics2.Lib
{
	/// <summary>A convenient mutable metric for throughput measurement</summary>
	public class MutableRate : MutableStat
	{
		internal MutableRate(string name, string description, bool extended)
			: base(name, description, "Ops", "Time", extended)
		{
		}
	}
}
