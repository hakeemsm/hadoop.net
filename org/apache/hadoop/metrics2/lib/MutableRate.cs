using Sharpen;

namespace org.apache.hadoop.metrics2.lib
{
	/// <summary>A convenient mutable metric for throughput measurement</summary>
	public class MutableRate : org.apache.hadoop.metrics2.lib.MutableStat
	{
		internal MutableRate(string name, string description, bool extended)
			: base(name, description, "Ops", "Time", extended)
		{
		}
	}
}
