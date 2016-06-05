

namespace Org.Apache.Hadoop.Metrics2
{
	/// <summary>The metrics collector interface</summary>
	public interface MetricsCollector
	{
		/// <summary>Add a metrics record</summary>
		/// <param name="name">of the record</param>
		/// <returns>a metrics record builder for the record</returns>
		MetricsRecordBuilder AddRecord(string name);

		/// <summary>Add a metrics record</summary>
		/// <param name="info">of the record</param>
		/// <returns>a metrics record builder for the record</returns>
		MetricsRecordBuilder AddRecord(MetricsInfo info);
	}
}
