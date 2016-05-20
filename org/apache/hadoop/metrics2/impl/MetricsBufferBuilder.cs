using Sharpen;

namespace org.apache.hadoop.metrics2.impl
{
	/// <summary>Builder for the immutable metrics buffers</summary>
	[System.Serializable]
	internal class MetricsBufferBuilder : System.Collections.Generic.List<org.apache.hadoop.metrics2.impl.MetricsBuffer.Entry
		>
	{
		private const long serialVersionUID = 1L;

		internal virtual bool add(string name, System.Collections.Generic.IEnumerable<org.apache.hadoop.metrics2.impl.MetricsRecordImpl
			> records)
		{
			return add(new org.apache.hadoop.metrics2.impl.MetricsBuffer.Entry(name, records)
				);
		}

		internal virtual org.apache.hadoop.metrics2.impl.MetricsBuffer get()
		{
			return new org.apache.hadoop.metrics2.impl.MetricsBuffer(this);
		}
	}
}
