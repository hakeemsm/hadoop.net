using System.Collections.Generic;
using Sharpen;

namespace Org.Apache.Hadoop.Metrics2.Impl
{
	/// <summary>Builder for the immutable metrics buffers</summary>
	[System.Serializable]
	internal class MetricsBufferBuilder : AList<MetricsBuffer.Entry>
	{
		private const long serialVersionUID = 1L;

		internal virtual bool Add(string name, IEnumerable<MetricsRecordImpl> records)
		{
			return AddItem(new MetricsBuffer.Entry(name, records));
		}

		internal virtual MetricsBuffer Get()
		{
			return new MetricsBuffer(this);
		}
	}
}
