using System.Collections.Generic;


namespace Org.Apache.Hadoop.Metrics2.Impl
{
	/// <summary>An immutable element for the sink queues.</summary>
	internal class MetricsBuffer : IEnumerable<MetricsBuffer.Entry>
	{
		private readonly IEnumerable<MetricsBuffer.Entry> mutable;

		internal MetricsBuffer(IEnumerable<MetricsBuffer.Entry> mutable)
		{
			this.mutable = mutable;
		}

		public override IEnumerator<MetricsBuffer.Entry> GetEnumerator()
		{
			return mutable.GetEnumerator();
		}

		internal class Entry
		{
			private readonly string sourceName;

			private readonly IEnumerable<MetricsRecordImpl> records;

			internal Entry(string name, IEnumerable<MetricsRecordImpl> records)
			{
				sourceName = name;
				this.records = records;
			}

			internal virtual string Name()
			{
				return sourceName;
			}

			internal virtual IEnumerable<MetricsRecordImpl> Records()
			{
				return records;
			}
		}
	}
}
