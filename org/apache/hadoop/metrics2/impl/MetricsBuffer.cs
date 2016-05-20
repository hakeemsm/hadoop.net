using Sharpen;

namespace org.apache.hadoop.metrics2.impl
{
	/// <summary>An immutable element for the sink queues.</summary>
	internal class MetricsBuffer : System.Collections.Generic.IEnumerable<org.apache.hadoop.metrics2.impl.MetricsBuffer.Entry
		>
	{
		private readonly System.Collections.Generic.IEnumerable<org.apache.hadoop.metrics2.impl.MetricsBuffer.Entry
			> mutable;

		internal MetricsBuffer(System.Collections.Generic.IEnumerable<org.apache.hadoop.metrics2.impl.MetricsBuffer.Entry
			> mutable)
		{
			this.mutable = mutable;
		}

		public override System.Collections.Generic.IEnumerator<org.apache.hadoop.metrics2.impl.MetricsBuffer.Entry
			> GetEnumerator()
		{
			return mutable.GetEnumerator();
		}

		internal class Entry
		{
			private readonly string sourceName;

			private readonly System.Collections.Generic.IEnumerable<org.apache.hadoop.metrics2.impl.MetricsRecordImpl
				> records;

			internal Entry(string name, System.Collections.Generic.IEnumerable<org.apache.hadoop.metrics2.impl.MetricsRecordImpl
				> records)
			{
				sourceName = name;
				this.records = records;
			}

			internal virtual string name()
			{
				return sourceName;
			}

			internal virtual System.Collections.Generic.IEnumerable<org.apache.hadoop.metrics2.impl.MetricsRecordImpl
				> records()
			{
				return records;
			}
		}
	}
}
