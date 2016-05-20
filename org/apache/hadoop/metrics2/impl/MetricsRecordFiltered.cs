using Sharpen;

namespace org.apache.hadoop.metrics2.impl
{
	internal class MetricsRecordFiltered : org.apache.hadoop.metrics2.impl.AbstractMetricsRecord
	{
		private readonly org.apache.hadoop.metrics2.MetricsRecord delegate_;

		private readonly org.apache.hadoop.metrics2.MetricsFilter filter;

		internal MetricsRecordFiltered(org.apache.hadoop.metrics2.MetricsRecord delegate_
			, org.apache.hadoop.metrics2.MetricsFilter filter)
		{
			this.delegate_ = delegate_;
			this.filter = filter;
		}

		public override long timestamp()
		{
			return delegate_.timestamp();
		}

		public override string name()
		{
			return delegate_.name();
		}

		public override string description()
		{
			return delegate_.description();
		}

		public override string context()
		{
			return delegate_.context();
		}

		public override System.Collections.Generic.ICollection<org.apache.hadoop.metrics2.MetricsTag
			> tags()
		{
			return delegate_.tags();
		}

		public override System.Collections.Generic.IEnumerable<org.apache.hadoop.metrics2.AbstractMetric
			> metrics()
		{
			return new _IEnumerable_61(this);
		}

		private sealed class _IEnumerable_61 : System.Collections.Generic.IEnumerable<org.apache.hadoop.metrics2.AbstractMetric
			>
		{
			public _IEnumerable_61(MetricsRecordFiltered _enclosing)
			{
				this._enclosing = _enclosing;
				this.it = this._enclosing.delegate_.metrics().GetEnumerator();
			}

			internal readonly System.Collections.Generic.IEnumerator<org.apache.hadoop.metrics2.AbstractMetric
				> it;

			public override System.Collections.Generic.IEnumerator<org.apache.hadoop.metrics2.AbstractMetric
				> GetEnumerator()
			{
				return new _AbstractIterator_64(this);
			}

			private sealed class _AbstractIterator_64 : com.google.common.collect.AbstractIterator
				<org.apache.hadoop.metrics2.AbstractMetric>
			{
				public _AbstractIterator_64(_IEnumerable_61 _enclosing)
				{
					this._enclosing = _enclosing;
				}

				protected override org.apache.hadoop.metrics2.AbstractMetric computeNext()
				{
					while (this._enclosing.it.MoveNext())
					{
						org.apache.hadoop.metrics2.AbstractMetric next = this._enclosing.it.Current;
						if (this._enclosing._enclosing.filter.accepts(next.name()))
						{
							return next;
						}
					}
					return this.endOfData();
				}

				private readonly _IEnumerable_61 _enclosing;
			}

			private readonly MetricsRecordFiltered _enclosing;
		}
	}
}
