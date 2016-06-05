using System.Collections.Generic;
using Com.Google.Common.Collect;
using Org.Apache.Hadoop.Metrics2;


namespace Org.Apache.Hadoop.Metrics2.Impl
{
	internal class MetricsRecordFiltered : AbstractMetricsRecord
	{
		private readonly MetricsRecord delegate_;

		private readonly MetricsFilter filter;

		internal MetricsRecordFiltered(MetricsRecord delegate_, MetricsFilter filter)
		{
			this.delegate_ = delegate_;
			this.filter = filter;
		}

		public override long Timestamp()
		{
			return delegate_.Timestamp();
		}

		public override string Name()
		{
			return delegate_.Name();
		}

		public override string Description()
		{
			return delegate_.Description();
		}

		public override string Context()
		{
			return delegate_.Context();
		}

		public override ICollection<MetricsTag> Tags()
		{
			return delegate_.Tags();
		}

		public override IEnumerable<AbstractMetric> Metrics()
		{
			return new _IEnumerable_61(this);
		}

		private sealed class _IEnumerable_61 : IEnumerable<AbstractMetric>
		{
			public _IEnumerable_61(MetricsRecordFiltered _enclosing)
			{
				this._enclosing = _enclosing;
				this.it = this._enclosing.delegate_.Metrics().GetEnumerator();
			}

			internal readonly IEnumerator<AbstractMetric> it;

			public override IEnumerator<AbstractMetric> GetEnumerator()
			{
				return new _AbstractIterator_64(this);
			}

			private sealed class _AbstractIterator_64 : AbstractIterator<AbstractMetric>
			{
				public _AbstractIterator_64(_IEnumerable_61 _enclosing)
				{
					this._enclosing = _enclosing;
				}

				protected override AbstractMetric ComputeNext()
				{
					while (this._enclosing.it.HasNext())
					{
						AbstractMetric next = this._enclosing.it.Next();
						if (this._enclosing._enclosing.filter.Accepts(next.Name()))
						{
							return next;
						}
					}
					return this.EndOfData();
				}

				private readonly _IEnumerable_61 _enclosing;
			}

			private readonly MetricsRecordFiltered _enclosing;
		}
	}
}
