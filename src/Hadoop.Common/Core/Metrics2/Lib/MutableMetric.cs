using Org.Apache.Hadoop.Metrics2;


namespace Org.Apache.Hadoop.Metrics2.Lib
{
	/// <summary>The mutable metric interface</summary>
	public abstract class MutableMetric
	{
		private volatile bool changed = true;

		/// <summary>Get a snapshot of the metric</summary>
		/// <param name="builder">the metrics record builder</param>
		/// <param name="all">if true, snapshot unchanged metrics as well</param>
		public abstract void Snapshot(MetricsRecordBuilder builder, bool all);

		/// <summary>Get a snapshot of metric if changed</summary>
		/// <param name="builder">the metrics record builder</param>
		public virtual void Snapshot(MetricsRecordBuilder builder)
		{
			Snapshot(builder, false);
		}

		/// <summary>Set the changed flag in mutable operations</summary>
		protected internal virtual void SetChanged()
		{
			changed = true;
		}

		/// <summary>Clear the changed flag in the snapshot operations</summary>
		protected internal virtual void ClearChanged()
		{
			changed = false;
		}

		/// <returns>true if metric is changed since last snapshot/snapshot</returns>
		public virtual bool Changed()
		{
			return changed;
		}
	}
}
