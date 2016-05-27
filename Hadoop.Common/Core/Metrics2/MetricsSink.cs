using Sharpen;

namespace Org.Apache.Hadoop.Metrics2
{
	/// <summary>The metrics sink interface.</summary>
	/// <remarks>
	/// The metrics sink interface. <p>
	/// Implementations of this interface consume the
	/// <see cref="MetricsRecord"/>
	/// generated
	/// from
	/// <see cref="MetricsSource"/>
	/// . It registers with
	/// <see cref="MetricsSystem"/>
	/// which
	/// periodically pushes the
	/// <see cref="MetricsRecord"/>
	/// to the sink using
	/// <see cref="PutMetrics(MetricsRecord)"/>
	/// method.  If the implementing class also
	/// implements
	/// <see cref="System.IDisposable"/>
	/// , then the MetricsSystem will close the sink when
	/// it is stopped.
	/// </remarks>
	public interface MetricsSink : MetricsPlugin
	{
		/// <summary>Put a metrics record in the sink</summary>
		/// <param name="record">the record to put</param>
		void PutMetrics(MetricsRecord record);

		/// <summary>Flush any buffered metrics</summary>
		void Flush();
	}
}
