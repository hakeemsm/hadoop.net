using Sharpen;

namespace org.apache.hadoop.metrics.spi
{
	/// <summary>
	/// A MetricsContext that does not emit data, but, unlike NullContextWithUpdate,
	/// does save it for retrieval with getAllRecords().
	/// </summary>
	/// <remarks>
	/// A MetricsContext that does not emit data, but, unlike NullContextWithUpdate,
	/// does save it for retrieval with getAllRecords().
	/// This is useful if you want to support
	/// <see cref="org.apache.hadoop.metrics.MetricsServlet"/>
	/// , but
	/// not emit metrics in any other way.
	/// </remarks>
	public class NoEmitMetricsContext : org.apache.hadoop.metrics.spi.AbstractMetricsContext
	{
		private const string PERIOD_PROPERTY = "period";

		/// <summary>Creates a new instance of NullContextWithUpdateThread</summary>
		[org.apache.hadoop.classification.InterfaceAudience.Private]
		public NoEmitMetricsContext()
		{
		}

		[org.apache.hadoop.classification.InterfaceAudience.Private]
		public override void init(string contextName, org.apache.hadoop.metrics.ContextFactory
			 factory)
		{
			base.init(contextName, factory);
			parseAndSetPeriod(PERIOD_PROPERTY);
		}

		/// <summary>Do-nothing version of emitRecord</summary>
		[org.apache.hadoop.classification.InterfaceAudience.Private]
		protected internal override void emitRecord(string contextName, string recordName
			, org.apache.hadoop.metrics.spi.OutputRecord outRec)
		{
		}
	}
}
