using Org.Apache.Hadoop.Classification;
using Org.Apache.Hadoop.Metrics;
using Sharpen;

namespace Org.Apache.Hadoop.Metrics.Spi
{
	/// <summary>
	/// A MetricsContext that does not emit data, but, unlike NullContextWithUpdate,
	/// does save it for retrieval with getAllRecords().
	/// </summary>
	/// <remarks>
	/// A MetricsContext that does not emit data, but, unlike NullContextWithUpdate,
	/// does save it for retrieval with getAllRecords().
	/// This is useful if you want to support
	/// <see cref="Org.Apache.Hadoop.Metrics.MetricsServlet"/>
	/// , but
	/// not emit metrics in any other way.
	/// </remarks>
	public class NoEmitMetricsContext : AbstractMetricsContext
	{
		private const string PeriodProperty = "period";

		/// <summary>Creates a new instance of NullContextWithUpdateThread</summary>
		[InterfaceAudience.Private]
		public NoEmitMetricsContext()
		{
		}

		[InterfaceAudience.Private]
		public override void Init(string contextName, ContextFactory factory)
		{
			base.Init(contextName, factory);
			ParseAndSetPeriod(PeriodProperty);
		}

		/// <summary>Do-nothing version of emitRecord</summary>
		[InterfaceAudience.Private]
		protected internal override void EmitRecord(string contextName, string recordName
			, OutputRecord outRec)
		{
		}
	}
}
