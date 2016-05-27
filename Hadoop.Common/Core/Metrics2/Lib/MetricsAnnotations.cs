using Org.Apache.Hadoop.Metrics2;
using Sharpen;

namespace Org.Apache.Hadoop.Metrics2.Lib
{
	/// <summary>Metrics annotation helpers.</summary>
	public class MetricsAnnotations
	{
		/// <summary>Make an metrics source from an annotated object.</summary>
		/// <param name="source">the annotated object.</param>
		/// <returns>a metrics source</returns>
		public static MetricsSource MakeSource(object source)
		{
			return new MetricsSourceBuilder(source, DefaultMetricsFactory.GetAnnotatedMetricsFactory
				()).Build();
		}

		public static MetricsSourceBuilder NewSourceBuilder(object source)
		{
			return new MetricsSourceBuilder(source, DefaultMetricsFactory.GetAnnotatedMetricsFactory
				());
		}
	}
}
