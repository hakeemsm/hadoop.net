using Sharpen;

namespace org.apache.hadoop.metrics2.lib
{
	/// <summary>Metrics annotation helpers.</summary>
	public class MetricsAnnotations
	{
		/// <summary>Make an metrics source from an annotated object.</summary>
		/// <param name="source">the annotated object.</param>
		/// <returns>a metrics source</returns>
		public static org.apache.hadoop.metrics2.MetricsSource makeSource(object source)
		{
			return new org.apache.hadoop.metrics2.lib.MetricsSourceBuilder(source, org.apache.hadoop.metrics2.lib.DefaultMetricsFactory
				.getAnnotatedMetricsFactory()).build();
		}

		public static org.apache.hadoop.metrics2.lib.MetricsSourceBuilder newSourceBuilder
			(object source)
		{
			return new org.apache.hadoop.metrics2.lib.MetricsSourceBuilder(source, org.apache.hadoop.metrics2.lib.DefaultMetricsFactory
				.getAnnotatedMetricsFactory());
		}
	}
}
