using Sharpen;

namespace org.apache.hadoop.metrics
{
	/// <summary>Utility class to simplify creation and reporting of hadoop metrics.</summary>
	/// <remarks>
	/// Utility class to simplify creation and reporting of hadoop metrics.
	/// For examples of usage, see NameNodeMetrics.
	/// </remarks>
	/// <seealso cref="MetricsRecord"/>
	/// <seealso cref="MetricsContext"/>
	/// <seealso cref="ContextFactory"/>
	public class MetricsUtil
	{
		public static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.metrics.MetricsUtil
			)));

		/// <summary>Don't allow creation of a new instance of Metrics</summary>
		private MetricsUtil()
		{
		}

		public static org.apache.hadoop.metrics.MetricsContext getContext(string contextName
			)
		{
			return getContext(contextName, contextName);
		}

		/// <summary>Utility method to return the named context.</summary>
		/// <remarks>
		/// Utility method to return the named context.
		/// If the desired context cannot be created for any reason, the exception
		/// is logged, and a null context is returned.
		/// </remarks>
		public static org.apache.hadoop.metrics.MetricsContext getContext(string refName, 
			string contextName)
		{
			org.apache.hadoop.metrics.MetricsContext metricsContext;
			try
			{
				metricsContext = org.apache.hadoop.metrics.ContextFactory.getFactory().getContext
					(refName, contextName);
				if (!metricsContext.isMonitoring())
				{
					metricsContext.startMonitoring();
				}
			}
			catch (System.Exception ex)
			{
				LOG.error("Unable to create metrics context " + contextName, ex);
				metricsContext = org.apache.hadoop.metrics.ContextFactory.getNullContext(contextName
					);
			}
			return metricsContext;
		}

		/// <summary>
		/// Utility method to create and return new metrics record instance within the
		/// given context.
		/// </summary>
		/// <remarks>
		/// Utility method to create and return new metrics record instance within the
		/// given context. This record is tagged with the host name.
		/// </remarks>
		/// <param name="context">the context</param>
		/// <param name="recordName">name of the record</param>
		/// <returns>newly created metrics record</returns>
		public static org.apache.hadoop.metrics.MetricsRecord createRecord(org.apache.hadoop.metrics.MetricsContext
			 context, string recordName)
		{
			org.apache.hadoop.metrics.MetricsRecord metricsRecord = context.createRecord(recordName
				);
			metricsRecord.setTag("hostName", getHostName());
			return metricsRecord;
		}

		/// <summary>Returns the host name.</summary>
		/// <remarks>
		/// Returns the host name.  If the host name is unobtainable, logs the
		/// exception and returns "unknown".
		/// </remarks>
		private static string getHostName()
		{
			string hostName = null;
			try
			{
				hostName = java.net.InetAddress.getLocalHost().getHostName();
			}
			catch (java.net.UnknownHostException ex)
			{
				LOG.info("Unable to obtain hostName", ex);
				hostName = "unknown";
			}
			return hostName;
		}
	}
}
