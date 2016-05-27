using System;
using System.Net;
using Org.Apache.Commons.Logging;
using Sharpen;

namespace Org.Apache.Hadoop.Metrics
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
		public static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Metrics.MetricsUtil
			));

		/// <summary>Don't allow creation of a new instance of Metrics</summary>
		private MetricsUtil()
		{
		}

		public static MetricsContext GetContext(string contextName)
		{
			return GetContext(contextName, contextName);
		}

		/// <summary>Utility method to return the named context.</summary>
		/// <remarks>
		/// Utility method to return the named context.
		/// If the desired context cannot be created for any reason, the exception
		/// is logged, and a null context is returned.
		/// </remarks>
		public static MetricsContext GetContext(string refName, string contextName)
		{
			MetricsContext metricsContext;
			try
			{
				metricsContext = ContextFactory.GetFactory().GetContext(refName, contextName);
				if (!metricsContext.IsMonitoring())
				{
					metricsContext.StartMonitoring();
				}
			}
			catch (Exception ex)
			{
				Log.Error("Unable to create metrics context " + contextName, ex);
				metricsContext = ContextFactory.GetNullContext(contextName);
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
		public static MetricsRecord CreateRecord(MetricsContext context, string recordName
			)
		{
			MetricsRecord metricsRecord = context.CreateRecord(recordName);
			metricsRecord.SetTag("hostName", GetHostName());
			return metricsRecord;
		}

		/// <summary>Returns the host name.</summary>
		/// <remarks>
		/// Returns the host name.  If the host name is unobtainable, logs the
		/// exception and returns "unknown".
		/// </remarks>
		private static string GetHostName()
		{
			string hostName = null;
			try
			{
				hostName = Sharpen.Runtime.GetLocalHost().GetHostName();
			}
			catch (UnknownHostException ex)
			{
				Log.Info("Unable to obtain hostName", ex);
				hostName = "unknown";
			}
			return hostName;
		}
	}
}
