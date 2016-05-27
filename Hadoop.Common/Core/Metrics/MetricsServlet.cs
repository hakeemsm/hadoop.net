using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using Javax.Servlet.Http;
using Org.Apache.Hadoop.Http;
using Org.Apache.Hadoop.Metrics.Spi;
using Org.Mortbay.Util.Ajax;
using Sharpen;

namespace Org.Apache.Hadoop.Metrics
{
	/// <summary>A servlet to print out metrics data.</summary>
	/// <remarks>
	/// A servlet to print out metrics data.  By default, the servlet returns a
	/// textual representation (no promises are made for parseability), and
	/// users can use "?format=json" for parseable output.
	/// </remarks>
	[System.Serializable]
	public class MetricsServlet : HttpServlet
	{
		/// <summary>A helper class to hold a TagMap and MetricMap.</summary>
		internal class TagsMetricsPair : JSON.Convertible
		{
			internal readonly AbstractMetricsContext.TagMap tagMap;

			internal readonly AbstractMetricsContext.MetricMap metricMap;

			public TagsMetricsPair(AbstractMetricsContext.TagMap tagMap, AbstractMetricsContext.MetricMap
				 metricMap)
			{
				this.tagMap = tagMap;
				this.metricMap = metricMap;
			}

			public virtual void FromJSON(IDictionary map)
			{
				throw new NotSupportedException();
			}

			/// <summary>Converts to JSON by providing an array.</summary>
			public virtual void ToJSON(JSON.Output @out)
			{
				@out.Add(new object[] { tagMap, metricMap });
			}
		}

		/// <summary>
		/// Collects all metric data, and returns a map:
		/// contextName -&gt; recordName -&gt; [ (tag-&gt;tagValue), (metric-&gt;metricValue) ].
		/// </summary>
		/// <remarks>
		/// Collects all metric data, and returns a map:
		/// contextName -&gt; recordName -&gt; [ (tag-&gt;tagValue), (metric-&gt;metricValue) ].
		/// The values are either String or Number.  The final value is implemented
		/// as a list of TagsMetricsPair.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		internal virtual IDictionary<string, IDictionary<string, IList<MetricsServlet.TagsMetricsPair
			>>> MakeMap(ICollection<MetricsContext> contexts)
		{
			IDictionary<string, IDictionary<string, IList<MetricsServlet.TagsMetricsPair>>> map
				 = new SortedDictionary<string, IDictionary<string, IList<MetricsServlet.TagsMetricsPair
				>>>();
			foreach (MetricsContext context in contexts)
			{
				IDictionary<string, IList<MetricsServlet.TagsMetricsPair>> records = new SortedDictionary
					<string, IList<MetricsServlet.TagsMetricsPair>>();
				map[context.GetContextName()] = records;
				foreach (KeyValuePair<string, ICollection<OutputRecord>> r in context.GetAllRecords
					())
				{
					IList<MetricsServlet.TagsMetricsPair> metricsAndTags = new AList<MetricsServlet.TagsMetricsPair
						>();
					records[r.Key] = metricsAndTags;
					foreach (OutputRecord outputRecord in r.Value)
					{
						AbstractMetricsContext.TagMap tagMap = outputRecord.GetTagsCopy();
						AbstractMetricsContext.MetricMap metricMap = outputRecord.GetMetricsCopy();
						metricsAndTags.AddItem(new MetricsServlet.TagsMetricsPair(tagMap, metricMap));
					}
				}
			}
			return map;
		}

		/// <exception cref="Javax.Servlet.ServletException"/>
		/// <exception cref="System.IO.IOException"/>
		protected override void DoGet(HttpServletRequest request, HttpServletResponse response
			)
		{
			if (!HttpServer2.IsInstrumentationAccessAllowed(GetServletContext(), request, response
				))
			{
				return;
			}
			string format = request.GetParameter("format");
			ICollection<MetricsContext> allContexts = ContextFactory.GetFactory().GetAllContexts
				();
			if ("json".Equals(format))
			{
				response.SetContentType("application/json; charset=utf-8");
				PrintWriter @out = response.GetWriter();
				try
				{
					// Uses Jetty's built-in JSON support to convert the map into JSON.
					@out.Write(new JSON().ToJSON(MakeMap(allContexts)));
				}
				finally
				{
					@out.Close();
				}
			}
			else
			{
				PrintWriter @out = response.GetWriter();
				try
				{
					PrintMap(@out, MakeMap(allContexts));
				}
				finally
				{
					@out.Close();
				}
			}
		}

		/// <summary>Prints metrics data in a multi-line text form.</summary>
		internal virtual void PrintMap(PrintWriter @out, IDictionary<string, IDictionary<
			string, IList<MetricsServlet.TagsMetricsPair>>> map)
		{
			foreach (KeyValuePair<string, IDictionary<string, IList<MetricsServlet.TagsMetricsPair
				>>> context in map)
			{
				@out.Write(context.Key);
				@out.Write("\n");
				foreach (KeyValuePair<string, IList<MetricsServlet.TagsMetricsPair>> record in context
					.Value)
				{
					Indent(@out, 1);
					@out.Write(record.Key);
					@out.Write("\n");
					foreach (MetricsServlet.TagsMetricsPair pair in record.Value)
					{
						Indent(@out, 2);
						// Prints tag values in the form "{key=value,key=value}:"
						@out.Write("{");
						bool first = true;
						foreach (KeyValuePair<string, object> tagValue in pair.tagMap)
						{
							if (first)
							{
								first = false;
							}
							else
							{
								@out.Write(",");
							}
							@out.Write(tagValue.Key);
							@out.Write("=");
							@out.Write(tagValue.Value.ToString());
						}
						@out.Write("}:\n");
						// Now print metric values, one per line
						foreach (KeyValuePair<string, Number> metricValue in pair.metricMap)
						{
							Indent(@out, 3);
							@out.Write(metricValue.Key);
							@out.Write("=");
							@out.Write(metricValue.Value.ToString());
							@out.Write("\n");
						}
					}
				}
			}
		}

		private void Indent(PrintWriter @out, int indent)
		{
			for (int i = 0; i < indent; ++i)
			{
				@out.Append("  ");
			}
		}
	}
}
