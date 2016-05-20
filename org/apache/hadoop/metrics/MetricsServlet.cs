using Sharpen;

namespace org.apache.hadoop.metrics
{
	/// <summary>A servlet to print out metrics data.</summary>
	/// <remarks>
	/// A servlet to print out metrics data.  By default, the servlet returns a
	/// textual representation (no promises are made for parseability), and
	/// users can use "?format=json" for parseable output.
	/// </remarks>
	[System.Serializable]
	public class MetricsServlet : javax.servlet.http.HttpServlet
	{
		/// <summary>A helper class to hold a TagMap and MetricMap.</summary>
		internal class TagsMetricsPair : org.mortbay.util.ajax.JSON.Convertible
		{
			internal readonly org.apache.hadoop.metrics.spi.AbstractMetricsContext.TagMap tagMap;

			internal readonly org.apache.hadoop.metrics.spi.AbstractMetricsContext.MetricMap 
				metricMap;

			public TagsMetricsPair(org.apache.hadoop.metrics.spi.AbstractMetricsContext.TagMap
				 tagMap, org.apache.hadoop.metrics.spi.AbstractMetricsContext.MetricMap metricMap
				)
			{
				this.tagMap = tagMap;
				this.metricMap = metricMap;
			}

			public virtual void fromJSON(System.Collections.IDictionary map)
			{
				throw new System.NotSupportedException();
			}

			/// <summary>Converts to JSON by providing an array.</summary>
			public virtual void toJSON(org.mortbay.util.ajax.JSON.Output @out)
			{
				@out.add(new object[] { tagMap, metricMap });
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
		internal virtual System.Collections.Generic.IDictionary<string, System.Collections.Generic.IDictionary
			<string, System.Collections.Generic.IList<org.apache.hadoop.metrics.MetricsServlet.TagsMetricsPair
			>>> makeMap(System.Collections.Generic.ICollection<org.apache.hadoop.metrics.MetricsContext
			> contexts)
		{
			System.Collections.Generic.IDictionary<string, System.Collections.Generic.IDictionary
				<string, System.Collections.Generic.IList<org.apache.hadoop.metrics.MetricsServlet.TagsMetricsPair
				>>> map = new System.Collections.Generic.SortedDictionary<string, System.Collections.Generic.IDictionary
				<string, System.Collections.Generic.IList<org.apache.hadoop.metrics.MetricsServlet.TagsMetricsPair
				>>>();
			foreach (org.apache.hadoop.metrics.MetricsContext context in contexts)
			{
				System.Collections.Generic.IDictionary<string, System.Collections.Generic.IList<org.apache.hadoop.metrics.MetricsServlet.TagsMetricsPair
					>> records = new System.Collections.Generic.SortedDictionary<string, System.Collections.Generic.IList
					<org.apache.hadoop.metrics.MetricsServlet.TagsMetricsPair>>();
				map[context.getContextName()] = records;
				foreach (System.Collections.Generic.KeyValuePair<string, System.Collections.Generic.ICollection
					<org.apache.hadoop.metrics.spi.OutputRecord>> r in context.getAllRecords())
				{
					System.Collections.Generic.IList<org.apache.hadoop.metrics.MetricsServlet.TagsMetricsPair
						> metricsAndTags = new System.Collections.Generic.List<org.apache.hadoop.metrics.MetricsServlet.TagsMetricsPair
						>();
					records[r.Key] = metricsAndTags;
					foreach (org.apache.hadoop.metrics.spi.OutputRecord outputRecord in r.Value)
					{
						org.apache.hadoop.metrics.spi.AbstractMetricsContext.TagMap tagMap = outputRecord
							.getTagsCopy();
						org.apache.hadoop.metrics.spi.AbstractMetricsContext.MetricMap metricMap = outputRecord
							.getMetricsCopy();
						metricsAndTags.add(new org.apache.hadoop.metrics.MetricsServlet.TagsMetricsPair(tagMap
							, metricMap));
					}
				}
			}
			return map;
		}

		/// <exception cref="javax.servlet.ServletException"/>
		/// <exception cref="System.IO.IOException"/>
		protected override void doGet(javax.servlet.http.HttpServletRequest request, javax.servlet.http.HttpServletResponse
			 response)
		{
			if (!org.apache.hadoop.http.HttpServer2.isInstrumentationAccessAllowed(getServletContext
				(), request, response))
			{
				return;
			}
			string format = request.getParameter("format");
			System.Collections.Generic.ICollection<org.apache.hadoop.metrics.MetricsContext> 
				allContexts = org.apache.hadoop.metrics.ContextFactory.getFactory().getAllContexts
				();
			if ("json".Equals(format))
			{
				response.setContentType("application/json; charset=utf-8");
				java.io.PrintWriter @out = response.getWriter();
				try
				{
					// Uses Jetty's built-in JSON support to convert the map into JSON.
					@out.print(new org.mortbay.util.ajax.JSON().toJSON(makeMap(allContexts)));
				}
				finally
				{
					@out.close();
				}
			}
			else
			{
				java.io.PrintWriter @out = response.getWriter();
				try
				{
					printMap(@out, makeMap(allContexts));
				}
				finally
				{
					@out.close();
				}
			}
		}

		/// <summary>Prints metrics data in a multi-line text form.</summary>
		internal virtual void printMap(java.io.PrintWriter @out, System.Collections.Generic.IDictionary
			<string, System.Collections.Generic.IDictionary<string, System.Collections.Generic.IList
			<org.apache.hadoop.metrics.MetricsServlet.TagsMetricsPair>>> map)
		{
			foreach (System.Collections.Generic.KeyValuePair<string, System.Collections.Generic.IDictionary
				<string, System.Collections.Generic.IList<org.apache.hadoop.metrics.MetricsServlet.TagsMetricsPair
				>>> context in map)
			{
				@out.print(context.Key);
				@out.print("\n");
				foreach (System.Collections.Generic.KeyValuePair<string, System.Collections.Generic.IList
					<org.apache.hadoop.metrics.MetricsServlet.TagsMetricsPair>> record in context.Value)
				{
					indent(@out, 1);
					@out.print(record.Key);
					@out.print("\n");
					foreach (org.apache.hadoop.metrics.MetricsServlet.TagsMetricsPair pair in record.
						Value)
					{
						indent(@out, 2);
						// Prints tag values in the form "{key=value,key=value}:"
						@out.print("{");
						bool first = true;
						foreach (System.Collections.Generic.KeyValuePair<string, object> tagValue in pair
							.tagMap)
						{
							if (first)
							{
								first = false;
							}
							else
							{
								@out.print(",");
							}
							@out.print(tagValue.Key);
							@out.print("=");
							@out.print(tagValue.Value.ToString());
						}
						@out.print("}:\n");
						// Now print metric values, one per line
						foreach (System.Collections.Generic.KeyValuePair<string, java.lang.Number> metricValue
							 in pair.metricMap)
						{
							indent(@out, 3);
							@out.print(metricValue.Key);
							@out.print("=");
							@out.print(metricValue.Value.ToString());
							@out.print("\n");
						}
					}
				}
			}
		}

		private void indent(java.io.PrintWriter @out, int indent)
		{
			for (int i = 0; i < indent; ++i)
			{
				@out.Append("  ");
			}
		}
	}
}
