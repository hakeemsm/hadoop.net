using Sharpen;

namespace org.apache.hadoop.conf
{
	/// <summary>A servlet for changing a node's configuration.</summary>
	/// <remarks>
	/// A servlet for changing a node's configuration.
	/// Reloads the configuration file, verifies whether changes are
	/// possible and asks the admin to approve the change.
	/// </remarks>
	[System.Serializable]
	public class ReconfigurationServlet : javax.servlet.http.HttpServlet
	{
		private const long serialVersionUID = 1L;

		private static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.conf.ReconfigurationServlet
			)));

		public const string CONF_SERVLET_RECONFIGURABLE_PREFIX = "conf.servlet.reconfigurable.";

		// the prefix used to fing the attribute holding the reconfigurable 
		// for a given request
		//
		// we get the attribute prefix + servlet path
		/// <exception cref="javax.servlet.ServletException"/>
		public override void init()
		{
			base.init();
		}

		private org.apache.hadoop.conf.Reconfigurable getReconfigurable(javax.servlet.http.HttpServletRequest
			 req)
		{
			LOG.info("servlet path: " + req.getServletPath());
			LOG.info("getting attribute: " + CONF_SERVLET_RECONFIGURABLE_PREFIX + req.getServletPath
				());
			return (org.apache.hadoop.conf.Reconfigurable)this.getServletContext().getAttribute
				(CONF_SERVLET_RECONFIGURABLE_PREFIX + req.getServletPath());
		}

		private void printHeader(java.io.PrintWriter @out, string nodeName)
		{
			@out.print("<html><head>");
			@out.printf("<title>%s Reconfiguration Utility</title>%n", org.apache.commons.lang.StringEscapeUtils
				.escapeHtml(nodeName));
			@out.print("</head><body>\n");
			@out.printf("<h1>%s Reconfiguration Utility</h1>%n", org.apache.commons.lang.StringEscapeUtils
				.escapeHtml(nodeName));
		}

		private void printFooter(java.io.PrintWriter @out)
		{
			@out.print("</body></html>\n");
		}

		/// <summary>Print configuration options that can be changed.</summary>
		private void printConf(java.io.PrintWriter @out, org.apache.hadoop.conf.Reconfigurable
			 reconf)
		{
			org.apache.hadoop.conf.Configuration oldConf = reconf.getConf();
			org.apache.hadoop.conf.Configuration newConf = new org.apache.hadoop.conf.Configuration
				();
			System.Collections.Generic.ICollection<org.apache.hadoop.conf.ReconfigurationUtil.PropertyChange
				> changes = org.apache.hadoop.conf.ReconfigurationUtil.getChangedProperties(newConf
				, oldConf);
			bool changeOK = true;
			@out.println("<form action=\"\" method=\"post\">");
			@out.println("<table border=\"1\">");
			@out.println("<tr><th>Property</th><th>Old value</th>");
			@out.println("<th>New value </th><th></th></tr>");
			foreach (org.apache.hadoop.conf.ReconfigurationUtil.PropertyChange c in changes)
			{
				@out.print("<tr><td>");
				if (!reconf.isPropertyReconfigurable(c.prop))
				{
					@out.print("<font color=\"red\">" + org.apache.commons.lang.StringEscapeUtils.escapeHtml
						(c.prop) + "</font>");
					changeOK = false;
				}
				else
				{
					@out.print(org.apache.commons.lang.StringEscapeUtils.escapeHtml(c.prop));
					@out.print("<input type=\"hidden\" name=\"" + org.apache.commons.lang.StringEscapeUtils
						.escapeHtml(c.prop) + "\" value=\"" + org.apache.commons.lang.StringEscapeUtils.
						escapeHtml(c.newVal) + "\"/>");
				}
				@out.print("</td><td>" + (c.oldVal == null ? "<it>default</it>" : org.apache.commons.lang.StringEscapeUtils
					.escapeHtml(c.oldVal)) + "</td><td>" + (c.newVal == null ? "<it>default</it>" : 
					org.apache.commons.lang.StringEscapeUtils.escapeHtml(c.newVal)) + "</td>");
				@out.print("</tr>\n");
			}
			@out.println("</table>");
			if (!changeOK)
			{
				@out.println("<p><font color=\"red\">WARNING: properties marked red" + " will not be changed until the next restart.</font></p>"
					);
			}
			@out.println("<input type=\"submit\" value=\"Apply\" />");
			@out.println("</form>");
		}

		private java.util.Enumeration<string> getParams(javax.servlet.http.HttpServletRequest
			 req)
		{
			return req.getParameterNames();
		}

		/// <summary>Apply configuratio changes after admin has approved them.</summary>
		/// <exception cref="org.apache.hadoop.conf.ReconfigurationException"/>
		private void applyChanges(java.io.PrintWriter @out, org.apache.hadoop.conf.Reconfigurable
			 reconf, javax.servlet.http.HttpServletRequest req)
		{
			org.apache.hadoop.conf.Configuration oldConf = reconf.getConf();
			org.apache.hadoop.conf.Configuration newConf = new org.apache.hadoop.conf.Configuration
				();
			java.util.Enumeration<string> @params = getParams(req);
			lock (oldConf)
			{
				while (@params.MoveNext())
				{
					string rawParam = @params.Current;
					string param = org.apache.commons.lang.StringEscapeUtils.unescapeHtml(rawParam);
					string value = org.apache.commons.lang.StringEscapeUtils.unescapeHtml(req.getParameter
						(rawParam));
					if (value != null)
					{
						if (value.Equals(newConf.getRaw(param)) || value.Equals("default") || value.Equals
							("null") || value.isEmpty())
						{
							if ((value.Equals("default") || value.Equals("null") || value.isEmpty()) && oldConf
								.getRaw(param) != null)
							{
								@out.println("<p>Changed \"" + org.apache.commons.lang.StringEscapeUtils.escapeHtml
									(param) + "\" from \"" + org.apache.commons.lang.StringEscapeUtils.escapeHtml(oldConf
									.getRaw(param)) + "\" to default</p>");
								reconf.reconfigureProperty(param, null);
							}
							else
							{
								if (!value.Equals("default") && !value.Equals("null") && !value.isEmpty() && (oldConf
									.getRaw(param) == null || !oldConf.getRaw(param).Equals(value)))
								{
									// change from default or value to different value
									if (oldConf.getRaw(param) == null)
									{
										@out.println("<p>Changed \"" + org.apache.commons.lang.StringEscapeUtils.escapeHtml
											(param) + "\" from default to \"" + org.apache.commons.lang.StringEscapeUtils.escapeHtml
											(value) + "\"</p>");
									}
									else
									{
										@out.println("<p>Changed \"" + org.apache.commons.lang.StringEscapeUtils.escapeHtml
											(param) + "\" from \"" + org.apache.commons.lang.StringEscapeUtils.escapeHtml(oldConf
											.getRaw(param)) + "\" to \"" + org.apache.commons.lang.StringEscapeUtils.escapeHtml
											(value) + "\"</p>");
									}
									reconf.reconfigureProperty(param, value);
								}
								else
								{
									LOG.info("property " + param + " unchanged");
								}
							}
						}
						else
						{
							// parameter value != newConf value
							@out.println("<p>\"" + org.apache.commons.lang.StringEscapeUtils.escapeHtml(param
								) + "\" not changed because value has changed from \"" + org.apache.commons.lang.StringEscapeUtils
								.escapeHtml(value) + "\" to \"" + org.apache.commons.lang.StringEscapeUtils.escapeHtml
								(newConf.getRaw(param)) + "\" since approval</p>");
						}
					}
				}
			}
		}

		/// <exception cref="javax.servlet.ServletException"/>
		/// <exception cref="System.IO.IOException"/>
		protected override void doGet(javax.servlet.http.HttpServletRequest req, javax.servlet.http.HttpServletResponse
			 resp)
		{
			LOG.info("GET");
			resp.setContentType("text/html");
			java.io.PrintWriter @out = resp.getWriter();
			org.apache.hadoop.conf.Reconfigurable reconf = getReconfigurable(req);
			string nodeName = Sharpen.Runtime.getClassForObject(reconf).getCanonicalName();
			printHeader(@out, nodeName);
			printConf(@out, reconf);
			printFooter(@out);
		}

		/// <exception cref="javax.servlet.ServletException"/>
		/// <exception cref="System.IO.IOException"/>
		protected override void doPost(javax.servlet.http.HttpServletRequest req, javax.servlet.http.HttpServletResponse
			 resp)
		{
			LOG.info("POST");
			resp.setContentType("text/html");
			java.io.PrintWriter @out = resp.getWriter();
			org.apache.hadoop.conf.Reconfigurable reconf = getReconfigurable(req);
			string nodeName = Sharpen.Runtime.getClassForObject(reconf).getCanonicalName();
			printHeader(@out, nodeName);
			try
			{
				applyChanges(@out, reconf, req);
			}
			catch (org.apache.hadoop.conf.ReconfigurationException e)
			{
				resp.sendError(javax.servlet.http.HttpServletResponse.SC_INTERNAL_SERVER_ERROR, org.apache.hadoop.util.StringUtils
					.stringifyException(e));
				return;
			}
			@out.println("<p><a href=\"" + req.getServletPath() + "\">back</a></p>");
			printFooter(@out);
		}
	}
}
