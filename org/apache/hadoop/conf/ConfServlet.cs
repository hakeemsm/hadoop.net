using Sharpen;

namespace org.apache.hadoop.conf
{
	/// <summary>A servlet to print out the running configuration data.</summary>
	[System.Serializable]
	public class ConfServlet : javax.servlet.http.HttpServlet
	{
		private const long serialVersionUID = 1L;

		private const string FORMAT_JSON = "json";

		private const string FORMAT_XML = "xml";

		private const string FORMAT_PARAM = "format";

		/// <summary>Return the Configuration of the daemon hosting this servlet.</summary>
		/// <remarks>
		/// Return the Configuration of the daemon hosting this servlet.
		/// This is populated when the HttpServer starts.
		/// </remarks>
		private org.apache.hadoop.conf.Configuration getConfFromContext()
		{
			org.apache.hadoop.conf.Configuration conf = (org.apache.hadoop.conf.Configuration
				)getServletContext().getAttribute(org.apache.hadoop.http.HttpServer2.CONF_CONTEXT_ATTRIBUTE
				);
			System.Diagnostics.Debug.Assert(conf != null);
			return conf;
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
			string format = request.getParameter(FORMAT_PARAM);
			if (null == format)
			{
				format = FORMAT_XML;
			}
			if (FORMAT_XML.Equals(format))
			{
				response.setContentType("text/xml; charset=utf-8");
			}
			else
			{
				if (FORMAT_JSON.Equals(format))
				{
					response.setContentType("application/json; charset=utf-8");
				}
			}
			System.IO.TextWriter @out = response.getWriter();
			try
			{
				writeResponse(getConfFromContext(), @out, format);
			}
			catch (org.apache.hadoop.conf.ConfServlet.BadFormatException bfe)
			{
				response.sendError(javax.servlet.http.HttpServletResponse.SC_BAD_REQUEST, bfe.Message
					);
			}
			@out.close();
		}

		/// <summary>Guts of the servlet - extracted for easy testing.</summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="org.apache.hadoop.conf.ConfServlet.BadFormatException"/>
		internal static void writeResponse(org.apache.hadoop.conf.Configuration conf, System.IO.TextWriter
			 @out, string format)
		{
			if (FORMAT_JSON.Equals(format))
			{
				org.apache.hadoop.conf.Configuration.dumpConfiguration(conf, @out);
			}
			else
			{
				if (FORMAT_XML.Equals(format))
				{
					conf.writeXml(@out);
				}
				else
				{
					throw new org.apache.hadoop.conf.ConfServlet.BadFormatException("Bad format: " + 
						format);
				}
			}
		}

		[System.Serializable]
		public class BadFormatException : System.Exception
		{
			private const long serialVersionUID = 1L;

			public BadFormatException(string msg)
				: base(msg)
			{
			}
		}
	}
}
