using System;
using System.IO;
using Hadoop.Common.Core.Conf;
using Javax.Servlet.Http;
using Org.Apache.Hadoop.Http;
using Sharpen;

namespace Org.Apache.Hadoop.Conf
{
	/// <summary>A servlet to print out the running configuration data.</summary>
	[System.Serializable]
	public class ConfServlet : HttpServlet
	{
		private const long serialVersionUID = 1L;

		private const string FormatJson = "json";

		private const string FormatXml = "xml";

		private const string FormatParam = "format";

		/// <summary>Return the Configuration of the daemon hosting this servlet.</summary>
		/// <remarks>
		/// Return the Configuration of the daemon hosting this servlet.
		/// This is populated when the HttpServer starts.
		/// </remarks>
		private Configuration GetConfFromContext()
		{
			Configuration conf = (Configuration)GetServletContext().GetAttribute(HttpServer2.
				ConfContextAttribute);
			System.Diagnostics.Debug.Assert(conf != null);
			return conf;
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
			string format = request.GetParameter(FormatParam);
			if (null == format)
			{
				format = FormatXml;
			}
			if (FormatXml.Equals(format))
			{
				response.SetContentType("text/xml; charset=utf-8");
			}
			else
			{
				if (FormatJson.Equals(format))
				{
					response.SetContentType("application/json; charset=utf-8");
				}
			}
			TextWriter @out = response.GetWriter();
			try
			{
				WriteResponse(GetConfFromContext(), @out, format);
			}
			catch (ConfServlet.BadFormatException bfe)
			{
				response.SendError(HttpServletResponse.ScBadRequest, bfe.Message);
			}
			@out.Close();
		}

		/// <summary>Guts of the servlet - extracted for easy testing.</summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.Conf.ConfServlet.BadFormatException"/>
		internal static void WriteResponse(Configuration conf, TextWriter @out, string format
			)
		{
			if (FormatJson.Equals(format))
			{
				Configuration.DumpConfiguration(conf, @out);
			}
			else
			{
				if (FormatXml.Equals(format))
				{
					conf.WriteXml(@out);
				}
				else
				{
					throw new ConfServlet.BadFormatException("Bad format: " + format);
				}
			}
		}

		[System.Serializable]
		public class BadFormatException : Exception
		{
			private const long serialVersionUID = 1L;

			public BadFormatException(string msg)
				: base(msg)
			{
			}
		}
	}
}
