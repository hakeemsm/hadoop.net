using System.Collections.Generic;
using System.IO;
using Hadoop.Common.Core.Conf;
using Javax.Servlet.Http;
using Org.Apache.Commons.Lang;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Util;


namespace Org.Apache.Hadoop.Conf
{
	/// <summary>A servlet for changing a node's configuration.</summary>
	/// <remarks>
	/// A servlet for changing a node's configuration.
	/// Reloads the configuration file, verifies whether changes are
	/// possible and asks the admin to approve the change.
	/// </remarks>
	[System.Serializable]
	public class ReconfigurationServlet : HttpServlet
	{
		private const long serialVersionUID = 1L;

		private static readonly Log Log = LogFactory.GetLog(typeof(ReconfigurationServlet
			));

		public const string ConfServletReconfigurablePrefix = "conf.servlet.reconfigurable.";

		// the prefix used to fing the attribute holding the reconfigurable 
		// for a given request
		//
		// we get the attribute prefix + servlet path
		/// <exception cref="Javax.Servlet.ServletException"/>
		public override void Init()
		{
			base.Init();
		}

		private Reconfigurable GetReconfigurable(HttpServletRequest req)
		{
			Log.Info("servlet path: " + req.GetServletPath());
			Log.Info("getting attribute: " + ConfServletReconfigurablePrefix + req.GetServletPath
				());
			return (Reconfigurable)this.GetServletContext().GetAttribute(ConfServletReconfigurablePrefix
				 + req.GetServletPath());
		}

		private void PrintHeader(PrintWriter @out, string nodeName)
		{
			@out.Write("<html><head>");
			@out.Printf("<title>%s Reconfiguration Utility</title>%n", StringEscapeUtils.EscapeHtml
				(nodeName));
			@out.Write("</head><body>\n");
			@out.Printf("<h1>%s Reconfiguration Utility</h1>%n", StringEscapeUtils.EscapeHtml
				(nodeName));
		}

		private void PrintFooter(PrintWriter @out)
		{
			@out.Write("</body></html>\n");
		}

		/// <summary>Print configuration options that can be changed.</summary>
		private void PrintConf(PrintWriter @out, Reconfigurable reconf)
		{
			Configuration oldConf = reconf.GetConf();
			Configuration newConf = new Configuration();
			ICollection<ReconfigurationUtil.PropertyChange> changes = ReconfigurationUtil.GetChangedProperties
				(newConf, oldConf);
			bool changeOK = true;
			@out.WriteLine("<form action=\"\" method=\"post\">");
			@out.WriteLine("<table border=\"1\">");
			@out.WriteLine("<tr><th>Property</th><th>Old value</th>");
			@out.WriteLine("<th>New value </th><th></th></tr>");
			foreach (ReconfigurationUtil.PropertyChange c in changes)
			{
				@out.Write("<tr><td>");
				if (!reconf.IsPropertyReconfigurable(c.prop))
				{
					@out.Write("<font color=\"red\">" + StringEscapeUtils.EscapeHtml(c.prop) + "</font>"
						);
					changeOK = false;
				}
				else
				{
					@out.Write(StringEscapeUtils.EscapeHtml(c.prop));
					@out.Write("<input type=\"hidden\" name=\"" + StringEscapeUtils.EscapeHtml(c.prop
						) + "\" value=\"" + StringEscapeUtils.EscapeHtml(c.newVal) + "\"/>");
				}
				@out.Write("</td><td>" + (c.oldVal == null ? "<it>default</it>" : StringEscapeUtils
					.EscapeHtml(c.oldVal)) + "</td><td>" + (c.newVal == null ? "<it>default</it>" : 
					StringEscapeUtils.EscapeHtml(c.newVal)) + "</td>");
				@out.Write("</tr>\n");
			}
			@out.WriteLine("</table>");
			if (!changeOK)
			{
				@out.WriteLine("<p><font color=\"red\">WARNING: properties marked red" + " will not be changed until the next restart.</font></p>"
					);
			}
			@out.WriteLine("<input type=\"submit\" value=\"Apply\" />");
			@out.WriteLine("</form>");
		}

		private Enumeration<string> GetParams(HttpServletRequest req)
		{
			return req.GetParameterNames();
		}

		/// <summary>Apply configuratio changes after admin has approved them.</summary>
		/// <exception cref="Org.Apache.Hadoop.Conf.ReconfigurationException"/>
		private void ApplyChanges(PrintWriter @out, Reconfigurable reconf, HttpServletRequest
			 req)
		{
			Configuration oldConf = reconf.GetConf();
			Configuration newConf = new Configuration();
			Enumeration<string> @params = GetParams(req);
			lock (oldConf)
			{
				while (@params.MoveNext())
				{
					string rawParam = @params.Current;
					string param = StringEscapeUtils.UnescapeHtml(rawParam);
					string value = StringEscapeUtils.UnescapeHtml(req.GetParameter(rawParam));
					if (value != null)
					{
						if (value.Equals(newConf.GetRaw(param)) || value.Equals("default") || value.Equals
							("null") || value.IsEmpty())
						{
							if ((value.Equals("default") || value.Equals("null") || value.IsEmpty()) && oldConf
								.GetRaw(param) != null)
							{
								@out.WriteLine("<p>Changed \"" + StringEscapeUtils.EscapeHtml(param) + "\" from \""
									 + StringEscapeUtils.EscapeHtml(oldConf.GetRaw(param)) + "\" to default</p>");
								reconf.ReconfigureProperty(param, null);
							}
							else
							{
								if (!value.Equals("default") && !value.Equals("null") && !value.IsEmpty() && (oldConf
									.GetRaw(param) == null || !oldConf.GetRaw(param).Equals(value)))
								{
									// change from default or value to different value
									if (oldConf.GetRaw(param) == null)
									{
										@out.WriteLine("<p>Changed \"" + StringEscapeUtils.EscapeHtml(param) + "\" from default to \""
											 + StringEscapeUtils.EscapeHtml(value) + "\"</p>");
									}
									else
									{
										@out.WriteLine("<p>Changed \"" + StringEscapeUtils.EscapeHtml(param) + "\" from \""
											 + StringEscapeUtils.EscapeHtml(oldConf.GetRaw(param)) + "\" to \"" + StringEscapeUtils
											.EscapeHtml(value) + "\"</p>");
									}
									reconf.ReconfigureProperty(param, value);
								}
								else
								{
									Log.Info("property " + param + " unchanged");
								}
							}
						}
						else
						{
							// parameter value != newConf value
							@out.WriteLine("<p>\"" + StringEscapeUtils.EscapeHtml(param) + "\" not changed because value has changed from \""
								 + StringEscapeUtils.EscapeHtml(value) + "\" to \"" + StringEscapeUtils.EscapeHtml
								(newConf.GetRaw(param)) + "\" since approval</p>");
						}
					}
				}
			}
		}

		/// <exception cref="Javax.Servlet.ServletException"/>
		/// <exception cref="System.IO.IOException"/>
		protected override void DoGet(HttpServletRequest req, HttpServletResponse resp)
		{
			Log.Info("GET");
			resp.SetContentType("text/html");
			PrintWriter @out = resp.GetWriter();
			Reconfigurable reconf = GetReconfigurable(req);
			string nodeName = reconf.GetType().GetCanonicalName();
			PrintHeader(@out, nodeName);
			PrintConf(@out, reconf);
			PrintFooter(@out);
		}

		/// <exception cref="Javax.Servlet.ServletException"/>
		/// <exception cref="System.IO.IOException"/>
		protected override void DoPost(HttpServletRequest req, HttpServletResponse resp)
		{
			Log.Info("POST");
			resp.SetContentType("text/html");
			PrintWriter @out = resp.GetWriter();
			Reconfigurable reconf = GetReconfigurable(req);
			string nodeName = reconf.GetType().GetCanonicalName();
			PrintHeader(@out, nodeName);
			try
			{
				ApplyChanges(@out, reconf, req);
			}
			catch (ReconfigurationException e)
			{
				resp.SendError(HttpServletResponse.ScInternalServerError, StringUtils.StringifyException
					(e));
				return;
			}
			@out.WriteLine("<p><a href=\"" + req.GetServletPath() + "\">back</a></p>");
			PrintFooter(@out);
		}
	}
}
