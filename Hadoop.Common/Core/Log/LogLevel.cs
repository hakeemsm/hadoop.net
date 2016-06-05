using System;
using System.IO;
using Com.Google.Common.Base;
using Javax.Servlet.Http;
using Org.Apache.Commons.Logging;
using Org.Apache.Commons.Logging.Impl;
using Org.Apache.Hadoop.Http;
using Org.Apache.Hadoop.Util;
using Org.Apache.Log4j;

using Logging;

namespace Org.Apache.Hadoop.Log
{
	/// <summary>Change log level in runtime.</summary>
	public class LogLevel
	{
		public const string Usages = "\nUsage: General options are:\n" + "\t[-getlevel <host:httpPort> <name>]\n"
			 + "\t[-setlevel <host:httpPort> <name> <level>]\n";

		/// <summary>A command line implementation</summary>
		public static void Main(string[] args)
		{
			if (args.Length == 3 && "-getlevel".Equals(args[0]))
			{
				Process("http://" + args[1] + "/logLevel?log=" + args[2]);
				return;
			}
			else
			{
				if (args.Length == 4 && "-setlevel".Equals(args[0]))
				{
					Process("http://" + args[1] + "/logLevel?log=" + args[2] + "&level=" + args[3]);
					return;
				}
			}
			System.Console.Error.WriteLine(Usages);
			System.Environment.Exit(-1);
		}

		private static void Process(string urlstring)
		{
			try
			{
				Uri url = new Uri(urlstring);
				System.Console.Out.WriteLine("Connecting to " + url);
				URLConnection connection = url.OpenConnection();
				connection.Connect();
				BufferedReader @in = new BufferedReader(new InputStreamReader(connection.GetInputStream
					(), Charsets.Utf8));
				for (string line; (line = @in.ReadLine()) != null; )
				{
					if (line.StartsWith(Marker))
					{
						System.Console.Out.WriteLine(Tag.Matcher(line).ReplaceAll(string.Empty));
					}
				}
				@in.Close();
			}
			catch (IOException ioe)
			{
				System.Console.Error.WriteLine(string.Empty + ioe);
			}
		}

		internal const string Marker = "<!-- OUTPUT -->";

		internal static readonly Pattern Tag = Pattern.Compile("<[^>]*>");

		/// <summary>A servlet implementation</summary>
		[System.Serializable]
		public class Servlet : HttpServlet
		{
			private const long serialVersionUID = 1L;

			/// <exception cref="Javax.Servlet.ServletException"/>
			/// <exception cref="System.IO.IOException"/>
			protected override void DoGet(HttpServletRequest request, HttpServletResponse response
				)
			{
				// Do the authorization
				if (!HttpServer2.HasAdministratorAccess(GetServletContext(), request, response))
				{
					return;
				}
				PrintWriter @out = ServletUtil.InitHTML(response, "Log Level");
				string logName = ServletUtil.GetParameter(request, "log");
				string level = ServletUtil.GetParameter(request, "level");
				if (logName != null)
				{
					@out.WriteLine("<br /><hr /><h3>Results</h3>");
					@out.WriteLine(Marker + "Submitted Log Name: <b>" + logName + "</b><br />");
					Org.Apache.Commons.Logging.Log log = LogFactory.GetLog(logName);
					@out.WriteLine(Marker + "Log Class: <b>" + log.GetType().FullName + "</b><br />");
					if (level != null)
					{
						@out.WriteLine(Marker + "Submitted Level: <b>" + level + "</b><br />");
					}
					if (log is Log4JLogger)
					{
						Process(((Log4JLogger)log).GetLogger(), level, @out);
					}
					else
					{
						if (log is Jdk14Logger)
						{
							Process(((Jdk14Logger)log).GetLogger(), level, @out);
						}
						else
						{
							@out.WriteLine("Sorry, " + log.GetType() + " not supported.<br />");
						}
					}
				}
				@out.WriteLine(Forms);
				@out.WriteLine(ServletUtil.HtmlTail);
			}

			internal const string Forms = "\n<br /><hr /><h3>Get / Set</h3>" + "\n<form>Log: <input type='text' size='50' name='log' /> "
				 + "<input type='submit' value='Get Log Level' />" + "</form>" + "\n<form>Log: <input type='text' size='50' name='log' /> "
				 + "Level: <input type='text' name='level' /> " + "<input type='submit' value='Set Log Level' />"
				 + "</form>";

			/// <exception cref="System.IO.IOException"/>
			private static void Process(Logger log, string level, PrintWriter @out)
			{
				if (level != null)
				{
					if (!level.Equals(Level.ToLevel(level).ToString()))
					{
						@out.WriteLine(Marker + "Bad level : <b>" + level + "</b><br />");
					}
					else
					{
						log.SetLevel(Level.ToLevel(level));
						@out.WriteLine(Marker + "Setting Level to " + level + " ...<br />");
					}
				}
				@out.WriteLine(Marker + "Effective level: <b>" + log.GetEffectiveLevel() + "</b><br />"
					);
			}

			/// <exception cref="System.IO.IOException"/>
			private static void Process(Logger log, string level, PrintWriter @out)
			{
				if (level != null)
				{
					log.SetLevel(Level.Parse(level));
					@out.WriteLine(Marker + "Setting Level to " + level + " ...<br />");
				}
				Level lev;
				for (; (lev = log.GetLevel()) == null; log = log.GetParent())
				{
				}
				@out.WriteLine(Marker + "Effective level: <b>" + lev + "</b><br />");
			}
		}
	}
}
