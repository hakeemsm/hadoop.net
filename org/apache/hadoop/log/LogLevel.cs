using Sharpen;

namespace org.apache.hadoop.log
{
	/// <summary>Change log level in runtime.</summary>
	public class LogLevel
	{
		public const string USAGES = "\nUsage: General options are:\n" + "\t[-getlevel <host:httpPort> <name>]\n"
			 + "\t[-setlevel <host:httpPort> <name> <level>]\n";

		/// <summary>A command line implementation</summary>
		public static void Main(string[] args)
		{
			if (args.Length == 3 && "-getlevel".Equals(args[0]))
			{
				process("http://" + args[1] + "/logLevel?log=" + args[2]);
				return;
			}
			else
			{
				if (args.Length == 4 && "-setlevel".Equals(args[0]))
				{
					process("http://" + args[1] + "/logLevel?log=" + args[2] + "&level=" + args[3]);
					return;
				}
			}
			System.Console.Error.WriteLine(USAGES);
			System.Environment.Exit(-1);
		}

		private static void process(string urlstring)
		{
			try
			{
				java.net.URL url = new java.net.URL(urlstring);
				System.Console.Out.WriteLine("Connecting to " + url);
				java.net.URLConnection connection = url.openConnection();
				connection.connect();
				java.io.BufferedReader @in = new java.io.BufferedReader(new java.io.InputStreamReader
					(connection.getInputStream(), com.google.common.@base.Charsets.UTF_8));
				for (string line; (line = @in.readLine()) != null; )
				{
					if (line.StartsWith(MARKER))
					{
						System.Console.Out.WriteLine(TAG.matcher(line).replaceAll(string.Empty));
					}
				}
				@in.close();
			}
			catch (System.IO.IOException ioe)
			{
				System.Console.Error.WriteLine(string.Empty + ioe);
			}
		}

		internal const string MARKER = "<!-- OUTPUT -->";

		internal static readonly java.util.regex.Pattern TAG = java.util.regex.Pattern.compile
			("<[^>]*>");

		/// <summary>A servlet implementation</summary>
		[System.Serializable]
		public class Servlet : javax.servlet.http.HttpServlet
		{
			private const long serialVersionUID = 1L;

			/// <exception cref="javax.servlet.ServletException"/>
			/// <exception cref="System.IO.IOException"/>
			protected override void doGet(javax.servlet.http.HttpServletRequest request, javax.servlet.http.HttpServletResponse
				 response)
			{
				// Do the authorization
				if (!org.apache.hadoop.http.HttpServer2.hasAdministratorAccess(getServletContext(
					), request, response))
				{
					return;
				}
				java.io.PrintWriter @out = org.apache.hadoop.util.ServletUtil.initHTML(response, 
					"Log Level");
				string logName = org.apache.hadoop.util.ServletUtil.getParameter(request, "log");
				string level = org.apache.hadoop.util.ServletUtil.getParameter(request, "level");
				if (logName != null)
				{
					@out.println("<br /><hr /><h3>Results</h3>");
					@out.println(MARKER + "Submitted Log Name: <b>" + logName + "</b><br />");
					org.apache.commons.logging.Log log = org.apache.commons.logging.LogFactory.getLog
						(logName);
					@out.println(MARKER + "Log Class: <b>" + Sharpen.Runtime.getClassForObject(log).getName
						() + "</b><br />");
					if (level != null)
					{
						@out.println(MARKER + "Submitted Level: <b>" + level + "</b><br />");
					}
					if (log is org.apache.commons.logging.impl.Log4JLogger)
					{
						process(((org.apache.commons.logging.impl.Log4JLogger)log).getLogger(), level, @out
							);
					}
					else
					{
						if (log is org.apache.commons.logging.impl.Jdk14Logger)
						{
							process(((org.apache.commons.logging.impl.Jdk14Logger)log).getLogger(), level, @out
								);
						}
						else
						{
							@out.println("Sorry, " + Sharpen.Runtime.getClassForObject(log) + " not supported.<br />"
								);
						}
					}
				}
				@out.println(FORMS);
				@out.println(org.apache.hadoop.util.ServletUtil.HTML_TAIL);
			}

			internal const string FORMS = "\n<br /><hr /><h3>Get / Set</h3>" + "\n<form>Log: <input type='text' size='50' name='log' /> "
				 + "<input type='submit' value='Get Log Level' />" + "</form>" + "\n<form>Log: <input type='text' size='50' name='log' /> "
				 + "Level: <input type='text' name='level' /> " + "<input type='submit' value='Set Log Level' />"
				 + "</form>";

			/// <exception cref="System.IO.IOException"/>
			private static void process(org.apache.log4j.Logger log, string level, java.io.PrintWriter
				 @out)
			{
				if (level != null)
				{
					if (!level.Equals(org.apache.log4j.Level.toLevel(level).ToString()))
					{
						@out.println(MARKER + "Bad level : <b>" + level + "</b><br />");
					}
					else
					{
						log.setLevel(org.apache.log4j.Level.toLevel(level));
						@out.println(MARKER + "Setting Level to " + level + " ...<br />");
					}
				}
				@out.println(MARKER + "Effective level: <b>" + log.getEffectiveLevel() + "</b><br />"
					);
			}

			/// <exception cref="System.IO.IOException"/>
			private static void process(java.util.logging.Logger log, string level, java.io.PrintWriter
				 @out)
			{
				if (level != null)
				{
					log.setLevel(java.util.logging.Level.parse(level));
					@out.println(MARKER + "Setting Level to " + level + " ...<br />");
				}
				java.util.logging.Level lev;
				for (; (lev = log.getLevel()) == null; log = log.getParent())
				{
				}
				@out.println(MARKER + "Effective level: <b>" + lev + "</b><br />");
			}
		}
	}
}
