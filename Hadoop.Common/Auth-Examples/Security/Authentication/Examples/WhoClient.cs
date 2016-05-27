using System;
using System.IO;
using System.Text;
using Org.Apache.Hadoop.Security.Authentication.Client;
using Sharpen;

namespace Org.Apache.Hadoop.Security.Authentication.Examples
{
	/// <summary>Example that uses <code>AuthenticatedURL</code>.</summary>
	public class WhoClient
	{
		public static void Main(string[] args)
		{
			try
			{
				if (args.Length != 1)
				{
					System.Console.Error.WriteLine("Usage: <URL>");
					System.Environment.Exit(-1);
				}
				AuthenticatedURL.Token token = new AuthenticatedURL.Token();
				Uri url = new Uri(args[0]);
				HttpURLConnection conn = new AuthenticatedURL().OpenConnection(url, token);
				System.Console.Out.WriteLine();
				System.Console.Out.WriteLine("Token value: " + token);
				System.Console.Out.WriteLine("Status code: " + conn.GetResponseCode() + " " + conn
					.GetResponseMessage());
				System.Console.Out.WriteLine();
				if (conn.GetResponseCode() == HttpURLConnection.HttpOk)
				{
					BufferedReader reader = new BufferedReader(new InputStreamReader(conn.GetInputStream
						(), Sharpen.Extensions.GetEncoding("UTF-8")));
					string line = reader.ReadLine();
					while (line != null)
					{
						System.Console.Out.WriteLine(line);
						line = reader.ReadLine();
					}
					reader.Close();
				}
				System.Console.Out.WriteLine();
			}
			catch (Exception ex)
			{
				System.Console.Error.WriteLine("ERROR: " + ex.Message);
				System.Environment.Exit(-1);
			}
		}
	}
}
