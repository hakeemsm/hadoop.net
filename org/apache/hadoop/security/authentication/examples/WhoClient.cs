using Sharpen;

namespace org.apache.hadoop.security.authentication.examples
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
				org.apache.hadoop.security.authentication.client.AuthenticatedURL.Token token = new 
					org.apache.hadoop.security.authentication.client.AuthenticatedURL.Token();
				java.net.URL url = new java.net.URL(args[0]);
				java.net.HttpURLConnection conn = new org.apache.hadoop.security.authentication.client.AuthenticatedURL
					().openConnection(url, token);
				System.Console.Out.WriteLine();
				System.Console.Out.WriteLine("Token value: " + token);
				System.Console.Out.WriteLine("Status code: " + conn.getResponseCode() + " " + conn
					.getResponseMessage());
				System.Console.Out.WriteLine();
				if (conn.getResponseCode() == java.net.HttpURLConnection.HTTP_OK)
				{
					java.io.BufferedReader reader = new java.io.BufferedReader(new java.io.InputStreamReader
						(conn.getInputStream(), java.nio.charset.Charset.forName("UTF-8")));
					string line = reader.readLine();
					while (line != null)
					{
						System.Console.Out.WriteLine(line);
						line = reader.readLine();
					}
					reader.close();
				}
				System.Console.Out.WriteLine();
			}
			catch (System.Exception ex)
			{
				System.Console.Error.WriteLine("ERROR: " + ex.Message);
				System.Environment.Exit(-1);
			}
		}
	}
}
