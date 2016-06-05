using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Org.Apache.Commons.IO;
using Org.Apache.Hadoop.FS;
using Org.Json.Simple.Parser;
using Sharpen;

namespace Org.Apache.Hadoop.FS.Http.Client
{
	/// <summary>Utility methods used by HttpFS classes.</summary>
	public class HttpFSUtils
	{
		public const string ServiceName = "/webhdfs";

		public const string ServiceVersion = "/v1";

		private const string ServicePath = ServiceName + ServiceVersion;

		/// <summary>
		/// Convenience method that creates an HTTP <code>URL</code> for the
		/// HttpFSServer file system operations.
		/// </summary>
		/// <remarks>
		/// Convenience method that creates an HTTP <code>URL</code> for the
		/// HttpFSServer file system operations.
		/// <p/>
		/// </remarks>
		/// <param name="path">the file path.</param>
		/// <param name="params">the query string parameters.</param>
		/// <returns>a <code>URL</code> for the HttpFSServer server,</returns>
		/// <exception cref="System.IO.IOException">thrown if an IO error occurs.</exception>
		internal static Uri CreateURL(Path path, IDictionary<string, string> @params)
		{
			return CreateURL(path, @params, null);
		}

		/// <summary>
		/// Convenience method that creates an HTTP <code>URL</code> for the
		/// HttpFSServer file system operations.
		/// </summary>
		/// <remarks>
		/// Convenience method that creates an HTTP <code>URL</code> for the
		/// HttpFSServer file system operations.
		/// <p/>
		/// </remarks>
		/// <param name="path">the file path.</param>
		/// <param name="params">the query string parameters.</param>
		/// <param name="multiValuedParams">multi valued parameters of the query string</param>
		/// <returns>URL a <code>URL</code> for the HttpFSServer server,</returns>
		/// <exception cref="System.IO.IOException">thrown if an IO error occurs.</exception>
		internal static Uri CreateURL(Path path, IDictionary<string, string> @params, IDictionary
			<string, IList<string>> multiValuedParams)
		{
			URI uri = path.ToUri();
			string realScheme;
			if (Sharpen.Runtime.EqualsIgnoreCase(uri.GetScheme(), HttpFSFileSystem.Scheme))
			{
				realScheme = "http";
			}
			else
			{
				if (Sharpen.Runtime.EqualsIgnoreCase(uri.GetScheme(), HttpsFSFileSystem.Scheme))
				{
					realScheme = "https";
				}
				else
				{
					throw new ArgumentException(MessageFormat.Format("Invalid scheme [{0}] it should be '"
						 + HttpFSFileSystem.Scheme + "' " + "or '" + HttpsFSFileSystem.Scheme + "'", uri
						));
				}
			}
			StringBuilder sb = new StringBuilder();
			sb.Append(realScheme).Append("://").Append(uri.GetAuthority()).Append(ServicePath
				).Append(uri.GetPath());
			string separator = "?";
			foreach (KeyValuePair<string, string> entry in @params)
			{
				sb.Append(separator).Append(entry.Key).Append("=").Append(URLEncoder.Encode(entry
					.Value, "UTF8"));
				separator = "&";
			}
			if (multiValuedParams != null)
			{
				foreach (KeyValuePair<string, IList<string>> multiValuedEntry in multiValuedParams)
				{
					string name = URLEncoder.Encode(multiValuedEntry.Key, "UTF8");
					IList<string> values = multiValuedEntry.Value;
					foreach (string value in values)
					{
						sb.Append(separator).Append(name).Append("=").Append(URLEncoder.Encode(value, "UTF8"
							));
						separator = "&";
					}
				}
			}
			return new Uri(sb.ToString());
		}

		/// <summary>
		/// Convenience method that JSON Parses the <code>InputStream</code> of a
		/// <code>HttpURLConnection</code>.
		/// </summary>
		/// <param name="conn">the <code>HttpURLConnection</code>.</param>
		/// <returns>the parsed JSON object.</returns>
		/// <exception cref="System.IO.IOException">
		/// thrown if the <code>InputStream</code> could not be
		/// JSON parsed.
		/// </exception>
		internal static object JsonParse(HttpURLConnection conn)
		{
			try
			{
				JSONParser parser = new JSONParser();
				return parser.Parse(new InputStreamReader(conn.GetInputStream(), Charsets.Utf8));
			}
			catch (ParseException ex)
			{
				throw new IOException("JSON parser error, " + ex.Message, ex);
			}
		}
	}
}
