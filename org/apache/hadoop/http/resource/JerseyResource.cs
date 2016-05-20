using Sharpen;

namespace org.apache.hadoop.http.resource
{
	/// <summary>A simple Jersey resource class TestHttpServer.</summary>
	/// <remarks>
	/// A simple Jersey resource class TestHttpServer.
	/// The servlet simply puts the path and the op parameter in a map
	/// and return it in JSON format in the response.
	/// </remarks>
	public class JerseyResource
	{
		internal static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.http.resource.JerseyResource
			)));

		public const string PATH = "path";

		public const string OP = "op";

		/// <exception cref="System.IO.IOException"/>
		[javax.ws.rs.GET]
		public virtual javax.ws.rs.core.Response get(string path, string op)
		{
			LOG.info("get: " + PATH + "=" + path + ", " + OP + "=" + op);
			System.Collections.Generic.IDictionary<string, object> m = new System.Collections.Generic.SortedDictionary
				<string, object>();
			m[PATH] = path;
			m[OP] = op;
			string js = org.mortbay.util.ajax.JSON.toString(m);
			return javax.ws.rs.core.Response.ok(js).type(javax.ws.rs.core.MediaType.APPLICATION_JSON
				).build();
		}
	}
}
