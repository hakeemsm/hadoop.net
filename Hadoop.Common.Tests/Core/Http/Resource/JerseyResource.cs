using System.Collections.Generic;
using Javax.WS.RS;
using Javax.WS.RS.Core;
using Org.Apache.Commons.Logging;
using Org.Mortbay.Util.Ajax;


namespace Org.Apache.Hadoop.Http.Resource
{
	/// <summary>A simple Jersey resource class TestHttpServer.</summary>
	/// <remarks>
	/// A simple Jersey resource class TestHttpServer.
	/// The servlet simply puts the path and the op parameter in a map
	/// and return it in JSON format in the response.
	/// </remarks>
	public class JerseyResource
	{
		internal static readonly Log Log = LogFactory.GetLog(typeof(JerseyResource));

		public const string Path = "path";

		public const string Op = "op";

		/// <exception cref="System.IO.IOException"/>
		[GET]
		public virtual Response Get(string path, string op)
		{
			Log.Info("get: " + Path + "=" + path + ", " + Op + "=" + op);
			IDictionary<string, object> m = new SortedDictionary<string, object>();
			m[Path] = path;
			m[Op] = op;
			string js = JSON.ToString(m);
			return Response.Ok(js).Type(MediaType.ApplicationJson).Build();
		}
	}
}
