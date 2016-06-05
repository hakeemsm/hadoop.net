using System.Collections.Generic;
using Com.Google.Common.Base;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Webapp.View
{
	public class DefaultPage : TextPage
	{
		internal static readonly Joiner valJoiner = Joiner.On(", ");

		public override void Render()
		{
			Puts("Request URI: ", Request().GetRequestURI());
			Puts("Query parameters:");
			IDictionary<string, string[]> @params = Request().GetParameterMap();
			foreach (KeyValuePair<string, string[]> e in @params)
			{
				Puts("  ", e.Key, "=", valJoiner.Join(e.Value));
			}
			Puts("More parameters:");
			foreach (KeyValuePair<string, string> e_1 in MoreParams())
			{
				Puts("  ", e_1.Key, "=", e_1.Value);
			}
			Puts("Path info: ", Request().GetPathInfo());
			Puts("Path translated: ", Request().GetPathTranslated());
			Puts("Auth type: ", Request().GetAuthType());
			Puts("Remote address: " + Request().GetRemoteAddr());
			Puts("Remote user: ", Request().GetRemoteUser());
			Puts("Servlet attributes:");
			Enumeration<string> attrNames = Request().GetAttributeNames();
			while (attrNames.MoveNext())
			{
				string key = attrNames.Current;
				Puts("  ", key, "=", Request().GetAttribute(key));
			}
			Puts("Headers:");
			Enumeration<string> headerNames = Request().GetHeaderNames();
			while (headerNames.MoveNext())
			{
				string key = headerNames.Current;
				Puts("  ", key, "=", Request().GetHeader(key));
			}
		}
	}
}
