using System.Collections.Generic;
using Com.Sun.Jersey.Spi.Container;
using Javax.WS.RS.Core;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Web
{
	/// <summary>
	/// A filter to change parameter names to lower cases
	/// so that parameter names are considered as case insensitive.
	/// </summary>
	public class ParamFilter : ResourceFilter
	{
		private sealed class _ContainerRequestFilter_39 : ContainerRequestFilter
		{
			public _ContainerRequestFilter_39()
			{
			}

			public ContainerRequest Filter(ContainerRequest request)
			{
				MultivaluedMap<string, string> parameters = request.GetQueryParameters();
				if (ParamFilter.ContainsUpperCase(parameters.Keys))
				{
					//rebuild URI
					URI lower = ParamFilter.RebuildQuery(request.GetRequestUri(), parameters);
					request.SetUris(request.GetBaseUri(), lower);
				}
				return request;
			}
		}

		private static readonly ContainerRequestFilter LowerCase = new _ContainerRequestFilter_39
			();

		public virtual ContainerRequestFilter GetRequestFilter()
		{
			return LowerCase;
		}

		public virtual ContainerResponseFilter GetResponseFilter()
		{
			return null;
		}

		/// <summary>Do the strings contain upper case letters?</summary>
		internal static bool ContainsUpperCase(IEnumerable<string> strings)
		{
			foreach (string s in strings)
			{
				for (int i = 0; i < s.Length; i++)
				{
					if (System.Char.IsUpper(s[i]))
					{
						return true;
					}
				}
			}
			return false;
		}

		/// <summary>Rebuild the URI query with lower case parameter names.</summary>
		private static URI RebuildQuery(URI uri, MultivaluedMap<string, string> parameters
			)
		{
			UriBuilder b = UriBuilder.FromUri(uri).ReplaceQuery(string.Empty);
			foreach (KeyValuePair<string, IList<string>> e in parameters)
			{
				string key = StringUtils.ToLowerCase(e.Key);
				foreach (string v in e.Value)
				{
					b = b.QueryParam(key, v);
				}
			}
			return b.Build();
		}
	}
}
