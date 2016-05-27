using System.Text;
using Javax.Servlet;
using Javax.Servlet.Http;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Security.Token.Delegation.Web;
using Sharpen;

namespace Org.Apache.Hadoop.Crypto.Key.Kms.Server
{
	/// <summary>
	/// Servlet filter that captures context of the HTTP request to be use in the
	/// scope of KMS calls on the server side.
	/// </summary>
	public class KMSMDCFilter : Filter
	{
		private class Data
		{
			private UserGroupInformation ugi;

			private string method;

			private StringBuilder url;

			private Data(UserGroupInformation ugi, string method, StringBuilder url)
			{
				this.ugi = ugi;
				this.method = method;
				this.url = url;
			}
		}

		private static ThreadLocal<KMSMDCFilter.Data> DataTl = new ThreadLocal<KMSMDCFilter.Data
			>();

		public static UserGroupInformation GetUgi()
		{
			return DataTl.Get().ugi;
		}

		public static string GetMethod()
		{
			return DataTl.Get().method;
		}

		public static string GetURL()
		{
			return DataTl.Get().url.ToString();
		}

		/// <exception cref="Javax.Servlet.ServletException"/>
		public virtual void Init(FilterConfig config)
		{
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Javax.Servlet.ServletException"/>
		public virtual void DoFilter(ServletRequest request, ServletResponse response, FilterChain
			 chain)
		{
			try
			{
				DataTl.Remove();
				UserGroupInformation ugi = HttpUserGroupInformation.Get();
				string method = ((HttpServletRequest)request).GetMethod();
				StringBuilder requestURL = ((HttpServletRequest)request).GetRequestURL();
				string queryString = ((HttpServletRequest)request).GetQueryString();
				if (queryString != null)
				{
					requestURL.Append("?").Append(queryString);
				}
				DataTl.Set(new KMSMDCFilter.Data(ugi, method, requestURL));
				chain.DoFilter(request, response);
			}
			finally
			{
				DataTl.Remove();
			}
		}

		public virtual void Destroy()
		{
		}
	}
}
