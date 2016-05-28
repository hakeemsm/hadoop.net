using System.Collections.Generic;
using Javax.Servlet;
using Javax.Servlet.Http;
using Org.Apache.Hadoop.FS.Http.Client;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.FS.Http.Server
{
	/// <summary>
	/// Filter that Enforces the content-type to be application/octet-stream for
	/// POST and PUT requests.
	/// </summary>
	public class CheckUploadContentTypeFilter : Filter
	{
		private static readonly ICollection<string> UploadOperations = new HashSet<string
			>();

		static CheckUploadContentTypeFilter()
		{
			UploadOperations.AddItem(HttpFSFileSystem.Operation.Append.ToString());
			UploadOperations.AddItem(HttpFSFileSystem.Operation.Create.ToString());
		}

		/// <summary>Initializes the filter.</summary>
		/// <remarks>
		/// Initializes the filter.
		/// <p>
		/// This implementation is a NOP.
		/// </remarks>
		/// <param name="config">filter configuration.</param>
		/// <exception cref="Javax.Servlet.ServletException">thrown if the filter could not be initialized.
		/// 	</exception>
		public virtual void Init(FilterConfig config)
		{
		}

		/// <summary>
		/// Enforces the content-type to be application/octet-stream for
		/// POST and PUT requests.
		/// </summary>
		/// <param name="request">servlet request.</param>
		/// <param name="response">servlet response.</param>
		/// <param name="chain">filter chain.</param>
		/// <exception cref="System.IO.IOException">thrown if an IO error occurrs.</exception>
		/// <exception cref="Javax.Servlet.ServletException">thrown if a servet error occurrs.
		/// 	</exception>
		public virtual void DoFilter(ServletRequest request, ServletResponse response, FilterChain
			 chain)
		{
			bool contentTypeOK = true;
			HttpServletRequest httpReq = (HttpServletRequest)request;
			HttpServletResponse httpRes = (HttpServletResponse)response;
			string method = httpReq.GetMethod();
			if (method.Equals("PUT") || method.Equals("POST"))
			{
				string op = httpReq.GetParameter(HttpFSFileSystem.OpParam);
				if (op != null && UploadOperations.Contains(StringUtils.ToUpperCase(op)))
				{
					if (Sharpen.Runtime.EqualsIgnoreCase("true", httpReq.GetParameter(HttpFSParametersProvider.DataParam
						.Name)))
					{
						string contentType = httpReq.GetContentType();
						contentTypeOK = Sharpen.Runtime.EqualsIgnoreCase(HttpFSFileSystem.UploadContentType
							, contentType);
					}
				}
			}
			if (contentTypeOK)
			{
				chain.DoFilter(httpReq, httpRes);
			}
			else
			{
				httpRes.SendError(HttpServletResponse.ScBadRequest, "Data upload requests must have content-type set to '"
					 + HttpFSFileSystem.UploadContentType + "'");
			}
		}

		/// <summary>Destroys the filter.</summary>
		/// <remarks>
		/// Destroys the filter.
		/// <p>
		/// This implementation is a NOP.
		/// </remarks>
		public virtual void Destroy()
		{
		}
	}
}
