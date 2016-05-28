using Javax.Servlet;
using Javax.Servlet.Http;
using Org.Apache.Hadoop.FS.Http.Client;
using Sharpen;

namespace Org.Apache.Hadoop.FS.Http.Server
{
	public class TestCheckUploadContentTypeFilter
	{
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void PutUpload()
		{
			Test("PUT", HttpFSFileSystem.Operation.Create.ToString(), "application/octet-stream"
				, true, false);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void PostUpload()
		{
			Test("POST", HttpFSFileSystem.Operation.Append.ToString(), "APPLICATION/OCTET-STREAM"
				, true, false);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void PutUploadWrong()
		{
			Test("PUT", HttpFSFileSystem.Operation.Create.ToString(), "plain/text", false, false
				);
			Test("PUT", HttpFSFileSystem.Operation.Create.ToString(), "plain/text", true, true
				);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void PostUploadWrong()
		{
			Test("POST", HttpFSFileSystem.Operation.Append.ToString(), "plain/text", false, false
				);
			Test("POST", HttpFSFileSystem.Operation.Append.ToString(), "plain/text", true, true
				);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void GetOther()
		{
			Test("GET", HttpFSFileSystem.Operation.Gethomedirectory.ToString(), "plain/text", 
				false, false);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void PutOther()
		{
			Test("PUT", HttpFSFileSystem.Operation.Mkdirs.ToString(), "plain/text", false, false
				);
		}

		/// <exception cref="System.Exception"/>
		private void Test(string method, string operation, string contentType, bool upload
			, bool error)
		{
			HttpServletRequest request = Org.Mockito.Mockito.Mock<HttpServletRequest>();
			HttpServletResponse response = Org.Mockito.Mockito.Mock<HttpServletResponse>();
			Org.Mockito.Mockito.Reset(request);
			Org.Mockito.Mockito.When(request.GetMethod()).ThenReturn(method);
			Org.Mockito.Mockito.When(request.GetParameter(HttpFSFileSystem.OpParam)).ThenReturn
				(operation);
			Org.Mockito.Mockito.When(request.GetParameter(HttpFSParametersProvider.DataParam.
				Name)).ThenReturn(bool.ToString(upload));
			Org.Mockito.Mockito.When(request.GetContentType()).ThenReturn(contentType);
			FilterChain chain = Org.Mockito.Mockito.Mock<FilterChain>();
			Filter filter = new CheckUploadContentTypeFilter();
			filter.DoFilter(request, response, chain);
			if (error)
			{
				Org.Mockito.Mockito.Verify(response).SendError(Org.Mockito.Mockito.Eq(HttpServletResponse
					.ScBadRequest), Org.Mockito.Mockito.Contains("Data upload"));
			}
			else
			{
				Org.Mockito.Mockito.Verify(chain).DoFilter(request, response);
			}
		}
	}
}
