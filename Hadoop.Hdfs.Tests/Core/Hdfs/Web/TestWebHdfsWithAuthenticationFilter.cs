using System.IO;
using System.Net;
using Javax.Servlet;
using Javax.Servlet.Http;
using NUnit.Framework;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Net;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Web
{
	public class TestWebHdfsWithAuthenticationFilter
	{
		private static bool authorized = false;

		public sealed class CustomizedFilter : Filter
		{
			/// <exception cref="Javax.Servlet.ServletException"/>
			public void Init(FilterConfig filterConfig)
			{
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="Javax.Servlet.ServletException"/>
			public void DoFilter(ServletRequest request, ServletResponse response, FilterChain
				 chain)
			{
				if (authorized)
				{
					chain.DoFilter(request, response);
				}
				else
				{
					((HttpServletResponse)response).SendError(HttpServletResponse.ScForbidden);
				}
			}

			public void Destroy()
			{
			}
		}

		private static Configuration conf;

		private static MiniDFSCluster cluster;

		private static FileSystem fs;

		/// <exception cref="System.IO.IOException"/>
		[BeforeClass]
		public static void SetUp()
		{
			conf = new Configuration();
			conf.Set(DFSConfigKeys.DfsWebhdfsAuthenticationFilterKey, typeof(TestWebHdfsWithAuthenticationFilter.CustomizedFilter
				).FullName);
			conf.Set(DFSConfigKeys.DfsNamenodeHttpAddressKey, "localhost:0");
			cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(1).Build();
			IPEndPoint addr = cluster.GetNameNode().GetHttpAddress();
			fs = FileSystem.Get(URI.Create("webhdfs://" + NetUtils.GetHostPortString(addr)), 
				conf);
			cluster.WaitActive();
		}

		/// <exception cref="System.IO.IOException"/>
		[AfterClass]
		public static void TearDown()
		{
			fs.Close();
			cluster.Shutdown();
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestWebHdfsAuthFilter()
		{
			// getFileStatus() is supposed to pass through with the default filter.
			authorized = false;
			try
			{
				fs.GetFileStatus(new Path("/"));
				NUnit.Framework.Assert.Fail("The filter fails to block the request");
			}
			catch (IOException)
			{
			}
			authorized = true;
			fs.GetFileStatus(new Path("/"));
		}
	}
}
