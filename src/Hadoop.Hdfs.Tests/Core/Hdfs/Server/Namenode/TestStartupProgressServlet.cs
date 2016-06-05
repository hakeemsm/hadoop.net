using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Collect;
using Javax.Servlet;
using Javax.Servlet.Http;
using Org.Apache.Hadoop.Hdfs.Server.Namenode.Startupprogress;
using Org.Mortbay.Util.Ajax;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	public class TestStartupProgressServlet
	{
		private HttpServletRequest req;

		private HttpServletResponse resp;

		private ByteArrayOutputStream respOut;

		private StartupProgress startupProgress;

		private StartupProgressServlet servlet;

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.SetUp]
		public virtual void SetUp()
		{
			startupProgress = new StartupProgress();
			ServletContext context = Org.Mockito.Mockito.Mock<ServletContext>();
			Org.Mockito.Mockito.When(context.GetAttribute(NameNodeHttpServer.StartupProgressAttributeKey
				)).ThenReturn(startupProgress);
			servlet = Org.Mockito.Mockito.Mock<StartupProgressServlet>();
			Org.Mockito.Mockito.When(servlet.GetServletContext()).ThenReturn(context);
			Org.Mockito.Mockito.DoCallRealMethod().When(servlet).DoGet(Any<HttpServletRequest
				>(), Any<HttpServletResponse>());
			req = Org.Mockito.Mockito.Mock<HttpServletRequest>();
			respOut = new ByteArrayOutputStream();
			PrintWriter writer = new PrintWriter(respOut);
			resp = Org.Mockito.Mockito.Mock<HttpServletResponse>();
			Org.Mockito.Mockito.When(resp.GetWriter()).ThenReturn(writer);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestInitialState()
		{
			string respBody = DoGetAndReturnResponseBody();
			NUnit.Framework.Assert.IsNotNull(respBody);
			IDictionary<string, object> expected = ImmutableMap.Builder<string, object>().Put
				("percentComplete", 0.0f).Put("phases", Arrays.AsList<object>(ImmutableMap.Builder
				<string, object>().Put("name", "LoadingFsImage").Put("desc", "Loading fsimage").
				Put("status", "PENDING").Put("percentComplete", 0.0f).Put("steps", Collections.EmptyList
				()).Build(), ImmutableMap.Builder<string, object>().Put("name", "LoadingEdits").
				Put("desc", "Loading edits").Put("status", "PENDING").Put("percentComplete", 0.0f
				).Put("steps", Collections.EmptyList()).Build(), ImmutableMap.Builder<string, object
				>().Put("name", "SavingCheckpoint").Put("desc", "Saving checkpoint").Put("status"
				, "PENDING").Put("percentComplete", 0.0f).Put("steps", Collections.EmptyList()).
				Build(), ImmutableMap.Builder<string, object>().Put("name", "SafeMode").Put("desc"
				, "Safe mode").Put("status", "PENDING").Put("percentComplete", 0.0f).Put("steps"
				, Collections.EmptyList()).Build())).Build();
			NUnit.Framework.Assert.AreEqual(JSON.ToString(expected), FilterJson(respBody));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestRunningState()
		{
			StartupProgressTestHelper.SetStartupProgressForRunningState(startupProgress);
			string respBody = DoGetAndReturnResponseBody();
			NUnit.Framework.Assert.IsNotNull(respBody);
			IDictionary<string, object> expected = ImmutableMap.Builder<string, object>().Put
				("percentComplete", 0.375f).Put("phases", Arrays.AsList<object>(ImmutableMap.Builder
				<string, object>().Put("name", "LoadingFsImage").Put("desc", "Loading fsimage").
				Put("status", "COMPLETE").Put("percentComplete", 1.0f).Put("steps", Sharpen.Collections
				.SingletonList<object>(ImmutableMap.Builder<string, object>().Put("name", "Inodes"
				).Put("desc", "inodes").Put("count", 100L).Put("total", 100L).Put("percentComplete"
				, 1.0f).Build())).Build(), ImmutableMap.Builder<string, object>().Put("name", "LoadingEdits"
				).Put("desc", "Loading edits").Put("status", "RUNNING").Put("percentComplete", 0.5f
				).Put("steps", Sharpen.Collections.SingletonList<object>(ImmutableMap.Builder<string
				, object>().Put("count", 100L).Put("file", "file").Put("size", 1000L).Put("total"
				, 200L).Put("percentComplete", 0.5f).Build())).Build(), ImmutableMap.Builder<string
				, object>().Put("name", "SavingCheckpoint").Put("desc", "Saving checkpoint").Put
				("status", "PENDING").Put("percentComplete", 0.0f).Put("steps", Sharpen.Collections
				.EmptyList()).Build(), ImmutableMap.Builder<string, object>().Put("name", "SafeMode"
				).Put("desc", "Safe mode").Put("status", "PENDING").Put("percentComplete", 0.0f)
				.Put("steps", Sharpen.Collections.EmptyList()).Build())).Build();
			NUnit.Framework.Assert.AreEqual(JSON.ToString(expected), FilterJson(respBody));
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestFinalState()
		{
			StartupProgressTestHelper.SetStartupProgressForFinalState(startupProgress);
			string respBody = DoGetAndReturnResponseBody();
			NUnit.Framework.Assert.IsNotNull(respBody);
			IDictionary<string, object> expected = ImmutableMap.Builder<string, object>().Put
				("percentComplete", 1.0f).Put("phases", Arrays.AsList<object>(ImmutableMap.Builder
				<string, object>().Put("name", "LoadingFsImage").Put("desc", "Loading fsimage").
				Put("status", "COMPLETE").Put("percentComplete", 1.0f).Put("steps", Sharpen.Collections
				.SingletonList<object>(ImmutableMap.Builder<string, object>().Put("name", "Inodes"
				).Put("desc", "inodes").Put("count", 100L).Put("total", 100L).Put("percentComplete"
				, 1.0f).Build())).Build(), ImmutableMap.Builder<string, object>().Put("name", "LoadingEdits"
				).Put("desc", "Loading edits").Put("status", "COMPLETE").Put("percentComplete", 
				1.0f).Put("steps", Sharpen.Collections.SingletonList<object>(ImmutableMap.Builder
				<string, object>().Put("count", 200L).Put("file", "file").Put("size", 1000L).Put
				("total", 200L).Put("percentComplete", 1.0f).Build())).Build(), ImmutableMap.Builder
				<string, object>().Put("name", "SavingCheckpoint").Put("desc", "Saving checkpoint"
				).Put("status", "COMPLETE").Put("percentComplete", 1.0f).Put("steps", Sharpen.Collections
				.SingletonList<object>(ImmutableMap.Builder<string, object>().Put("name", "Inodes"
				).Put("desc", "inodes").Put("count", 300L).Put("total", 300L).Put("percentComplete"
				, 1.0f).Build())).Build(), ImmutableMap.Builder<string, object>().Put("name", "SafeMode"
				).Put("desc", "Safe mode").Put("status", "COMPLETE").Put("percentComplete", 1.0f
				).Put("steps", Sharpen.Collections.SingletonList<object>(ImmutableMap.Builder<string
				, object>().Put("name", "AwaitingReportedBlocks").Put("desc", "awaiting reported blocks"
				).Put("count", 400L).Put("total", 400L).Put("percentComplete", 1.0f).Build())).Build
				())).Build();
			NUnit.Framework.Assert.AreEqual(JSON.ToString(expected), FilterJson(respBody));
		}

		/// <summary>
		/// Calls doGet on the servlet, captures the response body as a string, and
		/// returns it to the caller.
		/// </summary>
		/// <returns>String response body</returns>
		/// <exception cref="System.IO.IOException">thrown if there is an I/O error</exception>
		private string DoGetAndReturnResponseBody()
		{
			servlet.DoGet(req, resp);
			return Sharpen.Runtime.GetStringForBytes(respOut.ToByteArray(), "UTF-8");
		}

		/// <summary>
		/// Filters the given JSON response body, removing elements that would impede
		/// testing.
		/// </summary>
		/// <remarks>
		/// Filters the given JSON response body, removing elements that would impede
		/// testing.  Specifically, it removes elapsedTime fields, because we cannot
		/// predict the exact values.
		/// </remarks>
		/// <param name="str">String to filter</param>
		/// <returns>String filtered value</returns>
		private string FilterJson(string str)
		{
			return str.ReplaceAll("\"elapsedTime\":\\d+\\,", string.Empty).ReplaceAll("\\,\"elapsedTime\":\\d+"
				, string.Empty);
		}
	}
}
