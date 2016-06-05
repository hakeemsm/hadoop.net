using System.IO;
using NUnit.Framework;
using Org.Apache.Hadoop.Yarn.Webapp;
using Org.Apache.Hadoop.Yarn.Webapp.Test;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Webapp.View
{
	public class TestInfoBlock
	{
		public static StringWriter sw;

		public static PrintWriter pw;

		internal const string Javascript = "<script>alert('text')</script>";

		internal const string JavascriptEscaped = "&lt;script&gt;alert('text')&lt;/script&gt;";

		public class JavaScriptInfoBlock : InfoBlock
		{
			internal static ResponseInfo resInfo;

			static JavaScriptInfoBlock()
			{
				resInfo = new ResponseInfo();
				resInfo.("User_Name", Javascript);
			}

			public override PrintWriter Writer()
			{
				return TestInfoBlock.pw;
			}

			internal JavaScriptInfoBlock(ResponseInfo info)
				: base(resInfo)
			{
			}

			public JavaScriptInfoBlock()
				: base(resInfo)
			{
			}
		}

		public class MultilineInfoBlock : InfoBlock
		{
			internal static ResponseInfo resInfo;

			static MultilineInfoBlock()
			{
				resInfo = new ResponseInfo();
				resInfo.("Multiple_line_value", "This is one line.");
				resInfo.("Multiple_line_value", "This is first line.\nThis is second line.");
			}

			public override PrintWriter Writer()
			{
				return TestInfoBlock.pw;
			}

			internal MultilineInfoBlock(ResponseInfo info)
				: base(resInfo)
			{
			}

			public MultilineInfoBlock()
				: base(resInfo)
			{
			}
		}

		[SetUp]
		public virtual void Setup()
		{
			sw = new StringWriter();
			pw = new PrintWriter(sw);
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestMultilineInfoBlock()
		{
			WebAppTests.TestBlock(typeof(TestInfoBlock.MultilineInfoBlock));
			TestInfoBlock.pw.Flush();
			string output = TestInfoBlock.sw.ToString().ReplaceAll(" +", " ");
			string expectedMultilineData1 = string.Format("<tr class=\"odd\">%n" + " <th>%n Multiple_line_value%n </th>%n"
				 + " <td>%n This is one line.%n </td>%n");
			string expectedMultilineData2 = string.Format("<tr class=\"even\">%n" + " <th>%n Multiple_line_value%n </th>%n <td>%n <div>%n"
				 + " This is first line.%n </div>%n <div>%n" + " This is second line.%n </div>%n"
				);
			NUnit.Framework.Assert.IsTrue(output.Contains(expectedMultilineData1) && output.Contains
				(expectedMultilineData2));
		}

		/// <exception cref="System.Exception"/>
		public virtual void TestJavaScriptInfoBlock()
		{
			WebAppTests.TestBlock(typeof(TestInfoBlock.JavaScriptInfoBlock));
			TestInfoBlock.pw.Flush();
			string output = TestInfoBlock.sw.ToString();
			NUnit.Framework.Assert.IsFalse(output.Contains("<script>"));
			NUnit.Framework.Assert.IsTrue(output.Contains(JavascriptEscaped));
		}
	}
}
