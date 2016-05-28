using System.Collections.Generic;
using System.IO;
using System.Text;
using Javax.Servlet;
using Javax.Servlet.Http;
using NUnit.Framework;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Server.Common;
using Org.Apache.Hadoop.Net;
using Org.Mortbay.Jetty;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Server.Namenode
{
	internal class MockFSInputStream : FSInputStream
	{
		internal long currentPos = 0;

		/*
		* Mock input stream class that always outputs the current position of the stream.
		*/
		/// <exception cref="System.IO.IOException"/>
		public override int Read()
		{
			return (int)(currentPos++);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Close()
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Seek(long pos)
		{
			currentPos = pos;
		}

		/// <exception cref="System.IO.IOException"/>
		public override long GetPos()
		{
			return currentPos;
		}

		/// <exception cref="System.IO.IOException"/>
		public override bool SeekToNewSource(long targetPos)
		{
			return false;
		}
	}

	public class TestStreamFile
	{
		private readonly HdfsConfiguration Conf = new HdfsConfiguration();

		private readonly DFSClient clientMock = Org.Mockito.Mockito.Mock<DFSClient>();

		private readonly HttpServletRequest mockHttpServletRequest = Org.Mockito.Mockito.
			Mock<HttpServletRequest>();

		private readonly HttpServletResponse mockHttpServletResponse = Org.Mockito.Mockito
			.Mock<HttpServletResponse>();

		private readonly ServletContext mockServletContext = Org.Mockito.Mockito.Mock<ServletContext
			>();

		private sealed class _StreamFile_93 : StreamFile
		{
			public _StreamFile_93(TestStreamFile _enclosing)
			{
				this._enclosing = _enclosing;
				this.serialVersionUID = -5513776238875189473L;
			}

			private const long serialVersionUID;

			public override ServletContext GetServletContext()
			{
				return this._enclosing.mockServletContext;
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.Exception"/>
			protected internal override DFSClient GetDFSClient(HttpServletRequest request)
			{
				return this._enclosing.clientMock;
			}

			private readonly TestStreamFile _enclosing;
		}

		internal readonly StreamFile sfile;

		// return an array matching the output of mockfsinputstream
		private static byte[] GetOutputArray(int start, int count)
		{
			byte[] a = new byte[count];
			for (int i = 0; i < count; i++)
			{
				a[i] = unchecked((byte)(start + i));
			}
			return a;
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestWriteTo()
		{
			FSDataInputStream fsdin = new FSDataInputStream(new MockFSInputStream());
			ByteArrayOutputStream os = new ByteArrayOutputStream();
			// new int[]{s_1, c_1, s_2, c_2, ..., s_n, c_n} means to test
			// reading c_i bytes starting at s_i
			int[] pairs = new int[] { 0, 10000, 50, 100, 50, 6000, 1000, 2000, 0, 1, 0, 0, 5000
				, 0 };
			NUnit.Framework.Assert.IsTrue("Pairs array must be even", pairs.Length % 2 == 0);
			for (int i = 0; i < pairs.Length; i += 2)
			{
				StreamFile.CopyFromOffset(fsdin, os, pairs[i], pairs[i + 1]);
				Assert.AssertArrayEquals("Reading " + pairs[i + 1] + " bytes from offset " + pairs
					[i], GetOutputArray(pairs[i], pairs[i + 1]), os.ToByteArray());
				os.Reset();
			}
		}

		private IList<InclusiveByteRange> StrToRanges(string s, int contentLength)
		{
			IList<string> l = Arrays.AsList(new string[] { "bytes=" + s });
			Enumeration<object> e = (new Vector<string>(l)).GetEnumerator();
			return InclusiveByteRange.SatisfiableRanges(e, contentLength);
		}

		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestSendPartialData()
		{
			FSDataInputStream @in = new FSDataInputStream(new MockFSInputStream());
			ByteArrayOutputStream os = new ByteArrayOutputStream();
			{
				// test if multiple ranges, then 416
				IList<InclusiveByteRange> ranges = StrToRanges("0-,10-300", 500);
				HttpServletResponse response = Org.Mockito.Mockito.Mock<HttpServletResponse>();
				StreamFile.SendPartialData(@in, os, response, 500, ranges);
				// Multiple ranges should result in a 416 error
				Org.Mockito.Mockito.Verify(response).SetStatus(416);
			}
			{
				// test if no ranges, then 416
				os.Reset();
				HttpServletResponse response = Org.Mockito.Mockito.Mock<HttpServletResponse>();
				StreamFile.SendPartialData(@in, os, response, 500, null);
				// No ranges should result in a 416 error
				Org.Mockito.Mockito.Verify(response).SetStatus(416);
			}
			{
				// test if invalid single range (out of bounds), then 416
				IList<InclusiveByteRange> ranges = StrToRanges("600-800", 500);
				HttpServletResponse response = Org.Mockito.Mockito.Mock<HttpServletResponse>();
				StreamFile.SendPartialData(@in, os, response, 500, ranges);
				// Single (but invalid) range should result in a 416
				Org.Mockito.Mockito.Verify(response).SetStatus(416);
			}
			{
				// test if one (valid) range, then 206
				IList<InclusiveByteRange> ranges = StrToRanges("100-300", 500);
				HttpServletResponse response = Org.Mockito.Mockito.Mock<HttpServletResponse>();
				StreamFile.SendPartialData(@in, os, response, 500, ranges);
				// Single (valid) range should result in a 206
				Org.Mockito.Mockito.Verify(response).SetStatus(206);
				Assert.AssertArrayEquals("Byte range from 100-300", GetOutputArray(100, 201), os.
					ToByteArray());
			}
		}

		// Test for positive scenario
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestDoGetShouldWriteTheFileContentIntoServletOutputStream()
		{
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(Conf).NumDataNodes(1).Build();
			try
			{
				Path testFile = CreateFile();
				SetUpForDoGetTest(cluster, testFile);
				TestStreamFile.ServletOutputStreamExtn outStream = new TestStreamFile.ServletOutputStreamExtn
					();
				Org.Mockito.Mockito.DoReturn(outStream).When(mockHttpServletResponse).GetOutputStream
					();
				StreamFile sfile = new _StreamFile_222(this);
				sfile.DoGet(mockHttpServletRequest, mockHttpServletResponse);
				NUnit.Framework.Assert.AreEqual("Not writing the file data into ServletOutputStream"
					, outStream.GetResult(), "test");
			}
			finally
			{
				cluster.Shutdown();
			}
		}

		private sealed class _StreamFile_222 : StreamFile
		{
			public _StreamFile_222(TestStreamFile _enclosing)
			{
				this._enclosing = _enclosing;
				this.serialVersionUID = 7715590481809562722L;
			}

			private const long serialVersionUID;

			public override ServletContext GetServletContext()
			{
				return this._enclosing.mockServletContext;
			}

			private readonly TestStreamFile _enclosing;
		}

		// Test for cleaning the streams in exception cases also
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestDoGetShouldCloseTheDFSInputStreamIfResponseGetOutPutStreamThrowsAnyException
			()
		{
			MiniDFSCluster cluster = new MiniDFSCluster.Builder(Conf).NumDataNodes(1).Build();
			try
			{
				Path testFile = CreateFile();
				SetUpForDoGetTest(cluster, testFile);
				Org.Mockito.Mockito.DoThrow(new IOException()).When(mockHttpServletResponse).GetOutputStream
					();
				DFSInputStream fsMock = Org.Mockito.Mockito.Mock<DFSInputStream>();
				Org.Mockito.Mockito.DoReturn(fsMock).When(clientMock).Open(testFile.ToString());
				Org.Mockito.Mockito.DoReturn(Sharpen.Extensions.ValueOf(4)).When(fsMock).GetFileLength
					();
				try
				{
					sfile.DoGet(mockHttpServletRequest, mockHttpServletResponse);
					NUnit.Framework.Assert.Fail("Not throwing the IOException");
				}
				catch (IOException)
				{
					Org.Mockito.Mockito.Verify(clientMock, Org.Mockito.Mockito.AtLeastOnce()).Close();
				}
			}
			finally
			{
				cluster.Shutdown();
			}
		}

		private void SetUpForDoGetTest(MiniDFSCluster cluster, Path testFile)
		{
			Org.Mockito.Mockito.DoReturn(Conf).When(mockServletContext).GetAttribute(JspHelper
				.CurrentConf);
			Org.Mockito.Mockito.DoReturn(NetUtils.GetHostPortString(NameNode.GetAddress(Conf)
				)).When(mockHttpServletRequest).GetParameter("nnaddr");
			Org.Mockito.Mockito.DoReturn(testFile.ToString()).When(mockHttpServletRequest).GetPathInfo
				();
			Org.Mockito.Mockito.DoReturn("/streamFile" + testFile.ToString()).When(mockHttpServletRequest
				).GetRequestURI();
		}

		/// <exception cref="System.IO.IOException"/>
		internal static Path WriteFile(FileSystem fs, Path f)
		{
			DataOutputStream @out = fs.Create(f);
			try
			{
				@out.WriteBytes("test");
			}
			finally
			{
				@out.Close();
			}
			NUnit.Framework.Assert.IsTrue(fs.Exists(f));
			return f;
		}

		/// <exception cref="System.IO.IOException"/>
		private Path CreateFile()
		{
			FileSystem fs = FileSystem.Get(Conf);
			Path testFile = new Path("/test/mkdirs/doGet");
			WriteFile(fs, testFile);
			return testFile;
		}

		public class ServletOutputStreamExtn : ServletOutputStream
		{
			private readonly StringBuilder buffer = new StringBuilder(3);

			public virtual string GetResult()
			{
				return buffer.ToString();
			}

			/// <exception cref="System.IO.IOException"/>
			public override void Write(int b)
			{
				buffer.Append((char)b);
			}
		}

		public TestStreamFile()
		{
			sfile = new _StreamFile_93(this);
		}
	}
}
