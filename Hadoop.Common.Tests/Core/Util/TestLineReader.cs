using System.IO;
using System.Text;
using NUnit.Framework;
using Org.Apache.Hadoop.IO;
using Sharpen;

namespace Org.Apache.Hadoop.Util
{
	public class TestLineReader
	{
		private LineReader lineReader;

		private string TestData;

		private string Delimiter;

		private Text line;

		/// <exception cref="System.Exception"/>
		[Fact]
		public virtual void TestCustomDelimiter()
		{
			/* TEST_1
			* The test scenario is the tail of the buffer
			* equals the starting character/s of delimiter
			*
			* The Test Data is such that,
			*
			* 1) we will have "</entity>" as delimiter
			*
			* 2) The tail of the current buffer would be "</"
			*    which matches with the starting character sequence of delimiter.
			*
			* 3) The Head of the next buffer would be   "id>"
			*    which does NOT match with the remaining characters of delimiter.
			*
			* 4) Input data would be prefixed by char 'a'
			*    about numberOfCharToFillTheBuffer times.
			*    So that, one iteration to buffer the input data,
			*    would end at '</' ie equals starting 2 char of delimiter
			*
			* 5) For this we would take BufferSize as 64 * 1024;
			*
			* Check Condition
			*  In the second key value pair, the value should contain
			*  "</"  from currentToken and
			*  "id>" from next token
			*/
			Delimiter = "</entity>";
			string CurrentBufferTailToken = "</entity><entity><id>Gelesh</";
			// Ending part of Input Data Buffer
			// It contains '</' ie delimiter character 
			string NextBufferHeadToken = "id><name>Omathil</name></entity>";
			// Supposing the start of next buffer is this
			string Expected = (CurrentBufferTailToken + NextBufferHeadToken).Replace(Delimiter
				, string.Empty);
			// Expected ,must capture from both the buffer, excluding Delimiter
			string TestPartOfInput = CurrentBufferTailToken + NextBufferHeadToken;
			int BufferSize = 64 * 1024;
			int numberOfCharToFillTheBuffer = BufferSize - CurrentBufferTailToken.Length;
			StringBuilder fillerString = new StringBuilder();
			for (int i = 0; i < numberOfCharToFillTheBuffer; i++)
			{
				fillerString.Append('a');
			}
			// char 'a' as a filler for the test string
			TestData = fillerString + TestPartOfInput;
			lineReader = new LineReader(new ByteArrayInputStream(Sharpen.Runtime.GetBytesForString
				(TestData)), Sharpen.Runtime.GetBytesForString(Delimiter));
			line = new Org.Apache.Hadoop.IO.Text();
			lineReader.ReadLine(line);
			Assert.Equal(fillerString.ToString(), line.ToString());
			lineReader.ReadLine(line);
			Assert.Equal(Expected, line.ToString());
			/*TEST_2
			* The test scenario is such that,
			* the character/s preceding the delimiter,
			* equals the starting character/s of delimiter
			*/
			Delimiter = "record";
			StringBuilder TestStringBuilder = new StringBuilder();
			TestStringBuilder.Append(Delimiter + "Kerala ");
			TestStringBuilder.Append(Delimiter + "Bangalore");
			TestStringBuilder.Append(Delimiter + " North Korea");
			TestStringBuilder.Append(Delimiter + Delimiter + "Guantanamo");
			TestStringBuilder.Append(Delimiter + "ecord" + "recor" + "core");
			//~EOF with 're'
			TestData = TestStringBuilder.ToString();
			lineReader = new LineReader(new ByteArrayInputStream(Sharpen.Runtime.GetBytesForString
				(TestData)), Sharpen.Runtime.GetBytesForString(Delimiter));
			lineReader.ReadLine(line);
			Assert.Equal(string.Empty, line.ToString());
			lineReader.ReadLine(line);
			Assert.Equal("Kerala ", line.ToString());
			lineReader.ReadLine(line);
			Assert.Equal("Bangalore", line.ToString());
			lineReader.ReadLine(line);
			Assert.Equal(" North Korea", line.ToString());
			lineReader.ReadLine(line);
			Assert.Equal(string.Empty, line.ToString());
			lineReader.ReadLine(line);
			Assert.Equal("Guantanamo", line.ToString());
			lineReader.ReadLine(line);
			Assert.Equal(("ecord" + "recor" + "core"), line.ToString());
		}
	}
}
