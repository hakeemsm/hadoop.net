using Sharpen;

namespace org.apache.hadoop.io
{
	/// <summary>Unit tests for NonUTF8.</summary>
	public class TestTextNonUTF8 : NUnit.Framework.TestCase
	{
		/// <exception cref="System.Exception"/>
		public virtual void testNonUTF8()
		{
			// this is a non UTF8 byte array
			byte[] b = new byte[] { unchecked((byte)(-unchecked((int)(0x01)))), unchecked((byte
				)(-unchecked((int)(0x01)))), unchecked((byte)(-unchecked((int)(0x01)))), unchecked(
				(byte)(-unchecked((int)(0x01)))), unchecked((byte)(-unchecked((int)(0x01)))), unchecked(
				(byte)(-unchecked((int)(0x01)))), unchecked((byte)(-unchecked((int)(0x01)))) };
			bool nonUTF8 = false;
			org.apache.hadoop.io.Text t = new org.apache.hadoop.io.Text(b);
			try
			{
				org.apache.hadoop.io.Text.validateUTF8(b);
			}
			catch (java.nio.charset.MalformedInputException)
			{
				nonUTF8 = false;
			}
			// asserting that the byte array is non utf8
			NUnit.Framework.Assert.IsFalse(nonUTF8);
			byte[] ret = t.getBytes();
			// asseting that the byte array are the same when the Text
			// object is created.
			NUnit.Framework.Assert.IsTrue(java.util.Arrays.equals(b, ret));
		}

		/// <exception cref="System.Exception"/>
		public static void Main(string[] args)
		{
			org.apache.hadoop.io.TestTextNonUTF8 test = new org.apache.hadoop.io.TestTextNonUTF8
				();
			test.testNonUTF8();
		}
	}
}
