using NUnit.Framework;


namespace Org.Apache.Hadoop.IO
{
	/// <summary>Unit tests for NonUTF8.</summary>
	public class TestTextNonUTF8 : TestCase
	{
		/// <exception cref="System.Exception"/>
		public virtual void TestNonUTF8()
		{
			// this is a non UTF8 byte array
			byte[] b = new byte[] { unchecked((byte)(-unchecked((int)(0x01)))), unchecked((byte
				)(-unchecked((int)(0x01)))), unchecked((byte)(-unchecked((int)(0x01)))), unchecked(
				(byte)(-unchecked((int)(0x01)))), unchecked((byte)(-unchecked((int)(0x01)))), unchecked(
				(byte)(-unchecked((int)(0x01)))), unchecked((byte)(-unchecked((int)(0x01)))) };
			bool nonUTF8 = false;
			Text t = new Text(b);
			try
			{
				Text.ValidateUTF8(b);
			}
			catch (MalformedInputException)
			{
				nonUTF8 = false;
			}
			// asserting that the byte array is non utf8
			NUnit.Framework.Assert.IsFalse(nonUTF8);
			byte[] ret = t.GetBytes();
			// asseting that the byte array are the same when the Text
			// object is created.
			Assert.True(Arrays.Equals(b, ret));
		}

		/// <exception cref="System.Exception"/>
		public static void Main(string[] args)
		{
			TestTextNonUTF8 test = new TestTextNonUTF8();
			test.TestNonUTF8();
		}
	}
}
