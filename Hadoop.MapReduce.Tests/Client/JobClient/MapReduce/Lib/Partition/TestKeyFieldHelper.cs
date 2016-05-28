using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Lib.Partition
{
	public class TestKeyFieldHelper : TestCase
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(TestKeyFieldHelper));

		/// <summary>Test is key-field-helper's parse option.</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestparseOption()
		{
			KeyFieldHelper helper = new KeyFieldHelper();
			helper.SetKeyFieldSeparator("\t");
			string keySpecs = "-k1.2,3.4";
			string eKeySpecs = keySpecs;
			helper.ParseOption(keySpecs);
			string actKeySpecs = helper.KeySpecs()[0].ToString();
			NUnit.Framework.Assert.AreEqual("KeyFieldHelper's parsing is garbled", eKeySpecs, 
				actKeySpecs);
			// test -k a.b
			keySpecs = "-k 1.2";
			eKeySpecs = "-k1.2,0.0";
			helper = new KeyFieldHelper();
			helper.ParseOption(keySpecs);
			actKeySpecs = helper.KeySpecs()[0].ToString();
			NUnit.Framework.Assert.AreEqual("KeyFieldHelper's parsing is garbled", eKeySpecs, 
				actKeySpecs);
			keySpecs = "-nr -k1.2,3.4";
			eKeySpecs = "-k1.2,3.4nr";
			helper = new KeyFieldHelper();
			helper.ParseOption(keySpecs);
			actKeySpecs = helper.KeySpecs()[0].ToString();
			NUnit.Framework.Assert.AreEqual("KeyFieldHelper's parsing is garbled", eKeySpecs, 
				actKeySpecs);
			keySpecs = "-nr -k1.2,3.4n";
			eKeySpecs = "-k1.2,3.4n";
			helper = new KeyFieldHelper();
			helper.ParseOption(keySpecs);
			actKeySpecs = helper.KeySpecs()[0].ToString();
			NUnit.Framework.Assert.AreEqual("KeyFieldHelper's parsing is garbled", eKeySpecs, 
				actKeySpecs);
			keySpecs = "-nr -k1.2,3.4r";
			eKeySpecs = "-k1.2,3.4r";
			helper = new KeyFieldHelper();
			helper.ParseOption(keySpecs);
			actKeySpecs = helper.KeySpecs()[0].ToString();
			NUnit.Framework.Assert.AreEqual("KeyFieldHelper's parsing is garbled", eKeySpecs, 
				actKeySpecs);
			keySpecs = "-nr -k1.2,3.4 -k5.6,7.8n -k9.10,11.12r -k13.14,15.16nr";
			//1st
			eKeySpecs = "-k1.2,3.4nr";
			helper = new KeyFieldHelper();
			helper.ParseOption(keySpecs);
			actKeySpecs = helper.KeySpecs()[0].ToString();
			NUnit.Framework.Assert.AreEqual("KeyFieldHelper's parsing is garbled", eKeySpecs, 
				actKeySpecs);
			// 2nd
			eKeySpecs = "-k5.6,7.8n";
			actKeySpecs = helper.KeySpecs()[1].ToString();
			NUnit.Framework.Assert.AreEqual("KeyFieldHelper's parsing is garbled", eKeySpecs, 
				actKeySpecs);
			//3rd
			eKeySpecs = "-k9.10,11.12r";
			actKeySpecs = helper.KeySpecs()[2].ToString();
			NUnit.Framework.Assert.AreEqual("KeyFieldHelper's parsing is garbled", eKeySpecs, 
				actKeySpecs);
			//4th
			eKeySpecs = "-k13.14,15.16nr";
			actKeySpecs = helper.KeySpecs()[3].ToString();
			NUnit.Framework.Assert.AreEqual("KeyFieldHelper's parsing is garbled", eKeySpecs, 
				actKeySpecs);
			keySpecs = "-k1.2n,3.4";
			eKeySpecs = "-k1.2,3.4n";
			helper = new KeyFieldHelper();
			helper.ParseOption(keySpecs);
			actKeySpecs = helper.KeySpecs()[0].ToString();
			NUnit.Framework.Assert.AreEqual("KeyFieldHelper's parsing is garbled", eKeySpecs, 
				actKeySpecs);
			keySpecs = "-k1.2r,3.4";
			eKeySpecs = "-k1.2,3.4r";
			helper = new KeyFieldHelper();
			helper.ParseOption(keySpecs);
			actKeySpecs = helper.KeySpecs()[0].ToString();
			NUnit.Framework.Assert.AreEqual("KeyFieldHelper's parsing is garbled", eKeySpecs, 
				actKeySpecs);
			keySpecs = "-k1.2nr,3.4";
			eKeySpecs = "-k1.2,3.4nr";
			helper = new KeyFieldHelper();
			helper.ParseOption(keySpecs);
			actKeySpecs = helper.KeySpecs()[0].ToString();
			NUnit.Framework.Assert.AreEqual("KeyFieldHelper's parsing is garbled", eKeySpecs, 
				actKeySpecs);
			keySpecs = "-k1.2,3.4n";
			eKeySpecs = "-k1.2,3.4n";
			helper = new KeyFieldHelper();
			helper.ParseOption(keySpecs);
			actKeySpecs = helper.KeySpecs()[0].ToString();
			NUnit.Framework.Assert.AreEqual("KeyFieldHelper's parsing is garbled", eKeySpecs, 
				actKeySpecs);
			keySpecs = "-k1.2,3.4r";
			eKeySpecs = "-k1.2,3.4r";
			helper = new KeyFieldHelper();
			helper.ParseOption(keySpecs);
			actKeySpecs = helper.KeySpecs()[0].ToString();
			NUnit.Framework.Assert.AreEqual("KeyFieldHelper's parsing is garbled", eKeySpecs, 
				actKeySpecs);
			keySpecs = "-k1.2,3.4nr";
			eKeySpecs = "-k1.2,3.4nr";
			helper = new KeyFieldHelper();
			helper.ParseOption(keySpecs);
			actKeySpecs = helper.KeySpecs()[0].ToString();
			NUnit.Framework.Assert.AreEqual("KeyFieldHelper's parsing is garbled", eKeySpecs, 
				actKeySpecs);
			keySpecs = "-nr -k1.2,3.4 -k5.6,7.8";
			eKeySpecs = "-k1.2,3.4nr";
			helper = new KeyFieldHelper();
			helper.ParseOption(keySpecs);
			actKeySpecs = helper.KeySpecs()[0].ToString();
			NUnit.Framework.Assert.AreEqual("KeyFieldHelper's parsing is garbled", eKeySpecs, 
				actKeySpecs);
			eKeySpecs = "-k5.6,7.8nr";
			actKeySpecs = helper.KeySpecs()[1].ToString();
			NUnit.Framework.Assert.AreEqual("KeyFieldHelper's parsing is garbled", eKeySpecs, 
				actKeySpecs);
			keySpecs = "-n -k1.2,3.4 -k5.6,7.8";
			eKeySpecs = "-k1.2,3.4n";
			helper = new KeyFieldHelper();
			helper.ParseOption(keySpecs);
			actKeySpecs = helper.KeySpecs()[0].ToString();
			NUnit.Framework.Assert.AreEqual("KeyFieldHelper's parsing is garbled", eKeySpecs, 
				actKeySpecs);
			eKeySpecs = "-k5.6,7.8n";
			actKeySpecs = helper.KeySpecs()[1].ToString();
			NUnit.Framework.Assert.AreEqual("KeyFieldHelper's parsing is garbled", eKeySpecs, 
				actKeySpecs);
			keySpecs = "-r -k1.2,3.4 -k5.6,7.8";
			eKeySpecs = "-k1.2,3.4r";
			helper = new KeyFieldHelper();
			helper.ParseOption(keySpecs);
			actKeySpecs = helper.KeySpecs()[0].ToString();
			NUnit.Framework.Assert.AreEqual("KeyFieldHelper's parsing is garbled", eKeySpecs, 
				actKeySpecs);
			eKeySpecs = "-k5.6,7.8r";
			actKeySpecs = helper.KeySpecs()[1].ToString();
			NUnit.Framework.Assert.AreEqual("KeyFieldHelper's parsing is garbled", eKeySpecs, 
				actKeySpecs);
			keySpecs = "-k1.2,3.4n -k5.6,7.8";
			eKeySpecs = "-k1.2,3.4n";
			helper = new KeyFieldHelper();
			helper.ParseOption(keySpecs);
			actKeySpecs = helper.KeySpecs()[0].ToString();
			NUnit.Framework.Assert.AreEqual("KeyFieldHelper's parsing is garbled", eKeySpecs, 
				actKeySpecs);
			eKeySpecs = "-k5.6,7.8";
			actKeySpecs = helper.KeySpecs()[1].ToString();
			NUnit.Framework.Assert.AreEqual("KeyFieldHelper's parsing is garbled", eKeySpecs, 
				actKeySpecs);
			keySpecs = "-k1.2,3.4r -k5.6,7.8";
			eKeySpecs = "-k1.2,3.4r";
			helper = new KeyFieldHelper();
			helper.ParseOption(keySpecs);
			actKeySpecs = helper.KeySpecs()[0].ToString();
			NUnit.Framework.Assert.AreEqual("KeyFieldHelper's parsing is garbled", eKeySpecs, 
				actKeySpecs);
			eKeySpecs = "-k5.6,7.8";
			actKeySpecs = helper.KeySpecs()[1].ToString();
			NUnit.Framework.Assert.AreEqual("KeyFieldHelper's parsing is garbled", eKeySpecs, 
				actKeySpecs);
			keySpecs = "-k1.2,3.4nr -k5.6,7.8";
			eKeySpecs = "-k1.2,3.4nr";
			helper = new KeyFieldHelper();
			helper.ParseOption(keySpecs);
			actKeySpecs = helper.KeySpecs()[0].ToString();
			NUnit.Framework.Assert.AreEqual("KeyFieldHelper's parsing is garbled", eKeySpecs, 
				actKeySpecs);
			eKeySpecs = "-k5.6,7.8";
			actKeySpecs = helper.KeySpecs()[1].ToString();
			NUnit.Framework.Assert.AreEqual("KeyFieldHelper's parsing is garbled", eKeySpecs, 
				actKeySpecs);
			keySpecs = "-n";
			eKeySpecs = "-k1.1,0.0n";
			helper = new KeyFieldHelper();
			helper.ParseOption(keySpecs);
			actKeySpecs = helper.KeySpecs()[0].ToString();
			NUnit.Framework.Assert.AreEqual("KeyFieldHelper's parsing is garbled", eKeySpecs, 
				actKeySpecs);
			keySpecs = "-r";
			eKeySpecs = "-k1.1,0.0r";
			helper = new KeyFieldHelper();
			helper.ParseOption(keySpecs);
			actKeySpecs = helper.KeySpecs()[0].ToString();
			NUnit.Framework.Assert.AreEqual("KeyFieldHelper's parsing is garbled", eKeySpecs, 
				actKeySpecs);
			keySpecs = "-nr";
			eKeySpecs = "-k1.1,0.0nr";
			helper = new KeyFieldHelper();
			helper.ParseOption(keySpecs);
			actKeySpecs = helper.KeySpecs()[0].ToString();
			NUnit.Framework.Assert.AreEqual("KeyFieldHelper's parsing is garbled", eKeySpecs, 
				actKeySpecs);
		}

		/// <summary>Test is key-field-helper's getWordLengths.</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestGetWordLengths()
		{
			KeyFieldHelper helper = new KeyFieldHelper();
			helper.SetKeyFieldSeparator("\t");
			// test getWordLengths with unspecified key-specifications
			string input = "hi";
			int[] result = helper.GetWordLengths(Sharpen.Runtime.GetBytesForString(input), 0, 
				2);
			NUnit.Framework.Assert.IsTrue(Equals(result, new int[] { 1 }));
			// set the key specs
			helper.SetKeyFieldSpec(1, 2);
			// test getWordLengths with 3 words
			input = "hi\thello there";
			result = helper.GetWordLengths(Sharpen.Runtime.GetBytesForString(input), 0, input
				.Length);
			NUnit.Framework.Assert.IsTrue(Equals(result, new int[] { 2, 2, 11 }));
			// test getWordLengths with 4 words but with a different separator
			helper.SetKeyFieldSeparator(" ");
			input = "hi hello\tthere you";
			result = helper.GetWordLengths(Sharpen.Runtime.GetBytesForString(input), 0, input
				.Length);
			NUnit.Framework.Assert.IsTrue(Equals(result, new int[] { 3, 2, 11, 3 }));
			// test with non zero start index
			input = "hi hello there you where me there";
			//                 .....................
			result = helper.GetWordLengths(Sharpen.Runtime.GetBytesForString(input), 10, 33);
			NUnit.Framework.Assert.IsTrue(Equals(result, new int[] { 5, 4, 3, 5, 2, 3 }));
			input = "hi hello there you where me ";
			//                 ..................
			result = helper.GetWordLengths(Sharpen.Runtime.GetBytesForString(input), 10, input
				.Length);
			NUnit.Framework.Assert.IsTrue(Equals(result, new int[] { 5, 4, 3, 5, 2, 0 }));
			input = string.Empty;
			result = helper.GetWordLengths(Sharpen.Runtime.GetBytesForString(input), 0, 0);
			NUnit.Framework.Assert.IsTrue(Equals(result, new int[] { 1, 0 }));
			input = "  abc";
			result = helper.GetWordLengths(Sharpen.Runtime.GetBytesForString(input), 0, 5);
			NUnit.Framework.Assert.IsTrue(Equals(result, new int[] { 3, 0, 0, 3 }));
			input = "  abc";
			result = helper.GetWordLengths(Sharpen.Runtime.GetBytesForString(input), 0, 2);
			NUnit.Framework.Assert.IsTrue(Equals(result, new int[] { 3, 0, 0, 0 }));
			input = " abc ";
			result = helper.GetWordLengths(Sharpen.Runtime.GetBytesForString(input), 0, 2);
			NUnit.Framework.Assert.IsTrue(Equals(result, new int[] { 2, 0, 1 }));
			helper.SetKeyFieldSeparator("abcd");
			input = "abc";
			result = helper.GetWordLengths(Sharpen.Runtime.GetBytesForString(input), 0, 3);
			NUnit.Framework.Assert.IsTrue(Equals(result, new int[] { 1, 3 }));
		}

		/// <summary>Test is key-field-helper's getStartOffset/getEndOffset.</summary>
		/// <exception cref="System.Exception"/>
		public virtual void TestgetStartEndOffset()
		{
			KeyFieldHelper helper = new KeyFieldHelper();
			helper.SetKeyFieldSeparator("\t");
			// test getStartOffset with -k1,2
			helper.SetKeyFieldSpec(1, 2);
			string input = "hi\thello";
			string expectedOutput = input;
			TestKeySpecs(input, expectedOutput, helper);
			// test getStartOffset with -k1.0,0 .. should result into start = -1
			helper = new KeyFieldHelper();
			helper.SetKeyFieldSeparator("\t");
			helper.ParseOption("-k1.0,0");
			TestKeySpecs(input, null, helper);
			// test getStartOffset with -k1,0
			helper = new KeyFieldHelper();
			helper.SetKeyFieldSeparator("\t");
			helper.ParseOption("-k1,0");
			expectedOutput = input;
			TestKeySpecs(input, expectedOutput, helper);
			// test getStartOffset with -k1.2,0
			helper = new KeyFieldHelper();
			helper.SetKeyFieldSeparator("\t");
			helper.ParseOption("-k1.2,0");
			expectedOutput = "i\thello";
			TestKeySpecs(input, expectedOutput, helper);
			// test getWordLengths with -k1.0,2.3
			helper = new KeyFieldHelper();
			helper.SetKeyFieldSeparator("\t");
			helper.ParseOption("-k1.1,2.3");
			expectedOutput = "hi\thel";
			TestKeySpecs(input, expectedOutput, helper);
			// test getWordLengths with -k1.2,2.3
			helper = new KeyFieldHelper();
			helper.SetKeyFieldSeparator("\t");
			helper.ParseOption("-k1.2,2.3");
			expectedOutput = "i\thel";
			TestKeySpecs(input, expectedOutput, helper);
			// test getStartOffset with -k1.2,3.0
			helper = new KeyFieldHelper();
			helper.SetKeyFieldSeparator("\t");
			helper.ParseOption("-k1.2,3.0");
			expectedOutput = "i\thello";
			TestKeySpecs(input, expectedOutput, helper);
			// test getStartOffset with -k2,2
			helper = new KeyFieldHelper();
			helper.SetKeyFieldSeparator("\t");
			helper.ParseOption("-k2,2");
			expectedOutput = "hello";
			TestKeySpecs(input, expectedOutput, helper);
			// test getStartOffset with -k3.0,4.0
			helper = new KeyFieldHelper();
			helper.SetKeyFieldSeparator("\t");
			helper.ParseOption("-k3.1,4.0");
			TestKeySpecs(input, null, helper);
			// test getStartOffset with -k2.1
			helper = new KeyFieldHelper();
			input = "123123123123123hi\thello\thow";
			helper.SetKeyFieldSeparator("\t");
			helper.ParseOption("-k2.1");
			expectedOutput = "hello\thow";
			TestKeySpecs(input, expectedOutput, helper, 15, input.Length);
			// test getStartOffset with -k2.1,4 with end ending on \t
			helper = new KeyFieldHelper();
			input = "123123123123123hi\thello\t\thow\tare";
			helper.SetKeyFieldSeparator("\t");
			helper.ParseOption("-k2.1,3");
			expectedOutput = "hello\t";
			TestKeySpecs(input, expectedOutput, helper, 17, input.Length);
			// test getStartOffset with -k2.1 with end ending on \t
			helper = new KeyFieldHelper();
			input = "123123123123123hi\thello\thow\tare";
			helper.SetKeyFieldSeparator("\t");
			helper.ParseOption("-k2.1");
			expectedOutput = "hello\thow\t";
			TestKeySpecs(input, expectedOutput, helper, 17, 28);
			// test getStartOffset with -k2.1,3 with smaller length
			helper = new KeyFieldHelper();
			input = "123123123123123hi\thello\thow";
			helper.SetKeyFieldSeparator("\t");
			helper.ParseOption("-k2.1,3");
			expectedOutput = "hello";
			TestKeySpecs(input, expectedOutput, helper, 15, 23);
		}

		private void TestKeySpecs(string input, string expectedOutput, KeyFieldHelper helper
			)
		{
			TestKeySpecs(input, expectedOutput, helper, 0, -1);
		}

		private void TestKeySpecs(string input, string expectedOutput, KeyFieldHelper helper
			, int s1, int e1)
		{
			Log.Info("input : " + input);
			string keySpecs = helper.KeySpecs()[0].ToString();
			Log.Info("keyspecs : " + keySpecs);
			byte[] inputBytes = Sharpen.Runtime.GetBytesForString(input);
			// get the input bytes
			if (e1 == -1)
			{
				e1 = inputBytes.Length;
			}
			Log.Info("length : " + e1);
			// get the word lengths
			int[] indices = helper.GetWordLengths(inputBytes, s1, e1);
			// get the start index
			int start = helper.GetStartOffset(inputBytes, s1, e1, indices, helper.KeySpecs()[
				0]);
			Log.Info("start : " + start);
			if (expectedOutput == null)
			{
				NUnit.Framework.Assert.AreEqual("Expected -1 when the start index is invalid", -1
					, start);
				return;
			}
			// get the end index
			int end = helper.GetEndOffset(inputBytes, s1, e1, indices, helper.KeySpecs()[0]);
			Log.Info("end : " + end);
			//my fix
			end = (end >= inputBytes.Length) ? inputBytes.Length - 1 : end;
			int length = end + 1 - start;
			Log.Info("length : " + length);
			byte[] outputBytes = new byte[length];
			System.Array.Copy(inputBytes, start, outputBytes, 0, length);
			string output = Sharpen.Runtime.GetStringForBytes(outputBytes);
			Log.Info("output : " + output);
			Log.Info("expected-output : " + expectedOutput);
			NUnit.Framework.Assert.AreEqual(keySpecs + " failed on input '" + input + "'", expectedOutput
				, output);
		}

		// check for equality of 2 int arrays
		private bool Equals(int[] test, int[] expected)
		{
			// check array length
			if (test[0] != expected[0])
			{
				return false;
			}
			// if length is same then check the contents
			for (int i = 0; i < test[0] && i < expected[0]; ++i)
			{
				if (test[i] != expected[i])
				{
					return false;
				}
			}
			return true;
		}
	}
}
