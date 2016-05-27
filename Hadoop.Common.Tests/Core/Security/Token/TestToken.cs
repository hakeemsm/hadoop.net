using NUnit.Framework;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Security.Token.Delegation;
using Sharpen;

namespace Org.Apache.Hadoop.Security.Token
{
	/// <summary>Unit tests for Token</summary>
	public class TestToken : TestCase
	{
		internal static bool IsEqual(object a, object b)
		{
			return a == null ? b == null : a.Equals(b);
		}

		internal static bool CheckEqual(Org.Apache.Hadoop.Security.Token.Token<TokenIdentifier
			> a, Org.Apache.Hadoop.Security.Token.Token<TokenIdentifier> b)
		{
			return Arrays.Equals(a.GetIdentifier(), b.GetIdentifier()) && Arrays.Equals(a.GetPassword
				(), b.GetPassword()) && IsEqual(a.GetKind(), b.GetKind()) && IsEqual(a.GetService
				(), b.GetService());
		}

		/// <summary>Test token serialization</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void TestTokenSerialization()
		{
			// Get a token
			Org.Apache.Hadoop.Security.Token.Token<TokenIdentifier> sourceToken = new Org.Apache.Hadoop.Security.Token.Token
				<TokenIdentifier>();
			sourceToken.SetService(new Text("service"));
			// Write it to an output buffer
			DataOutputBuffer @out = new DataOutputBuffer();
			sourceToken.Write(@out);
			// Read the token back
			DataInputBuffer @in = new DataInputBuffer();
			@in.Reset(@out.GetData(), @out.GetLength());
			Org.Apache.Hadoop.Security.Token.Token<TokenIdentifier> destToken = new Org.Apache.Hadoop.Security.Token.Token
				<TokenIdentifier>();
			destToken.ReadFields(@in);
			NUnit.Framework.Assert.IsTrue(CheckEqual(sourceToken, destToken));
		}

		/// <exception cref="System.Exception"/>
		private static void CheckUrlSafe(string str)
		{
			int len = str.Length;
			for (int i = 0; i < len; ++i)
			{
				char ch = str[i];
				if (ch == '-')
				{
					continue;
				}
				if (ch == '_')
				{
					continue;
				}
				if (ch >= '0' && ch <= '9')
				{
					continue;
				}
				if (ch >= 'A' && ch <= 'Z')
				{
					continue;
				}
				if (ch >= 'a' && ch <= 'z')
				{
					continue;
				}
				Fail("Encoded string " + str + " has invalid character at position " + i);
			}
		}

		/// <exception cref="System.Exception"/>
		public static void TestEncodeWritable()
		{
			string[] values = new string[] { string.Empty, "a", "bb", "ccc", "dddd", "eeeee", 
				"ffffff", "ggggggg", "hhhhhhhh", "iiiiiiiii", "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLM"
				 + "NOPQRSTUVWXYZ01234567890!@#$%^&*()-=_+[]{}|;':,./<>?" };
			Org.Apache.Hadoop.Security.Token.Token<AbstractDelegationTokenIdentifier> orig;
			Org.Apache.Hadoop.Security.Token.Token<AbstractDelegationTokenIdentifier> copy = 
				new Org.Apache.Hadoop.Security.Token.Token<AbstractDelegationTokenIdentifier>();
			// ensure that for each string the input and output values match
			for (int i = 0; i < values.Length; ++i)
			{
				string val = values[i];
				System.Console.Out.WriteLine("Input = " + val);
				orig = new Org.Apache.Hadoop.Security.Token.Token<AbstractDelegationTokenIdentifier
					>(Sharpen.Runtime.GetBytesForString(val), Sharpen.Runtime.GetBytesForString(val)
					, new Text(val), new Text(val));
				string encode = orig.EncodeToUrlString();
				copy.DecodeFromUrlString(encode);
				NUnit.Framework.Assert.AreEqual(orig, copy);
				CheckUrlSafe(encode);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void TestDecodeIdentifier()
		{
			TestDelegationToken.TestDelegationTokenSecretManager secretManager = new TestDelegationToken.TestDelegationTokenSecretManager
				(0, 0, 0, 0);
			secretManager.StartThreads();
			TestDelegationToken.TestDelegationTokenIdentifier id = new TestDelegationToken.TestDelegationTokenIdentifier
				(new Text("owner"), new Text("renewer"), new Text("realUser"));
			Org.Apache.Hadoop.Security.Token.Token<TestDelegationToken.TestDelegationTokenIdentifier
				> token = new Org.Apache.Hadoop.Security.Token.Token<TestDelegationToken.TestDelegationTokenIdentifier
				>(id, secretManager);
			TokenIdentifier idCopy = token.DecodeIdentifier();
			NUnit.Framework.Assert.AreNotSame(id, idCopy);
			NUnit.Framework.Assert.AreEqual(id, idCopy);
		}
	}
}
