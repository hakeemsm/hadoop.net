using System;
using Javax.Servlet;
using NUnit.Framework;
using Org.Apache.Hadoop.Security.Authentication.Server;
using Sharpen;

namespace Org.Apache.Hadoop.Security.Authentication.Util
{
	public class TestSigner
	{
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestNullAndEmptyString()
		{
			Signer signer = new Signer(CreateStringSignerSecretProvider());
			try
			{
				signer.Sign(null);
				NUnit.Framework.Assert.Fail();
			}
			catch (ArgumentException)
			{
			}
			catch
			{
				// Expected
				NUnit.Framework.Assert.Fail();
			}
			try
			{
				signer.Sign(string.Empty);
				NUnit.Framework.Assert.Fail();
			}
			catch (ArgumentException)
			{
			}
			catch
			{
				// Expected
				NUnit.Framework.Assert.Fail();
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestSignature()
		{
			Signer signer = new Signer(CreateStringSignerSecretProvider());
			string s1 = signer.Sign("ok");
			string s2 = signer.Sign("ok");
			string s3 = signer.Sign("wrong");
			NUnit.Framework.Assert.AreEqual(s1, s2);
			Assert.AssertNotEquals(s1, s3);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestVerify()
		{
			Signer signer = new Signer(CreateStringSignerSecretProvider());
			string t = "test";
			string s = signer.Sign(t);
			string e = signer.VerifyAndExtract(s);
			NUnit.Framework.Assert.AreEqual(t, e);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestInvalidSignedText()
		{
			Signer signer = new Signer(CreateStringSignerSecretProvider());
			try
			{
				signer.VerifyAndExtract("test");
				NUnit.Framework.Assert.Fail();
			}
			catch (SignerException)
			{
			}
			catch
			{
				// Expected
				NUnit.Framework.Assert.Fail();
			}
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestTampering()
		{
			Signer signer = new Signer(CreateStringSignerSecretProvider());
			string t = "test";
			string s = signer.Sign(t);
			s += "x";
			try
			{
				signer.VerifyAndExtract(s);
				NUnit.Framework.Assert.Fail();
			}
			catch (SignerException)
			{
			}
			catch
			{
				// Expected
				NUnit.Framework.Assert.Fail();
			}
		}

		/// <exception cref="System.Exception"/>
		private StringSignerSecretProvider CreateStringSignerSecretProvider()
		{
			StringSignerSecretProvider secretProvider = new StringSignerSecretProvider();
			Properties secretProviderProps = new Properties();
			secretProviderProps.SetProperty(AuthenticationFilter.SignatureSecret, "secret");
			secretProvider.Init(secretProviderProps, null, -1);
			return secretProvider;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void TestMultipleSecrets()
		{
			TestSigner.TestSignerSecretProvider secretProvider = new TestSigner.TestSignerSecretProvider
				(this);
			Signer signer = new Signer(secretProvider);
			secretProvider.SetCurrentSecret("secretB");
			string t1 = "test";
			string s1 = signer.Sign(t1);
			string e1 = signer.VerifyAndExtract(s1);
			NUnit.Framework.Assert.AreEqual(t1, e1);
			secretProvider.SetPreviousSecret("secretA");
			string t2 = "test";
			string s2 = signer.Sign(t2);
			string e2 = signer.VerifyAndExtract(s2);
			NUnit.Framework.Assert.AreEqual(t2, e2);
			NUnit.Framework.Assert.AreEqual(s1, s2);
			//check is using current secret for signing
			secretProvider.SetCurrentSecret("secretC");
			secretProvider.SetPreviousSecret("secretB");
			string t3 = "test";
			string s3 = signer.Sign(t3);
			string e3 = signer.VerifyAndExtract(s3);
			NUnit.Framework.Assert.AreEqual(t3, e3);
			Assert.AssertNotEquals(s1, s3);
			//check not using current secret for signing
			string e1b = signer.VerifyAndExtract(s1);
			NUnit.Framework.Assert.AreEqual(t1, e1b);
			// previous secret still valid
			secretProvider.SetCurrentSecret("secretD");
			secretProvider.SetPreviousSecret("secretC");
			try
			{
				signer.VerifyAndExtract(s1);
				// previous secret no longer valid
				NUnit.Framework.Assert.Fail();
			}
			catch (SignerException)
			{
			}
		}

		internal class TestSignerSecretProvider : SignerSecretProvider
		{
			private byte[] currentSecret;

			private byte[] previousSecret;

			// Expected
			public override void Init(Properties config, ServletContext servletContext, long 
				tokenValidity)
			{
			}

			public override byte[] GetCurrentSecret()
			{
				return this.currentSecret;
			}

			public override byte[][] GetAllSecrets()
			{
				return new byte[][] { this.currentSecret, this.previousSecret };
			}

			public virtual void SetCurrentSecret(string secretStr)
			{
				this.currentSecret = Sharpen.Runtime.GetBytesForString(secretStr);
			}

			public virtual void SetPreviousSecret(string previousSecretStr)
			{
				this.previousSecret = Sharpen.Runtime.GetBytesForString(previousSecretStr);
			}

			internal TestSignerSecretProvider(TestSigner _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly TestSigner _enclosing;
		}
	}
}
