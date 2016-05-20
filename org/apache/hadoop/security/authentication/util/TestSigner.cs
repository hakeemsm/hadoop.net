using Sharpen;

namespace org.apache.hadoop.security.authentication.util
{
	public class TestSigner
	{
		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testNullAndEmptyString()
		{
			org.apache.hadoop.security.authentication.util.Signer signer = new org.apache.hadoop.security.authentication.util.Signer
				(createStringSignerSecretProvider());
			try
			{
				signer.sign(null);
				NUnit.Framework.Assert.Fail();
			}
			catch (System.ArgumentException)
			{
			}
			catch
			{
				// Expected
				NUnit.Framework.Assert.Fail();
			}
			try
			{
				signer.sign(string.Empty);
				NUnit.Framework.Assert.Fail();
			}
			catch (System.ArgumentException)
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
		public virtual void testSignature()
		{
			org.apache.hadoop.security.authentication.util.Signer signer = new org.apache.hadoop.security.authentication.util.Signer
				(createStringSignerSecretProvider());
			string s1 = signer.sign("ok");
			string s2 = signer.sign("ok");
			string s3 = signer.sign("wrong");
			NUnit.Framework.Assert.AreEqual(s1, s2);
			NUnit.Framework.Assert.assertNotEquals(s1, s3);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testVerify()
		{
			org.apache.hadoop.security.authentication.util.Signer signer = new org.apache.hadoop.security.authentication.util.Signer
				(createStringSignerSecretProvider());
			string t = "test";
			string s = signer.sign(t);
			string e = signer.verifyAndExtract(s);
			NUnit.Framework.Assert.AreEqual(t, e);
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testInvalidSignedText()
		{
			org.apache.hadoop.security.authentication.util.Signer signer = new org.apache.hadoop.security.authentication.util.Signer
				(createStringSignerSecretProvider());
			try
			{
				signer.verifyAndExtract("test");
				NUnit.Framework.Assert.Fail();
			}
			catch (org.apache.hadoop.security.authentication.util.SignerException)
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
		public virtual void testTampering()
		{
			org.apache.hadoop.security.authentication.util.Signer signer = new org.apache.hadoop.security.authentication.util.Signer
				(createStringSignerSecretProvider());
			string t = "test";
			string s = signer.sign(t);
			s += "x";
			try
			{
				signer.verifyAndExtract(s);
				NUnit.Framework.Assert.Fail();
			}
			catch (org.apache.hadoop.security.authentication.util.SignerException)
			{
			}
			catch
			{
				// Expected
				NUnit.Framework.Assert.Fail();
			}
		}

		/// <exception cref="System.Exception"/>
		private org.apache.hadoop.security.authentication.util.StringSignerSecretProvider
			 createStringSignerSecretProvider()
		{
			org.apache.hadoop.security.authentication.util.StringSignerSecretProvider secretProvider
				 = new org.apache.hadoop.security.authentication.util.StringSignerSecretProvider
				();
			java.util.Properties secretProviderProps = new java.util.Properties();
			secretProviderProps.setProperty(org.apache.hadoop.security.authentication.server.AuthenticationFilter
				.SIGNATURE_SECRET, "secret");
			secretProvider.init(secretProviderProps, null, -1);
			return secretProvider;
		}

		/// <exception cref="System.Exception"/>
		[NUnit.Framework.Test]
		public virtual void testMultipleSecrets()
		{
			org.apache.hadoop.security.authentication.util.TestSigner.TestSignerSecretProvider
				 secretProvider = new org.apache.hadoop.security.authentication.util.TestSigner.TestSignerSecretProvider
				(this);
			org.apache.hadoop.security.authentication.util.Signer signer = new org.apache.hadoop.security.authentication.util.Signer
				(secretProvider);
			secretProvider.setCurrentSecret("secretB");
			string t1 = "test";
			string s1 = signer.sign(t1);
			string e1 = signer.verifyAndExtract(s1);
			NUnit.Framework.Assert.AreEqual(t1, e1);
			secretProvider.setPreviousSecret("secretA");
			string t2 = "test";
			string s2 = signer.sign(t2);
			string e2 = signer.verifyAndExtract(s2);
			NUnit.Framework.Assert.AreEqual(t2, e2);
			NUnit.Framework.Assert.AreEqual(s1, s2);
			//check is using current secret for signing
			secretProvider.setCurrentSecret("secretC");
			secretProvider.setPreviousSecret("secretB");
			string t3 = "test";
			string s3 = signer.sign(t3);
			string e3 = signer.verifyAndExtract(s3);
			NUnit.Framework.Assert.AreEqual(t3, e3);
			NUnit.Framework.Assert.assertNotEquals(s1, s3);
			//check not using current secret for signing
			string e1b = signer.verifyAndExtract(s1);
			NUnit.Framework.Assert.AreEqual(t1, e1b);
			// previous secret still valid
			secretProvider.setCurrentSecret("secretD");
			secretProvider.setPreviousSecret("secretC");
			try
			{
				signer.verifyAndExtract(s1);
				// previous secret no longer valid
				NUnit.Framework.Assert.Fail();
			}
			catch (org.apache.hadoop.security.authentication.util.SignerException)
			{
			}
		}

		internal class TestSignerSecretProvider : org.apache.hadoop.security.authentication.util.SignerSecretProvider
		{
			private byte[] currentSecret;

			private byte[] previousSecret;

			// Expected
			public override void init(java.util.Properties config, javax.servlet.ServletContext
				 servletContext, long tokenValidity)
			{
			}

			public override byte[] getCurrentSecret()
			{
				return this.currentSecret;
			}

			public override byte[][] getAllSecrets()
			{
				return new byte[][] { this.currentSecret, this.previousSecret };
			}

			public virtual void setCurrentSecret(string secretStr)
			{
				this.currentSecret = Sharpen.Runtime.getBytesForString(secretStr);
			}

			public virtual void setPreviousSecret(string previousSecretStr)
			{
				this.previousSecret = Sharpen.Runtime.getBytesForString(previousSecretStr);
			}

			internal TestSignerSecretProvider(TestSigner _enclosing)
			{
				this._enclosing = _enclosing;
			}

			private readonly TestSigner _enclosing;
		}
	}
}
