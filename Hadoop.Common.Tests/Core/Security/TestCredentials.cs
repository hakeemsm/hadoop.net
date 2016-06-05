using System.Collections.Generic;
using System.IO;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Security.Token;


namespace Org.Apache.Hadoop.Security
{
	public class TestCredentials
	{
		private const string DefaultHmacAlgorithm = "HmacSHA1";

		private static readonly FilePath tmpDir = new FilePath(Runtime.GetProperty("test.build.data"
			, "/tmp"), "mapred");

		[NUnit.Framework.SetUp]
		public virtual void SetUp()
		{
			tmpDir.Mkdir();
		}

		[NUnit.Framework.TearDown]
		public virtual void TearDown()
		{
			tmpDir.Delete();
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="NoSuchAlgorithmException"/>
		[Fact]
		public virtual void TestReadWriteStorage<T>()
			where T : TokenIdentifier
		{
			// create tokenStorage Object
			Credentials ts = new Credentials();
			Org.Apache.Hadoop.Security.Token.Token<T> token1 = new Org.Apache.Hadoop.Security.Token.Token
				();
			Org.Apache.Hadoop.Security.Token.Token<T> token2 = new Org.Apache.Hadoop.Security.Token.Token
				();
			Text service1 = new Text("service1");
			Text service2 = new Text("service2");
			ICollection<Text> services = new AList<Text>();
			services.AddItem(service1);
			services.AddItem(service2);
			token1.SetService(service1);
			token2.SetService(service2);
			ts.AddToken(new Text("sometoken1"), token1);
			ts.AddToken(new Text("sometoken2"), token2);
			// create keys and put it in
			KeyGenerator kg = KeyGenerator.GetInstance(DefaultHmacAlgorithm);
			string alias = "alias";
			IDictionary<Text, byte[]> m = new Dictionary<Text, byte[]>(10);
			for (int i = 0; i < 10; i++)
			{
				Key key = kg.GenerateKey();
				m[new Text(alias + i)] = key.GetEncoded();
				ts.AddSecretKey(new Text(alias + i), key.GetEncoded());
			}
			// create file to store
			FilePath tmpFileName = new FilePath(tmpDir, "tokenStorageTest");
			DataOutputStream dos = new DataOutputStream(new FileOutputStream(tmpFileName));
			ts.Write(dos);
			dos.Close();
			// open and read it back
			DataInputStream dis = new DataInputStream(new FileInputStream(tmpFileName));
			ts = new Credentials();
			ts.ReadFields(dis);
			dis.Close();
			// get the tokens and compare the services
			ICollection<Org.Apache.Hadoop.Security.Token.Token<TokenIdentifier>> list = ts.GetAllTokens
				();
			Assert.Equal("getAllTokens should return collection of size 2"
				, list.Count, 2);
			bool foundFirst = false;
			bool foundSecond = false;
			foreach (Org.Apache.Hadoop.Security.Token.Token<TokenIdentifier> token in list)
			{
				if (token.GetService().Equals(service1))
				{
					foundFirst = true;
				}
				if (token.GetService().Equals(service2))
				{
					foundSecond = true;
				}
			}
			Assert.True("Tokens for services service1 and service2 must be present"
				, foundFirst && foundSecond);
			// compare secret keys
			int mapLen = m.Count;
			Assert.Equal("wrong number of keys in the Storage", mapLen, ts
				.NumberOfSecretKeys());
			foreach (Text a in m.Keys)
			{
				byte[] kTS = ts.GetSecretKey(a);
				byte[] kLocal = m[a];
				Assert.True("keys don't match for " + a, WritableComparator.CompareBytes
					(kTS, 0, kTS.Length, kLocal, 0, kLocal.Length) == 0);
			}
			tmpFileName.Delete();
		}

		internal static Text[] secret = new Text[] { new Text("secret1"), new Text("secret2"
			), new Text("secret3"), new Text("secret4") };

		internal static Text[] service = new Text[] { new Text("service1"), new Text("service2"
			), new Text("service3"), new Text("service4") };

		internal static Org.Apache.Hadoop.Security.Token.Token<object>[] token = new Org.Apache.Hadoop.Security.Token.Token
			<object>[] { new Org.Apache.Hadoop.Security.Token.Token<TokenIdentifier>(), new 
			Org.Apache.Hadoop.Security.Token.Token<TokenIdentifier>(), new Org.Apache.Hadoop.Security.Token.Token
			<TokenIdentifier>(), new Org.Apache.Hadoop.Security.Token.Token<TokenIdentifier>
			() };

		[Fact]
		public virtual void AddAll()
		{
			Credentials creds = new Credentials();
			creds.AddToken(service[0], token[0]);
			creds.AddToken(service[1], token[1]);
			creds.AddSecretKey(secret[0], secret[0].GetBytes());
			creds.AddSecretKey(secret[1], secret[1].GetBytes());
			Credentials credsToAdd = new Credentials();
			// one duplicate with different value, one new
			credsToAdd.AddToken(service[0], token[3]);
			credsToAdd.AddToken(service[2], token[2]);
			credsToAdd.AddSecretKey(secret[0], secret[3].GetBytes());
			credsToAdd.AddSecretKey(secret[2], secret[2].GetBytes());
			creds.AddAll(credsToAdd);
			Assert.Equal(3, creds.NumberOfTokens());
			Assert.Equal(3, creds.NumberOfSecretKeys());
			// existing token & secret should be overwritten
			Assert.Equal(token[3], creds.GetToken(service[0]));
			Assert.Equal(secret[3], new Text(creds.GetSecretKey(secret[0])
				));
			// non-duplicate token & secret should be present
			Assert.Equal(token[1], creds.GetToken(service[1]));
			Assert.Equal(secret[1], new Text(creds.GetSecretKey(secret[1])
				));
			// new token & secret should be added
			Assert.Equal(token[2], creds.GetToken(service[2]));
			Assert.Equal(secret[2], new Text(creds.GetSecretKey(secret[2])
				));
		}

		[Fact]
		public virtual void MergeAll()
		{
			Credentials creds = new Credentials();
			creds.AddToken(service[0], token[0]);
			creds.AddToken(service[1], token[1]);
			creds.AddSecretKey(secret[0], secret[0].GetBytes());
			creds.AddSecretKey(secret[1], secret[1].GetBytes());
			Credentials credsToAdd = new Credentials();
			// one duplicate with different value, one new
			credsToAdd.AddToken(service[0], token[3]);
			credsToAdd.AddToken(service[2], token[2]);
			credsToAdd.AddSecretKey(secret[0], secret[3].GetBytes());
			credsToAdd.AddSecretKey(secret[2], secret[2].GetBytes());
			creds.MergeAll(credsToAdd);
			Assert.Equal(3, creds.NumberOfTokens());
			Assert.Equal(3, creds.NumberOfSecretKeys());
			// existing token & secret should not be overwritten
			Assert.Equal(token[0], creds.GetToken(service[0]));
			Assert.Equal(secret[0], new Text(creds.GetSecretKey(secret[0])
				));
			// non-duplicate token & secret should be present
			Assert.Equal(token[1], creds.GetToken(service[1]));
			Assert.Equal(secret[1], new Text(creds.GetSecretKey(secret[1])
				));
			// new token & secret should be added
			Assert.Equal(token[2], creds.GetToken(service[2]));
			Assert.Equal(secret[2], new Text(creds.GetSecretKey(secret[2])
				));
		}

		[Fact]
		public virtual void TestAddTokensToUGI()
		{
			UserGroupInformation ugi = UserGroupInformation.CreateRemoteUser("someone");
			Credentials creds = new Credentials();
			for (int i = 0; i < service.Length; i++)
			{
				creds.AddToken(service[i], token[i]);
			}
			ugi.AddCredentials(creds);
			creds = ugi.GetCredentials();
			for (int i_1 = 0; i_1 < service.Length; i_1++)
			{
				NUnit.Framework.Assert.AreSame(token[i_1], creds.GetToken(service[i_1]));
			}
			Assert.Equal(service.Length, creds.NumberOfTokens());
		}
	}
}
