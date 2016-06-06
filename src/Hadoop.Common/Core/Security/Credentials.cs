using System.Collections.Generic;
using System.IO;
using Hadoop.Common.Core.Conf;
using Hadoop.Common.Core.IO;
using Org.Apache.Commons.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Security.Token;


namespace Org.Apache.Hadoop.Security
{
	/// <summary>
	/// A class that provides the facilities of reading and writing
	/// secret keys and Tokens.
	/// </summary>
	public class Credentials : IWritable
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Security.Credentials
			));

		private IDictionary<Text, byte[]> secretKeysMap = new Dictionary<Text, byte[]>();

		private IDictionary<Text, Org.Apache.Hadoop.Security.Token.Token<TokenIdentifier>
			> tokenMap = new Dictionary<Text, Org.Apache.Hadoop.Security.Token.Token<TokenIdentifier
			>>();

		/// <summary>Create an empty credentials instance</summary>
		public Credentials()
		{
		}

		/// <summary>Create a copy of the given credentials</summary>
		/// <param name="credentials">to copy</param>
		public Credentials(Org.Apache.Hadoop.Security.Credentials credentials)
		{
			this.AddAll(credentials);
		}

		/// <summary>Returns the Token object for the alias</summary>
		/// <param name="alias">the alias for the Token</param>
		/// <returns>token for this alias</returns>
		public virtual Org.Apache.Hadoop.Security.Token.Token<TokenIdentifier> GetToken(Text
			 alias)
		{
			return tokenMap[alias];
		}

		/// <summary>Add a token in the storage (in memory)</summary>
		/// <param name="alias">the alias for the key</param>
		/// <param name="t">the token object</param>
		public virtual void AddToken<_T0>(Text alias, Org.Apache.Hadoop.Security.Token.Token
			<_T0> t)
			where _T0 : TokenIdentifier
		{
			if (t != null)
			{
				tokenMap[alias] = t;
			}
			else
			{
				Log.Warn("Null token ignored for " + alias);
			}
		}

		/// <summary>Return all the tokens in the in-memory map</summary>
		public virtual ICollection<Org.Apache.Hadoop.Security.Token.Token<TokenIdentifier
			>> GetAllTokens()
		{
			return tokenMap.Values;
		}

		/// <returns>number of Tokens in the in-memory map</returns>
		public virtual int NumberOfTokens()
		{
			return tokenMap.Count;
		}

		/// <summary>Returns the key bytes for the alias</summary>
		/// <param name="alias">the alias for the key</param>
		/// <returns>key for this alias</returns>
		public virtual byte[] GetSecretKey(Text alias)
		{
			return secretKeysMap[alias];
		}

		/// <returns>number of keys in the in-memory map</returns>
		public virtual int NumberOfSecretKeys()
		{
			return secretKeysMap.Count;
		}

		/// <summary>Set the key for an alias</summary>
		/// <param name="alias">the alias for the key</param>
		/// <param name="key">the key bytes</param>
		public virtual void AddSecretKey(Text alias, byte[] key)
		{
			secretKeysMap[alias] = key;
		}

		/// <summary>Remove the key for a given alias.</summary>
		/// <param name="alias">the alias for the key</param>
		public virtual void RemoveSecretKey(Text alias)
		{
			Collections.Remove(secretKeysMap, alias);
		}

		/// <summary>Return all the secret key entries in the in-memory map</summary>
		public virtual IList<Text> GetAllSecretKeys()
		{
			IList<Text> list = new AList<Text>();
			Collections.AddAll(list, secretKeysMap.Keys);
			return list;
		}

		/// <summary>
		/// Convenience method for reading a token storage file, and loading the Tokens
		/// therein in the passed UGI
		/// </summary>
		/// <param name="filename"/>
		/// <param name="conf"/>
		/// <exception cref="System.IO.IOException"/>
		public static Org.Apache.Hadoop.Security.Credentials ReadTokenStorageFile(Path filename
			, Configuration conf)
		{
			FSDataInputStream @in = null;
			Org.Apache.Hadoop.Security.Credentials credentials = new Org.Apache.Hadoop.Security.Credentials
				();
			try
			{
				@in = filename.GetFileSystem(conf).Open(filename);
				credentials.ReadTokenStorageStream(@in);
				@in.Close();
				return credentials;
			}
			catch (IOException ioe)
			{
				throw new IOException("Exception reading " + filename, ioe);
			}
			finally
			{
				IOUtils.Cleanup(Log, @in);
			}
		}

		/// <summary>
		/// Convenience method for reading a token storage file, and loading the Tokens
		/// therein in the passed UGI
		/// </summary>
		/// <param name="filename"/>
		/// <param name="conf"/>
		/// <exception cref="System.IO.IOException"/>
		public static Org.Apache.Hadoop.Security.Credentials ReadTokenStorageFile(FilePath
			 filename, Configuration conf)
		{
			DataInputStream @in = null;
			Org.Apache.Hadoop.Security.Credentials credentials = new Org.Apache.Hadoop.Security.Credentials
				();
			try
			{
				@in = new DataInputStream(new BufferedInputStream(new FileInputStream(filename)));
				credentials.ReadTokenStorageStream(@in);
				return credentials;
			}
			catch (IOException ioe)
			{
				throw new IOException("Exception reading " + filename, ioe);
			}
			finally
			{
				IOUtils.Cleanup(Log, @in);
			}
		}

		/// <summary>
		/// Convenience method for reading a token storage file directly from a
		/// datainputstream
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void ReadTokenStorageStream(DataInputStream @in)
		{
			byte[] magic = new byte[TokenStorageMagic.Length];
			@in.ReadFully(magic);
			if (!Arrays.Equals(magic, TokenStorageMagic))
			{
				throw new IOException("Bad header found in token storage.");
			}
			byte version = @in.ReadByte();
			if (version != TokenStorageVersion)
			{
				throw new IOException("Unknown version " + version + " in token storage.");
			}
			ReadFields(@in);
		}

		private static readonly byte[] TokenStorageMagic = Runtime.GetBytesForString
			("HDTS", Charsets.Utf8);

		private const byte TokenStorageVersion = 0;

		/// <exception cref="System.IO.IOException"/>
		public virtual void WriteTokenStorageToStream(DataOutputStream os)
		{
			os.Write(TokenStorageMagic);
			os.Write(TokenStorageVersion);
			Write(os);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void WriteTokenStorageFile(Path filename, Configuration conf)
		{
			FSDataOutputStream os = filename.GetFileSystem(conf).Create(filename);
			WriteTokenStorageToStream(os);
			os.Close();
		}

		/// <summary>Stores all the keys to BinaryWriter</summary>
		/// <param name="out"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual void Write(BinaryWriter writer)
		{
			// write out tokens first
			WritableUtils.WriteVInt(@out, tokenMap.Count);
			foreach (KeyValuePair<Text, Org.Apache.Hadoop.Security.Token.Token<TokenIdentifier
				>> e in tokenMap)
			{
				e.Key.Write(@out);
				e.Value.Write(@out);
			}
			// now write out secret keys
			WritableUtils.WriteVInt(@out, secretKeysMap.Count);
			foreach (KeyValuePair<Text, byte[]> e_1 in secretKeysMap)
			{
				e_1.Key.Write(@out);
				WritableUtils.WriteVInt(@out, e_1.Value.Length);
				@out.Write(e_1.Value);
			}
		}

		/// <summary>Loads all the keys</summary>
		/// <param name="in"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual void ReadFields(BinaryReader reader)
		{
			secretKeysMap.Clear();
			tokenMap.Clear();
			int size = WritableUtils.ReadVInt(@in);
			for (int i = 0; i < size; i++)
			{
				Text alias = new Text();
				alias.ReadFields(@in);
				Org.Apache.Hadoop.Security.Token.Token<TokenIdentifier> t = new Org.Apache.Hadoop.Security.Token.Token
					<TokenIdentifier>();
				t.ReadFields(@in);
				tokenMap[alias] = t;
			}
			size = WritableUtils.ReadVInt(@in);
			for (int i_1 = 0; i_1 < size; i_1++)
			{
				Text alias = new Text();
				alias.ReadFields(@in);
				int len = WritableUtils.ReadVInt(@in);
				byte[] value = new byte[len];
				@in.ReadFully(value);
				secretKeysMap[alias] = value;
			}
		}

		/// <summary>Copy all of the credentials from one credential object into another.</summary>
		/// <remarks>
		/// Copy all of the credentials from one credential object into another.
		/// Existing secrets and tokens are overwritten.
		/// </remarks>
		/// <param name="other">the credentials to copy</param>
		public virtual void AddAll(Org.Apache.Hadoop.Security.Credentials other)
		{
			AddAll(other, true);
		}

		/// <summary>Copy all of the credentials from one credential object into another.</summary>
		/// <remarks>
		/// Copy all of the credentials from one credential object into another.
		/// Existing secrets and tokens are not overwritten.
		/// </remarks>
		/// <param name="other">the credentials to copy</param>
		public virtual void MergeAll(Org.Apache.Hadoop.Security.Credentials other)
		{
			AddAll(other, false);
		}

		private void AddAll(Org.Apache.Hadoop.Security.Credentials other, bool overwrite)
		{
			foreach (KeyValuePair<Text, byte[]> secret in other.secretKeysMap)
			{
				Text key = secret.Key;
				if (!secretKeysMap.Contains(key) || overwrite)
				{
					secretKeysMap[key] = secret.Value;
				}
			}
			foreach (KeyValuePair<Text, Org.Apache.Hadoop.Security.Token.Token<object>> token
				 in other.tokenMap)
			{
				Text key = token.Key;
				if (!tokenMap.Contains(key) || overwrite)
				{
					tokenMap[key] = token.Value;
				}
			}
		}
	}
}
