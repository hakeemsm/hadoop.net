using Sharpen;

namespace org.apache.hadoop.security
{
	/// <summary>
	/// A class that provides the facilities of reading and writing
	/// secret keys and Tokens.
	/// </summary>
	public class Credentials : org.apache.hadoop.io.Writable
	{
		private static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
			.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.security.Credentials
			)));

		private System.Collections.Generic.IDictionary<org.apache.hadoop.io.Text, byte[]>
			 secretKeysMap = new System.Collections.Generic.Dictionary<org.apache.hadoop.io.Text
			, byte[]>();

		private System.Collections.Generic.IDictionary<org.apache.hadoop.io.Text, org.apache.hadoop.security.token.Token
			<org.apache.hadoop.security.token.TokenIdentifier>> tokenMap = new System.Collections.Generic.Dictionary
			<org.apache.hadoop.io.Text, org.apache.hadoop.security.token.Token<org.apache.hadoop.security.token.TokenIdentifier
			>>();

		/// <summary>Create an empty credentials instance</summary>
		public Credentials()
		{
		}

		/// <summary>Create a copy of the given credentials</summary>
		/// <param name="credentials">to copy</param>
		public Credentials(org.apache.hadoop.security.Credentials credentials)
		{
			this.addAll(credentials);
		}

		/// <summary>Returns the Token object for the alias</summary>
		/// <param name="alias">the alias for the Token</param>
		/// <returns>token for this alias</returns>
		public virtual org.apache.hadoop.security.token.Token<org.apache.hadoop.security.token.TokenIdentifier
			> getToken(org.apache.hadoop.io.Text alias)
		{
			return tokenMap[alias];
		}

		/// <summary>Add a token in the storage (in memory)</summary>
		/// <param name="alias">the alias for the key</param>
		/// <param name="t">the token object</param>
		public virtual void addToken<_T0>(org.apache.hadoop.io.Text alias, org.apache.hadoop.security.token.Token
			<_T0> t)
			where _T0 : org.apache.hadoop.security.token.TokenIdentifier
		{
			if (t != null)
			{
				tokenMap[alias] = t;
			}
			else
			{
				LOG.warn("Null token ignored for " + alias);
			}
		}

		/// <summary>Return all the tokens in the in-memory map</summary>
		public virtual System.Collections.Generic.ICollection<org.apache.hadoop.security.token.Token
			<org.apache.hadoop.security.token.TokenIdentifier>> getAllTokens()
		{
			return tokenMap.Values;
		}

		/// <returns>number of Tokens in the in-memory map</returns>
		public virtual int numberOfTokens()
		{
			return tokenMap.Count;
		}

		/// <summary>Returns the key bytes for the alias</summary>
		/// <param name="alias">the alias for the key</param>
		/// <returns>key for this alias</returns>
		public virtual byte[] getSecretKey(org.apache.hadoop.io.Text alias)
		{
			return secretKeysMap[alias];
		}

		/// <returns>number of keys in the in-memory map</returns>
		public virtual int numberOfSecretKeys()
		{
			return secretKeysMap.Count;
		}

		/// <summary>Set the key for an alias</summary>
		/// <param name="alias">the alias for the key</param>
		/// <param name="key">the key bytes</param>
		public virtual void addSecretKey(org.apache.hadoop.io.Text alias, byte[] key)
		{
			secretKeysMap[alias] = key;
		}

		/// <summary>Remove the key for a given alias.</summary>
		/// <param name="alias">the alias for the key</param>
		public virtual void removeSecretKey(org.apache.hadoop.io.Text alias)
		{
			Sharpen.Collections.Remove(secretKeysMap, alias);
		}

		/// <summary>Return all the secret key entries in the in-memory map</summary>
		public virtual System.Collections.Generic.IList<org.apache.hadoop.io.Text> getAllSecretKeys
			()
		{
			System.Collections.Generic.IList<org.apache.hadoop.io.Text> list = new System.Collections.Generic.List
				<org.apache.hadoop.io.Text>();
			Sharpen.Collections.AddAll(list, secretKeysMap.Keys);
			return list;
		}

		/// <summary>
		/// Convenience method for reading a token storage file, and loading the Tokens
		/// therein in the passed UGI
		/// </summary>
		/// <param name="filename"/>
		/// <param name="conf"/>
		/// <exception cref="System.IO.IOException"/>
		public static org.apache.hadoop.security.Credentials readTokenStorageFile(org.apache.hadoop.fs.Path
			 filename, org.apache.hadoop.conf.Configuration conf)
		{
			org.apache.hadoop.fs.FSDataInputStream @in = null;
			org.apache.hadoop.security.Credentials credentials = new org.apache.hadoop.security.Credentials
				();
			try
			{
				@in = filename.getFileSystem(conf).open(filename);
				credentials.readTokenStorageStream(@in);
				@in.close();
				return credentials;
			}
			catch (System.IO.IOException ioe)
			{
				throw new System.IO.IOException("Exception reading " + filename, ioe);
			}
			finally
			{
				org.apache.hadoop.io.IOUtils.cleanup(LOG, @in);
			}
		}

		/// <summary>
		/// Convenience method for reading a token storage file, and loading the Tokens
		/// therein in the passed UGI
		/// </summary>
		/// <param name="filename"/>
		/// <param name="conf"/>
		/// <exception cref="System.IO.IOException"/>
		public static org.apache.hadoop.security.Credentials readTokenStorageFile(java.io.File
			 filename, org.apache.hadoop.conf.Configuration conf)
		{
			java.io.DataInputStream @in = null;
			org.apache.hadoop.security.Credentials credentials = new org.apache.hadoop.security.Credentials
				();
			try
			{
				@in = new java.io.DataInputStream(new java.io.BufferedInputStream(new java.io.FileInputStream
					(filename)));
				credentials.readTokenStorageStream(@in);
				return credentials;
			}
			catch (System.IO.IOException ioe)
			{
				throw new System.IO.IOException("Exception reading " + filename, ioe);
			}
			finally
			{
				org.apache.hadoop.io.IOUtils.cleanup(LOG, @in);
			}
		}

		/// <summary>
		/// Convenience method for reading a token storage file directly from a
		/// datainputstream
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void readTokenStorageStream(java.io.DataInputStream @in)
		{
			byte[] magic = new byte[TOKEN_STORAGE_MAGIC.Length];
			@in.readFully(magic);
			if (!java.util.Arrays.equals(magic, TOKEN_STORAGE_MAGIC))
			{
				throw new System.IO.IOException("Bad header found in token storage.");
			}
			byte version = @in.readByte();
			if (version != TOKEN_STORAGE_VERSION)
			{
				throw new System.IO.IOException("Unknown version " + version + " in token storage."
					);
			}
			readFields(@in);
		}

		private static readonly byte[] TOKEN_STORAGE_MAGIC = Sharpen.Runtime.getBytesForString
			("HDTS", org.apache.commons.io.Charsets.UTF_8);

		private const byte TOKEN_STORAGE_VERSION = 0;

		/// <exception cref="System.IO.IOException"/>
		public virtual void writeTokenStorageToStream(java.io.DataOutputStream os)
		{
			os.write(TOKEN_STORAGE_MAGIC);
			os.write(TOKEN_STORAGE_VERSION);
			write(os);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void writeTokenStorageFile(org.apache.hadoop.fs.Path filename, org.apache.hadoop.conf.Configuration
			 conf)
		{
			org.apache.hadoop.fs.FSDataOutputStream os = filename.getFileSystem(conf).create(
				filename);
			writeTokenStorageToStream(os);
			os.close();
		}

		/// <summary>Stores all the keys to DataOutput</summary>
		/// <param name="out"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual void write(java.io.DataOutput @out)
		{
			// write out tokens first
			org.apache.hadoop.io.WritableUtils.writeVInt(@out, tokenMap.Count);
			foreach (System.Collections.Generic.KeyValuePair<org.apache.hadoop.io.Text, org.apache.hadoop.security.token.Token
				<org.apache.hadoop.security.token.TokenIdentifier>> e in tokenMap)
			{
				e.Key.write(@out);
				e.Value.write(@out);
			}
			// now write out secret keys
			org.apache.hadoop.io.WritableUtils.writeVInt(@out, secretKeysMap.Count);
			foreach (System.Collections.Generic.KeyValuePair<org.apache.hadoop.io.Text, byte[]
				> e_1 in secretKeysMap)
			{
				e_1.Key.write(@out);
				org.apache.hadoop.io.WritableUtils.writeVInt(@out, e_1.Value.Length);
				@out.write(e_1.Value);
			}
		}

		/// <summary>Loads all the keys</summary>
		/// <param name="in"/>
		/// <exception cref="System.IO.IOException"/>
		public virtual void readFields(java.io.DataInput @in)
		{
			secretKeysMap.clear();
			tokenMap.clear();
			int size = org.apache.hadoop.io.WritableUtils.readVInt(@in);
			for (int i = 0; i < size; i++)
			{
				org.apache.hadoop.io.Text alias = new org.apache.hadoop.io.Text();
				alias.readFields(@in);
				org.apache.hadoop.security.token.Token<org.apache.hadoop.security.token.TokenIdentifier
					> t = new org.apache.hadoop.security.token.Token<org.apache.hadoop.security.token.TokenIdentifier
					>();
				t.readFields(@in);
				tokenMap[alias] = t;
			}
			size = org.apache.hadoop.io.WritableUtils.readVInt(@in);
			for (int i_1 = 0; i_1 < size; i_1++)
			{
				org.apache.hadoop.io.Text alias = new org.apache.hadoop.io.Text();
				alias.readFields(@in);
				int len = org.apache.hadoop.io.WritableUtils.readVInt(@in);
				byte[] value = new byte[len];
				@in.readFully(value);
				secretKeysMap[alias] = value;
			}
		}

		/// <summary>Copy all of the credentials from one credential object into another.</summary>
		/// <remarks>
		/// Copy all of the credentials from one credential object into another.
		/// Existing secrets and tokens are overwritten.
		/// </remarks>
		/// <param name="other">the credentials to copy</param>
		public virtual void addAll(org.apache.hadoop.security.Credentials other)
		{
			addAll(other, true);
		}

		/// <summary>Copy all of the credentials from one credential object into another.</summary>
		/// <remarks>
		/// Copy all of the credentials from one credential object into another.
		/// Existing secrets and tokens are not overwritten.
		/// </remarks>
		/// <param name="other">the credentials to copy</param>
		public virtual void mergeAll(org.apache.hadoop.security.Credentials other)
		{
			addAll(other, false);
		}

		private void addAll(org.apache.hadoop.security.Credentials other, bool overwrite)
		{
			foreach (System.Collections.Generic.KeyValuePair<org.apache.hadoop.io.Text, byte[]
				> secret in other.secretKeysMap)
			{
				org.apache.hadoop.io.Text key = secret.Key;
				if (!secretKeysMap.Contains(key) || overwrite)
				{
					secretKeysMap[key] = secret.Value;
				}
			}
			foreach (System.Collections.Generic.KeyValuePair<org.apache.hadoop.io.Text, org.apache.hadoop.security.token.Token
				<object>> token in other.tokenMap)
			{
				org.apache.hadoop.io.Text key = token.Key;
				if (!tokenMap.Contains(key) || overwrite)
				{
					tokenMap[key] = token.Value;
				}
			}
		}
	}
}
