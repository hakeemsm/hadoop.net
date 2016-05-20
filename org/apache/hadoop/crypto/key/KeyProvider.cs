using Sharpen;

namespace org.apache.hadoop.crypto.key
{
	/// <summary>A provider of secret key material for Hadoop applications.</summary>
	/// <remarks>
	/// A provider of secret key material for Hadoop applications. Provides an
	/// abstraction to separate key storage from users of encryption. It
	/// is intended to support getting or storing keys in a variety of ways,
	/// including third party bindings.
	/// <P/>
	/// <code>KeyProvider</code> implementations must be thread safe.
	/// </remarks>
	public abstract class KeyProvider
	{
		public const string DEFAULT_CIPHER_NAME = "hadoop.security.key.default.cipher";

		public const string DEFAULT_CIPHER = "AES/CTR/NoPadding";

		public const string DEFAULT_BITLENGTH_NAME = "hadoop.security.key.default.bitlength";

		public const int DEFAULT_BITLENGTH = 128;

		private readonly org.apache.hadoop.conf.Configuration conf;

		/// <summary>The combination of both the key version name and the key material.</summary>
		public class KeyVersion
		{
			private readonly string name;

			private readonly string versionName;

			private readonly byte[] material;

			protected internal KeyVersion(string name, string versionName, byte[] material)
			{
				this.name = name;
				this.versionName = versionName;
				this.material = material;
			}

			public virtual string getName()
			{
				return name;
			}

			public virtual string getVersionName()
			{
				return versionName;
			}

			public virtual byte[] getMaterial()
			{
				return material;
			}

			public override string ToString()
			{
				java.lang.StringBuilder buf = new java.lang.StringBuilder();
				buf.Append("key(");
				buf.Append(versionName);
				buf.Append(")=");
				if (material == null)
				{
					buf.Append("null");
				}
				else
				{
					foreach (byte b in material)
					{
						buf.Append(' ');
						int right = b & unchecked((int)(0xff));
						if (right < unchecked((int)(0x10)))
						{
							buf.Append('0');
						}
						buf.Append(int.toHexString(right));
					}
				}
				return buf.ToString();
			}
		}

		/// <summary>Key metadata that is associated with the key.</summary>
		public class Metadata
		{
			private const string CIPHER_FIELD = "cipher";

			private const string BIT_LENGTH_FIELD = "bitLength";

			private const string CREATED_FIELD = "created";

			private const string DESCRIPTION_FIELD = "description";

			private const string VERSIONS_FIELD = "versions";

			private const string ATTRIBUTES_FIELD = "attributes";

			private readonly string cipher;

			private readonly int bitLength;

			private readonly string description;

			private readonly System.DateTime created;

			private int versions;

			private System.Collections.Generic.IDictionary<string, string> attributes;

			protected internal Metadata(string cipher, int bitLength, string description, System.Collections.Generic.IDictionary
				<string, string> attributes, System.DateTime created, int versions)
			{
				this.cipher = cipher;
				this.bitLength = bitLength;
				this.description = description;
				this.attributes = (attributes == null || attributes.isEmpty()) ? null : attributes;
				this.created = created;
				this.versions = versions;
			}

			public override string ToString()
			{
				java.lang.StringBuilder metaSB = new java.lang.StringBuilder();
				metaSB.Append("cipher: ").Append(cipher).Append(", ");
				metaSB.Append("length: ").Append(bitLength).Append(", ");
				metaSB.Append("description: ").Append(description).Append(", ");
				metaSB.Append("created: ").Append(created).Append(", ");
				metaSB.Append("version: ").Append(versions).Append(", ");
				metaSB.Append("attributes: ");
				if ((attributes != null) && !attributes.isEmpty())
				{
					foreach (System.Collections.Generic.KeyValuePair<string, string> attribute in attributes)
					{
						metaSB.Append("[");
						metaSB.Append(attribute.Key);
						metaSB.Append("=");
						metaSB.Append(attribute.Value);
						metaSB.Append("], ");
					}
					Sharpen.Runtime.deleteCharAt(metaSB, metaSB.Length - 2);
				}
				else
				{
					// remove last ', '
					metaSB.Append("null");
				}
				return metaSB.ToString();
			}

			public virtual string getDescription()
			{
				return description;
			}

			public virtual System.DateTime getCreated()
			{
				return created;
			}

			public virtual string getCipher()
			{
				return cipher;
			}

			public virtual System.Collections.Generic.IDictionary<string, string> getAttributes
				()
			{
				return (attributes == null) ? java.util.Collections.EMPTY_MAP : attributes;
			}

			/// <summary>Get the algorithm from the cipher.</summary>
			/// <returns>the algorithm name</returns>
			public virtual string getAlgorithm()
			{
				int slash = cipher.IndexOf('/');
				if (slash == -1)
				{
					return cipher;
				}
				else
				{
					return Sharpen.Runtime.substring(cipher, 0, slash);
				}
			}

			public virtual int getBitLength()
			{
				return bitLength;
			}

			public virtual int getVersions()
			{
				return versions;
			}

			protected internal virtual int addVersion()
			{
				return versions++;
			}

			/// <summary>Serialize the metadata to a set of bytes.</summary>
			/// <returns>the serialized bytes</returns>
			/// <exception cref="System.IO.IOException"/>
			protected internal virtual byte[] serialize()
			{
				java.io.ByteArrayOutputStream buffer = new java.io.ByteArrayOutputStream();
				com.google.gson.stream.JsonWriter writer = new com.google.gson.stream.JsonWriter(
					new java.io.OutputStreamWriter(buffer, org.apache.commons.io.Charsets.UTF_8));
				try
				{
					writer.beginObject();
					if (cipher != null)
					{
						writer.name(CIPHER_FIELD).value(cipher);
					}
					if (bitLength != 0)
					{
						writer.name(BIT_LENGTH_FIELD).value(bitLength);
					}
					if (created != null)
					{
						writer.name(CREATED_FIELD).value(created.getTime());
					}
					if (description != null)
					{
						writer.name(DESCRIPTION_FIELD).value(description);
					}
					if (attributes != null && attributes.Count > 0)
					{
						writer.name(ATTRIBUTES_FIELD).beginObject();
						foreach (System.Collections.Generic.KeyValuePair<string, string> attribute in attributes)
						{
							writer.name(attribute.Key).value(attribute.Value);
						}
						writer.endObject();
					}
					writer.name(VERSIONS_FIELD).value(versions);
					writer.endObject();
					writer.flush();
				}
				finally
				{
					writer.close();
				}
				return buffer.toByteArray();
			}

			/// <summary>Deserialize a new metadata object from a set of bytes.</summary>
			/// <param name="bytes">the serialized metadata</param>
			/// <exception cref="System.IO.IOException"/>
			protected internal Metadata(byte[] bytes)
			{
				string cipher = null;
				int bitLength = 0;
				System.DateTime created = null;
				int versions = 0;
				string description = null;
				System.Collections.Generic.IDictionary<string, string> attributes = null;
				com.google.gson.stream.JsonReader reader = new com.google.gson.stream.JsonReader(
					new java.io.InputStreamReader(new java.io.ByteArrayInputStream(bytes), org.apache.commons.io.Charsets
					.UTF_8));
				try
				{
					reader.beginObject();
					while (reader.hasNext())
					{
						string field = reader.nextName();
						if (CIPHER_FIELD.Equals(field))
						{
							cipher = reader.nextString();
						}
						else
						{
							if (BIT_LENGTH_FIELD.Equals(field))
							{
								bitLength = reader.nextInt();
							}
							else
							{
								if (CREATED_FIELD.Equals(field))
								{
									created = new System.DateTime(reader.nextLong());
								}
								else
								{
									if (VERSIONS_FIELD.Equals(field))
									{
										versions = reader.nextInt();
									}
									else
									{
										if (DESCRIPTION_FIELD.Equals(field))
										{
											description = reader.nextString();
										}
										else
										{
											if (Sharpen.Runtime.equalsIgnoreCase(ATTRIBUTES_FIELD, field))
											{
												reader.beginObject();
												attributes = new System.Collections.Generic.Dictionary<string, string>();
												while (reader.hasNext())
												{
													attributes[reader.nextName()] = reader.nextString();
												}
												reader.endObject();
											}
										}
									}
								}
							}
						}
					}
					reader.endObject();
				}
				finally
				{
					reader.close();
				}
				this.cipher = cipher;
				this.bitLength = bitLength;
				this.created = created;
				this.description = description;
				this.attributes = attributes;
				this.versions = versions;
			}
		}

		/// <summary>Options when creating key objects.</summary>
		public class Options
		{
			private string cipher;

			private int bitLength;

			private string description;

			private System.Collections.Generic.IDictionary<string, string> attributes;

			public Options(org.apache.hadoop.conf.Configuration conf)
			{
				cipher = conf.get(DEFAULT_CIPHER_NAME, DEFAULT_CIPHER);
				bitLength = conf.getInt(DEFAULT_BITLENGTH_NAME, DEFAULT_BITLENGTH);
			}

			public virtual org.apache.hadoop.crypto.key.KeyProvider.Options setCipher(string 
				cipher)
			{
				this.cipher = cipher;
				return this;
			}

			public virtual org.apache.hadoop.crypto.key.KeyProvider.Options setBitLength(int 
				bitLength)
			{
				this.bitLength = bitLength;
				return this;
			}

			public virtual org.apache.hadoop.crypto.key.KeyProvider.Options setDescription(string
				 description)
			{
				this.description = description;
				return this;
			}

			public virtual org.apache.hadoop.crypto.key.KeyProvider.Options setAttributes(System.Collections.Generic.IDictionary
				<string, string> attributes)
			{
				if (attributes != null)
				{
					if (attributes.Contains(null))
					{
						throw new System.ArgumentException("attributes cannot have a NULL key");
					}
					this.attributes = new System.Collections.Generic.Dictionary<string, string>(attributes
						);
				}
				return this;
			}

			public virtual string getCipher()
			{
				return cipher;
			}

			public virtual int getBitLength()
			{
				return bitLength;
			}

			public virtual string getDescription()
			{
				return description;
			}

			public virtual System.Collections.Generic.IDictionary<string, string> getAttributes
				()
			{
				return (attributes == null) ? java.util.Collections.EMPTY_MAP : attributes;
			}

			public override string ToString()
			{
				return "Options{" + "cipher='" + cipher + '\'' + ", bitLength=" + bitLength + ", description='"
					 + description + '\'' + ", attributes=" + attributes + '}';
			}
		}

		/// <summary>Constructor.</summary>
		/// <param name="conf">configuration for the provider</param>
		public KeyProvider(org.apache.hadoop.conf.Configuration conf)
		{
			this.conf = new org.apache.hadoop.conf.Configuration(conf);
		}

		/// <summary>Return the provider configuration.</summary>
		/// <returns>the provider configuration</returns>
		public virtual org.apache.hadoop.conf.Configuration getConf()
		{
			return conf;
		}

		/// <summary>A helper function to create an options object.</summary>
		/// <param name="conf">the configuration to use</param>
		/// <returns>a new options object</returns>
		public static org.apache.hadoop.crypto.key.KeyProvider.Options options(org.apache.hadoop.conf.Configuration
			 conf)
		{
			return new org.apache.hadoop.crypto.key.KeyProvider.Options(conf);
		}

		/// <summary>
		/// Indicates whether this provider represents a store
		/// that is intended for transient use - such as the UserProvider
		/// is.
		/// </summary>
		/// <remarks>
		/// Indicates whether this provider represents a store
		/// that is intended for transient use - such as the UserProvider
		/// is. These providers are generally used to provide access to
		/// keying material rather than for long term storage.
		/// </remarks>
		/// <returns>true if transient, false otherwise</returns>
		public virtual bool isTransient()
		{
			return false;
		}

		/// <summary>Get the key material for a specific version of the key.</summary>
		/// <remarks>
		/// Get the key material for a specific version of the key. This method is used
		/// when decrypting data.
		/// </remarks>
		/// <param name="versionName">the name of a specific version of the key</param>
		/// <returns>the key material</returns>
		/// <exception cref="System.IO.IOException"/>
		public abstract org.apache.hadoop.crypto.key.KeyProvider.KeyVersion getKeyVersion
			(string versionName);

		/// <summary>Get the key names for all keys.</summary>
		/// <returns>the list of key names</returns>
		/// <exception cref="System.IO.IOException"/>
		public abstract System.Collections.Generic.IList<string> getKeys();

		/// <summary>Get key metadata in bulk.</summary>
		/// <param name="names">the names of the keys to get</param>
		/// <exception cref="System.IO.IOException"/>
		public virtual org.apache.hadoop.crypto.key.KeyProvider.Metadata[] getKeysMetadata
			(params string[] names)
		{
			org.apache.hadoop.crypto.key.KeyProvider.Metadata[] result = new org.apache.hadoop.crypto.key.KeyProvider.Metadata
				[names.Length];
			for (int i = 0; i < names.Length; ++i)
			{
				result[i] = getMetadata(names[i]);
			}
			return result;
		}

		/// <summary>Get the key material for all versions of a specific key name.</summary>
		/// <returns>the list of key material</returns>
		/// <exception cref="System.IO.IOException"/>
		public abstract System.Collections.Generic.IList<org.apache.hadoop.crypto.key.KeyProvider.KeyVersion
			> getKeyVersions(string name);

		/// <summary>
		/// Get the current version of the key, which should be used for encrypting new
		/// data.
		/// </summary>
		/// <param name="name">the base name of the key</param>
		/// <returns>
		/// the version name of the current version of the key or null if the
		/// key version doesn't exist
		/// </returns>
		/// <exception cref="System.IO.IOException"/>
		public virtual org.apache.hadoop.crypto.key.KeyProvider.KeyVersion getCurrentKey(
			string name)
		{
			org.apache.hadoop.crypto.key.KeyProvider.Metadata meta = getMetadata(name);
			if (meta == null)
			{
				return null;
			}
			return getKeyVersion(buildVersionName(name, meta.getVersions() - 1));
		}

		/// <summary>Get metadata about the key.</summary>
		/// <param name="name">the basename of the key</param>
		/// <returns>the key's metadata or null if the key doesn't exist</returns>
		/// <exception cref="System.IO.IOException"/>
		public abstract org.apache.hadoop.crypto.key.KeyProvider.Metadata getMetadata(string
			 name);

		/// <summary>Create a new key.</summary>
		/// <remarks>Create a new key. The given key must not already exist.</remarks>
		/// <param name="name">the base name of the key</param>
		/// <param name="material">the key material for the first version of the key.</param>
		/// <param name="options">the options for the new key.</param>
		/// <returns>the version name of the first version of the key.</returns>
		/// <exception cref="System.IO.IOException"/>
		public abstract org.apache.hadoop.crypto.key.KeyProvider.KeyVersion createKey(string
			 name, byte[] material, org.apache.hadoop.crypto.key.KeyProvider.Options options
			);

		/// <summary>Get the algorithm from the cipher.</summary>
		/// <returns>the algorithm name</returns>
		private string getAlgorithm(string cipher)
		{
			int slash = cipher.IndexOf('/');
			if (slash == -1)
			{
				return cipher;
			}
			else
			{
				return Sharpen.Runtime.substring(cipher, 0, slash);
			}
		}

		/// <summary>Generates a key material.</summary>
		/// <param name="size">length of the key.</param>
		/// <param name="algorithm">algorithm to use for generating the key.</param>
		/// <returns>the generated key.</returns>
		/// <exception cref="java.security.NoSuchAlgorithmException"/>
		protected internal virtual byte[] generateKey(int size, string algorithm)
		{
			algorithm = getAlgorithm(algorithm);
			javax.crypto.KeyGenerator keyGenerator = javax.crypto.KeyGenerator.getInstance(algorithm
				);
			keyGenerator.init(size);
			byte[] key = keyGenerator.generateKey().getEncoded();
			return key;
		}

		/// <summary>Create a new key generating the material for it.</summary>
		/// <remarks>
		/// Create a new key generating the material for it.
		/// The given key must not already exist.
		/// <p/>
		/// This implementation generates the key material and calls the
		/// <see cref="createKey(string, byte[], Options)"/>
		/// method.
		/// </remarks>
		/// <param name="name">the base name of the key</param>
		/// <param name="options">the options for the new key.</param>
		/// <returns>the version name of the first version of the key.</returns>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="java.security.NoSuchAlgorithmException"/>
		public virtual org.apache.hadoop.crypto.key.KeyProvider.KeyVersion createKey(string
			 name, org.apache.hadoop.crypto.key.KeyProvider.Options options)
		{
			byte[] material = generateKey(options.getBitLength(), options.getCipher());
			return createKey(name, material, options);
		}

		/// <summary>Delete the given key.</summary>
		/// <param name="name">the name of the key to delete</param>
		/// <exception cref="System.IO.IOException"/>
		public abstract void deleteKey(string name);

		/// <summary>Roll a new version of the given key.</summary>
		/// <param name="name">the basename of the key</param>
		/// <param name="material">the new key material</param>
		/// <returns>the name of the new version of the key</returns>
		/// <exception cref="System.IO.IOException"/>
		public abstract org.apache.hadoop.crypto.key.KeyProvider.KeyVersion rollNewVersion
			(string name, byte[] material);

		/// <summary>
		/// Can be used by implementing classes to close any resources
		/// that require closing
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void close()
		{
		}

		// NOP
		/// <summary>Roll a new version of the given key generating the material for it.</summary>
		/// <remarks>
		/// Roll a new version of the given key generating the material for it.
		/// <p/>
		/// This implementation generates the key material and calls the
		/// <see cref="rollNewVersion(string, byte[])"/>
		/// method.
		/// </remarks>
		/// <param name="name">the basename of the key</param>
		/// <returns>the name of the new version of the key</returns>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="java.security.NoSuchAlgorithmException"/>
		public virtual org.apache.hadoop.crypto.key.KeyProvider.KeyVersion rollNewVersion
			(string name)
		{
			org.apache.hadoop.crypto.key.KeyProvider.Metadata meta = getMetadata(name);
			byte[] material = generateKey(meta.getBitLength(), meta.getCipher());
			return rollNewVersion(name, material);
		}

		/// <summary>Ensures that any changes to the keys are written to persistent store.</summary>
		/// <exception cref="System.IO.IOException"/>
		public abstract void flush();

		/// <summary>Split the versionName in to a base name.</summary>
		/// <remarks>
		/// Split the versionName in to a base name. Converts "/aaa/bbb/3" to
		/// "/aaa/bbb".
		/// </remarks>
		/// <param name="versionName">the version name to split</param>
		/// <returns>the base name of the key</returns>
		/// <exception cref="System.IO.IOException"/>
		public static string getBaseName(string versionName)
		{
			int div = versionName.LastIndexOf('@');
			if (div == -1)
			{
				throw new System.IO.IOException("No version in key path " + versionName);
			}
			return Sharpen.Runtime.substring(versionName, 0, div);
		}

		/// <summary>Build a version string from a basename and version number.</summary>
		/// <remarks>
		/// Build a version string from a basename and version number. Converts
		/// "/aaa/bbb" and 3 to "/aaa/bbb@3".
		/// </remarks>
		/// <param name="name">the basename of the key</param>
		/// <param name="version">the version of the key</param>
		/// <returns>the versionName of the key.</returns>
		protected internal static string buildVersionName(string name, int version)
		{
			return name + "@" + version;
		}

		/// <summary>Find the provider with the given key.</summary>
		/// <param name="providerList">the list of providers</param>
		/// <param name="keyName">the key name we are looking for</param>
		/// <returns>the KeyProvider that has the key</returns>
		/// <exception cref="System.IO.IOException"/>
		public static org.apache.hadoop.crypto.key.KeyProvider findProvider(System.Collections.Generic.IList
			<org.apache.hadoop.crypto.key.KeyProvider> providerList, string keyName)
		{
			foreach (org.apache.hadoop.crypto.key.KeyProvider provider in providerList)
			{
				if (provider.getMetadata(keyName) != null)
				{
					return provider;
				}
			}
			throw new System.IO.IOException("Can't find KeyProvider for key " + keyName);
		}
	}
}
