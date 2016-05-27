using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Com.Google.Gson.Stream;
using Org.Apache.Commons.IO;
using Org.Apache.Hadoop.Conf;
using Sharpen;

namespace Org.Apache.Hadoop.Crypto.Key
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
		public const string DefaultCipherName = "hadoop.security.key.default.cipher";

		public const string DefaultCipher = "AES/CTR/NoPadding";

		public const string DefaultBitlengthName = "hadoop.security.key.default.bitlength";

		public const int DefaultBitlength = 128;

		private readonly Configuration conf;

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

			public virtual string GetName()
			{
				return name;
			}

			public virtual string GetVersionName()
			{
				return versionName;
			}

			public virtual byte[] GetMaterial()
			{
				return material;
			}

			public override string ToString()
			{
				StringBuilder buf = new StringBuilder();
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
						buf.Append(Sharpen.Extensions.ToHexString(right));
					}
				}
				return buf.ToString();
			}
		}

		/// <summary>Key metadata that is associated with the key.</summary>
		public class Metadata
		{
			private const string CipherField = "cipher";

			private const string BitLengthField = "bitLength";

			private const string CreatedField = "created";

			private const string DescriptionField = "description";

			private const string VersionsField = "versions";

			private const string AttributesField = "attributes";

			private readonly string cipher;

			private readonly int bitLength;

			private readonly string description;

			private readonly DateTime created;

			private int versions;

			private IDictionary<string, string> attributes;

			protected internal Metadata(string cipher, int bitLength, string description, IDictionary
				<string, string> attributes, DateTime created, int versions)
			{
				this.cipher = cipher;
				this.bitLength = bitLength;
				this.description = description;
				this.attributes = (attributes == null || attributes.IsEmpty()) ? null : attributes;
				this.created = created;
				this.versions = versions;
			}

			public override string ToString()
			{
				StringBuilder metaSB = new StringBuilder();
				metaSB.Append("cipher: ").Append(cipher).Append(", ");
				metaSB.Append("length: ").Append(bitLength).Append(", ");
				metaSB.Append("description: ").Append(description).Append(", ");
				metaSB.Append("created: ").Append(created).Append(", ");
				metaSB.Append("version: ").Append(versions).Append(", ");
				metaSB.Append("attributes: ");
				if ((attributes != null) && !attributes.IsEmpty())
				{
					foreach (KeyValuePair<string, string> attribute in attributes)
					{
						metaSB.Append("[");
						metaSB.Append(attribute.Key);
						metaSB.Append("=");
						metaSB.Append(attribute.Value);
						metaSB.Append("], ");
					}
					Sharpen.Runtime.DeleteCharAt(metaSB, metaSB.Length - 2);
				}
				else
				{
					// remove last ', '
					metaSB.Append("null");
				}
				return metaSB.ToString();
			}

			public virtual string GetDescription()
			{
				return description;
			}

			public virtual DateTime GetCreated()
			{
				return created;
			}

			public virtual string GetCipher()
			{
				return cipher;
			}

			public virtual IDictionary<string, string> GetAttributes()
			{
				return (attributes == null) ? Sharpen.Collections.EmptyMap : attributes;
			}

			/// <summary>Get the algorithm from the cipher.</summary>
			/// <returns>the algorithm name</returns>
			public virtual string GetAlgorithm()
			{
				int slash = cipher.IndexOf('/');
				if (slash == -1)
				{
					return cipher;
				}
				else
				{
					return Sharpen.Runtime.Substring(cipher, 0, slash);
				}
			}

			public virtual int GetBitLength()
			{
				return bitLength;
			}

			public virtual int GetVersions()
			{
				return versions;
			}

			protected internal virtual int AddVersion()
			{
				return versions++;
			}

			/// <summary>Serialize the metadata to a set of bytes.</summary>
			/// <returns>the serialized bytes</returns>
			/// <exception cref="System.IO.IOException"/>
			protected internal virtual byte[] Serialize()
			{
				ByteArrayOutputStream buffer = new ByteArrayOutputStream();
				JsonWriter writer = new JsonWriter(new OutputStreamWriter(buffer, Charsets.Utf8));
				try
				{
					writer.BeginObject();
					if (cipher != null)
					{
						writer.Name(CipherField).Value(cipher);
					}
					if (bitLength != 0)
					{
						writer.Name(BitLengthField).Value(bitLength);
					}
					if (created != null)
					{
						writer.Name(CreatedField).Value(created.GetTime());
					}
					if (description != null)
					{
						writer.Name(DescriptionField).Value(description);
					}
					if (attributes != null && attributes.Count > 0)
					{
						writer.Name(AttributesField).BeginObject();
						foreach (KeyValuePair<string, string> attribute in attributes)
						{
							writer.Name(attribute.Key).Value(attribute.Value);
						}
						writer.EndObject();
					}
					writer.Name(VersionsField).Value(versions);
					writer.EndObject();
					writer.Flush();
				}
				finally
				{
					writer.Close();
				}
				return buffer.ToByteArray();
			}

			/// <summary>Deserialize a new metadata object from a set of bytes.</summary>
			/// <param name="bytes">the serialized metadata</param>
			/// <exception cref="System.IO.IOException"/>
			protected internal Metadata(byte[] bytes)
			{
				string cipher = null;
				int bitLength = 0;
				DateTime created = null;
				int versions = 0;
				string description = null;
				IDictionary<string, string> attributes = null;
				JsonReader reader = new JsonReader(new InputStreamReader(new ByteArrayInputStream
					(bytes), Charsets.Utf8));
				try
				{
					reader.BeginObject();
					while (reader.HasNext())
					{
						string field = reader.NextName();
						if (CipherField.Equals(field))
						{
							cipher = reader.NextString();
						}
						else
						{
							if (BitLengthField.Equals(field))
							{
								bitLength = reader.NextInt();
							}
							else
							{
								if (CreatedField.Equals(field))
								{
									created = Sharpen.Extensions.CreateDate(reader.NextLong());
								}
								else
								{
									if (VersionsField.Equals(field))
									{
										versions = reader.NextInt();
									}
									else
									{
										if (DescriptionField.Equals(field))
										{
											description = reader.NextString();
										}
										else
										{
											if (Sharpen.Runtime.EqualsIgnoreCase(AttributesField, field))
											{
												reader.BeginObject();
												attributes = new Dictionary<string, string>();
												while (reader.HasNext())
												{
													attributes[reader.NextName()] = reader.NextString();
												}
												reader.EndObject();
											}
										}
									}
								}
							}
						}
					}
					reader.EndObject();
				}
				finally
				{
					reader.Close();
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

			private IDictionary<string, string> attributes;

			public Options(Configuration conf)
			{
				cipher = conf.Get(DefaultCipherName, DefaultCipher);
				bitLength = conf.GetInt(DefaultBitlengthName, DefaultBitlength);
			}

			public virtual KeyProvider.Options SetCipher(string cipher)
			{
				this.cipher = cipher;
				return this;
			}

			public virtual KeyProvider.Options SetBitLength(int bitLength)
			{
				this.bitLength = bitLength;
				return this;
			}

			public virtual KeyProvider.Options SetDescription(string description)
			{
				this.description = description;
				return this;
			}

			public virtual KeyProvider.Options SetAttributes(IDictionary<string, string> attributes
				)
			{
				if (attributes != null)
				{
					if (attributes.Contains(null))
					{
						throw new ArgumentException("attributes cannot have a NULL key");
					}
					this.attributes = new Dictionary<string, string>(attributes);
				}
				return this;
			}

			public virtual string GetCipher()
			{
				return cipher;
			}

			public virtual int GetBitLength()
			{
				return bitLength;
			}

			public virtual string GetDescription()
			{
				return description;
			}

			public virtual IDictionary<string, string> GetAttributes()
			{
				return (attributes == null) ? Sharpen.Collections.EmptyMap : attributes;
			}

			public override string ToString()
			{
				return "Options{" + "cipher='" + cipher + '\'' + ", bitLength=" + bitLength + ", description='"
					 + description + '\'' + ", attributes=" + attributes + '}';
			}
		}

		/// <summary>Constructor.</summary>
		/// <param name="conf">configuration for the provider</param>
		public KeyProvider(Configuration conf)
		{
			this.conf = new Configuration(conf);
		}

		/// <summary>Return the provider configuration.</summary>
		/// <returns>the provider configuration</returns>
		public virtual Configuration GetConf()
		{
			return conf;
		}

		/// <summary>A helper function to create an options object.</summary>
		/// <param name="conf">the configuration to use</param>
		/// <returns>a new options object</returns>
		public static KeyProvider.Options Options(Configuration conf)
		{
			return new KeyProvider.Options(conf);
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
		public virtual bool IsTransient()
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
		public abstract KeyProvider.KeyVersion GetKeyVersion(string versionName);

		/// <summary>Get the key names for all keys.</summary>
		/// <returns>the list of key names</returns>
		/// <exception cref="System.IO.IOException"/>
		public abstract IList<string> GetKeys();

		/// <summary>Get key metadata in bulk.</summary>
		/// <param name="names">the names of the keys to get</param>
		/// <exception cref="System.IO.IOException"/>
		public virtual KeyProvider.Metadata[] GetKeysMetadata(params string[] names)
		{
			KeyProvider.Metadata[] result = new KeyProvider.Metadata[names.Length];
			for (int i = 0; i < names.Length; ++i)
			{
				result[i] = GetMetadata(names[i]);
			}
			return result;
		}

		/// <summary>Get the key material for all versions of a specific key name.</summary>
		/// <returns>the list of key material</returns>
		/// <exception cref="System.IO.IOException"/>
		public abstract IList<KeyProvider.KeyVersion> GetKeyVersions(string name);

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
		public virtual KeyProvider.KeyVersion GetCurrentKey(string name)
		{
			KeyProvider.Metadata meta = GetMetadata(name);
			if (meta == null)
			{
				return null;
			}
			return GetKeyVersion(BuildVersionName(name, meta.GetVersions() - 1));
		}

		/// <summary>Get metadata about the key.</summary>
		/// <param name="name">the basename of the key</param>
		/// <returns>the key's metadata or null if the key doesn't exist</returns>
		/// <exception cref="System.IO.IOException"/>
		public abstract KeyProvider.Metadata GetMetadata(string name);

		/// <summary>Create a new key.</summary>
		/// <remarks>Create a new key. The given key must not already exist.</remarks>
		/// <param name="name">the base name of the key</param>
		/// <param name="material">the key material for the first version of the key.</param>
		/// <param name="options">the options for the new key.</param>
		/// <returns>the version name of the first version of the key.</returns>
		/// <exception cref="System.IO.IOException"/>
		public abstract KeyProvider.KeyVersion CreateKey(string name, byte[] material, KeyProvider.Options
			 options);

		/// <summary>Get the algorithm from the cipher.</summary>
		/// <returns>the algorithm name</returns>
		private string GetAlgorithm(string cipher)
		{
			int slash = cipher.IndexOf('/');
			if (slash == -1)
			{
				return cipher;
			}
			else
			{
				return Sharpen.Runtime.Substring(cipher, 0, slash);
			}
		}

		/// <summary>Generates a key material.</summary>
		/// <param name="size">length of the key.</param>
		/// <param name="algorithm">algorithm to use for generating the key.</param>
		/// <returns>the generated key.</returns>
		/// <exception cref="Sharpen.NoSuchAlgorithmException"/>
		protected internal virtual byte[] GenerateKey(int size, string algorithm)
		{
			algorithm = GetAlgorithm(algorithm);
			KeyGenerator keyGenerator = KeyGenerator.GetInstance(algorithm);
			keyGenerator.Init(size);
			byte[] key = keyGenerator.GenerateKey().GetEncoded();
			return key;
		}

		/// <summary>Create a new key generating the material for it.</summary>
		/// <remarks>
		/// Create a new key generating the material for it.
		/// The given key must not already exist.
		/// <p/>
		/// This implementation generates the key material and calls the
		/// <see cref="CreateKey(string, byte[], Options)"/>
		/// method.
		/// </remarks>
		/// <param name="name">the base name of the key</param>
		/// <param name="options">the options for the new key.</param>
		/// <returns>the version name of the first version of the key.</returns>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Sharpen.NoSuchAlgorithmException"/>
		public virtual KeyProvider.KeyVersion CreateKey(string name, KeyProvider.Options 
			options)
		{
			byte[] material = GenerateKey(options.GetBitLength(), options.GetCipher());
			return CreateKey(name, material, options);
		}

		/// <summary>Delete the given key.</summary>
		/// <param name="name">the name of the key to delete</param>
		/// <exception cref="System.IO.IOException"/>
		public abstract void DeleteKey(string name);

		/// <summary>Roll a new version of the given key.</summary>
		/// <param name="name">the basename of the key</param>
		/// <param name="material">the new key material</param>
		/// <returns>the name of the new version of the key</returns>
		/// <exception cref="System.IO.IOException"/>
		public abstract KeyProvider.KeyVersion RollNewVersion(string name, byte[] material
			);

		/// <summary>
		/// Can be used by implementing classes to close any resources
		/// that require closing
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void Close()
		{
		}

		// NOP
		/// <summary>Roll a new version of the given key generating the material for it.</summary>
		/// <remarks>
		/// Roll a new version of the given key generating the material for it.
		/// <p/>
		/// This implementation generates the key material and calls the
		/// <see cref="RollNewVersion(string, byte[])"/>
		/// method.
		/// </remarks>
		/// <param name="name">the basename of the key</param>
		/// <returns>the name of the new version of the key</returns>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Sharpen.NoSuchAlgorithmException"/>
		public virtual KeyProvider.KeyVersion RollNewVersion(string name)
		{
			KeyProvider.Metadata meta = GetMetadata(name);
			byte[] material = GenerateKey(meta.GetBitLength(), meta.GetCipher());
			return RollNewVersion(name, material);
		}

		/// <summary>Ensures that any changes to the keys are written to persistent store.</summary>
		/// <exception cref="System.IO.IOException"/>
		public abstract void Flush();

		/// <summary>Split the versionName in to a base name.</summary>
		/// <remarks>
		/// Split the versionName in to a base name. Converts "/aaa/bbb/3" to
		/// "/aaa/bbb".
		/// </remarks>
		/// <param name="versionName">the version name to split</param>
		/// <returns>the base name of the key</returns>
		/// <exception cref="System.IO.IOException"/>
		public static string GetBaseName(string versionName)
		{
			int div = versionName.LastIndexOf('@');
			if (div == -1)
			{
				throw new IOException("No version in key path " + versionName);
			}
			return Sharpen.Runtime.Substring(versionName, 0, div);
		}

		/// <summary>Build a version string from a basename and version number.</summary>
		/// <remarks>
		/// Build a version string from a basename and version number. Converts
		/// "/aaa/bbb" and 3 to "/aaa/bbb@3".
		/// </remarks>
		/// <param name="name">the basename of the key</param>
		/// <param name="version">the version of the key</param>
		/// <returns>the versionName of the key.</returns>
		protected internal static string BuildVersionName(string name, int version)
		{
			return name + "@" + version;
		}

		/// <summary>Find the provider with the given key.</summary>
		/// <param name="providerList">the list of providers</param>
		/// <param name="keyName">the key name we are looking for</param>
		/// <returns>the KeyProvider that has the key</returns>
		/// <exception cref="System.IO.IOException"/>
		public static KeyProvider FindProvider(IList<KeyProvider> providerList, string keyName
			)
		{
			foreach (KeyProvider provider in providerList)
			{
				if (provider.GetMetadata(keyName) != null)
				{
					return provider;
				}
			}
			throw new IOException("Can't find KeyProvider for key " + keyName);
		}
	}
}
