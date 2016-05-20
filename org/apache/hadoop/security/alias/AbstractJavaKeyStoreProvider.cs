using Sharpen;

namespace org.apache.hadoop.security.alias
{
	/// <summary>
	/// Abstract class for implementing credential providers that are based on
	/// Java Keystores as the underlying credential store.
	/// </summary>
	/// <remarks>
	/// Abstract class for implementing credential providers that are based on
	/// Java Keystores as the underlying credential store.
	/// The password for the keystore is taken from the HADOOP_CREDSTORE_PASSWORD
	/// environment variable with a default of 'none'.
	/// It is expected that for access to credential protected resource to copy the
	/// creds from the original provider into the job's Credentials object, which is
	/// accessed via the UserProvider. Therefore, these providers won't be directly
	/// used by MapReduce tasks.
	/// </remarks>
	public abstract class AbstractJavaKeyStoreProvider : org.apache.hadoop.security.alias.CredentialProvider
	{
		public const string CREDENTIAL_PASSWORD_NAME = "HADOOP_CREDSTORE_PASSWORD";

		public const string KEYSTORE_PASSWORD_FILE_KEY = "hadoop.security.credstore.java-keystore-provider.password-file";

		public const string KEYSTORE_PASSWORD_DEFAULT = "none";

		private org.apache.hadoop.fs.Path path;

		private readonly java.net.URI uri;

		private readonly java.security.KeyStore keyStore;

		private char[] password = null;

		private bool changed = false;

		private java.util.concurrent.locks.Lock readLock;

		private java.util.concurrent.locks.Lock writeLock;

		/// <exception cref="System.IO.IOException"/>
		protected internal AbstractJavaKeyStoreProvider(java.net.URI uri, org.apache.hadoop.conf.Configuration
			 conf)
		{
			this.uri = uri;
			initFileSystem(uri, conf);
			// Get the password from the user's environment
			if (Sharpen.Runtime.getenv().Contains(CREDENTIAL_PASSWORD_NAME))
			{
				password = Sharpen.Runtime.getenv(CREDENTIAL_PASSWORD_NAME).ToCharArray();
			}
			// if not in ENV get check for file
			if (password == null)
			{
				string pwFile = conf.get(KEYSTORE_PASSWORD_FILE_KEY);
				if (pwFile != null)
				{
					java.lang.ClassLoader cl = java.lang.Thread.currentThread().getContextClassLoader
						();
					java.net.URL pwdFile = cl.getResource(pwFile);
					if (pwdFile != null)
					{
						using (java.io.InputStream @is = pwdFile.openStream())
						{
							password = org.apache.commons.io.IOUtils.toString(@is).Trim().ToCharArray();
						}
					}
				}
			}
			if (password == null)
			{
				password = KEYSTORE_PASSWORD_DEFAULT.ToCharArray();
			}
			try
			{
				keyStore = java.security.KeyStore.getInstance("jceks");
				if (keystoreExists())
				{
					stashOriginalFilePermissions();
					using (java.io.InputStream @in = getInputStreamForFile())
					{
						keyStore.load(@in, password);
					}
				}
				else
				{
					createPermissions("700");
					// required to create an empty keystore. *sigh*
					keyStore.load(null, password);
				}
			}
			catch (java.security.KeyStoreException e)
			{
				throw new System.IO.IOException("Can't create keystore", e);
			}
			catch (java.security.NoSuchAlgorithmException e)
			{
				throw new System.IO.IOException("Can't load keystore " + getPathAsString(), e);
			}
			catch (java.security.cert.CertificateException e)
			{
				throw new System.IO.IOException("Can't load keystore " + getPathAsString(), e);
			}
			java.util.concurrent.locks.ReadWriteLock Lock = new java.util.concurrent.locks.ReentrantReadWriteLock
				(true);
			readLock = Lock.readLock();
			writeLock = Lock.writeLock();
		}

		public virtual org.apache.hadoop.fs.Path getPath()
		{
			return path;
		}

		public virtual void setPath(org.apache.hadoop.fs.Path p)
		{
			this.path = p;
		}

		public virtual char[] getPassword()
		{
			return password;
		}

		public virtual void setPassword(char[] pass)
		{
			this.password = pass;
		}

		public virtual bool isChanged()
		{
			return changed;
		}

		public virtual void setChanged(bool chg)
		{
			this.changed = chg;
		}

		public virtual java.util.concurrent.locks.Lock getReadLock()
		{
			return readLock;
		}

		public virtual void setReadLock(java.util.concurrent.locks.Lock rl)
		{
			this.readLock = rl;
		}

		public virtual java.util.concurrent.locks.Lock getWriteLock()
		{
			return writeLock;
		}

		public virtual void setWriteLock(java.util.concurrent.locks.Lock wl)
		{
			this.writeLock = wl;
		}

		public virtual java.net.URI getUri()
		{
			return uri;
		}

		public virtual java.security.KeyStore getKeyStore()
		{
			return keyStore;
		}

		public virtual System.Collections.Generic.IDictionary<string, org.apache.hadoop.security.alias.CredentialProvider.CredentialEntry
			> getCache()
		{
			return cache;
		}

		private readonly System.Collections.Generic.IDictionary<string, org.apache.hadoop.security.alias.CredentialProvider.CredentialEntry
			> cache = new System.Collections.Generic.Dictionary<string, org.apache.hadoop.security.alias.CredentialProvider.CredentialEntry
			>();

		protected internal string getPathAsString()
		{
			return getPath().ToString();
		}

		protected internal abstract string getSchemeName();

		/// <exception cref="System.IO.IOException"/>
		protected internal abstract java.io.OutputStream getOutputStreamForKeystore();

		/// <exception cref="System.IO.IOException"/>
		protected internal abstract bool keystoreExists();

		/// <exception cref="System.IO.IOException"/>
		protected internal abstract java.io.InputStream getInputStreamForFile();

		/// <exception cref="System.IO.IOException"/>
		protected internal abstract void createPermissions(string perms);

		/// <exception cref="System.IO.IOException"/>
		protected internal abstract void stashOriginalFilePermissions();

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual void initFileSystem(java.net.URI keystoreUri, org.apache.hadoop.conf.Configuration
			 conf)
		{
			path = org.apache.hadoop.security.ProviderUtils.unnestUri(keystoreUri);
		}

		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.security.alias.CredentialProvider.CredentialEntry
			 getCredentialEntry(string alias)
		{
			readLock.Lock();
			try
			{
				javax.crypto.spec.SecretKeySpec key = null;
				try
				{
					if (cache.Contains(alias))
					{
						return cache[alias];
					}
					if (!keyStore.containsAlias(alias))
					{
						return null;
					}
					key = (javax.crypto.spec.SecretKeySpec)keyStore.getKey(alias, password);
				}
				catch (java.security.KeyStoreException e)
				{
					throw new System.IO.IOException("Can't get credential " + alias + " from " + getPathAsString
						(), e);
				}
				catch (java.security.NoSuchAlgorithmException e)
				{
					throw new System.IO.IOException("Can't get algorithm for credential " + alias + " from "
						 + getPathAsString(), e);
				}
				catch (java.security.UnrecoverableKeyException e)
				{
					throw new System.IO.IOException("Can't recover credential " + alias + " from " + 
						getPathAsString(), e);
				}
				return new org.apache.hadoop.security.alias.CredentialProvider.CredentialEntry(alias
					, bytesToChars(key.getEncoded()));
			}
			finally
			{
				readLock.unlock();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public static char[] bytesToChars(byte[] bytes)
		{
			string pass;
			pass = new string(bytes, com.google.common.@base.Charsets.UTF_8);
			return pass.ToCharArray();
		}

		/// <exception cref="System.IO.IOException"/>
		public override System.Collections.Generic.IList<string> getAliases()
		{
			readLock.Lock();
			try
			{
				System.Collections.Generic.List<string> list = new System.Collections.Generic.List
					<string>();
				string alias = null;
				try
				{
					java.util.Enumeration<string> e = keyStore.aliases();
					while (e.MoveNext())
					{
						alias = e.Current;
						list.add(alias);
					}
				}
				catch (java.security.KeyStoreException e)
				{
					throw new System.IO.IOException("Can't get alias " + alias + " from " + getPathAsString
						(), e);
				}
				return list;
			}
			finally
			{
				readLock.unlock();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.security.alias.CredentialProvider.CredentialEntry
			 createCredentialEntry(string alias, char[] credential)
		{
			writeLock.Lock();
			try
			{
				if (keyStore.containsAlias(alias) || cache.Contains(alias))
				{
					throw new System.IO.IOException("Credential " + alias + " already exists in " + this
						);
				}
				return innerSetCredential(alias, credential);
			}
			catch (java.security.KeyStoreException e)
			{
				throw new System.IO.IOException("Problem looking up credential " + alias + " in "
					 + this, e);
			}
			finally
			{
				writeLock.unlock();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override void deleteCredentialEntry(string name)
		{
			writeLock.Lock();
			try
			{
				try
				{
					if (keyStore.containsAlias(name))
					{
						keyStore.deleteEntry(name);
					}
					else
					{
						throw new System.IO.IOException("Credential " + name + " does not exist in " + this
							);
					}
				}
				catch (java.security.KeyStoreException e)
				{
					throw new System.IO.IOException("Problem removing " + name + " from " + this, e);
				}
				Sharpen.Collections.Remove(cache, name);
				changed = true;
			}
			finally
			{
				writeLock.unlock();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual org.apache.hadoop.security.alias.CredentialProvider.CredentialEntry
			 innerSetCredential(string alias, char[] material)
		{
			writeLock.Lock();
			try
			{
				keyStore.setKeyEntry(alias, new javax.crypto.spec.SecretKeySpec(Sharpen.Runtime.getBytesForString
					(new string(material), "UTF-8"), "AES"), password, null);
			}
			catch (java.security.KeyStoreException e)
			{
				throw new System.IO.IOException("Can't store credential " + alias + " in " + this
					, e);
			}
			finally
			{
				writeLock.unlock();
			}
			changed = true;
			return new org.apache.hadoop.security.alias.CredentialProvider.CredentialEntry(alias
				, material);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void flush()
		{
			writeLock.Lock();
			try
			{
				if (!changed)
				{
					return;
				}
				// write out the keystore
				try
				{
					using (java.io.OutputStream @out = getOutputStreamForKeystore())
					{
						keyStore.store(@out, password);
					}
				}
				catch (java.security.KeyStoreException e)
				{
					throw new System.IO.IOException("Can't store keystore " + this, e);
				}
				catch (java.security.NoSuchAlgorithmException e)
				{
					throw new System.IO.IOException("No such algorithm storing keystore " + this, e);
				}
				catch (java.security.cert.CertificateException e)
				{
					throw new System.IO.IOException("Certificate exception storing keystore " + this, 
						e);
				}
				changed = false;
			}
			finally
			{
				writeLock.unlock();
			}
		}

		public override string ToString()
		{
			return uri.ToString();
		}
	}
}
