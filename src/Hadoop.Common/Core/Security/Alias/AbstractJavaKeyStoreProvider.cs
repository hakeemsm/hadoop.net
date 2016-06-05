using System;
using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Base;
using Hadoop.Common.Core.Conf;
using Org.Apache.Commons.IO;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Security;


namespace Org.Apache.Hadoop.Security.Alias
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
	public abstract class AbstractJavaKeyStoreProvider : CredentialProvider
	{
		public const string CredentialPasswordName = "HADOOP_CREDSTORE_PASSWORD";

		public const string KeystorePasswordFileKey = "hadoop.security.credstore.java-keystore-provider.password-file";

		public const string KeystorePasswordDefault = "none";

		private Path path;

		private readonly URI uri;

		private readonly KeyStore keyStore;

		private char[] password = null;

		private bool changed = false;

		private Lock readLock;

		private Lock writeLock;

		/// <exception cref="System.IO.IOException"/>
		protected internal AbstractJavaKeyStoreProvider(URI uri, Configuration conf)
		{
			this.uri = uri;
			InitFileSystem(uri, conf);
			// Get the password from the user's environment
			if (Runtime.GetEnv().Contains(CredentialPasswordName))
			{
				password = Runtime.Getenv(CredentialPasswordName).ToCharArray();
			}
			// if not in ENV get check for file
			if (password == null)
			{
				string pwFile = conf.Get(KeystorePasswordFileKey);
				if (pwFile != null)
				{
					ClassLoader cl = Thread.CurrentThread().GetContextClassLoader();
					Uri pwdFile = cl.GetResource(pwFile);
					if (pwdFile != null)
					{
						using (InputStream @is = pwdFile.OpenStream())
						{
							password = IOUtils.ToString(@is).Trim().ToCharArray();
						}
					}
				}
			}
			if (password == null)
			{
				password = KeystorePasswordDefault.ToCharArray();
			}
			try
			{
				keyStore = KeyStore.GetInstance("jceks");
				if (KeystoreExists())
				{
					StashOriginalFilePermissions();
					using (InputStream @in = GetInputStreamForFile())
					{
						keyStore.Load(@in, password);
					}
				}
				else
				{
					CreatePermissions("700");
					// required to create an empty keystore. *sigh*
					keyStore.Load(null, password);
				}
			}
			catch (KeyStoreException e)
			{
				throw new IOException("Can't create keystore", e);
			}
			catch (NoSuchAlgorithmException e)
			{
				throw new IOException("Can't load keystore " + GetPathAsString(), e);
			}
			catch (CertificateException e)
			{
				throw new IOException("Can't load keystore " + GetPathAsString(), e);
			}
			ReadWriteLock Lock = new ReentrantReadWriteLock(true);
			readLock = Lock.ReadLock();
			writeLock = Lock.WriteLock();
		}

		public virtual Path GetPath()
		{
			return path;
		}

		public virtual void SetPath(Path p)
		{
			this.path = p;
		}

		public virtual char[] GetPassword()
		{
			return password;
		}

		public virtual void SetPassword(char[] pass)
		{
			this.password = pass;
		}

		public virtual bool IsChanged()
		{
			return changed;
		}

		public virtual void SetChanged(bool chg)
		{
			this.changed = chg;
		}

		public virtual Lock GetReadLock()
		{
			return readLock;
		}

		public virtual void SetReadLock(Lock rl)
		{
			this.readLock = rl;
		}

		public virtual Lock GetWriteLock()
		{
			return writeLock;
		}

		public virtual void SetWriteLock(Lock wl)
		{
			this.writeLock = wl;
		}

		public virtual URI GetUri()
		{
			return uri;
		}

		public virtual KeyStore GetKeyStore()
		{
			return keyStore;
		}

		public virtual IDictionary<string, CredentialProvider.CredentialEntry> GetCache()
		{
			return cache;
		}

		private readonly IDictionary<string, CredentialProvider.CredentialEntry> cache = 
			new Dictionary<string, CredentialProvider.CredentialEntry>();

		protected internal string GetPathAsString()
		{
			return GetPath().ToString();
		}

		protected internal abstract string GetSchemeName();

		/// <exception cref="System.IO.IOException"/>
		protected internal abstract OutputStream GetOutputStreamForKeystore();

		/// <exception cref="System.IO.IOException"/>
		protected internal abstract bool KeystoreExists();

		/// <exception cref="System.IO.IOException"/>
		protected internal abstract InputStream GetInputStreamForFile();

		/// <exception cref="System.IO.IOException"/>
		protected internal abstract void CreatePermissions(string perms);

		/// <exception cref="System.IO.IOException"/>
		protected internal abstract void StashOriginalFilePermissions();

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual void InitFileSystem(URI keystoreUri, Configuration conf
			)
		{
			path = ProviderUtils.UnnestUri(keystoreUri);
		}

		/// <exception cref="System.IO.IOException"/>
		public override CredentialProvider.CredentialEntry GetCredentialEntry(string alias
			)
		{
			readLock.Lock();
			try
			{
				SecretKeySpec key = null;
				try
				{
					if (cache.Contains(alias))
					{
						return cache[alias];
					}
					if (!keyStore.ContainsAlias(alias))
					{
						return null;
					}
					key = (SecretKeySpec)keyStore.GetKey(alias, password);
				}
				catch (KeyStoreException e)
				{
					throw new IOException("Can't get credential " + alias + " from " + GetPathAsString
						(), e);
				}
				catch (NoSuchAlgorithmException e)
				{
					throw new IOException("Can't get algorithm for credential " + alias + " from " + 
						GetPathAsString(), e);
				}
				catch (UnrecoverableKeyException e)
				{
					throw new IOException("Can't recover credential " + alias + " from " + GetPathAsString
						(), e);
				}
				return new CredentialProvider.CredentialEntry(alias, BytesToChars(key.GetEncoded(
					)));
			}
			finally
			{
				readLock.Unlock();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public static char[] BytesToChars(byte[] bytes)
		{
			string pass;
			pass = new string(bytes, Charsets.Utf8);
			return pass.ToCharArray();
		}

		/// <exception cref="System.IO.IOException"/>
		public override IList<string> GetAliases()
		{
			readLock.Lock();
			try
			{
				AList<string> list = new AList<string>();
				string alias = null;
				try
				{
					Enumeration<string> e = keyStore.Aliases();
					while (e.MoveNext())
					{
						alias = e.Current;
						list.AddItem(alias);
					}
				}
				catch (KeyStoreException e)
				{
					throw new IOException("Can't get alias " + alias + " from " + GetPathAsString(), 
						e);
				}
				return list;
			}
			finally
			{
				readLock.Unlock();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override CredentialProvider.CredentialEntry CreateCredentialEntry(string alias
			, char[] credential)
		{
			writeLock.Lock();
			try
			{
				if (keyStore.ContainsAlias(alias) || cache.Contains(alias))
				{
					throw new IOException("Credential " + alias + " already exists in " + this);
				}
				return InnerSetCredential(alias, credential);
			}
			catch (KeyStoreException e)
			{
				throw new IOException("Problem looking up credential " + alias + " in " + this, e
					);
			}
			finally
			{
				writeLock.Unlock();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override void DeleteCredentialEntry(string name)
		{
			writeLock.Lock();
			try
			{
				try
				{
					if (keyStore.ContainsAlias(name))
					{
						keyStore.DeleteEntry(name);
					}
					else
					{
						throw new IOException("Credential " + name + " does not exist in " + this);
					}
				}
				catch (KeyStoreException e)
				{
					throw new IOException("Problem removing " + name + " from " + this, e);
				}
				Collections.Remove(cache, name);
				changed = true;
			}
			finally
			{
				writeLock.Unlock();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual CredentialProvider.CredentialEntry InnerSetCredential(string alias
			, char[] material)
		{
			writeLock.Lock();
			try
			{
				keyStore.SetKeyEntry(alias, new SecretKeySpec(Runtime.GetBytesForString(new 
					string(material), "UTF-8"), "AES"), password, null);
			}
			catch (KeyStoreException e)
			{
				throw new IOException("Can't store credential " + alias + " in " + this, e);
			}
			finally
			{
				writeLock.Unlock();
			}
			changed = true;
			return new CredentialProvider.CredentialEntry(alias, material);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Flush()
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
					using (OutputStream @out = GetOutputStreamForKeystore())
					{
						keyStore.Store(@out, password);
					}
				}
				catch (KeyStoreException e)
				{
					throw new IOException("Can't store keystore " + this, e);
				}
				catch (NoSuchAlgorithmException e)
				{
					throw new IOException("No such algorithm storing keystore " + this, e);
				}
				catch (CertificateException e)
				{
					throw new IOException("Certificate exception storing keystore " + this, e);
				}
				changed = false;
			}
			finally
			{
				writeLock.Unlock();
			}
		}

		public override string ToString()
		{
			return uri.ToString();
		}
	}
}
