using Sharpen;

namespace org.apache.hadoop.crypto.key
{
	/// <summary>KeyProvider based on Java's KeyStore file format.</summary>
	/// <remarks>
	/// KeyProvider based on Java's KeyStore file format. The file may be stored in
	/// any Hadoop FileSystem using the following name mangling:
	/// jks://hdfs@nn1.example.com/my/keys.jks -&gt; hdfs://nn1.example.com/my/keys.jks
	/// jks://file/home/owen/keys.jks -&gt; file:///home/owen/keys.jks
	/// <p/>
	/// If the <code>HADOOP_KEYSTORE_PASSWORD</code> environment variable is set,
	/// its value is used as the password for the keystore.
	/// <p/>
	/// If the <code>HADOOP_KEYSTORE_PASSWORD</code> environment variable is not set,
	/// the password for the keystore is read from file specified in the
	/// <see cref="KEYSTORE_PASSWORD_FILE_KEY"/>
	/// configuration property. The password file
	/// is looked up in Hadoop's configuration directory via the classpath.
	/// <p/>
	/// <b>NOTE:</b> Make sure the password in the password file does not have an
	/// ENTER at the end, else it won't be valid for the Java KeyStore.
	/// <p/>
	/// If the environment variable, nor the property are not set, the password used
	/// is 'none'.
	/// <p/>
	/// It is expected for encrypted InputFormats and OutputFormats to copy the keys
	/// from the original provider into the job's Credentials object, which is
	/// accessed via the UserProvider. Therefore, this provider won't be used by
	/// MapReduce tasks.
	/// </remarks>
	public class JavaKeyStoreProvider : org.apache.hadoop.crypto.key.KeyProvider
	{
		private const string KEY_METADATA = "KeyMetadata";

		private static org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(Sharpen.Runtime.getClassForType
			(typeof(org.apache.hadoop.crypto.key.JavaKeyStoreProvider)));

		public const string SCHEME_NAME = "jceks";

		public const string KEYSTORE_PASSWORD_FILE_KEY = "hadoop.security.keystore.java-keystore-provider.password-file";

		public const string KEYSTORE_PASSWORD_ENV_VAR = "HADOOP_KEYSTORE_PASSWORD";

		public static readonly char[] KEYSTORE_PASSWORD_DEFAULT = "none".ToCharArray();

		private readonly java.net.URI uri;

		private readonly org.apache.hadoop.fs.Path path;

		private readonly org.apache.hadoop.fs.FileSystem fs;

		private readonly org.apache.hadoop.fs.permission.FsPermission permissions;

		private readonly java.security.KeyStore keyStore;

		private char[] password;

		private bool changed = false;

		private java.util.concurrent.locks.Lock readLock;

		private java.util.concurrent.locks.Lock writeLock;

		private readonly System.Collections.Generic.IDictionary<string, org.apache.hadoop.crypto.key.KeyProvider.Metadata
			> cache = new System.Collections.Generic.Dictionary<string, org.apache.hadoop.crypto.key.KeyProvider.Metadata
			>();

		[com.google.common.annotations.VisibleForTesting]
		internal JavaKeyStoreProvider(org.apache.hadoop.crypto.key.JavaKeyStoreProvider other
			)
			: base(new org.apache.hadoop.conf.Configuration())
		{
			uri = other.uri;
			path = other.path;
			fs = other.fs;
			permissions = other.permissions;
			keyStore = other.keyStore;
			password = other.password;
			changed = other.changed;
			readLock = other.readLock;
			writeLock = other.writeLock;
		}

		/// <exception cref="System.IO.IOException"/>
		private JavaKeyStoreProvider(java.net.URI uri, org.apache.hadoop.conf.Configuration
			 conf)
			: base(conf)
		{
			this.uri = uri;
			path = org.apache.hadoop.security.ProviderUtils.unnestUri(uri);
			fs = path.getFileSystem(conf);
			// Get the password file from the conf, if not present from the user's
			// environment var
			if (Sharpen.Runtime.getenv().Contains(KEYSTORE_PASSWORD_ENV_VAR))
			{
				password = Sharpen.Runtime.getenv(KEYSTORE_PASSWORD_ENV_VAR).ToCharArray();
			}
			if (password == null)
			{
				string pwFile = conf.get(KEYSTORE_PASSWORD_FILE_KEY);
				if (pwFile != null)
				{
					java.lang.ClassLoader cl = java.lang.Thread.currentThread().getContextClassLoader
						();
					java.net.URL pwdFile = cl.getResource(pwFile);
					if (pwdFile == null)
					{
						// Provided Password file does not exist
						throw new System.IO.IOException("Password file does not exists");
					}
					using (java.io.InputStream @is = pwdFile.openStream())
					{
						password = org.apache.commons.io.IOUtils.toString(@is).Trim().ToCharArray();
					}
				}
			}
			if (password == null)
			{
				password = KEYSTORE_PASSWORD_DEFAULT;
			}
			try
			{
				org.apache.hadoop.fs.Path oldPath = constructOldPath(path);
				org.apache.hadoop.fs.Path newPath = constructNewPath(path);
				keyStore = java.security.KeyStore.getInstance(SCHEME_NAME);
				org.apache.hadoop.fs.permission.FsPermission perm = null;
				if (fs.exists(path))
				{
					// flush did not proceed to completion
					// _NEW should not exist
					if (fs.exists(newPath))
					{
						throw new System.IO.IOException(string.format("Keystore not loaded due to some inconsistency "
							 + "('%s' and '%s' should not exist together)!!", path, newPath));
					}
					perm = tryLoadFromPath(path, oldPath);
				}
				else
				{
					perm = tryLoadIncompleteFlush(oldPath, newPath);
				}
				// Need to save off permissions in case we need to
				// rewrite the keystore in flush()
				permissions = perm;
			}
			catch (java.security.KeyStoreException e)
			{
				throw new System.IO.IOException("Can't create keystore", e);
			}
			catch (java.security.NoSuchAlgorithmException e)
			{
				throw new System.IO.IOException("Can't load keystore " + path, e);
			}
			catch (java.security.cert.CertificateException e)
			{
				throw new System.IO.IOException("Can't load keystore " + path, e);
			}
			java.util.concurrent.locks.ReadWriteLock Lock = new java.util.concurrent.locks.ReentrantReadWriteLock
				(true);
			readLock = Lock.readLock();
			writeLock = Lock.writeLock();
		}

		/// <summary>
		/// Try loading from the user specified path, else load from the backup
		/// path in case Exception is not due to bad/wrong password
		/// </summary>
		/// <param name="path">Actual path to load from</param>
		/// <param name="backupPath">Backup path (_OLD)</param>
		/// <returns>The permissions of the loaded file</returns>
		/// <exception cref="java.security.NoSuchAlgorithmException"/>
		/// <exception cref="java.security.cert.CertificateException"/>
		/// <exception cref="System.IO.IOException"/>
		private org.apache.hadoop.fs.permission.FsPermission tryLoadFromPath(org.apache.hadoop.fs.Path
			 path, org.apache.hadoop.fs.Path backupPath)
		{
			org.apache.hadoop.fs.permission.FsPermission perm = null;
			try
			{
				perm = loadFromPath(path, password);
				// Remove _OLD if exists
				if (fs.exists(backupPath))
				{
					fs.delete(backupPath, true);
				}
				LOG.debug("KeyStore loaded successfully !!");
			}
			catch (System.IO.IOException ioe)
			{
				// If file is corrupted for some reason other than
				// wrong password try the _OLD file if exits
				if (!isBadorWrongPassword(ioe))
				{
					perm = loadFromPath(backupPath, password);
					// Rename CURRENT to CORRUPTED
					renameOrFail(path, new org.apache.hadoop.fs.Path(path.ToString() + "_CORRUPTED_" 
						+ Sharpen.Runtime.currentTimeMillis()));
					renameOrFail(backupPath, path);
					LOG.debug(string.format("KeyStore loaded successfully from '%s' since '%s'" + "was corrupted !!"
						, backupPath, path));
				}
				else
				{
					throw;
				}
			}
			return perm;
		}

		/// <summary>
		/// The KeyStore might have gone down during a flush, In which case either the
		/// _NEW or _OLD files might exists.
		/// </summary>
		/// <remarks>
		/// The KeyStore might have gone down during a flush, In which case either the
		/// _NEW or _OLD files might exists. This method tries to load the KeyStore
		/// from one of these intermediate files.
		/// </remarks>
		/// <param name="oldPath">the _OLD file created during flush</param>
		/// <param name="newPath">the _NEW file created during flush</param>
		/// <returns>The permissions of the loaded file</returns>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="java.security.NoSuchAlgorithmException"/>
		/// <exception cref="java.security.cert.CertificateException"/>
		private org.apache.hadoop.fs.permission.FsPermission tryLoadIncompleteFlush(org.apache.hadoop.fs.Path
			 oldPath, org.apache.hadoop.fs.Path newPath)
		{
			org.apache.hadoop.fs.permission.FsPermission perm = null;
			// Check if _NEW exists (in case flush had finished writing but not
			// completed the re-naming)
			if (fs.exists(newPath))
			{
				perm = loadAndReturnPerm(newPath, oldPath);
			}
			// try loading from _OLD (An earlier Flushing MIGHT not have completed
			// writing completely)
			if ((perm == null) && fs.exists(oldPath))
			{
				perm = loadAndReturnPerm(oldPath, newPath);
			}
			// If not loaded yet,
			// required to create an empty keystore. *sigh*
			if (perm == null)
			{
				keyStore.load(null, password);
				LOG.debug("KeyStore initialized anew successfully !!");
				perm = new org.apache.hadoop.fs.permission.FsPermission("700");
			}
			return perm;
		}

		/// <exception cref="java.security.NoSuchAlgorithmException"/>
		/// <exception cref="java.security.cert.CertificateException"/>
		/// <exception cref="System.IO.IOException"/>
		private org.apache.hadoop.fs.permission.FsPermission loadAndReturnPerm(org.apache.hadoop.fs.Path
			 pathToLoad, org.apache.hadoop.fs.Path pathToDelete)
		{
			org.apache.hadoop.fs.permission.FsPermission perm = null;
			try
			{
				perm = loadFromPath(pathToLoad, password);
				renameOrFail(pathToLoad, path);
				LOG.debug(string.format("KeyStore loaded successfully from '%s'!!", pathToLoad));
				if (fs.exists(pathToDelete))
				{
					fs.delete(pathToDelete, true);
				}
			}
			catch (System.IO.IOException e)
			{
				// Check for password issue : don't want to trash file due
				// to wrong password
				if (isBadorWrongPassword(e))
				{
					throw;
				}
			}
			return perm;
		}

		private bool isBadorWrongPassword(System.IO.IOException ioe)
		{
			// As per documentation this is supposed to be the way to figure
			// if password was correct
			if (ioe.InnerException is java.security.UnrecoverableKeyException)
			{
				return true;
			}
			// Unfortunately that doesn't seem to work..
			// Workaround :
			if ((ioe.InnerException == null) && (ioe.Message != null) && ((ioe.Message.contains
				("Keystore was tampered")) || (ioe.Message.contains("password was incorrect"))))
			{
				return true;
			}
			return false;
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="java.security.NoSuchAlgorithmException"/>
		/// <exception cref="java.security.cert.CertificateException"/>
		private org.apache.hadoop.fs.permission.FsPermission loadFromPath(org.apache.hadoop.fs.Path
			 p, char[] password)
		{
			using (org.apache.hadoop.fs.FSDataInputStream @in = fs.open(p))
			{
				org.apache.hadoop.fs.FileStatus s = fs.getFileStatus(p);
				keyStore.load(@in, password);
				return s.getPermission();
			}
		}

		private org.apache.hadoop.fs.Path constructNewPath(org.apache.hadoop.fs.Path path
			)
		{
			org.apache.hadoop.fs.Path newPath = new org.apache.hadoop.fs.Path(path.ToString()
				 + "_NEW");
			return newPath;
		}

		private org.apache.hadoop.fs.Path constructOldPath(org.apache.hadoop.fs.Path path
			)
		{
			org.apache.hadoop.fs.Path oldPath = new org.apache.hadoop.fs.Path(path.ToString()
				 + "_OLD");
			return oldPath;
		}

		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.crypto.key.KeyProvider.KeyVersion getKeyVersion
			(string versionName)
		{
			readLock.Lock();
			try
			{
				javax.crypto.spec.SecretKeySpec key = null;
				try
				{
					if (!keyStore.containsAlias(versionName))
					{
						return null;
					}
					key = (javax.crypto.spec.SecretKeySpec)keyStore.getKey(versionName, password);
				}
				catch (java.security.KeyStoreException e)
				{
					throw new System.IO.IOException("Can't get key " + versionName + " from " + path, 
						e);
				}
				catch (java.security.NoSuchAlgorithmException e)
				{
					throw new System.IO.IOException("Can't get algorithm for key " + key + " from " +
						 path, e);
				}
				catch (java.security.UnrecoverableKeyException e)
				{
					throw new System.IO.IOException("Can't recover key " + key + " from " + path, e);
				}
				return new org.apache.hadoop.crypto.key.KeyProvider.KeyVersion(getBaseName(versionName
					), versionName, key.getEncoded());
			}
			finally
			{
				readLock.unlock();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override System.Collections.Generic.IList<string> getKeys()
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
						// only include the metadata key names in the list of names
						if (!alias.contains("@"))
						{
							list.add(alias);
						}
					}
				}
				catch (java.security.KeyStoreException e)
				{
					throw new System.IO.IOException("Can't get key " + alias + " from " + path, e);
				}
				return list;
			}
			finally
			{
				readLock.unlock();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override System.Collections.Generic.IList<org.apache.hadoop.crypto.key.KeyProvider.KeyVersion
			> getKeyVersions(string name)
		{
			readLock.Lock();
			try
			{
				System.Collections.Generic.IList<org.apache.hadoop.crypto.key.KeyProvider.KeyVersion
					> list = new System.Collections.Generic.List<org.apache.hadoop.crypto.key.KeyProvider.KeyVersion
					>();
				org.apache.hadoop.crypto.key.KeyProvider.Metadata km = getMetadata(name);
				if (km != null)
				{
					int latestVersion = km.getVersions();
					org.apache.hadoop.crypto.key.KeyProvider.KeyVersion v = null;
					string versionName = null;
					for (int i = 0; i < latestVersion; i++)
					{
						versionName = buildVersionName(name, i);
						v = getKeyVersion(versionName);
						if (v != null)
						{
							list.add(v);
						}
					}
				}
				return list;
			}
			finally
			{
				readLock.unlock();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.crypto.key.KeyProvider.Metadata getMetadata(string
			 name)
		{
			readLock.Lock();
			try
			{
				if (cache.Contains(name))
				{
					return cache[name];
				}
				try
				{
					if (!keyStore.containsAlias(name))
					{
						return null;
					}
					org.apache.hadoop.crypto.key.KeyProvider.Metadata meta = ((org.apache.hadoop.crypto.key.JavaKeyStoreProvider.KeyMetadata
						)keyStore.getKey(name, password)).metadata;
					cache[name] = meta;
					return meta;
				}
				catch (System.InvalidCastException e)
				{
					throw new System.IO.IOException("Can't cast key for " + name + " in keystore " + 
						path + " to a KeyMetadata. Key may have been added using " + " keytool or some other non-Hadoop method."
						, e);
				}
				catch (java.security.KeyStoreException e)
				{
					throw new System.IO.IOException("Can't get metadata for " + name + " from keystore "
						 + path, e);
				}
				catch (java.security.NoSuchAlgorithmException e)
				{
					throw new System.IO.IOException("Can't get algorithm for " + name + " from keystore "
						 + path, e);
				}
				catch (java.security.UnrecoverableKeyException e)
				{
					throw new System.IO.IOException("Can't recover key for " + name + " from keystore "
						 + path, e);
				}
			}
			finally
			{
				readLock.unlock();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.crypto.key.KeyProvider.KeyVersion createKey(string
			 name, byte[] material, org.apache.hadoop.crypto.key.KeyProvider.Options options
			)
		{
			com.google.common.@base.Preconditions.checkArgument(name.Equals(org.apache.hadoop.util.StringUtils
				.toLowerCase(name)), "Uppercase key names are unsupported: %s", name);
			writeLock.Lock();
			try
			{
				try
				{
					if (keyStore.containsAlias(name) || cache.Contains(name))
					{
						throw new System.IO.IOException("Key " + name + " already exists in " + this);
					}
				}
				catch (java.security.KeyStoreException e)
				{
					throw new System.IO.IOException("Problem looking up key " + name + " in " + this, 
						e);
				}
				org.apache.hadoop.crypto.key.KeyProvider.Metadata meta = new org.apache.hadoop.crypto.key.KeyProvider.Metadata
					(options.getCipher(), options.getBitLength(), options.getDescription(), options.
					getAttributes(), new System.DateTime(), 1);
				if (options.getBitLength() != 8 * material.Length)
				{
					throw new System.IO.IOException("Wrong key length. Required " + options.getBitLength
						() + ", but got " + (8 * material.Length));
				}
				cache[name] = meta;
				string versionName = buildVersionName(name, 0);
				return innerSetKeyVersion(name, versionName, material, meta.getCipher());
			}
			finally
			{
				writeLock.unlock();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override void deleteKey(string name)
		{
			writeLock.Lock();
			try
			{
				org.apache.hadoop.crypto.key.KeyProvider.Metadata meta = getMetadata(name);
				if (meta == null)
				{
					throw new System.IO.IOException("Key " + name + " does not exist in " + this);
				}
				for (int v = 0; v < meta.getVersions(); ++v)
				{
					string versionName = buildVersionName(name, v);
					try
					{
						if (keyStore.containsAlias(versionName))
						{
							keyStore.deleteEntry(versionName);
						}
					}
					catch (java.security.KeyStoreException e)
					{
						throw new System.IO.IOException("Problem removing " + versionName + " from " + this
							, e);
					}
				}
				try
				{
					if (keyStore.containsAlias(name))
					{
						keyStore.deleteEntry(name);
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
		internal virtual org.apache.hadoop.crypto.key.KeyProvider.KeyVersion innerSetKeyVersion
			(string name, string versionName, byte[] material, string cipher)
		{
			try
			{
				keyStore.setKeyEntry(versionName, new javax.crypto.spec.SecretKeySpec(material, cipher
					), password, null);
			}
			catch (java.security.KeyStoreException e)
			{
				throw new System.IO.IOException("Can't store key " + versionName + " in " + this, 
					e);
			}
			changed = true;
			return new org.apache.hadoop.crypto.key.KeyProvider.KeyVersion(name, versionName, 
				material);
		}

		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.crypto.key.KeyProvider.KeyVersion rollNewVersion
			(string name, byte[] material)
		{
			writeLock.Lock();
			try
			{
				org.apache.hadoop.crypto.key.KeyProvider.Metadata meta = getMetadata(name);
				if (meta == null)
				{
					throw new System.IO.IOException("Key " + name + " not found");
				}
				if (meta.getBitLength() != 8 * material.Length)
				{
					throw new System.IO.IOException("Wrong key length. Required " + meta.getBitLength
						() + ", but got " + (8 * material.Length));
				}
				int nextVersion = meta.addVersion();
				string versionName = buildVersionName(name, nextVersion);
				return innerSetKeyVersion(name, versionName, material, meta.getCipher());
			}
			finally
			{
				writeLock.unlock();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override void flush()
		{
			org.apache.hadoop.fs.Path newPath = constructNewPath(path);
			org.apache.hadoop.fs.Path oldPath = constructOldPath(path);
			org.apache.hadoop.fs.Path resetPath = path;
			writeLock.Lock();
			try
			{
				if (!changed)
				{
					return;
				}
				// Might exist if a backup has been restored etc.
				if (fs.exists(newPath))
				{
					renameOrFail(newPath, new org.apache.hadoop.fs.Path(newPath.ToString() + "_ORPHANED_"
						 + Sharpen.Runtime.currentTimeMillis()));
				}
				if (fs.exists(oldPath))
				{
					renameOrFail(oldPath, new org.apache.hadoop.fs.Path(oldPath.ToString() + "_ORPHANED_"
						 + Sharpen.Runtime.currentTimeMillis()));
				}
				// put all of the updates into the keystore
				foreach (System.Collections.Generic.KeyValuePair<string, org.apache.hadoop.crypto.key.KeyProvider.Metadata
					> entry in cache)
				{
					try
					{
						keyStore.setKeyEntry(entry.Key, new org.apache.hadoop.crypto.key.JavaKeyStoreProvider.KeyMetadata
							(entry.Value), password, null);
					}
					catch (java.security.KeyStoreException e)
					{
						throw new System.IO.IOException("Can't set metadata key " + entry.Key, e);
					}
				}
				// Save old File first
				bool fileExisted = backupToOld(oldPath);
				if (fileExisted)
				{
					resetPath = oldPath;
				}
				// write out the keystore
				// Write to _NEW path first :
				try
				{
					writeToNew(newPath);
				}
				catch (System.IO.IOException ioe)
				{
					// rename _OLD back to curent and throw Exception
					revertFromOld(oldPath, fileExisted);
					resetPath = path;
					throw;
				}
				// Rename _NEW to CURRENT and delete _OLD
				cleanupNewAndOld(newPath, oldPath);
				changed = false;
			}
			catch (System.IO.IOException ioe)
			{
				resetKeyStoreState(resetPath);
				throw;
			}
			finally
			{
				writeLock.unlock();
			}
		}

		private void resetKeyStoreState(org.apache.hadoop.fs.Path path)
		{
			LOG.debug("Could not flush Keystore.." + "attempting to reset to previous state !!"
				);
			// 1) flush cache
			cache.clear();
			// 2) load keyStore from previous path
			try
			{
				loadFromPath(path, password);
				LOG.debug("KeyStore resetting to previously flushed state !!");
			}
			catch (System.Exception e)
			{
				LOG.debug("Could not reset Keystore to previous state", e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void cleanupNewAndOld(org.apache.hadoop.fs.Path newPath, org.apache.hadoop.fs.Path
			 oldPath)
		{
			// Rename _NEW to CURRENT
			renameOrFail(newPath, path);
			// Delete _OLD
			if (fs.exists(oldPath))
			{
				fs.delete(oldPath, true);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual void writeToNew(org.apache.hadoop.fs.Path newPath)
		{
			try
			{
				using (org.apache.hadoop.fs.FSDataOutputStream @out = org.apache.hadoop.fs.FileSystem
					.create(fs, newPath, permissions))
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
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual bool backupToOld(org.apache.hadoop.fs.Path oldPath)
		{
			bool fileExisted = false;
			if (fs.exists(path))
			{
				renameOrFail(path, oldPath);
				fileExisted = true;
			}
			return fileExisted;
		}

		/// <exception cref="System.IO.IOException"/>
		private void revertFromOld(org.apache.hadoop.fs.Path oldPath, bool fileExisted)
		{
			if (fileExisted)
			{
				renameOrFail(oldPath, path);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void renameOrFail(org.apache.hadoop.fs.Path src, org.apache.hadoop.fs.Path
			 dest)
		{
			if (!fs.rename(src, dest))
			{
				throw new System.IO.IOException("Rename unsuccessful : " + string.format("'%s' to '%s'"
					, src, dest));
			}
		}

		public override string ToString()
		{
			return uri.ToString();
		}

		/// <summary>The factory to create JksProviders, which is used by the ServiceLoader.</summary>
		public class Factory : org.apache.hadoop.crypto.key.KeyProviderFactory
		{
			/// <exception cref="System.IO.IOException"/>
			public override org.apache.hadoop.crypto.key.KeyProvider createProvider(java.net.URI
				 providerName, org.apache.hadoop.conf.Configuration conf)
			{
				if (SCHEME_NAME.Equals(providerName.getScheme()))
				{
					return new org.apache.hadoop.crypto.key.JavaKeyStoreProvider(providerName, conf);
				}
				return null;
			}
		}

		/// <summary>An adapter between a KeyStore Key and our Metadata.</summary>
		/// <remarks>
		/// An adapter between a KeyStore Key and our Metadata. This is used to store
		/// the metadata in a KeyStore even though isn't really a key.
		/// </remarks>
		[System.Serializable]
		public class KeyMetadata : java.security.Key
		{
			private org.apache.hadoop.crypto.key.KeyProvider.Metadata metadata;

			private const long serialVersionUID = 8405872419967874451L;

			private KeyMetadata(org.apache.hadoop.crypto.key.KeyProvider.Metadata meta)
			{
				this.metadata = meta;
			}

			public virtual string getAlgorithm()
			{
				return metadata.getCipher();
			}

			public virtual string getFormat()
			{
				return KEY_METADATA;
			}

			public virtual byte[] getEncoded()
			{
				return new byte[0];
			}

			/// <exception cref="System.IO.IOException"/>
			private void writeObject(java.io.ObjectOutputStream @out)
			{
				byte[] serialized = metadata.serialize();
				@out.writeInt(serialized.Length);
				@out.write(serialized);
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="java.lang.ClassNotFoundException"/>
			private void readObject(java.io.ObjectInputStream @in)
			{
				byte[] buf = new byte[@in.readInt()];
				@in.readFully(buf);
				metadata = new org.apache.hadoop.crypto.key.KeyProvider.Metadata(buf);
			}
		}
	}
}
