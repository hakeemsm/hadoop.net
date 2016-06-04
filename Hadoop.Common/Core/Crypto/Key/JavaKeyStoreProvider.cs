using System;
using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Annotations;
using Com.Google.Common.Base;
using Hadoop.Common.Core.Conf;
using Hadoop.Common.Core.Fs;
using Org.Apache.Commons.IO;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Security;
using Org.Apache.Hadoop.Util;
using Org.Slf4j;
using Sharpen;

namespace Org.Apache.Hadoop.Crypto.Key
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
	/// <see cref="KeystorePasswordFileKey"/>
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
	public class JavaKeyStoreProvider : KeyProvider
	{
		private const string KeyMetadata = "KeyMetadata";

		private static Logger Log = LoggerFactory.GetLogger(typeof(Org.Apache.Hadoop.Crypto.Key.JavaKeyStoreProvider
			));

		public const string SchemeName = "jceks";

		public const string KeystorePasswordFileKey = "hadoop.security.keystore.java-keystore-provider.password-file";

		public const string KeystorePasswordEnvVar = "HADOOP_KEYSTORE_PASSWORD";

		public static readonly char[] KeystorePasswordDefault = "none".ToCharArray();

		private readonly URI uri;

		private readonly Path path;

		private readonly FileSystem fs;

		private readonly FsPermission permissions;

		private readonly KeyStore keyStore;

		private char[] password;

		private bool changed = false;

		private Lock readLock;

		private Lock writeLock;

		private readonly IDictionary<string, KeyProvider.Metadata> cache = new Dictionary
			<string, KeyProvider.Metadata>();

		[VisibleForTesting]
		internal JavaKeyStoreProvider(Org.Apache.Hadoop.Crypto.Key.JavaKeyStoreProvider other
			)
			: base(new Configuration())
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
		private JavaKeyStoreProvider(URI uri, Configuration conf)
			: base(conf)
		{
			this.uri = uri;
			path = ProviderUtils.UnnestUri(uri);
			fs = path.GetFileSystem(conf);
			// Get the password file from the conf, if not present from the user's
			// environment var
			if (Sharpen.Runtime.GetEnv().Contains(KeystorePasswordEnvVar))
			{
				password = Runtime.Getenv(KeystorePasswordEnvVar).ToCharArray();
			}
			if (password == null)
			{
				string pwFile = conf.Get(KeystorePasswordFileKey);
				if (pwFile != null)
				{
					ClassLoader cl = Sharpen.Thread.CurrentThread().GetContextClassLoader();
					Uri pwdFile = cl.GetResource(pwFile);
					if (pwdFile == null)
					{
						// Provided Password file does not exist
						throw new IOException("Password file does not exists");
					}
					using (InputStream @is = pwdFile.OpenStream())
					{
						password = IOUtils.ToString(@is).Trim().ToCharArray();
					}
				}
			}
			if (password == null)
			{
				password = KeystorePasswordDefault;
			}
			try
			{
				Path oldPath = ConstructOldPath(path);
				Path newPath = ConstructNewPath(path);
				keyStore = KeyStore.GetInstance(SchemeName);
				FsPermission perm = null;
				if (fs.Exists(path))
				{
					// flush did not proceed to completion
					// _NEW should not exist
					if (fs.Exists(newPath))
					{
						throw new IOException(string.Format("Keystore not loaded due to some inconsistency "
							 + "('%s' and '%s' should not exist together)!!", path, newPath));
					}
					perm = TryLoadFromPath(path, oldPath);
				}
				else
				{
					perm = TryLoadIncompleteFlush(oldPath, newPath);
				}
				// Need to save off permissions in case we need to
				// rewrite the keystore in flush()
				permissions = perm;
			}
			catch (KeyStoreException e)
			{
				throw new IOException("Can't create keystore", e);
			}
			catch (NoSuchAlgorithmException e)
			{
				throw new IOException("Can't load keystore " + path, e);
			}
			catch (CertificateException e)
			{
				throw new IOException("Can't load keystore " + path, e);
			}
			ReadWriteLock Lock = new ReentrantReadWriteLock(true);
			readLock = Lock.ReadLock();
			writeLock = Lock.WriteLock();
		}

		/// <summary>
		/// Try loading from the user specified path, else load from the backup
		/// path in case Exception is not due to bad/wrong password
		/// </summary>
		/// <param name="path">Actual path to load from</param>
		/// <param name="backupPath">Backup path (_OLD)</param>
		/// <returns>The permissions of the loaded file</returns>
		/// <exception cref="Sharpen.NoSuchAlgorithmException"/>
		/// <exception cref="Sharpen.CertificateException"/>
		/// <exception cref="System.IO.IOException"/>
		private FsPermission TryLoadFromPath(Path path, Path backupPath)
		{
			FsPermission perm = null;
			try
			{
				perm = LoadFromPath(path, password);
				// Remove _OLD if exists
				if (fs.Exists(backupPath))
				{
					fs.Delete(backupPath, true);
				}
				Log.Debug("KeyStore loaded successfully !!");
			}
			catch (IOException ioe)
			{
				// If file is corrupted for some reason other than
				// wrong password try the _OLD file if exits
				if (!IsBadorWrongPassword(ioe))
				{
					perm = LoadFromPath(backupPath, password);
					// Rename CURRENT to CORRUPTED
					RenameOrFail(path, new Path(path.ToString() + "_CORRUPTED_" + Runtime.CurrentTimeMillis
						()));
					RenameOrFail(backupPath, path);
					Log.Debug(string.Format("KeyStore loaded successfully from '%s' since '%s'" + "was corrupted !!"
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
		/// <exception cref="Sharpen.NoSuchAlgorithmException"/>
		/// <exception cref="Sharpen.CertificateException"/>
		private FsPermission TryLoadIncompleteFlush(Path oldPath, Path newPath)
		{
			FsPermission perm = null;
			// Check if _NEW exists (in case flush had finished writing but not
			// completed the re-naming)
			if (fs.Exists(newPath))
			{
				perm = LoadAndReturnPerm(newPath, oldPath);
			}
			// try loading from _OLD (An earlier Flushing MIGHT not have completed
			// writing completely)
			if ((perm == null) && fs.Exists(oldPath))
			{
				perm = LoadAndReturnPerm(oldPath, newPath);
			}
			// If not loaded yet,
			// required to create an empty keystore. *sigh*
			if (perm == null)
			{
				keyStore.Load(null, password);
				Log.Debug("KeyStore initialized anew successfully !!");
				perm = new FsPermission("700");
			}
			return perm;
		}

		/// <exception cref="Sharpen.NoSuchAlgorithmException"/>
		/// <exception cref="Sharpen.CertificateException"/>
		/// <exception cref="System.IO.IOException"/>
		private FsPermission LoadAndReturnPerm(Path pathToLoad, Path pathToDelete)
		{
			FsPermission perm = null;
			try
			{
				perm = LoadFromPath(pathToLoad, password);
				RenameOrFail(pathToLoad, path);
				Log.Debug(string.Format("KeyStore loaded successfully from '%s'!!", pathToLoad));
				if (fs.Exists(pathToDelete))
				{
					fs.Delete(pathToDelete, true);
				}
			}
			catch (IOException e)
			{
				// Check for password issue : don't want to trash file due
				// to wrong password
				if (IsBadorWrongPassword(e))
				{
					throw;
				}
			}
			return perm;
		}

		private bool IsBadorWrongPassword(IOException ioe)
		{
			// As per documentation this is supposed to be the way to figure
			// if password was correct
			if (ioe.InnerException is UnrecoverableKeyException)
			{
				return true;
			}
			// Unfortunately that doesn't seem to work..
			// Workaround :
			if ((ioe.InnerException == null) && (ioe.Message != null) && ((ioe.Message.Contains
				("Keystore was tampered")) || (ioe.Message.Contains("password was incorrect"))))
			{
				return true;
			}
			return false;
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Sharpen.NoSuchAlgorithmException"/>
		/// <exception cref="Sharpen.CertificateException"/>
		private FsPermission LoadFromPath(Path p, char[] password)
		{
			using (FSDataInputStream @in = fs.Open(p))
			{
				FileStatus s = fs.GetFileStatus(p);
				keyStore.Load(@in, password);
				return s.GetPermission();
			}
		}

		private Path ConstructNewPath(Path path)
		{
			Path newPath = new Path(path.ToString() + "_NEW");
			return newPath;
		}

		private Path ConstructOldPath(Path path)
		{
			Path oldPath = new Path(path.ToString() + "_OLD");
			return oldPath;
		}

		/// <exception cref="System.IO.IOException"/>
		public override KeyProvider.KeyVersion GetKeyVersion(string versionName)
		{
			readLock.Lock();
			try
			{
				SecretKeySpec key = null;
				try
				{
					if (!keyStore.ContainsAlias(versionName))
					{
						return null;
					}
					key = (SecretKeySpec)keyStore.GetKey(versionName, password);
				}
				catch (KeyStoreException e)
				{
					throw new IOException("Can't get key " + versionName + " from " + path, e);
				}
				catch (NoSuchAlgorithmException e)
				{
					throw new IOException("Can't get algorithm for key " + key + " from " + path, e);
				}
				catch (UnrecoverableKeyException e)
				{
					throw new IOException("Can't recover key " + key + " from " + path, e);
				}
				return new KeyProvider.KeyVersion(GetBaseName(versionName), versionName, key.GetEncoded
					());
			}
			finally
			{
				readLock.Unlock();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override IList<string> GetKeys()
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
						// only include the metadata key names in the list of names
						if (!alias.Contains("@"))
						{
							list.AddItem(alias);
						}
					}
				}
				catch (KeyStoreException e)
				{
					throw new IOException("Can't get key " + alias + " from " + path, e);
				}
				return list;
			}
			finally
			{
				readLock.Unlock();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override IList<KeyProvider.KeyVersion> GetKeyVersions(string name)
		{
			readLock.Lock();
			try
			{
				IList<KeyProvider.KeyVersion> list = new AList<KeyProvider.KeyVersion>();
				KeyProvider.Metadata km = GetMetadata(name);
				if (km != null)
				{
					int latestVersion = km.GetVersions();
					KeyProvider.KeyVersion v = null;
					string versionName = null;
					for (int i = 0; i < latestVersion; i++)
					{
						versionName = BuildVersionName(name, i);
						v = GetKeyVersion(versionName);
						if (v != null)
						{
							list.AddItem(v);
						}
					}
				}
				return list;
			}
			finally
			{
				readLock.Unlock();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override KeyProvider.Metadata GetMetadata(string name)
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
					if (!keyStore.ContainsAlias(name))
					{
						return null;
					}
					KeyProvider.Metadata meta = ((JavaKeyStoreProvider.KeyMetadata)keyStore.GetKey(name
						, password)).metadata;
					cache[name] = meta;
					return meta;
				}
				catch (InvalidCastException e)
				{
					throw new IOException("Can't cast key for " + name + " in keystore " + path + " to a KeyMetadata. Key may have been added using "
						 + " keytool or some other non-Hadoop method.", e);
				}
				catch (KeyStoreException e)
				{
					throw new IOException("Can't get metadata for " + name + " from keystore " + path
						, e);
				}
				catch (NoSuchAlgorithmException e)
				{
					throw new IOException("Can't get algorithm for " + name + " from keystore " + path
						, e);
				}
				catch (UnrecoverableKeyException e)
				{
					throw new IOException("Can't recover key for " + name + " from keystore " + path, 
						e);
				}
			}
			finally
			{
				readLock.Unlock();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override KeyProvider.KeyVersion CreateKey(string name, byte[] material, KeyProvider.Options
			 options)
		{
			Preconditions.CheckArgument(name.Equals(StringUtils.ToLowerCase(name)), "Uppercase key names are unsupported: %s"
				, name);
			writeLock.Lock();
			try
			{
				try
				{
					if (keyStore.ContainsAlias(name) || cache.Contains(name))
					{
						throw new IOException("Key " + name + " already exists in " + this);
					}
				}
				catch (KeyStoreException e)
				{
					throw new IOException("Problem looking up key " + name + " in " + this, e);
				}
				KeyProvider.Metadata meta = new KeyProvider.Metadata(options.GetCipher(), options
					.GetBitLength(), options.GetDescription(), options.GetAttributes(), new DateTime
					(), 1);
				if (options.GetBitLength() != 8 * material.Length)
				{
					throw new IOException("Wrong key length. Required " + options.GetBitLength() + ", but got "
						 + (8 * material.Length));
				}
				cache[name] = meta;
				string versionName = BuildVersionName(name, 0);
				return InnerSetKeyVersion(name, versionName, material, meta.GetCipher());
			}
			finally
			{
				writeLock.Unlock();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override void DeleteKey(string name)
		{
			writeLock.Lock();
			try
			{
				KeyProvider.Metadata meta = GetMetadata(name);
				if (meta == null)
				{
					throw new IOException("Key " + name + " does not exist in " + this);
				}
				for (int v = 0; v < meta.GetVersions(); ++v)
				{
					string versionName = BuildVersionName(name, v);
					try
					{
						if (keyStore.ContainsAlias(versionName))
						{
							keyStore.DeleteEntry(versionName);
						}
					}
					catch (KeyStoreException e)
					{
						throw new IOException("Problem removing " + versionName + " from " + this, e);
					}
				}
				try
				{
					if (keyStore.ContainsAlias(name))
					{
						keyStore.DeleteEntry(name);
					}
				}
				catch (KeyStoreException e)
				{
					throw new IOException("Problem removing " + name + " from " + this, e);
				}
				Sharpen.Collections.Remove(cache, name);
				changed = true;
			}
			finally
			{
				writeLock.Unlock();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		internal virtual KeyProvider.KeyVersion InnerSetKeyVersion(string name, string versionName
			, byte[] material, string cipher)
		{
			try
			{
				keyStore.SetKeyEntry(versionName, new SecretKeySpec(material, cipher), password, 
					null);
			}
			catch (KeyStoreException e)
			{
				throw new IOException("Can't store key " + versionName + " in " + this, e);
			}
			changed = true;
			return new KeyProvider.KeyVersion(name, versionName, material);
		}

		/// <exception cref="System.IO.IOException"/>
		public override KeyProvider.KeyVersion RollNewVersion(string name, byte[] material
			)
		{
			writeLock.Lock();
			try
			{
				KeyProvider.Metadata meta = GetMetadata(name);
				if (meta == null)
				{
					throw new IOException("Key " + name + " not found");
				}
				if (meta.GetBitLength() != 8 * material.Length)
				{
					throw new IOException("Wrong key length. Required " + meta.GetBitLength() + ", but got "
						 + (8 * material.Length));
				}
				int nextVersion = meta.AddVersion();
				string versionName = BuildVersionName(name, nextVersion);
				return InnerSetKeyVersion(name, versionName, material, meta.GetCipher());
			}
			finally
			{
				writeLock.Unlock();
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Flush()
		{
			Path newPath = ConstructNewPath(path);
			Path oldPath = ConstructOldPath(path);
			Path resetPath = path;
			writeLock.Lock();
			try
			{
				if (!changed)
				{
					return;
				}
				// Might exist if a backup has been restored etc.
				if (fs.Exists(newPath))
				{
					RenameOrFail(newPath, new Path(newPath.ToString() + "_ORPHANED_" + Runtime.CurrentTimeMillis
						()));
				}
				if (fs.Exists(oldPath))
				{
					RenameOrFail(oldPath, new Path(oldPath.ToString() + "_ORPHANED_" + Runtime.CurrentTimeMillis
						()));
				}
				// put all of the updates into the keystore
				foreach (KeyValuePair<string, KeyProvider.Metadata> entry in cache)
				{
					try
					{
						keyStore.SetKeyEntry(entry.Key, new JavaKeyStoreProvider.KeyMetadata(entry.Value)
							, password, null);
					}
					catch (KeyStoreException e)
					{
						throw new IOException("Can't set metadata key " + entry.Key, e);
					}
				}
				// Save old File first
				bool fileExisted = BackupToOld(oldPath);
				if (fileExisted)
				{
					resetPath = oldPath;
				}
				// write out the keystore
				// Write to _NEW path first :
				try
				{
					WriteToNew(newPath);
				}
				catch (IOException ioe)
				{
					// rename _OLD back to curent and throw Exception
					RevertFromOld(oldPath, fileExisted);
					resetPath = path;
					throw;
				}
				// Rename _NEW to CURRENT and delete _OLD
				CleanupNewAndOld(newPath, oldPath);
				changed = false;
			}
			catch (IOException ioe)
			{
				ResetKeyStoreState(resetPath);
				throw;
			}
			finally
			{
				writeLock.Unlock();
			}
		}

		private void ResetKeyStoreState(Path path)
		{
			Log.Debug("Could not flush Keystore.." + "attempting to reset to previous state !!"
				);
			// 1) flush cache
			cache.Clear();
			// 2) load keyStore from previous path
			try
			{
				LoadFromPath(path, password);
				Log.Debug("KeyStore resetting to previously flushed state !!");
			}
			catch (Exception e)
			{
				Log.Debug("Could not reset Keystore to previous state", e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void CleanupNewAndOld(Path newPath, Path oldPath)
		{
			// Rename _NEW to CURRENT
			RenameOrFail(newPath, path);
			// Delete _OLD
			if (fs.Exists(oldPath))
			{
				fs.Delete(oldPath, true);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual void WriteToNew(Path newPath)
		{
			try
			{
				using (FSDataOutputStream @out = FileSystem.Create(fs, newPath, permissions))
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
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual bool BackupToOld(Path oldPath)
		{
			bool fileExisted = false;
			if (fs.Exists(path))
			{
				RenameOrFail(path, oldPath);
				fileExisted = true;
			}
			return fileExisted;
		}

		/// <exception cref="System.IO.IOException"/>
		private void RevertFromOld(Path oldPath, bool fileExisted)
		{
			if (fileExisted)
			{
				RenameOrFail(oldPath, path);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		private void RenameOrFail(Path src, Path dest)
		{
			if (!fs.Rename(src, dest))
			{
				throw new IOException("Rename unsuccessful : " + string.Format("'%s' to '%s'", src
					, dest));
			}
		}

		public override string ToString()
		{
			return uri.ToString();
		}

		/// <summary>The factory to create JksProviders, which is used by the ServiceLoader.</summary>
		public class Factory : KeyProviderFactory
		{
			/// <exception cref="System.IO.IOException"/>
			public override KeyProvider CreateProvider(URI providerName, Configuration conf)
			{
				if (SchemeName.Equals(providerName.GetScheme()))
				{
					return new JavaKeyStoreProvider(providerName, conf);
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
		public class KeyMetadata : Sharpen.Key
		{
			private KeyProvider.Metadata metadata;

			private const long serialVersionUID = 8405872419967874451L;

			private KeyMetadata(KeyProvider.Metadata meta)
			{
				this.metadata = meta;
			}

			public virtual string GetAlgorithm()
			{
				return metadata.GetCipher();
			}

			public virtual string GetFormat()
			{
				return KeyMetadata;
			}

			public virtual byte[] GetEncoded()
			{
				return new byte[0];
			}

			/// <exception cref="System.IO.IOException"/>
			private void WriteObject(ObjectOutputStream @out)
			{
				byte[] serialized = metadata.Serialize();
				@out.WriteInt(serialized.Length);
				@out.Write(serialized);
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="System.TypeLoadException"/>
			private void ReadObject(ObjectInputStream @in)
			{
				byte[] buf = new byte[@in.ReadInt()];
				@in.ReadFully(buf);
				metadata = new KeyProvider.Metadata(buf);
			}
		}
	}
}
