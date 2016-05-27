using System;
using System.Collections.Generic;
using System.IO;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Util;
using Sharpen;
using Sharpen.File;
using Sharpen.File.Attribute;

namespace Org.Apache.Hadoop.Security.Alias
{
	/// <summary>CredentialProvider based on Java's KeyStore file format.</summary>
	/// <remarks>
	/// CredentialProvider based on Java's KeyStore file format. The file may be
	/// stored only on the local filesystem using the following name mangling:
	/// localjceks://file/home/larry/creds.jceks -&gt; file:///home/larry/creds.jceks
	/// </remarks>
	public sealed class LocalJavaKeyStoreProvider : AbstractJavaKeyStoreProvider
	{
		public const string SchemeName = "localjceks";

		private FilePath file;

		private ICollection<PosixFilePermission> permissions;

		/// <exception cref="System.IO.IOException"/>
		private LocalJavaKeyStoreProvider(URI uri, Configuration conf)
			: base(uri, conf)
		{
		}

		protected internal override string GetSchemeName()
		{
			return SchemeName;
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override OutputStream GetOutputStreamForKeystore()
		{
			FileOutputStream @out = new FileOutputStream(file);
			return @out;
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override bool KeystoreExists()
		{
			return file.Exists();
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override InputStream GetInputStreamForFile()
		{
			FileInputStream @is = new FileInputStream(file);
			return @is;
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override void CreatePermissions(string perms)
		{
			int mode = 700;
			try
			{
				mode = System.Convert.ToInt32(perms, 8);
			}
			catch (FormatException nfe)
			{
				throw new IOException("Invalid permissions mode provided while " + "trying to createPermissions"
					, nfe);
			}
			permissions = ModeToPosixFilePermission(mode);
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override void StashOriginalFilePermissions()
		{
			// save off permissions in case we need to
			// rewrite the keystore in flush()
			if (!Shell.Windows)
			{
				Path path = Paths.Get(file.GetCanonicalPath());
				permissions = Files.GetPosixFilePermissions(path);
			}
			else
			{
				// On Windows, the JDK does not support the POSIX file permission APIs.
				// Instead, we can do a winutils call and translate.
				string[] cmd = Shell.GetGetPermissionCommand();
				string[] args = new string[cmd.Length + 1];
				System.Array.Copy(cmd, 0, args, 0, cmd.Length);
				args[cmd.Length] = file.GetCanonicalPath();
				string @out = Shell.ExecCommand(args);
				StringTokenizer t = new StringTokenizer(@out, Shell.TokenSeparatorRegex);
				// The winutils output consists of 10 characters because of the leading
				// directory indicator, i.e. "drwx------".  The JDK parsing method expects
				// a 9-character string, so remove the leading character.
				string permString = Sharpen.Runtime.Substring(t.NextToken(), 1);
				permissions = PosixFilePermissions.FromString(permString);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal override void InitFileSystem(URI uri, Configuration conf)
		{
			base.InitFileSystem(uri, conf);
			try
			{
				file = new FilePath(new URI(GetPath().ToString()));
			}
			catch (URISyntaxException e)
			{
				throw new IOException(e);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Flush()
		{
			base.Flush();
			if (!Shell.Windows)
			{
				Files.SetPosixFilePermissions(Paths.Get(file.GetCanonicalPath()), permissions);
			}
			else
			{
				// FsPermission expects a 10-character string because of the leading
				// directory indicator, i.e. "drwx------". The JDK toString method returns
				// a 9-character string, so prepend a leading character.
				FsPermission fsPermission = FsPermission.ValueOf("-" + PosixFilePermissions.ToString
					(permissions));
				FileUtil.SetPermission(file, fsPermission);
			}
		}

		/// <summary>The factory to create JksProviders, which is used by the ServiceLoader.</summary>
		public class Factory : CredentialProviderFactory
		{
			/// <exception cref="System.IO.IOException"/>
			public override CredentialProvider CreateProvider(URI providerName, Configuration
				 conf)
			{
				if (SchemeName.Equals(providerName.GetScheme()))
				{
					return new LocalJavaKeyStoreProvider(providerName, conf);
				}
				return null;
			}
		}

		private static ICollection<PosixFilePermission> ModeToPosixFilePermission(int mode
			)
		{
			ICollection<PosixFilePermission> perms = EnumSet.NoneOf<PosixFilePermission>();
			if ((mode & 0x1) != 0)
			{
				perms.AddItem(PosixFilePermission.OthersExecute);
			}
			if ((mode & 0x2) != 0)
			{
				perms.AddItem(PosixFilePermission.OthersWrite);
			}
			if ((mode & 0x4) != 0)
			{
				perms.AddItem(PosixFilePermission.OthersRead);
			}
			if ((mode & 0x8) != 0)
			{
				perms.AddItem(PosixFilePermission.GroupExecute);
			}
			if ((mode & 0x10) != 0)
			{
				perms.AddItem(PosixFilePermission.GroupWrite);
			}
			if ((mode & 0x20) != 0)
			{
				perms.AddItem(PosixFilePermission.GroupRead);
			}
			if ((mode & 0x40) != 0)
			{
				perms.AddItem(PosixFilePermission.OwnerExecute);
			}
			if ((mode & 0x80) != 0)
			{
				perms.AddItem(PosixFilePermission.OwnerWrite);
			}
			if ((mode & 0x100) != 0)
			{
				perms.AddItem(PosixFilePermission.OwnerRead);
			}
			return perms;
		}
	}
}
