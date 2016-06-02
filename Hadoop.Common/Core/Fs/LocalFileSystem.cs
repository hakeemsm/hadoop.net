using System;
using System.IO;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Path = Org.Apache.Hadoop.FS.Path;

namespace Hadoop.Common.Core.Fs
{
	/// <summary>Implement the FileSystem API for the checksumed local filesystem.</summary>
	public class LocalFileSystem : ChecksumFileSystem
	{
		internal static readonly Uri Name =  new Uri("file:///");

		private static Random rand = new Random();

		public LocalFileSystem()
			: this(new RawLocalFileSystem())
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Initialize(Uri name, Configuration conf)
		{
			if (fs.GetConf() == null)
			{
				fs.Initialize(name, conf);
			}
			string scheme = name.Scheme;
			if (!scheme.Equals(fs.GetUri().Scheme))
			{
				swapScheme = scheme;
			}
		}

		/// <summary>Return the protocol scheme for the FileSystem.</summary>
		/// <remarks>
		/// Return the protocol scheme for the FileSystem.
		/// <p/>
		/// </remarks>
		/// <returns><code>file</code></returns>
		public override string GetScheme()
		{
			return "file";
		}

		public virtual FileSystem GetRaw()
		{
			return GetRawFileSystem();
		}

		public LocalFileSystem(FileSystem rawLocalFileSystem)
			: base(rawLocalFileSystem)
		{
		}

		/// <summary>Convert a path to a File.</summary>
		public virtual FilePath PathToFile(Path path)
		{
			return ((RawLocalFileSystem)fs).PathToFile(path);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void CopyFromLocalFile(bool delSrc, Path src, Path dst)
		{
			FileUtil.Copy(this, src, this, dst, delSrc, GetConf());
		}

		/// <exception cref="System.IO.IOException"/>
		public override void CopyToLocalFile(bool delSrc, Path src, Path dst)
		{
			FileUtil.Copy(this, src, this, dst, delSrc, GetConf());
		}

		/// <summary>
		/// Moves files to a bad file directory on the same device, so that their
		/// storage will not be reused.
		/// </summary>
		public override bool ReportChecksumFailure(Path p, FSDataInputStream @in, long inPos
			, FSDataInputStream sums, long sumsPos)
		{
			try
			{
				// canonicalize f
				FilePath f = ((RawLocalFileSystem)fs).PathToFile(p).GetCanonicalFile();
				// find highest writable parent dir of f on the same device
				string device = new DF(f, GetConf()).GetMount();
				FilePath parent = f.GetParentFile();
				FilePath dir = null;
				while (parent != null && FileUtil.CanWrite(parent) && parent.ToString().StartsWith
					(device))
				{
					dir = parent;
					parent = parent.GetParentFile();
				}
				if (dir == null)
				{
					throw new IOException("not able to find the highest writable parent dir");
				}
				// move the file there
				FilePath badDir = new FilePath(dir, "bad_files");
				if (!badDir.Mkdirs())
				{
					if (!badDir.IsDirectory())
					{
						throw new IOException("Mkdirs failed to create " + badDir.ToString());
					}
				}
				string suffix = "." + rand.Next();
				FilePath badFile = new FilePath(badDir, f.GetName() + suffix);
				Log.Warn("Moving bad file " + f + " to " + badFile);
				@in.Close();
				// close it first
				bool b = f.RenameTo(badFile);
				// rename it
				if (!b)
				{
					Log.Warn("Ignoring failure of renameTo");
				}
				// move checksum file too
				FilePath checkFile = ((RawLocalFileSystem)fs).PathToFile(GetChecksumFile(p));
				// close the stream before rename to release the file handle
				sums.Close();
				b = checkFile.RenameTo(new FilePath(badDir, checkFile.GetName() + suffix));
				if (!b)
				{
					Log.Warn("Ignoring failure of renameTo");
				}
			}
			catch (IOException e)
			{
				Log.Warn("Error moving bad file " + p + ": " + e);
			}
			return false;
		}

		public override bool SupportsSymlinks()
		{
			return true;
		}

		/// <exception cref="System.IO.IOException"/>
		public override void CreateSymlink(Path target, Path link, bool createParent)
		{
			fs.CreateSymlink(target, link, createParent);
		}

		/// <exception cref="System.IO.IOException"/>
		public override FileStatus GetFileLinkStatus(Path f)
		{
			return fs.GetFileLinkStatus(f);
		}

		/// <exception cref="System.IO.IOException"/>
		public override Path GetLinkTarget(Path f)
		{
			return fs.GetLinkTarget(f);
		}
	}
}
