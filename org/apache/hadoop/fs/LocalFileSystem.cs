using Sharpen;

namespace org.apache.hadoop.fs
{
	/// <summary>Implement the FileSystem API for the checksumed local filesystem.</summary>
	public class LocalFileSystem : org.apache.hadoop.fs.ChecksumFileSystem
	{
		internal static readonly java.net.URI NAME = java.net.URI.create("file:///");

		private static java.util.Random rand = new java.util.Random();

		public LocalFileSystem()
			: this(new org.apache.hadoop.fs.RawLocalFileSystem())
		{
		}

		/// <exception cref="System.IO.IOException"/>
		public override void initialize(java.net.URI name, org.apache.hadoop.conf.Configuration
			 conf)
		{
			if (fs.getConf() == null)
			{
				fs.initialize(name, conf);
			}
			string scheme = name.getScheme();
			if (!scheme.Equals(fs.getUri().getScheme()))
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
		public override string getScheme()
		{
			return "file";
		}

		public virtual org.apache.hadoop.fs.FileSystem getRaw()
		{
			return getRawFileSystem();
		}

		public LocalFileSystem(org.apache.hadoop.fs.FileSystem rawLocalFileSystem)
			: base(rawLocalFileSystem)
		{
		}

		/// <summary>Convert a path to a File.</summary>
		public virtual java.io.File pathToFile(org.apache.hadoop.fs.Path path)
		{
			return ((org.apache.hadoop.fs.RawLocalFileSystem)fs).pathToFile(path);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void copyFromLocalFile(bool delSrc, org.apache.hadoop.fs.Path src
			, org.apache.hadoop.fs.Path dst)
		{
			org.apache.hadoop.fs.FileUtil.copy(this, src, this, dst, delSrc, getConf());
		}

		/// <exception cref="System.IO.IOException"/>
		public override void copyToLocalFile(bool delSrc, org.apache.hadoop.fs.Path src, 
			org.apache.hadoop.fs.Path dst)
		{
			org.apache.hadoop.fs.FileUtil.copy(this, src, this, dst, delSrc, getConf());
		}

		/// <summary>
		/// Moves files to a bad file directory on the same device, so that their
		/// storage will not be reused.
		/// </summary>
		public override bool reportChecksumFailure(org.apache.hadoop.fs.Path p, org.apache.hadoop.fs.FSDataInputStream
			 @in, long inPos, org.apache.hadoop.fs.FSDataInputStream sums, long sumsPos)
		{
			try
			{
				// canonicalize f
				java.io.File f = ((org.apache.hadoop.fs.RawLocalFileSystem)fs).pathToFile(p).getCanonicalFile
					();
				// find highest writable parent dir of f on the same device
				string device = new org.apache.hadoop.fs.DF(f, getConf()).getMount();
				java.io.File parent = f.getParentFile();
				java.io.File dir = null;
				while (parent != null && org.apache.hadoop.fs.FileUtil.canWrite(parent) && parent
					.ToString().StartsWith(device))
				{
					dir = parent;
					parent = parent.getParentFile();
				}
				if (dir == null)
				{
					throw new System.IO.IOException("not able to find the highest writable parent dir"
						);
				}
				// move the file there
				java.io.File badDir = new java.io.File(dir, "bad_files");
				if (!badDir.mkdirs())
				{
					if (!badDir.isDirectory())
					{
						throw new System.IO.IOException("Mkdirs failed to create " + badDir.ToString());
					}
				}
				string suffix = "." + rand.nextInt();
				java.io.File badFile = new java.io.File(badDir, f.getName() + suffix);
				LOG.warn("Moving bad file " + f + " to " + badFile);
				@in.close();
				// close it first
				bool b = f.renameTo(badFile);
				// rename it
				if (!b)
				{
					LOG.warn("Ignoring failure of renameTo");
				}
				// move checksum file too
				java.io.File checkFile = ((org.apache.hadoop.fs.RawLocalFileSystem)fs).pathToFile
					(getChecksumFile(p));
				// close the stream before rename to release the file handle
				sums.close();
				b = checkFile.renameTo(new java.io.File(badDir, checkFile.getName() + suffix));
				if (!b)
				{
					LOG.warn("Ignoring failure of renameTo");
				}
			}
			catch (System.IO.IOException e)
			{
				LOG.warn("Error moving bad file " + p + ": " + e);
			}
			return false;
		}

		public override bool supportsSymlinks()
		{
			return true;
		}

		/// <exception cref="System.IO.IOException"/>
		public override void createSymlink(org.apache.hadoop.fs.Path target, org.apache.hadoop.fs.Path
			 link, bool createParent)
		{
			fs.createSymlink(target, link, createParent);
		}

		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.FileStatus getFileLinkStatus(org.apache.hadoop.fs.Path
			 f)
		{
			return fs.getFileLinkStatus(f);
		}

		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.Path getLinkTarget(org.apache.hadoop.fs.Path
			 f)
		{
			return fs.getLinkTarget(f);
		}
	}
}
