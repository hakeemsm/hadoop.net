using System.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.IO.Nativeio;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Util
{
	/// <summary>
	/// A FileOutputStream that has the property that it will only show
	/// up at its destination once it has been entirely written and flushed
	/// to disk.
	/// </summary>
	/// <remarks>
	/// A FileOutputStream that has the property that it will only show
	/// up at its destination once it has been entirely written and flushed
	/// to disk. While being written, it will use a .tmp suffix.
	/// When the output stream is closed, it is flushed, fsynced, and
	/// will be moved into place, overwriting any file that already
	/// exists at that location.
	/// <b>NOTE</b>: on Windows platforms, it will not atomically
	/// replace the target file - instead the target file is deleted
	/// before this one is moved into place.
	/// </remarks>
	public class AtomicFileOutputStream : FilterOutputStream
	{
		private const string TmpExtension = ".tmp";

		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Hdfs.Util.AtomicFileOutputStream
			));

		private readonly FilePath origFile;

		private readonly FilePath tmpFile;

		/// <exception cref="System.IO.FileNotFoundException"/>
		public AtomicFileOutputStream(FilePath f)
			: base(new FileOutputStream(new FilePath(f.GetParentFile(), f.GetName() + TmpExtension
				)))
		{
			// Code unfortunately must be duplicated below since we can't assign anything
			// before calling super
			origFile = f.GetAbsoluteFile();
			tmpFile = new FilePath(f.GetParentFile(), f.GetName() + TmpExtension).GetAbsoluteFile
				();
		}

		/// <exception cref="System.IO.IOException"/>
		public override void Close()
		{
			bool triedToClose = false;
			bool success = false;
			try
			{
				Flush();
				((FileOutputStream)@out).GetChannel().Force(true);
				triedToClose = true;
				base.Close();
				success = true;
			}
			finally
			{
				if (success)
				{
					bool renamed = tmpFile.RenameTo(origFile);
					if (!renamed)
					{
						// On windows, renameTo does not replace.
						if (origFile.Exists() && !origFile.Delete())
						{
							throw new IOException("Could not delete original file " + origFile);
						}
						try
						{
							NativeIO.RenameTo(tmpFile, origFile);
						}
						catch (NativeIOException e)
						{
							throw new IOException("Could not rename temporary file " + tmpFile + " to " + origFile
								 + " due to failure in native rename. " + e.ToString());
						}
					}
				}
				else
				{
					if (!triedToClose)
					{
						// If we failed when flushing, try to close it to not leak an FD
						IOUtils.CloseStream(@out);
					}
					// close wasn't successful, try to delete the tmp file
					if (!tmpFile.Delete())
					{
						Log.Warn("Unable to delete tmp file " + tmpFile);
					}
				}
			}
		}

		/// <summary>
		/// Close the atomic file, but do not "commit" the temporary file
		/// on top of the destination.
		/// </summary>
		/// <remarks>
		/// Close the atomic file, but do not "commit" the temporary file
		/// on top of the destination. This should be used if there is a failure
		/// in writing.
		/// </remarks>
		public virtual void Abort()
		{
			try
			{
				base.Close();
			}
			catch (IOException ioe)
			{
				Log.Warn("Unable to abort file " + tmpFile, ioe);
			}
			if (!tmpFile.Delete())
			{
				Log.Warn("Unable to delete tmp file during abort " + tmpFile);
			}
		}
	}
}
