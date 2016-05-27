using System;
using System.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.FS
{
	/// <summary>Abstract Checksumed Fs.</summary>
	/// <remarks>
	/// Abstract Checksumed Fs.
	/// It provide a basic implementation of a Checksumed Fs,
	/// which creates a checksum file for each raw file.
	/// It generates & verifies checksums at the client side.
	/// </remarks>
	public abstract class ChecksumFs : FilterFs
	{
		private static readonly byte[] ChecksumVersion = new byte[] { (byte)('c'), (byte)
			('r'), (byte)('c'), 0 };

		private int defaultBytesPerChecksum = 512;

		private bool verifyChecksum = true;

		/*Evolving for a release,to be changed to Stable */
		public static double GetApproxChkSumLength(long size)
		{
			return ChecksumFs.ChecksumFSOutputSummer.ChksumAsFraction * size;
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Sharpen.URISyntaxException"/>
		public ChecksumFs(AbstractFileSystem theFs)
			: base(theFs)
		{
			defaultBytesPerChecksum = GetMyFs().GetServerDefaults().GetBytesPerChecksum();
		}

		/// <summary>Set whether to verify checksum.</summary>
		public override void SetVerifyChecksum(bool inVerifyChecksum)
		{
			this.verifyChecksum = inVerifyChecksum;
		}

		/// <summary>get the raw file system.</summary>
		public virtual AbstractFileSystem GetRawFs()
		{
			return GetMyFs();
		}

		/// <summary>Return the name of the checksum file associated with a file.</summary>
		public virtual Path GetChecksumFile(Path file)
		{
			return new Path(file.GetParent(), "." + file.GetName() + ".crc");
		}

		/// <summary>Return true iff file is a checksum file name.</summary>
		public static bool IsChecksumFile(Path file)
		{
			string name = file.GetName();
			return name.StartsWith(".") && name.EndsWith(".crc");
		}

		/// <summary>
		/// Return the length of the checksum file given the size of the
		/// actual file.
		/// </summary>
		public virtual long GetChecksumFileLength(Path file, long fileSize)
		{
			return GetChecksumLength(fileSize, GetBytesPerSum());
		}

		/// <summary>Return the bytes Per Checksum.</summary>
		public virtual int GetBytesPerSum()
		{
			return defaultBytesPerChecksum;
		}

		/// <exception cref="System.IO.IOException"/>
		private int GetSumBufferSize(int bytesPerSum, int bufferSize)
		{
			int defaultBufferSize = GetMyFs().GetServerDefaults().GetFileBufferSize();
			int proportionalBufferSize = bufferSize / bytesPerSum;
			return Math.Max(bytesPerSum, Math.Max(proportionalBufferSize, defaultBufferSize));
		}

		/// <summary>
		/// For open()'s FSInputStream
		/// It verifies that data matches checksums.
		/// </summary>
		private class ChecksumFSInputChecker : FSInputChecker
		{
			public static readonly Log Log = LogFactory.GetLog(typeof(FSInputChecker));

			private const int HeaderLength = 8;

			private ChecksumFs fs;

			private FSDataInputStream datas;

			private FSDataInputStream sums;

			private int bytesPerSum = 1;

			private long fileLen = -1L;

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
			public ChecksumFSInputChecker(ChecksumFs fs, Path file)
				: this(fs, file, fs.GetServerDefaults().GetFileBufferSize())
			{
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
			public ChecksumFSInputChecker(ChecksumFs fs, Path file, int bufferSize)
				: base(file, fs.GetFileStatus(file).GetReplication())
			{
				this.datas = fs.GetRawFs().Open(file, bufferSize);
				this.fs = fs;
				Path sumFile = fs.GetChecksumFile(file);
				try
				{
					int sumBufferSize = fs.GetSumBufferSize(fs.GetBytesPerSum(), bufferSize);
					sums = fs.GetRawFs().Open(sumFile, sumBufferSize);
					byte[] version = new byte[ChecksumVersion.Length];
					sums.ReadFully(version);
					if (!Arrays.Equals(version, ChecksumVersion))
					{
						throw new IOException("Not a checksum file: " + sumFile);
					}
					this.bytesPerSum = sums.ReadInt();
					Set(fs.verifyChecksum, DataChecksum.NewCrc32(), bytesPerSum, 4);
				}
				catch (FileNotFoundException)
				{
					// quietly ignore
					Set(fs.verifyChecksum, null, 1, 0);
				}
				catch (IOException e)
				{
					// loudly ignore
					Log.Warn("Problem opening checksum file: " + file + ".  Ignoring exception: ", e);
					Set(fs.verifyChecksum, null, 1, 0);
				}
			}

			private long GetChecksumFilePos(long dataPos)
			{
				return HeaderLength + 4 * (dataPos / bytesPerSum);
			}

			protected internal override long GetChunkPosition(long dataPos)
			{
				return dataPos / bytesPerSum * bytesPerSum;
			}

			/// <exception cref="System.IO.IOException"/>
			public override int Available()
			{
				return datas.Available() + base.Available();
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
			public override int Read(long position, byte[] b, int off, int len)
			{
				// parameter check
				if ((off | len | (off + len) | (b.Length - (off + len))) < 0)
				{
					throw new IndexOutOfRangeException();
				}
				else
				{
					if (len == 0)
					{
						return 0;
					}
				}
				if (position < 0)
				{
					throw new ArgumentException("Parameter position can not to be negative");
				}
				ChecksumFs.ChecksumFSInputChecker checker = new ChecksumFs.ChecksumFSInputChecker
					(fs, file);
				checker.Seek(position);
				int nread = checker.Read(b, off, len);
				checker.Close();
				return nread;
			}

			/// <exception cref="System.IO.IOException"/>
			public override void Close()
			{
				datas.Close();
				if (sums != null)
				{
					sums.Close();
				}
				Set(fs.verifyChecksum, null, 1, 0);
			}

			/// <exception cref="System.IO.IOException"/>
			public override bool SeekToNewSource(long targetPos)
			{
				long sumsPos = GetChecksumFilePos(targetPos);
				fs.ReportChecksumFailure(file, datas, targetPos, sums, sumsPos);
				bool newDataSource = datas.SeekToNewSource(targetPos);
				return sums.SeekToNewSource(sumsPos) || newDataSource;
			}

			/// <exception cref="System.IO.IOException"/>
			protected internal override int ReadChunk(long pos, byte[] buf, int offset, int len
				, byte[] checksum)
			{
				bool eof = false;
				if (NeedChecksum())
				{
					System.Diagnostics.Debug.Assert(checksum != null);
					// we have a checksum buffer
					System.Diagnostics.Debug.Assert(checksum.Length % ChecksumSize == 0);
					// it is sane length
					System.Diagnostics.Debug.Assert(len >= bytesPerSum);
					// we must read at least one chunk
					int checksumsToRead = Math.Min(len / bytesPerSum, checksum.Length / ChecksumSize);
					// number of checksums based on len to read
					// size of checksum buffer
					long checksumPos = GetChecksumFilePos(pos);
					if (checksumPos != sums.GetPos())
					{
						sums.Seek(checksumPos);
					}
					int sumLenRead = sums.Read(checksum, 0, ChecksumSize * checksumsToRead);
					if (sumLenRead >= 0 && sumLenRead % ChecksumSize != 0)
					{
						throw new EOFException("Checksum file not a length multiple of checksum size " + 
							"in " + file + " at " + pos + " checksumpos: " + checksumPos + " sumLenread: " +
							 sumLenRead);
					}
					if (sumLenRead <= 0)
					{
						// we're at the end of the file
						eof = true;
					}
					else
					{
						// Adjust amount of data to read based on how many checksum chunks we read
						len = Math.Min(len, bytesPerSum * (sumLenRead / ChecksumSize));
					}
				}
				if (pos != datas.GetPos())
				{
					datas.Seek(pos);
				}
				int nread = ReadFully(datas, buf, offset, len);
				if (eof && nread > 0)
				{
					throw new ChecksumException("Checksum error: " + file + " at " + pos, pos);
				}
				return nread;
			}

			/* Return the file length */
			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
			private long GetFileLength()
			{
				if (fileLen == -1L)
				{
					fileLen = fs.GetFileStatus(file).GetLen();
				}
				return fileLen;
			}

			/// <summary>
			/// Skips over and discards <code>n</code> bytes of data from the
			/// input stream.
			/// </summary>
			/// <remarks>
			/// Skips over and discards <code>n</code> bytes of data from the
			/// input stream.
			/// The <code>skip</code> method skips over some smaller number of bytes
			/// when reaching end of file before <code>n</code> bytes have been skipped.
			/// The actual number of bytes skipped is returned.  If <code>n</code> is
			/// negative, no bytes are skipped.
			/// </remarks>
			/// <param name="n">the number of bytes to be skipped.</param>
			/// <returns>the actual number of bytes skipped.</returns>
			/// <exception>
			/// IOException
			/// if an I/O error occurs.
			/// ChecksumException if the chunk to skip to is corrupted
			/// </exception>
			/// <exception cref="System.IO.IOException"/>
			public override long Skip(long n)
			{
				lock (this)
				{
					long curPos = GetPos();
					long fileLength = GetFileLength();
					if (n + curPos > fileLength)
					{
						n = fileLength - curPos;
					}
					return base.Skip(n);
				}
			}

			/// <summary>Seek to the given position in the stream.</summary>
			/// <remarks>
			/// Seek to the given position in the stream.
			/// The next read() will be from that position.
			/// <p>This method does not allow seek past the end of the file.
			/// This produces IOException.
			/// </remarks>
			/// <param name="pos">the postion to seek to.</param>
			/// <exception>
			/// IOException
			/// if an I/O error occurs or seeks after EOF
			/// ChecksumException if the chunk to seek to is corrupted
			/// </exception>
			/// <exception cref="System.IO.IOException"/>
			public override void Seek(long pos)
			{
				lock (this)
				{
					if (pos > GetFileLength())
					{
						throw new IOException("Cannot seek after EOF");
					}
					base.Seek(pos);
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override bool Truncate(Path f, long newLength)
		{
			throw new IOException("Not supported");
		}

		/// <summary>Opens an FSDataInputStream at the indicated Path.</summary>
		/// <param name="f">the file name to open</param>
		/// <param name="bufferSize">the size of the buffer to be used.</param>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		public override FSDataInputStream Open(Path f, int bufferSize)
		{
			return new FSDataInputStream(new ChecksumFs.ChecksumFSInputChecker(this, f, bufferSize
				));
		}

		/// <summary>Calculated the length of the checksum file in bytes.</summary>
		/// <param name="size">the length of the data file in bytes</param>
		/// <param name="bytesPerSum">the number of bytes in a checksum block</param>
		/// <returns>the number of bytes in the checksum file</returns>
		public static long GetChecksumLength(long size, int bytesPerSum)
		{
			//the checksum length is equal to size passed divided by bytesPerSum +
			//bytes written in the beginning of the checksum file.  
			return ((size + bytesPerSum - 1) / bytesPerSum) * 4 + ChecksumVersion.Length + 4;
		}

		/// <summary>This class provides an output stream for a checksummed file.</summary>
		/// <remarks>
		/// This class provides an output stream for a checksummed file.
		/// It generates checksums for data.
		/// </remarks>
		private class ChecksumFSOutputSummer : FSOutputSummer
		{
			private FSDataOutputStream datas;

			private FSDataOutputStream sums;

			private const float ChksumAsFraction = 0.01f;

			private bool isClosed = false;

			/// <exception cref="System.IO.IOException"/>
			public ChecksumFSOutputSummer(ChecksumFs fs, Path file, EnumSet<CreateFlag> createFlag
				, FsPermission absolutePermission, int bufferSize, short replication, long blockSize
				, Progressable progress, Options.ChecksumOpt checksumOpt, bool createParent)
				: base(DataChecksum.NewDataChecksum(DataChecksum.Type.Crc32, fs.GetBytesPerSum())
					)
			{
				// checksumOpt is passed down to the raw fs. Unless it implements
				// checksum impelemts internally, checksumOpt will be ignored.
				// If the raw fs does checksum internally, we will end up with
				// two layers of checksumming. i.e. checksumming checksum file.
				this.datas = fs.GetRawFs().CreateInternal(file, createFlag, absolutePermission, bufferSize
					, replication, blockSize, progress, checksumOpt, createParent);
				// Now create the chekcsumfile; adjust the buffsize
				int bytesPerSum = fs.GetBytesPerSum();
				int sumBufferSize = fs.GetSumBufferSize(bytesPerSum, bufferSize);
				this.sums = fs.GetRawFs().CreateInternal(fs.GetChecksumFile(file), EnumSet.Of(CreateFlag
					.Create, CreateFlag.Overwrite), absolutePermission, sumBufferSize, replication, 
					blockSize, progress, checksumOpt, createParent);
				sums.Write(ChecksumVersion, 0, ChecksumVersion.Length);
				sums.WriteInt(bytesPerSum);
			}

			/// <exception cref="System.IO.IOException"/>
			public override void Close()
			{
				try
				{
					FlushBuffer();
					sums.Close();
					datas.Close();
				}
				finally
				{
					isClosed = true;
				}
			}

			/// <exception cref="System.IO.IOException"/>
			protected internal override void WriteChunk(byte[] b, int offset, int len, byte[]
				 checksum, int ckoff, int cklen)
			{
				datas.Write(b, offset, len);
				sums.Write(checksum, ckoff, cklen);
			}

			/// <exception cref="System.IO.IOException"/>
			protected internal override void CheckClosed()
			{
				if (isClosed)
				{
					throw new ClosedChannelException();
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override FSDataOutputStream CreateInternal(Path f, EnumSet<CreateFlag> createFlag
			, FsPermission absolutePermission, int bufferSize, short replication, long blockSize
			, Progressable progress, Options.ChecksumOpt checksumOpt, bool createParent)
		{
			FSDataOutputStream @out = new FSDataOutputStream(new ChecksumFs.ChecksumFSOutputSummer
				(this, f, createFlag, absolutePermission, bufferSize, replication, blockSize, progress
				, checksumOpt, createParent), null);
			return @out;
		}

		/// <summary>Check if exists.</summary>
		/// <param name="f">source file</param>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		private bool Exists(Path f)
		{
			try
			{
				return GetMyFs().GetFileStatus(f) != null;
			}
			catch (FileNotFoundException)
			{
				return false;
			}
		}

		/// <summary>True iff the named path is a directory.</summary>
		/// <remarks>
		/// True iff the named path is a directory.
		/// Note: Avoid using this method. Instead reuse the FileStatus
		/// returned by getFileStatus() or listStatus() methods.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		private bool IsDirectory(Path f)
		{
			try
			{
				return GetMyFs().GetFileStatus(f).IsDirectory();
			}
			catch (FileNotFoundException)
			{
				return false;
			}
		}

		// f does not exist
		/// <summary>Set replication for an existing file.</summary>
		/// <remarks>
		/// Set replication for an existing file.
		/// Implement the abstract <tt>setReplication</tt> of <tt>FileSystem</tt>
		/// </remarks>
		/// <param name="src">file name</param>
		/// <param name="replication">new replication</param>
		/// <exception cref="System.IO.IOException"/>
		/// <returns>
		/// true if successful;
		/// false if file does not exist or is a directory
		/// </returns>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		public override bool SetReplication(Path src, short replication)
		{
			bool value = GetMyFs().SetReplication(src, replication);
			if (!value)
			{
				return false;
			}
			Path checkFile = GetChecksumFile(src);
			if (Exists(checkFile))
			{
				GetMyFs().SetReplication(checkFile, replication);
			}
			return true;
		}

		/// <summary>Rename files/dirs.</summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		public override void RenameInternal(Path src, Path dst)
		{
			if (IsDirectory(src))
			{
				GetMyFs().Rename(src, dst);
			}
			else
			{
				GetMyFs().Rename(src, dst);
				Path checkFile = GetChecksumFile(src);
				if (Exists(checkFile))
				{
					//try to rename checksum
					if (IsDirectory(dst))
					{
						GetMyFs().Rename(checkFile, dst);
					}
					else
					{
						GetMyFs().Rename(checkFile, GetChecksumFile(dst));
					}
				}
			}
		}

		/// <summary>
		/// Implement the delete(Path, boolean) in checksum
		/// file system.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		public override bool Delete(Path f, bool recursive)
		{
			FileStatus fstatus = null;
			try
			{
				fstatus = GetMyFs().GetFileStatus(f);
			}
			catch (FileNotFoundException)
			{
				return false;
			}
			if (fstatus.IsDirectory())
			{
				//this works since the crcs are in the same
				//directories and the files. so we just delete
				//everything in the underlying filesystem
				return GetMyFs().Delete(f, recursive);
			}
			else
			{
				Path checkFile = GetChecksumFile(f);
				if (Exists(checkFile))
				{
					GetMyFs().Delete(checkFile, true);
				}
				return GetMyFs().Delete(f, true);
			}
		}

		/// <summary>Report a checksum error to the file system.</summary>
		/// <param name="f">the file name containing the error</param>
		/// <param name="in">the stream open on the file</param>
		/// <param name="inPos">the position of the beginning of the bad data in the file</param>
		/// <param name="sums">the stream open on the checksum file</param>
		/// <param name="sumsPos">
		/// the position of the beginning of the bad data in the
		/// checksum file
		/// </param>
		/// <returns>if retry is neccessary</returns>
		public virtual bool ReportChecksumFailure(Path f, FSDataInputStream @in, long inPos
			, FSDataInputStream sums, long sumsPos)
		{
			return false;
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="Org.Apache.Hadoop.FS.UnresolvedLinkException"/>
		public override FileStatus[] ListStatus(Path f)
		{
			AList<FileStatus> results = new AList<FileStatus>();
			FileStatus[] listing = GetMyFs().ListStatus(f);
			if (listing != null)
			{
				for (int i = 0; i < listing.Length; i++)
				{
					if (!IsChecksumFile(listing[i].GetPath()))
					{
						results.AddItem(listing[i]);
					}
				}
			}
			return Sharpen.Collections.ToArray(results, new FileStatus[results.Count]);
		}
	}
}
