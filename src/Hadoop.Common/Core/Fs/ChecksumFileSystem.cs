using System;
using System.IO;
using Hadoop.Common.Core.Conf;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Permission;
using Org.Apache.Hadoop.Util;
using Path = Org.Apache.Hadoop.FS.Path;

namespace Hadoop.Common.Core.Fs
{
	/// <summary>Abstract Checksumed FileSystem.</summary>
	/// <remarks>
	/// Abstract Checksumed FileSystem.
	/// It provide a basic implementation of a Checksumed FileSystem,
	/// which creates a checksum file for each raw file.
	/// It generates & verifies checksums at the client side.
	/// </remarks>
	public abstract class ChecksumFileSystem : FilterFileSystem
	{
		private static readonly byte[] ChecksumVersion = new byte[] { (byte)('c'), (byte)
			('r'), (byte)('c'), 0 };

		private int bytesPerChecksum = 512;

		private bool verifyChecksum = true;

		private bool writeChecksum = true;

		public static double GetApproxChkSumLength(long size)
		{
			return ChecksumFileSystem.ChecksumFSOutputSummer.ChksumAsFraction * size;
		}

		public ChecksumFileSystem(FileSystem fs)
			: base(fs)
		{
		}

		public override void SetConf(Configuration conf)
		{
			base.SetConf(conf);
			if (conf != null)
			{
				bytesPerChecksum = conf.GetInt(LocalFileSystemConfigKeys.LocalFsBytesPerChecksumKey
					, LocalFileSystemConfigKeys.LocalFsBytesPerChecksumDefault);
			}
		}

		/// <summary>Set whether to verify checksum.</summary>
		public override void SetVerifyChecksum(bool verifyChecksum)
		{
			this.verifyChecksum = verifyChecksum;
		}

		public override void SetWriteChecksum(bool writeChecksum)
		{
			this.writeChecksum = writeChecksum;
		}

		/// <summary>get the raw file system</summary>
		public override FileSystem GetRawFileSystem()
		{
			return fs;
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

		/// <summary>Return the bytes Per Checksum</summary>
		public virtual int GetBytesPerSum()
		{
			return bytesPerChecksum;
		}

		private int GetSumBufferSize(int bytesPerSum, int bufferSize)
		{
			int defaultBufferSize = GetConf().GetInt(LocalFileSystemConfigKeys.LocalFsStreamBufferSizeKey
				, LocalFileSystemConfigKeys.LocalFsStreamBufferSizeDefault);
			int proportionalBufferSize = bufferSize / bytesPerSum;
			return Math.Max(bytesPerSum, Math.Max(proportionalBufferSize, defaultBufferSize));
		}

		/// <summary>
		/// For open()'s FSInputStream
		/// It verifies that data matches checksums.
		/// </summary>
		private class ChecksumFSInputChecker : FSInputChecker
		{
			private ChecksumFileSystem fs;

			private FSDataInputStream datas;

			private FSDataInputStream sums;

			private const int HeaderLength = 8;

			private int bytesPerSum = 1;

			/// <exception cref="System.IO.IOException"/>
			public ChecksumFSInputChecker(ChecksumFileSystem fs, Path file)
				: this(fs, file, fs.GetConf().GetInt(LocalFileSystemConfigKeys.LocalFsStreamBufferSizeKey
					, LocalFileSystemConfigKeys.LocalFsStreamBufferSizeDefault))
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public ChecksumFSInputChecker(ChecksumFileSystem fs, Path file, int bufferSize)
				: base(file, fs.GetFileStatus(file).GetReplication())
			{
				this.datas = fs.GetRawFileSystem().Open(file, bufferSize);
				this.fs = fs;
				Path sumFile = fs.GetChecksumFile(file);
				try
				{
					int sumBufferSize = fs.GetSumBufferSize(fs.GetBytesPerSum(), bufferSize);
					sums = fs.GetRawFileSystem().Open(sumFile, sumBufferSize);
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
				ChecksumFileSystem.ChecksumFSInputChecker checker = new ChecksumFileSystem.ChecksumFSInputChecker
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
						throw new ChecksumException("Checksum file not a length multiple of checksum size "
							 + "in " + file + " at " + pos + " checksumpos: " + checksumPos + " sumLenread: "
							 + sumLenRead, pos);
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
		}

		private class FSDataBoundedInputStream : FSDataInputStream
		{
			private FileSystem fs;

			private Path file;

			private long fileLen = -1L;

			internal FSDataBoundedInputStream(FileSystem fs, Path file, InputStream @in)
				: base(@in)
			{
				this.fs = fs;
				this.file = file;
			}

			public override bool MarkSupported()
			{
				return false;
			}

			/* Return the file length */
			/// <exception cref="System.IO.IOException"/>
			private long GetFileLength()
			{
				if (fileLen == -1L)
				{
					fileLen = fs.GetContentSummary(file).GetLength();
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
						throw new EOFException("Cannot seek after EOF");
					}
					base.Seek(pos);
				}
			}
		}

		/// <summary>Opens an FSDataInputStream at the indicated Path.</summary>
		/// <param name="f">the file name to open</param>
		/// <param name="bufferSize">the size of the buffer to be used.</param>
		/// <exception cref="System.IO.IOException"/>
		public override FSDataInputStream Open(Path f, int bufferSize)
		{
			FileSystem fs;
			InputStream @in;
			if (verifyChecksum)
			{
				fs = this;
				@in = new ChecksumFileSystem.ChecksumFSInputChecker(this, f, bufferSize);
			}
			else
			{
				fs = GetRawFileSystem();
				@in = fs.Open(f, bufferSize);
			}
			return new ChecksumFileSystem.FSDataBoundedInputStream(fs, f, @in);
		}

		/// <exception cref="System.IO.IOException"/>
		public override FSDataOutputStream Append(Path f, int bufferSize, Progressable progress
			)
		{
			throw new IOException("Not supported");
		}

		/// <exception cref="System.IO.IOException"/>
		public override bool Truncate(Path f, long newLength)
		{
			throw new IOException("Not supported");
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
			public ChecksumFSOutputSummer(ChecksumFileSystem fs, Path file, bool overwrite, int
				 bufferSize, short replication, long blockSize, Progressable progress, FsPermission
				 permission)
				: base(DataChecksum.NewDataChecksum(DataChecksum.Type.Crc32, fs.GetBytesPerSum())
					)
			{
				int bytesPerSum = fs.GetBytesPerSum();
				this.datas = fs.GetRawFileSystem().Create(file, permission, overwrite, bufferSize
					, replication, blockSize, progress);
				int sumBufferSize = fs.GetSumBufferSize(bytesPerSum, bufferSize);
				this.sums = fs.GetRawFileSystem().Create(fs.GetChecksumFile(file), permission, true
					, sumBufferSize, replication, blockSize, null);
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
		public override FSDataOutputStream Create(Path f, FsPermission permission, bool overwrite
			, int bufferSize, short replication, long blockSize, Progressable progress)
		{
			return Create(f, permission, overwrite, true, bufferSize, replication, blockSize, 
				progress);
		}

		/// <exception cref="System.IO.IOException"/>
		private FSDataOutputStream Create(Path f, FsPermission permission, bool overwrite
			, bool createParent, int bufferSize, short replication, long blockSize, Progressable
			 progress)
		{
			Path parent = f.GetParent();
			if (parent != null)
			{
				if (!createParent && !Exists(parent))
				{
					throw new FileNotFoundException("Parent directory doesn't exist: " + parent);
				}
				else
				{
					if (!Mkdirs(parent))
					{
						throw new IOException("Mkdirs failed to create " + parent + " (exists=" + Exists(
							parent) + ", cwd=" + GetWorkingDirectory() + ")");
					}
				}
			}
			FSDataOutputStream @out;
			if (writeChecksum)
			{
				@out = new FSDataOutputStream(new ChecksumFileSystem.ChecksumFSOutputSummer(this, 
					f, overwrite, bufferSize, replication, blockSize, progress, permission), null);
			}
			else
			{
				@out = fs.Create(f, permission, overwrite, bufferSize, replication, blockSize, progress
					);
				// remove the checksum file since we aren't writing one
				Path checkFile = GetChecksumFile(f);
				if (fs.Exists(checkFile))
				{
					fs.Delete(checkFile, true);
				}
			}
			return @out;
		}

		/// <exception cref="System.IO.IOException"/>
		public override FSDataOutputStream CreateNonRecursive(Path f, FsPermission permission
			, bool overwrite, int bufferSize, short replication, long blockSize, Progressable
			 progress)
		{
			return Create(f, permission, overwrite, false, bufferSize, replication, blockSize
				, progress);
		}

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
		public override bool SetReplication(Path src, short replication)
		{
			bool value = fs.SetReplication(src, replication);
			if (!value)
			{
				return false;
			}
			Path checkFile = GetChecksumFile(src);
			if (Exists(checkFile))
			{
				fs.SetReplication(checkFile, replication);
			}
			return true;
		}

		/// <summary>Rename files/dirs</summary>
		/// <exception cref="System.IO.IOException"/>
		public override bool Rename(Path src, Path dst)
		{
			if (fs.IsDirectory(src))
			{
				return fs.Rename(src, dst);
			}
			else
			{
				if (fs.IsDirectory(dst))
				{
					dst = new Path(dst, src.GetName());
				}
				bool value = fs.Rename(src, dst);
				if (!value)
				{
					return false;
				}
				Path srcCheckFile = GetChecksumFile(src);
				Path dstCheckFile = GetChecksumFile(dst);
				if (fs.Exists(srcCheckFile))
				{
					//try to rename checksum
					value = fs.Rename(srcCheckFile, dstCheckFile);
				}
				else
				{
					if (fs.Exists(dstCheckFile))
					{
						// no src checksum, so remove dst checksum
						value = fs.Delete(dstCheckFile, true);
					}
				}
				return value;
			}
		}

		/// <summary>
		/// Implement the delete(Path, boolean) in checksum
		/// file system.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		public override bool Delete(Path f, bool recursive)
		{
			FileStatus fstatus = null;
			try
			{
				fstatus = fs.GetFileStatus(f);
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
				return fs.Delete(f, recursive);
			}
			else
			{
				Path checkFile = GetChecksumFile(f);
				if (fs.Exists(checkFile))
				{
					fs.Delete(checkFile, true);
				}
				return fs.Delete(f, true);
			}
		}

		private sealed class _PathFilter_556 : PathFilter
		{
			public _PathFilter_556()
			{
			}

			public bool Accept(Path file)
			{
				return !ChecksumFileSystem.IsChecksumFile(file);
			}
		}

		private static readonly PathFilter DefaultFilter = new _PathFilter_556();

		/// <summary>
		/// List the statuses of the files/directories in the given path if the path is
		/// a directory.
		/// </summary>
		/// <param name="f">given path</param>
		/// <returns>the statuses of the files/directories in the given path</returns>
		/// <exception cref="System.IO.IOException"/>
		public override FileStatus[] ListStatus(Path f)
		{
			return fs.ListStatus(f, DefaultFilter);
		}

		/// <summary>
		/// List the statuses of the files/directories in the given path if the path is
		/// a directory.
		/// </summary>
		/// <param name="f">given path</param>
		/// <returns>the statuses of the files/directories in the given patch</returns>
		/// <exception cref="System.IO.IOException"/>
		public override RemoteIterator<LocatedFileStatus> ListLocatedStatus(Path f)
		{
			return fs.ListLocatedStatus(f, DefaultFilter);
		}

		/// <exception cref="System.IO.IOException"/>
		public override bool Mkdirs(Path f)
		{
			return fs.Mkdirs(f);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void CopyFromLocalFile(bool delSrc, Path src, Path dst)
		{
			Configuration conf = GetConf();
			FileUtil.Copy(GetLocal(conf), src, this, dst, delSrc, conf);
		}

		/// <summary>The src file is under FS, and the dst is on the local disk.</summary>
		/// <remarks>
		/// The src file is under FS, and the dst is on the local disk.
		/// Copy it from FS control to the local dst name.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public override void CopyToLocalFile(bool delSrc, Path src, Path dst)
		{
			Configuration conf = GetConf();
			FileUtil.Copy(this, src, GetLocal(conf), dst, delSrc, conf);
		}

		/// <summary>The src file is under FS, and the dst is on the local disk.</summary>
		/// <remarks>
		/// The src file is under FS, and the dst is on the local disk.
		/// Copy it from FS control to the local dst name.
		/// If src and dst are directories, the copyCrc parameter
		/// determines whether to copy CRC files.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public virtual void CopyToLocalFile(Path src, Path dst, bool copyCrc)
		{
			if (!fs.IsDirectory(src))
			{
				// source is a file
				fs.CopyToLocalFile(src, dst);
				FileSystem localFs = GetLocal(GetConf()).GetRawFileSystem();
				if (localFs.IsDirectory(dst))
				{
					dst = new Path(dst, src.GetName());
				}
				dst = GetChecksumFile(dst);
				if (localFs.Exists(dst))
				{
					//remove old local checksum file
					localFs.Delete(dst, true);
				}
				Path checksumFile = GetChecksumFile(src);
				if (copyCrc && fs.Exists(checksumFile))
				{
					//copy checksum file
					fs.CopyToLocalFile(checksumFile, dst);
				}
			}
			else
			{
				FileStatus[] srcs = ListStatus(src);
				foreach (FileStatus srcFile in srcs)
				{
					CopyToLocalFile(srcFile.GetPath(), new Path(dst, srcFile.GetPath().GetName()), copyCrc
						);
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override Path StartLocalOutput(Path fsOutputFile, Path tmpLocalFile)
		{
			return tmpLocalFile;
		}

		/// <exception cref="System.IO.IOException"/>
		public override void CompleteLocalOutput(Path fsOutputFile, Path tmpLocalFile)
		{
			MoveFromLocalFile(tmpLocalFile, fsOutputFile);
		}

		/// <summary>Report a checksum error to the file system.</summary>
		/// <param name="f">the file name containing the error</param>
		/// <param name="in">the stream open on the file</param>
		/// <param name="inPos">the position of the beginning of the bad data in the file</param>
		/// <param name="sums">the stream open on the checksum file</param>
		/// <param name="sumsPos">the position of the beginning of the bad data in the checksum file
		/// 	</param>
		/// <returns>if retry is neccessary</returns>
		public virtual bool ReportChecksumFailure(Path f, FSDataInputStream @in, long inPos
			, FSDataInputStream sums, long sumsPos)
		{
			return false;
		}
	}
}
