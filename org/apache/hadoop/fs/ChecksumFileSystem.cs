using Sharpen;

namespace org.apache.hadoop.fs
{
	/// <summary>Abstract Checksumed FileSystem.</summary>
	/// <remarks>
	/// Abstract Checksumed FileSystem.
	/// It provide a basic implementation of a Checksumed FileSystem,
	/// which creates a checksum file for each raw file.
	/// It generates & verifies checksums at the client side.
	/// </remarks>
	public abstract class ChecksumFileSystem : org.apache.hadoop.fs.FilterFileSystem
	{
		private static readonly byte[] CHECKSUM_VERSION = new byte[] { (byte)('c'), (byte
			)('r'), (byte)('c'), 0 };

		private int bytesPerChecksum = 512;

		private bool verifyChecksum = true;

		private bool writeChecksum = true;

		public static double getApproxChkSumLength(long size)
		{
			return org.apache.hadoop.fs.ChecksumFileSystem.ChecksumFSOutputSummer.CHKSUM_AS_FRACTION
				 * size;
		}

		public ChecksumFileSystem(org.apache.hadoop.fs.FileSystem fs)
			: base(fs)
		{
		}

		public override void setConf(org.apache.hadoop.conf.Configuration conf)
		{
			base.setConf(conf);
			if (conf != null)
			{
				bytesPerChecksum = conf.getInt(org.apache.hadoop.fs.LocalFileSystemConfigKeys.LOCAL_FS_BYTES_PER_CHECKSUM_KEY
					, org.apache.hadoop.fs.LocalFileSystemConfigKeys.LOCAL_FS_BYTES_PER_CHECKSUM_DEFAULT
					);
			}
		}

		/// <summary>Set whether to verify checksum.</summary>
		public override void setVerifyChecksum(bool verifyChecksum)
		{
			this.verifyChecksum = verifyChecksum;
		}

		public override void setWriteChecksum(bool writeChecksum)
		{
			this.writeChecksum = writeChecksum;
		}

		/// <summary>get the raw file system</summary>
		public override org.apache.hadoop.fs.FileSystem getRawFileSystem()
		{
			return fs;
		}

		/// <summary>Return the name of the checksum file associated with a file.</summary>
		public virtual org.apache.hadoop.fs.Path getChecksumFile(org.apache.hadoop.fs.Path
			 file)
		{
			return new org.apache.hadoop.fs.Path(file.getParent(), "." + file.getName() + ".crc"
				);
		}

		/// <summary>Return true iff file is a checksum file name.</summary>
		public static bool isChecksumFile(org.apache.hadoop.fs.Path file)
		{
			string name = file.getName();
			return name.StartsWith(".") && name.EndsWith(".crc");
		}

		/// <summary>
		/// Return the length of the checksum file given the size of the
		/// actual file.
		/// </summary>
		public virtual long getChecksumFileLength(org.apache.hadoop.fs.Path file, long fileSize
			)
		{
			return getChecksumLength(fileSize, getBytesPerSum());
		}

		/// <summary>Return the bytes Per Checksum</summary>
		public virtual int getBytesPerSum()
		{
			return bytesPerChecksum;
		}

		private int getSumBufferSize(int bytesPerSum, int bufferSize)
		{
			int defaultBufferSize = getConf().getInt(org.apache.hadoop.fs.LocalFileSystemConfigKeys
				.LOCAL_FS_STREAM_BUFFER_SIZE_KEY, org.apache.hadoop.fs.LocalFileSystemConfigKeys
				.LOCAL_FS_STREAM_BUFFER_SIZE_DEFAULT);
			int proportionalBufferSize = bufferSize / bytesPerSum;
			return System.Math.max(bytesPerSum, System.Math.max(proportionalBufferSize, defaultBufferSize
				));
		}

		/// <summary>
		/// For open()'s FSInputStream
		/// It verifies that data matches checksums.
		/// </summary>
		private class ChecksumFSInputChecker : org.apache.hadoop.fs.FSInputChecker
		{
			private org.apache.hadoop.fs.ChecksumFileSystem fs;

			private org.apache.hadoop.fs.FSDataInputStream datas;

			private org.apache.hadoop.fs.FSDataInputStream sums;

			private const int HEADER_LENGTH = 8;

			private int bytesPerSum = 1;

			/// <exception cref="System.IO.IOException"/>
			public ChecksumFSInputChecker(org.apache.hadoop.fs.ChecksumFileSystem fs, org.apache.hadoop.fs.Path
				 file)
				: this(fs, file, fs.getConf().getInt(org.apache.hadoop.fs.LocalFileSystemConfigKeys
					.LOCAL_FS_STREAM_BUFFER_SIZE_KEY, org.apache.hadoop.fs.LocalFileSystemConfigKeys
					.LOCAL_FS_STREAM_BUFFER_SIZE_DEFAULT))
			{
			}

			/// <exception cref="System.IO.IOException"/>
			public ChecksumFSInputChecker(org.apache.hadoop.fs.ChecksumFileSystem fs, org.apache.hadoop.fs.Path
				 file, int bufferSize)
				: base(file, fs.getFileStatus(file).getReplication())
			{
				this.datas = fs.getRawFileSystem().open(file, bufferSize);
				this.fs = fs;
				org.apache.hadoop.fs.Path sumFile = fs.getChecksumFile(file);
				try
				{
					int sumBufferSize = fs.getSumBufferSize(fs.getBytesPerSum(), bufferSize);
					sums = fs.getRawFileSystem().open(sumFile, sumBufferSize);
					byte[] version = new byte[CHECKSUM_VERSION.Length];
					sums.readFully(version);
					if (!java.util.Arrays.equals(version, CHECKSUM_VERSION))
					{
						throw new System.IO.IOException("Not a checksum file: " + sumFile);
					}
					this.bytesPerSum = sums.readInt();
					set(fs.verifyChecksum, org.apache.hadoop.util.DataChecksum.newCrc32(), bytesPerSum
						, 4);
				}
				catch (java.io.FileNotFoundException)
				{
					// quietly ignore
					set(fs.verifyChecksum, null, 1, 0);
				}
				catch (System.IO.IOException e)
				{
					// loudly ignore
					LOG.warn("Problem opening checksum file: " + file + ".  Ignoring exception: ", e);
					set(fs.verifyChecksum, null, 1, 0);
				}
			}

			private long getChecksumFilePos(long dataPos)
			{
				return HEADER_LENGTH + 4 * (dataPos / bytesPerSum);
			}

			protected internal override long getChunkPosition(long dataPos)
			{
				return dataPos / bytesPerSum * bytesPerSum;
			}

			/// <exception cref="System.IO.IOException"/>
			public override int available()
			{
				return datas.available() + base.available();
			}

			/// <exception cref="System.IO.IOException"/>
			public override int read(long position, byte[] b, int off, int len)
			{
				// parameter check
				if ((off | len | (off + len) | (b.Length - (off + len))) < 0)
				{
					throw new System.IndexOutOfRangeException();
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
					throw new System.ArgumentException("Parameter position can not to be negative");
				}
				org.apache.hadoop.fs.ChecksumFileSystem.ChecksumFSInputChecker checker = new org.apache.hadoop.fs.ChecksumFileSystem.ChecksumFSInputChecker
					(fs, file);
				checker.seek(position);
				int nread = checker.read(b, off, len);
				checker.close();
				return nread;
			}

			/// <exception cref="System.IO.IOException"/>
			public override void close()
			{
				datas.close();
				if (sums != null)
				{
					sums.close();
				}
				set(fs.verifyChecksum, null, 1, 0);
			}

			/// <exception cref="System.IO.IOException"/>
			public override bool seekToNewSource(long targetPos)
			{
				long sumsPos = getChecksumFilePos(targetPos);
				fs.reportChecksumFailure(file, datas, targetPos, sums, sumsPos);
				bool newDataSource = datas.seekToNewSource(targetPos);
				return sums.seekToNewSource(sumsPos) || newDataSource;
			}

			/// <exception cref="System.IO.IOException"/>
			protected internal override int readChunk(long pos, byte[] buf, int offset, int len
				, byte[] checksum)
			{
				bool eof = false;
				if (needChecksum())
				{
					System.Diagnostics.Debug.Assert(checksum != null);
					// we have a checksum buffer
					System.Diagnostics.Debug.Assert(checksum.Length % CHECKSUM_SIZE == 0);
					// it is sane length
					System.Diagnostics.Debug.Assert(len >= bytesPerSum);
					// we must read at least one chunk
					int checksumsToRead = System.Math.min(len / bytesPerSum, checksum.Length / CHECKSUM_SIZE
						);
					// number of checksums based on len to read
					// size of checksum buffer
					long checksumPos = getChecksumFilePos(pos);
					if (checksumPos != sums.getPos())
					{
						sums.seek(checksumPos);
					}
					int sumLenRead = sums.read(checksum, 0, CHECKSUM_SIZE * checksumsToRead);
					if (sumLenRead >= 0 && sumLenRead % CHECKSUM_SIZE != 0)
					{
						throw new org.apache.hadoop.fs.ChecksumException("Checksum file not a length multiple of checksum size "
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
						len = System.Math.min(len, bytesPerSum * (sumLenRead / CHECKSUM_SIZE));
					}
				}
				if (pos != datas.getPos())
				{
					datas.seek(pos);
				}
				int nread = readFully(datas, buf, offset, len);
				if (eof && nread > 0)
				{
					throw new org.apache.hadoop.fs.ChecksumException("Checksum error: " + file + " at "
						 + pos, pos);
				}
				return nread;
			}
		}

		private class FSDataBoundedInputStream : org.apache.hadoop.fs.FSDataInputStream
		{
			private org.apache.hadoop.fs.FileSystem fs;

			private org.apache.hadoop.fs.Path file;

			private long fileLen = -1L;

			internal FSDataBoundedInputStream(org.apache.hadoop.fs.FileSystem fs, org.apache.hadoop.fs.Path
				 file, java.io.InputStream @in)
				: base(@in)
			{
				this.fs = fs;
				this.file = file;
			}

			public override bool markSupported()
			{
				return false;
			}

			/* Return the file length */
			/// <exception cref="System.IO.IOException"/>
			private long getFileLength()
			{
				if (fileLen == -1L)
				{
					fileLen = fs.getContentSummary(file).getLength();
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
			public override long skip(long n)
			{
				lock (this)
				{
					long curPos = getPos();
					long fileLength = getFileLength();
					if (n + curPos > fileLength)
					{
						n = fileLength - curPos;
					}
					return base.skip(n);
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
			public override void seek(long pos)
			{
				lock (this)
				{
					if (pos > getFileLength())
					{
						throw new java.io.EOFException("Cannot seek after EOF");
					}
					base.seek(pos);
				}
			}
		}

		/// <summary>Opens an FSDataInputStream at the indicated Path.</summary>
		/// <param name="f">the file name to open</param>
		/// <param name="bufferSize">the size of the buffer to be used.</param>
		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.FSDataInputStream open(org.apache.hadoop.fs.Path
			 f, int bufferSize)
		{
			org.apache.hadoop.fs.FileSystem fs;
			java.io.InputStream @in;
			if (verifyChecksum)
			{
				fs = this;
				@in = new org.apache.hadoop.fs.ChecksumFileSystem.ChecksumFSInputChecker(this, f, 
					bufferSize);
			}
			else
			{
				fs = getRawFileSystem();
				@in = fs.open(f, bufferSize);
			}
			return new org.apache.hadoop.fs.ChecksumFileSystem.FSDataBoundedInputStream(fs, f
				, @in);
		}

		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.FSDataOutputStream append(org.apache.hadoop.fs.Path
			 f, int bufferSize, org.apache.hadoop.util.Progressable progress)
		{
			throw new System.IO.IOException("Not supported");
		}

		/// <exception cref="System.IO.IOException"/>
		public override bool truncate(org.apache.hadoop.fs.Path f, long newLength)
		{
			throw new System.IO.IOException("Not supported");
		}

		/// <summary>Calculated the length of the checksum file in bytes.</summary>
		/// <param name="size">the length of the data file in bytes</param>
		/// <param name="bytesPerSum">the number of bytes in a checksum block</param>
		/// <returns>the number of bytes in the checksum file</returns>
		public static long getChecksumLength(long size, int bytesPerSum)
		{
			//the checksum length is equal to size passed divided by bytesPerSum +
			//bytes written in the beginning of the checksum file.  
			return ((size + bytesPerSum - 1) / bytesPerSum) * 4 + CHECKSUM_VERSION.Length + 4;
		}

		/// <summary>This class provides an output stream for a checksummed file.</summary>
		/// <remarks>
		/// This class provides an output stream for a checksummed file.
		/// It generates checksums for data.
		/// </remarks>
		private class ChecksumFSOutputSummer : org.apache.hadoop.fs.FSOutputSummer
		{
			private org.apache.hadoop.fs.FSDataOutputStream datas;

			private org.apache.hadoop.fs.FSDataOutputStream sums;

			private const float CHKSUM_AS_FRACTION = 0.01f;

			private bool isClosed = false;

			/// <exception cref="System.IO.IOException"/>
			public ChecksumFSOutputSummer(org.apache.hadoop.fs.ChecksumFileSystem fs, org.apache.hadoop.fs.Path
				 file, bool overwrite, int bufferSize, short replication, long blockSize, org.apache.hadoop.util.Progressable
				 progress, org.apache.hadoop.fs.permission.FsPermission permission)
				: base(org.apache.hadoop.util.DataChecksum.newDataChecksum(org.apache.hadoop.util.DataChecksum.Type
					.CRC32, fs.getBytesPerSum()))
			{
				int bytesPerSum = fs.getBytesPerSum();
				this.datas = fs.getRawFileSystem().create(file, permission, overwrite, bufferSize
					, replication, blockSize, progress);
				int sumBufferSize = fs.getSumBufferSize(bytesPerSum, bufferSize);
				this.sums = fs.getRawFileSystem().create(fs.getChecksumFile(file), permission, true
					, sumBufferSize, replication, blockSize, null);
				sums.write(CHECKSUM_VERSION, 0, CHECKSUM_VERSION.Length);
				sums.writeInt(bytesPerSum);
			}

			/// <exception cref="System.IO.IOException"/>
			public override void close()
			{
				try
				{
					flushBuffer();
					sums.close();
					datas.close();
				}
				finally
				{
					isClosed = true;
				}
			}

			/// <exception cref="System.IO.IOException"/>
			protected internal override void writeChunk(byte[] b, int offset, int len, byte[]
				 checksum, int ckoff, int cklen)
			{
				datas.write(b, offset, len);
				sums.write(checksum, ckoff, cklen);
			}

			/// <exception cref="System.IO.IOException"/>
			protected internal override void checkClosed()
			{
				if (isClosed)
				{
					throw new java.nio.channels.ClosedChannelException();
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.FSDataOutputStream create(org.apache.hadoop.fs.Path
			 f, org.apache.hadoop.fs.permission.FsPermission permission, bool overwrite, int
			 bufferSize, short replication, long blockSize, org.apache.hadoop.util.Progressable
			 progress)
		{
			return create(f, permission, overwrite, true, bufferSize, replication, blockSize, 
				progress);
		}

		/// <exception cref="System.IO.IOException"/>
		private org.apache.hadoop.fs.FSDataOutputStream create(org.apache.hadoop.fs.Path 
			f, org.apache.hadoop.fs.permission.FsPermission permission, bool overwrite, bool
			 createParent, int bufferSize, short replication, long blockSize, org.apache.hadoop.util.Progressable
			 progress)
		{
			org.apache.hadoop.fs.Path parent = f.getParent();
			if (parent != null)
			{
				if (!createParent && !exists(parent))
				{
					throw new java.io.FileNotFoundException("Parent directory doesn't exist: " + parent
						);
				}
				else
				{
					if (!mkdirs(parent))
					{
						throw new System.IO.IOException("Mkdirs failed to create " + parent + " (exists="
							 + exists(parent) + ", cwd=" + getWorkingDirectory() + ")");
					}
				}
			}
			org.apache.hadoop.fs.FSDataOutputStream @out;
			if (writeChecksum)
			{
				@out = new org.apache.hadoop.fs.FSDataOutputStream(new org.apache.hadoop.fs.ChecksumFileSystem.ChecksumFSOutputSummer
					(this, f, overwrite, bufferSize, replication, blockSize, progress, permission), 
					null);
			}
			else
			{
				@out = fs.create(f, permission, overwrite, bufferSize, replication, blockSize, progress
					);
				// remove the checksum file since we aren't writing one
				org.apache.hadoop.fs.Path checkFile = getChecksumFile(f);
				if (fs.exists(checkFile))
				{
					fs.delete(checkFile, true);
				}
			}
			return @out;
		}

		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.FSDataOutputStream createNonRecursive(org.apache.hadoop.fs.Path
			 f, org.apache.hadoop.fs.permission.FsPermission permission, bool overwrite, int
			 bufferSize, short replication, long blockSize, org.apache.hadoop.util.Progressable
			 progress)
		{
			return create(f, permission, overwrite, false, bufferSize, replication, blockSize
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
		public override bool setReplication(org.apache.hadoop.fs.Path src, short replication
			)
		{
			bool value = fs.setReplication(src, replication);
			if (!value)
			{
				return false;
			}
			org.apache.hadoop.fs.Path checkFile = getChecksumFile(src);
			if (exists(checkFile))
			{
				fs.setReplication(checkFile, replication);
			}
			return true;
		}

		/// <summary>Rename files/dirs</summary>
		/// <exception cref="System.IO.IOException"/>
		public override bool rename(org.apache.hadoop.fs.Path src, org.apache.hadoop.fs.Path
			 dst)
		{
			if (fs.isDirectory(src))
			{
				return fs.rename(src, dst);
			}
			else
			{
				if (fs.isDirectory(dst))
				{
					dst = new org.apache.hadoop.fs.Path(dst, src.getName());
				}
				bool value = fs.rename(src, dst);
				if (!value)
				{
					return false;
				}
				org.apache.hadoop.fs.Path srcCheckFile = getChecksumFile(src);
				org.apache.hadoop.fs.Path dstCheckFile = getChecksumFile(dst);
				if (fs.exists(srcCheckFile))
				{
					//try to rename checksum
					value = fs.rename(srcCheckFile, dstCheckFile);
				}
				else
				{
					if (fs.exists(dstCheckFile))
					{
						// no src checksum, so remove dst checksum
						value = fs.delete(dstCheckFile, true);
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
		public override bool delete(org.apache.hadoop.fs.Path f, bool recursive)
		{
			org.apache.hadoop.fs.FileStatus fstatus = null;
			try
			{
				fstatus = fs.getFileStatus(f);
			}
			catch (java.io.FileNotFoundException)
			{
				return false;
			}
			if (fstatus.isDirectory())
			{
				//this works since the crcs are in the same
				//directories and the files. so we just delete
				//everything in the underlying filesystem
				return fs.delete(f, recursive);
			}
			else
			{
				org.apache.hadoop.fs.Path checkFile = getChecksumFile(f);
				if (fs.exists(checkFile))
				{
					fs.delete(checkFile, true);
				}
				return fs.delete(f, true);
			}
		}

		private sealed class _PathFilter_556 : org.apache.hadoop.fs.PathFilter
		{
			public _PathFilter_556()
			{
			}

			public bool accept(org.apache.hadoop.fs.Path file)
			{
				return !org.apache.hadoop.fs.ChecksumFileSystem.isChecksumFile(file);
			}
		}

		private static readonly org.apache.hadoop.fs.PathFilter DEFAULT_FILTER = new _PathFilter_556
			();

		/// <summary>
		/// List the statuses of the files/directories in the given path if the path is
		/// a directory.
		/// </summary>
		/// <param name="f">given path</param>
		/// <returns>the statuses of the files/directories in the given path</returns>
		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.FileStatus[] listStatus(org.apache.hadoop.fs.Path
			 f)
		{
			return fs.listStatus(f, DEFAULT_FILTER);
		}

		/// <summary>
		/// List the statuses of the files/directories in the given path if the path is
		/// a directory.
		/// </summary>
		/// <param name="f">given path</param>
		/// <returns>the statuses of the files/directories in the given patch</returns>
		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.RemoteIterator<org.apache.hadoop.fs.LocatedFileStatus
			> listLocatedStatus(org.apache.hadoop.fs.Path f)
		{
			return fs.listLocatedStatus(f, DEFAULT_FILTER);
		}

		/// <exception cref="System.IO.IOException"/>
		public override bool mkdirs(org.apache.hadoop.fs.Path f)
		{
			return fs.mkdirs(f);
		}

		/// <exception cref="System.IO.IOException"/>
		public override void copyFromLocalFile(bool delSrc, org.apache.hadoop.fs.Path src
			, org.apache.hadoop.fs.Path dst)
		{
			org.apache.hadoop.conf.Configuration conf = getConf();
			org.apache.hadoop.fs.FileUtil.copy(getLocal(conf), src, this, dst, delSrc, conf);
		}

		/// <summary>The src file is under FS, and the dst is on the local disk.</summary>
		/// <remarks>
		/// The src file is under FS, and the dst is on the local disk.
		/// Copy it from FS control to the local dst name.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public override void copyToLocalFile(bool delSrc, org.apache.hadoop.fs.Path src, 
			org.apache.hadoop.fs.Path dst)
		{
			org.apache.hadoop.conf.Configuration conf = getConf();
			org.apache.hadoop.fs.FileUtil.copy(this, src, getLocal(conf), dst, delSrc, conf);
		}

		/// <summary>The src file is under FS, and the dst is on the local disk.</summary>
		/// <remarks>
		/// The src file is under FS, and the dst is on the local disk.
		/// Copy it from FS control to the local dst name.
		/// If src and dst are directories, the copyCrc parameter
		/// determines whether to copy CRC files.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public virtual void copyToLocalFile(org.apache.hadoop.fs.Path src, org.apache.hadoop.fs.Path
			 dst, bool copyCrc)
		{
			if (!fs.isDirectory(src))
			{
				// source is a file
				fs.copyToLocalFile(src, dst);
				org.apache.hadoop.fs.FileSystem localFs = getLocal(getConf()).getRawFileSystem();
				if (localFs.isDirectory(dst))
				{
					dst = new org.apache.hadoop.fs.Path(dst, src.getName());
				}
				dst = getChecksumFile(dst);
				if (localFs.exists(dst))
				{
					//remove old local checksum file
					localFs.delete(dst, true);
				}
				org.apache.hadoop.fs.Path checksumFile = getChecksumFile(src);
				if (copyCrc && fs.exists(checksumFile))
				{
					//copy checksum file
					fs.copyToLocalFile(checksumFile, dst);
				}
			}
			else
			{
				org.apache.hadoop.fs.FileStatus[] srcs = listStatus(src);
				foreach (org.apache.hadoop.fs.FileStatus srcFile in srcs)
				{
					copyToLocalFile(srcFile.getPath(), new org.apache.hadoop.fs.Path(dst, srcFile.getPath
						().getName()), copyCrc);
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override org.apache.hadoop.fs.Path startLocalOutput(org.apache.hadoop.fs.Path
			 fsOutputFile, org.apache.hadoop.fs.Path tmpLocalFile)
		{
			return tmpLocalFile;
		}

		/// <exception cref="System.IO.IOException"/>
		public override void completeLocalOutput(org.apache.hadoop.fs.Path fsOutputFile, 
			org.apache.hadoop.fs.Path tmpLocalFile)
		{
			moveFromLocalFile(tmpLocalFile, fsOutputFile);
		}

		/// <summary>Report a checksum error to the file system.</summary>
		/// <param name="f">the file name containing the error</param>
		/// <param name="in">the stream open on the file</param>
		/// <param name="inPos">the position of the beginning of the bad data in the file</param>
		/// <param name="sums">the stream open on the checksum file</param>
		/// <param name="sumsPos">the position of the beginning of the bad data in the checksum file
		/// 	</param>
		/// <returns>if retry is neccessary</returns>
		public virtual bool reportChecksumFailure(org.apache.hadoop.fs.Path f, org.apache.hadoop.fs.FSDataInputStream
			 @in, long inPos, org.apache.hadoop.fs.FSDataInputStream sums, long sumsPos)
		{
			return false;
		}
	}
}
