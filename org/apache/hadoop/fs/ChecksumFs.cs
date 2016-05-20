using Sharpen;

namespace org.apache.hadoop.fs
{
	/// <summary>Abstract Checksumed Fs.</summary>
	/// <remarks>
	/// Abstract Checksumed Fs.
	/// It provide a basic implementation of a Checksumed Fs,
	/// which creates a checksum file for each raw file.
	/// It generates & verifies checksums at the client side.
	/// </remarks>
	public abstract class ChecksumFs : org.apache.hadoop.fs.FilterFs
	{
		private static readonly byte[] CHECKSUM_VERSION = new byte[] { (byte)('c'), (byte
			)('r'), (byte)('c'), 0 };

		private int defaultBytesPerChecksum = 512;

		private bool verifyChecksum = true;

		/*Evolving for a release,to be changed to Stable */
		public static double getApproxChkSumLength(long size)
		{
			return org.apache.hadoop.fs.ChecksumFs.ChecksumFSOutputSummer.CHKSUM_AS_FRACTION 
				* size;
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="java.net.URISyntaxException"/>
		public ChecksumFs(org.apache.hadoop.fs.AbstractFileSystem theFs)
			: base(theFs)
		{
			defaultBytesPerChecksum = getMyFs().getServerDefaults().getBytesPerChecksum();
		}

		/// <summary>Set whether to verify checksum.</summary>
		public override void setVerifyChecksum(bool inVerifyChecksum)
		{
			this.verifyChecksum = inVerifyChecksum;
		}

		/// <summary>get the raw file system.</summary>
		public virtual org.apache.hadoop.fs.AbstractFileSystem getRawFs()
		{
			return getMyFs();
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

		/// <summary>Return the bytes Per Checksum.</summary>
		public virtual int getBytesPerSum()
		{
			return defaultBytesPerChecksum;
		}

		/// <exception cref="System.IO.IOException"/>
		private int getSumBufferSize(int bytesPerSum, int bufferSize)
		{
			int defaultBufferSize = getMyFs().getServerDefaults().getFileBufferSize();
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
			public static readonly org.apache.commons.logging.Log LOG = org.apache.commons.logging.LogFactory
				.getLog(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.FSInputChecker
				)));

			private const int HEADER_LENGTH = 8;

			private org.apache.hadoop.fs.ChecksumFs fs;

			private org.apache.hadoop.fs.FSDataInputStream datas;

			private org.apache.hadoop.fs.FSDataInputStream sums;

			private int bytesPerSum = 1;

			private long fileLen = -1L;

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="org.apache.hadoop.fs.UnresolvedLinkException"/>
			public ChecksumFSInputChecker(org.apache.hadoop.fs.ChecksumFs fs, org.apache.hadoop.fs.Path
				 file)
				: this(fs, file, fs.getServerDefaults().getFileBufferSize())
			{
			}

			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="org.apache.hadoop.fs.UnresolvedLinkException"/>
			public ChecksumFSInputChecker(org.apache.hadoop.fs.ChecksumFs fs, org.apache.hadoop.fs.Path
				 file, int bufferSize)
				: base(file, fs.getFileStatus(file).getReplication())
			{
				this.datas = fs.getRawFs().open(file, bufferSize);
				this.fs = fs;
				org.apache.hadoop.fs.Path sumFile = fs.getChecksumFile(file);
				try
				{
					int sumBufferSize = fs.getSumBufferSize(fs.getBytesPerSum(), bufferSize);
					sums = fs.getRawFs().open(sumFile, sumBufferSize);
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
			/// <exception cref="org.apache.hadoop.fs.UnresolvedLinkException"/>
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
				org.apache.hadoop.fs.ChecksumFs.ChecksumFSInputChecker checker = new org.apache.hadoop.fs.ChecksumFs.ChecksumFSInputChecker
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
						throw new java.io.EOFException("Checksum file not a length multiple of checksum size "
							 + "in " + file + " at " + pos + " checksumpos: " + checksumPos + " sumLenread: "
							 + sumLenRead);
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

			/* Return the file length */
			/// <exception cref="System.IO.IOException"/>
			/// <exception cref="org.apache.hadoop.fs.UnresolvedLinkException"/>
			private long getFileLength()
			{
				if (fileLen == -1L)
				{
					fileLen = fs.getFileStatus(file).getLen();
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
						throw new System.IO.IOException("Cannot seek after EOF");
					}
					base.seek(pos);
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public override bool truncate(org.apache.hadoop.fs.Path f, long newLength)
		{
			throw new System.IO.IOException("Not supported");
		}

		/// <summary>Opens an FSDataInputStream at the indicated Path.</summary>
		/// <param name="f">the file name to open</param>
		/// <param name="bufferSize">the size of the buffer to be used.</param>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="org.apache.hadoop.fs.UnresolvedLinkException"/>
		public override org.apache.hadoop.fs.FSDataInputStream open(org.apache.hadoop.fs.Path
			 f, int bufferSize)
		{
			return new org.apache.hadoop.fs.FSDataInputStream(new org.apache.hadoop.fs.ChecksumFs.ChecksumFSInputChecker
				(this, f, bufferSize));
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
			public ChecksumFSOutputSummer(org.apache.hadoop.fs.ChecksumFs fs, org.apache.hadoop.fs.Path
				 file, java.util.EnumSet<org.apache.hadoop.fs.CreateFlag> createFlag, org.apache.hadoop.fs.permission.FsPermission
				 absolutePermission, int bufferSize, short replication, long blockSize, org.apache.hadoop.util.Progressable
				 progress, org.apache.hadoop.fs.Options.ChecksumOpt checksumOpt, bool createParent
				)
				: base(org.apache.hadoop.util.DataChecksum.newDataChecksum(org.apache.hadoop.util.DataChecksum.Type
					.CRC32, fs.getBytesPerSum()))
			{
				// checksumOpt is passed down to the raw fs. Unless it implements
				// checksum impelemts internally, checksumOpt will be ignored.
				// If the raw fs does checksum internally, we will end up with
				// two layers of checksumming. i.e. checksumming checksum file.
				this.datas = fs.getRawFs().createInternal(file, createFlag, absolutePermission, bufferSize
					, replication, blockSize, progress, checksumOpt, createParent);
				// Now create the chekcsumfile; adjust the buffsize
				int bytesPerSum = fs.getBytesPerSum();
				int sumBufferSize = fs.getSumBufferSize(bytesPerSum, bufferSize);
				this.sums = fs.getRawFs().createInternal(fs.getChecksumFile(file), java.util.EnumSet
					.of(org.apache.hadoop.fs.CreateFlag.CREATE, org.apache.hadoop.fs.CreateFlag.OVERWRITE
					), absolutePermission, sumBufferSize, replication, blockSize, progress, checksumOpt
					, createParent);
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
		public override org.apache.hadoop.fs.FSDataOutputStream createInternal(org.apache.hadoop.fs.Path
			 f, java.util.EnumSet<org.apache.hadoop.fs.CreateFlag> createFlag, org.apache.hadoop.fs.permission.FsPermission
			 absolutePermission, int bufferSize, short replication, long blockSize, org.apache.hadoop.util.Progressable
			 progress, org.apache.hadoop.fs.Options.ChecksumOpt checksumOpt, bool createParent
			)
		{
			org.apache.hadoop.fs.FSDataOutputStream @out = new org.apache.hadoop.fs.FSDataOutputStream
				(new org.apache.hadoop.fs.ChecksumFs.ChecksumFSOutputSummer(this, f, createFlag, 
				absolutePermission, bufferSize, replication, blockSize, progress, checksumOpt, createParent
				), null);
			return @out;
		}

		/// <summary>Check if exists.</summary>
		/// <param name="f">source file</param>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="org.apache.hadoop.fs.UnresolvedLinkException"/>
		private bool exists(org.apache.hadoop.fs.Path f)
		{
			try
			{
				return getMyFs().getFileStatus(f) != null;
			}
			catch (java.io.FileNotFoundException)
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
		/// <exception cref="org.apache.hadoop.fs.UnresolvedLinkException"/>
		private bool isDirectory(org.apache.hadoop.fs.Path f)
		{
			try
			{
				return getMyFs().getFileStatus(f).isDirectory();
			}
			catch (java.io.FileNotFoundException)
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
		/// <exception cref="org.apache.hadoop.fs.UnresolvedLinkException"/>
		public override bool setReplication(org.apache.hadoop.fs.Path src, short replication
			)
		{
			bool value = getMyFs().setReplication(src, replication);
			if (!value)
			{
				return false;
			}
			org.apache.hadoop.fs.Path checkFile = getChecksumFile(src);
			if (exists(checkFile))
			{
				getMyFs().setReplication(checkFile, replication);
			}
			return true;
		}

		/// <summary>Rename files/dirs.</summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="org.apache.hadoop.fs.UnresolvedLinkException"/>
		public override void renameInternal(org.apache.hadoop.fs.Path src, org.apache.hadoop.fs.Path
			 dst)
		{
			if (isDirectory(src))
			{
				getMyFs().rename(src, dst);
			}
			else
			{
				getMyFs().rename(src, dst);
				org.apache.hadoop.fs.Path checkFile = getChecksumFile(src);
				if (exists(checkFile))
				{
					//try to rename checksum
					if (isDirectory(dst))
					{
						getMyFs().rename(checkFile, dst);
					}
					else
					{
						getMyFs().rename(checkFile, getChecksumFile(dst));
					}
				}
			}
		}

		/// <summary>
		/// Implement the delete(Path, boolean) in checksum
		/// file system.
		/// </summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="org.apache.hadoop.fs.UnresolvedLinkException"/>
		public override bool delete(org.apache.hadoop.fs.Path f, bool recursive)
		{
			org.apache.hadoop.fs.FileStatus fstatus = null;
			try
			{
				fstatus = getMyFs().getFileStatus(f);
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
				return getMyFs().delete(f, recursive);
			}
			else
			{
				org.apache.hadoop.fs.Path checkFile = getChecksumFile(f);
				if (exists(checkFile))
				{
					getMyFs().delete(checkFile, true);
				}
				return getMyFs().delete(f, true);
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
		public virtual bool reportChecksumFailure(org.apache.hadoop.fs.Path f, org.apache.hadoop.fs.FSDataInputStream
			 @in, long inPos, org.apache.hadoop.fs.FSDataInputStream sums, long sumsPos)
		{
			return false;
		}

		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="org.apache.hadoop.fs.UnresolvedLinkException"/>
		public override org.apache.hadoop.fs.FileStatus[] listStatus(org.apache.hadoop.fs.Path
			 f)
		{
			System.Collections.Generic.List<org.apache.hadoop.fs.FileStatus> results = new System.Collections.Generic.List
				<org.apache.hadoop.fs.FileStatus>();
			org.apache.hadoop.fs.FileStatus[] listing = getMyFs().listStatus(f);
			if (listing != null)
			{
				for (int i = 0; i < listing.Length; i++)
				{
					if (!isChecksumFile(listing[i].getPath()))
					{
						results.add(listing[i]);
					}
				}
			}
			return Sharpen.Collections.ToArray(results, new org.apache.hadoop.fs.FileStatus[results
				.Count]);
		}
	}
}
