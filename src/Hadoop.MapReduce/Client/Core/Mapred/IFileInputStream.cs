using System;
using System.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	/// <summary>A checksum input stream, used for IFiles.</summary>
	/// <remarks>
	/// A checksum input stream, used for IFiles.
	/// Used to validate the checksum of files created by
	/// <see cref="IFileOutputStream"/>
	/// .
	/// </remarks>
	public class IFileInputStream : InputStream
	{
		private readonly InputStream @in;

		private readonly FileDescriptor inFd;

		private readonly long length;

		private readonly long dataLength;

		private DataChecksum sum;

		private long currentOffset = 0;

		private readonly byte[] b = new byte[1];

		private byte[] csum = null;

		private int checksumSize;

		private ReadaheadPool.ReadaheadRequest curReadahead = null;

		private ReadaheadPool raPool = ReadaheadPool.GetInstance();

		private bool readahead;

		private int readaheadLength;

		public static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Mapred.IFileInputStream
			));

		private bool disableChecksumValidation = false;

		/// <summary>Create a checksum input stream that reads</summary>
		/// <param name="in">The input stream to be verified for checksum.</param>
		/// <param name="len">The length of the input stream including checksum bytes.</param>
		public IFileInputStream(InputStream @in, long len, Configuration conf)
		{
			//The input stream to be verified for checksum.
			// the file descriptor, if it is known
			//The total length of the input file
			this.@in = @in;
			this.inFd = GetFileDescriptorIfAvail(@in);
			sum = DataChecksum.NewDataChecksum(DataChecksum.Type.Crc32, int.MaxValue);
			checksumSize = sum.GetChecksumSize();
			length = len;
			dataLength = length - checksumSize;
			conf = (conf != null) ? conf : new Configuration();
			readahead = conf.GetBoolean(MRConfig.MapredIfileReadahead, MRConfig.DefaultMapredIfileReadahead
				);
			readaheadLength = conf.GetInt(MRConfig.MapredIfileReadaheadBytes, MRConfig.DefaultMapredIfileReadaheadBytes
				);
			DoReadahead();
		}

		private static FileDescriptor GetFileDescriptorIfAvail(InputStream @in)
		{
			FileDescriptor fd = null;
			try
			{
				if (@in is HasFileDescriptor)
				{
					fd = ((HasFileDescriptor)@in).GetFileDescriptor();
				}
				else
				{
					if (@in is FileInputStream)
					{
						fd = ((FileInputStream)@in).GetFD();
					}
				}
			}
			catch (IOException e)
			{
				Log.Info("Unable to determine FileDescriptor", e);
			}
			return fd;
		}

		/// <summary>Close the input stream.</summary>
		/// <remarks>
		/// Close the input stream. Note that we need to read to the end of the
		/// stream to validate the checksum.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public override void Close()
		{
			if (curReadahead != null)
			{
				curReadahead.Cancel();
			}
			if (currentOffset < dataLength)
			{
				byte[] t = new byte[Math.Min((int)(int.MaxValue & (dataLength - currentOffset)), 
					32 * 1024)];
				while (currentOffset < dataLength)
				{
					int n = Read(t, 0, t.Length);
					if (0 == n)
					{
						throw new EOFException("Could not validate checksum");
					}
				}
			}
			@in.Close();
		}

		/// <exception cref="System.IO.IOException"/>
		public override long Skip(long n)
		{
			throw new IOException("Skip not supported for IFileInputStream");
		}

		public virtual long GetPosition()
		{
			return (currentOffset >= dataLength) ? dataLength : currentOffset;
		}

		public virtual long GetSize()
		{
			return checksumSize;
		}

		/// <summary>Read bytes from the stream.</summary>
		/// <remarks>
		/// Read bytes from the stream.
		/// At EOF, checksum is validated, but the checksum
		/// bytes are not passed back in the buffer.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public override int Read(byte[] b, int off, int len)
		{
			if (currentOffset >= dataLength)
			{
				return -1;
			}
			DoReadahead();
			return DoRead(b, off, len);
		}

		private void DoReadahead()
		{
			if (raPool != null && inFd != null && readahead)
			{
				curReadahead = raPool.ReadaheadStream("ifile", inFd, currentOffset, readaheadLength
					, dataLength, curReadahead);
			}
		}

		/// <summary>Read bytes from the stream.</summary>
		/// <remarks>
		/// Read bytes from the stream.
		/// At EOF, checksum is validated and sent back
		/// as the last four bytes of the buffer. The caller should handle
		/// these bytes appropriately
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		public virtual int ReadWithChecksum(byte[] b, int off, int len)
		{
			if (currentOffset == length)
			{
				return -1;
			}
			else
			{
				if (currentOffset >= dataLength)
				{
					// If the previous read drained off all the data, then just return
					// the checksum now. Note that checksum validation would have 
					// happened in the earlier read
					int lenToCopy = (int)(checksumSize - (currentOffset - dataLength));
					if (len < lenToCopy)
					{
						lenToCopy = len;
					}
					System.Array.Copy(csum, (int)(currentOffset - dataLength), b, off, lenToCopy);
					currentOffset += lenToCopy;
					return lenToCopy;
				}
			}
			int bytesRead = DoRead(b, off, len);
			if (currentOffset == dataLength)
			{
				if (len >= bytesRead + checksumSize)
				{
					System.Array.Copy(csum, 0, b, off + bytesRead, checksumSize);
					bytesRead += checksumSize;
					currentOffset += checksumSize;
				}
			}
			return bytesRead;
		}

		/// <exception cref="System.IO.IOException"/>
		private int DoRead(byte[] b, int off, int len)
		{
			// If we are trying to read past the end of data, just read
			// the left over data
			if (currentOffset + len > dataLength)
			{
				len = (int)dataLength - (int)currentOffset;
			}
			int bytesRead = @in.Read(b, off, len);
			if (bytesRead < 0)
			{
				throw new ChecksumException("Checksum Error", 0);
			}
			sum.Update(b, off, bytesRead);
			currentOffset += bytesRead;
			if (disableChecksumValidation)
			{
				return bytesRead;
			}
			if (currentOffset == dataLength)
			{
				// The last four bytes are checksum. Strip them and verify
				csum = new byte[checksumSize];
				IOUtils.ReadFully(@in, csum, 0, checksumSize);
				if (!sum.Compare(csum, 0))
				{
					throw new ChecksumException("Checksum Error", 0);
				}
			}
			return bytesRead;
		}

		/// <exception cref="System.IO.IOException"/>
		public override int Read()
		{
			b[0] = 0;
			int l = Read(b, 0, 1);
			if (l < 0)
			{
				return l;
			}
			// Upgrade the b[0] to an int so as not to misinterpret the
			// first bit of the byte as a sign bit
			int result = unchecked((int)(0xFF)) & b[0];
			return result;
		}

		public virtual byte[] GetChecksum()
		{
			return csum;
		}

		internal virtual void DisableChecksumValidation()
		{
			disableChecksumValidation = true;
		}
	}
}
