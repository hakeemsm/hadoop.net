using System;
using System.IO;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.IO.Compress;
using Org.Apache.Hadoop.IO.Serializer;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred
{
	/// <summary>
	/// <code>IFile</code> is the simple &lt;key-len, value-len, key, value&gt; format
	/// for the intermediate map-outputs in Map-Reduce.
	/// </summary>
	/// <remarks>
	/// <code>IFile</code> is the simple &lt;key-len, value-len, key, value&gt; format
	/// for the intermediate map-outputs in Map-Reduce.
	/// There is a <code>Writer</code> to write out map-outputs in this format and
	/// a <code>Reader</code> to read files of this format.
	/// </remarks>
	public class IFile
	{
		private static readonly Log Log = LogFactory.GetLog(typeof(IFile));

		public const int EofMarker = -1;

		/// <summary><code>IFile.Writer</code> to write out intermediate map-outputs.</summary>
		public class Writer<K, V>
		{
			internal FSDataOutputStream @out;

			internal bool ownOutputStream = false;

			internal long start = 0;

			internal FSDataOutputStream rawOut;

			internal CompressionOutputStream compressedOut;

			internal Compressor compressor;

			internal bool compressOutput = false;

			internal long decompressedBytesWritten = 0;

			internal long compressedBytesWritten = 0;

			private long numRecordsWritten = 0;

			private readonly Counters.Counter writtenRecordsCounter;

			internal IFileOutputStream checksumOut;

			internal Type keyClass;

			internal Type valueClass;

			internal Org.Apache.Hadoop.IO.Serializer.Serializer<K> keySerializer;

			internal Org.Apache.Hadoop.IO.Serializer.Serializer<V> valueSerializer;

			internal DataOutputBuffer buffer = new DataOutputBuffer();

			/// <exception cref="System.IO.IOException"/>
			public Writer(Configuration conf, FSDataOutputStream @out, Type keyClass, Type valueClass
				, CompressionCodec codec, Counters.Counter writesCounter)
				: this(conf, @out, keyClass, valueClass, codec, writesCounter, false)
			{
			}

			protected internal Writer(Counters.Counter writesCounter)
			{
				// End of File Marker
				// Count records written to disk
				writtenRecordsCounter = writesCounter;
			}

			/// <exception cref="System.IO.IOException"/>
			public Writer(Configuration conf, FSDataOutputStream @out, Type keyClass, Type valueClass
				, CompressionCodec codec, Counters.Counter writesCounter, bool ownOutputStream)
			{
				this.writtenRecordsCounter = writesCounter;
				this.checksumOut = new IFileOutputStream(@out);
				this.rawOut = @out;
				this.start = this.rawOut.GetPos();
				if (codec != null)
				{
					this.compressor = CodecPool.GetCompressor(codec);
					if (this.compressor != null)
					{
						this.compressor.Reset();
						this.compressedOut = codec.CreateOutputStream(checksumOut, compressor);
						this.@out = new FSDataOutputStream(this.compressedOut, null);
						this.compressOutput = true;
					}
					else
					{
						Log.Warn("Could not obtain compressor from CodecPool");
						this.@out = new FSDataOutputStream(checksumOut, null);
					}
				}
				else
				{
					this.@out = new FSDataOutputStream(checksumOut, null);
				}
				this.keyClass = keyClass;
				this.valueClass = valueClass;
				if (keyClass != null)
				{
					SerializationFactory serializationFactory = new SerializationFactory(conf);
					this.keySerializer = serializationFactory.GetSerializer(keyClass);
					this.keySerializer.Open(buffer);
					this.valueSerializer = serializationFactory.GetSerializer(valueClass);
					this.valueSerializer.Open(buffer);
				}
				this.ownOutputStream = ownOutputStream;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Close()
			{
				// When IFile writer is created by BackupStore, we do not have
				// Key and Value classes set. So, check before closing the
				// serializers
				if (keyClass != null)
				{
					keySerializer.Close();
					valueSerializer.Close();
				}
				// Write EOF_MARKER for key/value length
				WritableUtils.WriteVInt(@out, EofMarker);
				WritableUtils.WriteVInt(@out, EofMarker);
				decompressedBytesWritten += 2 * WritableUtils.GetVIntSize(EofMarker);
				//Flush the stream
				@out.Flush();
				if (compressOutput)
				{
					// Flush
					compressedOut.Finish();
					compressedOut.ResetState();
				}
				// Close the underlying stream iff we own it...
				if (ownOutputStream)
				{
					@out.Close();
				}
				else
				{
					// Write the checksum
					checksumOut.Finish();
				}
				compressedBytesWritten = rawOut.GetPos() - start;
				if (compressOutput)
				{
					// Return back the compressor
					CodecPool.ReturnCompressor(compressor);
					compressor = null;
				}
				@out = null;
				if (writtenRecordsCounter != null)
				{
					writtenRecordsCounter.Increment(numRecordsWritten);
				}
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Append(K key, V value)
			{
				if (key.GetType() != keyClass)
				{
					throw new IOException("wrong key class: " + key.GetType() + " is not " + keyClass
						);
				}
				if (value.GetType() != valueClass)
				{
					throw new IOException("wrong value class: " + value.GetType() + " is not " + valueClass
						);
				}
				// Append the 'key'
				keySerializer.Serialize(key);
				int keyLength = buffer.GetLength();
				if (keyLength < 0)
				{
					throw new IOException("Negative key-length not allowed: " + keyLength + " for " +
						 key);
				}
				// Append the 'value'
				valueSerializer.Serialize(value);
				int valueLength = buffer.GetLength() - keyLength;
				if (valueLength < 0)
				{
					throw new IOException("Negative value-length not allowed: " + valueLength + " for "
						 + value);
				}
				// Write the record out
				WritableUtils.WriteVInt(@out, keyLength);
				// key length
				WritableUtils.WriteVInt(@out, valueLength);
				// value length
				@out.Write(buffer.GetData(), 0, buffer.GetLength());
				// data
				// Reset
				buffer.Reset();
				// Update bytes written
				decompressedBytesWritten += keyLength + valueLength + WritableUtils.GetVIntSize(keyLength
					) + WritableUtils.GetVIntSize(valueLength);
				++numRecordsWritten;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Append(DataInputBuffer key, DataInputBuffer value)
			{
				int keyLength = key.GetLength() - key.GetPosition();
				if (keyLength < 0)
				{
					throw new IOException("Negative key-length not allowed: " + keyLength + " for " +
						 key);
				}
				int valueLength = value.GetLength() - value.GetPosition();
				if (valueLength < 0)
				{
					throw new IOException("Negative value-length not allowed: " + valueLength + " for "
						 + value);
				}
				WritableUtils.WriteVInt(@out, keyLength);
				WritableUtils.WriteVInt(@out, valueLength);
				@out.Write(key.GetData(), key.GetPosition(), keyLength);
				@out.Write(value.GetData(), value.GetPosition(), valueLength);
				// Update bytes written
				decompressedBytesWritten += keyLength + valueLength + WritableUtils.GetVIntSize(keyLength
					) + WritableUtils.GetVIntSize(valueLength);
				++numRecordsWritten;
			}

			// Required for mark/reset
			public virtual DataOutputStream GetOutputStream()
			{
				return @out;
			}

			// Required for mark/reset
			public virtual void UpdateCountersForExternalAppend(long length)
			{
				++numRecordsWritten;
				decompressedBytesWritten += length;
			}

			public virtual long GetRawLength()
			{
				return decompressedBytesWritten;
			}

			public virtual long GetCompressedLength()
			{
				return compressedBytesWritten;
			}
		}

		/// <summary><code>IFile.Reader</code> to read intermediate map-outputs.</summary>
		public class Reader<K, V>
		{
			private const int DefaultBufferSize = 128 * 1024;

			private const int MaxVintSize = 9;

			private long numRecordsRead = 0;

			private readonly Counters.Counter readRecordsCounter;

			internal readonly InputStream @in;

			internal Decompressor decompressor;

			public long bytesRead = 0;

			protected internal readonly long fileLength;

			protected internal bool eof = false;

			internal readonly IFileInputStream checksumIn;

			protected internal byte[] buffer = null;

			protected internal int bufferSize = DefaultBufferSize;

			protected internal DataInputStream dataIn;

			protected internal int recNo = 1;

			protected internal int currentKeyLength;

			protected internal int currentValueLength;

			internal byte[] keyBytes = new byte[0];

			/// <summary>Construct an IFile Reader.</summary>
			/// <param name="conf">Configuration File</param>
			/// <param name="fs">FileSystem</param>
			/// <param name="file">
			/// Path of the file to be opened. This file should have
			/// checksum bytes for the data at the end of the file.
			/// </param>
			/// <param name="codec">codec</param>
			/// <param name="readsCounter">Counter for records read from disk</param>
			/// <exception cref="System.IO.IOException"/>
			public Reader(Configuration conf, FileSystem fs, Path file, CompressionCodec codec
				, Counters.Counter readsCounter)
				: this(conf, fs.Open(file), fs.GetFileStatus(file).GetLen(), codec, readsCounter)
			{
			}

			/// <summary>Construct an IFile Reader.</summary>
			/// <param name="conf">Configuration File</param>
			/// <param name="in">The input stream</param>
			/// <param name="length">
			/// Length of the data in the stream, including the checksum
			/// bytes.
			/// </param>
			/// <param name="codec">codec</param>
			/// <param name="readsCounter">Counter for records read from disk</param>
			/// <exception cref="System.IO.IOException"/>
			public Reader(Configuration conf, FSDataInputStream @in, long length, CompressionCodec
				 codec, Counters.Counter readsCounter)
			{
				// Count records read from disk
				// Possibly decompressed stream that we read
				readRecordsCounter = readsCounter;
				checksumIn = new IFileInputStream(@in, length, conf);
				if (codec != null)
				{
					decompressor = CodecPool.GetDecompressor(codec);
					if (decompressor != null)
					{
						this.@in = codec.CreateInputStream(checksumIn, decompressor);
					}
					else
					{
						Log.Warn("Could not obtain decompressor from CodecPool");
						this.@in = checksumIn;
					}
				}
				else
				{
					this.@in = checksumIn;
				}
				this.dataIn = new DataInputStream(this.@in);
				this.fileLength = length;
				if (conf != null)
				{
					bufferSize = conf.GetInt("io.file.buffer.size", DefaultBufferSize);
				}
			}

			public virtual long GetLength()
			{
				return fileLength - checksumIn.GetSize();
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual long GetPosition()
			{
				return checksumIn.GetPosition();
			}

			/// <summary>Read upto len bytes into buf starting at offset off.</summary>
			/// <param name="buf">buffer</param>
			/// <param name="off">offset</param>
			/// <param name="len">length of buffer</param>
			/// <returns>the no. of bytes read</returns>
			/// <exception cref="System.IO.IOException"/>
			private int ReadData(byte[] buf, int off, int len)
			{
				int bytesRead = 0;
				while (bytesRead < len)
				{
					int n = IOUtils.WrappedReadForCompressedData(@in, buf, off + bytesRead, len - bytesRead
						);
					if (n < 0)
					{
						return bytesRead;
					}
					bytesRead += n;
				}
				return len;
			}

			/// <exception cref="System.IO.IOException"/>
			protected internal virtual bool PositionToNextRecord(DataInput dIn)
			{
				// Sanity check
				if (eof)
				{
					throw new EOFException("Completed reading " + bytesRead);
				}
				// Read key and value lengths
				currentKeyLength = WritableUtils.ReadVInt(dIn);
				currentValueLength = WritableUtils.ReadVInt(dIn);
				bytesRead += WritableUtils.GetVIntSize(currentKeyLength) + WritableUtils.GetVIntSize
					(currentValueLength);
				// Check for EOF
				if (currentKeyLength == EofMarker && currentValueLength == EofMarker)
				{
					eof = true;
					return false;
				}
				// Sanity check
				if (currentKeyLength < 0)
				{
					throw new IOException("Rec# " + recNo + ": Negative key-length: " + currentKeyLength
						);
				}
				if (currentValueLength < 0)
				{
					throw new IOException("Rec# " + recNo + ": Negative value-length: " + currentValueLength
						);
				}
				return true;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual bool NextRawKey(DataInputBuffer key)
			{
				if (!PositionToNextRecord(dataIn))
				{
					return false;
				}
				if (keyBytes.Length < currentKeyLength)
				{
					keyBytes = new byte[currentKeyLength << 1];
				}
				int i = ReadData(keyBytes, 0, currentKeyLength);
				if (i != currentKeyLength)
				{
					throw new IOException("Asked for " + currentKeyLength + " Got: " + i);
				}
				key.Reset(keyBytes, currentKeyLength);
				bytesRead += currentKeyLength;
				return true;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void NextRawValue(DataInputBuffer value)
			{
				byte[] valBytes = (value.GetData().Length < currentValueLength) ? new byte[currentValueLength
					 << 1] : value.GetData();
				int i = ReadData(valBytes, 0, currentValueLength);
				if (i != currentValueLength)
				{
					throw new IOException("Asked for " + currentValueLength + " Got: " + i);
				}
				value.Reset(valBytes, currentValueLength);
				// Record the bytes read
				bytesRead += currentValueLength;
				++recNo;
				++numRecordsRead;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Close()
			{
				// Close the underlying stream
				@in.Close();
				// Release the buffer
				dataIn = null;
				buffer = null;
				if (readRecordsCounter != null)
				{
					readRecordsCounter.Increment(numRecordsRead);
				}
				// Return the decompressor
				if (decompressor != null)
				{
					decompressor.Reset();
					CodecPool.ReturnDecompressor(decompressor);
					decompressor = null;
				}
			}

			public virtual void Reset(int offset)
			{
				return;
			}

			public virtual void DisableChecksumValidation()
			{
				checksumIn.DisableChecksumValidation();
			}
		}
	}
}
