using Sharpen;

namespace org.apache.hadoop.fs.shell
{
	/// <summary>Display contents or checksums of files</summary>
	internal class Display : org.apache.hadoop.fs.shell.FsCommand
	{
		public static void registerCommands(org.apache.hadoop.fs.shell.CommandFactory factory
			)
		{
			factory.addClass(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.shell.Display.Cat
				)), "-cat");
			factory.addClass(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.shell.Display.Text
				)), "-text");
			factory.addClass(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.fs.shell.Display.Checksum
				)), "-checksum");
		}

		/// <summary>Displays file content to stdout</summary>
		public class Cat : org.apache.hadoop.fs.shell.Display
		{
			public const string NAME = "cat";

			public const string USAGE = "[-ignoreCrc] <src> ...";

			public const string DESCRIPTION = "Fetch all files that match the file pattern <src> "
				 + "and display their content on stdout.\n";

			private bool verifyChecksum = true;

			/// <exception cref="System.IO.IOException"/>
			protected internal override void processOptions(System.Collections.Generic.LinkedList
				<string> args)
			{
				org.apache.hadoop.fs.shell.CommandFormat cf = new org.apache.hadoop.fs.shell.CommandFormat
					(1, int.MaxValue, "ignoreCrc");
				cf.parse(args);
				verifyChecksum = !cf.getOpt("ignoreCrc");
			}

			/// <exception cref="System.IO.IOException"/>
			protected internal override void processPath(org.apache.hadoop.fs.shell.PathData 
				item)
			{
				if (item.stat.isDirectory())
				{
					throw new org.apache.hadoop.fs.PathIsDirectoryException(item.ToString());
				}
				item.fs.setVerifyChecksum(verifyChecksum);
				printToStdout(getInputStream(item));
			}

			/// <exception cref="System.IO.IOException"/>
			private void printToStdout(java.io.InputStream @in)
			{
				try
				{
					org.apache.hadoop.io.IOUtils.copyBytes(@in, @out, getConf(), false);
				}
				finally
				{
					@in.close();
				}
			}

			/// <exception cref="System.IO.IOException"/>
			protected internal virtual java.io.InputStream getInputStream(org.apache.hadoop.fs.shell.PathData
				 item)
			{
				return item.fs.open(item.path);
			}
		}

		/// <summary>
		/// Same behavior as "-cat", but handles zip and TextRecordInputStream
		/// and Avro encodings.
		/// </summary>
		public class Text : org.apache.hadoop.fs.shell.Display.Cat
		{
			public const string NAME = "text";

			public const string USAGE = org.apache.hadoop.fs.shell.Display.Cat.USAGE;

			public const string DESCRIPTION = "Takes a source file and outputs the file in text format.\n"
				 + "The allowed formats are zip and TextRecordInputStream and Avro.";

			/// <exception cref="System.IO.IOException"/>
			protected internal override java.io.InputStream getInputStream(org.apache.hadoop.fs.shell.PathData
				 item)
			{
				org.apache.hadoop.fs.FSDataInputStream i = (org.apache.hadoop.fs.FSDataInputStream
					)base.getInputStream(item);
				// Handle 0 and 1-byte files
				short leadBytes;
				try
				{
					leadBytes = i.readShort();
				}
				catch (java.io.EOFException)
				{
					i.seek(0);
					return i;
				}
				switch (leadBytes)
				{
					case unchecked((int)(0x1f8b)):
					{
						// Check type of stream first
						// RFC 1952
						// Must be gzip
						i.seek(0);
						return new java.util.zip.GZIPInputStream(i);
					}

					case unchecked((int)(0x5345)):
					{
						// 'S' 'E'
						// Might be a SequenceFile
						if (i.readByte() == 'Q')
						{
							i.close();
							return new org.apache.hadoop.fs.shell.Display.TextRecordInputStream(this, item.stat
								);
						}
						goto default;
					}

					default:
					{
						// Check the type of compression instead, depending on Codec class's
						// own detection methods, based on the provided path.
						org.apache.hadoop.io.compress.CompressionCodecFactory cf = new org.apache.hadoop.io.compress.CompressionCodecFactory
							(getConf());
						org.apache.hadoop.io.compress.CompressionCodec codec = cf.getCodec(item.path);
						if (codec != null)
						{
							i.seek(0);
							return codec.createInputStream(i);
						}
						break;
					}

					case unchecked((int)(0x4f62)):
					{
						// 'O' 'b'
						if (i.readByte() == 'j')
						{
							i.close();
							return new org.apache.hadoop.fs.shell.Display.AvroFileInputStream(item.stat);
						}
						break;
					}
				}
				// File is non-compressed, or not a file container we know.
				i.seek(0);
				return i;
			}
		}

		public class Checksum : org.apache.hadoop.fs.shell.Display
		{
			public const string NAME = "checksum";

			public const string USAGE = "<src> ...";

			public const string DESCRIPTION = "Dump checksum information for files that match the file "
				 + "pattern <src> to stdout. Note that this requires a round-trip " + "to a datanode storing each block of the file, and thus is not "
				 + "efficient to run on a large number of files. The checksum of a " + "file depends on its content, block size and the checksum "
				 + "algorithm and parameters used for creating the file.";

			/// <exception cref="System.IO.IOException"/>
			protected internal override void processPath(org.apache.hadoop.fs.shell.PathData 
				item)
			{
				if (item.stat.isDirectory())
				{
					throw new org.apache.hadoop.fs.PathIsDirectoryException(item.ToString());
				}
				org.apache.hadoop.fs.FileChecksum checksum = item.fs.getFileChecksum(item.path);
				if (checksum == null)
				{
					@out.printf("%s\tNONE\t%n", item.ToString());
				}
				else
				{
					string checksumString = org.apache.hadoop.util.StringUtils.byteToHexString(checksum
						.getBytes(), 0, checksum.getLength());
					@out.printf("%s\t%s\t%s%n", item.ToString(), checksum.getAlgorithmName(), checksumString
						);
				}
			}
		}

		protected internal class TextRecordInputStream : java.io.InputStream
		{
			internal org.apache.hadoop.io.SequenceFile.Reader r;

			internal org.apache.hadoop.io.WritableComparable<object> key;

			internal org.apache.hadoop.io.Writable val;

			internal org.apache.hadoop.io.DataInputBuffer inbuf;

			internal org.apache.hadoop.io.DataOutputBuffer outbuf;

			/// <exception cref="System.IO.IOException"/>
			public TextRecordInputStream(Display _enclosing, org.apache.hadoop.fs.FileStatus 
				f)
			{
				this._enclosing = _enclosing;
				org.apache.hadoop.fs.Path fpath = f.getPath();
				org.apache.hadoop.conf.Configuration lconf = this._enclosing.getConf();
				this.r = new org.apache.hadoop.io.SequenceFile.Reader(lconf, org.apache.hadoop.io.SequenceFile.Reader
					.file(fpath));
				this.key = org.apache.hadoop.util.ReflectionUtils.newInstance(this.r.getKeyClass(
					).asSubclass<org.apache.hadoop.io.WritableComparable>(), lconf);
				this.val = org.apache.hadoop.util.ReflectionUtils.newInstance(this.r.getValueClass
					().asSubclass<org.apache.hadoop.io.Writable>(), lconf);
				this.inbuf = new org.apache.hadoop.io.DataInputBuffer();
				this.outbuf = new org.apache.hadoop.io.DataOutputBuffer();
			}

			/// <exception cref="System.IO.IOException"/>
			public override int read()
			{
				int ret;
				if (null == this.inbuf || -1 == (ret = this.inbuf.read()))
				{
					if (!this.r.next(this.key, this.val))
					{
						return -1;
					}
					byte[] tmp = Sharpen.Runtime.getBytesForString(this.key.ToString(), org.apache.commons.io.Charsets
						.UTF_8);
					this.outbuf.write(tmp, 0, tmp.Length);
					this.outbuf.write('\t');
					tmp = Sharpen.Runtime.getBytesForString(this.val.ToString(), org.apache.commons.io.Charsets
						.UTF_8);
					this.outbuf.write(tmp, 0, tmp.Length);
					this.outbuf.write('\n');
					this.inbuf.reset(this.outbuf.getData(), this.outbuf.getLength());
					this.outbuf.reset();
					ret = this.inbuf.read();
				}
				return ret;
			}

			/// <exception cref="System.IO.IOException"/>
			public override void close()
			{
				this.r.close();
				base.close();
			}

			private readonly Display _enclosing;
		}

		/// <summary>
		/// This class transforms a binary Avro data file into an InputStream
		/// with data that is in a human readable JSON format.
		/// </summary>
		protected internal class AvroFileInputStream : java.io.InputStream
		{
			private int pos;

			private byte[] buffer;

			private java.io.ByteArrayOutputStream output;

			private org.apache.avro.file.FileReader<object> fileReader;

			private org.apache.avro.io.DatumWriter<object> writer;

			private org.apache.avro.io.JsonEncoder encoder;

			/// <exception cref="System.IO.IOException"/>
			public AvroFileInputStream(org.apache.hadoop.fs.FileStatus status)
			{
				pos = 0;
				buffer = new byte[0];
				org.apache.avro.generic.GenericDatumReader<object> reader = new org.apache.avro.generic.GenericDatumReader
					<object>();
				org.apache.hadoop.fs.FileContext fc = org.apache.hadoop.fs.FileContext.getFileContext
					(new org.apache.hadoop.conf.Configuration());
				fileReader = org.apache.avro.file.DataFileReader.openReader(new org.apache.hadoop.fs.AvroFSInput
					(fc, status.getPath()), reader);
				org.apache.avro.Schema schema = fileReader.getSchema();
				writer = new org.apache.avro.generic.GenericDatumWriter<object>(schema);
				output = new java.io.ByteArrayOutputStream();
				org.codehaus.jackson.JsonGenerator generator = new org.codehaus.jackson.JsonFactory
					().createJsonGenerator(output, org.codehaus.jackson.JsonEncoding.UTF8);
				org.codehaus.jackson.util.MinimalPrettyPrinter prettyPrinter = new org.codehaus.jackson.util.MinimalPrettyPrinter
					();
				prettyPrinter.setRootValueSeparator(Sharpen.Runtime.getProperty("line.separator")
					);
				generator.setPrettyPrinter(prettyPrinter);
				encoder = org.apache.avro.io.EncoderFactory.get().jsonEncoder(schema, generator);
			}

			/// <summary>Read a single byte from the stream.</summary>
			/// <exception cref="System.IO.IOException"/>
			public override int read()
			{
				if (pos < buffer.Length)
				{
					return buffer[pos++];
				}
				if (!fileReader.MoveNext())
				{
					return -1;
				}
				writer.write(fileReader.Current, encoder);
				encoder.flush();
				if (!fileReader.MoveNext())
				{
					// Write a new line after the last Avro record.
					output.write(Sharpen.Runtime.getBytesForString(Sharpen.Runtime.getProperty("line.separator"
						), org.apache.commons.io.Charsets.UTF_8));
					output.flush();
				}
				pos = 0;
				buffer = output.toByteArray();
				output.reset();
				return read();
			}

			/// <summary>Close the stream.</summary>
			/// <exception cref="System.IO.IOException"/>
			public override void close()
			{
				fileReader.close();
				output.close();
				base.close();
			}
		}
	}
}
