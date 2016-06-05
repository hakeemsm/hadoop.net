using System.Collections.Generic;
using Hadoop.Common.Core.Conf;
using Hadoop.Common.Core.IO;
using Hadoop.Common.Core.Util;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.FS.Shell;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.IO.Compress;
using Org.Apache.Hadoop.Util;
using Path = Org.Apache.Hadoop.FS.Path;

namespace Hadoop.Common.Core.Fs.Shell
{
	/// <summary>Display contents or checksums of files</summary>
	internal class Display : FsCommand
	{
		public static void RegisterCommands(CommandFactory factory)
		{
			factory.AddClass(typeof(Display.Cat), "-cat");
			factory.AddClass(typeof(Display.Text), "-text");
			factory.AddClass(typeof(Display.Checksum), "-checksum");
		}

		/// <summary>Displays file content to stdout</summary>
		public class Cat : Display
		{
			public const string Name = "cat";

			public const string Usage = "[-ignoreCrc] <src> ...";

			public const string Description = "Fetch all files that match the file pattern <src> "
				 + "and display their content on stdout.\n";

			private bool verifyChecksum = true;

			/// <exception cref="System.IO.IOException"/>
			protected internal override void ProcessOptions(List<string> args)
			{
				CommandFormat cf = new CommandFormat(1, int.MaxValue, "ignoreCrc");
				cf.Parse(args);
				verifyChecksum = !cf.GetOpt("ignoreCrc");
			}

			/// <exception cref="System.IO.IOException"/>
			protected internal override void ProcessPath(PathData item)
			{
				if (item.stat.IsDirectory())
				{
					throw new PathIsDirectoryException(item.ToString());
				}
				item.fs.SetVerifyChecksum(verifyChecksum);
				PrintToStdout(GetInputStream(item));
			}

			/// <exception cref="System.IO.IOException"/>
			private void PrintToStdout(InputStream @in)
			{
				try
				{
					IOUtils.CopyBytes(@in, @out, GetConf(), false);
				}
				finally
				{
					@in.Close();
				}
			}

			/// <exception cref="System.IO.IOException"/>
			protected internal virtual InputStream GetInputStream(PathData item)
			{
				return item.fs.Open(item.path);
			}
		}

		/// <summary>
		/// Same behavior as "-cat", but handles zip and TextRecordInputStream
		/// and Avro encodings.
		/// </summary>
		public class Text : Display.Cat
		{
			public const string Name = "text";

			public const string Usage = Display.Cat.Usage;

			public const string Description = "Takes a source file and outputs the file in text format.\n"
				 + "The allowed formats are zip and TextRecordInputStream and Avro.";

			/// <exception cref="System.IO.IOException"/>
			protected internal override InputStream GetInputStream(PathData item)
			{
				FSDataInputStream i = (FSDataInputStream)base.GetInputStream(item);
				// Handle 0 and 1-byte files
				short leadBytes;
				try
				{
					leadBytes = i.ReadShort();
				}
				catch (EOFException)
				{
					i.Seek(0);
					return i;
				}
				switch (leadBytes)
				{
					case unchecked((int)(0x1f8b)):
					{
						// Check type of stream first
						// RFC 1952
						// Must be gzip
						i.Seek(0);
						return new GZIPInputStream(i);
					}

					case unchecked((int)(0x5345)):
					{
						// 'S' 'E'
						// Might be a SequenceFile
						if (i.ReadByte() == 'Q')
						{
							i.Close();
							return new Display.TextRecordInputStream(this, item.stat);
						}
						goto default;
					}

					default:
					{
						// Check the type of compression instead, depending on Codec class's
						// own detection methods, based on the provided path.
						CompressionCodecFactory cf = new CompressionCodecFactory(GetConf());
						CompressionCodec codec = cf.GetCodec(item.path);
						if (codec != null)
						{
							i.Seek(0);
							return codec.CreateInputStream(i);
						}
						break;
					}

					case unchecked((int)(0x4f62)):
					{
						// 'O' 'b'
						if (i.ReadByte() == 'j')
						{
							i.Close();
							return new Display.AvroFileInputStream(item.stat);
						}
						break;
					}
				}
				// File is non-compressed, or not a file container we know.
				i.Seek(0);
				return i;
			}
		}

		public class Checksum : Display
		{
			public const string Name = "checksum";

			public const string Usage = "<src> ...";

			public const string Description = "Dump checksum information for files that match the file "
				 + "pattern <src> to stdout. Note that this requires a round-trip " + "to a datanode storing each block of the file, and thus is not "
				 + "efficient to run on a large number of files. The checksum of a " + "file depends on its content, block size and the checksum "
				 + "algorithm and parameters used for creating the file.";

			/// <exception cref="System.IO.IOException"/>
			protected internal override void ProcessPath(PathData item)
			{
				if (item.stat.IsDirectory())
				{
					throw new PathIsDirectoryException(item.ToString());
				}
				FileChecksum checksum = item.fs.GetFileChecksum(item.path);
				if (checksum == null)
				{
					@out.Printf("%s\tNONE\t%n", item.ToString());
				}
				else
				{
					string checksumString = StringUtils.ByteToHexString(checksum.GetBytes(), 0, checksum
						.GetLength());
					@out.Printf("%s\t%s\t%s%n", item.ToString(), checksum.GetAlgorithmName(), checksumString
						);
				}
			}
		}

		protected internal class TextRecordInputStream : InputStream
		{
			internal SequenceFile.Reader r;

			internal IWritableComparable<object> key;

			internal IWritable val;

			internal DataInputBuffer inbuf;

			internal DataOutputBuffer outbuf;

			/// <exception cref="System.IO.IOException"/>
			public TextRecordInputStream(Display _enclosing, FileStatus f)
			{
				this._enclosing = _enclosing;
				Path fpath = f.GetPath();
				Configuration lconf = this._enclosing.GetConf();
				this.r = new SequenceFile.Reader(lconf, SequenceFile.Reader.File(fpath));
				this.key = ReflectionUtils.NewInstance(this.r.GetKeyClass().AsSubclass<IWritableComparable<>>(), lconf);
				this.val = ReflectionUtils.NewInstance(this.r.GetValueClass().AsSubclass<IWritable
					>(), lconf);
				this.inbuf = new DataInputBuffer();
				this.outbuf = new DataOutputBuffer();
			}

			/// <exception cref="System.IO.IOException"/>
			public override int Read()
			{
				int ret;
				if (null == this.inbuf || -1 == (ret = this.inbuf.Read()))
				{
					if (!this.r.Next(this.key, this.val))
					{
						return -1;
					}
					byte[] tmp = Runtime.GetBytesForString(this.key.ToString(), Charsets.Utf8
						);
					this.outbuf.Write(tmp, 0, tmp.Length);
					this.outbuf.Write('\t');
					tmp = Runtime.GetBytesForString(this.val.ToString(), Charsets.Utf8);
					this.outbuf.Write(tmp, 0, tmp.Length);
					this.outbuf.Write('\n');
					this.inbuf.Reset(this.outbuf.GetData(), this.outbuf.GetLength());
					this.outbuf.Reset();
					ret = this.inbuf.Read();
				}
				return ret;
			}

			/// <exception cref="System.IO.IOException"/>
			public override void Close()
			{
				this.r.Close();
				base.Close();
			}

			private readonly Display _enclosing;
		}

		/// <summary>
		/// This class transforms a binary Avro data file into an InputStream
		/// with data that is in a human readable JSON format.
		/// </summary>
		protected internal class AvroFileInputStream : InputStream
		{
			private int pos;

			private byte[] buffer;

			private ByteArrayOutputStream output;

			private FileReader<object> fileReader;

			private DatumWriter<object> writer;

			private JsonEncoder encoder;

			/// <exception cref="System.IO.IOException"/>
			public AvroFileInputStream(FileStatus status)
			{
				pos = 0;
				buffer = new byte[0];
				GenericDatumReader<object> reader = new GenericDatumReader<object>();
				FileContext fc = FileContext.GetFileContext(new Configuration());
				fileReader = DataFileReader.OpenReader(new AvroFSInput(fc, status.GetPath()), reader
					);
				Schema schema = fileReader.GetSchema();
				writer = new GenericDatumWriter<object>(schema);
				output = new ByteArrayOutputStream();
				JsonGenerator generator = new JsonFactory().CreateJsonGenerator(output, JsonEncoding
					.Utf8);
				MinimalPrettyPrinter prettyPrinter = new MinimalPrettyPrinter();
				prettyPrinter.SetRootValueSeparator(Runtime.GetProperty("line.separator"));
				generator.SetPrettyPrinter(prettyPrinter);
				encoder = EncoderFactory.Get().JsonEncoder(schema, generator);
			}

			/// <summary>Read a single byte from the stream.</summary>
			/// <exception cref="System.IO.IOException"/>
			public override int Read()
			{
				if (pos < buffer.Length)
				{
					return buffer[pos++];
				}
				if (!fileReader.HasNext())
				{
					return -1;
				}
				writer.Write(fileReader.Next(), encoder);
				encoder.Flush();
				if (!fileReader.HasNext())
				{
					// Write a new line after the last Avro record.
					output.Write(Runtime.GetBytesForString(Runtime.GetProperty("line.separator"
						), Charsets.Utf8));
					output.Flush();
				}
				pos = 0;
				buffer = output.ToByteArray();
				output.Reset();
				return Read();
			}

			/// <summary>Close the stream.</summary>
			/// <exception cref="System.IO.IOException"/>
			public override void Close()
			{
				fileReader.Close();
				output.Close();
				base.Close();
			}
		}
	}
}
