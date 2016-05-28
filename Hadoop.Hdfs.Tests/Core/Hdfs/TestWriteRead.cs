using System.IO;
using NUnit.Framework;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs.Client;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	public class TestWriteRead
	{
		private const int WrNtimes = 350;

		private const int WrChunkSize = 10000;

		private const int BufferSize = 8192 * 100;

		private const string RootDir = "/tmp/";

		private const long blockSize = 1024 * 100;

		internal string filenameOption = RootDir + "fileX1";

		internal int chunkSizeOption = 10000;

		internal int loopOption = 10;

		private MiniDFSCluster cluster;

		private Configuration conf;

		private FileSystem mfs;

		private FileContext mfc;

		private bool useFCOption = false;

		private bool verboseOption = true;

		private bool positionReadOption = false;

		private bool truncateOption = false;

		private readonly bool abortTestOnFailure = true;

		private static Log Log = LogFactory.GetLog(typeof(TestWriteRead));

		// Junit test settings. 
		// command-line options. Different defaults for unit test vs real cluster
		// = new HdfsConfiguration();
		// = cluster.getFileSystem();
		// = FileContext.getFileContext();
		// configuration
		// use either FileSystem or FileContext
		/// <exception cref="System.Exception"/>
		[SetUp]
		public virtual void InitJunitModeTest()
		{
			Log.Info("initJunitModeTest");
			conf = new HdfsConfiguration();
			conf.SetLong(DFSConfigKeys.DfsBlockSizeKey, blockSize);
			// 100K
			// blocksize
			cluster = new MiniDFSCluster.Builder(conf).NumDataNodes(3).Build();
			cluster.WaitActive();
			mfs = cluster.GetFileSystem();
			mfc = FileContext.GetFileContext();
			Path rootdir = new Path(RootDir);
			mfs.Mkdirs(rootdir);
		}

		[TearDown]
		public virtual void Shutdown()
		{
			cluster.Shutdown();
		}

		// Equivalence of @Before for cluster mode testing.
		/// <exception cref="System.IO.IOException"/>
		private void InitClusterModeTest()
		{
			Log = LogFactory.GetLog(typeof(TestWriteRead));
			Log.Info("initClusterModeTest");
			conf = new Configuration();
			mfc = FileContext.GetFileContext();
			mfs = FileSystem.Get(conf);
		}

		/// <summary>Junit Test reading while writing.</summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestWriteReadSeq()
		{
			useFCOption = false;
			positionReadOption = false;
			string fname = filenameOption;
			long rdBeginPos = 0;
			// need to run long enough to fail: takes 25 to 35 seec on Mac
			int stat = TestWriteAndRead(fname, WrNtimes, WrChunkSize, rdBeginPos);
			Log.Info("Summary status from test1: status= " + stat);
			NUnit.Framework.Assert.AreEqual(0, stat);
		}

		/// <summary>Junit Test position read while writing.</summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestWriteReadPos()
		{
			string fname = filenameOption;
			positionReadOption = true;
			// position read
			long rdBeginPos = 0;
			int stat = TestWriteAndRead(fname, WrNtimes, WrChunkSize, rdBeginPos);
			NUnit.Framework.Assert.AreEqual(0, stat);
		}

		/// <summary>Junit Test position read of the current block being written.</summary>
		/// <exception cref="System.IO.IOException"/>
		[NUnit.Framework.Test]
		public virtual void TestReadPosCurrentBlock()
		{
			string fname = filenameOption;
			positionReadOption = true;
			// position read
			int wrChunkSize = (int)(blockSize) + (int)(blockSize / 2);
			long rdBeginPos = blockSize + 1;
			int numTimes = 5;
			int stat = TestWriteAndRead(fname, numTimes, wrChunkSize, rdBeginPos);
			NUnit.Framework.Assert.AreEqual(0, stat);
		}

		// equivalent of TestWriteRead1
		/// <exception cref="System.IO.IOException"/>
		private int ClusterTestWriteRead1()
		{
			long rdBeginPos = 0;
			int stat = TestWriteAndRead(filenameOption, loopOption, chunkSizeOption, rdBeginPos
				);
			return stat;
		}

		/// <summary>Open the file to read from begin to end.</summary>
		/// <remarks>
		/// Open the file to read from begin to end. Then close the file.
		/// Return number of bytes read.
		/// Support both sequential read and position read.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		private long ReadData(string fname, byte[] buffer, long byteExpected, long beginPosition
			)
		{
			long totalByteRead = 0;
			Path path = GetFullyQualifiedPath(fname);
			FSDataInputStream @in = null;
			try
			{
				@in = OpenInputStream(path);
				long visibleLenFromReadStream = ((HdfsDataInputStream)@in).GetVisibleLength();
				if (visibleLenFromReadStream < byteExpected)
				{
					throw new IOException(visibleLenFromReadStream + " = visibleLenFromReadStream < bytesExpected= "
						 + byteExpected);
				}
				totalByteRead = ReadUntilEnd(@in, buffer, buffer.Length, fname, beginPosition, visibleLenFromReadStream
					, positionReadOption);
				@in.Close();
				// reading more data than visibleLeng is OK, but not less
				if (totalByteRead + beginPosition < byteExpected)
				{
					throw new IOException("readData mismatch in byte read: expected=" + byteExpected 
						+ " ; got " + (totalByteRead + beginPosition));
				}
				return totalByteRead + beginPosition;
			}
			catch (IOException e)
			{
				throw new IOException("##### Caught Exception in readData. " + "Total Byte Read so far = "
					 + totalByteRead + " beginPosition = " + beginPosition, e);
			}
			finally
			{
				if (@in != null)
				{
					@in.Close();
				}
			}
		}

		/// <summary>read chunks into buffer repeatedly until total of VisibleLen byte are read.
		/// 	</summary>
		/// <remarks>
		/// read chunks into buffer repeatedly until total of VisibleLen byte are read.
		/// Return total number of bytes read
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		private long ReadUntilEnd(FSDataInputStream @in, byte[] buffer, long size, string
			 fname, long pos, long visibleLen, bool positionReadOption)
		{
			if (pos >= visibleLen || visibleLen <= 0)
			{
				return 0;
			}
			int chunkNumber = 0;
			long totalByteRead = 0;
			long currentPosition = pos;
			int byteRead = 0;
			long byteLeftToRead = visibleLen - pos;
			int byteToReadThisRound = 0;
			if (!positionReadOption)
			{
				@in.Seek(pos);
				currentPosition = @in.GetPos();
			}
			if (verboseOption)
			{
				Log.Info("reader begin: position: " + pos + " ; currentOffset = " + currentPosition
					 + " ; bufferSize =" + buffer.Length + " ; Filename = " + fname);
			}
			try
			{
				while (byteLeftToRead > 0 && currentPosition < visibleLen)
				{
					byteToReadThisRound = (int)(byteLeftToRead >= buffer.Length ? buffer.Length : byteLeftToRead
						);
					if (positionReadOption)
					{
						byteRead = @in.Read(currentPosition, buffer, 0, byteToReadThisRound);
					}
					else
					{
						byteRead = @in.Read(buffer, 0, byteToReadThisRound);
					}
					if (byteRead <= 0)
					{
						break;
					}
					chunkNumber++;
					totalByteRead += byteRead;
					currentPosition += byteRead;
					byteLeftToRead -= byteRead;
					if (verboseOption)
					{
						Log.Info("reader: Number of byte read: " + byteRead + " ; totalByteRead = " + totalByteRead
							 + " ; currentPosition=" + currentPosition + " ; chunkNumber =" + chunkNumber + 
							"; File name = " + fname);
					}
				}
			}
			catch (IOException e)
			{
				throw new IOException("#### Exception caught in readUntilEnd: reader  currentOffset = "
					 + currentPosition + " ; totalByteRead =" + totalByteRead + " ; latest byteRead = "
					 + byteRead + "; visibleLen= " + visibleLen + " ; bufferLen = " + buffer.Length 
					+ " ; Filename = " + fname, e);
			}
			if (verboseOption)
			{
				Log.Info("reader end:   position: " + pos + " ; currentOffset = " + currentPosition
					 + " ; totalByteRead =" + totalByteRead + " ; Filename = " + fname);
			}
			return totalByteRead;
		}

		/// <exception cref="System.IO.IOException"/>
		private void WriteData(FSDataOutputStream @out, byte[] buffer, int length)
		{
			int totalByteWritten = 0;
			int remainToWrite = length;
			while (remainToWrite > 0)
			{
				int toWriteThisRound = remainToWrite > buffer.Length ? buffer.Length : remainToWrite;
				@out.Write(buffer, 0, toWriteThisRound);
				totalByteWritten += toWriteThisRound;
				remainToWrite -= toWriteThisRound;
			}
			if (totalByteWritten != length)
			{
				throw new IOException("WriteData: failure in write. Attempt to write " + length +
					 " ; written=" + totalByteWritten);
			}
		}

		/// <summary>Common routine to do position read while open the file for write.</summary>
		/// <remarks>
		/// Common routine to do position read while open the file for write.
		/// After each iteration of write, do a read of the file from begin to end.
		/// Return 0 on success, else number of failure.
		/// </remarks>
		/// <exception cref="System.IO.IOException"/>
		private int TestWriteAndRead(string fname, int loopN, int chunkSize, long readBeginPosition
			)
		{
			int countOfFailures = 0;
			long byteVisibleToRead = 0;
			FSDataOutputStream @out = null;
			byte[] outBuffer = new byte[BufferSize];
			byte[] inBuffer = new byte[BufferSize];
			for (int i = 0; i < BufferSize; i++)
			{
				outBuffer[i] = unchecked((byte)(i & unchecked((int)(0x00ff))));
			}
			try
			{
				Path path = GetFullyQualifiedPath(fname);
				long fileLengthBeforeOpen = 0;
				if (IfExists(path))
				{
					if (truncateOption)
					{
						@out = useFCOption ? mfc.Create(path, EnumSet.Of(CreateFlag.Overwrite)) : mfs.Create
							(path, truncateOption);
						Log.Info("File already exists. File open with Truncate mode: " + path);
					}
					else
					{
						@out = useFCOption ? mfc.Create(path, EnumSet.Of(CreateFlag.Append)) : mfs.Append
							(path);
						fileLengthBeforeOpen = GetFileLengthFromNN(path);
						Log.Info("File already exists of size " + fileLengthBeforeOpen + " File open for Append mode: "
							 + path);
					}
				}
				else
				{
					@out = useFCOption ? mfc.Create(path, EnumSet.Of(CreateFlag.Create)) : mfs.Create
						(path);
				}
				long totalByteWritten = fileLengthBeforeOpen;
				long totalByteVisible = fileLengthBeforeOpen;
				long totalByteWrittenButNotVisible = 0;
				bool toFlush;
				for (int i_1 = 0; i_1 < loopN; i_1++)
				{
					toFlush = (i_1 % 2) == 0;
					WriteData(@out, outBuffer, chunkSize);
					totalByteWritten += chunkSize;
					if (toFlush)
					{
						@out.Hflush();
						totalByteVisible += chunkSize + totalByteWrittenButNotVisible;
						totalByteWrittenButNotVisible = 0;
					}
					else
					{
						totalByteWrittenButNotVisible += chunkSize;
					}
					if (verboseOption)
					{
						Log.Info("TestReadWrite - Written " + chunkSize + ". Total written = " + totalByteWritten
							 + ". TotalByteVisible = " + totalByteVisible + " to file " + fname);
					}
					byteVisibleToRead = ReadData(fname, inBuffer, totalByteVisible, readBeginPosition
						);
					string readmsg = "Written=" + totalByteWritten + " ; Expected Visible=" + totalByteVisible
						 + " ; Got Visible=" + byteVisibleToRead + " of file " + fname;
					if (byteVisibleToRead >= totalByteVisible && byteVisibleToRead <= totalByteWritten)
					{
						readmsg = "pass: reader sees expected number of visible byte. " + readmsg + " [pass]";
					}
					else
					{
						countOfFailures++;
						readmsg = "fail: reader see different number of visible byte. " + readmsg + " [fail]";
						throw new IOException(readmsg);
					}
					Log.Info(readmsg);
				}
				// test the automatic flush after close
				WriteData(@out, outBuffer, chunkSize);
				totalByteWritten += chunkSize;
				totalByteVisible += chunkSize + totalByteWrittenButNotVisible;
				totalByteWrittenButNotVisible += 0;
				@out.Close();
				byteVisibleToRead = ReadData(fname, inBuffer, totalByteVisible, readBeginPosition
					);
				string readmsg2 = "Written=" + totalByteWritten + " ; Expected Visible=" + totalByteVisible
					 + " ; Got Visible=" + byteVisibleToRead + " of file " + fname;
				string readmsg_1;
				if (byteVisibleToRead >= totalByteVisible && byteVisibleToRead <= totalByteWritten)
				{
					readmsg_1 = "pass: reader sees expected number of visible byte on close. " + readmsg2
						 + " [pass]";
				}
				else
				{
					countOfFailures++;
					readmsg_1 = "fail: reader sees different number of visible byte on close. " + readmsg2
						 + " [fail]";
					Log.Info(readmsg_1);
					throw new IOException(readmsg_1);
				}
				// now check if NN got the same length 
				long lenFromFc = GetFileLengthFromNN(path);
				if (lenFromFc != byteVisibleToRead)
				{
					readmsg_1 = "fail: reader sees different number of visible byte from NN " + readmsg2
						 + " [fail]";
					throw new IOException(readmsg_1);
				}
			}
			catch (IOException e)
			{
				throw new IOException("##### Caught Exception in testAppendWriteAndRead. Close file. "
					 + "Total Byte Read so far = " + byteVisibleToRead, e);
			}
			finally
			{
				if (@out != null)
				{
					@out.Close();
				}
			}
			return -countOfFailures;
		}

		// //////////////////////////////////////////////////////////////////////
		// // helper function:
		// /////////////////////////////////////////////////////////////////////
		/// <exception cref="System.IO.IOException"/>
		private FSDataInputStream OpenInputStream(Path path)
		{
			FSDataInputStream @in = useFCOption ? mfc.Open(path) : mfs.Open(path);
			return @in;
		}

		// length of a file (path name) from NN.
		/// <exception cref="System.IO.IOException"/>
		private long GetFileLengthFromNN(Path path)
		{
			FileStatus fileStatus = useFCOption ? mfc.GetFileStatus(path) : mfs.GetFileStatus
				(path);
			return fileStatus.GetLen();
		}

		/// <exception cref="System.IO.IOException"/>
		private bool IfExists(Path path)
		{
			return useFCOption ? mfc.Util().Exists(path) : mfs.Exists(path);
		}

		private Path GetFullyQualifiedPath(string pathString)
		{
			return useFCOption ? mfc.MakeQualified(new Path(RootDir, pathString)) : mfs.MakeQualified
				(new Path(RootDir, pathString));
		}

		private void Usage()
		{
			Log.Info("Usage: [-useSeqRead | -usePosRead] [-append|truncate]" + " -chunkSize nn -loop ntimes  -f filename"
				);
			System.Console.Out.WriteLine("Usage: [-useSeqRead | -usePosRead] [-append|truncate]"
				 + " -chunkSize nn -loop ntimes  -f filename");
			System.Console.Out.WriteLine("Defaults: -chunkSize=10000, -loop=10, -f=/tmp/fileX1, "
				 + "use sequential read, use append mode if file already exists");
			System.Environment.Exit(0);
		}

		private void DumpOptions()
		{
			Log.Info("  Option setting: filenameOption = " + filenameOption);
			Log.Info("  Option setting: chunkSizeOption = " + chunkSizeOption);
			Log.Info("  Option setting: loopOption = " + loopOption);
			Log.Info("  Option setting: posReadOption = " + positionReadOption);
			Log.Info("  Option setting: truncateOption = " + truncateOption);
			Log.Info("  Option setting: verboseOption = " + verboseOption);
		}

		private void GetCmdLineOption(string[] args)
		{
			for (int i = 0; i < args.Length; i++)
			{
				if (args[i].Equals("-f"))
				{
					filenameOption = args[++i];
				}
				else
				{
					if (args[i].Equals("-chunkSize"))
					{
						chunkSizeOption = System.Convert.ToInt32(args[++i]);
					}
					else
					{
						if (args[i].Equals("-loop"))
						{
							loopOption = System.Convert.ToInt32(args[++i]);
						}
						else
						{
							if (args[i].Equals("-usePosRead"))
							{
								positionReadOption = true;
							}
							else
							{
								if (args[i].Equals("-useSeqRead"))
								{
									positionReadOption = false;
								}
								else
								{
									if (args[i].Equals("-truncate"))
									{
										truncateOption = true;
									}
									else
									{
										if (args[i].Equals("-append"))
										{
											truncateOption = false;
										}
										else
										{
											if (args[i].Equals("-verbose"))
											{
												verboseOption = true;
											}
											else
											{
												if (args[i].Equals("-noVerbose"))
												{
													verboseOption = false;
												}
												else
												{
													Usage();
												}
											}
										}
									}
								}
							}
						}
					}
				}
			}
			if (verboseOption)
			{
				DumpOptions();
			}
			return;
		}

		/// <summary>Entry point of the test when using a real cluster.</summary>
		/// <remarks>
		/// Entry point of the test when using a real cluster.
		/// Usage: [-loop ntimes] [-chunkSize nn] [-f filename]
		/// [-useSeqRead |-usePosRead] [-append |-truncate] [-verbose |-noVerbose]
		/// -loop: iterate ntimes: each iteration consists of a write, then a read
		/// -chunkSize: number of byte for each write
		/// -f filename: filename to write and read
		/// [-useSeqRead | -usePosRead]: use Position Read, or default Sequential Read
		/// [-append | -truncate]: if file already exist, Truncate or default Append
		/// [-verbose | -noVerbose]: additional debugging messages if verbose is on
		/// Default: -loop = 10; -chunkSize = 10000; -f filename = /tmp/fileX1
		/// Use Sequential Read, Append Mode, verbose on.
		/// </remarks>
		public static void Main(string[] args)
		{
			try
			{
				TestWriteRead trw = new TestWriteRead();
				trw.InitClusterModeTest();
				trw.GetCmdLineOption(args);
				int stat = trw.ClusterTestWriteRead1();
				if (stat == 0)
				{
					System.Console.Out.WriteLine("Status: clusterTestWriteRead1 test PASS");
				}
				else
				{
					System.Console.Out.WriteLine("Status: clusterTestWriteRead1 test FAIL with " + stat
						 + " failures");
				}
				System.Environment.Exit(stat);
			}
			catch (IOException e)
			{
				Log.Info("#### Exception in Main");
				Sharpen.Runtime.PrintStackTrace(e);
				System.Environment.Exit(-2);
			}
		}
	}
}
