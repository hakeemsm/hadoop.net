using System;
using System.IO;
using System.Net;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs
{
	/// <summary>
	/// This program executes a specified operation that applies load to
	/// the NameNode.
	/// </summary>
	/// <remarks>
	/// This program executes a specified operation that applies load to
	/// the NameNode. Possible operations include create/writing files,
	/// opening/reading files, renaming files, and deleting files.
	/// When run simultaneously on multiple nodes, this program functions
	/// as a stress-test and benchmark for namenode, especially when
	/// the number of bytes written to each file is small.
	/// This version does not use the map reduce framework
	/// </remarks>
	public class NNBenchWithoutMR
	{
		private static readonly Log Log = LogFactory.GetLog("org.apache.hadoop.hdfs.NNBench"
			);

		private static long startTime = 0;

		private static int numFiles = 0;

		private static long bytesPerBlock = 1;

		private static long blocksPerFile = 0;

		private static long bytesPerFile = 1;

		private static Path baseDir = null;

		private static FileSystem fileSys = null;

		private static Path taskDir = null;

		private static byte[] buffer;

		private static long maxExceptionsPerFile = 200;

		// variable initialzed from command line arguments
		// variables initialized in main()
		/// <summary>
		/// Returns when the current number of seconds from the epoch equals
		/// the command line argument given by <code>-startTime</code>.
		/// </summary>
		/// <remarks>
		/// Returns when the current number of seconds from the epoch equals
		/// the command line argument given by <code>-startTime</code>.
		/// This allows multiple instances of this program, running on clock
		/// synchronized nodes, to start at roughly the same time.
		/// </remarks>
		internal static void Barrier()
		{
			long sleepTime;
			while ((sleepTime = startTime - Runtime.CurrentTimeMillis()) > 0)
			{
				try
				{
					Sharpen.Thread.Sleep(sleepTime);
				}
				catch (Exception)
				{
				}
			}
		}

		//This left empty on purpose
		private static void HandleException(string operation, Exception e, int singleFileExceptions
			)
		{
			Log.Warn("Exception while " + operation + ": " + StringUtils.StringifyException(e
				));
			if (singleFileExceptions >= maxExceptionsPerFile)
			{
				throw new RuntimeException(singleFileExceptions + " exceptions for a single file exceeds threshold. Aborting"
					);
			}
		}

		/// <summary>Create and write to a given number of files.</summary>
		/// <remarks>
		/// Create and write to a given number of files.  Repeat each remote
		/// operation until is suceeds (does not throw an exception).
		/// </remarks>
		/// <returns>the number of exceptions caught</returns>
		internal static int CreateWrite()
		{
			int totalExceptions = 0;
			FSDataOutputStream @out = null;
			bool success;
			for (int index = 0; index < numFiles; index++)
			{
				int singleFileExceptions = 0;
				do
				{
					// create file until is succeeds or max exceptions reached
					try
					{
						@out = fileSys.Create(new Path(taskDir, string.Empty + index), false, 512, (short
							)1, bytesPerBlock);
						success = true;
					}
					catch (IOException ioe)
					{
						success = false;
						totalExceptions++;
						HandleException("creating file #" + index, ioe, ++singleFileExceptions);
					}
				}
				while (!success);
				long toBeWritten = bytesPerFile;
				while (toBeWritten > 0)
				{
					int nbytes = (int)Math.Min(buffer.Length, toBeWritten);
					toBeWritten -= nbytes;
					try
					{
						// only try once
						@out.Write(buffer, 0, nbytes);
					}
					catch (IOException ioe)
					{
						totalExceptions++;
						HandleException("writing to file #" + index, ioe, ++singleFileExceptions);
					}
				}
				do
				{
					// close file until is succeeds
					try
					{
						@out.Close();
						success = true;
					}
					catch (IOException ioe)
					{
						success = false;
						totalExceptions++;
						HandleException("closing file #" + index, ioe, ++singleFileExceptions);
					}
				}
				while (!success);
			}
			return totalExceptions;
		}

		/// <summary>Open and read a given number of files.</summary>
		/// <returns>the number of exceptions caught</returns>
		internal static int OpenRead()
		{
			int totalExceptions = 0;
			FSDataInputStream @in;
			for (int index = 0; index < numFiles; index++)
			{
				int singleFileExceptions = 0;
				try
				{
					@in = fileSys.Open(new Path(taskDir, string.Empty + index), 512);
					long toBeRead = bytesPerFile;
					while (toBeRead > 0)
					{
						int nbytes = (int)Math.Min(buffer.Length, toBeRead);
						toBeRead -= nbytes;
						try
						{
							// only try once && we don't care about a number of bytes read
							@in.Read(buffer, 0, nbytes);
						}
						catch (IOException ioe)
						{
							totalExceptions++;
							HandleException("reading from file #" + index, ioe, ++singleFileExceptions);
						}
					}
					@in.Close();
				}
				catch (IOException ioe)
				{
					totalExceptions++;
					HandleException("opening file #" + index, ioe, ++singleFileExceptions);
				}
			}
			return totalExceptions;
		}

		/// <summary>Rename a given number of files.</summary>
		/// <remarks>
		/// Rename a given number of files.  Repeat each remote
		/// operation until is suceeds (does not throw an exception).
		/// </remarks>
		/// <returns>the number of exceptions caught</returns>
		internal static int Rename()
		{
			int totalExceptions = 0;
			bool success;
			for (int index = 0; index < numFiles; index++)
			{
				int singleFileExceptions = 0;
				do
				{
					// rename file until is succeeds
					try
					{
						// Possible result of this operation is at no interest to us for it
						// can return false only if the namesystem
						// could rename the path from the name
						// space (e.g. no Exception has been thrown)
						fileSys.Rename(new Path(taskDir, string.Empty + index), new Path(taskDir, "A" + index
							));
						success = true;
					}
					catch (IOException ioe)
					{
						success = false;
						totalExceptions++;
						HandleException("creating file #" + index, ioe, ++singleFileExceptions);
					}
				}
				while (!success);
			}
			return totalExceptions;
		}

		/// <summary>Delete a given number of files.</summary>
		/// <remarks>
		/// Delete a given number of files.  Repeat each remote
		/// operation until is suceeds (does not throw an exception).
		/// </remarks>
		/// <returns>the number of exceptions caught</returns>
		internal static int Delete()
		{
			int totalExceptions = 0;
			bool success;
			for (int index = 0; index < numFiles; index++)
			{
				int singleFileExceptions = 0;
				do
				{
					// delete file until is succeeds
					try
					{
						// Possible result of this operation is at no interest to us for it
						// can return false only if namesystem
						// delete could remove the path from the name
						// space (e.g. no Exception has been thrown)
						fileSys.Delete(new Path(taskDir, "A" + index), true);
						success = true;
					}
					catch (IOException ioe)
					{
						success = false;
						totalExceptions++;
						HandleException("creating file #" + index, ioe, ++singleFileExceptions);
					}
				}
				while (!success);
			}
			return totalExceptions;
		}

		/// <summary>
		/// This launches a given namenode operation (<code>-operation</code>),
		/// starting at a given time (<code>-startTime</code>).
		/// </summary>
		/// <remarks>
		/// This launches a given namenode operation (<code>-operation</code>),
		/// starting at a given time (<code>-startTime</code>).  The files used
		/// by the openRead, rename, and delete operations are the same files
		/// created by the createWrite operation.  Typically, the program
		/// would be run four times, once for each operation in this order:
		/// createWrite, openRead, rename, delete.
		/// <pre>
		/// Usage: nnbench
		/// -operation <one of createWrite, openRead, rename, or delete>
		/// -baseDir <base output/input DFS path>
		/// -startTime <time to start, given in seconds from the epoch>
		/// -numFiles <number of files to create, read, rename, or delete>
		/// -blocksPerFile <number of blocks to create per file>
		/// [-bytesPerBlock <number of bytes to write to each block, default is 1>]
		/// [-bytesPerChecksum <value for io.bytes.per.checksum>]
		/// </pre>
		/// </remarks>
		/// <param name="args">is an array of the program command line arguments</param>
		/// <exception cref="System.IO.IOException">indicates a problem with test startup</exception>
		public static void Main(string[] args)
		{
			string version = "NameNodeBenchmark.0.3";
			System.Console.Out.WriteLine(version);
			int bytesPerChecksum = -1;
			string usage = "Usage: nnbench " + "  -operation <one of createWrite, openRead, rename, or delete> "
				 + "  -baseDir <base output/input DFS path> " + "  -startTime <time to start, given in seconds from the epoch> "
				 + "  -numFiles <number of files to create> " + "  -blocksPerFile <number of blocks to create per file> "
				 + "  [-bytesPerBlock <number of bytes to write to each block, default is 1>] " 
				+ "  [-bytesPerChecksum <value for io.bytes.per.checksum>]" + "Note: bytesPerBlock MUST be a multiple of bytesPerChecksum";
			string operation = null;
			for (int i = 0; i < args.Length; i++)
			{
				// parse command line
				if (args[i].Equals("-baseDir"))
				{
					baseDir = new Path(args[++i]);
				}
				else
				{
					if (args[i].Equals("-numFiles"))
					{
						numFiles = System.Convert.ToInt32(args[++i]);
					}
					else
					{
						if (args[i].Equals("-blocksPerFile"))
						{
							blocksPerFile = System.Convert.ToInt32(args[++i]);
						}
						else
						{
							if (args[i].Equals("-bytesPerBlock"))
							{
								bytesPerBlock = long.Parse(args[++i]);
							}
							else
							{
								if (args[i].Equals("-bytesPerChecksum"))
								{
									bytesPerChecksum = System.Convert.ToInt32(args[++i]);
								}
								else
								{
									if (args[i].Equals("-startTime"))
									{
										startTime = long.Parse(args[++i]) * 1000;
									}
									else
									{
										if (args[i].Equals("-operation"))
										{
											operation = args[++i];
										}
										else
										{
											System.Console.Out.WriteLine(usage);
											System.Environment.Exit(-1);
										}
									}
								}
							}
						}
					}
				}
			}
			bytesPerFile = bytesPerBlock * blocksPerFile;
			JobConf jobConf = new JobConf(new Configuration(), typeof(NNBench));
			if (bytesPerChecksum < 0)
			{
				// if it is not set in cmdline
				bytesPerChecksum = jobConf.GetInt("io.bytes.per.checksum", 512);
			}
			jobConf.Set("io.bytes.per.checksum", Sharpen.Extensions.ToString(bytesPerChecksum
				));
			System.Console.Out.WriteLine("Inputs: ");
			System.Console.Out.WriteLine("   operation: " + operation);
			System.Console.Out.WriteLine("   baseDir: " + baseDir);
			System.Console.Out.WriteLine("   startTime: " + startTime);
			System.Console.Out.WriteLine("   numFiles: " + numFiles);
			System.Console.Out.WriteLine("   blocksPerFile: " + blocksPerFile);
			System.Console.Out.WriteLine("   bytesPerBlock: " + bytesPerBlock);
			System.Console.Out.WriteLine("   bytesPerChecksum: " + bytesPerChecksum);
			if (operation == null || baseDir == null || numFiles < 1 || blocksPerFile < 1 || 
				bytesPerBlock < 0 || bytesPerBlock % bytesPerChecksum != 0)
			{
				// verify args
				System.Console.Error.WriteLine(usage);
				System.Environment.Exit(-1);
			}
			fileSys = FileSystem.Get(jobConf);
			string uniqueId = Sharpen.Runtime.GetLocalHost().GetHostName();
			taskDir = new Path(baseDir, uniqueId);
			// initialize buffer used for writing/reading file
			buffer = new byte[(int)Math.Min(bytesPerFile, 32768L)];
			DateTime execTime;
			DateTime endTime;
			long duration;
			int exceptions = 0;
			Barrier();
			// wait for coordinated start time
			execTime = new DateTime();
			System.Console.Out.WriteLine("Job started: " + startTime);
			if (operation.Equals("createWrite"))
			{
				if (!fileSys.Mkdirs(taskDir))
				{
					throw new IOException("Mkdirs failed to create " + taskDir.ToString());
				}
				exceptions = CreateWrite();
			}
			else
			{
				if (operation.Equals("openRead"))
				{
					exceptions = OpenRead();
				}
				else
				{
					if (operation.Equals("rename"))
					{
						exceptions = Rename();
					}
					else
					{
						if (operation.Equals("delete"))
						{
							exceptions = Delete();
						}
						else
						{
							System.Console.Error.WriteLine(usage);
							System.Environment.Exit(-1);
						}
					}
				}
			}
			endTime = new DateTime();
			System.Console.Out.WriteLine("Job ended: " + endTime);
			duration = (endTime.GetTime() - execTime.GetTime()) / 1000;
			System.Console.Out.WriteLine("The " + operation + " job took " + duration + " seconds."
				);
			System.Console.Out.WriteLine("The job recorded " + exceptions + " exceptions.");
		}
	}
}
