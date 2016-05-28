using System;
using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Util.Concurrent;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Org.Apache.Hadoop.Hdfs.Server.Datanode;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Tools
{
	/// <summary>This class implements debug operations on the HDFS command-line.</summary>
	/// <remarks>
	/// This class implements debug operations on the HDFS command-line.
	/// These operations are only for debugging, and may change or disappear
	/// between HDFS versions.
	/// </remarks>
	public class DebugAdmin : Configured, Tool
	{
		/// <summary>All the debug commands we can run.</summary>
		private DebugAdmin.DebugCommand[] DebugCommands = new DebugAdmin.DebugCommand[] { 
			new DebugAdmin.VerifyBlockChecksumCommand(this), new DebugAdmin.RecoverLeaseCommand
			(this), new DebugAdmin.HelpCommand(this) };

		/// <summary>The base class for debug commands.</summary>
		private abstract class DebugCommand
		{
			internal readonly string name;

			internal readonly string usageText;

			internal readonly string helpText;

			internal DebugCommand(DebugAdmin _enclosing, string name, string usageText, string
				 helpText)
			{
				this._enclosing = _enclosing;
				this.name = name;
				this.usageText = usageText;
				this.helpText = helpText;
			}

			/// <exception cref="System.IO.IOException"/>
			internal abstract int Run(IList<string> args);

			private readonly DebugAdmin _enclosing;
		}

		private static int HeaderLen = 7;

		/// <summary>The command for verifying a block metadata file and possibly block file.
		/// 	</summary>
		private class VerifyBlockChecksumCommand : DebugAdmin.DebugCommand
		{
			internal VerifyBlockChecksumCommand(DebugAdmin _enclosing)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
			}

			/// <exception cref="System.IO.IOException"/>
			internal override int Run(IList<string> args)
			{
				if (args.Count == 0)
				{
					System.Console.Out.WriteLine(this.usageText);
					System.Console.Out.WriteLine(this.helpText + "\n");
					return 1;
				}
				string blockFile = StringUtils.PopOptionWithArgument("-block", args);
				string metaFile = StringUtils.PopOptionWithArgument("-meta", args);
				if (metaFile == null)
				{
					System.Console.Error.WriteLine("You must specify a meta file with -meta");
					return 1;
				}
				FileInputStream metaStream = null;
				FileInputStream dataStream = null;
				FileChannel metaChannel = null;
				FileChannel dataChannel = null;
				DataInputStream checksumStream = null;
				try
				{
					BlockMetadataHeader header;
					try
					{
						metaStream = new FileInputStream(metaFile);
						checksumStream = new DataInputStream(metaStream);
						header = BlockMetadataHeader.ReadHeader(checksumStream);
						metaChannel = metaStream.GetChannel();
						metaChannel.Position(DebugAdmin.HeaderLen);
					}
					catch (RuntimeException e)
					{
						System.Console.Error.WriteLine("Failed to read HDFS metadata file header for " + 
							metaFile + ": " + StringUtils.StringifyException(e));
						return 1;
					}
					catch (IOException e)
					{
						System.Console.Error.WriteLine("Failed to read HDFS metadata file header for " + 
							metaFile + ": " + StringUtils.StringifyException(e));
						return 1;
					}
					DataChecksum checksum = header.GetChecksum();
					System.Console.Out.WriteLine("Checksum type: " + checksum.ToString());
					if (blockFile == null)
					{
						return 0;
					}
					ByteBuffer metaBuf;
					ByteBuffer dataBuf;
					try
					{
						dataStream = new FileInputStream(blockFile);
						dataChannel = dataStream.GetChannel();
						int ChecksumsPerBuf = 1024 * 32;
						metaBuf = ByteBuffer.Allocate(checksum.GetChecksumSize() * ChecksumsPerBuf);
						dataBuf = ByteBuffer.Allocate(checksum.GetBytesPerChecksum() * ChecksumsPerBuf);
					}
					catch (IOException e)
					{
						System.Console.Error.WriteLine("Failed to open HDFS block file for " + blockFile 
							+ ": " + StringUtils.StringifyException(e));
						return 1;
					}
					long offset = 0;
					while (true)
					{
						dataBuf.Clear();
						int dataRead = -1;
						try
						{
							dataRead = dataChannel.Read(dataBuf);
							if (dataRead < 0)
							{
								break;
							}
						}
						catch (IOException e)
						{
							System.Console.Error.WriteLine("Got I/O error reading block file " + blockFile + 
								"from disk at offset " + dataChannel.Position() + ": " + StringUtils.StringifyException
								(e));
							return 1;
						}
						try
						{
							int csumToRead = (((checksum.GetBytesPerChecksum() - 1) + dataRead) / checksum.GetBytesPerChecksum
								()) * checksum.GetChecksumSize();
							metaBuf.Clear();
							metaBuf.Limit(csumToRead);
							metaChannel.Read(metaBuf);
							dataBuf.Flip();
							metaBuf.Flip();
						}
						catch (IOException e)
						{
							System.Console.Error.WriteLine("Got I/O error reading metadata file " + metaFile 
								+ "from disk at offset " + metaChannel.Position() + ": " + StringUtils.StringifyException
								(e));
							return 1;
						}
						try
						{
							checksum.VerifyChunkedSums(dataBuf, metaBuf, blockFile, offset);
						}
						catch (IOException e)
						{
							System.Console.Out.WriteLine("verifyChunkedSums error: " + StringUtils.StringifyException
								(e));
							return 1;
						}
						offset += dataRead;
					}
					System.Console.Out.WriteLine("Checksum verification succeeded on block file " + blockFile
						);
					return 0;
				}
				finally
				{
					IOUtils.Cleanup(null, metaStream, dataStream, checksumStream);
				}
			}

			private readonly DebugAdmin _enclosing;
		}

		/// <summary>The command for recovering a file lease.</summary>
		private class RecoverLeaseCommand : DebugAdmin.DebugCommand
		{
			internal RecoverLeaseCommand(DebugAdmin _enclosing)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
			}

			private const int TimeoutMs = 5000;

			/// <exception cref="System.IO.IOException"/>
			internal override int Run(IList<string> args)
			{
				if (args.Count == 0)
				{
					System.Console.Out.WriteLine(this.usageText);
					System.Console.Out.WriteLine(this.helpText + "\n");
					return 1;
				}
				string pathStr = StringUtils.PopOptionWithArgument("-path", args);
				string retriesStr = StringUtils.PopOptionWithArgument("-retries", args);
				if (pathStr == null)
				{
					System.Console.Error.WriteLine("You must supply a -path argument to " + "recoverLease."
						);
					return 1;
				}
				int maxRetries = 1;
				if (retriesStr != null)
				{
					try
					{
						maxRetries = System.Convert.ToInt32(retriesStr);
					}
					catch (FormatException e)
					{
						System.Console.Error.WriteLine("Failed to parse the argument to -retries: " + StringUtils
							.StringifyException(e));
						return 1;
					}
				}
				FileSystem fs;
				try
				{
					fs = FileSystem.NewInstance(new URI(pathStr), this._enclosing.GetConf(), null);
				}
				catch (URISyntaxException e)
				{
					System.Console.Error.WriteLine("URISyntaxException for " + pathStr + ":" + StringUtils
						.StringifyException(e));
					return 1;
				}
				catch (Exception e)
				{
					System.Console.Error.WriteLine("InterruptedException for " + pathStr + ":" + StringUtils
						.StringifyException(e));
					return 1;
				}
				DistributedFileSystem dfs = null;
				try
				{
					dfs = (DistributedFileSystem)fs;
				}
				catch (InvalidCastException)
				{
					System.Console.Error.WriteLine("Invalid filesystem for path " + pathStr + ": " + 
						"needed scheme hdfs, but got: " + fs.GetScheme());
					return 1;
				}
				for (int retry = 0; true; )
				{
					bool recovered = false;
					IOException ioe = null;
					try
					{
						recovered = dfs.RecoverLease(new Path(pathStr));
					}
					catch (IOException e)
					{
						ioe = e;
					}
					if (recovered)
					{
						System.Console.Out.WriteLine("recoverLease SUCCEEDED on " + pathStr);
						return 0;
					}
					if (ioe != null)
					{
						System.Console.Error.WriteLine("recoverLease got exception: ");
						Sharpen.Runtime.PrintStackTrace(ioe);
					}
					else
					{
						System.Console.Error.WriteLine("recoverLease returned false.");
					}
					retry++;
					if (retry >= maxRetries)
					{
						break;
					}
					System.Console.Error.WriteLine("Retrying in " + DebugAdmin.RecoverLeaseCommand.TimeoutMs
						 + " ms...");
					Uninterruptibles.SleepUninterruptibly(DebugAdmin.RecoverLeaseCommand.TimeoutMs, TimeUnit
						.Milliseconds);
					System.Console.Error.WriteLine("Retry #" + retry);
				}
				System.Console.Error.WriteLine("Giving up on recoverLease for " + pathStr + " after "
					 + maxRetries + (maxRetries == 1 ? " try." : " tries."));
				return 1;
			}

			private readonly DebugAdmin _enclosing;
		}

		/// <summary>The command for getting help about other commands.</summary>
		private class HelpCommand : DebugAdmin.DebugCommand
		{
			internal HelpCommand(DebugAdmin _enclosing)
				: base(_enclosing)
			{
				this._enclosing = _enclosing;
			}

			internal override int Run(IList<string> args)
			{
				DebugAdmin.DebugCommand command = this._enclosing.PopCommand(args);
				if (command == null)
				{
					this._enclosing.PrintUsage();
					return 0;
				}
				System.Console.Out.WriteLine(command.usageText);
				System.Console.Out.WriteLine(command.helpText + "\n");
				return 0;
			}

			private readonly DebugAdmin _enclosing;
		}

		public DebugAdmin(Configuration conf)
			: base(conf)
		{
		}

		private DebugAdmin.DebugCommand PopCommand(IList<string> args)
		{
			string commandStr = (args.Count == 0) ? string.Empty : args[0];
			if (commandStr.StartsWith("-"))
			{
				commandStr = Sharpen.Runtime.Substring(commandStr, 1);
			}
			foreach (DebugAdmin.DebugCommand command in DebugCommands)
			{
				if (command.name.Equals(commandStr))
				{
					args.Remove(0);
					return command;
				}
			}
			return null;
		}

		public virtual int Run(string[] argv)
		{
			List<string> args = new List<string>();
			for (int j = 0; j < argv.Length; ++j)
			{
				args.AddItem(argv[j]);
			}
			DebugAdmin.DebugCommand command = PopCommand(args);
			if (command == null)
			{
				PrintUsage();
				return 0;
			}
			try
			{
				return command.Run(args);
			}
			catch (IOException e)
			{
				System.Console.Error.WriteLine("IOException: " + StringUtils.StringifyException(e
					));
				return 1;
			}
			catch (RuntimeException e)
			{
				System.Console.Error.WriteLine("RuntimeException: " + StringUtils.StringifyException
					(e));
				return 1;
			}
		}

		private void PrintUsage()
		{
			System.Console.Out.WriteLine("Usage: hdfs debug <command> [arguments]\n");
			foreach (DebugAdmin.DebugCommand command in DebugCommands)
			{
				if (!command.name.Equals("help"))
				{
					System.Console.Out.WriteLine(command.usageText);
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public static void Main(string[] argsArray)
		{
			DebugAdmin debugAdmin = new DebugAdmin(new Configuration());
			System.Environment.Exit(debugAdmin.Run(argsArray));
		}
	}
}
